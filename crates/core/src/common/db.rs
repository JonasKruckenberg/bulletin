//! The database seam: how the engine connects, migrates, and — from M2 Phase 4 on — how every
//! scoped query reaches the DB through a single RLS-aware chokepoint.
//!
//! **Two-context RLS (design §12).** The runtime role is a non-owner, non-superuser role with no
//! `BYPASSRLS`; `event` and `cluster` (the scope-bearing content tables) carry `FORCE ROW LEVEL
//! SECURITY` policies keyed on a transaction-local `app.subscriber_id` GUC. All scoped work passes
//! through [`with_scope`] / [`begin_scope`], which set that GUC, so a logic bug that forgets a
//! `scope_subscriber_id` predicate still cannot read or write another tenant's private rows — the DB
//! refuses. The typed [`Scope`](crate::common::scope::Scope) + the build-path scope-invariant
//! proptest stay the *primary* defense; RLS is the backstop.

use std::future::Future;
use std::pin::Pin;

use anyhow::{Context, Result};
use sqlx::{PgConnection, PgExecutor, PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::common::scope::Scope;

/// The name of the least-privilege runtime role the app logs in as (serve / worker / debug). Created
/// by the RLS migration, granted by [`grant_runtime_role`], and named in every policy. The
/// owner/migration role (which owns the DDL) is a separate role on a separate connection string.
pub const RUNTIME_ROLE: &str = "bulletin_app";

/// The RLS context a unit of DB work runs in — selects which row-policy applies. Two table families,
/// two semantics for the same context:
///
/// - **Content tables** (`event`, `cluster`): the no-subscriber context sees/writes only public
///   rows; a subscriber context adds its own private rows; **`Admin` gets no extra reach** (public
///   only), so there is no backdoor to another tenant's private content — it is readable *only* in
///   its owner's context.
/// - **Control-plane / delivery tables** (`connection`, `subscriber`, `digest`, `digest_item`,
///   `private_build_watermark`): the **default (no-subscriber) context is denied entirely**
///   (fail-closed); a subscriber context sees only its own rows; `Admin` is the explicit
///   control-plane reach the cron sweeps, status, and operator/debug commands opt into.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScopeCtx {
    /// PublicBuild, public ingest, and public-only content reads. Maps to an empty `app.subscriber_id`
    /// — public content only, and **no** control-plane access (those tables fail closed here).
    NoSubscriber,
    /// One subscriber's context (private-build, generate, digest delivery): public ∪ own-private
    /// content, and own control-plane/delivery rows.
    Subscriber(Uuid),
    /// The control-plane context for trusted, cross-tenant orchestration that names no single
    /// subscriber: the cron tick's due-sweeps, `status`, the poll/webhook connection lookups, and
    /// operator/debug commands. Reaches every control-plane row — but, deliberately, **not** another
    /// tenant's private content (content tables treat it like the no-subscriber context: public only).
    Admin,
}

impl ScopeCtx {
    /// The context a finalized event must be written in: a public event ingests in the
    /// no-subscriber context, a private event as its owner — mirroring the write side of the policy
    /// (public writes only with no subscriber set; private writes only by the owner).
    pub fn for_scope(scope: &Scope) -> ScopeCtx {
        match scope {
            Scope::Public => ScopeCtx::NoSubscriber,
            Scope::Private(owner) => ScopeCtx::Subscriber(*owner),
        }
    }

    /// The `app.subscriber_id` GUC value: empty for the no-subscriber context (the policy treats
    /// empty and unset alike → public content, control-plane denied), the subscriber's UUID text for
    /// a subscriber context, and the `*` sentinel for `Admin` (no real UUID is `*`, so it only ever
    /// satisfies the explicit control-plane `= '*'` clause, never an "own row" comparison).
    fn guc(&self) -> String {
        match self {
            ScopeCtx::NoSubscriber => String::new(),
            ScopeCtx::Subscriber(id) => id.to_string(),
            ScopeCtx::Admin => "*".to_string(),
        }
    }
}

pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPool::connect(database_url).await
}

pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    let mut m = sqlx::migrate!("./migrations");
    m.ignore_missing = true;
    m.run(pool).await
}

/// Sets the transaction-local `app.subscriber_id` GUC the RLS policies read. Uses `set_config(...,
/// is_local => true)` — the function form of `SET LOCAL`, so it accepts a bind parameter and is
/// transaction-scoped (auto-reset at COMMIT/ROLLBACK), which makes it pool- and PgBouncer-safe (no
/// leak onto the next checkout of a pooled connection). Must run inside a transaction to have effect.
pub async fn set_scope(executor: impl PgExecutor<'_>, ctx: ScopeCtx) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config('app.subscriber_id', $1, true)")
        .bind(ctx.guc())
        .execute(executor)
        .await?;
    Ok(())
}

/// Opens a transaction already pinned to `ctx`. The lower-level primitive behind [`with_scope`], for
/// the few flows that must own the transaction directly (the builds, which hold an advisory lock and
/// advance a watermark across several statements). Caller commits/rolls back.
pub async fn begin_scope(
    pool: &PgPool,
    ctx: ScopeCtx,
) -> Result<Transaction<'static, Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    set_scope(&mut *tx, ctx).await?;
    Ok(tx)
}

/// The canonical scoped-connection path: open a transaction pinned to `ctx`, hand its connection to
/// `f`, and commit on `Ok` / roll back on `Err`. Every query `f` runs is governed by the RLS policy
/// for `ctx`, so isolation holds even if a query forgets its own scope predicate. Prefer this over a
/// bare `pool` for anything touching `event` / `cluster`.
pub async fn with_scope<T, F>(pool: &PgPool, ctx: ScopeCtx, f: F) -> Result<T>
where
    F: for<'c> FnOnce(&'c mut PgConnection) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>,
{
    let mut tx = begin_scope(pool, ctx)
        .await
        .context("open scoped transaction")?;
    match f(&mut tx).await {
        Ok(value) => {
            tx.commit().await.context("commit scoped transaction")?;
            Ok(value)
        }
        Err(e) => {
            // Best-effort rollback; surface the original error regardless.
            let _ = tx.rollback().await;
            Err(e)
        }
    }
}

/// Grants the least-privilege runtime role the table/sequence access it needs — re-run on every
/// `migrate` so tables added by a later migration (and the apalis queue schema, created after the
/// domain migrations) are always covered. Idempotent. Run by the **owner** connection; the runtime
/// role itself can grant nothing.
///
/// This is privilege plumbing, deliberately *not* in a checksum-frozen migration: the grant set
/// tracks the current schema rather than a point-in-time snapshot, and it must also reach the apalis
/// schema, which doesn't exist when the domain migrations run.
pub async fn grant_runtime_role(pool: &PgPool) -> Result<(), sqlx::Error> {
    for stmt in [
        format!("GRANT USAGE ON SCHEMA public TO {RUNTIME_ROLE}"),
        format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {RUNTIME_ROLE}"
        ),
        format!("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {RUNTIME_ROLE}"),
    ] {
        sqlx::query(&stmt).execute(pool).await?;
    }

    // The apalis queue lives in its own schema, created by `setup_storage` after the domain
    // migrations — grant it only once it exists (it won't in the pure DB tests that skip the queue).
    let apalis_present: bool =
        sqlx::query("SELECT to_regnamespace('apalis') IS NOT NULL AS present")
            .fetch_one(pool)
            .await?
            .try_get("present")?;
    if apalis_present {
        for stmt in [
            format!("GRANT USAGE ON SCHEMA apalis TO {RUNTIME_ROLE}"),
            format!("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA apalis TO {RUNTIME_ROLE}"),
            format!("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA apalis TO {RUNTIME_ROLE}"),
            format!("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA apalis TO {RUNTIME_ROLE}"),
        ] {
            sqlx::query(&stmt).execute(pool).await?;
        }
    }
    Ok(())
}
