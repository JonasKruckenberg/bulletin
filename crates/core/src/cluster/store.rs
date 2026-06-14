//! The PublicBuild store contract: the build watermark, the dirty-group scan, and the cluster
//! upsert. Durable state is the events; the `cluster` rows and `build_watermark` are a rebuildable
//! cache this advances.

use crate::cluster::ClusterRollup;
use crate::common::{
    event::{from_row, Event},
    kind::SourceKind,
    scope::Scope,
};
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgExecutor, Row};
use uuid::Uuid;

/// A 64-bit key for the PublicBuild transaction-level advisory lock. Arbitrary but fixed;
/// `pg_try_advisory_xact_lock` auto-releases at transaction end (no leak on crash).
const BUILD_LOCK_KEY: i64 = 0x6275_6c6c_6574_6e01; // "bulletn\x01"

/// Tries to take the PublicBuild advisory lock on `executor`'s transaction. Returns `false`
/// if another build holds it — the caller then no-ops (its events are covered by the holder).
pub async fn try_build_lock(executor: impl PgExecutor<'_>) -> Result<bool, sqlx::Error> {
    let row = sqlx::query("SELECT pg_try_advisory_xact_lock($1) AS locked")
        .bind(BUILD_LOCK_KEY)
        .fetch_one(executor)
        .await?;
    Ok(row.get("locked"))
}

/// Reads `(built_through, now())` in one shot. `now()` is the high-watermark snapshot for
/// this build; processing the half-open range `(built_through, hwm]` and advancing to `hwm`
/// keeps the watermark monotonic and race-free against concurrent inserts.
pub async fn build_bounds(
    executor: impl PgExecutor<'_>,
) -> Result<(DateTime<Utc>, DateTime<Utc>), sqlx::Error> {
    let row = sqlx::query("SELECT built_through, now() AS hwm FROM build_watermark")
        .fetch_one(executor)
        .await?;
    Ok((row.get("built_through"), row.get("hwm")))
}

/// Distinct public `(source, group_key)` groups touched by events ingested in `(lo, hi]` —
/// the "dirty" groups PublicBuild must recompute this pass.
pub async fn dirty_public_groups(
    executor: impl PgExecutor<'_>,
    lo: DateTime<Utc>,
    hi: DateTime<Utc>,
) -> Result<Vec<(SourceKind, String)>, sqlx::Error> {
    sqlx::query(
        "SELECT DISTINCT source, group_key
         FROM event
         WHERE scope_kind = 'public' AND ingest_time > $1 AND ingest_time <= $2",
    )
    .bind(lo)
    .bind(hi)
    .try_map(|row: PgRow| Ok((row.try_get("source")?, row.get::<String, _>("group_key"))))
    .fetch_all(executor)
    .await
}

/// Upserts the recomputed rollup for one group in the given `scope`. Idempotent: re-running a build
/// overwrites the cache in place (durable state is the events, not this row). Scope is part of the
/// cluster identity, so a public and a private group with the same `(source, group_key)` are
/// distinct rows — a private event can never land in a public cluster.
pub async fn upsert_cluster(
    executor: impl PgExecutor<'_>,
    scope: &Scope,
    source: SourceKind,
    group_key: &str,
    r: &ClusterRollup,
) -> Result<Uuid, sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = match scope {
        Scope::Public => ("public", None::<Uuid>),
        Scope::Private(sub) => ("private", Some(*sub)),
    };
    let row = sqlx::query(
        "INSERT INTO cluster
            (scope_kind, scope_subscriber_id, source, group_key, title, link, last_event_time, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, now())
         ON CONFLICT ON CONSTRAINT cluster_identity DO UPDATE SET
            title = EXCLUDED.title,
            link = EXCLUDED.link,
            last_event_time = EXCLUDED.last_event_time,
            updated_at = now()
         RETURNING id",
    )
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(source)
    .bind(group_key)
    .bind(&r.title)
    .bind(r.link.as_deref())
    .bind(r.last_event_time)
    .fetch_one(executor)
    .await?;
    Ok(row.get("id"))
}

/// Advances the build watermark to `hwm` (monotonic via GREATEST).
pub async fn advance_build_watermark(
    executor: impl PgExecutor<'_>,
    hwm: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE build_watermark SET built_through = GREATEST(built_through, $1)")
        .bind(hwm)
        .execute(executor)
        .await?;
    Ok(())
}

/// True iff any public event is ingested-but-not-yet-built. The tick uses this to decide
/// whether to enqueue a PublicBuild (watermark-gated; no redundant builds when quiet).
pub async fn unbuilt_public_events_exist(
    executor: impl PgExecutor<'_>,
) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(
        "SELECT EXISTS (
            SELECT 1 FROM event
            WHERE scope_kind = 'public'
              AND ingest_time > (SELECT built_through FROM build_watermark)
         ) AS dirty",
    )
    .fetch_one(executor)
    .await?;
    Ok(row.get("dirty"))
}

/// Loads every public event in one within-source group `(source, group_key)`, ordered by
/// `(event_time, id)` so the pure `rollup` is deterministic. The build's drain-read over the
/// event log — called once per dirty group.
pub async fn list_public_group_events(
    executor: impl PgExecutor<'_>,
    source: SourceKind,
    group_key: &str,
) -> Result<Vec<Event>, sqlx::Error> {
    sqlx::query(
        "SELECT id, fingerprint, source, scope_kind, scope_subscriber_id,
                event_time, title, body, links, group_key, entities,
                content_kind, severity_hint, ingest_time, raw
         FROM event
         WHERE scope_kind = 'public' AND source = $1 AND group_key = $2
         ORDER BY event_time, id",
    )
    .bind(source)
    .bind(group_key)
    .try_map(from_row)
    .fetch_all(executor)
    .await
}

// ── Private build (per subscriber, just-in-time) ──────────────────────────

/// Distinct private `(source, group_key)` groups owned by `subscriber_id` — the groups the
/// just-in-time private build recomputes before a subscriber's digest. Unlike the public build there
/// is no watermark: private volume per subscriber is small and the rollup is idempotent, so each
/// digest simply rebuilds the owner's private clusters (design §9.1).
pub async fn dirty_private_groups(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
) -> Result<Vec<(SourceKind, String)>, sqlx::Error> {
    sqlx::query(
        "SELECT DISTINCT source, group_key
         FROM event
         WHERE scope_kind = 'private' AND scope_subscriber_id = $1",
    )
    .bind(subscriber_id)
    .try_map(|row: PgRow| Ok((row.try_get("source")?, row.get::<String, _>("group_key"))))
    .fetch_all(executor)
    .await
}

/// Loads every private event owned by `subscriber_id` in one within-source group, ordered by
/// `(event_time, id)` so `rollup` is deterministic. The private counterpart to
/// [`list_public_group_events`]; the `scope_subscriber_id = $1` predicate is the isolation boundary.
pub async fn list_private_group_events(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    source: SourceKind,
    group_key: &str,
) -> Result<Vec<Event>, sqlx::Error> {
    sqlx::query(
        "SELECT id, fingerprint, source, scope_kind, scope_subscriber_id,
                event_time, title, body, links, group_key, entities,
                content_kind, severity_hint, ingest_time, raw
         FROM event
         WHERE scope_kind = 'private' AND scope_subscriber_id = $1
           AND source = $2 AND group_key = $3
         ORDER BY event_time, id",
    )
    .bind(subscriber_id)
    .bind(source)
    .bind(group_key)
    .try_map(from_row)
    .fetch_all(executor)
    .await
}
