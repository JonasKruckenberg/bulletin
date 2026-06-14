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

/// Distinct `(source, group_key)` groups *in `scope`* touched by events ingested in `(lo, hi]` — the
/// "dirty" groups a build (public or private) must recompute this pass. `IS NOT DISTINCT FROM`
/// matches the scope's nullable subscriber uniformly: public's NULL against NULL rows, a private
/// owner against their own — the isolation boundary, shared by both builds.
pub async fn dirty_groups(
    executor: impl PgExecutor<'_>,
    scope: &Scope,
    lo: DateTime<Utc>,
    hi: DateTime<Utc>,
) -> Result<Vec<(SourceKind, String)>, sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = scope.to_columns();
    sqlx::query(
        "SELECT DISTINCT source, group_key
         FROM event
         WHERE scope_kind = $1 AND scope_subscriber_id IS NOT DISTINCT FROM $2
           AND ingest_time > $3 AND ingest_time <= $4",
    )
    .bind(scope_kind)
    .bind(scope_subscriber_id)
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
    let (scope_kind, scope_subscriber_id) = scope.to_columns();
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

/// Loads every event *in `scope`* within one source group `(source, group_key)`, ordered by
/// `(event_time, id)` so the pure `rollup` is deterministic. The build's drain-read over the event
/// log — called once per dirty group, by both the public and private build. The
/// `scope_subscriber_id IS NOT DISTINCT FROM` clause is the isolation boundary: a private read can
/// only see its owner's events, a public read only the shared ones.
pub async fn list_group_events(
    executor: impl PgExecutor<'_>,
    scope: &Scope,
    source: SourceKind,
    group_key: &str,
) -> Result<Vec<Event>, sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = scope.to_columns();
    sqlx::query(
        "SELECT id, fingerprint, source, scope_kind, scope_subscriber_id,
                event_time, title, body, links, group_key, entities,
                content_kind, severity_hint, ingest_time, raw
         FROM event
         WHERE scope_kind = $1 AND scope_subscriber_id IS NOT DISTINCT FROM $2
           AND source = $3 AND group_key = $4
         ORDER BY event_time, id",
    )
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(source)
    .bind(group_key)
    .try_map(from_row)
    .fetch_all(executor)
    .await
}

// ── Private build (per subscriber, watermark-bounded) ─────────────────────

/// A 64-bit seed for the per-subscriber PrivateBuild advisory lock — combined with the subscriber
/// id via `hashtextextended` so each subscriber's build serializes independently (mirrors the
/// public build's single `BUILD_LOCK_KEY`). Auto-released at transaction end.
const PRIVATE_BUILD_LOCK_SEED: i64 = 0x6275_6c6c_6574_6e02; // "bulletn\x02"

/// Tries to take the PrivateBuild advisory lock for `subscriber_id` on `executor`'s transaction.
/// Returns `false` if another build for the same subscriber holds it — the caller then no-ops (its
/// events are covered by the holder), exactly like the public build.
pub async fn try_private_build_lock(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let row =
        sqlx::query("SELECT pg_try_advisory_xact_lock(hashtextextended($1::text, $2)) AS locked")
            .bind(subscriber_id)
            .bind(PRIVATE_BUILD_LOCK_SEED)
            .fetch_one(executor)
            .await?;
    Ok(row.get("locked"))
}

/// Reads `(built_through, now())` for one subscriber's private build. A missing watermark row is the
/// epoch, so the first build covers all of the subscriber's private history; thereafter the
/// half-open range `(built_through, hwm]` bounds the work (mirrors `build_bounds`).
pub async fn private_build_bounds(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
) -> Result<(DateTime<Utc>, DateTime<Utc>), sqlx::Error> {
    let row = sqlx::query(
        "SELECT coalesce(
                  (SELECT built_through FROM private_build_watermark WHERE subscriber_id = $1),
                  'epoch'::timestamptz
                ) AS built_through,
                now() AS hwm",
    )
    .bind(subscriber_id)
    .fetch_one(executor)
    .await?;
    Ok((row.get("built_through"), row.get("hwm")))
}

/// Advances one subscriber's private build watermark to `hwm` (monotonic via GREATEST), creating the
/// row on first build.
pub async fn advance_private_build_watermark(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    hwm: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO private_build_watermark (subscriber_id, built_through)
         VALUES ($1, $2)
         ON CONFLICT (subscriber_id) DO UPDATE
            SET built_through = GREATEST(private_build_watermark.built_through, EXCLUDED.built_through)",
    )
    .bind(subscriber_id)
    .bind(hwm)
    .execute(executor)
    .await?;
    Ok(())
}
