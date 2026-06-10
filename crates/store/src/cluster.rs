use bulletin_core::{
    cluster::{Cluster, ClusterRollup},
    id::Id,
    kind::SourceKind,
    select::Candidate,
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
    .try_map(|row: PgRow| {
        let source: String = row.get("source");
        let source = SourceKind::try_from(source.as_str())
            .map_err(|_| sqlx::Error::Decode(format!("unknown source kind: {source}").into()))?;
        Ok((source, row.get::<String, _>("group_key")))
    })
    .fetch_all(executor)
    .await
}

/// Upserts the recomputed rollup for one group. Idempotent: re-running a build overwrites the
/// cache in place (durable state is the events, not this row).
pub async fn upsert_cluster(
    executor: impl PgExecutor<'_>,
    source: SourceKind,
    group_key: &str,
    r: &ClusterRollup,
) -> Result<Id<Cluster>, sqlx::Error> {
    let row = sqlx::query(
        "INSERT INTO cluster (source, group_key, title, link, last_event_time, updated_at)
         VALUES ($1, $2, $3, $4, $5, now())
         ON CONFLICT ON CONSTRAINT cluster_identity DO UPDATE SET
            title = EXCLUDED.title,
            link = EXCLUDED.link,
            last_event_time = EXCLUDED.last_event_time,
            updated_at = now()
         RETURNING id",
    )
    .bind(source.as_str())
    .bind(group_key)
    .bind(&r.title)
    .bind(r.link.as_deref())
    .bind(r.last_event_time)
    .fetch_one(executor)
    .await?;
    Ok(Id::new(row.get("id")))
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

/// Human-readable identity (source, title) of a cluster — the display half of a selection
/// `Decision`, which carries only the id.
pub struct ClusterDisplay {
    pub id: Uuid,
    pub source: SourceKind,
    pub title: String,
}

/// Display fields for a set of clusters by id, for `debug digest-explain` to pair each
/// selection verdict with a human-readable cluster. Order is unspecified — callers index by id.
pub async fn cluster_display(
    executor: impl PgExecutor<'_>,
    ids: &[Uuid],
) -> Result<Vec<ClusterDisplay>, sqlx::Error> {
    sqlx::query("SELECT id, source, title FROM cluster WHERE id = ANY($1)")
        .bind(ids)
        .try_map(|row: PgRow| {
            let source: String = row.get("source");
            let source = SourceKind::try_from(source.as_str()).map_err(|_| {
                sqlx::Error::Decode(format!("unknown source kind: {source}").into())
            })?;
            Ok(ClusterDisplay {
                id: row.get("id"),
                source,
                title: row.get("title"),
            })
        })
        .fetch_all(executor)
        .await
}

/// Clusters that received a new event (by ingest_time) in `(last_run, window_end]` — the
/// digest's candidate set. Carries the M1 relevance stub (1.0). Ordered newest-first; the pure
/// `select()` applies the floor and cap. `last_run = None` ⇒ unbounded lower edge.
pub async fn candidates_in_window(
    executor: impl PgExecutor<'_>,
    last_run: Option<DateTime<Utc>>,
    window_end: DateTime<Utc>,
) -> Result<Vec<Candidate>, sqlx::Error> {
    sqlx::query(
        "SELECT c.id, c.last_event_time
         FROM cluster c
         WHERE EXISTS (
             SELECT 1 FROM event e
             WHERE e.scope_kind = 'public'
               AND e.source = c.source
               AND e.group_key = c.group_key
               AND e.ingest_time > COALESCE($1, '-infinity'::timestamptz)
               AND e.ingest_time <= $2
         )
         ORDER BY c.last_event_time DESC",
    )
    .bind(last_run)
    .bind(window_end)
    .try_map(|row: PgRow| {
        Ok(Candidate {
            cluster_id: Id::new(row.get("id")),
            last_event_time: row.get("last_event_time"),
            relevance: 1.0, // M1 stub — M4 fills this in
        })
    })
    .fetch_all(executor)
    .await
}
