//! The persistence seam for the Phase-2 enrichment sweep: read the frontier of pending public events
//! and write grounded entity tokens back onto an event before it is clustered. Both run on the
//! no-subscriber (public) RLS context the sweep opens; the `event_update` policy
//! (migration 20200101000035) authorizes the write.

use crate::common::event::{from_row, Event};
use crate::common::kind::SourceKind;
use sqlx::PgExecutor;
use uuid::Uuid;

/// The `event` column list in [`from_row`] order — mirrors the ingest/cluster stores' projection
/// (incl. the Phase-1 `connection_id`/`full_text` columns `from_row` reads).
const EVENT_COLUMNS: &str = "id, fingerprint, source, scope_kind, scope_subscriber_id, \
     event_time, title, body, links, group_key, entities, \
     content_kind, severity_hint, ingest_time, raw, connection_id, full_text";

/// The pending frontier: public events from an *enrichable* (link-poor) source, not yet enriched
/// **and** not yet built (still ahead of the build watermark, so a write still feeds clustering).
/// Ordered oldest-first and capped at `limit` so a backlog drains over several sweeps and the oldest —
/// closest to its grace deadline — go first.
///
/// Excluding already-built events (`ingest_time <= built_through`) keeps the sweep from re-enriching
/// an event whose cluster has already rolled up its entities (a late write would not re-dirty it),
/// and bounds the work to the live window rather than lifetime history. The `source` filter
/// ([`SourceKind::enrichable_sources`]) skips the structurally-rich sources (GitHub/Slack), whose
/// events already carry clean `repo:`/`user:` entities — enriching them only mines title noise (the
/// repo owner as a bogus `org:`) that over-links unrelated repos.
pub async fn pending_public_events(
    executor: impl PgExecutor<'_>,
    limit: i64,
) -> Result<Vec<Event>, sqlx::Error> {
    sqlx::query(&format!(
        "SELECT {EVENT_COLUMNS}
         FROM event
         WHERE scope_kind = 'public'
           AND enriched_at IS NULL
           AND source = ANY($2)
           AND ingest_time > (SELECT built_through FROM build_watermark)
         ORDER BY ingest_time
         LIMIT $1"
    ))
    .bind(limit)
    .bind(SourceKind::enrichable_sources())
    .try_map(from_row)
    .fetch_all(executor)
    .await
}

/// Union `new_entities` onto one event's `entities` (kept sorted + de-duplicated, the cluster rollup's
/// invariant), write the classified `salience` as the event's `severity_hint` (the priority-boost the
/// cluster rollup maxes onto `cluster.max_severity`), and stamp `enriched_at = now()`. Salience is
/// written as `GREATEST` of any existing hint so a structural hint is never lowered, and only when
/// positive — a `routine` (0) classification leaves the hint `NULL`, exactly as an un-enriched event.
/// Idempotent: re-running unions the same tokens and re-asserts the same `GREATEST`. Marking
/// `enriched_at` even when nothing grounded is deliberate — a clean pass must not be retried forever.
pub async fn apply_enrichment(
    executor: impl PgExecutor<'_>,
    event_id: Uuid,
    new_entities: &[String],
    salience: i16,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE event
         SET entities = ARRAY(
                 SELECT DISTINCT e
                 FROM unnest(entities || $2::text[]) AS e
                 ORDER BY e
             ),
             severity_hint = CASE
                 WHEN $3::smallint > 0
                     THEN GREATEST(COALESCE(severity_hint, 0), $3::smallint)
                 ELSE severity_hint
             END,
             enriched_at = now()
         WHERE id = $1",
    )
    .bind(event_id)
    .bind(new_entities)
    .bind(salience)
    .execute(executor)
    .await?;
    Ok(())
}
