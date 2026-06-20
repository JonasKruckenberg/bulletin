//! The persistence seam for the Phase-2 enrichment sweep: read the frontier of pending public events
//! and write grounded entity tokens back onto an event before it is clustered. Both run on the
//! no-subscriber (public) RLS context the sweep opens; the `event_update` policy
//! (migration 20200101000035) authorizes the write.

use crate::common::event::{from_row, Event};
use sqlx::PgExecutor;
use uuid::Uuid;

/// The `event` column list in [`from_row`] order — mirrors the ingest/cluster stores' projection
/// (incl. the Phase-1 `connection_id`/`full_text` columns `from_row` reads).
const EVENT_COLUMNS: &str = "id, fingerprint, source, scope_kind, scope_subscriber_id, \
     event_time, title, body, links, group_key, entities, \
     content_kind, severity_hint, ingest_time, raw, connection_id, full_text";

/// The pending frontier: public events not yet enriched **and** not yet built (still ahead of the
/// build watermark, so a write still feeds clustering). Ordered oldest-first and capped at `limit` so
/// a backlog drains over several sweeps and the oldest — closest to its grace deadline — go first.
///
/// Excluding already-built events (`ingest_time <= built_through`) keeps the sweep from re-enriching
/// an event whose cluster has already rolled up its entities (a late write would not re-dirty it),
/// and bounds the work to the live window rather than lifetime history.
pub async fn pending_public_events(
    executor: impl PgExecutor<'_>,
    limit: i64,
) -> Result<Vec<Event>, sqlx::Error> {
    sqlx::query(&format!(
        "SELECT {EVENT_COLUMNS}
         FROM event
         WHERE scope_kind = 'public'
           AND enriched_at IS NULL
           AND ingest_time > (SELECT built_through FROM build_watermark)
         ORDER BY ingest_time
         LIMIT $1"
    ))
    .bind(limit)
    .try_map(from_row)
    .fetch_all(executor)
    .await
}

/// Union `new_entities` onto one event's `entities` (kept sorted + de-duplicated, the cluster rollup's
/// invariant) and stamp `enriched_at = now()`. Idempotent: re-running unions the same tokens back to
/// the same set. Marking `enriched_at` even when `new_entities` is empty is deliberate — a clean pass
/// that grounded nothing must not be retried forever.
pub async fn apply_enrichment(
    executor: impl PgExecutor<'_>,
    event_id: Uuid,
    new_entities: &[String],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE event
         SET entities = ARRAY(
                 SELECT DISTINCT e
                 FROM unnest(entities || $2::text[]) AS e
                 ORDER BY e
             ),
             enriched_at = now()
         WHERE id = $1",
    )
    .bind(event_id)
    .bind(new_entities)
    .execute(executor)
    .await?;
    Ok(())
}
