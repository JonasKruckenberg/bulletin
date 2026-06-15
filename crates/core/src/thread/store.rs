//! The persistence seam for `thread_maintenance`: read the co-occurrence sources (the subscriber's
//! recent **stories** + their entities) and the prior thread set, write back thread rows (with
//! id-forwarding / `merged_into`) and the projected `entity_weight` map, and advance the
//! per-subscriber maintenance watermark. Every read/write is fenced to the subscriber, exactly like
//! the story/cluster stores (design §7).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use uuid::Uuid;

use crate::common::db::{begin_scope, ScopeCtx};
use crate::common::watermark;
use crate::identity::{CanonicalId, ConfidenceBand};
use crate::thread::{ExistingThread, ThreadOrigin, ThreadState};

/// One co-occurrence source: a story's resolved entity spine, the distinct sources it spans, and its
/// recency. Fed to the pure [`super::co_occurrence`] builder (after remapping entities through
/// identity resolution).
pub struct StorySource {
    pub entities: Vec<CanonicalId>,
    /// Distinct source kinds (as text) across the story's member clusters — for `source_diversity`.
    pub sources: Vec<String>,
    pub last_event_time: DateTime<Utc>,
}

/// The engaged co-occurrence sources for `subscriber_id` over a rolling window from `window_start`
/// (design §5.1 step 2): the subscriber's live stories with activity in the window, each carrying the
/// union of its member clusters' `entities` and the distinct sources it spans. A story already fuses
/// `public ∪ own-private` clusters across sources, so this is the right cross-source unit (and is
/// per-subscriber by construction — no cross-tenant read).
pub async fn co_occurrence_sources(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    window_start: DateTime<Utc>,
) -> Result<Vec<StorySource>, sqlx::Error> {
    sqlx::query(
        "SELECT s.last_event_time,
                array_remove(array_agg(DISTINCT ent), NULL) AS entities,
                array_agg(DISTINCT c.source)                AS sources
         FROM story s
         CROSS JOIN LATERAL jsonb_array_elements(s.clusters) AS m
         JOIN cluster c ON c.id = (m.value ->> 'cluster_id')::uuid
         LEFT JOIN LATERAL unnest(c.entities) AS ent ON true
         WHERE s.subscriber_id = $1 AND s.merged_into IS NULL
           AND s.last_event_time >= $2
         GROUP BY s.id, s.last_event_time
         ORDER BY s.last_event_time",
    )
    .bind(subscriber_id)
    .bind(window_start)
    .try_map(|row: PgRow| {
        Ok(StorySource {
            entities: row.get("entities"),
            sources: row.get("sources"),
            last_event_time: row.get("last_event_time"),
        })
    })
    .fetch_all(executor)
    .await
}

/// A prior thread row, with the fields maintenance needs to id-forward, decay, and re-score it.
pub struct ThreadRow {
    pub id: Uuid,
    pub origin: ThreadOrigin,
    pub pinned: bool,
    pub affinity: f32,
    pub entities: Vec<CanonicalId>,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_story_time: Option<DateTime<Utc>>,
}

impl ThreadRow {
    /// The identity-relevant projection the pure id-forwarding matcher needs.
    pub fn as_existing(&self) -> ExistingThread {
        ExistingThread {
            id: self.id,
            entities: self.entities.clone(),
            pinned: self.pinned,
        }
    }
}

/// Load `subscriber_id`'s live threads (`merged_into IS NULL`, archived included for reactivation):
/// both the prior set the new communities id-forward onto and the rows whose affinity decays.
pub async fn load_threads(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
) -> Result<Vec<ThreadRow>, sqlx::Error> {
    sqlx::query(
        "SELECT id, origin, pinned, affinity, entities, first_seen, last_story_time
         FROM thread
         WHERE subscriber_id = $1 AND merged_into IS NULL",
    )
    .bind(subscriber_id)
    .try_map(|row: PgRow| {
        Ok(ThreadRow {
            id: row.get("id"),
            origin: ThreadOrigin::parse(&row.get::<String, _>("origin")),
            pinned: row.get("pinned"),
            affinity: row.get("affinity"),
            entities: row.get("entities"),
            first_seen: row.get("first_seen"),
            last_story_time: row.get("last_story_time"),
        })
    })
    .fetch_all(executor)
    .await
}

/// The fully-computed state of one thread to persist this pass. `id` is `Some` for an existing thread
/// (keep/merge winner), `None` to mint a new emergent thread; `absorb` lists threads merging *into*
/// this one (each gets `merged_into = this.id`).
pub struct ThreadUpsert {
    pub id: Option<Uuid>,
    pub origin: ThreadOrigin,
    pub pinned: bool,
    pub entities: Vec<CanonicalId>,
    pub affinity: f32,
    pub state: ThreadState,
    pub confidence: ConfidenceBand,
    pub story_count: i32,
    pub source_diversity: i32,
    pub baseline_rate: f32,
    pub first_seen: Option<DateTime<Utc>>,
    pub last_story_time: Option<DateTime<Utc>>,
    pub absorb: Vec<Uuid>,
}

/// Persist the pass's thread set: upsert each thread (insert when `id` is `None`, update otherwise)
/// and forward every absorbed thread's `merged_into` to its winner. Runs on the caller's scoped
/// connection (the whole maintenance pass is one Subscriber-scoped transaction), so a crash can't
/// leave a half-forwarded merge.
pub async fn save_threads(
    conn: &mut sqlx::PgConnection,
    subscriber_id: Uuid,
    upserts: &[ThreadUpsert],
) -> Result<(), sqlx::Error> {
    for u in upserts {
        let id = match u.id {
            Some(id) => {
                sqlx::query(
                    "UPDATE thread SET
                        entities = $2, affinity = $3, state = $4, story_count = $5,
                        source_diversity = $6, baseline_rate = $7, last_story_time = $8,
                        confidence = $10
                     WHERE id = $1 AND subscriber_id = $9",
                )
                .bind(id)
                .bind(&u.entities)
                .bind(u.affinity)
                .bind(u.state.as_str())
                .bind(u.story_count)
                .bind(u.source_diversity)
                .bind(u.baseline_rate)
                .bind(u.last_story_time)
                .bind(subscriber_id)
                .bind(u.confidence.as_str())
                .execute(&mut *conn)
                .await?;
                id
            }
            None => sqlx::query(
                "INSERT INTO thread
                    (subscriber_id, origin, pinned, entities, affinity, state, confidence,
                     story_count, source_diversity, baseline_rate, first_seen, last_story_time)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                 RETURNING id",
            )
            .bind(subscriber_id)
            .bind(u.origin.as_str())
            .bind(u.pinned)
            .bind(&u.entities)
            .bind(u.affinity)
            .bind(u.state.as_str())
            .bind(u.confidence.as_str())
            .bind(u.story_count)
            .bind(u.source_diversity)
            .bind(u.baseline_rate)
            .bind(u.first_seen)
            .bind(u.last_story_time)
            .fetch_one(&mut *conn)
            .await?
            .get("id"),
        };
        if !u.absorb.is_empty() {
            sqlx::query(
                "UPDATE thread SET merged_into = $2 WHERE id = ANY($1) AND subscriber_id = $3",
            )
            .bind(&u.absorb)
            .bind(id)
            .bind(subscriber_id)
            .execute(&mut *conn)
            .await?;
        }
    }
    Ok(())
}

/// Overwrite the subscriber's projected `entity_weight` map (`subscriber.affinity` jsonb) — the
/// fire-time relevance input, of which `thread_maintenance` is the sole writer.
pub async fn save_entity_weights(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    weights: &BTreeMap<CanonicalId, f32>,
) -> Result<(), sqlx::Error> {
    let json = serde_json::to_value(weights).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
    sqlx::query("UPDATE subscriber SET affinity = $2 WHERE id = $1")
        .bind(subscriber_id)
        .bind(json)
        .execute(executor)
        .await?;
    Ok(())
}

/// Read the subscriber's `entity_weight` map — the fire-time relevance input. An empty map (the
/// default, or no maintenance run yet) means no thread weighting, so the layer stays inert until a
/// pass has run.
pub async fn load_entity_weights(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
) -> Result<BTreeMap<CanonicalId, f32>, sqlx::Error> {
    let json: serde_json::Value = sqlx::query("SELECT affinity FROM subscriber WHERE id = $1")
        .bind(subscriber_id)
        .fetch_optional(executor)
        .await?
        .map(|row| row.get("affinity"))
        .unwrap_or_else(|| serde_json::json!({}));
    Ok(serde_json::from_value(json).unwrap_or_default())
}

/// The incremental feedback cursor for the subscriber's maintenance (a missing row reads as the
/// epoch, so the first pass folds all prior care feedback in once).
pub async fn feedback_cursor(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
) -> Result<DateTime<Utc>, sqlx::Error> {
    watermark::read_through(executor, "thread_maintenance_watermark", subscriber_id).await
}

/// Advance the subscriber's maintenance watermark to `through` (monotonic), stamping `ran_at = now()`
/// (the due-query clock).
pub async fn advance_watermark(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    through: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    watermark::advance(
        executor,
        "thread_maintenance_watermark",
        subscriber_id,
        through,
        true,
    )
    .await
}

/// Subscribers due for a maintenance pass: those whose last run is older than `cadence` (or who have
/// never run). Mirrors `due_subscribers` — the tick enqueues only this small due set, not a full
/// subscriber scan every minute.
pub async fn due_for_maintenance(
    pool: &PgPool,
    cadence: chrono::Duration,
) -> Result<Vec<Uuid>, sqlx::Error> {
    let cadence_secs = cadence.num_seconds().max(1);
    // Enumerates subscribers + their watermarks across owners → the admin (control-plane) context,
    // like `due_subscribers`. Self-scoped so the tick needs no scope ceremony.
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let due = sqlx::query(
        "SELECT s.id
         FROM subscriber s
         LEFT JOIN thread_maintenance_watermark w ON w.subscriber_id = s.id
         WHERE coalesce(w.ran_at, 'epoch'::timestamptz) <= now() - make_interval(secs => $1)",
    )
    .bind(cadence_secs as f64)
    .try_map(|row: PgRow| Ok(row.get("id")))
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(due)
}

/// Find the best thread for a freshly-selected story at fire time (design §5.2 thread-assign): among
/// the subscriber's active/dormant threads sharing ≥ `min_overlap` entities with `entities`, pick the
/// one maximizing `overlap × affinity` (ties → highest affinity, then oldest id). `None` if nothing
/// overlaps. The GIN `thread_entities` index narrows to the few candidate threads.
pub async fn assign_thread(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    entities: &[CanonicalId],
    min_overlap: i64,
) -> Result<Option<Uuid>, sqlx::Error> {
    if entities.is_empty() {
        return Ok(None);
    }
    let row = sqlx::query(
        "SELECT id FROM (
            SELECT t.id,
                   (SELECT count(*) FROM unnest(t.entities) AS e
                     WHERE e = ANY($2)) AS overlap,
                   t.affinity
            FROM thread t
            WHERE t.subscriber_id = $1
              AND t.merged_into IS NULL
              AND t.state <> 'archived'
              AND t.entities && $2
         ) cand
         WHERE overlap >= $3
         ORDER BY overlap::real * affinity DESC, affinity DESC, id
         LIMIT 1",
    )
    .bind(subscriber_id)
    .bind(entities)
    .bind(min_overlap)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|row| row.get("id")))
}

/// Stamp the assigned `thread_id` onto each selected digest item (design §5.2). A `None` assignment
/// leaves the column null. One `UPDATE` per item over the small selected set; takes a `&mut
/// PgConnection` so the caller can run it inside the digest's RLS-scoped transaction.
pub async fn assign_thread_ids(
    conn: &mut sqlx::PgConnection,
    digest_id: Uuid,
    assignments: &[(Uuid, Option<Uuid>)],
) -> Result<(), sqlx::Error> {
    for (story_id, thread_id) in assignments {
        if let Some(thread_id) = thread_id {
            sqlx::query(
                "UPDATE digest_item SET thread_id = $3 WHERE digest_id = $1 AND story_id = $2",
            )
            .bind(digest_id)
            .bind(story_id)
            .bind(thread_id)
            .execute(&mut *conn)
            .await?;
        }
    }
    Ok(())
}
