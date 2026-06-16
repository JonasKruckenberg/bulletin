//! The summarization store contract (Phase A): the work-queue read and the summary write on the
//! `cluster` cache. Durable state is the events; `cluster.summary` is a rebuildable cache this
//! advances — exactly like the rollup columns beside it. Gated with the rest of the model edge.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{postgres::PgRow, PgConnection, Row};
use uuid::Uuid;

use crate::common::kind::SourceKind;
use crate::common::scope::Scope;
use crate::summarize::{ClusterSummary, ThreadSummary};

/// A cluster the sweep should consider (re)summarizing: its scope-aware identity (so the events can be
/// re-read) plus the stored `summary_hash` for the exact staleness re-check.
pub(crate) struct DueCluster {
    pub id: Uuid,
    pub source: SourceKind,
    pub group_key: String,
    pub title: String,
    pub summary_hash: Option<Vec<u8>>,
}

/// Clusters whose content changed since (or were never) summarized, in the given `scope` — the
/// summarizer work queue (§2.1). The cheap SQL gate is `summarized_at IS NULL` (the partial index)
/// **or** `updated_at > summarized_at` (the build bumps `updated_at` on every recompute) **or** a
/// model/prompt upgrade (`summary_model <> $current`). The exact `summary_hash` re-check in the sweep
/// then drops the rare case where `updated_at` moved but the content didn't. Newest-first, bounded by
/// `limit` so one pass drains a slice of a large backlog. Reads `cluster` (RLS-protected) → the caller
/// runs it in the matching scope context.
pub(crate) async fn clusters_needing_summary(
    conn: &mut PgConnection,
    scope: &Scope,
    current_model: &str,
    limit: i64,
) -> Result<Vec<DueCluster>, sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = scope.to_columns();
    sqlx::query(
        "SELECT id, source, group_key, title, summary_hash
         FROM cluster
         WHERE scope_kind = $1 AND scope_subscriber_id IS NOT DISTINCT FROM $2
           AND ( summarized_at IS NULL
                 OR updated_at > summarized_at
                 OR summary_model IS DISTINCT FROM $3 )
         ORDER BY last_event_time DESC
         LIMIT $4",
    )
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(current_model)
    .bind(limit)
    .try_map(|row: PgRow| {
        Ok(DueCluster {
            id: row.get("id"),
            source: row.try_get("source")?,
            group_key: row.get("group_key"),
            title: row.get("title"),
            summary_hash: row.get("summary_hash"),
        })
    })
    .fetch_all(conn)
    .await
}

/// Write a freshly generated (or baseline) summary onto the cluster, stamping the content signature,
/// model/prompt provenance, and timestamp. Idempotent — re-running with the same content overwrites
/// in place. Runs in the cluster's scope context (public sweep: no-subscriber; private: the owner).
pub(crate) async fn store_summary(
    conn: &mut PgConnection,
    cluster_id: Uuid,
    summary: &ClusterSummary,
    hash: &[u8],
    model: &str,
) -> Result<(), sqlx::Error> {
    let json: Value =
        serde_json::to_value(summary).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
    sqlx::query(
        "UPDATE cluster
         SET summary = $2, summary_hash = $3, summary_model = $4, summarized_at = now()
         WHERE id = $1",
    )
    .bind(cluster_id)
    .bind(json)
    .bind(hash)
    .bind(model)
    .execute(conn)
    .await?;
    Ok(())
}

/// Advance only the `summarized_at` watermark for a cluster whose content was unchanged (the cache
/// hit) — so the cheap `updated_at > summarized_at` gate stops re-flagging it every sweep, without a
/// pointless model call. Leaves the existing summary/hash/model untouched.
pub(crate) async fn touch_summarized(
    conn: &mut PgConnection,
    cluster_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE cluster SET summarized_at = $2 WHERE id = $1")
        .bind(cluster_id)
        .bind(Utc::now())
        .execute(conn)
        .await?;
    Ok(())
}

// ── Phase C — story cross-source synthesis (§2.2) ────────────────────────────────────────────────

/// A story the synthesis pass should consider (re)synthesizing: its id plus the member cluster ids
/// (so their summaries can be re-read) and the stored `summary_sig` for the exact staleness re-check.
pub(crate) struct DueStory {
    pub id: Uuid,
    pub cluster_ids: Vec<Uuid>,
    pub summary_sig: Option<Vec<u8>>,
    pub summary_model: Option<String>,
}

/// The subscriber's live stories whose membership/content changed since (or were never) synthesized —
/// the Phase-C work queue (§2.2). The cheap SQL gate mirrors the cluster sweep: `summarized_at IS
/// NULL` **or** `updated_at > summarized_at` (the per-fire recompute bumps `updated_at`) **or** a
/// model/prompt upgrade; the exact `summary_sig` re-check in the sweep then drops the rare false
/// positive. Newest-first, bounded by `limit`. Runs in the subscriber's RLS context (story/cluster
/// fenced to owner ∪ public).
pub(crate) async fn stories_needing_summary(
    conn: &mut PgConnection,
    subscriber_id: Uuid,
    current_model: &str,
    limit: i64,
) -> Result<Vec<DueStory>, sqlx::Error> {
    sqlx::query(
        "SELECT s.id,
                array_agg((m.value ->> 'cluster_id')::uuid) AS cluster_ids,
                s.summary_sig, s.summary_model
         FROM story s
         CROSS JOIN LATERAL jsonb_array_elements(s.clusters) AS m
         WHERE s.subscriber_id = $1 AND s.merged_into IS NULL
           AND ( s.summarized_at IS NULL
                 OR s.updated_at > s.summarized_at
                 OR s.summary_model IS DISTINCT FROM $2 )
         GROUP BY s.id, s.last_event_time, s.summary_sig, s.summary_model
         ORDER BY s.last_event_time DESC
         LIMIT $3",
    )
    .bind(subscriber_id)
    .bind(current_model)
    .bind(limit)
    .try_map(|row: PgRow| {
        Ok(DueStory {
            id: row.get("id"),
            cluster_ids: row.get("cluster_ids"),
            summary_sig: row.get("summary_sig"),
            summary_model: row.get("summary_model"),
        })
    })
    .fetch_all(conn)
    .await
}

/// One member cluster of a story, as the synthesis needs it: its precomputed [`ClusterSummary`] and
/// its content `summary_hash` (the §2.2 member-signature input). Returned newest-first, so `[0]` is
/// the representative.
pub(crate) struct MemberSummary {
    pub summary: ClusterSummary,
    pub summary_hash: Option<Vec<u8>>,
}

/// Load the member clusters' summaries for a story (by cluster id), **newest-first** (so the caller's
/// `members[0]` is the representative). Decodes `cluster.summary` tolerantly to the inert default (a
/// never-summarized member contributes empty facts, not a failure). Reads `cluster` (RLS-protected) →
/// the subscriber's scope.
pub(crate) async fn load_member_summaries(
    conn: &mut PgConnection,
    cluster_ids: &[Uuid],
) -> Result<Vec<MemberSummary>, sqlx::Error> {
    sqlx::query(
        "SELECT summary, summary_hash
         FROM cluster WHERE id = ANY($1)
         ORDER BY last_event_time DESC, id DESC",
    )
    .bind(cluster_ids)
    .try_map(|row: PgRow| {
        let summary: ClusterSummary =
            serde_json::from_value(row.try_get("summary")?).unwrap_or_default();
        Ok(MemberSummary {
            summary,
            summary_hash: row.get("summary_hash"),
        })
    })
    .fetch_all(conn)
    .await
}

/// Write a freshly synthesized story summary, stamping the member signature + provenance + timestamp.
/// Idempotent — re-running with the same membership overwrites in place.
pub(crate) async fn store_story_summary(
    conn: &mut PgConnection,
    story_id: Uuid,
    summary: &ClusterSummary,
    sig: &[u8],
    model: &str,
) -> Result<(), sqlx::Error> {
    let json: Value =
        serde_json::to_value(summary).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
    sqlx::query(
        "UPDATE story
         SET summary = $2, summary_sig = $3, summary_model = $4, summarized_at = now()
         WHERE id = $1",
    )
    .bind(story_id)
    .bind(json)
    .bind(sig)
    .bind(model)
    .execute(conn)
    .await?;
    Ok(())
}

/// Advance only the `summarized_at` watermark for a story whose member signature was unchanged (the
/// cache hit) or that needs no synthesis (a singleton / all-unsummarized story) — so the cheap
/// `updated_at > summarized_at` gate stops re-flagging it, without a pointless model call.
pub(crate) async fn touch_story_summarized(
    conn: &mut PgConnection,
    story_id: Uuid,
    model: &str,
) -> Result<(), sqlx::Error> {
    // Stamp the model too, so a story we deliberately skip under the *current* model isn't re-flagged
    // by the `summary_model IS DISTINCT FROM` clause every pass.
    sqlx::query("UPDATE story SET summarized_at = now(), summary_model = $2 WHERE id = $1")
        .bind(story_id)
        .bind(model)
        .execute(conn)
        .await?;
    Ok(())
}

// ── Phase B — thread label + delta (§2.3) ────────────────────────────────────────────────────────

/// A thread the label/delta pass should consider (re)summarizing: its id, entity spine (the label
/// inputs + the recent-story match), the readable label already stored (skip a stable one), the prior
/// `delta_through` watermark, and provenance for the staleness gate.
pub(crate) struct DueThread {
    pub id: Uuid,
    pub entities: Vec<String>,
    pub summary: ThreadSummary,
    /// The prior delta flag — preserved when a pass finds no new stories (e.g. a model-only re-fire),
    /// so a still-valid delta isn't cleared.
    pub delta: Option<String>,
    pub delta_through: Option<DateTime<Utc>>,
    pub last_story_time: Option<DateTime<Utc>>,
    pub summary_model: Option<String>,
}

/// The subscriber's non-archived threads due for a label/delta pass: never summarized, a model/prompt
/// upgrade, or new stories since `delta_through` (so the delta is restated). Newest-active first,
/// bounded by `limit`. Runs in the subscriber's RLS context (thread fenced to owner).
pub(crate) async fn threads_needing_summary(
    conn: &mut PgConnection,
    subscriber_id: Uuid,
    current_model: &str,
    limit: i64,
) -> Result<Vec<DueThread>, sqlx::Error> {
    sqlx::query(
        "SELECT id, entities, summary, delta, delta_through, last_story_time, summary_model
         FROM thread
         WHERE subscriber_id = $1 AND merged_into IS NULL AND state <> 'archived'
           AND ( summarized_at IS NULL
                 OR summary_model IS DISTINCT FROM $2
                 OR last_story_time IS DISTINCT FROM delta_through )
         ORDER BY last_story_time DESC NULLS LAST
         LIMIT $3",
    )
    .bind(subscriber_id)
    .bind(current_model)
    .bind(limit)
    .try_map(|row: PgRow| {
        let summary: ThreadSummary =
            serde_json::from_value(row.try_get("summary")?).unwrap_or_default();
        Ok(DueThread {
            id: row.get("id"),
            entities: row.get("entities"),
            summary,
            delta: row.get("delta"),
            delta_through: row.get("delta_through"),
            last_story_time: row.get("last_story_time"),
            summary_model: row.get("summary_model"),
        })
    })
    .fetch_all(conn)
    .await
}

/// One recent story on a thread: its recency + the representative member cluster's headline (the LLM
/// label/delta inputs). Headline degrades to the raw cluster title when no cluster summary has run.
pub(crate) struct ThreadStory {
    pub last_event_time: DateTime<Utc>,
    pub headline: String,
}

/// The subscriber's recent stories overlapping a thread's entity spine, newest-first (bounded by
/// `limit`) — the headlines that feed the label and (filtered by `delta_through` in the sweep) the
/// delta. The representative headline is the story's latest member cluster's `summary.headline`,
/// degrading to its `title`. Reads `story`/`cluster` (RLS-protected) → the subscriber's scope.
pub(crate) async fn thread_recent_stories(
    conn: &mut PgConnection,
    subscriber_id: Uuid,
    entities: &[String],
    limit: i64,
) -> Result<Vec<ThreadStory>, sqlx::Error> {
    sqlx::query(
        "SELECT s.last_event_time,
                coalesce(nullif(rep.summary ->> 'headline', ''), rep.title) AS headline
         FROM story s
         CROSS JOIN LATERAL (
             SELECT c.title, c.summary
             FROM jsonb_array_elements(s.clusters) AS m
             JOIN cluster c ON c.id = (m.value ->> 'cluster_id')::uuid
             ORDER BY c.last_event_time DESC, c.id DESC
             LIMIT 1
         ) AS rep
         WHERE s.subscriber_id = $1 AND s.merged_into IS NULL
           AND EXISTS (
               SELECT 1
               FROM jsonb_array_elements(s.clusters) AS m2
               JOIN cluster c2 ON c2.id = (m2.value ->> 'cluster_id')::uuid
               WHERE c2.entities && $2
           )
         ORDER BY s.last_event_time DESC
         LIMIT $3",
    )
    .bind(subscriber_id)
    .bind(entities)
    .bind(limit)
    .try_map(|row: PgRow| {
        Ok(ThreadStory {
            last_event_time: row.get("last_event_time"),
            headline: row.get("headline"),
        })
    })
    .fetch_all(conn)
    .await
}

/// Write a thread's label/delta pass result: the readable label onto `thread.summary` (leaving the
/// deterministic `thread.label` as the baseline beneath it), the delta flag + its watermark, and the
/// model/timestamp provenance. `delta_through` is set to the thread's `last_story_time` (the watermark
/// the delta covers) — including `NULL` for a story-less thread, so the `last_story_time IS DISTINCT
/// FROM delta_through` due-gate clears (a non-null `now` would leave a `NULL`-story thread forever due).
pub(crate) async fn store_thread_summary(
    conn: &mut PgConnection,
    thread_id: Uuid,
    summary: &ThreadSummary,
    delta: Option<&str>,
    delta_through: Option<DateTime<Utc>>,
    model: &str,
) -> Result<(), sqlx::Error> {
    let json: Value =
        serde_json::to_value(summary).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
    sqlx::query(
        "UPDATE thread
         SET summary = $2, delta = $3, delta_through = $4, summary_model = $5, summarized_at = now()
         WHERE id = $1",
    )
    .bind(thread_id)
    .bind(json)
    .bind(delta)
    .bind(delta_through)
    .bind(model)
    .execute(conn)
    .await?;
    Ok(())
}
