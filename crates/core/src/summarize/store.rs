//! The summarization store contract (Phase A): the work-queue read and the summary write on the
//! `cluster` cache. Durable state is the events; `cluster.summary` is a rebuildable cache this
//! advances — exactly like the rollup columns beside it. Gated with the rest of the model edge.

use chrono::Utc;
use serde_json::Value;
use sqlx::{postgres::PgRow, PgConnection, Row};
use uuid::Uuid;

use crate::common::kind::SourceKind;
use crate::common::scope::Scope;
use crate::summarize::ClusterSummary;

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
