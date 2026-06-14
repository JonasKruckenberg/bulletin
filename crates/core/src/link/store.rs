//! Story persistence: read the prior assignment (for stable-id forwarding) and write the recomputed
//! one. Stories are a **per-subscriber recomputed cache** (design §5.3), so this is an upsert of the
//! current components plus the retro-merge tombstones — durable truth stays the events/clusters.
//!
//! Every read and write is fenced by `subscriber_id`: a story is always Private-scoped to its owner
//! (design §4), so this can only touch the caller's own stories — Phase 4's RLS makes that a
//! DB-enforced guarantee rather than a query convention, exactly like the cluster store.

use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use uuid::Uuid;

use crate::link::{Assignment, LinkCluster, PriorMember};

/// The subscriber's candidate clusters for linking: their scope (`public ∪ own-private`) within the
/// freshness floor, carrying the blocking substrate (`entities`) and recency span. The pre-M3 digest
/// selected over exactly this predicate (clusters directly) — the same isolation boundary
/// (`scope_kind = 'public' OR scope_subscriber_id = $1`) and the same `updated_at` floor — so linking
/// sees exactly the clusters the digest would have considered pre-M3. Ordered by id for a
/// deterministic linking input.
pub async fn candidate_clusters(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    last_run: Option<DateTime<Utc>>,
    horizon_days: i32,
) -> Result<Vec<LinkCluster>, sqlx::Error> {
    sqlx::query(
        "SELECT id, entities, first_event_time, last_event_time
         FROM cluster
         WHERE (scope_kind = 'public' OR scope_subscriber_id = $1)
           AND updated_at >= LEAST($2, now() - make_interval(days => $3))
         ORDER BY id",
    )
    .bind(subscriber_id)
    .bind(last_run)
    .bind(horizon_days)
    .try_map(|row: PgRow| {
        Ok(LinkCluster {
            id: row.get("id"),
            entities: row.get("entities"),
            first_event_time: row.get("first_event_time"),
            last_event_time: row.get("last_event_time"),
        })
    })
    .fetch_all(executor)
    .await
}

/// The subscriber's *prior* story assignment: one [`PriorMember`] per (live) story member, the input
/// the recompute forwards stable ids from. Walks each live story's `clusters` jsonb to its member
/// cluster ids; `delivered` reflects whether the story was ever in a delivered digest
/// (`last_delivered_at` set) — the gate on the asymmetric-merge rule.
pub async fn load_prior_members(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
) -> Result<Vec<PriorMember>, sqlx::Error> {
    sqlx::query(
        "SELECT s.id AS story_id,
                (s.last_delivered_at IS NOT NULL) AS delivered,
                (c->>'cluster_id')::uuid AS cluster_id
         FROM story s
         CROSS JOIN LATERAL jsonb_array_elements(s.clusters) AS c
         WHERE s.subscriber_id = $1 AND s.merged_into IS NULL",
    )
    .bind(subscriber_id)
    .try_map(|row: PgRow| {
        Ok(PriorMember {
            cluster_id: row.get("cluster_id"),
            story_id: row.get("story_id"),
            delivered: row.get("delivered"),
        })
    })
    .fetch_all(executor)
    .await
}

/// Persist a recompute: upsert the current stories and record the retro-merges, in one transaction so
/// the assignment is never observed half-written. A surviving story's row is rewritten in place
/// (preserving its `created_at`/`last_delivered_at`); a retro-merge loser is tombstoned
/// (`merged_into = survivor`, `clusters = []`) so a stale deep-link redirects (design §8.2).
pub async fn persist_assignment(
    pool: &PgPool,
    subscriber_id: Uuid,
    assignment: &Assignment,
) -> Result<(), sqlx::Error> {
    // Writes only this subscriber's own stories → its RLS context (story is fail-closed otherwise).
    let mut tx = crate::common::db::begin_scope(
        pool,
        crate::common::db::ScopeCtx::Subscriber(subscriber_id),
    )
    .await?;

    for story in &assignment.stories {
        let clusters =
            serde_json::to_value(&story.clusters).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
        sqlx::query(
            "INSERT INTO story
                (id, subscriber_id, clusters, first_event_time, last_event_time)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (id) DO UPDATE SET
                clusters = EXCLUDED.clusters,
                first_event_time = EXCLUDED.first_event_time,
                last_event_time = EXCLUDED.last_event_time,
                merged_into = NULL,
                updated_at = now()",
        )
        .bind(story.id)
        .bind(subscriber_id)
        .bind(clusters)
        .bind(story.first_event_time)
        .bind(story.last_event_time)
        .execute(&mut *tx)
        .await?;
    }

    for merge in &assignment.merges {
        sqlx::query(
            "UPDATE story
             SET merged_into = $2, clusters = '[]'::jsonb, updated_at = now()
             WHERE id = $1 AND subscriber_id = $3",
        )
        .bind(merge.loser)
        .bind(merge.survivor)
        .bind(subscriber_id)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await
}

/// Marks every story carried by a delivered digest as delivered (`last_delivered_at`). Called inside
/// the deliver transaction so the asymmetric-merge gate (only a strong edge merges two delivered
/// stories) reflects exactly what the subscriber has seen.
pub async fn mark_stories_delivered(
    executor: impl PgExecutor<'_>,
    digest_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE story SET last_delivered_at = now()
         WHERE id IN (SELECT story_id FROM digest_item WHERE digest_id = $1)",
    )
    .bind(digest_id)
    .execute(executor)
    .await?;
    Ok(())
}
