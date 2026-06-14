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

/// The subscriber's candidate clusters for linking: their scope (`public ∪ own-private`), carrying
/// the blocking substrate (`entities`) and recency span. Two arms, unioned (`scope_kind = 'public' OR
/// scope_subscriber_id = $1` is the isolation boundary — never another subscriber's private cluster):
///
///  1. **In-floor** — public ∪ own-private clusters updated since the freshness floor `min(last_run,
///     now − horizon)`. The bulk of the candidate set, served by the `cluster_*_recency` indexes.
///  2. **Cross-boundary seed** — public clusters that share a **strong key** (a `cve:`/`url:`, mirrors
///     `entity::is_strong`) with the subscriber's *active* (in-floor) private clusters, **regardless
///     of the freshness floor on the public side**. This is the design's blocking seed (§8.2):
///     without it, a fresh private incident referencing `CVE-X` would never link to the public
///     advisory about `CVE-X` if that advisory has aged out of the floor — the exact cross-source
///     connection the product is built to surface. GIN-served via the `cluster_entities` index
///     (`entities && <strong keys>`). Public-only, so the seed keys filter shared clusters but no
///     private datum ever crosses scope.
///
/// (The other half of the design's blocking seed — public clusters matching the subscriber's
/// *affinity* — lands with relevance in M4; until then the in-floor arm carries the public set.)
/// Ordered by id for a deterministic linking input.
pub async fn candidate_clusters(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    last_run: Option<DateTime<Utc>>,
    horizon_days: i32,
) -> Result<Vec<LinkCluster>, sqlx::Error> {
    sqlx::query(
        "WITH floor AS (SELECT LEAST($2, now() - make_interval(days => $3)) AS lo),
              in_floor AS (
                  SELECT id, entities, first_event_time, last_event_time
                  FROM cluster
                  WHERE (scope_kind = 'public' OR scope_subscriber_id = $1)
                    AND updated_at >= (SELECT lo FROM floor)
              ),
              -- The strong keys (cve:/url:, mirrors entity::is_strong) the subscriber's *active*
              -- private clusters carry — floored like in_floor, so the seed scales with recent
              -- private activity (a fresh incident), not lifetime history. NULL when they have
              -- none, which makes the seed below a no-op.
              private_strong AS (
                  SELECT array_agg(DISTINCT e) AS keys
                  FROM cluster c, unnest(c.entities) AS e
                  WHERE c.scope_subscriber_id = $1
                    AND c.updated_at >= (SELECT lo FROM floor)
                    AND (e LIKE 'cve:%' OR e LIKE 'url:%')
              ),
              -- Public clusters sharing a strong key with those — *regardless of the floor*, so an
              -- aged-out advisory still links (a strong CVE/URL edge ignores temporal distance).
              cross_boundary AS (
                  SELECT id, entities, first_event_time, last_event_time
                  FROM cluster
                  WHERE scope_kind = 'public'
                    AND entities && (SELECT keys FROM private_strong)
              )
         SELECT * FROM in_floor
         UNION
         SELECT * FROM cross_boundary
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
    let mut tx = pool.begin().await?;

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
