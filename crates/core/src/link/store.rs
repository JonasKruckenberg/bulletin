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

/// Hard upper bound (days) on how far the candidate floor may reach back, regardless of how long
/// ago the subscriber last ran. The floor is normally `LEAST(last_run, now − horizon)` so it always
/// reaches back to the last delivery — but a subscriber dormant for months would otherwise pull
/// their entire cluster history into one linking pass (an unbounded scan + O(n²) blocking cost).
/// This caps that reach for **perf**: events older than this can't corroborate a fresh one anyway
/// (they're long past any decay horizon), so bounding the scan costs nothing the digest would surface.
const MAX_LOOKBACK_DAYS: i32 = 90;

/// The subscriber's candidate clusters for linking: their scope (`subscribed-public ∪ own-private`),
/// carrying the blocking substrate (`entities`) and recency span. Two arms, unioned
/// (`scope_subscriber_id = $1` is the isolation boundary — never another subscriber's private
/// cluster):
///
///  1. **In-floor** — clusters updated since the freshness floor `min(last_run, now − horizon)`,
///     itself clamped to at most [`MAX_LOOKBACK_DAYS`] back (a dormant subscriber's reach is bounded
///     for perf): a
///     public cluster only when the subscriber subscribes to the `connection` that produced it (the
///     sources they chose for their digest; an unattributed public row — no origin on record — stays
///     global), plus all own-private. The bulk of the candidate set, served by the `cluster_*_recency`
///     indexes + the `subscription` join.
///  2. **Cross-boundary seed** — public clusters that share a **strong key** (a `cve:`/`url:`, mirrors
///     `entity::link_strength`) with the subscriber's *active* (in-floor) private clusters, **regardless
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
///
/// When `require_summary` is set (always, in the strict §3.7 policy — the caller passes `true`), a
/// cluster is a candidate **only once it carries a gate-passed model summary** (`summary`'s `band` is
/// `confirmed`/`probable`). A cluster that was never summarized, or whose model output the faithfulness
/// gate rejected, is *withheld* and slips to a later digest once the off-path sweep summarizes it — so a
/// digest never ships an item without a real grounded summary. Strict, with no age valve: a prolonged
/// sidecar outage withholds those clusters until it recovers rather than degrading to a baseline.
pub async fn candidate_clusters(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    last_run: Option<DateTime<Utc>>,
    horizon_days: i32,
    require_summary: bool,
) -> Result<Vec<LinkCluster>, sqlx::Error> {
    // The summary gate is a bound bool ($4), not interpolated SQL: when `require_summary` is false the
    // `$4` arm short-circuits the predicate true (allow all); when true, the cluster must carry a
    // gate-passed model summary. `band` is read from the `summary` jsonb — a never-summarized cluster
    // ('{}') has no band and a rejected baseline bands `uncertain`, so both are excluded. Applied to the
    // two public-candidate arms (in_floor, cross_boundary), never to the private_strong seed.
    sqlx::query(
        // The candidate floor reaches back to the last delivery (`LEAST(last_run, now − horizon)`,
        // so nothing since the last digest ages out unconsidered), but never past `MAX_LOOKBACK_DAYS`
        // ($5) — the `GREATEST` clamp bounds a dormant subscriber's scan for perf without dropping any
        // context a fresh event could still corroborate with.
        "WITH floor AS (
                  SELECT GREATEST(
                             LEAST($2, now() - make_interval(days => $3)),
                             now() - make_interval(days => $5)
                         ) AS lo),
              in_floor AS (
                  SELECT id, scope_kind, source, entities, first_event_time, last_event_time,
                         event_count, content_depth, max_severity
                  FROM cluster
                  -- The candidate scope, now an explicit per-source filter: a public cluster enters
                  -- only if the subscriber subscribes to the connection that produced it (the source
                  -- they chose for their digest); own-private always. `scope_subscriber_id = $1` stays
                  -- the isolation boundary — never another subscriber's private cluster.
                  --
                  -- `connection_id IS NULL` (an unattributed public cluster — no originating feed on
                  -- record) is treated as global. Real ingest (poll/webhook) always stamps the
                  -- connection, so this only covers off-path/fixture rows; an attributed public source
                  -- is always subscription-gated. Source selection is a product filter, not a privacy
                  -- boundary (that's RLS), so failing open for the degenerate no-origin case is safe.
                  WHERE (
                          (scope_kind = 'public'
                              AND (connection_id IS NULL
                                   OR connection_id IN (
                                       SELECT connection_id FROM subscription
                                       WHERE subscriber_id = $1)))
                          OR scope_subscriber_id = $1
                        )
                    AND updated_at >= (SELECT lo FROM floor)
                    AND (NOT $4 OR summary ->> 'band' IN ('confirmed', 'probable'))
              ),
              -- The strong keys (cve:/url:, mirrors entity::link_strength) the subscriber's *active*
              -- private clusters carry — floored like in_floor, so the seed scales with recent
              -- private activity (a fresh incident), not lifetime history. NULL when they have
              -- none, which makes the seed below a no-op. (Ungated: an unsummarized private incident
              -- can still pull in its related public advisory, which is itself summary-gated below;
              -- the incident itself stays out of the candidate set via the in_floor gate.)
              private_strong AS (
                  SELECT array_agg(DISTINCT e) AS keys
                  FROM cluster c, unnest(c.entities) AS e
                  WHERE c.scope_subscriber_id = $1
                    AND c.updated_at >= (SELECT lo FROM floor)
                    AND (e LIKE 'cve:%' OR e LIKE 'url:%')
              ),
              -- Public clusters sharing a strong key with those — *regardless of the floor*, so an
              -- aged-out advisory still links (a strong CVE/URL edge ignores temporal distance).
              -- Intentionally NOT subscription-gated: this seed is not browsing an unsubscribed
              -- source, it enriches a story the subscriber already gets (via their own private
              -- incident) with the public advisory that incident strong-keys to — the product cross-
              -- source link. It only fires on a cve:/url: match to the subscriber own active private
              -- clusters, so it cannot pull in arbitrary public content.
              cross_boundary AS (
                  SELECT id, scope_kind, source, entities, first_event_time, last_event_time,
                         event_count, content_depth, max_severity
                  FROM cluster
                  WHERE scope_kind = 'public'
                    AND entities && (SELECT keys FROM private_strong)
                    AND (NOT $4 OR summary ->> 'band' IN ('confirmed', 'probable'))
              )
         SELECT * FROM in_floor
         UNION
         SELECT * FROM cross_boundary
         ORDER BY id",
    )
    .bind(subscriber_id)
    .bind(last_run)
    .bind(horizon_days)
    .bind(require_summary)
    .bind(MAX_LOOKBACK_DAYS)
    .try_map(|row: PgRow| {
        let scope_kind: String = row.get("scope_kind");
        Ok(LinkCluster {
            id: row.get("id"),
            entities: row.get("entities"),
            first_event_time: row.get("first_event_time"),
            last_event_time: row.get("last_event_time"),
            source: row.try_get("source")?,
            event_count: row.get("event_count"),
            content_depth: row.try_get("content_depth")?,
            max_severity: row.get("max_severity"),
            is_own_private: scope_kind == "private",
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
                (id, subscriber_id, clusters, first_event_time, last_event_time,
                 event_count, source_diversity, content_depth, max_severity)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (id) DO UPDATE SET
                clusters = EXCLUDED.clusters,
                first_event_time = EXCLUDED.first_event_time,
                last_event_time = EXCLUDED.last_event_time,
                event_count = EXCLUDED.event_count,
                source_diversity = EXCLUDED.source_diversity,
                content_depth = EXCLUDED.content_depth,
                max_severity = EXCLUDED.max_severity,
                merged_into = NULL,
                -- Bump updated_at only when a synthesis-relevant field actually changed, so a no-op
                -- re-upsert (identical membership + aggregates) leaves it untouched. This is what lets
                -- the story-synthesis sweep's `updated_at > summarized_at` due-gate quiesce: without it
                -- the blanket `now()` re-flagged every live story on every fire (membership/recency
                -- moves still bump it, so a real content change is still caught next sweep).
                updated_at = CASE
                    WHEN (story.clusters, story.first_event_time, story.last_event_time,
                          story.event_count, story.source_diversity, story.content_depth,
                          story.max_severity, story.merged_into)
                       IS DISTINCT FROM
                         (EXCLUDED.clusters, EXCLUDED.first_event_time, EXCLUDED.last_event_time,
                          EXCLUDED.event_count, EXCLUDED.source_diversity, EXCLUDED.content_depth,
                          EXCLUDED.max_severity, NULL::uuid)
                    THEN now() ELSE story.updated_at END",
        )
        .bind(story.id)
        .bind(subscriber_id)
        .bind(clusters)
        .bind(story.first_event_time)
        .bind(story.last_event_time)
        .bind(story.event_count)
        .bind(story.source_diversity)
        .bind(story.content_depth)
        .bind(story.max_severity)
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
