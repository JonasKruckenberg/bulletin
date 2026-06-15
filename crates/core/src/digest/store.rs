use std::collections::HashMap;

use crate::common::db::{begin_scope, ScopeCtx};
use crate::common::kind::SourceKind;
use crate::digest::select::{
    DecisionRecord, Format, ItemReason, ReplaySnapshot, ScoringConfig, Shown,
};
use crate::identity::ConfidenceBand;
use crate::link::ClusterRef;
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgConnection, PgPool, Row};
use uuid::Uuid;

/// Loads the global scoring config (the singleton `digest_config` row). The table carries no
/// per-subscriber data, so (like `build_watermark`) it is un-RLS'd and readable in any context.
pub async fn load_config(pool: &PgPool) -> Result<ScoringConfig, sqlx::Error> {
    let row = sqlx::query(
        "SELECT relevance_floor, scope_bonus, severity_weight, recency_half_life_days,
                thread_half_life_days, story_cap, note_cap, resurface_penalty
         FROM digest_config WHERE id = true",
    )
    .fetch_one(pool)
    .await?;
    Ok(ScoringConfig {
        relevance_floor: row.get::<f64, _>("relevance_floor") as f32,
        scope_bonus: row.get::<f64, _>("scope_bonus") as f32,
        severity_weight: row.get::<f64, _>("severity_weight") as f32,
        recency_half_life_days: row.get("recency_half_life_days"),
        thread_half_life_days: row.get("thread_half_life_days"),
        story_cap: row.get::<i32, _>("story_cap") as usize,
        note_cap: row.get::<i32, _>("note_cap") as usize,
        resurface_penalty: row.get::<f64, _>("resurface_penalty") as f32,
    })
}

/// Overwrite the singleton `digest_config` row with `cfg` — the `debug config-set` writer. The table
/// is global (no per-subscriber data, un-RLS'd like `load_config`), so it writes on the pool directly.
/// Callers load the current config, apply their overrides, and pass the merged value here.
pub async fn update_config(pool: &PgPool, cfg: &ScoringConfig) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE digest_config SET
            relevance_floor = $1, scope_bonus = $2, severity_weight = $3,
            recency_half_life_days = $4, thread_half_life_days = $5,
            story_cap = $6, note_cap = $7, resurface_penalty = $8
         WHERE id = true",
    )
    .bind(cfg.relevance_floor as f64)
    .bind(cfg.scope_bonus as f64)
    .bind(cfg.severity_weight as f64)
    .bind(cfg.recency_half_life_days)
    .bind(cfg.thread_half_life_days)
    .bind(cfg.story_cap as i32)
    .bind(cfg.note_cap as i32)
    .bind(cfg.resurface_penalty as f64)
    .execute(pool)
    .await?;
    Ok(())
}

/// A story selected into a digest, with the recency anchor + format to freeze on its `digest_item`
/// (design §9.4 re-surface snapshot). The flow builds these from the selected [`Decision`]s.
pub struct FrozenItem {
    pub story_id: Uuid,
    pub last_event_time: DateTime<Utc>,
    pub format: Format,
}

/// Per story, the snapshot of how it was last shown to this subscriber — the most recent prior
/// `digest_item` in the half-open window `[window_end − horizon, window_end)` (strictly *before*
/// `window_end` so an idempotent re-run doesn't shadow itself; bounded *below* by `horizon_days` so
/// the scan stays small and matches the candidate lookback — a story unseen for longer than the
/// horizon has aged out of the candidate set anyway). Feeds the re-surface suppression in selection
/// (design §9.4). Runs in the subscriber's RLS context (digest/digest_item are fenced to their owner).
///
/// `story_last_event_time IS NOT NULL AND format IS NOT NULL` also skips pre-M4 `digest_item` rows
/// (added before migration 024 had the snapshot columns): such a story isn't damped on its first
/// post-migration re-fire, then self-heals once it's been frozen with a snapshot — a one-cycle grace.
pub async fn last_shown(
    conn: &mut PgConnection,
    subscriber_id: Uuid,
    window_end: DateTime<Utc>,
    horizon_days: i32,
) -> Result<HashMap<Uuid, Shown>, sqlx::Error> {
    sqlx::query(
        "SELECT DISTINCT ON (di.story_id)
                di.story_id, di.story_last_event_time, di.format
         FROM digest_item di
         JOIN digest d ON d.id = di.digest_id
         WHERE d.subscriber_id = $1
           AND d.window_end < $2
           AND d.window_end >= $2 - make_interval(days => $3)
           AND di.story_last_event_time IS NOT NULL AND di.format IS NOT NULL
         ORDER BY di.story_id, d.window_end DESC",
    )
    .bind(subscriber_id)
    .bind(window_end)
    .bind(horizon_days)
    .try_map(|row: PgRow| {
        let format = Format::try_from(row.get::<String, _>("format").as_str())
            .map_err(|e| sqlx::Error::Decode(e.into()))?;
        Ok((
            row.get::<Uuid, _>("story_id"),
            Shown {
                last_event_time: row.get("story_last_event_time"),
                format,
            },
        ))
    })
    .fetch_all(&mut *conn)
    .await
    .map(|rows| rows.into_iter().collect())
}

pub struct DigestRow {
    pub id: Uuid,
    pub subscriber_id: Uuid,
    /// The scheduled boundary that fired — the digest's identity (`UNIQUE(subscriber_id, window_end)`).
    pub window_end: DateTime<Utc>,
    pub delivered_at: Option<DateTime<Utc>>,
}

fn row_to_digest(row: PgRow) -> Result<DigestRow, sqlx::Error> {
    Ok(DigestRow {
        id: row.get("id"),
        subscriber_id: row.get("subscriber_id"),
        window_end: row.get("window_end"),
        delivered_at: row.get("delivered_at"),
    })
}

/// One rendered row of a digest: a selected **story**, in position order. The headline is its
/// representative (the latest member cluster); `connections` are the *other* clusters fused into it —
/// the M3 cross-source value, each with the `link_reason` for why it belongs (design §8.2/§10.2).
/// A singleton story has no connections and renders exactly like a pre-M3 cluster item.
pub struct RenderItem {
    pub title: String,
    pub link: Option<String>,
    pub source: SourceKind,
    pub last_event_time: DateTime<Utc>,
    pub connections: Vec<Connection>,
    /// The persistent thread this story was assigned to (design §5.2 thread-grouped render), with its
    /// identity confidence band — rendered as a chip ("Acme migration" / "possibly …"). `None` until
    /// `thread_maintenance` has named a thread, so an un-threaded digest renders exactly as before.
    pub thread: Option<ThreadTag>,
    /// The decision log behind this item's rank (relevance term + entity spine) — surfaced in the
    /// debug trace. Empty for a pure-recency selection.
    pub reason: ItemReason,
}

/// The thread chip on a rendered item: the thread's label and identity confidence band.
pub struct ThreadTag {
    pub label: String,
    pub confidence: ConfidenceBand,
}

/// A non-representative member of a story, rendered beneath the headline as "connected" context.
pub struct Connection {
    pub title: String,
    pub link: Option<String>,
    pub source: SourceKind,
    pub link_reason: Option<String>,
}

/// The display fields of one cluster, keyed by id — the building block both render paths (and the
/// `digest-explain` dry-run) assemble a story's `RenderItem` from.
pub(crate) struct ClusterCard {
    title: String,
    link: Option<String>,
    source: SourceKind,
    last_event_time: DateTime<Utc>,
}

/// Fetch the display card for each cluster id (order unspecified; callers index by id). Reads
/// `cluster` (RLS-protected) → the caller runs it in the subscriber's scope (own ∪ public visible).
pub(crate) async fn cluster_cards(
    conn: &mut PgConnection,
    ids: &[Uuid],
) -> Result<HashMap<Uuid, ClusterCard>, sqlx::Error> {
    sqlx::query("SELECT id, title, link, source, last_event_time FROM cluster WHERE id = ANY($1)")
        .bind(ids)
        .try_map(|row: PgRow| {
            Ok((
                row.get::<Uuid, _>("id"),
                ClusterCard {
                    title: row.get("title"),
                    link: row.get("link"),
                    source: row.try_get("source")?,
                    last_event_time: row.get("last_event_time"),
                },
            ))
        })
        .fetch_all(conn)
        .await
        .map(|rows| rows.into_iter().collect())
}

/// Assemble a story's `RenderItem` from its member refs and the cluster cards: the representative is
/// the latest member (tie-broken by cluster id for determinism); the rest become `connections`,
/// newest-first, carrying their `link_reason`. Returns `None` if no member resolves to a card (a
/// tombstoned/empty story).
pub(crate) fn build_render_item(
    members: &[ClusterRef],
    cards: &HashMap<Uuid, ClusterCard>,
) -> Option<RenderItem> {
    // Resolve members to cards, keeping the ref alongside (for link_reason), newest-first.
    let mut resolved: Vec<(&ClusterRef, &ClusterCard)> = members
        .iter()
        .filter_map(|m| cards.get(&m.cluster_id).map(|c| (m, c)))
        .collect();
    resolved.sort_by(|(ma, ca), (mb, cb)| {
        cb.last_event_time
            .cmp(&ca.last_event_time)
            .then(ma.cluster_id.cmp(&mb.cluster_id))
    });

    let (_, rep) = *resolved.first()?;
    let connections = resolved[1..]
        .iter()
        .map(|(m, c)| Connection {
            title: c.title.clone(),
            link: c.link.clone(),
            source: c.source,
            link_reason: m.link_reason.clone(),
        })
        .collect();

    Some(RenderItem {
        title: rep.title.clone(),
        link: rep.link.clone(),
        source: rep.source,
        last_event_time: rep.last_event_time,
        connections,
        thread: None,
        reason: ItemReason::default(),
    })
}

/// Idempotently gets-or-creates the digest for `(subscriber, window_end)` with its selected
/// **stories** frozen as `digest_item` rows — digest and items commit together, so the digest is
/// never observed without its selection. On a retry the row already exists; the unique window
/// constraint makes the insert a no-op and the existing (frozen) items are returned untouched.
///
/// Runs in the subscriber's RLS context (the digest + its items + the referenced stories are all
/// theirs), in one transaction so the digest is never observed without its selection.
pub async fn create_with_items(
    pool: &PgPool,
    subscriber_id: Uuid,
    window_end: DateTime<Utc>,
    items: &[FrozenItem],
) -> Result<DigestRow, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Subscriber(subscriber_id)).await?;

    let created = sqlx::query(
        "INSERT INTO digest (subscriber_id, window_end)
         VALUES ($1, $2)
         ON CONFLICT (subscriber_id, window_end) DO NOTHING
         RETURNING id, subscriber_id, window_end, delivered_at",
    )
    .bind(subscriber_id)
    .bind(window_end)
    .try_map(row_to_digest)
    .fetch_optional(&mut *tx)
    .await?;

    let row = match created {
        Some(row) => {
            for (position, item) in items.iter().enumerate() {
                // Freeze the re-surface snapshot too (design §9.4): the recency anchor + format this
                // story was shown at, so the next fire can damp it if no new events arrive.
                sqlx::query(
                    "INSERT INTO digest_item
                        (digest_id, story_id, position, story_last_event_time, format)
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(row.id)
                .bind(item.story_id)
                .bind(position as i32)
                .bind(item.last_event_time)
                .bind(item.format.as_str())
                .execute(&mut *tx)
                .await?;
            }
            row
        }
        // Already exists — its items are frozen from the original transaction.
        None => {
            sqlx::query(
                "SELECT id, subscriber_id, window_end, delivered_at
             FROM digest WHERE subscriber_id = $1 AND window_end = $2",
            )
            .bind(subscriber_id)
            .bind(window_end)
            .try_map(row_to_digest)
            .fetch_one(&mut *tx)
            .await?
        }
    };

    tx.commit().await?;
    Ok(row)
}

/// The digest's frozen stories, each assembled into a [`RenderItem`] (representative + connections +
/// its assigned thread chip), in render order. Walks `digest_item → story.clusters → cluster cards`,
/// left-joining the assigned thread (fenced to the digest's own subscriber, so a stray `thread_id`
/// could never surface another tenant's label). Touches `digest_item` / `story` / `cluster` /
/// `thread` (all RLS-protected) → the caller runs it in the subscriber's scope.
pub async fn render_items(
    conn: &mut PgConnection,
    digest_id: Uuid,
) -> Result<Vec<RenderItem>, sqlx::Error> {
    // The decision log lives once on the digest; index it by story for the per-item debug trace.
    let reason_by_story: HashMap<Uuid, ItemReason> = load_decisions(conn, digest_id)
        .await?
        .into_iter()
        .map(|d| (d.story_id, d.reason))
        .collect();

    let stories: Vec<(Uuid, Vec<ClusterRef>, Option<ThreadTag>)> = sqlx::query(
        "SELECT di.story_id, s.clusters, t.label AS thread_label, t.confidence AS thread_confidence
         FROM digest_item di
         JOIN digest d ON d.id = di.digest_id
         JOIN story s  ON s.id = di.story_id
         LEFT JOIN thread t ON t.id = di.thread_id AND t.subscriber_id = d.subscriber_id
         WHERE di.digest_id = $1
         ORDER BY di.position",
    )
    .bind(digest_id)
    .try_map(|row: PgRow| {
        let clusters: serde_json::Value = row.get("clusters");
        let members: Vec<ClusterRef> =
            serde_json::from_value(clusters).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        let tag = row
            .get::<Option<String>, _>("thread_label")
            .map(|label| ThreadTag {
                label,
                confidence: ConfidenceBand::parse(&row.get::<String, _>("thread_confidence")),
            });
        Ok((row.get::<Uuid, _>("story_id"), members, tag))
    })
    .fetch_all(&mut *conn)
    .await?;

    let stories = stories
        .into_iter()
        .map(|(story_id, members, tag)| {
            (
                members,
                tag,
                reason_by_story.get(&story_id).cloned().unwrap_or_default(),
            )
        })
        .collect();
    assemble_items(conn, stories).await
}

/// Render items for an explicit, ordered set of linked stories — the ad-hoc dispatch / preview path,
/// whose selection isn't frozen into `digest_item` rows. `reasons` (keyed by story id, supplied by
/// the in-memory selection) carries the same decision log the frozen path persists. Reads `cluster`
/// (RLS-protected) → runs in the subscriber's scope.
pub async fn render_items_for_stories(
    conn: &mut PgConnection,
    stories: &[crate::link::LinkedStory],
    reasons: &HashMap<Uuid, ItemReason>,
) -> Result<Vec<RenderItem>, sqlx::Error> {
    let stories = stories
        .iter()
        .map(|s| {
            (
                s.clusters.clone(),
                None,
                reasons.get(&s.id).cloned().unwrap_or_default(),
            )
        })
        .collect();
    assemble_items(conn, stories).await
}

/// Shared assembler: fetch every referenced cluster's card once, then build a `RenderItem` per story
/// (in input order), attaching its thread tag + decision-log reason. Stories that resolve to no card
/// (tombstoned/empty) are skipped.
async fn assemble_items(
    conn: &mut PgConnection,
    stories: Vec<(Vec<ClusterRef>, Option<ThreadTag>, ItemReason)>,
) -> Result<Vec<RenderItem>, sqlx::Error> {
    let ids: Vec<Uuid> = stories
        .iter()
        .flat_map(|(members, _, _)| members.iter().map(|m| m.cluster_id))
        .collect();
    let cards = cluster_cards(conn, &ids).await?;
    Ok(stories
        .into_iter()
        .filter_map(|(members, tag, reason)| {
            build_render_item(&members, &cards).map(|mut item| {
                item.thread = tag;
                item.reason = reason;
                item
            })
        })
        .collect())
}

/// Persist the digest's full decision log (design §10.2) as a **structured** jsonb array on the
/// `digest` row — every candidate story, its `Verdict`, and the reasoning behind its rank, drops
/// included. Stored structured (typed records, not a flattened string) so a later explain UI / feed
/// can re-render and query it. Runs on the caller's subscriber-scoped connection; recorded regardless
/// of the thread-weighting feature.
pub async fn record_decisions(
    conn: &mut PgConnection,
    digest_id: Uuid,
    decisions: &[DecisionRecord],
) -> Result<(), sqlx::Error> {
    let json = serde_json::to_value(decisions).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
    sqlx::query("UPDATE digest SET decisions = $2 WHERE id = $1")
        .bind(digest_id)
        .bind(json)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Load a digest's decision log (the structured `digest.decisions` array). Empty for a pre-thread
/// digest (the `'[]'` default). Used by the render debug trace and any later explain consumer.
pub async fn load_decisions(
    conn: &mut PgConnection,
    digest_id: Uuid,
) -> Result<Vec<DecisionRecord>, sqlx::Error> {
    let json: serde_json::Value = sqlx::query("SELECT decisions FROM digest WHERE id = $1")
        .bind(digest_id)
        .fetch_optional(&mut *conn)
        .await?
        .map(|row| row.get("decisions"))
        .unwrap_or_else(|| serde_json::json!([]));
    Ok(serde_json::from_value(json).unwrap_or_default())
}

/// The decision logs of a subscriber's most recent `limit` digests, newest-first — the eval harness
/// input (design §10.3). Reads `digest` (RLS-protected) → runs in the subscriber's own scope. A
/// pre-M4 digest's `'[]'` default decodes to an empty log.
pub async fn load_decision_logs(
    conn: &mut PgConnection,
    subscriber_id: Uuid,
    limit: i64,
) -> Result<Vec<Vec<DecisionRecord>>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT decisions FROM digest
         WHERE subscriber_id = $1
         ORDER BY window_end DESC
         LIMIT $2",
    )
    .bind(subscriber_id)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let json: serde_json::Value = row.get("decisions");
            serde_json::from_value(json).unwrap_or_default()
        })
        .collect())
}

/// Persist the digest's replay snapshot (the frozen `select` input) as `digest.candidates` jsonb, so
/// it can be re-scored under a trial config later (the eval sweep). Best-effort, like the decision log.
/// Runs on the caller's subscriber-scoped connection (the candidates are the subscriber's own spine).
pub async fn record_candidates(
    conn: &mut PgConnection,
    digest_id: Uuid,
    snapshot: &ReplaySnapshot,
) -> Result<(), sqlx::Error> {
    let json = serde_json::to_value(snapshot).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
    sqlx::query("UPDATE digest SET candidates = $2 WHERE id = $1")
        .bind(digest_id)
        .bind(json)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The replay snapshots of a subscriber's most recent `limit` digests that have one (newest-first) —
/// the config-sweep input. Skips pre-snapshot digests (NULL `candidates`). Reads `digest`
/// (RLS-protected) → the subscriber's own scope.
pub async fn load_candidate_snapshots(
    conn: &mut PgConnection,
    subscriber_id: Uuid,
    limit: i64,
) -> Result<Vec<ReplaySnapshot>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT candidates FROM digest
         WHERE subscriber_id = $1 AND candidates IS NOT NULL
         ORDER BY window_end DESC
         LIMIT $2",
    )
    .bind(subscriber_id)
    .bind(limit)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| serde_json::from_value(row.get("candidates")).ok())
        .collect())
}

/// This subscriber's story-level feedback signals (`target_type = 'story'`), newest-first — the
/// grades the eval harness scores selection against. Reads `feedback` (RLS-protected) → the
/// subscriber's own scope. A non-uuid `target_id` is skipped (defensive; the API binds story ids).
pub async fn story_feedback(
    conn: &mut PgConnection,
    subscriber_id: Uuid,
) -> Result<Vec<(Uuid, String)>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT target_id, signal FROM feedback
         WHERE subscriber_id = $1 AND target_type = 'story'
         ORDER BY created_at DESC",
    )
    .bind(subscriber_id)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            Uuid::parse_str(&row.get::<String, _>("target_id"))
                .ok()
                .map(|id| (id, row.get::<String, _>("signal")))
        })
        .collect())
}

/// Marks the digest delivered and advances the subscriber's schedule in one transaction, so the
/// "delivered ⇒ schedule moved" invariant can't tear across a crash. `delivered_through` becomes
/// the new `last_run_at` (the lookback's consideration floor); `next_run_at` jumps to the next
/// future boundary (coalescing). The `delivered_at IS NULL` guard makes a re-run a no-op.
///
/// All three writes (digest, its stories, the subscriber schedule) are this subscriber's own rows →
/// the subscriber RLS context, atomically.
pub async fn mark_delivered(
    pool: &PgPool,
    digest_id: Uuid,
    subscriber_id: Uuid,
    delivered_through: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Subscriber(subscriber_id)).await?;
    sqlx::query("UPDATE digest SET delivered_at = now() WHERE id = $1 AND delivered_at IS NULL")
        .bind(digest_id)
        .execute(&mut *tx)
        .await?;
    // Stamp the carried stories as delivered (gates the asymmetric-merge rule, §8.2) in the same
    // transaction, so "delivered ⇒ story seen" can't tear across a crash.
    crate::link::store::mark_stories_delivered(&mut *tx, digest_id).await?;
    crate::digest::subscriber::advance_after_delivery(&mut *tx, subscriber_id, delivered_through)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Recent digests with subscriber email and item count, for the debug CLI. A cross-subscriber
/// operator view → the admin control-plane context (digest/subscriber/digest_item are fail-closed
/// outside it).
pub async fn list_digests(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<(DigestRow, String, i64)>, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let rows = sqlx::query(
        "SELECT d.id, d.subscriber_id, d.window_end, d.delivered_at,
                s.email,
                (SELECT count(*) FROM digest_item di WHERE di.digest_id = d.id) AS item_count
         FROM digest d JOIN subscriber s ON s.id = d.subscriber_id
         ORDER BY d.window_end DESC
         LIMIT $1",
    )
    .bind(limit)
    .try_map(|row: PgRow| {
        let email: String = row.get("email");
        let count: i64 = row.get("item_count");
        Ok((row_to_digest(row)?, email, count))
    })
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows)
}

/// The subscriber that owns a story, if it exists — the control-plane lookup provenance uses to pick
/// the RLS scope to read the story's (possibly private) events in. Read as `admin`.
pub async fn story_owner(
    conn: &mut PgConnection,
    story_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query("SELECT subscriber_id FROM story WHERE id = $1")
        .bind(story_id)
        .try_map(|row: PgRow| row.try_get::<Uuid, _>("subscriber_id"))
        .fetch_optional(conn)
        .await
}

/// One event behind a story — a row of its provenance timeline (design §10.1).
pub struct TimelineEntry {
    pub event_time: DateTime<Utc>,
    pub source: SourceKind,
    pub title: String,
    pub link: Option<String>,
}

/// "Show the data behind this story" (design §10.1): walk a story's member clusters → each cluster's
/// `(scope, source, group_key)` → its events, returned oldest-first with source + link. Never
/// collapses membership — the digest references the story, the trail stays in the durable log. Reads
/// `story` / `cluster` / `event` (all RLS-protected) → runs in the owner's subscriber scope. A
/// tombstoned story is followed one `merged_into` hop to its survivor.
pub async fn story_timeline(
    conn: &mut PgConnection,
    story_id: Uuid,
) -> Result<Vec<TimelineEntry>, sqlx::Error> {
    // The member cluster ids: the live story's `clusters`, or — if this id was retro-merged — its
    // survivor's (a single hop, mirroring the deep-link redirect in §8.2).
    let member_ids: Vec<Uuid> = sqlx::query(
        "SELECT (c->>'cluster_id')::uuid AS cluster_id
         FROM story s
         CROSS JOIN LATERAL jsonb_array_elements(
             coalesce(
                 (SELECT surv.clusters FROM story surv WHERE surv.id = s.merged_into),
                 s.clusters
             )
         ) AS c
         WHERE s.id = $1",
    )
    .bind(story_id)
    .try_map(|row: PgRow| row.try_get::<Uuid, _>("cluster_id"))
    .fetch_all(&mut *conn)
    .await?;
    if member_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Each cluster's events, by its scope-aware identity `(scope, source, group_key)`, oldest-first.
    sqlx::query(
        "SELECT e.event_time, e.source, e.title, e.links
         FROM cluster c
         JOIN event e
           ON e.scope_kind = c.scope_kind
          AND e.scope_subscriber_id IS NOT DISTINCT FROM c.scope_subscriber_id
          AND e.source = c.source
          AND e.group_key = c.group_key
         WHERE c.id = ANY($1)
         ORDER BY e.event_time, e.id",
    )
    .bind(&member_ids)
    .try_map(|row: PgRow| {
        let links: Vec<String> = row.get("links");
        Ok(TimelineEntry {
            event_time: row.get("event_time"),
            source: row.try_get("source")?,
            title: row.get("title"),
            link: links.into_iter().next(),
        })
    })
    .fetch_all(&mut *conn)
    .await
}
