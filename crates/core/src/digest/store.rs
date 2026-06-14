use std::collections::HashMap;

use crate::common::kind::SourceKind;
use crate::link::ClusterRef;
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgPool, Row};
use uuid::Uuid;

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

/// Fetch the display card for each cluster id (order unspecified; callers index by id).
pub(crate) async fn cluster_cards(
    pool: &PgPool,
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
        .fetch_all(pool)
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
    })
}

/// Idempotently gets-or-creates the digest for `(subscriber, window_end)` with its selected
/// **stories** frozen as `digest_item` rows — digest and items commit together, so the digest is
/// never observed without its selection. On a retry the row already exists; the unique window
/// constraint makes the insert a no-op and the existing (frozen) items are returned untouched.
pub async fn create_with_items(
    pool: &PgPool,
    subscriber_id: Uuid,
    window_end: DateTime<Utc>,
    story_ids: &[Uuid],
) -> Result<DigestRow, sqlx::Error> {
    let mut tx = pool.begin().await?;

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
            for (position, story_id) in story_ids.iter().enumerate() {
                sqlx::query(
                    "INSERT INTO digest_item (digest_id, story_id, position) VALUES ($1, $2, $3)",
                )
                .bind(row.id)
                .bind(story_id)
                .bind(position as i32)
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

/// The digest's frozen stories, each assembled into a [`RenderItem`] (representative + connections),
/// in render order. Walks `digest_item → story.clusters → cluster cards`.
pub async fn render_items(pool: &PgPool, digest_id: Uuid) -> Result<Vec<RenderItem>, sqlx::Error> {
    let stories: Vec<(i32, Vec<ClusterRef>)> = sqlx::query(
        "SELECT di.position, s.clusters
         FROM digest_item di JOIN story s ON s.id = di.story_id
         WHERE di.digest_id = $1
         ORDER BY di.position",
    )
    .bind(digest_id)
    .try_map(|row: PgRow| {
        let clusters: serde_json::Value = row.get("clusters");
        let members: Vec<ClusterRef> =
            serde_json::from_value(clusters).map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        Ok((row.get::<i32, _>("position"), members))
    })
    .fetch_all(pool)
    .await?;

    assemble_items(pool, stories.into_iter().map(|(_, m)| m).collect()).await
}

/// Render items for an explicit, ordered set of linked stories — the ad-hoc dispatch / preview path,
/// whose selection isn't frozen into `digest_item` rows. Preserves the given order.
pub async fn render_items_for_stories(
    pool: &PgPool,
    stories: &[crate::link::LinkedStory],
) -> Result<Vec<RenderItem>, sqlx::Error> {
    assemble_items(pool, stories.iter().map(|s| s.clusters.clone()).collect()).await
}

/// Shared assembler: fetch every referenced cluster's card once, then build a `RenderItem` per story
/// (in input order). Stories that resolve to no card (tombstoned/empty) are skipped.
async fn assemble_items(
    pool: &PgPool,
    stories: Vec<Vec<ClusterRef>>,
) -> Result<Vec<RenderItem>, sqlx::Error> {
    let ids: Vec<Uuid> = stories
        .iter()
        .flat_map(|members| members.iter().map(|m| m.cluster_id))
        .collect();
    let cards = cluster_cards(pool, &ids).await?;
    Ok(stories
        .iter()
        .filter_map(|members| build_render_item(members, &cards))
        .collect())
}

/// Marks the digest delivered and advances the subscriber's schedule in one transaction, so the
/// "delivered ⇒ schedule moved" invariant can't tear across a crash. `delivered_through` becomes
/// the new `last_run_at` (the lookback's consideration floor); `next_run_at` jumps to the next
/// future boundary (coalescing). The `delivered_at IS NULL` guard makes a re-run a no-op.
pub async fn mark_delivered(
    pool: &PgPool,
    digest_id: Uuid,
    subscriber_id: Uuid,
    delivered_through: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
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

/// Recent digests with subscriber email and item count, for the debug CLI.
pub async fn list_digests(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<(DigestRow, String, i64)>, sqlx::Error> {
    sqlx::query(
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
    .fetch_all(pool)
    .await
}
