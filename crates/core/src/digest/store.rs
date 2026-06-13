use std::collections::HashMap;

use crate::common::kind::SourceKind;
use crate::digest::select::Candidate;
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
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

/// One rendered row of a digest: a selected cluster's representative fields, in position order.
pub struct RenderItem {
    pub title: String,
    pub link: Option<String>,
    pub source: SourceKind,
    pub last_event_time: DateTime<Utc>,
}

/// The digest's candidate set: clusters built/updated since the **consideration floor**
/// `min(last_run, now − horizon_days)` — a freshness-scored lookback, not a window partition. The
/// floor always reaches back to the last delivery (so nothing since then is missed, even a backdated
/// event, since the bound is on `cluster.updated_at` = ingest/build recency) and pulls in older
/// context up to the horizon. Ordered newest-first; the pure `select()` applies the cap. A cluster
/// may legitimately appear in consecutive digests (design §9.4). `last_run = None` ⇒ floor is just
/// `now − horizon_days`.
pub async fn candidates_in_lookback(
    executor: impl PgExecutor<'_>,
    last_run: Option<DateTime<Utc>>,
    horizon_days: i32,
) -> Result<Vec<Candidate>, sqlx::Error> {
    sqlx::query(
        "SELECT id, last_event_time
         FROM cluster
         WHERE updated_at >= LEAST(
                 COALESCE($1, now() - make_interval(days => $2)),
                 now() - make_interval(days => $2))
         ORDER BY last_event_time DESC",
    )
    .bind(last_run)
    .bind(horizon_days)
    .try_map(|row: PgRow| {
        Ok(Candidate {
            cluster_id: row.get("id"),
            last_event_time: row.get("last_event_time"),
        })
    })
    .fetch_all(executor)
    .await
}

/// Human-readable identity (source, title) of a cluster — the display half of a selection
/// `Decision`, which carries only the id.
pub struct ClusterDisplay {
    pub id: Uuid,
    pub source: SourceKind,
    pub title: String,
}

/// Display fields for a set of clusters by id, for `debug digest-explain` to pair each
/// selection verdict with a human-readable cluster. Order is unspecified — callers index by id.
pub async fn cluster_display(
    executor: impl PgExecutor<'_>,
    ids: &[Uuid],
) -> Result<Vec<ClusterDisplay>, sqlx::Error> {
    sqlx::query("SELECT id, source, title FROM cluster WHERE id = ANY($1)")
        .bind(ids)
        .try_map(|row: PgRow| {
            Ok(ClusterDisplay {
                id: row.get("id"),
                source: row.try_get("source")?,
                title: row.get("title"),
            })
        })
        .fetch_all(executor)
        .await
}

/// Idempotently gets-or-creates the digest for `(subscriber, window_end)` with its selected
/// clusters frozen as `digest_item` rows — digest and items commit together, so the digest is
/// never observed without its selection. On a retry the row already exists; the unique window
/// constraint makes the insert a no-op and the existing (frozen) items are returned untouched.
pub async fn create_with_items(
    pool: &PgPool,
    subscriber_id: Uuid,
    window_end: DateTime<Utc>,
    cluster_ids: &[Uuid],
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
            for (position, cluster_id) in cluster_ids.iter().enumerate() {
                sqlx::query(
                    "INSERT INTO digest_item (digest_id, cluster_id, position) VALUES ($1, $2, $3)",
                )
                .bind(row.id)
                .bind(cluster_id)
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

/// The digest's items joined to their clusters, in render order.
pub async fn render_items(pool: &PgPool, digest_id: Uuid) -> Result<Vec<RenderItem>, sqlx::Error> {
    sqlx::query(
        "SELECT c.title, c.link, c.source, c.last_event_time
         FROM digest_item di
         JOIN cluster c ON c.id = di.cluster_id
         WHERE di.digest_id = $1
         ORDER BY di.position",
    )
    .bind(digest_id)
    .try_map(|row: PgRow| {
        Ok(RenderItem {
            title: row.get("title"),
            link: row.get("link"),
            source: row.try_get("source")?,
            last_event_time: row.get("last_event_time"),
        })
    })
    .fetch_all(pool)
    .await
}

/// Render items for an explicit, ordered set of cluster ids — the ad-hoc dispatch path, whose
/// selection isn't frozen into `digest_item` rows. Preserves the given order; silently skips ids
/// with no matching cluster.
pub async fn render_items_for_clusters(
    pool: &PgPool,
    cluster_ids: &[Uuid],
) -> Result<Vec<RenderItem>, sqlx::Error> {
    let mut by_id: HashMap<Uuid, RenderItem> = sqlx::query(
        "SELECT id, title, link, source, last_event_time FROM cluster WHERE id = ANY($1)",
    )
    .bind(cluster_ids)
    .try_map(|row: PgRow| {
        Ok((
            row.get::<Uuid, _>("id"),
            RenderItem {
                title: row.get("title"),
                link: row.get("link"),
                source: row.try_get("source")?,
                last_event_time: row.get("last_event_time"),
            },
        ))
    })
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();
    Ok(cluster_ids.iter().filter_map(|id| by_id.remove(id)).collect())
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
