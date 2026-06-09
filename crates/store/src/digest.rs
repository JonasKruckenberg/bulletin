use bulletin_core::{cluster::Cluster, id::Id, kind::SourceKind};
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgPool, Row};
use uuid::Uuid;

/// Marker for `Id<Digest>`. The persisted row is `DigestRow`.
pub struct Digest;

pub struct DigestRow {
    pub id: Uuid,
    pub subscriber_id: Uuid,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub delivered_at: Option<DateTime<Utc>>,
}

fn row_to_digest(row: PgRow) -> Result<DigestRow, sqlx::Error> {
    Ok(DigestRow {
        id: row.get("id"),
        subscriber_id: row.get("subscriber_id"),
        window_start: row.get("window_start"),
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

/// Idempotently gets-or-creates the digest for `(subscriber, window_end)` with its selected
/// clusters frozen as `digest_item` rows — digest and items commit together, so the digest is
/// never observed without its selection. On a retry the row already exists; the unique window
/// constraint makes the insert a no-op and the existing (frozen) items are returned untouched.
pub async fn create_with_items(
    pool: &PgPool,
    subscriber_id: Uuid,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    cluster_ids: &[Id<Cluster>],
) -> Result<DigestRow, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let created = sqlx::query(
        "INSERT INTO digest (subscriber_id, window_start, window_end)
         VALUES ($1, $2, $3)
         ON CONFLICT (subscriber_id, window_end) DO NOTHING
         RETURNING id, subscriber_id, window_start, window_end, delivered_at",
    )
    .bind(subscriber_id)
    .bind(window_start)
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
                .bind(cluster_id.as_uuid())
                .bind(position as i32)
                .execute(&mut *tx)
                .await?;
            }
            row
        }
        // Already exists — its items are frozen from the original transaction.
        None => sqlx::query(
            "SELECT id, subscriber_id, window_start, window_end, delivered_at
             FROM digest WHERE subscriber_id = $1 AND window_end = $2",
        )
        .bind(subscriber_id)
        .bind(window_end)
        .try_map(row_to_digest)
        .fetch_one(&mut *tx)
        .await?,
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
        let source: String = row.get("source");
        let source = SourceKind::try_from(source.as_str())
            .map_err(|_| sqlx::Error::Decode(format!("unknown source kind: {source}").into()))?;
        Ok(RenderItem {
            title: row.get("title"),
            link: row.get("link"),
            source,
            last_event_time: row.get("last_event_time"),
        })
    })
    .fetch_all(pool)
    .await
}

/// Marks the digest delivered and advances the subscriber's watermark in one transaction, so
/// the "delivered ⇒ watermark moved" invariant can't tear across a crash. The `delivered_at IS
/// NULL` guard makes a re-run a no-op (idempotent).
pub async fn mark_delivered(
    pool: &PgPool,
    digest_id: Uuid,
    subscriber_id: Uuid,
    window_end: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query("UPDATE digest SET delivered_at = now() WHERE id = $1 AND delivered_at IS NULL")
        .bind(digest_id)
        .execute(&mut *tx)
        .await?;
    crate::subscriber::advance_after_delivery(&mut *tx, subscriber_id, window_end).await?;
    tx.commit().await?;
    Ok(())
}

/// Recent digests with subscriber email and item count, for the debug CLI.
pub async fn list_digests(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<(DigestRow, String, i64)>, sqlx::Error> {
    sqlx::query(
        "SELECT d.id, d.subscriber_id, d.window_start, d.window_end, d.delivered_at,
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
