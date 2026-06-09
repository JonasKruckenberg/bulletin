use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use uuid::Uuid;

pub struct SubscriberRow {
    pub id: Uuid,
    pub email: String,
    pub interval_days: i32,
    pub max_items: i32,
    pub next_run_at: DateTime<Utc>,
    pub last_run_at: Option<DateTime<Utc>>,
}

fn row_to_subscriber(row: PgRow) -> Result<SubscriberRow, sqlx::Error> {
    Ok(SubscriberRow {
        id: row.get("id"),
        email: row.get("email"),
        interval_days: row.get("interval_days"),
        max_items: row.get("max_items"),
        next_run_at: row.get("next_run_at"),
        last_run_at: row.get("last_run_at"),
    })
}

/// Inserts a subscriber (first digest due immediately) and returns its id.
pub async fn insert_subscriber(
    pool: &PgPool,
    email: &str,
    interval_days: i32,
) -> Result<Uuid, sqlx::Error> {
    let row = sqlx::query(
        "INSERT INTO subscriber (email, interval_days) VALUES ($1, $2) RETURNING id",
    )
    .bind(email)
    .bind(interval_days)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn list_subscribers(pool: &PgPool) -> Result<Vec<SubscriberRow>, sqlx::Error> {
    sqlx::query(
        "SELECT id, email, interval_days, max_items, next_run_at, last_run_at
         FROM subscriber ORDER BY next_run_at",
    )
    .try_map(row_to_subscriber)
    .fetch_all(pool)
    .await
}

pub async fn load_subscriber(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<SubscriberRow>, sqlx::Error> {
    sqlx::query(
        "SELECT id, email, interval_days, max_items, next_run_at, last_run_at
         FROM subscriber WHERE id = $1",
    )
    .bind(id)
    .try_map(row_to_subscriber)
    .fetch_optional(pool)
    .await
}

/// Subscribers whose digest is due AND whose window is fully built: the boundary has passed
/// (`next_run_at <= now()`) and no public event ingested before it is still unbuilt. The
/// second clause is how the tick honors PublicBuild → GenerateDigest without job-chaining.
pub async fn due_subscribers(pool: &PgPool) -> Result<Vec<SubscriberRow>, sqlx::Error> {
    sqlx::query(
        "SELECT s.id, s.email, s.interval_days, s.max_items, s.next_run_at, s.last_run_at
         FROM subscriber s
         WHERE s.next_run_at <= now()
           AND NOT EXISTS (
               SELECT 1 FROM event e
               WHERE e.scope_kind = 'public'
                 AND e.ingest_time <= s.next_run_at
                 AND e.ingest_time > (SELECT built_through FROM build_watermark)
           )
         ORDER BY s.next_run_at",
    )
    .try_map(row_to_subscriber)
    .fetch_all(pool)
    .await
}

/// Advances the digest watermark after delivery: the delivered boundary becomes `last_run_at`,
/// and `next_run_at` moves one interval forward. Called only once the digest is delivered, so a
/// crashed run is simply still due next tick (design §9.3).
pub async fn advance_after_delivery(
    executor: impl PgExecutor<'_>,
    id: Uuid,
    window_end: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE subscriber
         SET last_run_at = $2,
             next_run_at = $2 + (interval_days || ' days')::interval
         WHERE id = $1",
    )
    .bind(id)
    .bind(window_end)
    .execute(executor)
    .await?;
    Ok(())
}
