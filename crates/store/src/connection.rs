use bulletin_core::kind::SourceKind;
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgPool, Row};
use uuid::Uuid;

pub struct ConnectionRow {
    pub id: Uuid,
    pub source: SourceKind,
    pub status: String,
    pub config: serde_json::Value,
    pub cursor: Option<serde_json::Value>,
    pub poll_interval_secs: i64,
    pub next_poll_at: DateTime<Utc>,
    pub last_polled_at: Option<DateTime<Utc>>,
    pub consecutive_failures: i16,
}

fn row_to_connection(row: PgRow) -> Result<ConnectionRow, sqlx::Error> {
    let source: String = row.get("source");
    let source = SourceKind::try_from(source.as_str())
        .map_err(|_| sqlx::Error::Decode(format!("unknown source kind: {source}").into()))?;
    Ok(ConnectionRow {
        id: row.get("id"),
        source,
        status: row.get("status"),
        config: row.get("config"),
        cursor: row.get("cursor"),
        poll_interval_secs: row.get("poll_interval_secs"),
        next_poll_at: row.get("next_poll_at"),
        last_polled_at: row.get("last_polled_at"),
        consecutive_failures: row.get("consecutive_failures"),
    })
}

/// Returns all active connections whose `next_poll_at` is in the past.
pub async fn due_connections(pool: &PgPool) -> Result<Vec<ConnectionRow>, sqlx::Error> {
    sqlx::query(
        "SELECT id, source, status, config, cursor, poll_interval_secs,
                next_poll_at, last_polled_at, consecutive_failures
         FROM connection
         WHERE status = 'active' AND next_poll_at <= now()",
    )
    .try_map(row_to_connection)
    .fetch_all(pool)
    .await
}

/// Inserts a new active connection and returns its generated id.
pub async fn insert_connection(
    pool: &PgPool,
    source: SourceKind,
    config: serde_json::Value,
    poll_interval_secs: i64,
) -> Result<Uuid, sqlx::Error> {
    let row = sqlx::query(
        "INSERT INTO connection (source, config, poll_interval_secs)
         VALUES ($1, $2, $3)
         RETURNING id",
    )
    .bind(source.as_str())
    .bind(config)
    .bind(poll_interval_secs)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

/// Returns all connections regardless of status.
pub async fn list_connections(pool: &PgPool) -> Result<Vec<ConnectionRow>, sqlx::Error> {
    sqlx::query(
        "SELECT id, source, status, config, cursor, poll_interval_secs,
                next_poll_at, last_polled_at, consecutive_failures
         FROM connection ORDER BY next_poll_at",
    )
    .try_map(row_to_connection)
    .fetch_all(pool)
    .await
}

/// Deletes a connection by id. Returns true if a row was deleted.
pub async fn delete_connection(pool: &PgPool, id: Uuid) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("DELETE FROM connection WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Loads a single connection by id.
pub async fn load_connection(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<ConnectionRow>, sqlx::Error> {
    sqlx::query(
        "SELECT id, source, status, config, cursor, poll_interval_secs,
                next_poll_at, last_polled_at, consecutive_failures
         FROM connection WHERE id = $1",
    )
    .bind(id)
    .try_map(row_to_connection)
    .fetch_optional(pool)
    .await
}

/// Advances the cursor after a successful poll and resets the failure counter.
pub async fn advance_cursor(
    pool: &PgPool,
    id: Uuid,
    cursor: serde_json::Value,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE connection
         SET cursor = $2,
             last_polled_at = now(),
             next_poll_at = now() + (poll_interval_secs || ' seconds')::interval,
             consecutive_failures = 0
         WHERE id = $1",
    )
    .bind(id)
    .bind(cursor)
    .execute(pool)
    .await?;
    Ok(())
}

/// Records a failed poll: increments failure count and applies exponential backoff.
/// Flips status to 'errored' after 5 consecutive failures.
pub async fn record_failure(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE connection
         SET consecutive_failures = consecutive_failures + 1,
             next_poll_at = now() + least(
                 poll_interval_secs * power(2, consecutive_failures)::bigint,
                 86400  -- cap at 24 h
             ) * interval '1 second',
             status = CASE WHEN consecutive_failures + 1 >= 5 THEN 'errored' ELSE status END
         WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}
