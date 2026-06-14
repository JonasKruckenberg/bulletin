//! The ingest flow's persistence: the `connection` rows it polls + schedules, and appending
//! normalized events to the **event log** (`event` table, fingerprint-deduped).

use crate::common::event::{from_row, Event, NewEvent};
use crate::common::{kind::SourceKind, scope::Scope};
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgPool, Row};
use uuid::Uuid;

// ── Connections ────────────────────────────────────────────────────────

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
    /// The owning subscriber (`None` = a global/public source like RSS). `finalize` binds a private
    /// event to this owner — the IDOR/§12 boundary, since it comes from OUR row, not the payload.
    pub subscriber_id: Option<Uuid>,
}

fn row_to_connection(row: PgRow) -> Result<ConnectionRow, sqlx::Error> {
    Ok(ConnectionRow {
        id: row.get("id"),
        source: row.try_get("source")?,
        status: row.get("status"),
        config: row.get("config"),
        cursor: row.get("cursor"),
        poll_interval_secs: row.get("poll_interval_secs"),
        next_poll_at: row.get("next_poll_at"),
        last_polled_at: row.get("last_polled_at"),
        consecutive_failures: row.get("consecutive_failures"),
        subscriber_id: row.get("subscriber_id"),
    })
}

/// Returns all active connections whose `next_poll_at` is in the past.
pub async fn due_connections(pool: &PgPool) -> Result<Vec<ConnectionRow>, sqlx::Error> {
    sqlx::query(
        "SELECT id, source, status, config, cursor, poll_interval_secs,
                next_poll_at, last_polled_at, consecutive_failures, subscriber_id
         FROM connection
         WHERE status = 'active' AND next_poll_at <= now()",
    )
    .try_map(row_to_connection)
    .fetch_all(pool)
    .await
}

/// Inserts a new active connection and returns its generated id. `owner` is the subscriber that owns
/// this connection's private events (`None` for a global/public source like RSS); a GitHub
/// connection that can see private repos must be owned, or its private events would have no scope to
/// bind to and `finalize` would treat them as public.
pub async fn insert_connection(
    pool: &PgPool,
    source: SourceKind,
    config: serde_json::Value,
    poll_interval_secs: i64,
    owner: Option<Uuid>,
) -> Result<Uuid, sqlx::Error> {
    let row = sqlx::query(
        "INSERT INTO connection (source, config, poll_interval_secs, subscriber_id)
         VALUES ($1, $2, $3, $4)
         RETURNING id",
    )
    .bind(source)
    .bind(config)
    .bind(poll_interval_secs)
    .bind(owner)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

/// Returns all connections regardless of status.
pub async fn list_connections(pool: &PgPool) -> Result<Vec<ConnectionRow>, sqlx::Error> {
    sqlx::query(
        "SELECT id, source, status, config, cursor, poll_interval_secs,
                next_poll_at, last_polled_at, consecutive_failures, subscriber_id
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
                next_poll_at, last_polled_at, consecutive_failures, subscriber_id
         FROM connection WHERE id = $1",
    )
    .bind(id)
    .try_map(row_to_connection)
    .fetch_optional(pool)
    .await
}

/// Resolves the connection a webhook delivery routes to, by `(source, provider_account_id)` — the
/// webhook routing key (GitHub: installation_id). This is the IDOR-defense boundary: the caller
/// derives the subscriber/scope from the returned row, never from the webhook payload.
pub async fn resolve_connection_by_provider(
    pool: &PgPool,
    source: SourceKind,
    provider_account_id: &str,
) -> Result<Option<ConnectionRow>, sqlx::Error> {
    sqlx::query(
        "SELECT id, source, status, config, cursor, poll_interval_secs,
                next_poll_at, last_polled_at, consecutive_failures, subscriber_id
         FROM connection
         WHERE source = $1 AND provider_account_id = $2",
    )
    .bind(source)
    .bind(provider_account_id)
    .try_map(row_to_connection)
    .fetch_optional(pool)
    .await
}

/// Sets a connection's status (e.g. a GitHub App suspend/uninstall lifecycle webhook → 'suspended'
/// / 'revoked'). Any non-'active' value pauses polling via the `due_connections` predicate.
pub async fn update_connection_status(
    pool: &PgPool,
    id: Uuid,
    status: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE connection SET status = $2 WHERE id = $1")
        .bind(id)
        .bind(status)
        .execute(pool)
        .await?;
    Ok(())
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

// ── Event log (append) ───────────────────────────────────────────────────

/// Appends `ev` to the event log, deduplicating on fingerprint.
/// Returns `Some(event)` if inserted, `None` if the fingerprint already existed.
pub async fn insert_event(pool: &PgPool, ev: &NewEvent) -> Result<Option<Event>, sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = match &ev.scope {
        Scope::Public => ("public", None::<Uuid>),
        Scope::Private(sub_id) => ("private", Some(*sub_id)),
    };

    sqlx::query(
        r#"
        INSERT INTO event (
            fingerprint, source, scope_kind, scope_subscriber_id,
            event_time, title, body, links, group_key, entities,
            content_kind, severity_hint, raw
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (fingerprint) DO NOTHING
        RETURNING
            id, fingerprint, source, scope_kind, scope_subscriber_id,
            event_time, title, body, links, group_key, entities,
            content_kind, severity_hint, ingest_time, raw
        "#,
    )
    .bind(&ev.fingerprint.0[..])
    .bind(ev.source)
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(ev.event_time)
    .bind(&ev.title)
    .bind(ev.body.as_deref())
    .bind(&ev.links)
    .bind(&ev.group_key)
    .bind(&ev.entities)
    .bind(ev.content_kind)
    .bind(ev.severity_hint)
    .bind(ev.raw.as_deref())
    .try_map(from_row)
    .fetch_optional(pool)
    .await
}

/// Returns the most recent `limit` events ordered by ingest_time descending (debug dump).
pub async fn list_events(pool: &PgPool, limit: i64) -> Result<Vec<Event>, sqlx::Error> {
    sqlx::query(
        "SELECT id, fingerprint, source, scope_kind, scope_subscriber_id,
                event_time, title, body, links, group_key, entities,
                content_kind, severity_hint, ingest_time, raw
         FROM event
         ORDER BY ingest_time DESC
         LIMIT $1",
    )
    .bind(limit)
    .try_map(from_row)
    .fetch_all(pool)
    .await
}
