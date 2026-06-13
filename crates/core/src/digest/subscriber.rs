use chrono::{DateTime, NaiveTime, Utc};
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use uuid::Uuid;

/// The subscriber's selectable columns. Scheduling is a wall-clock target — `digest_time`
/// (local time-of-day) in `timezone` (IANA name) — rather than an offset from signup, so the
/// digest stays at the chosen local time across DST and across travel.
pub struct SubscriberRow {
    pub id: Uuid,
    pub email: String,
    pub interval_days: i32,
    pub max_items: i32,
    pub timezone: String,
    pub digest_time: NaiveTime,
    pub next_run_at: DateTime<Utc>,
    pub last_run_at: Option<DateTime<Utc>>,
}

/// The column list every read shares — kept in one place so a schema add can't drift between
/// `load`, `list`, and `due` (they must agree for selection to be consistent).
const SELECT_COLS: &str =
    "id, email, interval_days, max_items, timezone, digest_time, next_run_at, last_run_at";

fn row_to_subscriber(row: PgRow) -> Result<SubscriberRow, sqlx::Error> {
    Ok(SubscriberRow {
        id: row.get("id"),
        email: row.get("email"),
        interval_days: row.get("interval_days"),
        max_items: row.get("max_items"),
        timezone: row.get("timezone"),
        digest_time: row.get("digest_time"),
        next_run_at: row.get("next_run_at"),
        last_run_at: row.get("last_run_at"),
    })
}

/// Inserts a subscriber and returns its id. The first digest fires at the next occurrence of
/// `digest_time` in `timezone` (the next earliest local slot), not immediately — so it lands at
/// the subscriber's chosen hour from day one. An unknown `timezone` is rejected by the database
/// (the `next_digest_boundary` call can't resolve it).
pub async fn insert_subscriber(
    pool: &PgPool,
    email: &str,
    interval_days: i32,
    timezone: &str,
    digest_time: NaiveTime,
) -> Result<Uuid, sqlx::Error> {
    let row = sqlx::query(
        "INSERT INTO subscriber (email, interval_days, timezone, digest_time, next_run_at)
         VALUES ($1, $2, $3, $4, next_digest_boundary(now(), $3, $4, 1))
         RETURNING id",
    )
    .bind(email)
    .bind(interval_days)
    .bind(timezone)
    .bind(digest_time)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

/// Re-points a subscriber onto a new timezone and/or local digest time and snaps `next_run_at`
/// to the next earliest occurrence of that time, never before the last delivered window. This is
/// the "subscriber traveled / changed their mind" path: it's a pure reschedule, so
///
///   * **no digest is lost** — `last_run_at` (the selection's lower bound) is untouched, so every
///     event since the last delivery still falls in the next window; the window only reshapes.
///   * **no digest is replayed** — anchoring on `GREATEST(now(), last_run_at)` keeps the new
///     boundary strictly after what was already delivered.
///   * **it's safe while a digest is due** — moving the boundary into the future simply defers the
///     pending send to the new schedule (the tick re-reads it; see [`super::generate`]'s guard).
///
/// Returns `false` if no subscriber has that id. An unknown `timezone` is rejected by the database.
pub async fn update_preferences(
    pool: &PgPool,
    id: Uuid,
    timezone: &str,
    digest_time: NaiveTime,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE subscriber
         SET timezone = $2,
             digest_time = $3,
             next_run_at = next_digest_boundary(
                 GREATEST(now(), COALESCE(last_run_at, now())), $2, $3, 1)
         WHERE id = $1",
    )
    .bind(id)
    .bind(timezone)
    .bind(digest_time)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn list_subscribers(pool: &PgPool) -> Result<Vec<SubscriberRow>, sqlx::Error> {
    sqlx::query(&format!(
        "SELECT {SELECT_COLS} FROM subscriber ORDER BY next_run_at"
    ))
    .try_map(row_to_subscriber)
    .fetch_all(pool)
    .await
}

pub async fn load_subscriber(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<SubscriberRow>, sqlx::Error> {
    sqlx::query(&format!("SELECT {SELECT_COLS} FROM subscriber WHERE id = $1"))
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
        "SELECT s.id, s.email, s.interval_days, s.max_items, s.timezone, s.digest_time,
                s.next_run_at, s.last_run_at
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
/// and `next_run_at` moves one cadence forward to the next occurrence of `digest_time` in the
/// subscriber's `timezone` (DST-safe — see `next_digest_boundary`). Called only once the digest is
/// delivered, so a crashed run is simply still due next tick (design §9.3).
pub async fn advance_after_delivery(
    executor: impl PgExecutor<'_>,
    id: Uuid,
    window_end: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE subscriber
         SET last_run_at = $2,
             next_run_at = next_digest_boundary($2, timezone, digest_time, interval_days)
         WHERE id = $1",
    )
    .bind(id)
    .bind(window_end)
    .execute(executor)
    .await?;
    Ok(())
}
