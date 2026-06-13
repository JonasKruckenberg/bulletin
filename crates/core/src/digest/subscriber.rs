use chrono::{DateTime, NaiveTime, Utc};
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use uuid::Uuid;

/// A subscriber's delivery cadence. `Weekly` carries its (stable) weekday, so the "weekly ⇔ has a
/// weekday" invariant is unrepresentable-when-wrong in Rust — the DB CHECK is just a backstop.
/// Weekday is 0 = Sunday .. 6 = Saturday (Postgres DOW), matching the `next_run` SQL function. The
/// two-column storage shape (`freq` text, `on_weekday` int) is an implementation detail of [`columns`]
/// / [`from_columns`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recurrence {
    Daily,
    Weekly { weekday: i32 },
}

impl Recurrence {
    /// Build (and validate) from user input — the single validation path shared by signup and
    /// preference updates: `weekday` must be present iff weekly, and in `0..=6`.
    pub fn new(freq: &str, weekday: Option<i32>) -> Result<Self, String> {
        match (freq, weekday) {
            ("daily", None) => Ok(Recurrence::Daily),
            ("daily", Some(_)) => Err("daily takes no weekday".into()),
            ("weekly", Some(d)) if (0..=6).contains(&d) => Ok(Recurrence::Weekly { weekday: d }),
            ("weekly", Some(_)) => Err("weekday must be 0..=6 (0 = Sunday)".into()),
            ("weekly", None) => Err("weekly requires a weekday 0..=6 (0 = Sunday)".into()),
            (other, _) => Err(format!("unknown frequency '{other}' (expected daily or weekly)")),
        }
    }

    /// The stored `(freq, on_weekday)` shape.
    fn columns(self) -> (&'static str, Option<i32>) {
        match self {
            Recurrence::Daily => ("daily", None),
            Recurrence::Weekly { weekday } => ("weekly", Some(weekday)),
        }
    }

    fn from_columns(freq: &str, on_weekday: Option<i32>) -> Result<Self, sqlx::Error> {
        Self::new(freq, on_weekday).map_err(|e| sqlx::Error::Decode(e.into()))
    }

    /// A compact human label for the debug CLI / status output.
    pub fn label(self) -> String {
        match self {
            Recurrence::Daily => "daily".to_string(),
            Recurrence::Weekly { weekday } => format!("weekly d{weekday}"),
        }
    }
}

/// The subscriber's selectable columns. Scheduling is a [`Recurrence`] at a local `digest_time` in
/// `timezone` (IANA name) — rather than an offset from signup — so the digest stays at the chosen
/// local time across DST and across travel. The boundary math lives in the SQL `next_run` function.
pub struct SubscriberRow {
    pub id: Uuid,
    pub email: String,
    pub recurrence: Recurrence,
    pub max_items: i32,
    pub timezone: String,
    pub digest_time: NaiveTime,
    pub next_run_at: DateTime<Utc>,
    pub last_run_at: Option<DateTime<Utc>>,
}

/// The column list every read shares — kept in one place so a schema add can't drift between
/// `load`, `list`, and `due` (they must agree for selection to be consistent).
const SELECT_COLS: &str =
    "id, email, freq, on_weekday, max_items, timezone, digest_time, next_run_at, last_run_at";

fn row_to_subscriber(row: PgRow) -> Result<SubscriberRow, sqlx::Error> {
    Ok(SubscriberRow {
        id: row.get("id"),
        email: row.get("email"),
        recurrence: Recurrence::from_columns(row.get("freq"), row.get("on_weekday"))?,
        max_items: row.get("max_items"),
        timezone: row.get("timezone"),
        digest_time: row.get("digest_time"),
        next_run_at: row.get("next_run_at"),
        last_run_at: row.get("last_run_at"),
    })
}

/// Inserts a subscriber and returns its id. The first digest fires at the next occurrence of the
/// recurrence (the next earliest local slot), not immediately — so it lands at the subscriber's
/// chosen time from day one. An unknown `timezone` is rejected by the database (the `next_run` call).
pub async fn insert_subscriber(
    pool: &PgPool,
    email: &str,
    recurrence: Recurrence,
    timezone: &str,
    digest_time: NaiveTime,
) -> Result<Uuid, sqlx::Error> {
    let (freq, on_weekday) = recurrence.columns();
    let row = sqlx::query(
        "INSERT INTO subscriber (email, freq, on_weekday, timezone, digest_time, next_run_at)
         VALUES ($1, $2, $3, $4, $5, next_run(now(), $4, $5, $2, $3))
         RETURNING id",
    )
    .bind(email)
    .bind(freq)
    .bind(on_weekday)
    .bind(timezone)
    .bind(digest_time)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

/// Re-points a subscriber onto a new recurrence (frequency / weekday / timezone / local time) and
/// snaps `next_run_at` to the next earliest occurrence, never before the last delivered window.
/// This is the "subscriber traveled / changed their mind" path: a pure reschedule, so
///
///   * **no digest is lost** — `last_run_at` (the selection's lower bound) is untouched, so every
///     event since the last delivery still feeds the next digest; the window only reshapes.
///   * **no digest is replayed** — anchoring on `GREATEST(now(), last_run_at)` keeps the new
///     boundary strictly after what was already delivered.
///   * **it's safe while a digest is due** — moving the boundary into the future simply defers the
///     pending send to the new schedule (the tick re-reads it; see [`super::generate`]'s guard).
///
/// Returns `false` if no subscriber has that id. An unknown `timezone` is rejected by the database.
pub async fn update_preferences(
    pool: &PgPool,
    id: Uuid,
    recurrence: Recurrence,
    timezone: &str,
    digest_time: NaiveTime,
) -> Result<bool, sqlx::Error> {
    let (freq, on_weekday) = recurrence.columns();
    let result = sqlx::query(
        "UPDATE subscriber
         SET freq = $2,
             on_weekday = $3,
             timezone = $4,
             digest_time = $5,
             next_run_at = next_run(
                 GREATEST(now(), COALESCE(last_run_at, now())), $4, $5, $2, $3)
         WHERE id = $1",
    )
    .bind(id)
    .bind(freq)
    .bind(on_weekday)
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

/// Subscribers whose digest is due: the boundary has passed (`next_run_at <= now()`). There is no
/// build gate — projection reads whatever the materialization side has built so far (design §9.4);
/// an event not yet clustered just isn't a candidate this fire and is re-considered on a later one
/// (it's never lost from the durable log, though freshness ranking may leave it unsurfaced).
pub async fn due_subscribers(pool: &PgPool) -> Result<Vec<SubscriberRow>, sqlx::Error> {
    sqlx::query(&format!(
        "SELECT {SELECT_COLS} FROM subscriber WHERE next_run_at <= now() ORDER BY next_run_at"
    ))
    .try_map(row_to_subscriber)
    .fetch_all(pool)
    .await
}

/// Advances the schedule after delivery: `delivered_through` becomes `last_run_at`, and
/// `next_run_at` jumps to the next recurrence boundary **strictly after now** (DST-safe — see the
/// `next_run` SQL function). Jumping past *now* rather than stepping one period is what **coalesces**
/// missed boundaries: after an outage the subscriber fires once and resumes cadence, never a backlog
/// burst. Called only once the digest is delivered, so a crashed run is simply still due next tick
/// (design §9.2–§9.3).
pub async fn advance_after_delivery(
    executor: impl PgExecutor<'_>,
    id: Uuid,
    delivered_through: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE subscriber
         SET last_run_at = $2,
             next_run_at = next_run(now(), timezone, digest_time, freq, on_weekday)
         WHERE id = $1",
    )
    .bind(id)
    .bind(delivered_through)
    .execute(executor)
    .await?;
    Ok(())
}
