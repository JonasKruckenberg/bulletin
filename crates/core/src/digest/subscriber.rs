use crate::common::db::{begin_scope, ScopeCtx};
use chrono::{DateTime, NaiveTime, Utc};
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use uuid::Uuid;

// `subscriber` is under RLS (fail-closed): a subscriber context sees only its own row (generate's
// load + the post-delivery schedule advance run there); the tick's due-sweep, `status`, and operator
// commands reach every row only in the **admin** control-plane context. Each function below opens its
// own scoped transaction, so callers need no scope ceremony.

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
            (other, _) => Err(format!(
                "unknown frequency '{other}' (expected daily or weekly)"
            )),
        }
    }

    /// The stored `(freq, on_weekday)` shape — also the encoding the API serializes over the wire, so
    /// it's `pub`: the one place the `(freq, weekday)` mapping lives, rather than a hand-written match
    /// per caller that a new variant could silently desync.
    pub fn columns(self) -> (&'static str, Option<i32>) {
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

/// Defaults a caller takes for a subscriber's schedule when it leaves a field blank. Shared by the CLI
/// (`debug subscriber-add` clap defaults) and the gRPC admin API so the two can't seed different values.
pub const DEFAULT_FREQ: &str = "daily";
pub const DEFAULT_TIMEZONE: &str = "UTC";
pub const DEFAULT_DIGEST_TIME: &str = "09:00";

/// Validated schedule inputs for [`insert_subscriber`], produced by [`validate_schedule`].
#[derive(Debug)]
pub struct Schedule {
    pub recurrence: Recurrence,
    /// The canonical IANA name — validated here, so an unknown zone is a clean up-front error rather
    /// than a deferred database failure inside `next_run()`.
    pub timezone: String,
    pub digest_time: NaiveTime,
}

/// Validate + default the schedule fields — the single parsing path the CLI and the gRPC admin API
/// share. An empty `freq`/`timezone`/`digest_time` takes the default; an empty `freq` *with* a weekday
/// is read as weekly (rather than rejected for "daily takes no weekday"). The timezone is validated up
/// front against the IANA database, closing the gap where a bogus zone surfaced as an opaque 500.
/// Returns a human-readable message on invalid input.
pub fn validate_schedule(
    freq: &str,
    weekday: Option<i32>,
    timezone: &str,
    digest_time: &str,
) -> Result<Schedule, String> {
    let freq = match (freq.is_empty(), weekday.is_some()) {
        (true, true) => "weekly",
        (true, false) => DEFAULT_FREQ,
        (false, _) => freq,
    };
    let recurrence = Recurrence::new(freq, weekday)?;

    let timezone = if timezone.is_empty() {
        DEFAULT_TIMEZONE
    } else {
        timezone
    };
    let timezone: chrono_tz::Tz = timezone
        .parse()
        .map_err(|_| format!("unknown timezone '{timezone}'"))?;

    let digest_time = if digest_time.is_empty() {
        DEFAULT_DIGEST_TIME
    } else {
        digest_time
    };
    let digest_time = NaiveTime::parse_from_str(digest_time, "%H:%M")
        .map_err(|_| "digest_time must be HH:MM (24-hour)".to_string())?;

    Ok(Schedule {
        recurrence,
        timezone: timezone.name().to_string(),
        digest_time,
    })
}

/// The subscriber's selectable columns. Scheduling is a [`Recurrence`] at a local `digest_time` in
/// `timezone` (IANA name) — rather than an offset from signup — so the digest stays at the chosen
/// local time across DST and across travel. The boundary math lives in the SQL `next_run` function.
pub struct SubscriberRow {
    pub id: Uuid,
    pub email: String,
    /// Optional display name used to personalize the digest greeting. `None` (or blank) falls back
    /// to the bare time-of-day salutation.
    pub name: Option<String>,
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
    "id, email, name, freq, on_weekday, max_items, timezone, digest_time, next_run_at, last_run_at";

fn row_to_subscriber(row: PgRow) -> Result<SubscriberRow, sqlx::Error> {
    Ok(SubscriberRow {
        id: row.get("id"),
        email: row.get("email"),
        name: row.get("name"),
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
    name: Option<&str>,
    recurrence: Recurrence,
    timezone: &str,
    digest_time: NaiveTime,
) -> Result<Uuid, sqlx::Error> {
    let (freq, on_weekday) = recurrence.columns();
    // Signup is an operator/control-plane action (admin context); the new id isn't known yet, so a
    // subscriber context couldn't authorize the insert anyway.
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let row = sqlx::query(
        "INSERT INTO subscriber (email, name, freq, on_weekday, timezone, digest_time, next_run_at)
         VALUES ($1, $2, $3, $4, $5, $6, next_run(now(), $5, $6, $3, $4))
         RETURNING id",
    )
    .bind(email)
    .bind(normalize_name(name))
    .bind(freq)
    .bind(on_weekday)
    .bind(timezone)
    .bind(digest_time)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row.get("id"))
}

/// Trims a supplied name and treats a blank one as absent, so a stray `--name ''` (or whitespace)
/// is stored as `NULL` rather than an empty string the greeting would awkwardly splice in.
fn normalize_name(name: Option<&str>) -> Option<String> {
    name.map(str::trim)
        .filter(|n| !n.is_empty())
        .map(str::to_string)
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
    // A self-service edit of one's own row → that subscriber's context.
    let mut tx = begin_scope(pool, ScopeCtx::Subscriber(id)).await?;
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
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() > 0)
}

pub async fn list_subscribers(pool: &PgPool) -> Result<Vec<SubscriberRow>, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let rows = sqlx::query(&format!(
        "SELECT {SELECT_COLS} FROM subscriber ORDER BY next_run_at"
    ))
    .try_map(row_to_subscriber)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows)
}

/// Deletes a subscriber by id, returning whether a row was removed. Their digests cascade
/// (digest.subscriber_id is ON DELETE CASCADE), so this also clears their digest history. An operator
/// action → admin context (the cascade itself bypasses RLS, as Postgres does for all RI actions).
pub async fn delete_subscriber(pool: &PgPool, id: Uuid) -> Result<bool, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let result = sqlx::query("DELETE FROM subscriber WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    let deleted = result.rows_affected() > 0;
    // The delete cascades to the subscriber's connections, subscriptions, digests, stories, and
    // (via the scope-FK) their private clusters/events — the whole private footprint is reclaimed.
    tracing::info!(subscriber_id = %id, deleted, "subscriber deleted (private cache reclaimed)");
    Ok(deleted)
}

/// Loads one subscriber by id — generate's first step, so it runs in *that subscriber's* context
/// (the only one in which the row is visible under RLS, fail-closed otherwise).
pub async fn load_subscriber(
    pool: &PgPool,
    id: Uuid,
) -> Result<Option<SubscriberRow>, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Subscriber(id)).await?;
    let row = sqlx::query(&format!(
        "SELECT {SELECT_COLS} FROM subscriber WHERE id = $1"
    ))
    .bind(id)
    .try_map(row_to_subscriber)
    .fetch_optional(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(row)
}

/// Subscribers whose digest is due: the boundary has passed (`next_run_at <= now()`). There is no
/// build gate — projection reads whatever the materialization side has built so far (design §9.4);
/// an event not yet clustered just isn't a candidate this fire and is re-considered on a later one
/// (it's never lost from the durable log, though freshness ranking may leave it unsurfaced).
pub async fn due_subscribers(pool: &PgPool) -> Result<Vec<SubscriberRow>, sqlx::Error> {
    // The cron sweep enumerates every owner → admin control-plane context.
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let rows = sqlx::query(&format!(
        "SELECT {SELECT_COLS} FROM subscriber WHERE next_run_at <= now() ORDER BY next_run_at"
    ))
    .try_map(row_to_subscriber)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(rows)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_schedule_applies_defaults() {
        let s = validate_schedule("", None, "", "").unwrap();
        assert_eq!(s.recurrence, Recurrence::Daily);
        assert_eq!(s.timezone, "UTC");
        assert_eq!(s.digest_time, NaiveTime::from_hms_opt(9, 0, 0).unwrap());
    }

    #[test]
    fn empty_freq_with_weekday_is_weekly() {
        // The proto "empty ⇒ daily" default must not reject a caller that set only the weekday.
        let s = validate_schedule("", Some(3), "UTC", "08:30").unwrap();
        assert_eq!(s.recurrence, Recurrence::Weekly { weekday: 3 });
        assert_eq!(s.digest_time, NaiveTime::from_hms_opt(8, 30, 0).unwrap());
    }

    #[test]
    fn unknown_timezone_is_rejected_up_front() {
        let err = validate_schedule("daily", None, "Mars/Phobos", "09:00").unwrap_err();
        assert!(err.contains("timezone"), "{err}");
    }

    #[test]
    fn timezone_is_canonicalized() {
        let s = validate_schedule("daily", None, "America/New_York", "09:00").unwrap();
        assert_eq!(s.timezone, "America/New_York");
    }

    #[test]
    fn bad_digest_time_and_bad_freq_are_rejected() {
        assert!(validate_schedule("daily", None, "UTC", "9am").is_err());
        assert!(validate_schedule("monthly", None, "UTC", "09:00").is_err());
        // weekday out of range is rejected by Recurrence::new
        assert!(validate_schedule("weekly", Some(9), "UTC", "09:00").is_err());
    }
}
