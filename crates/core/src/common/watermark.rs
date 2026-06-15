//! Per-subscriber `built_through` watermark helpers, shared by the private-build cursor and the
//! thread-maintenance cursor. Each backing table is `(subscriber_id PK, built_through, [ran_at])`,
//! and the read default (epoch) + monotonic-`GREATEST` advance convention live here in one place
//! rather than copied per store.

use chrono::{DateTime, Utc};
use sqlx::{PgExecutor, Row};
use uuid::Uuid;

/// Reads a subscriber's `built_through`, defaulting a missing row to the epoch so the first pass
/// folds in all prior history. `table` is a trusted `&'static str` (the store's own table name),
/// never caller input — it is interpolated into the SQL.
pub async fn read_through(
    executor: impl PgExecutor<'_>,
    table: &'static str,
    subscriber_id: Uuid,
) -> Result<DateTime<Utc>, sqlx::Error> {
    let sql = format!(
        "SELECT coalesce(
                  (SELECT built_through FROM {table} WHERE subscriber_id = $1),
                  'epoch'::timestamptz) AS built_through"
    );
    let row = sqlx::query(&sql)
        .bind(subscriber_id)
        .fetch_one(executor)
        .await?;
    Ok(row.get("built_through"))
}

/// Advances a subscriber's watermark to `through` (monotonic via `GREATEST`), creating the row on
/// first write. When `stamp_ran_at`, also sets `ran_at = now()` — the due-query clock the
/// maintenance cadence reads. `table` is a trusted `&'static str`, never caller input.
pub async fn advance(
    executor: impl PgExecutor<'_>,
    table: &'static str,
    subscriber_id: Uuid,
    through: DateTime<Utc>,
    stamp_ran_at: bool,
) -> Result<(), sqlx::Error> {
    let sql = if stamp_ran_at {
        format!(
            "INSERT INTO {table} (subscriber_id, built_through, ran_at)
             VALUES ($1, $2, now())
             ON CONFLICT (subscriber_id) DO UPDATE SET
                built_through = GREATEST({table}.built_through, EXCLUDED.built_through),
                ran_at = now()"
        )
    } else {
        format!(
            "INSERT INTO {table} (subscriber_id, built_through)
             VALUES ($1, $2)
             ON CONFLICT (subscriber_id) DO UPDATE
                SET built_through = GREATEST({table}.built_through, EXCLUDED.built_through)"
        )
    };
    sqlx::query(&sql)
        .bind(subscriber_id)
        .bind(through)
        .execute(executor)
        .await?;
    Ok(())
}
