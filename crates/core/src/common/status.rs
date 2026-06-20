//! A single-glance snapshot of pipeline state, for `debug status`. Every field is a cheap
//! aggregate over a domain table (plus the apalis queue), so the whole report answers "what's
//! in the system right now, and is anything stuck?" without trawling individual rows.
//!
//! The whole report runs in one **admin** control-plane transaction (the only context in which the
//! fail-closed control-plane tables are readable). Note RLS still applies to the *content* tables
//! (`event`, `cluster`) even under admin — they have no admin backdoor — so `event`/`cluster`
//! aggregates here count the **public** scope only; private rows are counted only in their owner's
//! context, which `status` never assumes. Connection/subscriber/digest counts are global.

use crate::common::db::{begin_scope, ScopeCtx};
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};

#[derive(Debug)]
pub struct StatusReport {
    pub connections: ConnectionStats,
    pub events: EventStats,
    pub build: BuildStatus,
    pub clusters: ClusterStats,
    pub subscribers: SubscriberStats,
    pub digests: DigestStats,
    /// `None` if the apalis schema isn't set up yet (run `migrate` first).
    pub queue: Option<Vec<QueueStats>>,
}

#[derive(Debug)]
pub struct ConnectionStats {
    pub total: i64,
    pub active: i64,
    pub paused: i64,
    pub errored: i64,
    /// Active connections whose `next_poll_at` has passed — overdue to poll.
    pub due_now: i64,
}

#[derive(Debug)]
pub struct EventStats {
    pub total: i64,
    /// Public events ingested after the build watermark — not yet clustered.
    pub unbuilt: i64,
    pub latest_ingest: Option<DateTime<Utc>>,
    pub by_source: Vec<(String, i64)>,
}

#[derive(Debug)]
pub struct BuildStatus {
    pub built_through: DateTime<Utc>,
    /// `now() - built_through` in seconds — how far behind clustering is.
    pub lag_secs: i64,
}

#[derive(Debug)]
pub struct ClusterStats {
    pub total: i64,
    pub latest_updated: Option<DateTime<Utc>>,
}

#[derive(Debug)]
pub struct SubscriberStats {
    pub total: i64,
    pub daily: i64,
    pub weekly: i64,
    /// Subscribers whose `next_run_at` has passed (a digest is owed). With the build gate gone,
    /// this is exactly what the next tick will dispatch.
    pub due_now: i64,
    pub next_run: Option<DateTime<Utc>>,
    /// Total `subscription` rows — subscriber↔source edges across all subscribers. The steady-state
    /// size of the relation that drives digest composition.
    pub subscriptions: i64,
}

#[derive(Debug)]
pub struct DigestStats {
    pub total: i64,
    pub pending: i64,
    pub delivered: i64,
    pub last_delivered: Option<DateTime<Utc>>,
}

/// Per-`job_type` apalis queue counts. `oldest_pending_secs` is the age of the oldest runnable
/// Pending task — a growing value means a backed-up (or stalled) worker.
#[derive(Debug)]
pub struct QueueStats {
    pub job_type: String,
    pub pending: i64,
    pub running: i64,
    pub done: i64,
    pub failed: i64,
    pub killed: i64,
    pub oldest_pending_secs: Option<i64>,
}

/// Gathers the full report in one admin-scoped transaction. Each section is one round-trip; the
/// queue section is skipped (→ `None`) when the apalis schema doesn't exist yet.
pub async fn gather(pool: &sqlx::PgPool) -> Result<StatusReport, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let report = StatusReport {
        connections: connection_stats(&mut tx).await?,
        events: event_stats(&mut tx).await?,
        build: build_status(&mut tx).await?,
        clusters: cluster_stats(&mut tx).await?,
        subscribers: subscriber_stats(&mut tx).await?,
        digests: digest_stats(&mut tx).await?,
        queue: queue_stats(&mut tx).await?,
    };
    tx.commit().await?;
    Ok(report)
}

async fn connection_stats(conn: &mut PgConnection) -> Result<ConnectionStats, sqlx::Error> {
    let row = sqlx::query(
        "SELECT count(*) AS total,
                count(*) FILTER (WHERE status = 'active')  AS active,
                count(*) FILTER (WHERE status = 'paused')  AS paused,
                count(*) FILTER (WHERE status = 'errored') AS errored,
                count(*) FILTER (WHERE status = 'active' AND next_poll_at <= now()) AS due_now
         FROM connection",
    )
    .fetch_one(&mut *conn)
    .await?;
    Ok(ConnectionStats {
        total: row.get("total"),
        active: row.get("active"),
        paused: row.get("paused"),
        errored: row.get("errored"),
        due_now: row.get("due_now"),
    })
}

async fn event_stats(conn: &mut PgConnection) -> Result<EventStats, sqlx::Error> {
    let agg = sqlx::query(
        "SELECT count(*) AS total,
                count(*) FILTER (
                    WHERE scope_kind = 'public'
                      AND ingest_time > (SELECT built_through FROM build_watermark)
                ) AS unbuilt,
                max(ingest_time) AS latest_ingest
         FROM event",
    )
    .fetch_one(&mut *conn)
    .await?;

    let by_source =
        sqlx::query("SELECT source, count(*) AS n FROM event GROUP BY source ORDER BY n DESC")
            .try_map(|row: sqlx::postgres::PgRow| {
                Ok((row.get::<String, _>("source"), row.get::<i64, _>("n")))
            })
            .fetch_all(&mut *conn)
            .await?;

    Ok(EventStats {
        total: agg.get("total"),
        unbuilt: agg.get("unbuilt"),
        latest_ingest: agg.get("latest_ingest"),
        by_source,
    })
}

async fn build_status(conn: &mut PgConnection) -> Result<BuildStatus, sqlx::Error> {
    let row = sqlx::query(
        "SELECT built_through,
                extract(epoch FROM (now() - built_through))::bigint AS lag_secs
         FROM build_watermark",
    )
    .fetch_one(&mut *conn)
    .await?;
    Ok(BuildStatus {
        built_through: row.get("built_through"),
        lag_secs: row.get("lag_secs"),
    })
}

async fn cluster_stats(conn: &mut PgConnection) -> Result<ClusterStats, sqlx::Error> {
    let row =
        sqlx::query("SELECT count(*) AS total, max(updated_at) AS latest_updated FROM cluster")
            .fetch_one(&mut *conn)
            .await?;
    Ok(ClusterStats {
        total: row.get("total"),
        latest_updated: row.get("latest_updated"),
    })
}

async fn subscriber_stats(conn: &mut PgConnection) -> Result<SubscriberStats, sqlx::Error> {
    let row = sqlx::query(
        "SELECT count(*) AS total,
                count(*) FILTER (WHERE freq = 'daily')  AS daily,
                count(*) FILTER (WHERE freq = 'weekly') AS weekly,
                count(*) FILTER (WHERE next_run_at <= now()) AS due_now,
                min(next_run_at) AS next_run,
                (SELECT count(*) FROM subscription) AS subscriptions
         FROM subscriber",
    )
    .fetch_one(&mut *conn)
    .await?;
    Ok(SubscriberStats {
        total: row.get("total"),
        daily: row.get("daily"),
        weekly: row.get("weekly"),
        due_now: row.get("due_now"),
        next_run: row.get("next_run"),
        subscriptions: row.get("subscriptions"),
    })
}

async fn digest_stats(conn: &mut PgConnection) -> Result<DigestStats, sqlx::Error> {
    let row = sqlx::query(
        "SELECT count(*) AS total,
                count(*) FILTER (WHERE delivered_at IS NULL)     AS pending,
                count(*) FILTER (WHERE delivered_at IS NOT NULL) AS delivered,
                max(delivered_at) AS last_delivered
         FROM digest",
    )
    .fetch_one(&mut *conn)
    .await?;
    Ok(DigestStats {
        total: row.get("total"),
        pending: row.get("pending"),
        delivered: row.get("delivered"),
        last_delivered: row.get("last_delivered"),
    })
}

async fn queue_stats(conn: &mut PgConnection) -> Result<Option<Vec<QueueStats>>, sqlx::Error> {
    let exists: bool = sqlx::query("SELECT to_regclass('apalis.jobs') IS NOT NULL AS present")
        .fetch_one(&mut *conn)
        .await?
        .get("present");
    if !exists {
        return Ok(None);
    }

    let rows = sqlx::query(
        "SELECT job_type,
                count(*) FILTER (WHERE status = 'Pending') AS pending,
                count(*) FILTER (WHERE status = 'Running') AS running,
                count(*) FILTER (WHERE status = 'Done')    AS done,
                count(*) FILTER (WHERE status = 'Failed')  AS failed,
                count(*) FILTER (WHERE status = 'Killed')  AS killed,
                extract(epoch FROM (
                    now() - min(run_at) FILTER (WHERE status = 'Pending' AND run_at <= now())
                ))::bigint AS oldest_pending_secs
         FROM apalis.jobs
         GROUP BY job_type
         ORDER BY job_type",
    )
    .try_map(|row: sqlx::postgres::PgRow| {
        Ok(QueueStats {
            job_type: row.get("job_type"),
            pending: row.get("pending"),
            running: row.get("running"),
            done: row.get("done"),
            failed: row.get("failed"),
            killed: row.get("killed"),
            oldest_pending_secs: row.get("oldest_pending_secs"),
        })
    })
    .fetch_all(&mut *conn)
    .await?;
    Ok(Some(rows))
}
