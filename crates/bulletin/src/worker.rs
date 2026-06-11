use anyhow::{Context, Result};
use apalis::prelude::*;
use apalis_cron::{CronStream, Tick};
use apalis_postgres::PostgresStorage;
use chrono::Utc;
use cron::Schedule;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::str::FromStr;
use std::time::Instant;
use tracing::Instrument;
use ulid::Ulid;
use uuid::Uuid;

use bulletin_connectors::rss::RssConnection;
use bulletin_core::{connector::Connection, kind::SourceKind, scope::Scope};
use bulletin_store::{
    cluster::unbuilt_public_events_exist,
    connection::{advance_cursor, due_connections, load_connection, record_failure, ConnectionRow},
    event::insert_event,
    subscriber::due_subscribers,
};

use crate::email::EmailConfig;

pub async fn setup_storage(pool: &PgPool) -> Result<()> {
    let mut m = PostgresStorage::<(), (), ()>::migrations();
    m.ignore_missing = true;
    m.run(pool).await.context("apalis migrations failed")?;
    Ok(())
}

// ── Job payloads ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollConnectionJob {
    pub connection_id: Uuid,
}

/// PublicBuild carries no payload — it always processes "everything new since the watermark".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicBuildJob;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateDigestJob {
    pub subscriber_id: Uuid,
}

// ── Job instrumentation ────────────────────────────────────────────────

/// Wraps a queued-job body in a stable span + timing. The apalis `TaskId` is a ULID, so it is
/// both a stable correlation id (stamped on every log line for the task) and an embedded enqueue
/// clock — `wait_ms` (enqueue → pickup) and `elapsed_ms` (run time) come for free, no backend
/// peeking. A slow or backed-up pipeline then shows up directly as large wait/elapsed on the
/// "job complete" line, and `attempt > 1` flags a retry.
async fn traced(
    job: &'static str,
    task_id: TaskId<Ulid>,
    attempt: Attempt,
    fut: impl std::future::Future<Output = Result<(), BoxDynError>>,
) -> Result<(), BoxDynError> {
    let enqueued_ms = task_id.inner().timestamp_ms() as i64;
    let span = tracing::info_span!("job", job, task_id = %task_id, attempt = attempt.current());
    async move {
        let wait_ms = (Utc::now().timestamp_millis() - enqueued_ms).max(0);
        let started = Instant::now();
        let result = fut.await;
        let elapsed = started.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;
        let outcome = if result.is_ok() { "ok" } else { "err" };
        metrics::histogram!("bulletin_job_duration_seconds", "job" => job)
            .record(elapsed.as_secs_f64());
        metrics::counter!("bulletin_jobs_total", "job" => job, "outcome" => outcome).increment(1);
        match &result {
            Ok(()) => tracing::info!(wait_ms, elapsed_ms, "job complete"),
            Err(e) => tracing::warn!(wait_ms, elapsed_ms, error = %e, "job failed"),
        }
        result
    }
    .instrument(span)
    .await
}

// ── PollConnection ─────────────────────────────────────────────────────

/// M1 dispatch: RSS only. Becomes a full enum when GitHub lands in M2.
enum ConnDispatch {
    Rss(RssConnection),
}

#[derive(Deserialize)]
struct RssConfig {
    url: String,
}

fn build_dispatch(row: &ConnectionRow) -> Result<ConnDispatch, Box<dyn std::error::Error>> {
    match row.source {
        SourceKind::Rss => {
            let cfg: RssConfig = serde_json::from_value(row.config.clone())?;
            Ok(ConnDispatch::Rss(RssConnection::new(cfg.url)))
        }
        _ => Err(format!("unsupported source {:?} in M1", row.source).into()),
    }
}

async fn handle_poll_connection(
    job: PollConnectionJob,
    task_id: TaskId<Ulid>,
    attempt: Attempt,
    pool: Data<PgPool>,
) -> Result<(), BoxDynError> {
    traced(
        "poll_connection",
        task_id,
        attempt,
        poll_connection(job, pool),
    )
    .await
}

async fn poll_connection(job: PollConnectionJob, pool: Data<PgPool>) -> Result<(), BoxDynError> {
    let conn_row = match load_connection(&pool, job.connection_id).await? {
        Some(r) => r,
        None => {
            tracing::warn!(connection_id = %job.connection_id, "connection not found");
            return Ok(());
        }
    };

    if conn_row.status != "active" {
        tracing::debug!(connection_id = %job.connection_id, status = %conn_row.status, "skipping non-active connection");
        return Ok(());
    }

    let dispatch = match build_dispatch(&conn_row) {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(connection_id = %job.connection_id, error = %e, "build_dispatch failed");
            return Ok(());
        }
    };

    // Cursor/creds erase to serde_json::Value at the arm boundary; typed within each arm.
    let result = match dispatch {
        ConnDispatch::Rss(conn) => {
            let cursor = conn_row
                .cursor
                .clone()
                .map(|v| serde_json::from_value(v).unwrap_or_default())
                .unwrap_or_default();
            conn.poll(cursor).await.map(|b| {
                let builders = b
                    .items
                    .into_iter()
                    .flat_map(|item| conn.to_events(item))
                    .collect::<Vec<_>>();
                let new_cursor =
                    serde_json::to_value(&b.cursor).expect("RssCursor always serializes");
                (builders, new_cursor)
            })
        }
    };

    match result {
        Ok((builders, new_cursor)) => {
            let total = builders.len();
            let mut inserted = 0usize;
            // Events committed before cursor advance — crash-safety invariant (arch §3).
            for builder in builders {
                let ev = builder.finalize(Scope::Public);
                if insert_event(&pool, &ev).await?.is_some() {
                    inserted += 1;
                }
            }
            let source = conn_row.source.as_str();
            metrics::counter!("bulletin_events_ingested_total", "source" => source)
                .increment(inserted as u64);
            metrics::counter!("bulletin_events_deduplicated_total", "source" => source)
                .increment((total - inserted) as u64);
            tracing::info!(
                connection_id = %conn_row.id,
                source,
                inserted,
                deduplicated = total - inserted,
                "poll complete"
            );
            advance_cursor(&pool, conn_row.id, new_cursor).await?;
        }
        Err(e) => {
            metrics::counter!("bulletin_poll_failures_total", "source" => conn_row.source.as_str())
                .increment(1);
            tracing::warn!(connection_id = %job.connection_id, error = %e, "poll failed");
            record_failure(&pool, conn_row.id).await?;
        }
    }

    Ok(())
}

// ── PublicBuild ────────────────────────────────────────────────────────

async fn handle_public_build(
    job: PublicBuildJob,
    task_id: TaskId<Ulid>,
    attempt: Attempt,
    pool: Data<PgPool>,
) -> Result<(), BoxDynError> {
    traced("public_build", task_id, attempt, public_build(job, pool)).await
}

async fn public_build(_: PublicBuildJob, pool: Data<PgPool>) -> Result<(), BoxDynError> {
    match crate::build::run(&pool).await {
        Ok(Some(stats)) => {
            tracing::info!(dirty_groups = stats.dirty_groups, "public build complete");
        }
        Ok(None) => tracing::debug!("public build skipped (lock held by a concurrent build)"),
        Err(e) => {
            tracing::error!(error = %format!("{e:#}"), "public build failed");
            return Err(format!("{e:#}").into());
        }
    }
    Ok(())
}

// ── GenerateDigest ─────────────────────────────────────────────────────

async fn handle_generate_digest(
    job: GenerateDigestJob,
    task_id: TaskId<Ulid>,
    attempt: Attempt,
    pool: Data<PgPool>,
    email: Data<EmailConfig>,
) -> Result<(), BoxDynError> {
    traced(
        "generate_digest",
        task_id,
        attempt,
        generate_digest(job, pool, email),
    )
    .await
}

async fn generate_digest(
    job: GenerateDigestJob,
    pool: Data<PgPool>,
    email: Data<EmailConfig>,
) -> Result<(), BoxDynError> {
    match crate::digest::run(&pool, &email, job.subscriber_id).await {
        Ok(outcome) => {
            tracing::info!(subscriber_id = %job.subscriber_id, ?outcome, "digest generated");
        }
        Err(e) => {
            tracing::error!(subscriber_id = %job.subscriber_id, error = %format!("{e:#}"), "digest failed");
            return Err(format!("{e:#}").into());
        }
    }
    Ok(())
}

// ── Cron tick: the three due-sweeps ────────────────────────────────────

/// A duplicate-enqueue of a `GenerateDigest` for an already-seen `(subscriber, window)` hits
/// apalis's permanent `(job_type, idempotency_key)` unique index — expected, not an error.
fn is_duplicate_enqueue(e: &TaskSinkError<sqlx::Error>) -> bool {
    matches!(e, TaskSinkError::PushError(err)
        if err.as_database_error().is_some_and(|db| db.is_unique_violation()))
}

/// The tick is the sole enqueuer. It reads three "what's due" conditions and pushes work;
/// it advances no watermarks itself (the processing jobs do). The PublicBuild → GenerateDigest
/// dependency is honored as a *data precondition* of the digest sweep (`due_subscribers` only
/// returns a subscriber once every public event before its boundary is built), so no job needs
/// to chain another.
async fn handle_tick(_: Tick<Utc>, pool: Data<PgPool>) -> Result<(), BoxDynError> {
    let span = tracing::info_span!("tick");
    async move {
        let started = Instant::now();
        let result = run_tick(pool).await;
        let elapsed_ms = started.elapsed().as_millis() as u64;
        match &result {
            Ok(()) => tracing::debug!(elapsed_ms, "tick complete"),
            Err(e) => tracing::warn!(elapsed_ms, error = %e, "tick failed"),
        }
        result
    }
    .instrument(span)
    .await
}

async fn run_tick(pool: Data<PgPool>) -> Result<(), BoxDynError> {
    // 1. Connections due to poll → PollConnection (dedup: next_poll_at watermark).
    let due = due_connections(&pool).await?;
    if !due.is_empty() {
        tracing::info!(count = due.len(), "tick: dispatching due connections");
        let mut storage: PostgresStorage<PollConnectionJob> = PostgresStorage::new(&pool);
        for row in due {
            storage
                .push(PollConnectionJob {
                    connection_id: row.id,
                })
                .await?;
        }
    }

    // 2. New public events to cluster → PublicBuild (dedup: watermark gate + advisory lock).
    if unbuilt_public_events_exist(&*pool).await? {
        tracing::debug!("tick: enqueuing public build");
        let mut storage: PostgresStorage<PublicBuildJob> = PostgresStorage::new(&pool);
        storage.push(PublicBuildJob).await?;
    }

    // 3. Subscribers due *and* fully built → GenerateDigest (dedup: apalis idempotency key).
    let subs = due_subscribers(&pool).await?;
    if !subs.is_empty() {
        tracing::info!(count = subs.len(), "tick: dispatching due digests");
        let mut storage: PostgresStorage<GenerateDigestJob> = PostgresStorage::new(&pool);
        for s in subs {
            // window_end = next_run_at boundary; once-per-window-ever key.
            let key = format!("digest:{}:{}", s.id, s.next_run_at.timestamp());
            let task = TaskBuilder::new(GenerateDigestJob {
                subscriber_id: s.id,
            })
            .with_idempotency_key(key)
            .build();
            match storage.push_task(task).await {
                Ok(()) => {}
                Err(e) if is_duplicate_enqueue(&e) => {
                    tracing::debug!(subscriber_id = %s.id, "digest already enqueued for this window");
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    // Publish gauge snapshots for Prometheus. These are cheap aggregates already computed for
    // `debug status`; refreshing them once per tick keeps the DB read off the metrics scrape path.
    // A gather failure must not fail the tick.
    match bulletin_store::status::gather(&pool).await {
        Ok(report) => publish_gauges(&report),
        Err(e) => tracing::warn!(error = %e, "status gather for metrics failed"),
    }

    Ok(())
}

/// Mirrors the `debug status` aggregates into Prometheus gauges: the "is anything stuck?"
/// watchpoints (unbuilt events, build lag, due work, per-job-type queue depth). Set on the tick;
/// the exporter renders the cached values on scrape.
fn publish_gauges(r: &bulletin_store::status::StatusReport) {
    metrics::gauge!("bulletin_connections_active").set(r.connections.active as f64);
    metrics::gauge!("bulletin_events_unbuilt").set(r.events.unbuilt as f64);
    metrics::gauge!("bulletin_build_lag_seconds").set(r.build.lag_secs as f64);
    metrics::gauge!("bulletin_subscribers_due").set(r.subscribers.due_now as f64);
    if let Some(queue) = &r.queue {
        for q in queue {
            metrics::gauge!("bulletin_queue_depth", "job_type" => q.job_type.clone())
                .set(q.pending as f64);
        }
    }
}

// ── Monitor wiring ─────────────────────────────────────────────────────

pub async fn start(pool: PgPool, email: EmailConfig) -> Result<()> {
    setup_storage(&pool).await?;

    // One local-clock cron drives all three sweeps; duplicate ticks across replicas are
    // harmless because each sweep is watermark-gated (and the digest sweep also idempotency-keyed).
    let schedule = Schedule::from_str("0 * * * * *").context("invalid cron expression")?;

    let pool_tick = pool.clone();
    let pool_poll = pool.clone();
    let pool_build = pool.clone();
    let pool_digest = pool.clone();

    Monitor::new()
        .register(move |_| {
            let pool = pool_tick.clone();
            WorkerBuilder::new("bulletin-tick")
                .backend(CronStream::new(schedule.clone()))
                .data(pool)
                .build(handle_tick)
        })
        .register(move |_| {
            let pool = pool_poll.clone();
            let storage: PostgresStorage<PollConnectionJob> = PostgresStorage::new(&pool);
            WorkerBuilder::new("bulletin-poll-connection")
                .backend(storage)
                .data(pool)
                .build(handle_poll_connection)
        })
        .register(move |_| {
            let pool = pool_build.clone();
            let storage: PostgresStorage<PublicBuildJob> = PostgresStorage::new(&pool);
            WorkerBuilder::new("bulletin-public-build")
                .backend(storage)
                .data(pool)
                .build(handle_public_build)
        })
        .register(move |_| {
            let pool = pool_digest.clone();
            let email = email.clone();
            let storage: PostgresStorage<GenerateDigestJob> = PostgresStorage::new(&pool);
            WorkerBuilder::new("bulletin-generate-digest")
                .backend(storage)
                .data(pool)
                .data(email)
                .build(handle_generate_digest)
        })
        .run()
        .await
        .context("worker monitor exited with error")?;

    Ok(())
}
