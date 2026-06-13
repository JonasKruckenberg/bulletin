//! The trigger layer: a cron tick (the sole enqueuer) plus three apalis workers that do nothing
//! but call the corresponding `core` flow and translate its outcome into metrics. All durability/
//! dedup lives in the flows' watermarks; apalis just schedules, retries, and distributes.

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

use bulletin_core::cluster::store::unbuilt_public_events_exist;
use bulletin_core::digest::subscriber::due_subscribers;
use bulletin_core::digest::DigestOutcome;
use bulletin_core::ingest::store::due_connections;
use bulletin_core::ingest::PollOutcome;

use crate::metric;
use crate::transport::EmailConfig;

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
/// peeking. A slow or backed-up pipeline shows up directly as large wait/elapsed on the "job
/// complete" line, and `attempt > 1` flags a retry.
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
        metric::job_finished(job, result.is_ok(), elapsed);
        match &result {
            Ok(()) => tracing::info!(wait_ms, elapsed_ms, "job complete"),
            Err(e) => tracing::warn!(wait_ms, elapsed_ms, error = %e, "job failed"),
        }
        result
    }
    .instrument(span)
    .await
}

// ── Job handlers: each is just `flow → metrics` ────────────────────────

async fn poll_connection(
    job: PollConnectionJob,
    task_id: TaskId<Ulid>,
    attempt: Attempt,
    pool: Data<PgPool>,
) -> Result<(), BoxDynError> {
    traced("poll_connection", task_id, attempt, async move {
        match bulletin_core::ingest::poll(&pool, job.connection_id).await {
            Ok(PollOutcome::Polled {
                source,
                inserted,
                deduplicated,
            }) => metric::poll_result(source.as_str(), inserted, deduplicated),
            Ok(PollOutcome::Failed { source }) => metric::poll_failed(source.as_str()),
            Ok(PollOutcome::Skipped) => {}
            Err(e) => return Err(format!("{e:#}").into()),
        }
        Ok(())
    })
    .await
}

async fn public_build(
    _job: PublicBuildJob,
    task_id: TaskId<Ulid>,
    attempt: Attempt,
    pool: Data<PgPool>,
) -> Result<(), BoxDynError> {
    traced("public_build", task_id, attempt, async move {
        match bulletin_core::cluster::build(&pool).await {
            Ok(Some(stats)) => {
                tracing::info!(dirty_groups = stats.dirty_groups, "public build complete")
            }
            Ok(None) => tracing::debug!("public build skipped (lock held by a concurrent build)"),
            Err(e) => {
                tracing::error!(error = %format!("{e:#}"), "public build failed");
                return Err(format!("{e:#}").into());
            }
        }
        Ok(())
    })
    .await
}

async fn generate_digest(
    job: GenerateDigestJob,
    task_id: TaskId<Ulid>,
    attempt: Attempt,
    pool: Data<PgPool>,
    email: Data<EmailConfig>,
) -> Result<(), BoxDynError> {
    traced("generate_digest", task_id, attempt, async move {
        let sender = email.build_sender().map_err(|e| format!("{e:#}"))?;
        let content = email.content();
        match bulletin_core::digest::generate(&pool, &sender, job.subscriber_id, &content).await {
            Ok(outcome) => {
                if matches!(outcome, DigestOutcome::Delivered { .. }) {
                    metric::digest_delivered();
                }
                tracing::info!(subscriber_id = %job.subscriber_id, ?outcome, "digest generated");
            }
            Err(e) => {
                tracing::error!(subscriber_id = %job.subscriber_id, error = %format!("{e:#}"), "digest failed");
                return Err(format!("{e:#}").into());
            }
        }
        Ok(())
    })
    .await
}

// ── Cron tick: the three due-sweeps ────────────────────────────────────

/// A duplicate-enqueue of a `GenerateDigest` for an already-seen `(subscriber, window)` hits
/// apalis's permanent `(job_type, idempotency_key)` unique index — expected, not an error.
fn is_duplicate_enqueue(e: &TaskSinkError<sqlx::Error>) -> bool {
    matches!(e, TaskSinkError::PushError(err)
        if err.as_database_error().is_some_and(|db| db.is_unique_violation()))
}

/// The tick is the sole enqueuer. It reads three "what's due" conditions and pushes work; it
/// advances no watermarks itself (the flows do). Build and digest are **decoupled** (design
/// §3.0/§9.4): the digest sweep does not wait on clustering — projection reads whatever the
/// materialization side has built, and an unbuilt event simply rides the next fire (never lost).
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

    // 3. Subscribers due → GenerateDigest (dedup: apalis idempotency key). No build gate — the
    //    digest reads the latest materialized snapshot; unbuilt events ride the next fire.
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

    // Refresh the Prometheus gauges from the same cheap aggregates `debug status` reads, once per
    // tick — keeps the DB read off the scrape path. A gather failure must not fail the tick.
    match bulletin_core::status::gather(&pool).await {
        Ok(report) => metric::publish_gauges(&report),
        Err(e) => tracing::warn!(error = %e, "status gather for metrics failed"),
    }

    Ok(())
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
                .build(poll_connection)
        })
        .register(move |_| {
            let pool = pool_build.clone();
            let storage: PostgresStorage<PublicBuildJob> = PostgresStorage::new(&pool);
            WorkerBuilder::new("bulletin-public-build")
                .backend(storage)
                .data(pool)
                .build(public_build)
        })
        .register(move |_| {
            let pool = pool_digest.clone();
            let email = email.clone();
            let storage: PostgresStorage<GenerateDigestJob> = PostgresStorage::new(&pool);
            WorkerBuilder::new("bulletin-generate-digest")
                .backend(storage)
                .data(pool)
                .data(email)
                .build(generate_digest)
        })
        .run()
        .await
        .context("worker monitor exited with error")?;

    Ok(())
}
