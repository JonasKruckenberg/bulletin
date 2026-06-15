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
use bulletin_core::ingest::{ConnectorCtx, PollOutcome, WebhookOutcome};
use bulletin_core::kind::SourceKind;

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

/// thread_maintenance for one subscriber (design `docs/thread-layer.md` §5.1): the write-side,
/// best-effort job that rebuilds the subscriber's identity graph + threads and projects the
/// entity-weight map. Coalesced to a relaxed cadence by an hourly idempotency key, and never on the
/// punctual digest path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadMaintenanceJob {
    pub subscriber_id: Uuid,
}

/// A verified webhook delivery, taken off the HTTP edge for off-request-path processing. Carries the
/// raw body plus the two header values the body itself doesn't hold: the activity `event_type` and
/// the `delivery_id` (also the enqueue idempotency key — GitHub retries on a non-2xx). `body` is the
/// JSON text, not `Vec<u8>`: apalis stores job args as JSON, where a byte vec balloons into an
/// integer array — and a verified GitHub delivery is UTF-8 JSON anyway.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessWebhookJob {
    pub source: SourceKind,
    pub event_type: String,
    pub delivery_id: String,
    pub body: String,
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
    if attempt.current() > 1 {
        metric::job_retried(job);
    }
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

/// Flattens an `anyhow` error chain into the `BoxDynError` apalis wants from a failed job.
fn boxed(e: anyhow::Error) -> BoxDynError {
    format!("{e:#}").into()
}

// ── Job handlers: each is just `flow → metrics` ────────────────────────

async fn poll_connection(
    job: PollConnectionJob,
    task_id: TaskId<Ulid>,
    attempt: Attempt,
    pool: Data<PgPool>,
    ctx: Data<ConnectorCtx>,
) -> Result<(), BoxDynError> {
    traced("poll_connection", task_id, attempt, async move {
        match bulletin_core::ingest::poll(&pool, job.connection_id, &ctx).await {
            Ok(PollOutcome::Polled {
                source,
                inserted,
                deduplicated,
            }) => metric::ingest_result(source.as_str(), "poll", inserted, deduplicated),
            Ok(PollOutcome::Failed { source }) => metric::poll_failed(source.as_str()),
            Ok(PollOutcome::Skipped) => {}
            Err(e) => return Err(boxed(e)),
        }
        Ok(())
    })
    .await
}

/// Process one verified webhook delivery: resolve our connection, normalize, append (or apply a
/// lifecycle status change). The ingest counters carry `intake = "webhook"` so the realtime path is
/// distinguishable from the poll backstop — the health signal behind M2's "drop the webhook, the
/// poll still recovers it" guarantee.
async fn process_webhook(
    job: ProcessWebhookJob,
    task_id: TaskId<Ulid>,
    attempt: Attempt,
    pool: Data<PgPool>,
) -> Result<(), BoxDynError> {
    traced("process_webhook", task_id, attempt, async move {
        match bulletin_core::ingest::process_webhook(
            &pool,
            job.source,
            &job.event_type,
            &job.delivery_id,
            job.body.as_bytes(),
        )
        .await
        {
            Ok(WebhookOutcome::Ingested {
                source,
                inserted,
                deduplicated,
            }) => metric::ingest_result(source.as_str(), "webhook", inserted, deduplicated),
            Ok(WebhookOutcome::Lifecycle { .. })
            | Ok(WebhookOutcome::Unrouted { .. })
            | Ok(WebhookOutcome::Skipped) => {}
            Err(e) => return Err(boxed(e)),
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
                return Err(boxed(e));
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
        let sender = email.build_sender().map_err(boxed)?;
        let content = email.content();
        match bulletin_core::digest::generate(&pool, &sender, job.subscriber_id, &content).await {
            Ok(outcome) => {
                // `delivered` and `empty` both sent an email; `already_delivered`/`not_yet_due`
                // sent nothing. Record every variant so the counter doesn't undercount real sends.
                match outcome {
                    DigestOutcome::Delivered { items } => {
                        metric::digest_outcome("delivered");
                        metric::digest_items(items);
                    }
                    DigestOutcome::Empty => metric::digest_outcome("empty"),
                    DigestOutcome::AlreadyDelivered => metric::digest_outcome("already_delivered"),
                    DigestOutcome::NotYetDue => metric::digest_outcome("not_yet_due"),
                }
                tracing::info!(subscriber_id = %job.subscriber_id, ?outcome, "digest generated");
            }
            Err(e) => {
                tracing::error!(subscriber_id = %job.subscriber_id, error = %format!("{e:#}"), "digest failed");
                return Err(boxed(e));
            }
        }
        Ok(())
    })
    .await
}

/// Run one subscriber's thread_maintenance pass. Best-effort by contract: a failure is logged and
/// surfaced as a failed job (so apalis retries), but it never blocks or corrupts a digest — the
/// prior thread state simply stands until the next pass succeeds.
async fn thread_maintenance(
    job: ThreadMaintenanceJob,
    task_id: TaskId<Ulid>,
    attempt: Attempt,
    pool: Data<PgPool>,
) -> Result<(), BoxDynError> {
    traced("thread_maintenance", task_id, attempt, async move {
        let cfg = bulletin_core::thread::MaintenanceConfig::default();
        match bulletin_core::thread::maintain(&pool, job.subscriber_id, Utc::now(), &cfg).await {
            Ok(stats) => tracing::info!(
                subscriber_id = %job.subscriber_id,
                sources = stats.sources,
                entities = stats.entities,
                communities = stats.communities,
                threads = stats.threads_written,
                weighted_entities = stats.weighted_entities,
                "thread maintenance complete"
            ),
            Err(e) => return Err(boxed(e)),
        }
        Ok(())
    })
    .await
}

/// How often a subscriber's thread-maintenance pass runs (a relaxed, off-path cadence).
#[cfg(feature = "thread-weighting")]
const MAINTENANCE_CADENCE_HOURS: i64 = 1;

/// Enqueue a thread-maintenance pass for each subscriber **due** for one (last run older than the
/// cadence), coalesced by an hourly idempotency key so re-ticks before the pass runs collapse. Only
/// the small due set is touched — not the whole subscriber table every minute. A no-op build without
/// the `thread-weighting` feature.
#[cfg(feature = "thread-weighting")]
async fn enqueue_due_maintenance(pool: &PgPool) -> Result<(), BoxDynError> {
    let cadence = chrono::Duration::hours(MAINTENANCE_CADENCE_HOURS);
    let due = bulletin_core::thread::store::due_for_maintenance(pool, cadence).await?;
    if due.is_empty() {
        return Ok(());
    }
    let mut storage: PostgresStorage<ThreadMaintenanceJob> = PostgresStorage::new(pool);
    let bucket = Utc::now().timestamp() / (MAINTENANCE_CADENCE_HOURS * 3600);
    for subscriber_id in due {
        let task = TaskBuilder::new(ThreadMaintenanceJob { subscriber_id })
            .with_idempotency_key(format!("thread_maint:{subscriber_id}:{bucket}"))
            .build();
        match storage.push_task(task).await {
            Ok(()) => {}
            Err(e) if is_duplicate_enqueue(&e) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[cfg(not(feature = "thread-weighting"))]
async fn enqueue_due_maintenance(_: &PgPool) -> Result<(), BoxDynError> {
    Ok(())
}

// ── Cron tick: the three due-sweeps ────────────────────────────────────

/// A duplicate-enqueue of a `GenerateDigest` for an already-seen `(subscriber, window)` hits
/// apalis's permanent `(job_type, idempotency_key)` unique index — expected, not an error.
pub(crate) fn is_duplicate_enqueue(e: &TaskSinkError<sqlx::Error>) -> bool {
    matches!(e, TaskSinkError::PushError(err)
        if err.as_database_error().is_some_and(|db| db.is_unique_violation()))
}

/// The tick is the sole enqueuer. It reads three "what's due" conditions and pushes work; it
/// advances no watermarks itself (the flows do). Build and digest are **decoupled** (design
/// §3.0/§9.4): the digest sweep does not wait on clustering — projection reads whatever the
/// materialization side has built, and an unbuilt event rides a later fire (never lost from the log).
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

    // 4. Thread maintenance (write-side, off the punctual path) — only the subscribers actually due
    //    for a pass (a watermark-gated due query, like the digest sweep), not a full scan every tick.
    //    Compiled out entirely without the `thread-weighting` feature.
    enqueue_due_maintenance(&pool).await?;

    // Refresh the Prometheus gauges from the same cheap aggregates `debug status` reads, once per
    // tick — keeps the DB read off the scrape path. A gather failure must not fail the tick.
    match bulletin_core::status::gather(&pool).await {
        Ok(report) => metric::publish_gauges(&report),
        Err(e) => {
            metric::status_gather_failed();
            tracing::warn!(error = %e, "status gather for metrics failed");
        }
    }

    Ok(())
}

// ── Monitor wiring ─────────────────────────────────────────────────────

pub async fn start(pool: PgPool, email: EmailConfig, connectors: ConnectorCtx) -> Result<()> {
    setup_storage(&pool).await?;

    // One local-clock cron drives all three sweeps; duplicate ticks across replicas are
    // harmless because each sweep is watermark-gated (and the digest sweep also idempotency-keyed).
    let schedule = Schedule::from_str("0 * * * * *").context("invalid cron expression")?;

    // Each `register` factory owns its own `PgPool` handle (a cheap Arc clone); cloning in the
    // capture block keeps the clone next to its use instead of a ladder of pre-named bindings.
    Monitor::new()
        .register({
            let pool = pool.clone();
            move |_| {
                WorkerBuilder::new("bulletin-tick")
                    .backend(CronStream::new(schedule.clone()))
                    .data(pool.clone())
                    .build(handle_tick)
            }
        })
        .register({
            let pool = pool.clone();
            move |_| {
                WorkerBuilder::new("bulletin-poll-connection")
                    .backend(PostgresStorage::<PollConnectionJob>::new(&pool))
                    .data(pool.clone())
                    .data(connectors.clone())
                    .build(poll_connection)
            }
        })
        .register({
            let pool = pool.clone();
            move |_| {
                WorkerBuilder::new("bulletin-public-build")
                    .backend(PostgresStorage::<PublicBuildJob>::new(&pool))
                    .data(pool.clone())
                    .build(public_build)
            }
        })
        .register({
            let pool = pool.clone();
            move |_| {
                WorkerBuilder::new("bulletin-generate-digest")
                    .backend(PostgresStorage::<GenerateDigestJob>::new(&pool))
                    .data(pool.clone())
                    .data(email.clone())
                    .build(generate_digest)
            }
        })
        .register({
            let pool = pool.clone();
            move |_| {
                WorkerBuilder::new("bulletin-process-webhook")
                    .backend(PostgresStorage::<ProcessWebhookJob>::new(&pool))
                    .data(pool.clone())
                    .build(process_webhook)
            }
        })
        .register({
            let pool = pool.clone();
            move |_| {
                WorkerBuilder::new("bulletin-thread-maintenance")
                    .backend(PostgresStorage::<ThreadMaintenanceJob>::new(&pool))
                    .data(pool.clone())
                    .build(thread_maintenance)
            }
        })
        .run()
        .await
        .context("worker monitor exited with error")?;

    Ok(())
}
