//! Every `bulletin_*` metric the pipeline exports, in one place. Call sites in the worker and
//! digest paths go through these thin recorders rather than scattering metric-name string
//! literals across the codebase. `init` installs the Prometheus exporter the `worker` / `all`
//! commands scrape.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use bulletin_core::status::StatusReport;
use metrics_exporter_prometheus::Matcher;

/// Job-duration histogram buckets, in seconds: sub-millisecond polls up to multi-minute builds.
/// Without explicit buckets the Prometheus exporter renders `histogram!` as a *summary* (rolling
/// quantiles), which can't be aggregated across instances or fed to `histogram_quantile`. Setting
/// buckets makes `bulletin_job_duration_seconds` a real, aggregatable histogram.
const JOB_DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0,
];

/// Items-per-delivered-digest buckets. Small integer counts; `max_items` is typically single- to
/// low-double-digits, so the tail past ~50 is lumped into `+Inf`.
const DIGEST_ITEMS_BUCKETS: &[f64] = &[0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0];

/// Per-call latency buckets for `bulletin_llm_call_duration_seconds`, in seconds. A local sidecar call
/// runs from tens of milliseconds (cache-warm classify) up to the request timeout (default 120s) on a
/// cold CPU generation, so the bucketing is denser in the 0.1–10s working range and reaches the
/// timeout ceiling. Recorded from `bulletin-core` only in an `llm-summarization` build; harmless to
/// register either way (the matcher simply never fires when nothing emits the metric).
const LLM_CALL_DURATION_BUCKETS: &[f64] = &[
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0, 90.0, 120.0,
];

/// Installs the global Prometheus recorder and starts its own HTTP exporter on `addr` (not an app
/// route — the exporter's listener also drives histogram upkeep). Must run inside the tokio
/// runtime, since it spawns the listener.
pub fn init(addr: SocketAddr) -> Result<()> {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(addr)
        .set_buckets_for_metric(
            Matcher::Full("bulletin_job_duration_seconds".to_string()),
            JOB_DURATION_BUCKETS,
        )
        .context("set job-duration buckets")?
        .set_buckets_for_metric(
            Matcher::Full("bulletin_digest_items".to_string()),
            DIGEST_ITEMS_BUCKETS,
        )
        .context("set digest-items buckets")?
        .set_buckets_for_metric(
            Matcher::Full("bulletin_llm_call_duration_seconds".to_string()),
            LLM_CALL_DURATION_BUCKETS,
        )
        .context("set llm-call-duration buckets")?
        .install()
        .context("install prometheus exporter")?;
    tracing::info!(%addr, "metrics exporter listening");
    Ok(())
}

/// A finished queued job: duration histogram + outcome counter, both keyed by job name. The
/// `wait_ms`/`elapsed_ms` detail goes to the log line; this is the scrape-friendly aggregate.
pub fn job_finished(job: &'static str, ok: bool, elapsed: Duration) {
    metrics::histogram!("bulletin_job_duration_seconds", "job" => job)
        .record(elapsed.as_secs_f64());
    let outcome = if ok { "ok" } else { "err" };
    metrics::counter!("bulletin_jobs_total", "job" => job, "outcome" => outcome).increment(1);
}

/// A job body that ran on a retry (apalis `attempt > 1`). Sustained retries flag a flapping
/// dependency before it tips into an outright failure on the final attempt.
pub fn job_retried(job: &'static str) {
    metrics::counter!("bulletin_job_retries_total", "job" => job).increment(1);
}

/// One ingest pass: how many events were newly appended vs collapsed as duplicates, labelled by
/// `intake` ("poll" | "webhook") so the realtime path and the reconciliation backstop are
/// distinguishable — a webhook path that's gone quiet (everything arriving via "poll") is the signal
/// behind M2's "drop the webhook, the poll still recovers it" guarantee.
pub fn ingest_result(
    source: &'static str,
    intake: &'static str,
    inserted: usize,
    deduplicated: usize,
) {
    metrics::counter!("bulletin_events_ingested_total", "source" => source, "intake" => intake)
        .increment(inserted as u64);
    metrics::counter!("bulletin_events_deduplicated_total", "source" => source, "intake" => intake)
        .increment(deduplicated as u64);
}

/// A connection poll that errored (before backoff is applied).
pub fn poll_failed(source: &'static str) {
    metrics::counter!("bulletin_poll_failures_total", "source" => source).increment(1);
}

/// The terminal outcome of one `generate_digest` run, keyed by variant. `delivered` and `empty`
/// both put an email on the wire (the "all caught up" note is still a delivery); `already_delivered`
/// and `not_yet_due` send nothing. Counting every variant — not just `delivered` — keeps the
/// emails-actually-sent total honest: `delivered + empty`.
pub fn digest_outcome(outcome: &'static str) {
    metrics::counter!("bulletin_digests_total", "outcome" => outcome).increment(1);
}

/// How many items a delivered digest carried — the substance of a send. Empty windows record `0`
/// via the `empty` outcome instead, so this histogram covers only the `delivered` path.
pub fn digest_items(items: usize) {
    metrics::histogram!("bulletin_digest_items").record(items as f64);
}

/// The per-tick `status::gather` that feeds the gauges failed. The tick swallows the error so it
/// doesn't fail the whole tick; this counter makes the resulting gauge staleness visible instead
/// of silent.
pub fn status_gather_failed() {
    metrics::counter!("bulletin_status_gather_failures_total").increment(1);
}

/// Mirrors the `debug status` aggregates into gauges: the "is anything stuck?" watchpoints (unbuilt
/// events, build lag, due work, per-job-type queue depth/age) plus freshness timestamps. Set once
/// per tick; the exporter renders the cached values on scrape, keeping the DB read off the scrape
/// path. Every value here is already computed by `gather`, so this is free instrumentation.
pub fn publish_gauges(r: &StatusReport) {
    // Connections.
    metrics::gauge!("bulletin_connections_active").set(r.connections.active as f64);
    metrics::gauge!("bulletin_connections_errored").set(r.connections.errored as f64);
    metrics::gauge!("bulletin_connections_due").set(r.connections.due_now as f64);

    // Ingest / build freshness.
    metrics::gauge!("bulletin_events_unbuilt").set(r.events.unbuilt as f64);
    metrics::gauge!("bulletin_build_lag_seconds").set(r.build.lag_secs as f64);
    if let Some(ts) = r.events.latest_ingest {
        metrics::gauge!("bulletin_last_ingest_timestamp_seconds").set(ts.timestamp() as f64);
    }

    // Clusters.
    metrics::gauge!("bulletin_clusters_total").set(r.clusters.total as f64);

    // Subscribers (cadence breakdown lets you see who's owed which schedule).
    metrics::gauge!("bulletin_subscribers", "freq" => "daily").set(r.subscribers.daily as f64);
    metrics::gauge!("bulletin_subscribers", "freq" => "weekly").set(r.subscribers.weekly as f64);
    metrics::gauge!("bulletin_subscribers_due").set(r.subscribers.due_now as f64);

    // Digests (delivery backlog + last-delivery freshness).
    metrics::gauge!("bulletin_digests_pending").set(r.digests.pending as f64);
    if let Some(ts) = r.digests.last_delivered {
        metrics::gauge!("bulletin_last_delivered_timestamp_seconds").set(ts.timestamp() as f64);
    }

    // Queue: depth, in-flight, failures, and the oldest runnable Pending's age — the truest
    // "stuck worker" signal (a small queue that never drains shows up here, not in depth).
    if let Some(queue) = &r.queue {
        for q in queue {
            metrics::gauge!("bulletin_queue_depth", "job_type" => q.job_type.clone())
                .set(q.pending as f64);
            metrics::gauge!("bulletin_queue_running", "job_type" => q.job_type.clone())
                .set(q.running as f64);
            metrics::gauge!("bulletin_queue_failed", "job_type" => q.job_type.clone())
                .set(q.failed as f64);
            metrics::gauge!("bulletin_queue_killed", "job_type" => q.job_type.clone())
                .set(q.killed as f64);
            let oldest = q.oldest_pending_secs.unwrap_or(0);
            metrics::gauge!("bulletin_queue_oldest_pending_seconds", "job_type" => q.job_type.clone())
                .set(oldest as f64);
        }
    }
}
