//! Every `bulletin_*` metric the pipeline exports, in one place. Call sites in the worker and
//! digest paths go through these thin recorders rather than scattering metric-name string
//! literals across the codebase. `init` installs the Prometheus exporter the `worker` / `all`
//! commands scrape.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use bulletin_core::status::StatusReport;

/// Installs the global Prometheus recorder and starts its own HTTP exporter on `addr` (not an app
/// route — the exporter's listener also drives histogram upkeep). Must run inside the tokio
/// runtime, since it spawns the listener.
pub fn init(addr: SocketAddr) -> Result<()> {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(addr)
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

/// One connection poll: how many events were newly ingested vs collapsed as duplicates.
pub fn poll_result(source: &'static str, inserted: usize, deduplicated: usize) {
    metrics::counter!("bulletin_events_ingested_total", "source" => source)
        .increment(inserted as u64);
    metrics::counter!("bulletin_events_deduplicated_total", "source" => source)
        .increment(deduplicated as u64);
}

/// A connection poll that errored (before backoff is applied).
pub fn poll_failed(source: &'static str) {
    metrics::counter!("bulletin_poll_failures_total", "source" => source).increment(1);
}

/// A digest that was actually sent (not skipped empty / already-delivered).
pub fn digest_delivered() {
    metrics::counter!("bulletin_digests_delivered_total").increment(1);
}

/// Mirrors the `debug status` aggregates into gauges: the "is anything stuck?" watchpoints
/// (unbuilt events, build lag, due work, per-job-type queue depth). Set once per tick; the
/// exporter renders the cached values on scrape, keeping the DB read off the scrape path.
pub fn publish_gauges(r: &StatusReport) {
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
