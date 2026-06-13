# Grafana — Bulletin

`bulletin-overview.json` is an importable dashboard for the `bulletin_*` Prometheus series
exported on `metrics.addr` (default `127.0.0.1:9464`).

## Import

*Dashboards → New → Import* → paste `bulletin-overview.json` → select your Prometheus
datasource (the dashboard exposes a `datasource` variable, so it isn't pinned to a specific UID).

## Layout

- **Health / SLO** — emails actually sent (24h), job error ratio, subscribers due, build lag,
  connection health.
- **Jobs** — throughput, outcomes, duration p50/p95/p99 (real histogram quantiles), retries.
- **Queue / backlog** — depth, oldest-pending age (the real "stuck worker" signal), failures,
  unbuilt events.
- **Ingestion** — ingest rate, dedup ratio, poll failures, time-since-last-ingest.
- **Digests / delivery** — outcomes, items per digest, pending, time-since-last-delivery,
  cadence mix.

## Notes on the metrics

- **Emails sent = `delivered + empty`.** An empty window still sends an "all caught up" email and
  is counted under `bulletin_digests_total{outcome="empty"}`. Don't use a delivered-only series
  for delivery SLOs — it undercounts.
- **`bulletin_build_lag_seconds` is a content-freshness signal, not a delivery blocker.** Build
  and digest are decoupled — a digest reads the latest materialized snapshot and unbuilt events
  ride the next tick. Page on delivery/queue/freshness, not on build lag.
- **Gauges refresh once per minute** (the cron tick calls `status::gather`). Range/rate windows
  should be ≥1m. If `bulletin_status_gather_failures_total` is climbing, the gauges are stale.

## Suggested alerts

| Alert | Expression | For |
|---|---|---|
| Digest worker stalled | `bulletin_subscribers_due > 0` | 15m |
| Queue stuck | `max(bulletin_queue_oldest_pending_seconds) > 600` | 10m |
| Job error rate high | `sum(rate(bulletin_jobs_total{outcome="err"}[10m])) / clamp_min(sum(rate(bulletin_jobs_total[10m])), 1) > 0.1` | 15m |
| No deliveries | `time() - max(bulletin_last_delivered_timestamp_seconds) > 172800` | 0m |
| Ingestion stalled | `time() - max(bulletin_last_ingest_timestamp_seconds) > 86400` | 0m |
| Connections errored | `bulletin_connections_errored > 0` | 30m |
| Metrics going stale | `increase(bulletin_status_gather_failures_total[15m]) > 0` | 0m |

Tune the "no deliveries" / "ingestion stalled" thresholds to your slowest cadence (weekly digests,
slow feeds).
