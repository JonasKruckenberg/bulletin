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
- **Ingestion** — ingest rate (split by `intake`: poll backstop vs realtime webhook), dedup ratio,
  poll failures, time-since-last-ingest.
- **Digests / delivery** — outcomes, items per digest, pending, time-since-last-delivery,
  cadence mix.
- **LLM summarization** — per-call latency (p50/p95/p99 by phase), call outcomes, token
  throughput, faithfulness-gate rejections, and content-hash cache hit ratio. Empty unless the
  binary is built with `--features llm-summarization`; the model edge (and these metrics) compile
  out by default.

## Notes on the metrics

- **Emails sent = `delivered + empty`.** An empty window still sends an "all caught up" email and
  is counted under `bulletin_digests_total{outcome="empty"}`. Don't use a delivered-only series
  for delivery SLOs — it undercounts.
- **`bulletin_build_lag_seconds` is a content-freshness signal, not a delivery blocker.** Build
  and digest are decoupled — a digest reads the latest materialized snapshot and unbuilt events
  ride the next tick. Page on delivery/queue/freshness, not on build lag.
- **Gauges refresh once per minute** (the cron tick calls `status::gather`). Range/rate windows
  should be ≥1m. If `bulletin_status_gather_failures_total` is climbing, the gauges are stale.
- **The `bulletin_llm_*` series only exist in a `llm-summarization` build.** They are recorded from
  `bulletin-core` at the single `chat_json` choke point all five `phase`s route through
  (`summarize` | `comprehend` | `synthesize` | `label` | `delta`). `bulletin_llm_call_duration_seconds`
  is keyed on `phase` only (so a fast `connect` failure doesn't skew the latency distribution); the
  success/error split lives on `bulletin_llm_calls_total{outcome}`, whose `outcome` reuses the same
  buckets as the structured logs (`ok` | `timeout` | `connect` | `status` | `decode` | `transport` |
  `response`). Token counts come from the sidecar's `usage` block and are absent if it omits one.
- **The LLM path is best-effort and off the punctual path.** A failed call or a gate rejection
  degrades that cluster/story to its deterministic baseline — never a late or wrong digest. Treat the
  LLM panels as quality/efficiency signals, not delivery SLOs: page on the delivery/queue rows, watch
  these.

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
| LLM sidecar unreachable | `sum(rate(bulletin_llm_calls_total{outcome=~"connect|timeout"}[10m])) / clamp_min(sum(rate(bulletin_llm_calls_total[10m])), 1) > 0.5` | 15m |

Tune the "no deliveries" / "ingestion stalled" thresholds to your slowest cadence (weekly digests,
slow feeds). The LLM-sidecar alert only fires where `llm-summarization` is deployed — skip it
otherwise, since the series won't exist.
