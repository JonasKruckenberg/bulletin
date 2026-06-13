## Data Flow

### `Scope`

TDB

### `Inbox`

WebHook endpoints and CRON scrape jobs feed into this queue. 
The queue is periodically emptied by an async ingest job that normalizes and deduplicates
the raw events into _canicalized_ `Event`s.

### `Event`

A common event format that represents an event in time.

## `Cluster`

Events related to the same underlying topic are aggregated into a cluster. Cluster processing happens in 2 phases:
1. Per-scope: `group` -> `link` -> `signals` events are grouped, cross-cluster links are resolved and each cluster is given a general `salience`(how important is this to begin with) score
2. Per-user: `gate` -> `ranke` -> `classify` -> `inhibit` clusters are scored and sorted by their relevance for the particular user. This step takes takes user feedback and preferences into account. The `N` most relevant clusters are getting promoted to `Story` or `Note`.

## `Story`

Stories are rich and substantive. They represent complex proceedings that may span many sources (e.g. a security incident reponse with slack messages, github issues, PRs, commits, emails, etc.) or one big source (e.g. a published video or blog post). 

Stories are rendered with a headline, short summary, and timeline of its constituent events.

## `Note`

Note are small but highly relevant. They represent events that do not warrant a headline, summary and timeline but are still important to flag. Examples: A band published a new album, a library published new release, an online order is shipped.

Notes are rendered in a compact format with one or two sentences max.

# Deployment (NixOS)

Bulletin ships a flake `package`, an `overlay`, and a `nixosModule`. Your server's own
nixos-config flake consumes it as an input.

## Consumer config

```nix
{
  inputs.bulletin.url = "github:<you>/bulletin";

  # …in the host configuration:
  imports = [ inputs.bulletin.nixosModules.default ];
  nixpkgs.overlays = [ inputs.bulletin.overlays.default ];

  services.bulletin.enable = true;
}
```

The defaults are dogfood-ready: it provisions a local PostgreSQL `bulletin` db + role over
**unix-socket peer auth** (no passwords), writes digests as `.eml` into
`/var/lib/bulletin/outbox` (file transport), logs JSON to journald, exposes Prometheus on
`127.0.0.1:9464`, and runs `bulletin all` as a hardened static `bulletin` user. A oneshot
`bulletin-migrate` runs first; on a local DB it `pg_dump`s a pre-migrate snapshot into
`/var/lib/bulletin/backups` and is gated with `Requires=`, so a failed migration leaves the
old instance running rather than starting a half-migrated binary.

Options: `database.createLocally` / `database.url`, `http.addr`, `metrics.addr`,
`log.format` / `log.level`, `email.transport` / `email.from` / `email.smtpSecretFile`,
`openFirewall`.

### Real email (Proton SMTP submission)

The `file` transport is the default and needs no external service. For real delivery, Proton
Mail Plus (and Unlimited) support SMTP submission with an SMTP token on a **custom-domain**
address — no Proton Bridge process required. Generate a token at Settings → IMAP/SMTP → SMTP
tokens, then drop an agenix secret containing:

```
BULLETIN_SMTP_HOST=smtp.protonmail.ch
BULLETIN_SMTP_USERNAME=bulletin@your.domain
BULLETIN_SMTP_PASSWORD=<the SMTP token>
```

Point `email.smtpSecretFile` at it (loaded via systemd `EnvironmentFile`, never in the Nix
store), set `email.from` to the same custom-domain address, and set `email.transport = "smtp"`.
The transport enforces TLS (STARTTLS on 587 by default, or `BULLETIN_SMTP_TLS=implicit` for
465), so the token is never sent in cleartext.

## Build cache

CI (`.github/workflows/ci.yml`) builds `.#bulletin` and pushes it to the `bulletin` Cachix
cache. Add that cache as a substituter on the server so `nixos-rebuild` pulls the prebuilt
binary instead of compiling.

## Continuous deploy (recommended: deploy-rs)

deploy-rs gives health-gated auto-rollback: a failed `bulletin-migrate` or a `bulletin.service`
that doesn't pass `/health` (its `ExecStartPost` curls it) yields a non-zero activation and
reverts the generation. In the nixos-config flake:

```nix
deploy.nodes.<host>.profiles.system = {
  user = "root";
  path = deploy-rs.lib.x86_64-linux.activate.nixos self.nixosConfigurations.<host>;
  # autoRollback = true; magicRollback = true;
};
```

Iterate by bumping the input: `nix flake update bulletin && deploy .#<host>`.

**Migration discipline:** migrations are append-only — never edit an applied `.sql` (sqlx
checksums them) — and additive/expand-contract so the running binary tolerates the new schema.
`ignore_missing = true` lets an older binary roll back over a newer schema; fix a bad migration
by rolling *forward* with a new file, never with down-migrations.

## Logs → Loki (via your existing Alloy)

The service logs JSON to stdout → journald. Point Alloy's journal source at it; keep labels
low-cardinality (dynamic fields go to structured metadata, not labels):

```alloy
loki.relabel "bulletin" {
  forward_to = []
  rule { source_labels = ["__journal__systemd_unit"];     target_label = "unit" }
  rule { source_labels = ["__journal_priority_keyword"];  target_label = "priority" }
}

loki.source.journal "journal" {
  forward_to    = [loki.process.bulletin.receiver]
  relabel_rules = loki.relabel.bulletin.rules
  labels        = { job = "systemd-journal" }
}

loki.process "bulletin" {
  forward_to = [loki.write.default.receiver]
  stage.json               { expressions = { level = "level", target = "target" } }
  stage.structured_metadata { values     = { level = "", target = "" } }
}
```

Query in Grafana: `{unit="bulletin.service"} | json`.

## Metrics → Prometheus

```yaml
scrape_configs:
  - job_name: bulletin
    static_configs:
      - targets: ["127.0.0.1:9464"]
```

Counters: `bulletin_jobs_total{job,outcome}`, `bulletin_job_retries_total{job}`,
`bulletin_events_ingested_total{source}`, `bulletin_events_deduplicated_total{source}`,
`bulletin_poll_failures_total{source}`, `bulletin_digests_total{outcome}` (`delivered`/`empty`
both put an email on the wire, so emails sent = `delivered + empty`),
`bulletin_status_gather_failures_total`. Histograms: `bulletin_job_duration_seconds{job}`,
`bulletin_digest_items` (items per delivered digest). Gauges (refreshed once per minute by the
cron tick): `bulletin_queue_depth{job_type}`, `bulletin_queue_running{job_type}`,
`bulletin_queue_failed{job_type}`, `bulletin_queue_killed{job_type}`,
`bulletin_queue_oldest_pending_seconds{job_type}`, `bulletin_build_lag_seconds`,
`bulletin_events_unbuilt`, `bulletin_clusters_total`, `bulletin_connections_active`,
`bulletin_connections_errored`, `bulletin_connections_due`, `bulletin_subscribers{freq}`,
`bulletin_subscribers_due`, `bulletin_digests_pending`,
`bulletin_last_ingest_timestamp_seconds`, `bulletin_last_delivered_timestamp_seconds`.

### Grafana dashboard

A ready-to-import overview dashboard lives at `ops/grafana/bulletin-overview.json` (health/SLO,
jobs, queue backlog, ingestion, delivery). Import it via *Dashboards → New → Import*, paste the
JSON, and pick your Prometheus datasource when prompted. See `ops/grafana/README.md` for the
suggested alert rules.

## Seeding & ops

The `bulletin` CLI is on PATH and presets `DATABASE_URL`, so run it as the service user:

```sh
sudo -u bulletin bulletin debug connection-add --source rss --config '{"url":"https://…/feed.xml"}'
sudo -u bulletin bulletin debug subscriber-add  --email you@proton.me
sudo -u bulletin bulletin debug status
```

## Iteration loop

Tuning digest *logic* needs neither Nix nor a deploy:

- **Tier 0 — local, zero prod risk:** `pg_dump -Fc` the server DB, restore into the local nix
  dev Postgres, then `cargo run -p bulletin -- --database-url postgres:///bulletin?host=/tmp
  debug digest-explain <id>`. Native incremental compile; iterate freely.
- **Tier 1 — live data, no deploy:** `sudo -u bulletin bulletin debug digest-explain <id>` on
  the server. `digest-explain` is **read-only** (no writes, no send) — safe to re-run after
  every change. Do *not* loop on `digest-run`: it sends and advances the subscriber watermark
  (consuming the window). To eyeball a rendered `.eml`, `digest-run` a throwaway clone
  subscriber and read `/var/lib/bulletin/outbox` (file transport means a stray run never emails
  anyone).
- **Tier 2 — full deploy:** only for schema/unit/observability changes (the deploy-rs path).
