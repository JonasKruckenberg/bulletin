//! The `bulletin debug …` inspection commands. **A thin gRPC client of the admin plane** (`bulletin
//! api`) — even when run locally. Each command builds a request, calls the matching admin RPC, and
//! prints the response; it opens no database and builds no mailer, so an operator can run it without
//! the runtime DB credential or the SMTP secret (the engine behind the API holds those and does the
//! work). Kept out of `main.rs` so it stays a thin dispatcher.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use bulletin_core::digest::subscriber::Recurrence;
use clap::Subcommand;
use prost_types::Timestamp;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Endpoint;
use tonic::Request;
use uuid::Uuid;

use crate::api::proto;
use proto::admin_service_client::AdminServiceClient;
use proto::unstable_debug_service_client::UnstableDebugServiceClient;

#[derive(Subcommand)]
pub enum DebugCommand {
    /// Insert a new connection row
    ConnectionAdd {
        #[arg(long)]
        source: String,
        /// JSON config blob, e.g. '{"url":"https://..."}'
        #[arg(long)]
        config: String,
        #[arg(long, default_value_t = bulletin_core::ingest::DEFAULT_POLL_INTERVAL_SECS)]
        poll_interval: i64,
        /// Owning subscriber id — required for a source that can see private repos (its private
        /// events bind to this owner's scope). Omit for a global/public source like RSS.
        #[arg(long)]
        owner: Option<Uuid>,
    },
    /// List all connection rows
    ConnectionList,
    /// Delete a connection row by id
    ConnectionRm { id: Uuid },
    /// Dump recent events
    EventList {
        #[arg(long, default_value = "20")]
        limit: i64,
    },
    /// Seed a subscriber (first digest fires at the next scheduled local time)
    SubscriberAdd {
        #[arg(long)]
        email: String,
        /// Display name used to personalize the digest greeting (optional)
        #[arg(long)]
        name: Option<String>,
        /// Recurrence frequency: daily | weekly
        #[arg(long, default_value = bulletin_core::digest::subscriber::DEFAULT_FREQ)]
        freq: String,
        /// Day of week for weekly digests: 0=Sun .. 6=Sat (required iff --freq weekly)
        #[arg(long)]
        weekday: Option<i32>,
        /// IANA timezone the digest time is interpreted in, e.g. America/New_York
        #[arg(long, default_value = bulletin_core::digest::subscriber::DEFAULT_TIMEZONE)]
        timezone: String,
        /// Local time-of-day to deliver, HH:MM (24-hour)
        #[arg(long, default_value = bulletin_core::digest::subscriber::DEFAULT_DIGEST_TIME)]
        digest_time: String,
    },
    /// List subscribers
    SubscriberList,
    /// Delete a subscriber row by id (cascades to their digests)
    SubscriberRm { id: Uuid },
    /// Run PublicBuild once, inline (cluster new public events now)
    BuildRun,
    /// Run GenerateDigest once for a subscriber, inline (select → render → deliver)
    DigestRun { subscriber: Uuid },
    /// Dispatch a one-off digest NOW over the last N days, ignoring the subscriber's schedule.
    /// Does not advance their watermark or freeze a scheduled digest — a manual preview/send.
    DigestDispatch {
        subscriber: Uuid,
        /// Lookback window in days
        #[arg(long, default_value_t = 7)]
        lookback_days: i32,
    },
    /// List recent digests with their state
    DigestList {
        #[arg(long, default_value = "20")]
        limit: i64,
    },
    /// Explain a subscriber's selection: every candidate cluster + why it's in or out (dry-run)
    DigestExplain { subscriber: Uuid },
    /// Eval selection quality from the decision log + feedback (read-only): volume/format balance now,
    /// precision + nDCG once feedback flows. With --config, A/B the current vs a trial scoring config
    /// by replaying the frozen candidate snapshots (a real config sweep over history)
    DigestEval {
        subscriber: Uuid,
        /// How many recent digests to score
        #[arg(long, default_value = "50")]
        limit: i64,
        /// Path to a trial scoring config (JSON, the shape `debug config` prints): replays history
        /// under it and the current config, side by side
        #[arg(long)]
        config: Option<std::path::PathBuf>,
    },
    /// Show the data behind a story: its event timeline (source · link · time), oldest-first
    DigestProvenance { story_id: Uuid },
    /// Show the current scoring config as JSON (copy, edit, feed to `digest-eval --config`)
    Config,
    /// Update scoring knobs in digest_config — only the flags you pass change; prints the new config
    ConfigSet {
        #[arg(long)]
        relevance_floor: Option<f32>,
        #[arg(long)]
        scope_bonus: Option<f32>,
        #[arg(long)]
        severity_weight: Option<f32>,
        #[arg(long)]
        recency_half_life_days: Option<f64>,
        #[arg(long)]
        thread_half_life_days: Option<f64>,
        #[arg(long)]
        story_cap: Option<usize>,
        #[arg(long)]
        note_cap: Option<usize>,
        #[arg(long)]
        resurface_penalty: Option<f32>,
    },
    /// Print a single-glance snapshot of pipeline state (events, clusters, queue, …)
    Status,
}

/// Dispatch a `debug` command against the admin plane at `api_addr`, presenting `admin_key` as the
/// bearer. The CLI is a pure gRPC client here: no DB pool, no mailer — the engine does the work.
pub async fn run(
    api_addr: SocketAddr,
    admin_key: Option<String>,
    command: DebugCommand,
) -> Result<()> {
    // Dial the API over plaintext h2 (loopback by default; front with TLS off-box). The bearer is
    // attached by an interceptor so every RPC on this client is authenticated uniformly.
    let channel = Endpoint::from_shared(format!("http://{api_addr}"))
        .context("build API endpoint")?
        .connect()
        .await
        .with_context(|| {
            format!("connect to bulletin api at {api_addr} (is `bulletin api` running?)")
        })?;

    let token: Option<MetadataValue<Ascii>> = match admin_key {
        Some(k) => Some(
            format!("Bearer {}", k.trim())
                .parse()
                .context("--api-admin-key is not a valid bearer token")?,
        ),
        None => None,
    };
    // Two clients over the one channel: the wire-stable `AdminService` and the `UnstableDebugService`.
    // Both present the same admin bearer (attached by the interceptor).
    let mut admin = AdminServiceClient::with_interceptor(channel.clone(), bearer(token.clone()));
    let mut dbg = UnstableDebugServiceClient::with_interceptor(channel, bearer(token));

    match command {
        DebugCommand::ConnectionAdd {
            source,
            config,
            poll_interval,
            owner,
        } => {
            let conn = admin
                .create_connection(proto::CreateConnectionRequest {
                    source,
                    config_json: config,
                    poll_interval_secs: poll_interval,
                    owner: owner.map(|o| o.to_string()),
                })
                .await?
                .into_inner();
            println!("{}", conn.id);
        }
        DebugCommand::ConnectionList => {
            let rows = admin
                .list_connections(proto::ListConnectionsRequest {})
                .await?
                .into_inner()
                .connections;
            if rows.is_empty() {
                println!("no connections");
            }
            for r in rows {
                println!(
                    "{}\t{}\t{}\tpoll={}s\tnext={}\tconfig={}",
                    r.id,
                    r.source,
                    r.status,
                    r.poll_interval_secs,
                    req_ts(&r.next_poll_at),
                    r.config_json,
                );
            }
        }
        DebugCommand::ConnectionRm { id } => {
            let deleted = admin
                .delete_connection(proto::DeleteConnectionRequest { id: id.to_string() })
                .await?
                .into_inner()
                .deleted;
            if deleted {
                println!("deleted {id}");
            } else {
                println!("not found: {id}");
            }
        }
        DebugCommand::EventList { limit } => {
            let events = admin
                .list_events(proto::ListEventsRequest { limit })
                .await?
                .into_inner()
                .events;
            if events.is_empty() {
                println!("no events");
            }
            for ev in events {
                println!("{}\t{}\t{}", req_ts(&ev.ingest_time), ev.source, ev.title);
                for link in &ev.links {
                    println!("  {link}");
                }
            }
        }
        DebugCommand::SubscriberAdd {
            email,
            name,
            freq,
            weekday,
            timezone,
            digest_time,
        } => {
            let sub = admin
                .create_subscriber(proto::CreateSubscriberRequest {
                    email,
                    name,
                    freq,
                    weekday,
                    timezone,
                    digest_time,
                })
                .await?
                .into_inner();
            println!("{}", sub.id);
        }
        DebugCommand::SubscriberList => {
            let rows = admin
                .list_subscribers(proto::ListSubscribersRequest {})
                .await?
                .into_inner()
                .subscribers;
            if rows.is_empty() {
                println!("no subscribers");
            }
            for s in rows {
                // Rebuild the core `Recurrence` from the (freq, weekday) columns and let *it* format
                // the cadence, so the label stays identical to the scheduled path (no client-side
                // reimplementation to drift from `Recurrence::label`). weekday is present iff weekly.
                let cadence = match s.weekday {
                    Some(weekday) => Recurrence::Weekly { weekday }.label(),
                    None => Recurrence::Daily.label(),
                };
                println!(
                    "{}\t{}\t{}\t{}\t{} {}\tmax={}\tnext={}\tlast={}",
                    s.id,
                    s.email,
                    s.name.as_deref().unwrap_or("-"),
                    cadence,
                    s.digest_time,
                    s.timezone,
                    s.max_items,
                    req_ts(&s.next_run_at),
                    opt_ts(&s.last_run_at),
                );
            }
        }
        DebugCommand::SubscriberRm { id } => {
            let deleted = admin
                .delete_subscriber(proto::DeleteSubscriberRequest { id: id.to_string() })
                .await?
                .into_inner()
                .deleted;
            if deleted {
                println!("deleted {id}");
            } else {
                println!("not found: {id}");
            }
        }
        DebugCommand::BuildRun => {
            let r = dbg.run_build(proto::RunBuildRequest {}).await?.into_inner();
            if r.skipped {
                println!("skipped (another build in progress)");
            } else {
                println!(
                    "built {} group(s); watermark → {}",
                    r.dirty_groups,
                    req_ts(&r.built_through)
                );
            }
        }
        DebugCommand::DigestRun { subscriber } => {
            let outcome = dbg
                .run_digest(proto::RunDigestRequest {
                    subscriber: subscriber.to_string(),
                })
                .await?
                .into_inner();
            println!("{}", format_outcome(&outcome));
        }
        DebugCommand::DigestDispatch {
            subscriber,
            lookback_days,
        } => {
            if lookback_days < 1 {
                anyhow::bail!("--lookback-days must be >= 1");
            }
            let outcome = dbg
                .dispatch_digest(proto::DispatchDigestRequest {
                    subscriber: subscriber.to_string(),
                    lookback_days,
                })
                .await?
                .into_inner();
            println!("{}", format_outcome(&outcome));
        }
        DebugCommand::DigestList { limit } => {
            let rows = admin
                .list_digests(proto::ListDigestsRequest { limit })
                .await?
                .into_inner()
                .digests;
            if rows.is_empty() {
                println!("no digests");
            }
            for d in rows {
                let status = d
                    .delivered_at
                    .as_ref()
                    .map(|t| format!("delivered {}", fmt_ts(t)))
                    .unwrap_or_else(|| "pending".to_string());
                println!(
                    "{}\t{}\t{}\titems={}\twindow_end={}",
                    d.id,
                    d.subscriber_email,
                    status,
                    d.item_count,
                    req_ts(&d.window_end),
                );
            }
        }
        DebugCommand::DigestExplain { subscriber } => {
            let rows = dbg
                .explain_digest(proto::ExplainDigestRequest {
                    subscriber: subscriber.to_string(),
                })
                .await?
                .into_inner()
                .rows;
            print_explain(&rows);
        }
        DebugCommand::DigestEval {
            subscriber,
            limit,
            config,
        } => {
            // Read + validate any trial config locally (so the path shows in a parse error), then send
            // it as canonical JSON; the engine replays history under it.
            let trial_config_json = match &config {
                None => None,
                Some(path) => {
                    let cfg: bulletin_core::digest::select::ScoringConfig =
                        serde_json::from_str(&std::fs::read_to_string(path)?).map_err(|e| {
                            anyhow::anyhow!("parse trial config {}: {e}", path.display())
                        })?;
                    Some(serde_json::to_string(&cfg)?)
                }
            };
            let resp = dbg
                .eval_digest(proto::EvalDigestRequest {
                    subscriber: subscriber.to_string(),
                    limit,
                    trial_config_json,
                })
                .await?
                .into_inner();
            match (resp.trial, &config) {
                (Some(trial), Some(path)) => {
                    println!("== current config ==");
                    print!("{}", resp.baseline);
                    println!("\n== trial config ({}) ==", path.display());
                    print!("{trial}");
                }
                _ => print!("{}", resp.baseline),
            }
        }
        DebugCommand::DigestProvenance { story_id } => {
            let entries = dbg
                .get_digest_provenance(proto::GetDigestProvenanceRequest {
                    story_id: story_id.to_string(),
                })
                .await?
                .into_inner()
                .entries;
            print_provenance(story_id, &entries);
        }
        DebugCommand::Config => {
            let cfg = dbg
                .get_config(proto::GetConfigRequest {})
                .await?
                .into_inner();
            println!("{}", serde_json::to_string_pretty(&to_core_config(&cfg))?);
        }
        DebugCommand::ConfigSet {
            relevance_floor,
            scope_bonus,
            severity_weight,
            recency_half_life_days,
            thread_half_life_days,
            story_cap,
            note_cap,
            resurface_penalty,
        } => {
            let cfg = dbg
                .set_config(proto::SetConfigRequest {
                    relevance_floor,
                    scope_bonus,
                    severity_weight,
                    recency_half_life_days,
                    thread_half_life_days,
                    story_cap: story_cap.map(|v| v as u64),
                    note_cap: note_cap.map(|v| v as u64),
                    resurface_penalty,
                })
                .await?
                .into_inner();
            println!("{}", serde_json::to_string_pretty(&to_core_config(&cfg))?);
        }
        DebugCommand::Status => {
            let report = admin
                .get_status(proto::GetStatusRequest {})
                .await?
                .into_inner();
            print_status(&report);
        }
    }
    Ok(())
}

/// Builds a tonic client interceptor that attaches the admin bearer to every request. Returned as a
/// `Clone` closure so the two clients (stable + unstable) can each own a copy. With no key configured
/// it attaches nothing and the server fails the call closed.
fn bearer(
    token: Option<MetadataValue<Ascii>>,
) -> impl FnMut(Request<()>) -> Result<Request<()>, tonic::Status> + Clone {
    move |mut req: Request<()>| {
        if let Some(t) = &token {
            req.metadata_mut().insert("authorization", t.clone());
        }
        Ok(req)
    }
}

/// A required protobuf timestamp (semantically non-null, but always `Option` on the wire) → the
/// engine's canonical `YYYY-MM-DDTHH:MM:SSZ`, with a `?` fallback if the server somehow omitted it.
fn req_ts(t: &Option<Timestamp>) -> String {
    t.as_ref().map(fmt_ts).unwrap_or_else(|| "?".to_string())
}

/// An optional protobuf timestamp → the canonical format, or `never` when absent (the dashboard idiom).
fn opt_ts(t: &Option<Timestamp>) -> String {
    t.as_ref()
        .map(fmt_ts)
        .unwrap_or_else(|| "never".to_string())
}

fn fmt_ts(t: &Timestamp) -> String {
    chrono::DateTime::from_timestamp(t.seconds, t.nanos as u32)
        .map(|d| d.format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .unwrap_or_else(|| "?".to_string())
}

/// Reconstruct core's `ScoringConfig` from the wire shape so the JSON `debug config` prints is the
/// exact serde form `digest-eval --config` round-trips.
fn to_core_config(c: &proto::ScoringConfig) -> bulletin_core::digest::select::ScoringConfig {
    bulletin_core::digest::select::ScoringConfig {
        relevance_floor: c.relevance_floor,
        scope_bonus: c.scope_bonus,
        severity_weight: c.severity_weight,
        recency_half_life_days: c.recency_half_life_days,
        thread_half_life_days: c.thread_half_life_days,
        story_cap: c.story_cap as usize,
        note_cap: c.note_cap as usize,
        resurface_penalty: c.resurface_penalty,
    }
}

/// Render a `DigestOutcome` the way the old inline `{outcome:?}` did, from the wire kind + count.
fn format_outcome(o: &proto::DigestOutcome) -> String {
    match o.kind.as_str() {
        "delivered" => format!("Delivered {{ items: {} }}", o.items),
        "empty" => "Empty".to_string(),
        "already_delivered" => "AlreadyDelivered".to_string(),
        "not_yet_due" => "NotYetDue".to_string(),
        "lead_deferred" => "LeadDeferred (no LLM lead; delivery deferred)".to_string(),
        other => other.to_string(),
    }
}

/// Renders a `digest-explain` dry-run: one tab-separated row per candidate **story** (verdict,
/// position or recency rank, time, representative source + title), and — indented beneath a fused
/// story — each connected cluster with the `link_reason` for why it joined (the M3 cross-source
/// value). Closes with a one-line tally. Read top-down to see where the cap fell.
fn print_explain(rows: &[proto::ExplainRow]) {
    if rows.is_empty() {
        println!("no candidate stories in this subscriber's lookback");
        return;
    }

    let (mut selected, mut over_cap, mut dropped) = (0, 0, 0);
    for r in rows {
        let slot = match r.verdict.as_str() {
            "SELECTED" => {
                selected += 1;
                format!("pos={}", r.position.unwrap_or(0))
            }
            "OVER_CAP" => {
                over_cap += 1;
                format!("rank={}", r.rank.unwrap_or(0))
            }
            "DROPPED" => {
                dropped += 1;
                r.drop_cause.clone().unwrap_or_default()
            }
            _ => String::new(),
        };
        // The M4 scoring outcome (design §10.2): format · richness · relevance · priority.
        println!(
            "{}\t{}\t{}\t{}\t{}\t[{} {} rel={:.2} pri={:.3}]\t{}",
            r.verdict,
            slot,
            req_ts(&r.last_event_time),
            r.source,
            r.story_id,
            r.format,
            r.richness,
            r.relevance,
            r.priority,
            r.title,
        );
        for conn in &r.connections {
            println!(
                "    ↳ [{}] {} — {}",
                conn.source,
                conn.title,
                conn.link_reason.as_deref().unwrap_or("linked"),
            );
        }
    }
    println!("\n{selected} selected · {over_cap} over cap · {dropped} dropped");
}

/// Renders a story's provenance timeline (design §10.1) — one event per line, oldest-first, each with
/// its time, source, title, and backing link. The "show the data behind this story" drill-down.
fn print_provenance(story_id: Uuid, entries: &[proto::TimelineEntry]) {
    if entries.is_empty() {
        println!("no events behind story {story_id} (unknown, tombstoned, or empty)");
        return;
    }
    println!("timeline for story {story_id} ({} events):", entries.len());
    for e in entries {
        println!("{}\t{}\t{}", req_ts(&e.event_time), e.source, e.title);
        if let Some(link) = &e.link {
            println!("  {link}");
        }
    }
}

/// Renders the `status` dashboard: each subsystem on its own line(s). The watchpoints to scan are
/// materialization freshness (unbuilt events, build lag, latest ingest), projection backlog
/// (subscribers due now, pending digests), and queue depth. Build lag no longer gates digests —
/// a due subscriber fires regardless; it just means very recent events may ride the next one.
fn print_status(r: &proto::StatusReport) {
    let c = r.connections.unwrap_or_default();
    println!(
        "connections  {} total ({} active, {} paused, {} errored); {} due now",
        c.total, c.active, c.paused, c.errored, c.due_now
    );

    let e = r.events.clone().unwrap_or_default();
    println!(
        "events       {} total, {} unbuilt; latest ingest {}",
        e.total,
        e.unbuilt,
        opt_ts(&e.latest_ingest)
    );
    for sc in &e.by_source {
        println!("               {}: {}", sc.source, sc.count);
    }

    let b = r.build.unwrap_or_default();
    println!(
        "build        built_through {} ({}s behind now)",
        req_ts(&b.built_through),
        b.lag_secs
    );

    let cl = r.clusters.unwrap_or_default();
    println!(
        "clusters     {} total; latest updated {}",
        cl.total,
        opt_ts(&cl.latest_updated)
    );

    let s = r.subscribers.unwrap_or_default();
    println!(
        "subscribers  {} total ({} daily, {} weekly); {} due now; next run {}",
        s.total,
        s.daily,
        s.weekly,
        s.due_now,
        opt_ts(&s.next_run)
    );

    let d = r.digests.unwrap_or_default();
    println!(
        "digests      {} total ({} pending, {} delivered); last delivered {}",
        d.total,
        d.pending,
        d.delivered,
        opt_ts(&d.last_delivered)
    );

    if !r.queue_initialized {
        println!("queue        not initialized (run `migrate`)");
    } else if r.queue.is_empty() {
        println!("queue        empty");
    } else {
        println!("queue        (apalis jobs by type)");
        for q in &r.queue {
            let oldest = q
                .oldest_pending_secs
                .map(|s| format!("; oldest pending {s}s"))
                .unwrap_or_default();
            println!(
                "               {}: {} pending, {} running, {} done, {} failed, {} killed{}",
                q.job_type, q.pending, q.running, q.done, q.failed, q.killed, oldest,
            );
        }
    }
}
