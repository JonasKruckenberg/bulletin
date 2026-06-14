//! The `bulletin debug …` inspection commands: seed/list connections and subscribers, run the
//! pipeline stages inline, and dump state. Kept out of `main.rs` so it stays a thin dispatcher.

use anyhow::{Context, Result};
use bulletin_core::digest::subscriber::Recurrence;
use bulletin_core::status::StatusReport;
use bulletin_core::{cluster, digest, ingest, kind::SourceKind, status};
use clap::Subcommand;
use sqlx::PgPool;
use uuid::Uuid;

use crate::transport::EmailConfig;

#[derive(Subcommand)]
pub enum DebugCommand {
    /// Insert a new connection row
    ConnectionAdd {
        #[arg(long)]
        source: String,
        /// JSON config blob, e.g. '{"url":"https://..."}'
        #[arg(long)]
        config: String,
        #[arg(long, default_value = "900")]
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
        #[arg(long, default_value = "daily")]
        freq: String,
        /// Day of week for weekly digests: 0=Sun .. 6=Sat (required iff --freq weekly)
        #[arg(long)]
        weekday: Option<i32>,
        /// IANA timezone the digest time is interpreted in, e.g. America/New_York
        #[arg(long, default_value = "UTC")]
        timezone: String,
        /// Local time-of-day to deliver, HH:MM (24-hour)
        #[arg(long, default_value = "09:00")]
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
    /// Print a single-glance snapshot of pipeline state (events, clusters, queue, …)
    Status,
}

pub async fn run(pool: &PgPool, email: &EmailConfig, command: DebugCommand) -> Result<()> {
    match command {
        DebugCommand::ConnectionAdd {
            source,
            config,
            poll_interval,
            owner,
        } => {
            let source = SourceKind::try_from(source.as_str()).map_err(|_| {
                anyhow::anyhow!("unknown source '{}'; valid: rss, github, slack", source)
            })?;
            // A private-capable source must be owned, or its private events would have no scope to
            // bind to (the DB CHECK enforces this too; this is the friendly up-front error).
            if source.can_emit_private() && owner.is_none() {
                anyhow::bail!(
                    "a {} connection can see private content and must be owned — pass --owner <subscriber-id>",
                    source.as_str()
                );
            }
            let config: serde_json::Value =
                serde_json::from_str(&config).context("--config is not valid JSON")?;
            // Webhook routing key. For GitHub the installation_id (already in --config, not a secret)
            // doubles as `provider_account_id`, so lifecycle/content webhooks resolve to THIS row —
            // derived from our own seed config, never a delivery payload (the IDOR boundary). Without
            // it a seeded GitHub connection would poll but silently drop every webhook as unrouted.
            let provider_account_id = match source {
                SourceKind::Github => Some(
                    config
                        .get("installation_id")
                        .and_then(|v| v.as_i64())
                        .context("a github --config needs an integer \"installation_id\"")?
                        .to_string(),
                ),
                _ => None,
            };
            let id = ingest::store::insert_connection(
                pool,
                source,
                config,
                poll_interval,
                owner,
                provider_account_id.as_deref(),
            )
            .await?;
            println!("{id}");
        }
        DebugCommand::ConnectionList => {
            let rows = ingest::store::list_connections(pool).await?;
            if rows.is_empty() {
                println!("no connections");
            }
            for r in rows {
                println!(
                    "{}\t{}\t{}\tpoll={}s\tnext={}\tconfig={}",
                    r.id,
                    r.source.as_str(),
                    r.status,
                    r.poll_interval_secs,
                    r.next_poll_at.format("%Y-%m-%dT%H:%M:%SZ"),
                    r.config,
                );
            }
        }
        DebugCommand::ConnectionRm { id } => {
            if ingest::store::delete_connection(pool, id).await? {
                println!("deleted {id}");
            } else {
                println!("not found: {id}");
            }
        }
        DebugCommand::EventList { limit } => {
            let events = ingest::store::list_events(pool, limit).await?;
            if events.is_empty() {
                println!("no events");
            }
            for ev in events {
                println!(
                    "{}\t{}\t{}",
                    ev.ingest_time.format("%Y-%m-%dT%H:%M:%SZ"),
                    ev.source.as_str(),
                    ev.title,
                );
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
            let recurrence = Recurrence::new(&freq, weekday).map_err(|e| anyhow::anyhow!("{e}"))?;
            let digest_time = chrono::NaiveTime::parse_from_str(&digest_time, "%H:%M")
                .context("--digest-time must be HH:MM (24-hour)")?;
            let id = digest::subscriber::insert_subscriber(
                pool,
                &email,
                name.as_deref(),
                recurrence,
                &timezone,
                digest_time,
            )
            .await?;
            println!("{id}");
        }
        DebugCommand::SubscriberList => {
            let rows = digest::subscriber::list_subscribers(pool).await?;
            if rows.is_empty() {
                println!("no subscribers");
            }
            for s in rows {
                println!(
                    "{}\t{}\t{}\t{}\t{} {}\tmax={}\tnext={}\tlast={}",
                    s.id,
                    s.email,
                    s.name.as_deref().unwrap_or("-"),
                    s.recurrence.label(),
                    s.digest_time.format("%H:%M"),
                    s.timezone,
                    s.max_items,
                    s.next_run_at.format("%Y-%m-%dT%H:%M:%SZ"),
                    s.last_run_at
                        .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                        .unwrap_or_else(|| "never".to_string()),
                );
            }
        }
        DebugCommand::SubscriberRm { id } => {
            if digest::subscriber::delete_subscriber(pool, id).await? {
                println!("deleted {id}");
            } else {
                println!("not found: {id}");
            }
        }
        DebugCommand::BuildRun => match cluster::build(pool).await? {
            Some(stats) => println!(
                "built {} group(s); watermark → {}",
                stats.dirty_groups,
                stats.built_through.format("%Y-%m-%dT%H:%M:%SZ")
            ),
            None => println!("skipped (another build in progress)"),
        },
        DebugCommand::DigestRun { subscriber } => {
            let sender = email.build_sender()?;
            let outcome = digest::generate(pool, &sender, subscriber, &email.content()).await?;
            println!("{outcome:?}");
        }
        DebugCommand::DigestDispatch {
            subscriber,
            lookback_days,
        } => {
            if lookback_days < 1 {
                anyhow::bail!("--lookback-days must be >= 1");
            }
            let sender = email.build_sender()?;
            let outcome =
                digest::dispatch_now(pool, &sender, subscriber, lookback_days, &email.content())
                    .await?;
            println!("{outcome:?}");
        }
        DebugCommand::DigestList { limit } => {
            let rows = digest::store::list_digests(pool, limit).await?;
            if rows.is_empty() {
                println!("no digests");
            }
            for (d, email, item_count) in rows {
                let status = d
                    .delivered_at
                    .map(|t| format!("delivered {}", t.format("%Y-%m-%dT%H:%M:%SZ")))
                    .unwrap_or_else(|| "pending".to_string());
                println!(
                    "{}\t{}\t{}\titems={}\twindow_end={}",
                    d.id,
                    email,
                    status,
                    item_count,
                    d.window_end.format("%Y-%m-%dT%H:%M:%SZ"),
                );
            }
        }
        DebugCommand::DigestExplain { subscriber } => {
            print_explain(&digest::explain(pool, subscriber).await?);
        }
        DebugCommand::Status => {
            print_status(&status::gather(pool).await?);
        }
    }
    Ok(())
}

/// Renders a `digest-explain` dry-run: one tab-separated row per candidate **story** (verdict,
/// position or recency rank, time, representative source + title), and — indented beneath a fused
/// story — each connected cluster with the `link_reason` for why it joined (the M3 cross-source
/// value). Closes with a one-line tally. Read top-down to see where the cap fell.
fn print_explain(rows: &[digest::ExplainRow]) {
    use bulletin_core::digest::select::Verdict;

    if rows.is_empty() {
        println!("no candidate stories in this subscriber's lookback");
        return;
    }

    let (mut selected, mut over_cap) = (0, 0);
    for r in rows {
        let (verdict, slot) = match r.verdict {
            Verdict::Selected { position } => {
                selected += 1;
                ("SELECTED", format!("pos={position}"))
            }
            Verdict::OverCap { rank } => {
                over_cap += 1;
                ("OVER_CAP", format!("rank={rank}"))
            }
        };
        let (source, title) = match &r.item {
            Some(item) => (item.source.as_str(), item.title.as_str()),
            None => ("?", "<empty story>"),
        };
        println!(
            "{verdict}\t{slot}\t{}\t{}\t{}\t{}",
            r.last_event_time.format("%Y-%m-%dT%H:%M:%SZ"),
            source,
            r.story_id,
            title,
        );
        for conn in r.item.iter().flat_map(|i| i.connections.iter()) {
            println!(
                "    ↳ [{}] {} — {}",
                conn.source.as_str(),
                conn.title,
                conn.link_reason.as_deref().unwrap_or("linked"),
            );
        }
    }
    println!("\n{selected} selected · {over_cap} over cap");
}

/// Renders the `status` dashboard: each subsystem on its own line(s). The watchpoints to scan are
/// materialization freshness (unbuilt events, build lag, latest ingest), projection backlog
/// (subscribers due now, pending digests), and queue depth. Build lag no longer gates digests —
/// a due subscriber fires regardless; it just means very recent events may ride the next one.
fn print_status(r: &StatusReport) {
    fn ts(t: Option<chrono::DateTime<chrono::Utc>>) -> String {
        t.map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string())
            .unwrap_or_else(|| "never".to_string())
    }

    let c = &r.connections;
    println!(
        "connections  {} total ({} active, {} paused, {} errored); {} due now",
        c.total, c.active, c.paused, c.errored, c.due_now
    );

    let e = &r.events;
    println!(
        "events       {} total, {} unbuilt; latest ingest {}",
        e.total,
        e.unbuilt,
        ts(e.latest_ingest)
    );
    for (source, n) in &e.by_source {
        println!("               {source}: {n}");
    }

    let b = &r.build;
    println!(
        "build        built_through {} ({}s behind now)",
        b.built_through.format("%Y-%m-%dT%H:%M:%SZ"),
        b.lag_secs
    );

    let cl = &r.clusters;
    println!(
        "clusters     {} total; latest updated {}",
        cl.total,
        ts(cl.latest_updated)
    );

    let s = &r.subscribers;
    println!(
        "subscribers  {} total ({} daily, {} weekly); {} due now; next run {}",
        s.total,
        s.daily,
        s.weekly,
        s.due_now,
        ts(s.next_run)
    );

    let d = &r.digests;
    println!(
        "digests      {} total ({} pending, {} delivered); last delivered {}",
        d.total,
        d.pending,
        d.delivered,
        ts(d.last_delivered)
    );

    match &r.queue {
        None => println!("queue        not initialized (run `migrate`)"),
        Some(rows) if rows.is_empty() => println!("queue        empty"),
        Some(rows) => {
            println!("queue        (apalis jobs by type)");
            for q in rows {
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
}
