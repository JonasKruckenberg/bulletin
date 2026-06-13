//! The `bulletin debug …` inspection commands: seed/list connections and subscribers, run the
//! pipeline stages inline, and dump state. Kept out of `main.rs` so it stays a thin dispatcher.

use anyhow::{Context, Result};
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
    /// Seed a subscriber (first digest is due immediately)
    SubscriberAdd {
        #[arg(long)]
        email: String,
        /// Digest cadence in days (1 = daily, 7 = weekly)
        #[arg(long, default_value_t = 1)]
        interval_days: i32,
    },
    /// List subscribers
    SubscriberList,
    /// Run PublicBuild once, inline (cluster new public events now)
    BuildRun,
    /// Run GenerateDigest once for a subscriber, inline (select → render → deliver)
    DigestRun { subscriber: Uuid },
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
        } => {
            let source = SourceKind::try_from(source.as_str()).map_err(|_| {
                anyhow::anyhow!("unknown source '{}'; valid: rss, github, slack", source)
            })?;
            let config: serde_json::Value =
                serde_json::from_str(&config).context("--config is not valid JSON")?;
            let id = ingest::store::insert_connection(pool, source, config, poll_interval).await?;
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
            interval_days,
        } => {
            if interval_days < 1 {
                anyhow::bail!("--interval-days must be >= 1");
            }
            let id = digest::subscriber::insert_subscriber(pool, &email, interval_days).await?;
            println!("{id}");
        }
        DebugCommand::SubscriberList => {
            let rows = digest::subscriber::list_subscribers(pool).await?;
            if rows.is_empty() {
                println!("no subscribers");
            }
            for s in rows {
                println!(
                    "{}\t{}\tevery {}d\tmax={}\tnext={}\tlast={}",
                    s.id,
                    s.email,
                    s.interval_days,
                    s.max_items,
                    s.next_run_at.format("%Y-%m-%dT%H:%M:%SZ"),
                    s.last_run_at
                        .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                        .unwrap_or_else(|| "never".to_string()),
                );
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

/// Renders a `digest-explain` dry-run: one tab-separated row per candidate (verdict, position or
/// recency rank, time, source, title), then a one-line tally. Read top-down to see where the cap
/// fell.
fn print_explain(rows: &[digest::ExplainRow]) {
    use bulletin_core::digest::select::Verdict;

    if rows.is_empty() {
        println!("no candidate clusters in this subscriber's window");
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
        println!(
            "{verdict}\t{slot}\t{}\t{}\t{}\t{}",
            r.last_event_time.format("%Y-%m-%dT%H:%M:%SZ"),
            r.source.map(|s| s.as_str()).unwrap_or("?"),
            r.cluster_id,
            r.title.as_deref().unwrap_or("<missing cluster>"),
        );
    }
    println!("\n{selected} selected · {over_cap} over cap");
}

/// Renders the `status` dashboard: each subsystem on its own line(s), with the watchpoints that
/// usually explain "why is nothing happening?" (unbuilt events, build lag, due counts, queue
/// backlog) called out inline.
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
        "subscribers  {} total; {} due now; next run {}",
        s.total,
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
