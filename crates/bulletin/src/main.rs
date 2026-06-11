mod build;
mod digest;
mod email;
mod serve;
mod worker;

use anyhow::{Context, Result};
use bulletin_core::kind::SourceKind;
use clap::{Parser, Subcommand, ValueEnum};
use std::net::SocketAddr;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "bulletin")]
struct Cli {
    #[command(subcommand)]
    command: Command,
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    /// Bind address for the health HTTP server (`serve` / `all`).
    #[arg(long, env = "BULLETIN_HTTP_ADDR", default_value = "127.0.0.1:3000")]
    http_addr: SocketAddr,
    /// Bind address for the Prometheus metrics exporter (`worker` / `all`).
    #[arg(long, env = "BULLETIN_METRICS_ADDR", default_value = "127.0.0.1:9464")]
    metrics_addr: SocketAddr,
    /// Log output format: `text` (human) or `json` (one structured line per event, for Loki).
    #[arg(long, env = "BULLETIN_LOG_FORMAT", default_value = "text")]
    log_format: LogFormat,
    /// Email delivery config (worker + `debug digest-run`); defaults to local file transport.
    #[command(flatten)]
    email: email::EmailConfig,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

#[derive(Subcommand)]
enum Command {
    Serve,
    Worker,
    Migrate,
    All,
    Debug {
        #[command(subcommand)]
        command: DebugCommand,
    },
}

#[derive(Subcommand)]
enum DebugCommand {
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.log_format);

    match cli.command {
        Command::Migrate => {
            let pool = bulletin_store::connect(&cli.database_url)
                .await
                .context("failed to connect to database")?;
            tracing::info!("running bulletin migrations");
            bulletin_store::migrate(&pool)
                .await
                .context("bulletin migrations failed")?;
            tracing::info!("running apalis storage setup");
            worker::setup_storage(&pool)
                .await
                .context("apalis storage setup failed")?;
            tracing::info!("migrations complete");
        }
        Command::Serve => {
            tracing::info!(addr = %cli.http_addr, "starting HTTP server");
            serve::start(cli.http_addr).await?;
        }
        Command::Worker => {
            install_metrics(cli.metrics_addr)?;
            let pool = bulletin_store::connect(&cli.database_url)
                .await
                .context("failed to connect to database")?;
            tracing::info!("starting worker");
            worker::start(pool, cli.email.clone()).await?;
        }
        Command::All => {
            install_metrics(cli.metrics_addr)?;
            let pool = bulletin_store::connect(&cli.database_url)
                .await
                .context("failed to connect to database")?;
            tracing::info!(addr = %cli.http_addr, "starting server + worker");
            tokio::try_join!(
                serve::start(cli.http_addr),
                worker::start(pool, cli.email.clone())
            )?;
        }
        Command::Debug { command } => {
            let pool = bulletin_store::connect(&cli.database_url)
                .await
                .context("failed to connect to database")?;
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
                    let id = bulletin_store::connection::insert_connection(
                        &pool,
                        source,
                        config,
                        poll_interval,
                    )
                    .await?;
                    println!("{id}");
                }
                DebugCommand::ConnectionList => {
                    let rows = bulletin_store::connection::list_connections(&pool).await?;
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
                    let deleted = bulletin_store::connection::delete_connection(&pool, id).await?;
                    if deleted {
                        println!("deleted {id}");
                    } else {
                        println!("not found: {id}");
                    }
                }
                DebugCommand::EventList { limit } => {
                    let events = bulletin_store::event::list_events(&pool, limit).await?;
                    if events.is_empty() {
                        println!("no events");
                    }
                    for ev in events {
                        println!(
                            "{}\t{}\t{}\t{}",
                            ev.ingest_time.format("%Y-%m-%dT%H:%M:%SZ"),
                            ev.source.as_str(),
                            ev.content_kind.as_str(),
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
                    let id =
                        bulletin_store::subscriber::insert_subscriber(&pool, &email, interval_days)
                            .await?;
                    println!("{id}");
                }
                DebugCommand::SubscriberList => {
                    let rows = bulletin_store::subscriber::list_subscribers(&pool).await?;
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
                DebugCommand::BuildRun => match build::run(&pool).await? {
                    Some(stats) => println!(
                        "built {} group(s); watermark → {}",
                        stats.dirty_groups,
                        stats.built_through.format("%Y-%m-%dT%H:%M:%SZ")
                    ),
                    None => println!("skipped (another build in progress)"),
                },
                DebugCommand::DigestRun { subscriber } => {
                    let outcome = digest::run(&pool, &cli.email, subscriber).await?;
                    println!("{outcome:?}");
                }
                DebugCommand::DigestList { limit } => {
                    let rows = bulletin_store::digest::list_digests(&pool, limit).await?;
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
                    let rows = digest::explain(&pool, subscriber).await?;
                    print_explain(&rows);
                }
                DebugCommand::Status => {
                    let report = bulletin_store::status::gather(&pool).await?;
                    print_status(&report);
                }
            }
        }
    }

    Ok(())
}

/// Initializes tracing. `text` is the human default for local dev; `json` emits one structured
/// object per event (`flatten_event` lifts span/event fields to the top level) so the server's
/// journald → Alloy → Loki pipeline parses fields without unwrapping. `RUST_LOG` drives the filter
/// in both modes.
fn init_tracing(format: LogFormat) {
    let registry = tracing_subscriber::registry().with(EnvFilter::from_default_env());
    match format {
        LogFormat::Text => registry.with(tracing_subscriber::fmt::layer()).init(),
        LogFormat::Json => registry
            .with(tracing_subscriber::fmt::layer().json().flatten_event(true))
            .init(),
    }
}

/// Installs the global Prometheus recorder and starts its own HTTP exporter on `addr` (not an app
/// route — the exporter's listener also drives histogram upkeep). The long-running pipeline
/// (`worker` / `all`) then exposes `bulletin_*` series for Prometheus to scrape. Must run inside
/// the tokio runtime, since it spawns the listener.
fn install_metrics(addr: SocketAddr) -> Result<()> {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .context("install prometheus exporter")?;
    tracing::info!(%addr, "metrics exporter listening");
    Ok(())
}

/// Renders a `digest-explain` dry-run: one tab-separated row per candidate (verdict, position
/// or recency rank, relevance, time, source, title), then a one-line tally. Read top-down to
/// see exactly where the cap fell.
fn print_explain(rows: &[digest::ExplainRow]) {
    use bulletin_core::select::Verdict;

    if rows.is_empty() {
        println!("no candidate clusters in this subscriber's window");
        return;
    }

    let (mut selected, mut over_cap, mut below_floor) = (0, 0, 0);
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
            Verdict::BelowFloor => {
                below_floor += 1;
                ("BELOW_FLOOR", "-".to_string())
            }
        };
        println!(
            "{verdict}\t{slot}\trel={:.2}\t{}\t{}\t{}\t{}",
            r.relevance,
            r.last_event_time.format("%Y-%m-%dT%H:%M:%SZ"),
            r.source.map(|s| s.as_str()).unwrap_or("?"),
            r.cluster_id,
            r.title.as_deref().unwrap_or("<missing cluster>"),
        );
    }
    println!("\n{selected} selected · {over_cap} over cap · {below_floor} below floor");
}

/// Renders the `status` dashboard: each subsystem on its own line(s), with the watchpoints that
/// usually explain "why is nothing happening?" (unbuilt events, build lag, due counts, queue
/// backlog) called out inline.
fn print_status(r: &bulletin_store::status::StatusReport) {
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
