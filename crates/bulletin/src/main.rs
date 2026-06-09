mod build;
mod digest;
mod email;
mod serve;
mod worker;

use anyhow::{Context, Result};
use bulletin_core::kind::SourceKind;
use clap::{Parser, Subcommand};
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
    /// Email delivery config (worker + `debug digest-run`); defaults to local file transport.
    #[command(flatten)]
    email: email::EmailConfig,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Migrate => {
            let pool = bulletin_store::connect(&cli.database_url)
                .await
                .context("failed to connect to database")?;
            tracing::info!("running bulletin migrations");
            bulletin_store::migrate(&pool).await.context("bulletin migrations failed")?;
            tracing::info!("running apalis storage setup");
            worker::setup_storage(&pool).await.context("apalis storage setup failed")?;
            tracing::info!("migrations complete");
        }
        Command::Serve => {
            let addr: SocketAddr = "0.0.0.0:3000".parse()?;
            tracing::info!(%addr, "starting HTTP server");
            serve::start(addr).await?;
        }
        Command::Worker => {
            let pool = bulletin_store::connect(&cli.database_url)
                .await
                .context("failed to connect to database")?;
            tracing::info!("starting worker");
            worker::start(pool, cli.email.clone()).await?;
        }
        Command::All => {
            let pool = bulletin_store::connect(&cli.database_url)
                .await
                .context("failed to connect to database")?;
            let addr: SocketAddr = "0.0.0.0:3000".parse()?;
            tracing::info!(%addr, "starting server + worker");
            tokio::try_join!(serve::start(addr), worker::start(pool, cli.email.clone()))?;
        }
        Command::Debug { command } => {
            let pool = bulletin_store::connect(&cli.database_url)
                .await
                .context("failed to connect to database")?;
            match command {
                DebugCommand::ConnectionAdd { source, config, poll_interval } => {
                    let source = SourceKind::try_from(source.as_str())
                        .map_err(|_| anyhow::anyhow!("unknown source '{}'; valid: rss, github, slack", source))?;
                    let config: serde_json::Value = serde_json::from_str(&config)
                        .context("--config is not valid JSON")?;
                    let id = bulletin_store::connection::insert_connection(
                        &pool, source, config, poll_interval,
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
                    let deleted =
                        bulletin_store::connection::delete_connection(&pool, id).await?;
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
                DebugCommand::SubscriberAdd { email, interval_days } => {
                    if interval_days < 1 {
                        anyhow::bail!("--interval-days must be >= 1");
                    }
                    let id = bulletin_store::subscriber::insert_subscriber(
                        &pool,
                        &email,
                        interval_days,
                    )
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
            }
        }
    }

    Ok(())
}
