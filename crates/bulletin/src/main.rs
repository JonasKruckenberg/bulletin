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
            worker::start(pool).await?;
        }
        Command::All => {
            let pool = bulletin_store::connect(&cli.database_url)
                .await
                .context("failed to connect to database")?;
            let addr: SocketAddr = "0.0.0.0:3000".parse()?;
            tracing::info!(%addr, "starting server + worker");
            tokio::try_join!(serve::start(addr), worker::start(pool))?;
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
            }
        }
    }

    Ok(())
}
