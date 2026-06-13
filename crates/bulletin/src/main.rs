mod debug;
mod metric;
mod transport;
mod worker;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use sqlx::PgPool;
use std::net::SocketAddr;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

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
    email: transport::EmailConfig,
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
        command: debug::DebugCommand,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.log_format);

    match cli.command {
        Command::Migrate => {
            let pool = connect_pool(&cli.database_url).await?;
            tracing::info!("running bulletin migrations");
            bulletin_core::migrate(&pool)
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
            serve_health(cli.http_addr).await?;
        }
        Command::Worker => {
            metric::init(cli.metrics_addr)?;
            let pool = connect_pool(&cli.database_url).await?;
            tracing::info!("starting worker");
            worker::start(pool, cli.email.clone(), connector_ctx()).await?;
        }
        Command::All => {
            metric::init(cli.metrics_addr)?;
            let pool = connect_pool(&cli.database_url).await?;
            tracing::info!(addr = %cli.http_addr, "starting server + worker");
            tokio::try_join!(
                serve_health(cli.http_addr),
                worker::start(pool, cli.email.clone(), connector_ctx())
            )?;
        }
        Command::Debug { command } => {
            let pool = connect_pool(&cli.database_url).await?;
            debug::run(&pool, &cli.email, command).await?;
        }
    }

    Ok(())
}

/// Opens the shared Postgres pool. Every command that touches the DB goes through here.
async fn connect_pool(database_url: &str) -> Result<PgPool> {
    bulletin_core::connect(database_url)
        .await
        .context("failed to connect to database")
}

/// The app-level connector context the worker hands to each poll. GitHub's App credentials are
/// envelope-encrypted at rest in a later phase, so `github` is `None` here — a GitHub connection
/// polled now is skipped with a clear log, while RSS works unchanged ("plumbing now, secrets later").
fn connector_ctx() -> bulletin_core::ingest::ConnectorCtx {
    bulletin_core::ingest::ConnectorCtx::default()
}

/// The liveness HTTP server (`serve` / `all`): a single `/health` route, separate from the
/// metrics exporter the worker installs.
async fn serve_health(addr: SocketAddr) -> Result<()> {
    let app = axum::Router::new().route("/health", axum::routing::get(|| async { "ok" }));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind to {addr}"))?;
    axum::serve(listener, app)
        .await
        .context("HTTP server error")?;
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
