mod debug;
mod metric;
mod transport;
mod webhook;
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
    /// Runtime DB connection string — the **least-privilege role** (`bulletin_app`: non-owner,
    /// no `BYPASSRLS`) that `serve` / `worker` / `debug` log in as. Under it the two-context RLS
    /// policies (design §12) physically confine each query to its scope.
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    /// Migration DB connection string — the **owner/migration role** that owns the DDL and runs
    /// `migrate`. Defaults to `--database-url` when unset (single-role dev). In production this is a
    /// separate, more-privileged role from the runtime one, so a runtime credential can never alter
    /// the schema or disable RLS.
    #[arg(long, env = "BULLETIN_MIGRATION_DATABASE_URL")]
    migration_database_url: Option<String>,
    /// Bind address for the health HTTP server (`serve` / `all`).
    #[arg(long, env = "BULLETIN_HTTP_ADDR", default_value = "127.0.0.1:3000")]
    http_addr: SocketAddr,
    /// Bind address for the Prometheus metrics exporter (`worker` / `all`).
    #[arg(long, env = "BULLETIN_METRICS_ADDR", default_value = "127.0.0.1:9464")]
    metrics_addr: SocketAddr,
    /// Log output format: `text` (human) or `json` (one structured line per event, for Loki).
    #[arg(long, env = "BULLETIN_LOG_FORMAT", default_value = "text")]
    log_format: LogFormat,
    /// GitHub App webhook signing secret — the HMAC-SHA256 key for `X-Hub-Signature-256` over the
    /// raw body (`serve` / `all`). Plumbed now; sealed at rest in a later phase ("plumbing now,
    /// secrets later"). Absent → `/webhooks/github` fails closed (rejects every delivery).
    #[arg(long, env = "BULLETIN_GITHUB_WEBHOOK_SECRET")]
    github_webhook_secret: Option<String>,
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
            // Migrate as the owner role: it owns the DDL, creates the runtime role + RLS policies,
            // and (afterwards) grants the runtime role its table access.
            let migration_url = cli
                .migration_database_url
                .as_deref()
                .unwrap_or(&cli.database_url);
            let pool = connect_pool(migration_url).await?;
            tracing::info!("running bulletin migrations");
            bulletin_core::migrate(&pool)
                .await
                .context("bulletin migrations failed")?;
            tracing::info!("running apalis storage setup");
            worker::setup_storage(&pool)
                .await
                .context("apalis storage setup failed")?;
            // Re-grant the runtime role its access every migrate, so tables added by this run (and
            // the apalis queue schema, just created above) are always reachable by `bulletin_app`.
            tracing::info!("granting runtime role access");
            bulletin_core::grant_runtime_role(&pool)
                .await
                .context("granting runtime role access failed")?;
            tracing::info!("migrations complete");
        }
        Command::Serve => {
            let pool = connect_pool(&cli.database_url).await?;
            tracing::info!(addr = %cli.http_addr, "starting HTTP server");
            serve(cli.http_addr, pool, cli.github_webhook_secret.clone()).await?;
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
                serve(
                    cli.http_addr,
                    pool.clone(),
                    cli.github_webhook_secret.clone()
                ),
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

/// The HTTP server (`serve` / `all`): liveness `/health` + the webhook catcher (`/webhooks/github`),
/// separate from the metrics exporter the worker installs. Needs the pool (for the apalis enqueue
/// handle) and the webhook secret (for edge HMAC verification).
async fn serve(
    addr: SocketAddr,
    pool: PgPool,
    github_webhook_secret: Option<String>,
) -> Result<()> {
    let app = webhook::router(pool, github_webhook_secret.map(String::into_bytes));
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
