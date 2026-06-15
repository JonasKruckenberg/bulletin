mod api;
mod debug;
mod metric;
mod secrets;
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
    /// policies (design §12) physically confine each query to its scope. Required for every role,
    /// but not for the offline `secrets` key tooling.
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,
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
    /// Bind address for the gRPC service API (`api` / `all`). Loopback by default — front it with TLS
    /// before exposing it off-box.
    #[arg(long, env = "BULLETIN_API_ADDR", default_value = "127.0.0.1:50051")]
    api_addr: SocketAddr,
    /// Bearer key authorizing the gRPC **admin plane** (`api` / `all`). Absent ⇒ every admin RPC is
    /// rejected (fail-closed). Sealing it under the master key is future work; a plain env value is
    /// accepted now, like the dev webhook-secret fallback.
    #[arg(long, env = "BULLETIN_API_ADMIN_KEY")]
    api_admin_key: Option<String>,
    /// Log output format: `text` (human) or `json` (one structured line per event, for Loki).
    #[arg(long, env = "BULLETIN_LOG_FORMAT", default_value = "text")]
    log_format: LogFormat,
    /// Credential-at-rest config: the app master key + the (sealed) GitHub App key / webhook secret
    /// the runtime roles unseal at startup, plus the `secrets` tools that produce them.
    #[command(flatten)]
    secrets: secrets::SecretConfig,
    /// Email delivery config (worker + `debug digest-run`); defaults to local file transport.
    #[command(flatten)]
    email: transport::EmailConfig,
}

impl Cli {
    /// The runtime DB URL, required for every DB-touching role. (The offline `secrets` tooling never
    /// calls this, so it can run without a database configured.)
    fn database_url(&self) -> Result<&str> {
        self.database_url.as_deref().context(DATABASE_URL_REQUIRED)
    }
}

/// Shared error text for the two places that resolve the runtime DB URL (the `database_url` accessor
/// and the `Debug` arm, which can't use it because `cli.command` is already partially moved).
const DATABASE_URL_REQUIRED: &str = "DATABASE_URL is required (set --database-url or the env var)";

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
    /// Run the gRPC service API server (admin plane).
    Api,
    Debug {
        #[command(subcommand)]
        command: debug::DebugCommand,
    },
    /// Offline credential tooling: generate the master key, seal secrets for config. No database.
    Secrets {
        #[command(subcommand)]
        command: secrets::SecretsCommand,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.log_format);

    match cli.command {
        // Offline key tooling — handled before any DB connect so it needs no DATABASE_URL.
        Command::Secrets { command } => {
            return secrets::run(&cli.secrets, command);
        }
        Command::Migrate => {
            // Migrate as the owner role: it owns the DDL, creates the runtime role + RLS policies,
            // and (afterwards) grants the runtime role its table access.
            let migration_url = cli
                .migration_database_url
                .as_deref()
                .map(Ok)
                .unwrap_or_else(|| cli.database_url())?;
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
            let pool = connect_pool(cli.database_url()?).await?;
            let webhook_secret = cli.secrets.webhook_secret()?;
            tracing::info!(addr = %cli.http_addr, "starting HTTP server");
            serve(cli.http_addr, pool, webhook_secret).await?;
        }
        Command::Worker => {
            metric::init(cli.metrics_addr)?;
            let pool = connect_pool(cli.database_url()?).await?;
            let connectors = cli.secrets.connector_ctx()?;
            tracing::info!("starting worker");
            worker::start(pool, cli.email.clone(), connectors).await?;
        }
        Command::Api => {
            let pool = connect_pool(cli.database_url()?).await?;
            tracing::info!(addr = %cli.api_addr, "starting gRPC API server");
            api::serve(cli.api_addr, pool, cli.api_admin_key.clone()).await?;
        }
        Command::All => {
            metric::init(cli.metrics_addr)?;
            let pool = connect_pool(cli.database_url()?).await?;
            let webhook_secret = cli.secrets.webhook_secret()?;
            let connectors = cli.secrets.connector_ctx()?;
            tracing::info!(addr = %cli.http_addr, "starting server + worker + api");
            tokio::try_join!(
                serve(cli.http_addr, pool.clone(), webhook_secret),
                worker::start(pool.clone(), cli.email.clone(), connectors),
                api::serve(cli.api_addr, pool, cli.api_admin_key.clone())
            )?;
        }
        Command::Debug { command } => {
            // `command` is moved out here, so reach the URL by field (the `&self` method would
            // borrow the partially-moved `cli`).
            let url = cli.database_url.as_deref().context(DATABASE_URL_REQUIRED)?;
            let pool = connect_pool(url).await?;
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

/// The HTTP server (`serve` / `all`): liveness `/health` + the webhook catcher (`/webhooks/github`),
/// separate from the metrics exporter the worker installs. Needs the pool (for the apalis enqueue
/// handle) and the webhook secret (for edge HMAC verification).
async fn serve(
    addr: SocketAddr,
    pool: PgPool,
    github_webhook_secret: Option<Vec<u8>>,
) -> Result<()> {
    let app = webhook::router(pool, github_webhook_secret);
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
