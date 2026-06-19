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
    /// no `BYPASSRLS`) that `serve` / `worker` / `api` log in as. Under it the two-context RLS
    /// policies (design §12) physically confine each query to its scope. Required for those roles, but
    /// not for the offline `secrets` tooling or the `debug` CLI (now a gRPC client of `api`).
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
    /// Bind address for the gRPC service API (`api` / `all`), and the address the `debug` CLI dials as
    /// a client. Loopback by default — front it with TLS before exposing it off-box.
    #[arg(long, env = "BULLETIN_API_ADDR", default_value = "127.0.0.1:50051")]
    api_addr: SocketAddr,
    /// Bearer key authorizing the gRPC **admin plane**. On `api` / `all` it gates every admin RPC
    /// (absent ⇒ all rejected, fail-closed); on `debug` it is the bearer the CLI presents to the API.
    /// Sealing it under the master key is future work; a plain env value is accepted now, like the dev
    /// webhook-secret fallback.
    #[arg(long, env = "BULLETIN_API_ADMIN_KEY")]
    api_admin_key: Option<String>,
    /// Log output format: `text` (human) or `json` (one structured line per event, for Loki).
    #[arg(long, env = "BULLETIN_LOG_FORMAT", default_value = "text")]
    log_format: LogFormat,
    /// Credential-at-rest config: the app master key + the (sealed) GitHub App key / webhook secret
    /// the runtime roles unseal at startup, plus the `secrets` tools that produce them.
    #[command(flatten)]
    secrets: secrets::SecretConfig,
    /// Email delivery config (worker + the `api` digest-send RPCs); defaults to local file transport.
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

/// Shared error text for resolving the runtime DB URL (the `database_url` accessor and the `Migrate`
/// arm, which falls back to it when no migration URL is set).
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
    /// Read-only faithfulness eval (the `digest-explain` hook, `docs/llm-summarization.md` §3.4/§7):
    /// generate candidate summaries for a sample of historical public clusters, run them through the
    /// faithfulness gate, and report the Vectara-style entity/number accuracy rate — **storing nothing
    /// and touching no digest**. Requires a reachable summarization sidecar (it measures the model).
    SummaryEval {
        /// How many recent public clusters to sample.
        #[arg(long, default_value_t = 100)]
        limit: i64,
    },
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
            // Fail loud if the summarization sidecar isn't reachable — it's a required dependency (§3.7).
            ensure_sidecar_ready().await?;
            tracing::info!("starting worker");
            worker::start(pool, cli.email.clone(), connectors).await?;
        }
        Command::Api => {
            let pool = connect_pool(cli.database_url()?).await?;
            tracing::info!(addr = %cli.api_addr, "starting gRPC API server");
            api::serve(
                cli.api_addr,
                pool,
                cli.api_admin_key.clone(),
                cli.email.clone(),
            )
            .await?;
        }
        Command::All => {
            metric::init(cli.metrics_addr)?;
            let pool = connect_pool(cli.database_url()?).await?;
            let webhook_secret = cli.secrets.webhook_secret()?;
            let connectors = cli.secrets.connector_ctx()?;
            // Verify the summarization sidecar *before* binding `/health`. Summarization is required
            // (§3.7), so a box that can't reach its sidecar never reports healthy — the deploy's
            // `ExecStartPost` health probe fails and the rollout rolls back, instead of starting a worker
            // that quarantines the corpus and defers every digest.
            ensure_sidecar_ready().await?;
            tracing::info!(addr = %cli.http_addr, "starting server + worker + api");
            tokio::try_join!(
                serve(cli.http_addr, pool.clone(), webhook_secret),
                worker::start(pool.clone(), cli.email.clone(), connectors),
                api::serve(
                    cli.api_addr,
                    pool,
                    cli.api_admin_key.clone(),
                    cli.email.clone()
                )
            )?;
        }
        Command::SummaryEval { limit } => {
            if limit < 1 {
                anyhow::bail!("--limit must be >= 1");
            }
            // Connect inside the handler so a deterministic (feature-off) build fails on the missing
            // feature *before* it complains about a missing DATABASE_URL — the more useful error.
            run_summary_eval(cli.database_url.as_deref(), limit).await?;
        }
        Command::Debug { command } => {
            // `debug` is a thin gRPC client of the admin plane (`bulletin api`) — even locally. It
            // never opens the DB or builds a mailer, so it needs neither DATABASE_URL nor the SMTP
            // secret; the engine behind the API holds those and does the work.
            debug::run(cli.api_addr, cli.api_admin_key.clone(), command).await?;
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

/// Startup gate: refuse to run the worker if the local summarization sidecar can't be reached.
/// Summarization is a mandatory part of the pipeline now (§3.7), so an unreachable sidecar at boot is a
/// deployment/config error (wrong `BULLETIN_LLM_BASE_URL`, sidecar down, model didn't load) we fail
/// loudly on — rather than start a worker that quarantines the whole corpus and defers every digest.
/// Gives the sidecar a bounded window to come up (it may still be loading its GGUF), tunable via
/// `BULLETIN_LLM_STARTUP_TIMEOUT_SECS` (default 60). Once past this gate the *running* path tracks and
/// retries a transient blip mid-sweep per cluster (§3.7); the boot gate just refuses an absent sidecar.
async fn ensure_sidecar_ready() -> Result<()> {
    use std::time::Duration;
    let cfg = bulletin_core::summarize::SummarizationConfig::from_env();
    let timeout_secs = std::env::var("BULLETIN_LLM_STARTUP_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(60);
    tracing::info!(
        base_url = %cfg.base_url,
        timeout_s = timeout_secs,
        "verifying summarization sidecar is reachable"
    );
    bulletin_core::summarize::client::ensure_reachable(&cfg, Duration::from_secs(timeout_secs))
        .await
        .context(
            "summarization sidecar unreachable at startup. Summarization is required (§3.7): bring up \
             the llama-server sidecar and check BULLETIN_LLM_BASE_URL",
        )?;
    tracing::info!("summarization sidecar reachable");
    Ok(())
}

/// The `summary-eval` command (the `digest-explain` hook, `docs/llm-summarization.md` §3.4/§7): a
/// read-only faithfulness eval that generates candidate summaries for a sample of historical public
/// clusters, runs them through the faithfulness gate, and prints the Vectara-style accuracy rate —
/// without storing anything. The sidecar must be reachable (the eval can't measure the model without
/// it), so this gates on it up front like the worker does.
async fn run_summary_eval(database_url: Option<&str>, limit: i64) -> Result<()> {
    let database_url = database_url.context(DATABASE_URL_REQUIRED)?;
    ensure_sidecar_ready().await?;
    let pool = connect_pool(database_url).await?;
    let cfg = bulletin_core::summarize::SummarizationConfig::from_env();
    let report = bulletin_core::summarize::eval_public(&pool, &cfg, limit)
        .await
        .context("faithfulness eval failed")?;
    println!("{report}");
    Ok(())
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
