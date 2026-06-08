mod serve;
mod worker;

use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Parser)]
#[command(name = "bulletin")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve,
    Worker,
    Migrate,
    All,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();
    let database_url = std::env::var("DATABASE_URL")?;

    match cli.command {
        Command::Migrate => {
            let pool = bulletin_store::connect(&database_url).await?;
            bulletin_store::migrate(&pool).await?;
            worker::setup_storage(&pool).await?;
        }
        Command::Serve => {
            let addr: SocketAddr = "0.0.0.0:3000".parse()?;
            serve::start(addr).await?;
        }
        Command::Worker => {
            let pool = bulletin_store::connect(&database_url).await?;
            worker::start(pool).await?;
        }
        Command::All => {
            let pool = bulletin_store::connect(&database_url).await?;
            let addr: SocketAddr = "0.0.0.0:3000".parse()?;
            tokio::try_join!(serve::start(addr), worker::start(pool))?;
        }
    }

    Ok(())
}
