use axum::{routing::get, Router};
use std::net::SocketAddr;

async fn health() -> &'static str {
    "ok"
}

pub async fn start(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new().route("/health", get(health));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
