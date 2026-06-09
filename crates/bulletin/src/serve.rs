use anyhow::{Context, Result};
use axum::{routing::get, Router};
use std::net::SocketAddr;

async fn health() -> &'static str {
    "ok"
}

pub async fn start(addr: SocketAddr) -> Result<()> {
    let app = Router::new().route("/health", get(health));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind to {addr}"))?;
    axum::serve(listener, app).await.context("HTTP server error")?;
    Ok(())
}
