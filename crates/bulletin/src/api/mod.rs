//! The `bulletin api` role: a tonic gRPC server exposing the **admin plane** (control-plane
//! management + read) over the wire, mirroring the `debug` operator commands. A third *trigger* over
//! the engine alongside `serve`/`worker` and the CLI — it adds no business logic, it authenticates a
//! caller and calls the same self-scoping `core` functions.
//!
//! Auth → scope: the admin bearer authorizes the admin plane; the `core` store fns then run their work
//! in `ScopeCtx::Admin`. The subscriber plane (per-subscriber tokens → `ScopeCtx::Subscriber`) is A2.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use sqlx::PgPool;
use tonic::transport::Server;

mod admin;
mod auth;
mod convert;
mod error;

/// The generated protobuf types + service stubs (`proto/bulletin/v1/bulletin.proto`), plus the encoded
/// descriptor set the reflection service serves so `grpcurl`/IDE tooling can introspect without the
/// `.proto`.
pub mod proto {
    tonic::include_proto!("bulletin.v1");
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/bulletin_descriptor.bin"));
}

use crate::transport::EmailConfig;
use auth::{admin_interceptor, AuthState};
use proto::admin_service_server::AdminServiceServer;
use proto::unstable_debug_service_server::UnstableDebugServiceServer;

/// Runs the gRPC API server until shutdown. `admin_key` authorizes the admin plane; without it every
/// admin RPC is rejected (fail-closed) and we warn once at startup, mirroring the webhook catcher.
/// `email` is the engine's delivery config: the digest-send RPCs build the mailer from it server-side,
/// so the `debug` CLI can fire a digest without holding the SMTP credential locally.
pub async fn serve(
    addr: SocketAddr,
    pool: PgPool,
    admin_key: Option<String>,
    email: EmailConfig,
) -> Result<()> {
    if admin_key.is_none() {
        tracing::warn!(
            "no API admin key configured; all gRPC admin calls will be rejected \
             (set --api-admin-key or BULLETIN_API_ADMIN_KEY)"
        );
    }
    let auth = Arc::new(AuthState::new(admin_key));
    // One `AdminApi` backs both planes. Auth is enforced by an interceptor over each whole service
    // (so the handlers carry no per-RPC auth check — one chokepoint per service, no by-omission gap).
    // The unstable debug plane is its own service (its instability lives in the name) but runs under
    // the same admin bearer.
    let api = admin::AdminApi::new(pool, email);
    let admin = AdminServiceServer::with_interceptor(api.clone(), admin_interceptor(auth.clone()));
    let debug = UnstableDebugServiceServer::with_interceptor(api, admin_interceptor(auth));

    // gRPC server reflection (v1) so tooling can list services/methods over the wire.
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .context("build gRPC reflection service")?;

    // Standard grpc.health.v1 probe alongside `serve`'s HTTP /health.
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<AdminServiceServer<admin::AdminApi>>()
        .await;

    tracing::info!(%addr, "gRPC API listening");
    Server::builder()
        .add_service(admin)
        .add_service(debug)
        .add_service(reflection)
        .add_service(health_service)
        .serve(addr)
        .await
        .context("gRPC API server error")?;
    Ok(())
}
