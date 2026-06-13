//! The HTTP webhook catcher (`serve` role): authenticate a raw delivery at the edge, then enqueue a
//! `ProcessWebhook` job and return fast (design §3A). All parsing, connection resolution, and ingest
//! happen off the request path in the job — the edge does only HMAC verify + a quick 2xx, so a slow
//! DB or a burst of deliveries can't block GitHub's delivery timeout.

use std::sync::Arc;

use apalis::prelude::*;
use apalis_postgres::PostgresStorage;
use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use sqlx::PgPool;

use bulletin_core::ingest::github::webhook::GithubWebhook;
use bulletin_core::ingest::realtime::{RealtimeConnector, Verified, WebhookHeaders};
use bulletin_core::kind::SourceKind;

use crate::worker::{is_duplicate_enqueue, ProcessWebhookJob};

#[derive(Clone)]
struct WebhookState {
    pool: PgPool,
    github: Arc<GithubWebhook>,
}

/// Builds the `serve` router: liveness (`/health`) + the GitHub webhook catcher
/// (`POST /webhooks/github`). Without a webhook secret the catcher fails closed — every delivery is
/// rejected — and we log it once at startup so the misconfiguration is visible.
pub fn router(pool: PgPool, github_webhook_secret: Option<Vec<u8>>) -> Router {
    if github_webhook_secret.is_none() {
        tracing::warn!(
            "no GitHub webhook secret configured; deliveries to /webhooks/github will be rejected"
        );
    }
    let state = WebhookState {
        pool,
        github: Arc::new(GithubWebhook::new(github_webhook_secret)),
    };
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/webhooks/github", post(github_webhook))
        .with_state(state)
}

fn header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(String::from)
}

async fn github_webhook(
    State(state): State<WebhookState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let wh = WebhookHeaders {
        signature: header(&headers, "X-Hub-Signature-256"),
        event_type: header(&headers, "X-GitHub-Event"),
        delivery_id: header(&headers, "X-GitHub-Delivery"),
    };

    // Verify over the raw bytes BEFORE any parse — fail closed on anything but a valid signature.
    match state.github.verify(&wh, body.as_ref()) {
        Verified::Authentic => {}
        Verified::Challenge(echo) => return (StatusCode::OK, echo).into_response(),
        Verified::Invalid => {
            tracing::warn!("rejected webhook with invalid signature");
            return (StatusCode::UNAUTHORIZED, "invalid signature").into_response();
        }
    }

    let event_type = wh.event_type.unwrap_or_default();
    // `ping` is GitHub's delivery test — authenticated, but there's nothing to ingest.
    if event_type == "ping" {
        return (StatusCode::OK, "pong").into_response();
    }
    let delivery_id = wh.delivery_id.unwrap_or_default();

    let mut storage = PostgresStorage::<ProcessWebhookJob>::new(&state.pool);
    let task = TaskBuilder::new(ProcessWebhookJob {
        source: SourceKind::Github,
        event_type,
        delivery_id: delivery_id.clone(),
        raw_body: body.to_vec(),
    })
    // Collapse re-deliveries of the same X-GitHub-Delivery (GitHub retries on a non-2xx).
    .with_idempotency_key(format!("gh-webhook:{delivery_id}"))
    .build();

    match storage.push_task(task).await {
        Ok(()) => (StatusCode::ACCEPTED, "queued").into_response(),
        Err(e) if is_duplicate_enqueue(&e) => {
            (StatusCode::OK, "duplicate delivery").into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to enqueue webhook job");
            (StatusCode::INTERNAL_SERVER_ERROR, "enqueue failed").into_response()
        }
    }
}
