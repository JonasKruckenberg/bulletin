//! The HTTP webhook catcher (`serve` role): authenticate a raw delivery at the edge, then enqueue a
//! `ProcessWebhook` job and return fast (design §3A). All parsing, connection resolution, and ingest
//! happen off the request path in the job — the edge does only HMAC verify + a quick 2xx, so a slow
//! DB or a burst of deliveries can't block GitHub's delivery timeout.

use std::sync::Arc;

use apalis::prelude::*;
use apalis_postgres::PostgresStorage;
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
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

/// GitHub's documented maximum webhook payload size. axum's default extractor limit is 2 MB, which
/// would 413 the larger deliveries (big pushes/PRs) *after* they pass signature verification — and
/// GitHub then retries them forever — so we lift the cap to GitHub's own ceiling on this route.
const MAX_WEBHOOK_BODY: usize = 25 * 1024 * 1024;

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
        .route(
            "/webhooks/github",
            post(github_webhook).layer(DefaultBodyLimit::max(MAX_WEBHOOK_BODY)),
        )
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
    let mut builder = TaskBuilder::new(ProcessWebhookJob {
        source: SourceKind::Github,
        event_type,
        delivery_id: delivery_id.clone(),
        // A verified GitHub delivery is UTF-8 JSON; `_lossy` only guards a non-conforming sender and
        // never drops the delivery (the job re-parses the JSON anyway).
        body: String::from_utf8_lossy(&body).into_owned(),
    });
    // Collapse re-deliveries of the same X-GitHub-Delivery (GitHub retries on a non-2xx). Skip the
    // key if the header was absent — an empty id would alias every keyless delivery onto one job.
    if !delivery_id.is_empty() {
        builder = builder.with_idempotency_key(format!("gh-webhook:{delivery_id}"));
    }
    let task = builder.build();

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
