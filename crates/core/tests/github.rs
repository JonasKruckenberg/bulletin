//! GitHub connector coverage: the central event-map normalization + dedup identity (pure, no DB),
//! and a poll/reconciliation cycle against a local mock of the REST API (no DB, mirrors poll_rss).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use bulletin_core::ingest::github::app::GithubApp;
use bulletin_core::ingest::github::event_map::{self, GithubEvent};
use bulletin_core::ingest::github::token::StaticTokenProvider;
use bulletin_core::ingest::github::webhook::{self, GithubWebhook};
use bulletin_core::ingest::github::GithubConnection;
use bulletin_core::ingest::realtime::{
    Inbound, LifecycleStatus, RealtimeConnector, RealtimeDispatch, Verified, WebhookHeaders,
};
use bulletin_core::ingest::Connection;
use bulletin_core::kind::{ContentKind, SourceKind};
use bulletin_core::scope::Scope;
use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;
use tokio::net::TcpListener;
use uuid::Uuid;

fn event(value: serde_json::Value) -> GithubEvent {
    serde_json::from_value(value).expect("fixture is a valid GithubEvent")
}

// ── event_map: per-type normalization ───────────────────────────────────

#[test]
fn issue_event_maps_to_longform_thread() {
    let ev = event(json!({
        "id": "100", "type": "IssuesEvent", "actor": {"login": "alice"},
        "repo": {"name": "octo/repo"}, "created_at": "2026-06-10T10:00:00Z",
        "payload": {"action": "opened", "issue": {
            "id": 555, "number": 42, "title": "A bug",
            "html_url": "https://github.com/octo/repo/issues/42"
        }}
    }));
    assert_eq!(event_map::stable_id(&ev), "issue:555:opened");

    let new = event_map::to_builder(ev).finalize(None);
    assert_eq!(new.source, SourceKind::Github);
    assert_eq!(new.content_kind, ContentKind::Longform);
    assert_eq!(new.group_key, "gh:octo/repo#issue-42");
    assert!(new.title.contains("issue opened: A bug"));
    assert_eq!(new.links, vec!["https://github.com/octo/repo/issues/42"]);
    // Structural entities are namespaced (`repo:`/`user:`); `finalize` also derives the `url:`/
    // `domain:` keys from the html_url. Sorted + deduped.
    assert_eq!(
        new.entities,
        vec![
            "domain:github.com".to_string(),
            "repo:octo/repo".to_string(),
            "url:https://github.com/octo/repo/issues/42".to_string(),
            "user:alice".to_string(),
        ]
    );
}

#[test]
fn release_event_is_an_announcement() {
    let ev = event(json!({
        "id": "101", "type": "ReleaseEvent", "actor": {"login": "bob"},
        "repo": {"name": "octo/repo"}, "created_at": "2026-06-10T11:00:00Z",
        "payload": {"action": "published", "release": {
            "id": 777, "name": "v1.2.0", "tag_name": "v1.2.0",
            "html_url": "https://github.com/octo/repo/releases/tag/v1.2.0"
        }}
    }));
    assert_eq!(event_map::stable_id(&ev), "release:777:published");
    let new = event_map::to_builder(ev).finalize(None);
    assert_eq!(new.content_kind, ContentKind::Announcement);
    assert_eq!(new.group_key, "gh:octo/repo@release-v1.2.0");
}

#[test]
fn push_event_identity_is_head_sha() {
    let ev = event(json!({
        "id": "102", "type": "PushEvent", "actor": {"login": "carol"},
        "repo": {"name": "octo/repo"}, "created_at": "2026-06-10T12:00:00Z",
        "payload": {"ref": "refs/heads/main", "head": "abc123"}
    }));
    assert_eq!(event_map::stable_id(&ev), "push:octo/repo:abc123");
    let new = event_map::to_builder(ev).finalize(None);
    assert_eq!(new.content_kind, ContentKind::Message);
    assert_eq!(new.group_key, "gh:octo/repo@refs/heads/main");
}

// Unrecognized activity is *captured*, not dropped — it normalizes to a generic message event so
// it can be promoted to a richer mapping later (one-file change in event_map).
#[test]
fn unknown_event_is_captured_generically() {
    let ev = event(json!({
        "id": "103", "type": "WatchEvent", "actor": {"login": "dave"},
        "repo": {"name": "octo/repo"}, "created_at": "2026-06-10T13:00:00Z",
        "payload": {"action": "started"}
    }));
    assert_eq!(event_map::stable_id(&ev), "WatchEvent:103");
    let new = event_map::to_builder(ev).finalize(None);
    assert_eq!(new.content_kind, ContentKind::Message);
    assert_eq!(new.group_key, "gh:octo/repo:WatchEvent");
    assert!(new.title.contains("WatchEvent"));
}

// The fingerprint is content-independent (§5.2): the same activity re-observed — by a re-poll or
// (Phase 2) a webhook — collapses on `UNIQUE(fingerprint)`. Identity is the issue id + action, not
// the REST event id, so a differing title/event-id does not spawn a duplicate.
#[test]
fn same_activity_dedups_across_differing_event_ids() {
    let a = event_map::to_builder(event(json!({
        "id": "200", "type": "IssuesEvent", "repo": {"name": "octo/repo"},
        "created_at": "2026-06-10T10:00:00Z",
        "payload": {"action": "opened", "issue": {"id": 555, "number": 42, "title": "First title"}}
    })))
    .finalize(None);
    let b = event_map::to_builder(event(json!({
        "id": "999", "type": "IssuesEvent", "repo": {"name": "octo/repo"},
        "created_at": "2026-06-10T10:05:00Z",
        "payload": {"action": "opened", "issue": {"id": 555, "number": 42, "title": "Edited title"}}
    })))
    .finalize(None);
    assert_eq!(a.fingerprint, b.fingerprint, "same activity → one event");
}

#[test]
fn distinct_activities_never_collide() {
    let a = event_map::to_builder(event(json!({
        "id": "1", "type": "IssuesEvent", "repo": {"name": "octo/repo"},
        "created_at": "2026-06-10T10:00:00Z",
        "payload": {"action": "opened", "issue": {"id": 1, "number": 1, "title": "x"}}
    })))
    .finalize(None);
    let b = event_map::to_builder(event(json!({
        "id": "2", "type": "IssuesEvent", "repo": {"name": "octo/repo"},
        "created_at": "2026-06-10T10:00:00Z",
        "payload": {"action": "closed", "issue": {"id": 1, "number": 1, "title": "x"}}
    })))
    .finalize(None);
    assert_ne!(
        a.fingerprint, b.fingerprint,
        "open vs close are distinct events"
    );
}

// ── visibility → scope (Phase 3) ─────────────────────────────────────────

// Visibility threads from the source object all the way to `Scope`: a private-repo event (REST feed
// `public:false`) finalizes to `Private(owner)`, a public one stays shared even on an owned
// connection, and a private item from an *ownerless* connection can't be bound to anyone so it stays
// public. The adapter only ever reports the bool; `finalize` owns the subscriber binding.
#[test]
fn visibility_threads_through_to_scope() {
    let owner = Uuid::from_u128(42);

    let private = event_map::to_builder(event(json!({
        "id": "500", "type": "IssuesEvent", "repo": {"name": "octo/secret"},
        "created_at": "2026-06-10T10:00:00Z", "public": false,
        "payload": {"action": "opened", "issue": {"id": 9, "number": 1, "title": "private"}}
    })))
    .finalize(Some(owner));
    assert_eq!(private.scope, Scope::Private(owner));

    // Public repo (the feed default `public:true`) stays shared even though the connection is owned.
    let public = event_map::to_builder(event(json!({
        "id": "501", "type": "IssuesEvent", "repo": {"name": "octo/open"},
        "created_at": "2026-06-10T10:00:00Z",
        "payload": {"action": "opened", "issue": {"id": 10, "number": 2, "title": "public"}}
    })))
    .finalize(Some(owner));
    assert_eq!(public.scope, Scope::Public);

    // Private item, ownerless connection → no subscriber to bind to, so it can't become private.
    let ownerless = event_map::to_builder(event(json!({
        "id": "502", "type": "IssuesEvent", "repo": {"name": "octo/secret"},
        "created_at": "2026-06-10T10:00:00Z", "public": false,
        "payload": {"action": "opened", "issue": {"id": 11, "number": 3, "title": "x"}}
    })))
    .finalize(None);
    assert_eq!(ownerless.scope, Scope::Public);
}

// A webhook from a private repo carries `repository.private:true` (not a top-level `public` flag);
// it threads to the same owner scope as the poll path.
#[test]
fn private_webhook_finalizes_to_owner_scope() {
    let owner = Uuid::from_u128(7);
    let body = json!({
        "action": "opened",
        "issue": {"id": 555, "number": 42, "title": "A bug"},
        "repository": {"full_name": "octo/secret", "private": true},
        "sender": {"login": "alice"},
        "installation": {"id": 1}
    });
    let ev =
        event_map::to_builder(event_map::from_webhook("issues", "d-1", body)).finalize(Some(owner));
    assert_eq!(ev.scope, Scope::Private(owner));
}

// ── webhook intake: dedup-against-poll, signature verify, routing, lifecycle ──

// A webhook `issues` delivery and the poll's REST `IssuesEvent` for the same issue produce one
// event: the webhook body is wrapped as the REST payload shape, so `stable_id` (issue id + action)
// matches and `UNIQUE(fingerprint)` collapses the overlap — even with a differing title.
#[test]
fn webhook_issue_dedups_against_polled_event() {
    let polled = event_map::to_builder(event(json!({
        "id": "300", "type": "IssuesEvent", "actor": {"login": "alice"},
        "repo": {"name": "octo/repo"}, "created_at": "2026-06-10T10:00:00Z",
        "payload": {"action": "opened", "issue": {
            "id": 555, "number": 42, "title": "A bug",
            "html_url": "https://github.com/octo/repo/issues/42"
        }}
    })))
    .finalize(None);

    // The `issues` webhook body: action + issue at the top level, plus repository/sender/installation.
    let body = json!({
        "action": "opened",
        "issue": {
            "id": 555, "number": 42, "title": "A bug (edited later)",
            "html_url": "https://github.com/octo/repo/issues/42",
            "updated_at": "2026-06-10T10:05:00Z"
        },
        "repository": {"full_name": "octo/repo", "private": false},
        "sender": {"login": "alice"},
        "installation": {"id": 12345}
    });
    let hooked =
        event_map::to_builder(event_map::from_webhook("issues", "delivery-1", body)).finalize(None);

    assert_eq!(
        polled.fingerprint, hooked.fingerprint,
        "webhook + poll for the same issue collapse to one event"
    );
    assert_eq!(hooked.source, SourceKind::Github);
    assert_eq!(hooked.group_key, "gh:octo/repo#issue-42");
    assert_eq!(hooked.content_kind, ContentKind::Longform);
}

// Push identity is the head SHA on both intakes (REST `payload.head`, webhook top-level `after`).
#[test]
fn webhook_push_identity_matches_polled_head_sha() {
    let polled = event_map::to_builder(event(json!({
        "id": "299", "type": "PushEvent", "actor": {"login": "bob"},
        "repo": {"name": "octo/repo"}, "created_at": "2026-06-10T09:00:00Z",
        "payload": {"ref": "refs/heads/main", "head": "deadbeef"}
    })))
    .finalize(None);

    let body = json!({
        "ref": "refs/heads/main", "after": "deadbeef",
        "head_commit": {"timestamp": "2026-06-10T09:00:00Z"},
        "repository": {"full_name": "octo/repo"},
        "sender": {"login": "bob"},
        "installation": {"id": 1}
    });
    let hooked =
        event_map::to_builder(event_map::from_webhook("push", "delivery-2", body)).finalize(None);

    assert_eq!(polled.fingerprint, hooked.fingerprint);
}

fn sign(secret: &[u8], body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

fn headers(sig: Option<&str>, event_type: &str) -> WebhookHeaders {
    WebhookHeaders {
        signature: sig.map(String::from),
        event_type: Some(event_type.to_string()),
        delivery_id: Some("d-1".to_string()),
    }
}

#[test]
fn verify_accepts_a_valid_signature() {
    let secret = b"s3cr3t";
    let body = br#"{"action":"opened"}"#;
    let wh = GithubWebhook::new(Some(secret.to_vec()));
    let sig = sign(secret, body);
    assert!(matches!(
        wh.verify(&headers(Some(&sig), "issues"), body),
        Verified::Authentic
    ));
}

#[test]
fn verify_rejects_a_tampered_body() {
    let secret = b"s3cr3t";
    let wh = GithubWebhook::new(Some(secret.to_vec()));
    let sig = sign(secret, br#"{"action":"opened"}"#);
    // Same signature, different body → the recomputed MAC won't match.
    assert!(matches!(
        wh.verify(&headers(Some(&sig), "issues"), br#"{"action":"closed"}"#),
        Verified::Invalid
    ));
}

#[test]
fn verify_rejects_a_wrong_secret() {
    let wh = GithubWebhook::new(Some(b"right".to_vec()));
    let sig = sign(b"wrong", br#"{"x":1}"#);
    assert!(matches!(
        wh.verify(&headers(Some(&sig), "issues"), br#"{"x":1}"#),
        Verified::Invalid
    ));
}

#[test]
fn verify_fails_closed_without_a_secret() {
    let wh = GithubWebhook::new(None);
    let sig = sign(b"any", br#"{}"#);
    assert!(matches!(
        wh.verify(&headers(Some(&sig), "ping"), br#"{}"#),
        Verified::Invalid
    ));
}

#[test]
fn verify_rejects_missing_or_malformed_signature() {
    let wh = GithubWebhook::new(Some(b"s".to_vec()));
    assert!(matches!(
        wh.verify(&headers(None, "issues"), b"{}"),
        Verified::Invalid
    ));
    // No "sha256=" prefix.
    assert!(matches!(
        wh.verify(&headers(Some("deadbeef"), "issues"), b"{}"),
        Verified::Invalid
    ));
}

#[test]
fn route_extracts_the_installation_id() {
    let body = json!({"action": "opened", "installation": {"id": 98765}}).to_string();
    assert_eq!(webhook::route(body.as_bytes()).unwrap(), "98765");
}

#[test]
fn route_errors_when_installation_is_missing() {
    let body = json!({"action": "opened"}).to_string();
    assert!(webhook::route(body.as_bytes()).is_err());
}

#[test]
fn installation_deleted_is_a_revoke_lifecycle() {
    let dispatch = RealtimeDispatch::build(SourceKind::Github).unwrap();
    let body = json!({"action": "deleted", "installation": {"id": 1}}).to_string();
    match dispatch
        .accept_and_normalize("installation", "d", body.as_bytes())
        .unwrap()
    {
        Inbound::Lifecycle(c) => assert_eq!(c.status, LifecycleStatus::Revoked),
        Inbound::Events(_) => panic!("expected a lifecycle change"),
    }
}

#[test]
fn content_webhook_normalizes_through_the_dispatch() {
    let dispatch = RealtimeDispatch::build(SourceKind::Github).unwrap();
    let body = json!({
        "action": "opened",
        "issue": {"id": 7, "number": 3, "title": "t"},
        "repository": {"full_name": "o/r"},
        "sender": {"login": "a"},
        "installation": {"id": 1}
    })
    .to_string();
    match dispatch
        .accept_and_normalize("issues", "d", body.as_bytes())
        .unwrap()
    {
        Inbound::Events(builders) => {
            assert_eq!(builders.len(), 1);
            let ev = builders.into_iter().next().unwrap().finalize(None);
            assert_eq!(ev.group_key, "gh:o/r#issue-3");
        }
        Inbound::Lifecycle(_) => panic!("expected events"),
    }
}

// ── poll: reconciliation against a mocked REST API ───────────────────────

const EVENTS_FEED: &str = r#"[
  {"id":"300","type":"IssuesEvent","actor":{"login":"alice"},"repo":{"name":"octo/repo"},
   "created_at":"2026-06-10T10:00:00Z",
   "payload":{"action":"opened","issue":{"id":1,"number":7,"title":"Newest","html_url":"https://x/7"}}},
  {"id":"299","type":"PushEvent","actor":{"login":"bob"},"repo":{"name":"octo/repo"},
   "created_at":"2026-06-10T09:00:00Z","payload":{"ref":"refs/heads/main","head":"deadbeef"}}
]"#;

async fn events_handler(headers: HeaderMap) -> impl IntoResponse {
    // Conditional GET: a matching validator means "nothing new" → 304, no body.
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("v1"))
    {
        return (StatusCode::NOT_MODIFIED, [(header::ETAG, "\"v1\"")]).into_response();
    }
    (StatusCode::OK, [(header::ETAG, "\"v1\"")], EVENTS_FEED).into_response()
}

async fn serve_github() -> String {
    let app = Router::new()
        .route(
            "/installation/repositories",
            get(|| async { r#"{"repositories":[{"full_name":"octo/repo"}]}"# }),
        )
        .route("/repos/octo/repo/events", get(events_handler));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{port}")
}

fn connection(base_url: &str) -> GithubConnection {
    GithubConnection::new(base_url, Arc::new(StaticTokenProvider::new("ghs_test")))
}

#[tokio::test]
async fn first_poll_reconciles_all_repo_events() {
    let base = serve_github().await;
    let conn = connection(&base);

    let batch = conn.poll(Default::default()).await.expect("poll succeeds");
    assert_eq!(batch.items.len(), 2);

    let builders: Vec<_> = batch
        .items
        .into_iter()
        .flat_map(|i| conn.to_events(i))
        .collect();
    assert_eq!(builders.len(), 2);

    // Cursor recorded the repo's validator + high-water mark for the next (conditional) poll.
    let repo = batch.cursor.repos.get("octo/repo").expect("repo cursor");
    assert_eq!(repo.etag.as_deref(), Some("\"v1\""));
    assert_eq!(repo.last_event_id.as_deref(), Some("300"));
}

#[tokio::test]
async fn second_poll_is_conditional_and_returns_nothing() {
    let base = serve_github().await;
    let conn = connection(&base);

    let first = conn.poll(Default::default()).await.unwrap();
    // Re-poll with the recorded cursor → If-None-Match → 304 → no new items.
    let second = conn.poll(first.cursor).await.unwrap();
    assert!(
        second.items.is_empty(),
        "unchanged feed yields nothing on a conditional re-poll"
    );
}

// ── GitHub App: real installation-token minting (RS256 JWT → access token) ─
//
// A throwaway 2048-bit RSA key (generated for the test, never a real App key) signs the app JWT;
// the mock asserts the exchange presents a bearer JWT and counts mints to prove the per-connection
// expiry cache. This is the Phase 5 seam that flips `ConnectorCtx.github` from `None` to a live
// token factory — "a real (operator-seeded) install ingests end-to-end via poll".
const TEST_APP_KEY: &[u8] = include_bytes!("fixtures/github_app_test_key.pem");

#[derive(Clone)]
struct AppMockState {
    mints: Arc<AtomicUsize>,
    expires_at: String,
}

async fn access_token_handler(
    State(state): State<AppMockState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    // The exchange must authenticate with a signed app JWT as a bearer token (JWTs start "ey…").
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(
        auth.starts_with("Bearer ey"),
        "token exchange must present a JWT bearer, got {auth:?}"
    );
    state.mints.fetch_add(1, Ordering::SeqCst);
    let body = format!(
        r#"{{"token":"ghs_minted_token","expires_at":"{}"}}"#,
        state.expires_at
    );
    (StatusCode::CREATED, body).into_response()
}

async fn serve_github_app(mints: Arc<AtomicUsize>, expires_at: &str) -> String {
    let state = AppMockState {
        mints,
        expires_at: expires_at.to_string(),
    };
    let app = Router::new()
        .route(
            "/app/installations/42/access_tokens",
            post(access_token_handler),
        )
        .route(
            "/installation/repositories",
            get(|| async { r#"{"repositories":[{"full_name":"octo/repo"}]}"# }),
        )
        .route("/repos/octo/repo/events", get(events_handler))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{port}")
}

#[tokio::test]
async fn app_mints_installation_token_then_caches_it() {
    let mints = Arc::new(AtomicUsize::new(0));
    let base = serve_github_app(mints.clone(), "2999-01-01T00:00:00Z").await;
    let app = GithubApp::new(&base, 12345, TEST_APP_KEY).expect("valid app key");
    let conn = GithubConnection::new(&base, app.installation_tokens(42));

    let batch = conn.poll(Default::default()).await.expect("poll succeeds");
    assert_eq!(batch.items.len(), 2);
    assert_eq!(
        mints.load(Ordering::SeqCst),
        1,
        "one mint for the first poll"
    );

    // The far-future token is reused on the next poll — no second exchange.
    conn.poll(batch.cursor).await.expect("second poll");
    assert_eq!(
        mints.load(Ordering::SeqCst),
        1,
        "a live cached token is not re-minted"
    );
}

#[tokio::test]
async fn app_refreshes_an_expired_token() {
    let mints = Arc::new(AtomicUsize::new(0));
    // An already-expired token forces a re-mint on every access.
    let base = serve_github_app(mints.clone(), "2000-01-01T00:00:00Z").await;
    let app = GithubApp::new(&base, 12345, TEST_APP_KEY).expect("valid app key");
    let conn = GithubConnection::new(&base, app.installation_tokens(42));

    conn.poll(Default::default()).await.expect("first poll");
    conn.poll(Default::default()).await.expect("second poll");
    assert_eq!(
        mints.load(Ordering::SeqCst),
        2,
        "an expired token is re-minted each poll"
    );
}

#[test]
fn app_rejects_a_non_pem_private_key() {
    assert!(GithubApp::new("https://api.github.com", 1, b"not a pem").is_err());
}
