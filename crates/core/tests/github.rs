//! GitHub connector coverage: the central event-map normalization + dedup identity (pure, no DB),
//! and a poll/reconciliation cycle against a local mock of the REST API (no DB, mirrors poll_rss).

use std::sync::Arc;

use axum::{
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use bulletin_core::ingest::github::event_map::{self, GithubEvent};
use bulletin_core::ingest::github::token::StaticTokenProvider;
use bulletin_core::ingest::github::GithubConnection;
use bulletin_core::ingest::Connection;
use bulletin_core::kind::{ContentKind, SourceKind};
use bulletin_core::scope::Scope;
use serde_json::json;
use tokio::net::TcpListener;

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

    let new = event_map::to_builder(ev).finalize(Scope::Public);
    assert_eq!(new.source, SourceKind::Github);
    assert_eq!(new.content_kind, ContentKind::Longform);
    assert_eq!(new.group_key, "gh:octo/repo#issue-42");
    assert!(new.title.contains("issue opened: A bug"));
    assert_eq!(new.links, vec!["https://github.com/octo/repo/issues/42"]);
    assert_eq!(
        new.entities,
        vec!["octo/repo".to_string(), "alice".to_string()]
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
    let new = event_map::to_builder(ev).finalize(Scope::Public);
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
    let new = event_map::to_builder(ev).finalize(Scope::Public);
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
    let new = event_map::to_builder(ev).finalize(Scope::Public);
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
    .finalize(Scope::Public);
    let b = event_map::to_builder(event(json!({
        "id": "999", "type": "IssuesEvent", "repo": {"name": "octo/repo"},
        "created_at": "2026-06-10T10:05:00Z",
        "payload": {"action": "opened", "issue": {"id": 555, "number": 42, "title": "Edited title"}}
    })))
    .finalize(Scope::Public);
    assert_eq!(a.fingerprint, b.fingerprint, "same activity → one event");
}

#[test]
fn distinct_activities_never_collide() {
    let a = event_map::to_builder(event(json!({
        "id": "1", "type": "IssuesEvent", "repo": {"name": "octo/repo"},
        "created_at": "2026-06-10T10:00:00Z",
        "payload": {"action": "opened", "issue": {"id": 1, "number": 1, "title": "x"}}
    })))
    .finalize(Scope::Public);
    let b = event_map::to_builder(event(json!({
        "id": "2", "type": "IssuesEvent", "repo": {"name": "octo/repo"},
        "created_at": "2026-06-10T10:00:00Z",
        "payload": {"action": "closed", "issue": {"id": 1, "number": 1, "title": "x"}}
    })))
    .finalize(Scope::Public);
    assert_ne!(
        a.fingerprint, b.fingerprint,
        "open vs close are distinct events"
    );
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
