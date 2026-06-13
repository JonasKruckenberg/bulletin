//! The GitHub connector's per-connection worker.
//!
//! GitHub is `Connection + RealtimeConnection` (design §5.4): webhooks (Phase 2) carry freshness,
//! but the **poll is the correctness floor** — a cursor-driven reconciliation over the REST events
//! feed that recovers anything a lossy webhook dropped, with `UNIQUE(fingerprint)` collapsing the
//! overlap. This module is the poll foundation + the shared `to_events`; the realtime head lands in
//! Phase 2 on top of the same normalization.
//!
//! Reconciliation walks the installation's repos and reads each repo's recent activity
//! (`GET /repos/{owner}/{repo}/events`) with a per-repo conditional GET (ETag) and a last-seen-id
//! high-water mark, so a quiet repo costs one 304 and a busy one yields only what's new. The unified
//! events feed is exactly the "capture everything, classify in one place" surface
//! ([`event_map`]).

pub mod event_map;
pub mod token;
pub mod webhook;

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::common::event::EventBuilder;
use crate::ingest::realtime::{Inbound, LifecycleChange, RealtimeConnection};
use crate::ingest::{Batch, Connection, SourceError};
use event_map::GithubEvent;
use token::TokenProvider;

/// Default REST API root; overridable so tests can point at a local mock server.
pub const DEFAULT_API_BASE: &str = "https://api.github.com";

/// Per-connection config persisted in `connection.config`. The `installation_id` is the App
/// installation this connection ingests + the webhook routing key (Phase 2) — **not a secret**
/// (design §3A). An explicit `repos` allowlist is optional; absent, we reconcile every repo the
/// installation can see.
#[derive(Debug, Clone, Deserialize)]
pub struct GithubConfig {
    pub installation_id: i64,
    #[serde(default)]
    pub repos: Option<Vec<String>>,
}

/// Opaque poll cursor: per-repo conditional-GET validator + the newest activity id already seen,
/// so the next poll fetches only newer events (the feed is newest-first).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GithubCursor {
    #[serde(default)]
    pub repos: HashMap<String, RepoCursor>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RepoCursor {
    pub etag: Option<String>,
    pub last_event_id: Option<String>,
}

/// The live worker for one GitHub `connection` row.
pub struct GithubConnection {
    client: reqwest::Client,
    base_url: String,
    token: Arc<dyn TokenProvider>,
    /// Explicit repo allowlist; `None` → discover via `/installation/repositories`.
    repos: Option<Vec<String>>,
}

impl GithubConnection {
    pub fn new(base_url: impl Into<String>, token: Arc<dyn TokenProvider>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
            token,
            repos: None,
        }
    }

    pub fn with_repos(mut self, repos: Option<Vec<String>>) -> Self {
        self.repos = repos;
        self
    }

    /// A realtime-only worker for the webhook path: it normalizes deliveries (`accept_webhook` /
    /// `to_events`, all pure, no network) but must never `poll` — its token provider errors if
    /// asked. This lets webhooks ingest before the App credentials (`ConnectorCtx.github`) are wired
    /// in Phase 5: webhook normalization needs no token, only the poll does.
    pub fn realtime_only() -> Self {
        Self::new(DEFAULT_API_BASE, Arc::new(token::UnavailableToken))
    }

    /// Common header set: bearer auth, the JSON media type, the pinned API version, and the UA
    /// GitHub requires. The installation token is short-lived and never logged.
    fn authed(&self, req: reqwest::RequestBuilder, token: &str) -> reqwest::RequestBuilder {
        req.bearer_auth(token)
            .header(reqwest::header::ACCEPT, "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .header(reqwest::header::USER_AGENT, "bulletin")
    }

    /// The repos to reconcile: the configured allowlist, or every repo the installation can see.
    async fn repos_to_poll(&self, token: &str) -> Result<Vec<String>, SourceError> {
        if let Some(repos) = &self.repos {
            return Ok(repos.clone());
        }
        let url = format!("{}/installation/repositories", self.base_url);
        let resp = self
            .authed(self.client.get(&url), token)
            .send()
            .await
            .map_err(|e| SourceError::Request(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(SourceError::Request(format!(
                "list repositories: HTTP {}",
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| SourceError::Request(e.to_string()))?;
        let parsed: InstallationRepos = serde_json::from_slice(&bytes)
            .map_err(|e| SourceError::Parse(format!("installation repositories: {e}")))?;
        Ok(parsed
            .repositories
            .into_iter()
            .map(|r| r.full_name)
            .collect())
    }

    /// Fetch one repo's recent activity newer than the cursor's high-water mark. Returns the events
    /// (newest-first, trimmed to those after `last_event_id`) and the cursor to persist for it.
    async fn poll_repo(
        &self,
        token: &str,
        repo: &str,
        cursor: RepoCursor,
    ) -> Result<(Vec<GithubEvent>, RepoCursor), SourceError> {
        let url = format!("{}/repos/{repo}/events", self.base_url);
        let mut req = self.authed(self.client.get(&url), token);
        if let Some(etag) = &cursor.etag {
            req = req.header(reqwest::header::IF_NONE_MATCH, etag.as_str());
        }
        let resp = req
            .send()
            .await
            .map_err(|e| SourceError::Request(e.to_string()))?;

        if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok((vec![], cursor)); // nothing new for this repo
        }
        if !resp.status().is_success() {
            return Err(SourceError::Request(format!(
                "repo events {repo}: HTTP {}",
                resp.status()
            )));
        }

        let new_etag = resp
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| SourceError::Request(e.to_string()))?;
        let all: Vec<GithubEvent> = serde_json::from_slice(&bytes)
            .map_err(|e| SourceError::Parse(format!("repo events {repo}: {e}")))?;

        // Feed is newest-first; take until we reach the last id we already ingested.
        let fresh: Vec<GithubEvent> = match &cursor.last_event_id {
            Some(seen) => all.into_iter().take_while(|e| &e.id != seen).collect(),
            None => all, // first poll: take the page
        };
        // Advance the high-water mark to the newest id observed (even if it was a 304-less empty
        // page); keep the prior one when this page had nothing.
        let newest = fresh.first().map(|e| e.id.clone()).or(cursor.last_event_id);

        Ok((
            fresh,
            RepoCursor {
                etag: new_etag,
                last_event_id: newest,
            },
        ))
    }
}

#[derive(Deserialize)]
struct InstallationRepos {
    #[serde(default)]
    repositories: Vec<RepoEntry>,
}

#[derive(Deserialize)]
struct RepoEntry {
    full_name: String,
}

impl Connection for GithubConnection {
    type Cursor = GithubCursor;
    type Item = GithubEvent;

    async fn poll(
        &self,
        cursor: Self::Cursor,
    ) -> Result<Batch<Self::Item, Self::Cursor>, SourceError> {
        let token = self.token.access_token().await?;
        let repos = self.repos_to_poll(&token.secret).await?;

        let mut items = Vec::new();
        let mut next = GithubCursor::default();
        for repo in repos {
            let repo_cursor = cursor.repos.get(&repo).cloned().unwrap_or_default();
            let (events, new_cursor) = self.poll_repo(&token.secret, &repo, repo_cursor).await?;
            items.extend(events);
            next.repos.insert(repo, new_cursor);
        }

        Ok(Batch {
            items,
            cursor: next,
        })
    }

    fn to_events(&self, item: Self::Item) -> Vec<EventBuilder> {
        vec![event_map::to_builder(item)]
    }
}

impl RealtimeConnection for GithubConnection {
    /// Normalize a verified GitHub webhook delivery. App lifecycle events (`installation` /
    /// `installation_repositories`) become a status change; everything else is synthesized into the
    /// same [`GithubEvent`] shape the poll yields, so the two intakes dedup. `hydrate` is the
    /// default identity — GitHub webhook payloads are already complete (no follow-up fetch).
    fn accept_webhook(
        &self,
        event_type: &str,
        delivery_id: &str,
        body: &[u8],
    ) -> Result<Inbound<Self::Item>, SourceError> {
        let value: serde_json::Value = serde_json::from_slice(body)
            .map_err(|e| SourceError::Parse(format!("webhook body is not JSON: {e}")))?;

        if matches!(event_type, "installation" | "installation_repositories") {
            return Ok(match event_map::lifecycle_status(&value) {
                Some(status) => Inbound::Lifecycle(LifecycleChange { status }),
                // install / new_permissions / repos add-remove: nothing to ingest, status unchanged.
                None => Inbound::Events(vec![]),
            });
        }

        Ok(Inbound::Events(vec![event_map::from_webhook(
            event_type,
            delivery_id,
            value,
        )]))
    }
}
