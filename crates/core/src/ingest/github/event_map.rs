//! The single, legible GitHub-activity → canonical-event mapping.
//!
//! GitHub speaks one vocabulary (`PushEvent`, `IssuesEvent`, `ReleaseEvent`, …); the rest of the
//! pipeline must not learn it. **Everything is captured** — known types are mapped richly, anything
//! unrecognized falls through to a generic event (never silently dropped), so new activity shows up
//! in digests and can be promoted to a richer mapping later by editing *only this file*.
//!
//! Two functions own the source semantics, both keyed off `GithubEvent.kind`:
//! - [`stable_id`] — the **dedup identity** folded into the fingerprint (§5.2). It is derived from
//!   *content* ids present in both the REST events feed and the webhook payload (issue/PR/release/
//!   comment ids, push head SHA), so the reconciliation poll and a webhook for the same activity
//!   collapse onto one event (`ON CONFLICT DO NOTHING`). It must **never** use the REST event id or
//!   the webhook delivery id — those differ across the two intakes and would defeat dedup.
//! - [`to_builder`] — title / `group_key` / `content_kind` / links / entities.
//!
//! `group_key` clusters within a source: issues+their comments share an issue key, PR activity
//! shares a PR key, so a thread groups into one cluster (§8.1).

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::common::{event::EventBuilder, kind::ContentKind, kind::SourceKind};

/// One activity from the REST events feed (`GET /repos/{owner}/{repo}/events`) or, later, a webhook
/// (Phase 2 maps a webhook body onto the same shape). `payload` is the type-specific blob we reach
/// into per [`stable_id`]/[`to_builder`]; tolerant by design (missing fields degrade, never panic).
#[derive(Debug, Clone, Deserialize)]
pub struct GithubEvent {
    /// REST events-feed id — used for poll pagination/cursoring, **not** for the fingerprint.
    #[serde(default)]
    pub id: String,
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub repo: RepoRef,
    #[serde(default)]
    pub actor: ActorRef,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RepoRef {
    /// `owner/repo`.
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ActorRef {
    #[serde(default)]
    pub login: String,
}

/// Read `payload.{path}.id` as a string (GitHub ids are large integers; keep them as text).
fn obj_id(ev: &GithubEvent, path: &str) -> Option<String> {
    ev.payload.get(path).and_then(|o| o.get("id")).map(|v| {
        v.as_i64()
            .map(|n| n.to_string())
            .unwrap_or_else(|| v.to_string())
    })
}

fn payload_str(ev: &GithubEvent, path: &[&str]) -> Option<String> {
    let mut cur = &ev.payload;
    for key in path {
        cur = cur.get(key)?;
    }
    cur.as_str().map(str::to_owned)
}

fn action(ev: &GithubEvent) -> Option<String> {
    payload_str(ev, &["action"])
}

/// The content-identity dedup key (see module docs). Falls back to the REST event id only for
/// activity we have no content id for — poll-only, and webhooks for those types are rare.
pub fn stable_id(ev: &GithubEvent) -> String {
    let repo = &ev.repo.name;
    let act = action(ev).unwrap_or_default();
    match ev.kind.as_str() {
        "IssuesEvent" => match obj_id(ev, "issue") {
            Some(id) => format!("issue:{id}:{act}"),
            None => fallback(ev),
        },
        "IssueCommentEvent" | "CommitCommentEvent" | "PullRequestReviewCommentEvent" => {
            match obj_id(ev, "comment") {
                Some(id) => format!("comment:{id}"),
                None => fallback(ev),
            }
        }
        "PullRequestEvent" => match obj_id(ev, "pull_request") {
            Some(id) => format!("pr:{id}:{act}"),
            None => fallback(ev),
        },
        "PullRequestReviewEvent" => match obj_id(ev, "review") {
            Some(id) => format!("review:{id}"),
            None => fallback(ev),
        },
        "ReleaseEvent" => match obj_id(ev, "release") {
            Some(id) => format!("release:{id}:{act}"),
            None => fallback(ev),
        },
        // Push identity = the head SHA (REST: payload.head; webhook: payload.after — Phase 2 maps
        // the webhook field to the same value), so a re-poll/webhook overlap collapses.
        "PushEvent" => match payload_str(ev, &["head"]).or_else(|| payload_str(ev, &["after"])) {
            Some(sha) => format!("push:{repo}:{sha}"),
            None => fallback(ev),
        },
        _ => fallback(ev),
    }
}

/// Last resort when no content id exists: the REST event id (unique within the feed). Poll-only.
fn fallback(ev: &GithubEvent) -> String {
    format!("{}:{}", ev.kind, ev.id)
}

/// `(scope, source, group_key)` is the cluster key (§5.3); within GitHub we group a thread's
/// activity (issue/PR + its comments + reviews) under one key so it lands in one cluster.
fn group_key(ev: &GithubEvent) -> String {
    let repo = &ev.repo.name;
    let issue_no = || payload_str(ev, &["issue", "number"]).or(num(ev, &["issue", "number"]));
    let pr_no =
        || payload_str(ev, &["pull_request", "number"]).or(num(ev, &["pull_request", "number"]));
    match ev.kind.as_str() {
        "IssuesEvent" | "IssueCommentEvent" => {
            format!("gh:{repo}#issue-{}", issue_no().unwrap_or_default())
        }
        "PullRequestEvent" | "PullRequestReviewEvent" | "PullRequestReviewCommentEvent" => {
            format!("gh:{repo}#pr-{}", pr_no().unwrap_or_default())
        }
        "ReleaseEvent" => format!(
            "gh:{repo}@release-{}",
            payload_str(ev, &["release", "tag_name"]).unwrap_or_default()
        ),
        "PushEvent" => format!(
            "gh:{repo}@{}",
            payload_str(ev, &["ref"]).unwrap_or_else(|| "push".to_owned())
        ),
        // One loose cluster per repo per activity type for everything else.
        other => format!("gh:{repo}:{other}"),
    }
}

fn num(ev: &GithubEvent, path: &[&str]) -> Option<String> {
    let mut cur = &ev.payload;
    for key in path {
        cur = cur.get(key)?;
    }
    cur.as_i64().map(|n| n.to_string())
}

/// Depth signal per activity (§5.1). Releases announce; issues/PRs are longform; chatter is a
/// message. The default for unmapped activity is the least-deep `Message`.
fn content_kind(kind: &str) -> ContentKind {
    match kind {
        "ReleaseEvent" => ContentKind::Announcement,
        "IssuesEvent" | "PullRequestEvent" => ContentKind::Longform,
        _ => ContentKind::Message,
    }
}

fn html_url(ev: &GithubEvent) -> Option<String> {
    // The object's web URL, by type — what a reader clicks through to.
    payload_str(ev, &["issue", "html_url"])
        .or_else(|| payload_str(ev, &["pull_request", "html_url"]))
        .or_else(|| payload_str(ev, &["release", "html_url"]))
        .or_else(|| payload_str(ev, &["comment", "html_url"]))
}

/// A short, human-readable headline per activity — the cluster's representative title.
fn title(ev: &GithubEvent) -> String {
    let repo = &ev.repo.name;
    let act = action(ev).unwrap_or_default();
    match ev.kind.as_str() {
        "IssuesEvent" => format!(
            "{repo} issue {act}: {}",
            payload_str(ev, &["issue", "title"]).unwrap_or_default()
        ),
        "PullRequestEvent" => format!(
            "{repo} PR {act}: {}",
            payload_str(ev, &["pull_request", "title"]).unwrap_or_default()
        ),
        "ReleaseEvent" => format!(
            "{repo} release {}",
            payload_str(ev, &["release", "name"])
                .or_else(|| payload_str(ev, &["release", "tag_name"]))
                .unwrap_or_default()
        ),
        "PushEvent" => format!(
            "{repo}: push to {}",
            payload_str(ev, &["ref"]).unwrap_or_else(|| "a branch".to_owned())
        ),
        "IssueCommentEvent" | "PullRequestReviewCommentEvent" | "CommitCommentEvent" => {
            format!("{repo}: new comment")
        }
        other => format!("{repo}: {other}"),
    }
}

/// Map one GitHub activity to a connector-side event builder. Infra `finalize`s it (scope +
/// fingerprint); the `stable_id` here is what the fingerprint is computed over.
pub fn to_builder(ev: GithubEvent) -> EventBuilder {
    let mut entities = vec![ev.repo.name.clone()];
    if !ev.actor.login.is_empty() {
        entities.push(ev.actor.login.clone());
    }
    let links: Vec<String> = html_url(&ev).into_iter().collect();

    EventBuilder::new(
        SourceKind::Github,
        stable_id(&ev),
        ev.created_at,
        title(&ev),
        group_key(&ev),
    )
    .content_kind(content_kind(&ev.kind))
    .links(links)
    .entities(entities)
}
