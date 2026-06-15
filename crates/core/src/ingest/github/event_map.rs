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
use crate::ingest::realtime::LifecycleStatus;

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
    /// Repo visibility, as the REST events feed reports it (`public = false` for private-repo
    /// activity). Defaults to `true` (public) when absent; the poll also folds in the repo-list
    /// privacy, and [`from_webhook`] sets it from `repository.private`. `to_builder` passes
    /// `is_private = !public` to the builder, where `finalize` binds it to the owner's scope.
    #[serde(default = "default_public")]
    pub public: bool,
}

fn default_public() -> bool {
    true
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

/// Walk a JSON object path, returning the leaf value when every segment exists. The one traversal
/// behind `payload_str`/`num` and the webhook-timestamp lookup.
fn dig<'a>(v: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    let mut cur = v;
    for key in path {
        cur = cur.get(key)?;
    }
    Some(cur)
}

fn payload_str(ev: &GithubEvent, path: &[&str]) -> Option<String> {
    dig(&ev.payload, path)?.as_str().map(str::to_owned)
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
    dig(&ev.payload, path)?.as_i64().map(|n| n.to_string())
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

// ── Webhook intake (Phase 2) ─────────────────────────────────────────────
//
// A webhook delivery's body *is* the REST events-feed item's `payload` shape — it carries `action`
// plus the typed object (`issue`/`pull_request`/`release`/…) at the top level. Wrapping it as a
// `GithubEvent.payload` lets a webhook reuse the exact `stable_id`/`group_key`/`to_builder` path the
// poll uses, so the two intakes dedup on `UNIQUE(fingerprint)` and the long tail is still captured.

/// A webhook's `X-GitHub-Event` type (snake_case) → the REST events-feed `type` (PascalCase +
/// `Event`) by GitHub's uniform naming rule: `issues` → `IssuesEvent`, `pull_request` →
/// `PullRequestEvent`, `fork` → `ForkEvent`. Producing the REST `type` string routes the synthesized
/// event through the same per-type handling (and the same generic fallback for the long tail).
fn rest_type(event_type: &str) -> String {
    let mut out = String::new();
    for part in event_type.split('_') {
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            out.extend(first.to_uppercase());
            out.push_str(chars.as_str());
        }
    }
    out.push_str("Event");
    out
}

/// Best-effort activity timestamp for a webhook (the body has no single top-level `created_at` like
/// the REST feed). Reads the type's natural timestamp; the caller falls back to "now" when absent.
/// Time isn't folded into the fingerprint, so a small webhook/poll skew never spawns a duplicate.
fn webhook_time(rest_kind: &str, body: &serde_json::Value) -> Option<DateTime<Utc>> {
    let path: &[&str] = match rest_kind {
        "IssuesEvent" => &["issue", "updated_at"],
        "PullRequestEvent" => &["pull_request", "updated_at"],
        "PullRequestReviewEvent" => &["review", "submitted_at"],
        "IssueCommentEvent" | "CommitCommentEvent" | "PullRequestReviewCommentEvent" => {
            &["comment", "updated_at"]
        }
        "ReleaseEvent" => &["release", "published_at"],
        "PushEvent" => &["head_commit", "timestamp"],
        _ => return None,
    };
    dig(body, path)?
        .as_str()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

/// Synthesize a [`GithubEvent`] from a verified webhook delivery so it flows through the same
/// normalization as a polled event (and dedups against it). `repo`/`actor`/`created_at` are lifted
/// from the body; the delivery id is the generic-fallback identity (only used for activity with no
/// content id of its own — the same role the REST event id plays on the poll path).
pub fn from_webhook(event_type: &str, delivery_id: &str, body: serde_json::Value) -> GithubEvent {
    let kind = rest_type(event_type);
    let repo = body
        .get("repository")
        .and_then(|r| r.get("full_name"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let actor = body
        .get("sender")
        .and_then(|s| s.get("login"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let created_at = webhook_time(&kind, &body).unwrap_or_else(Utc::now);
    // Webhooks carry repo visibility directly (`repository.private`); absent ⇒ treat as public.
    let private = body
        .get("repository")
        .and_then(|r| r.get("private"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    GithubEvent {
        id: delivery_id.to_string(),
        kind,
        repo: RepoRef { name: repo },
        actor: ActorRef { login: actor },
        created_at,
        payload: body,
        public: !private,
    }
}

/// The connection-status change implied by a GitHub App lifecycle webhook (`installation` /
/// `installation_repositories`), if any. A suspend/uninstall pauses/revokes our connection; install,
/// unsuspend (→ back to active), permission-accept, and repo add/remove either restore or carry no
/// status change (`None` = leave the status untouched, ingest nothing).
pub fn lifecycle_status(body: &serde_json::Value) -> Option<LifecycleStatus> {
    match body.get("action").and_then(|a| a.as_str()) {
        Some("deleted") => Some(LifecycleStatus::Revoked),
        Some("suspend") => Some(LifecycleStatus::Suspended),
        Some("unsuspend") => Some(LifecycleStatus::Active),
        _ => None,
    }
}

/// Map one GitHub activity to a connector-side event builder. Infra `finalize`s it (scope +
/// fingerprint); the `stable_id` here is what the fingerprint is computed over.
pub fn to_builder(ev: GithubEvent) -> EventBuilder {
    // Structural entities, namespaced so they classify as *weak* linking keys and don't collide
    // across kinds (`repo:acme/x` ≠ `user:acme`). `finalize` adds the cross-source `cve:`/`url:`
    // keys mined from the title/links on top (§8.2).
    let mut entities = vec![format!("repo:{}", ev.repo.name)];
    if !ev.actor.login.is_empty() {
        entities.push(format!("user:{}", ev.actor.login));
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
    // The adapter reports only the structural bool; `finalize` binds it to the connection's owner.
    .private(!ev.public)
}
