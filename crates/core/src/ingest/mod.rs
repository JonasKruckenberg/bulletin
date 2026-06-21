//! The ingest flow: poll a connection's source, normalize items to events, and append them to
//! the event log (`store::insert_event`). Producer side of the ingest→clustering seam.

pub mod fetch;
pub mod github;
pub(crate) mod html_text;
pub mod realtime;
pub mod rss;
pub mod store;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::db::{begin_scope, ScopeCtx};
use crate::common::{
    event::{EventBuilder, NewEvent},
    kind::SourceKind,
};
use crate::ingest::github::token::TokenProvider;

pub struct Batch<I, C> {
    pub items: Vec<I>,
    pub cursor: C,
}

/// The poll interval (seconds) a new connection takes when the caller leaves it unset.
pub const DEFAULT_POLL_INTERVAL_SECS: i64 = 900;

/// The depth threshold (chars): text at or above this carries enough substance to ground a Story-depth
/// multi-sentence tldr; below it is a thin [`Announcement`](crate::common::kind::ContentKind::Announcement)
/// → a headline-only Note. Shared by the RSS ingest gate (which applies it to the feed snippet, [`rss`])
/// and the article-fetch re-derivation (which re-applies it to the fetched `full_text`, [`fetch`]), so the
/// two can't drift on what "longform" means.
pub(crate) const LONGFORM_MIN_CHARS: usize = 400;

/// Validated inputs for [`store::insert_connection`], produced by [`prepare_connection`].
pub struct NewConnection {
    pub source: SourceKind,
    pub config: serde_json::Value,
    pub poll_interval_secs: i64,
    pub owner: Option<Uuid>,
    pub provider_account_id: Option<String>,
}

/// Validate + normalize the inputs for a new connection — the single place the CLI
/// (`debug connection-add`) and the gRPC admin API agree on what a valid connection is, so the rules
/// can't drift between them. Centralizes: the source vocabulary, the private-source-must-be-owned
/// guard (mirrors the `connection_private_source_owned` DB CHECK), and the GitHub `installation_id` →
/// `provider_account_id` webhook routing key (the IDOR boundary — derived from our own config, never a
/// delivery payload). Returns a human-readable message on invalid input; the caller maps it to its own
/// error type (`anyhow` for the CLI, `Status::invalid_argument` for the API).
pub fn prepare_connection(
    source: &str,
    config_json: &str,
    poll_interval_secs: i64,
    owner: Option<Uuid>,
) -> Result<NewConnection, String> {
    let source = SourceKind::try_from(source)
        .map_err(|_| format!("unknown source '{source}'; valid: rss, github, slack"))?;
    if source.can_emit_private() && owner.is_none() {
        return Err(format!(
            "a {} connection can see private content and must be owned — provide an owner",
            source.as_str()
        ));
    }
    let config: serde_json::Value =
        serde_json::from_str(config_json).map_err(|e| format!("config is not valid JSON: {e}"))?;
    // The webhook routing key, set at seed time so a content/lifecycle delivery resolves back to THIS
    // row. Only GitHub has one today (the installation_id); a future private source needs its own here.
    let provider_account_id = match source {
        SourceKind::Github => Some(
            config
                .get("installation_id")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| "a github config needs an integer \"installation_id\"".to_string())?
                .to_string(),
        ),
        _ => None,
    };
    let poll_interval_secs = if poll_interval_secs > 0 {
        poll_interval_secs
    } else {
        DEFAULT_POLL_INTERVAL_SECS
    };
    Ok(NewConnection {
        source,
        config,
        poll_interval_secs,
        owner,
        provider_account_id,
    })
}

#[derive(Debug)]
pub enum SourceError {
    Request(String),
    Parse(String),
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceError::Request(msg) => write!(f, "request error: {msg}"),
            SourceError::Parse(msg) => write!(f, "parse error: {msg}"),
        }
    }
}

impl std::error::Error for SourceError {}

/// Per-tenant live worker for one `connection` row. Every connector implements this.
pub trait Connection: Send + Sync {
    /// Opaque, source-private incremental-fetch position. Infra persists as JSON, never reads it.
    type Cursor: serde::Serialize + serde::de::DeserializeOwned + Default + Send + Sync;
    /// One unit of content from the source, complete after a poll.
    type Item: Send;

    fn poll(
        &self,
        cursor: Self::Cursor,
    ) -> impl std::future::Future<Output = Result<Batch<Self::Item, Self::Cursor>, SourceError>> + Send;

    /// Pure normalization: source-specific item → connector-side event builders. Infra calls
    /// `finalize(owner)` on each builder to stamp the scope boundary and fingerprint.
    fn to_events(&self, item: Self::Item) -> Vec<EventBuilder>;
}

/// App-level connector context: the cross-tenant config/secrets a per-connection worker needs to
/// come alive (design §5.4 — the `Connector` factory role). RSS needs nothing; GitHub needs its
/// App credentials to mint installation tokens. Those secrets land with credential-at-rest in a
/// later phase, so `github` is `None` for now and a GitHub connection polled without it is skipped
/// with a clear log — the path exists, the secret doesn't yet ("plumbing now, secrets later").
#[derive(Clone, Default)]
pub struct ConnectorCtx {
    pub github: Option<GithubCtx>,
}

/// What GitHub connections need at the app level: the REST base URL (overridable for tests) and a
/// factory turning an `installation_id` into a token provider. The factory indirection keeps the
/// real JWT→installation-token minting (which touches the App private key) behind the same seam a
/// test's static token uses.
#[derive(Clone)]
pub struct GithubCtx {
    pub base_url: String,
    pub token_factory: Arc<dyn Fn(i64) -> Arc<dyn TokenProvider> + Send + Sync>,
}

/// Why a connection couldn't be built into a live worker — all non-fatal (the poll is skipped).
#[derive(Debug)]
pub enum BuildError {
    /// The source isn't wired in this build (e.g. its app credentials aren't configured yet).
    NotConfigured(SourceKind),
    /// `connection.config` didn't match the source's expected shape.
    BadConfig(String),
    /// A source with no connector yet (Slack lands in M6).
    Unsupported(SourceKind),
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuildError::NotConfigured(s) => {
                write!(f, "{} not configured in this build", s.as_str())
            }
            BuildError::BadConfig(msg) => write!(f, "invalid connection config: {msg}"),
            BuildError::Unsupported(s) => write!(f, "unsupported source: {}", s.as_str()),
        }
    }
}

/// Hand-written dispatch over the closed source set (design §5.1/§5.4): each arm holds a concrete
/// `Connection`, the `poll → to_events` chain runs *inside* the typed arm, and only `core` types
/// (the normalized builders + the JSON cursor) cross out. Adding a 4th source makes the compiler
/// flag the missing arm. (RSS-only `RssConfig` parsing now lives in `build`.)
pub enum ConnDispatch {
    Rss(rss::RssConnection),
    Github(github::GithubConnection),
}

impl ConnDispatch {
    /// Build the live worker for a connection row from its `config` + the app context.
    pub fn build(row: &store::ConnectionRow, ctx: &ConnectorCtx) -> Result<Self, BuildError> {
        match row.source {
            SourceKind::Rss => {
                let cfg: rss::RssConfig = serde_json::from_value(row.config.clone())
                    .map_err(|e| BuildError::BadConfig(e.to_string()))?;
                Ok(ConnDispatch::Rss(rss::RssConnection::new(cfg.url)))
            }
            SourceKind::Github => {
                let gh = ctx
                    .github
                    .as_ref()
                    .ok_or(BuildError::NotConfigured(SourceKind::Github))?;
                let cfg: github::GithubConfig = serde_json::from_value(row.config.clone())
                    .map_err(|e| BuildError::BadConfig(e.to_string()))?;
                let token = (gh.token_factory)(cfg.installation_id);
                Ok(ConnDispatch::Github(
                    github::GithubConnection::new(&gh.base_url, token).with_repos(cfg.repos),
                ))
            }
            SourceKind::Slack => Err(BuildError::Unsupported(SourceKind::Slack)),
        }
    }

    /// Poll using the persisted cursor JSON and normalize into connector-side builders, returning
    /// the next cursor to persist. The cursor is erased to JSON here; `Item` never escapes the arm.
    pub async fn poll_and_normalize(
        &self,
        cursor: Option<serde_json::Value>,
    ) -> Result<(Vec<EventBuilder>, serde_json::Value), SourceError> {
        match self {
            ConnDispatch::Rss(c) => poll_inner(c, cursor).await,
            ConnDispatch::Github(c) => poll_inner(c, cursor).await,
        }
    }
}

/// The shared, source-generic poll step: deserialize the opaque cursor (default on absent/garbage),
/// poll, re-serialize the next cursor, and run the (pure) `to_events` normalization. Monomorphized
/// per arm, so there's no boxing and `Self::Item`/`Self::Cursor` stay private to the connector.
async fn poll_inner<C: Connection>(
    conn: &C,
    cursor: Option<serde_json::Value>,
) -> Result<(Vec<EventBuilder>, serde_json::Value), SourceError> {
    let cursor: C::Cursor = cursor
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default();
    let batch = conn.poll(cursor).await?;
    let next = serde_json::to_value(&batch.cursor)
        .map_err(|e| SourceError::Parse(format!("cursor does not serialize: {e}")))?;
    let builders = batch
        .items
        .into_iter()
        .flat_map(|item| conn.to_events(item))
        .collect();
    Ok((builders, next))
}

/// What one `poll` did — the trigger layer turns this into metrics. `Failed` means the connector
/// errored and backoff was already recorded; `Skipped` means there was nothing to poll.
pub enum PollOutcome {
    Polled {
        source: SourceKind,
        inserted: usize,
        deduplicated: usize,
    },
    Skipped,
    Failed {
        source: SourceKind,
    },
}

/// Appends finalized events to the log under RLS, returning `(inserted, deduplicated)`. Each event
/// is written in the context its scope demands — a public event in the no-subscriber context, a
/// private event in its owner's — because the DB write policy refuses any other pairing. Events are
/// grouped by context and committed one transaction per context (at most two for a single
/// connection: public + the owner), so the per-event scope discipline costs no transaction-per-row.
async fn append_scoped(pool: &PgPool, events: Vec<NewEvent>) -> Result<(usize, usize)> {
    let total = events.len();
    let mut groups: HashMap<ScopeCtx, Vec<NewEvent>> = HashMap::new();
    for ev in events {
        groups
            .entry(ScopeCtx::for_scope(&ev.scope))
            .or_default()
            .push(ev);
    }

    let mut inserted = 0usize;
    for (ctx, evs) in groups {
        let mut tx = begin_scope(pool, ctx)
            .await
            .context("open scoped ingest txn")?;
        for ev in &evs {
            if store::insert_event(&mut *tx, ev).await?.is_some() {
                inserted += 1;
            }
        }
        tx.commit().await.context("commit scoped ingest txn")?;
    }
    Ok((inserted, total - inserted))
}

/// Ingest one connection: load it, fetch its source, append new events to the log, then advance
/// the cursor (or back off on failure). Events commit before the cursor advances — the
/// crash-safety invariant: a re-poll re-fetches, and fingerprint dedup collapses the overlap.
pub async fn poll(pool: &PgPool, connection_id: Uuid, ctx: &ConnectorCtx) -> Result<PollOutcome> {
    let conn_row = match store::load_connection(pool, connection_id).await? {
        Some(r) => r,
        None => {
            tracing::warn!(%connection_id, "connection not found");
            return Ok(PollOutcome::Skipped);
        }
    };
    if conn_row.status != "active" {
        tracing::debug!(%connection_id, status = %conn_row.status, "skipping non-active connection");
        return Ok(PollOutcome::Skipped);
    }

    let source = conn_row.source;
    let dispatch = match ConnDispatch::build(&conn_row, ctx) {
        Ok(d) => d,
        Err(e) => {
            // Not fatal: a misconfigured or not-yet-wired source is skipped, not retried.
            tracing::warn!(%connection_id, source = source.as_str(), error = %e, "cannot build connection; skipping");
            return Ok(PollOutcome::Skipped);
        }
    };

    match dispatch.poll_and_normalize(conn_row.cursor.clone()).await {
        Ok((builders, new_cursor)) => {
            // Per-event scope: `finalize` maps the builder's `is_private` flag against THIS
            // connection's owner — a private-repo item becomes `Private(owner)`, public stays
            // shared. The owner comes from our row, never the polled payload (§12 risk #1).
            // `append_scoped` then writes each event in the RLS context its scope requires.
            let events: Vec<NewEvent> = builders
                .into_iter()
                .map(|b| {
                    b.connection(Some(conn_row.id))
                        .finalize(conn_row.subscriber_id)
                })
                .collect();
            let (inserted, deduplicated) = append_scoped(pool, events).await?;
            tracing::info!(
                connection_id = %conn_row.id,
                source = source.as_str(),
                inserted,
                deduplicated,
                "poll complete"
            );
            store::advance_cursor(pool, conn_row.id, new_cursor).await?;
            Ok(PollOutcome::Polled {
                source,
                inserted,
                deduplicated,
            })
        }
        Err(e) => {
            tracing::warn!(%connection_id, error = %e, "poll failed");
            store::record_failure(pool, conn_row.id).await?;
            Ok(PollOutcome::Failed { source })
        }
    }
}

// ── Webhook intake (realtime) ─────────────────────────────────────────────

/// What one `process_webhook` did — the trigger layer turns this into metrics.
pub enum WebhookOutcome {
    /// Activity normalized + appended (dedup-collapsed against any prior poll/webhook overlap).
    Ingested {
        source: SourceKind,
        inserted: usize,
        deduplicated: usize,
    },
    /// A lifecycle webhook updated the connection's status.
    Lifecycle {
        source: SourceKind,
        status: &'static str,
    },
    /// No connection matched the delivery's routing key (unknown/foreign install) — dropped.
    Unrouted { source: SourceKind },
    /// Nothing to do (unroutable body, or an unsupported realtime source).
    Skipped,
}

/// Ingest one **verified** webhook delivery (design §3A/§5.4). The HTTP edge has already
/// authenticated the raw bytes; here we (1) peek the routing key credential-free, (2) resolve OUR
/// connection by `(source, provider_account_id)` — deriving nothing about scope/subscriber from the
/// payload (IDOR defense), (3) normalize via the same path the poll uses, and (4) append (fingerprint
/// dedup collapses any poll overlap) or apply a lifecycle status change.
pub async fn process_webhook(
    pool: &PgPool,
    source: SourceKind,
    event_type: &str,
    delivery_id: &str,
    body: &[u8],
) -> Result<WebhookOutcome> {
    let provider_account_id = match realtime::route(source, body) {
        Ok(id) => id,
        Err(e) => {
            tracing::warn!(source = source.as_str(), error = %e, "cannot route webhook; dropping");
            return Ok(WebhookOutcome::Skipped);
        }
    };

    // IDOR defense: the connection (and thus its owner/scope) comes from OUR row keyed by the
    // routing id, never from anything else the payload claims. An unknown install is dropped.
    let conn =
        match store::resolve_connection_by_provider(pool, source, &provider_account_id).await? {
            Some(c) => c,
            None => {
                tracing::warn!(
                    source = source.as_str(),
                    provider_account_id,
                    "no connection for webhook; dropping"
                );
                return Ok(WebhookOutcome::Unrouted { source });
            }
        };

    let dispatch = match realtime::RealtimeDispatch::build(source) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(source = source.as_str(), error = %e, "no realtime worker; dropping");
            return Ok(WebhookOutcome::Skipped);
        }
    };

    match dispatch.accept_and_normalize(event_type, delivery_id, body)? {
        realtime::Inbound::Events(builders) => {
            // Per-event scope, derived from the *resolved* connection's owner (never the webhook
            // payload — IDOR defense): a private-repo delivery becomes `Private(owner)`. The webhook
            // body's `repository.private` only sets the builder's `is_private` flag. `append_scoped`
            // writes each event in the RLS context its scope requires.
            let events: Vec<NewEvent> = builders
                .into_iter()
                .map(|b| b.connection(Some(conn.id)).finalize(conn.subscriber_id))
                .collect();
            let (inserted, deduplicated) = append_scoped(pool, events).await?;
            tracing::info!(
                connection_id = %conn.id,
                source = source.as_str(),
                event_type,
                inserted,
                deduplicated,
                "webhook ingested"
            );
            Ok(WebhookOutcome::Ingested {
                source,
                inserted,
                deduplicated,
            })
        }
        realtime::Inbound::Lifecycle(change) => {
            let status = change.status.as_str();
            store::update_connection_status(pool, conn.id, status).await?;
            tracing::info!(
                connection_id = %conn.id,
                source = source.as_str(),
                status,
                "webhook lifecycle status change"
            );
            Ok(WebhookOutcome::Lifecycle { source, status })
        }
    }
}
