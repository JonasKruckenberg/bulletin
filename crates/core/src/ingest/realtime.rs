//! The realtime (webhook) head of the connector model (design §5.4) — the push counterpart to the
//! pull [`Connection`]. A [`RealtimeConnection`] turns a verified webhook delivery into the *same*
//! `EventBuilder`s the poll produces, so the reconciliation poll (the correctness floor) and a
//! webhook for the same activity collapse on `UNIQUE(fingerprint)`; a dropped webhook is recovered
//! by the next poll.
//!
//! Two layers, mirroring the pull side:
//! - **App-level** [`RealtimeConnector`]: `verify` (authenticate the raw bytes against the app
//!   secret, constant-time) + `route` (credential-free peek of the routing key). Runs at the HTTP
//!   edge, *before* any connection is resolved.
//! - **Per-connection** [`RealtimeConnection`]: `accept_webhook` (body → source items) + `hydrate`
//!   (thin-notification → full item; identity for GitHub). Dispatched through [`RealtimeDispatch`],
//!   which — having **no RSS arm** — makes "route a webhook to a pull-only source" uncompilable.

use crate::common::{event::EventBuilder, kind::SourceKind};
use crate::ingest::{github, BuildError, Connection, SourceError};

/// The webhook headers the edge lifts off the HTTP request so `core` stays transport-free.
pub struct WebhookHeaders {
    /// `X-Hub-Signature-256: sha256=<hex>` — the HMAC the app secret must reproduce.
    pub signature: Option<String>,
    /// `X-GitHub-Event` — the activity type. The body alone doesn't name it, so it rides the
    /// `ProcessWebhook` payload and is passed into `accept_webhook` (the design "key wrinkle").
    pub event_type: Option<String>,
    /// `X-GitHub-Delivery` — the per-delivery id: the enqueue idempotency key + the
    /// generic-event fallback identity (for activity with no content id of its own).
    pub delivery_id: Option<String>,
}

/// The verdict of authenticating a raw delivery. `Challenge` carries an echo body for sources that
/// do a URL-verification handshake (e.g. Slack, M6); GitHub only ever returns `Authentic`/`Invalid`.
pub enum Verified {
    Authentic,
    Challenge(Vec<u8>),
    Invalid,
}

/// The connection states a lifecycle webhook can move a connection into. `as_str` is the
/// `connection.status` text persisted; anything but `active` pauses polling.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleStatus {
    Active,
    Suspended,
    Revoked,
}

impl LifecycleStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            LifecycleStatus::Active => "active",
            LifecycleStatus::Suspended => "suspended",
            LifecycleStatus::Revoked => "revoked",
        }
    }
}

/// A connection's lifecycle transition signalled out-of-band by the source (e.g. a GitHub App
/// suspend/uninstall). It updates `connection.status`, never the event log.
pub struct LifecycleChange {
    pub status: LifecycleStatus,
}

/// What `accept_webhook` produced from one delivery: source items to ingest, or a lifecycle signal.
pub enum Inbound<I> {
    Events(Vec<I>),
    Lifecycle(LifecycleChange),
}

/// `Inbound` after normalization — connector-side builders ready for `finalize`, or a lifecycle
/// change. The realtime counterpart to the pull path's `(Vec<EventBuilder>, cursor)`.
pub enum NormalizedInbound {
    Events(Vec<EventBuilder>),
    Lifecycle(LifecycleChange),
}

/// App-level realtime head for a source (design §5.4). `verify`/`route` run at the HTTP edge with
/// only the app secret + raw bytes — no connection resolved yet.
pub trait RealtimeConnector: Send + Sync {
    /// Authenticate the raw body against the app secret in **constant time**, over the bytes exactly
    /// as received (no parse first — a parse-then-verify is a signature-bypass foothold).
    fn verify(&self, headers: &WebhookHeaders, body: &[u8]) -> Verified;

    /// Extract the *routing key* (GitHub: `installation.id`) used to look up OUR connection row.
    /// Credential-free and **never** trusted for authorization beyond routing — the subscriber and
    /// scope come from the resolved row, not the payload (IDOR defense, design §12).
    fn route(&self, body: &[u8]) -> Result<String, SourceError>;
}

/// Per-connection realtime worker — a [`Connection`] that also accepts webhooks (design §5.4).
pub trait RealtimeConnection: Connection {
    /// Normalize a verified delivery into source items (or a lifecycle change). `event_type` and
    /// `delivery_id` come from the headers (the body alone doesn't carry the activity type).
    fn accept_webhook(
        &self,
        event_type: &str,
        delivery_id: &str,
        body: &[u8],
    ) -> Result<Inbound<Self::Item>, SourceError>;

    /// Turn a thin webhook notification into a full item (default: identity). GitHub webhook
    /// payloads are already complete, so it never fetches; a source whose webhook is a bare pointer
    /// overrides this to hydrate via its API.
    fn hydrate(&self, item: Self::Item) -> Self::Item {
        item
    }
}

/// Hand-written dispatch over the *realtime-capable* source set — **no RSS arm**, so routing a
/// webhook to a pull-only source fails to compile (design §5.4). Mirrors `ConnDispatch` on the
/// pull side.
pub enum RealtimeDispatch {
    Github(github::GithubConnection),
}

impl RealtimeDispatch {
    /// Build the realtime worker for a source. Unlike `ConnDispatch::build`, this needs no
    /// `ConnectorCtx`: webhook normalization is a pure function of the delivery body — the App token
    /// (the part `ctx.github` carries) is only exercised by `poll`. So webhooks ingest even while
    /// "plumbing now, secrets later" leaves `ctx.github == None` (Phase 5 wires the poll creds).
    pub fn build(source: SourceKind) -> Result<Self, BuildError> {
        match source {
            SourceKind::Github => Ok(RealtimeDispatch::Github(
                github::GithubConnection::realtime_only(),
            )),
            other => Err(BuildError::Unsupported(other)),
        }
    }

    /// Accept + normalize one delivery: `accept_webhook → hydrate → to_events`, the realtime mirror
    /// of `poll → to_events`. `Item` never escapes the arm.
    pub fn accept_and_normalize(
        &self,
        event_type: &str,
        delivery_id: &str,
        body: &[u8],
    ) -> Result<NormalizedInbound, SourceError> {
        match self {
            RealtimeDispatch::Github(c) => match c.accept_webhook(event_type, delivery_id, body)? {
                Inbound::Events(items) => {
                    let builders = items
                        .into_iter()
                        .map(|i| c.hydrate(i))
                        .flat_map(|i| c.to_events(i))
                        .collect();
                    Ok(NormalizedInbound::Events(builders))
                }
                Inbound::Lifecycle(change) => Ok(NormalizedInbound::Lifecycle(change)),
            },
        }
    }
}

/// The app-level routing peek for a source — the `route` half of [`RealtimeConnector`] reached
/// without constructing the (secret-bearing) verifier, since the worker job has no secret and
/// doesn't need one to resolve a connection. Closed match over the realtime source set.
pub fn route(source: SourceKind, body: &[u8]) -> Result<String, SourceError> {
    match source {
        SourceKind::Github => github::webhook::route(body),
        other => Err(SourceError::Parse(format!(
            "{} has no webhook routing",
            other.as_str()
        ))),
    }
}
