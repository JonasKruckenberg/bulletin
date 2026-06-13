//! GitHub's app-level webhook head: signature verification + routing (design §3A).
//!
//! [`GithubWebhook::verify`] reproduces the `X-Hub-Signature-256` HMAC-SHA256 over the **raw**
//! request bytes with the App's webhook secret and compares in constant time (the `hmac` crate's
//! `verify_slice`). [`route`] peeks `installation.id` to resolve OUR connection — it trusts nothing
//! else in the payload for authorization (IDOR defense).
//!
//! The webhook secret is *plumbed* here but only *sealed at rest* in Phase 5 ("plumbing now,
//! secrets later"); until an operator sets it, `verify` fails closed (every delivery is `Invalid`).

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::ingest::realtime::{RealtimeConnector, Verified, WebhookHeaders};
use crate::ingest::SourceError;

type HmacSha256 = Hmac<Sha256>;

/// The app-level GitHub webhook verifier. Holds the webhook signing secret (or `None` until an
/// operator configures it). `route` needs no secret, so it is also a free fn (below) for the worker
/// job, which never holds the secret.
pub struct GithubWebhook {
    secret: Option<Vec<u8>>,
}

impl GithubWebhook {
    /// `secret = None` → unconfigured: `verify` rejects every delivery (fail closed).
    pub fn new(secret: Option<Vec<u8>>) -> Self {
        Self { secret }
    }
}

impl RealtimeConnector for GithubWebhook {
    fn verify(&self, headers: &WebhookHeaders, body: &[u8]) -> Verified {
        let Some(secret) = &self.secret else {
            return Verified::Invalid; // no secret configured → reject (fail closed)
        };
        let Some(sig) = headers.signature.as_deref() else {
            return Verified::Invalid; // GitHub always signs; a missing signature is a reject
        };
        // Header form is exactly "sha256=<hex>"; reject anything else.
        let Some(hex_sig) = sig.strip_prefix("sha256=") else {
            return Verified::Invalid;
        };
        let Ok(expected) = hex::decode(hex_sig) else {
            return Verified::Invalid;
        };
        let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
        mac.update(body);
        // `verify_slice` is constant-time and length-checks the tag before comparing.
        match mac.verify_slice(&expected) {
            Ok(()) => Verified::Authentic,
            Err(_) => Verified::Invalid,
        }
    }

    fn route(&self, body: &[u8]) -> Result<String, SourceError> {
        route(body)
    }
}

/// Peek the installation id (`installation.id`) — the webhook routing key — without the secret.
/// Used by the worker job (which has no secret) to resolve our connection row by
/// `(source, provider_account_id)`.
pub fn route(body: &[u8]) -> Result<String, SourceError> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| SourceError::Parse(format!("webhook body is not JSON: {e}")))?;
    value
        .get("installation")
        .and_then(|i| i.get("id"))
        .and_then(|id| id.as_i64())
        .map(|id| id.to_string())
        .ok_or_else(|| SourceError::Parse("webhook missing installation.id".to_string()))
}
