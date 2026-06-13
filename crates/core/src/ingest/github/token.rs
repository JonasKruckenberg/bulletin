//! Installation-token plumbing for the GitHub connector.
//!
//! GitHub App auth is two hops: an app-level JWT (RS256 over the App's private key, ≤10 min) is
//! exchanged for a short-lived (~1 h) *installation* token that the REST calls actually carry
//! (design §3A). The minting that touches the **secret** (the App private key) lands with
//! credential-at-rest in a later phase; this module defines the *port* the connector drives plus a
//! static provider so the poll/normalize path is exercised end-to-end against fixtures now.
//!
//! Mental model: the connector never sees the App key — it asks a `TokenProvider` for a bearer
//! token and forgets where it came from. Swapping the static provider for a real JWT→installation
//! exchange (and later a managed-KMS-backed key) is a backend change behind this trait.

use chrono::{DateTime, Utc};
use std::future::Future;
use std::pin::Pin;

use crate::ingest::SourceError;

/// A bearer token plus its expiry. `secret` is the raw `Authorization: token …` value; it is
/// short-lived and never persisted (design §3A "Ephemeral" tier).
#[derive(Clone)]
pub struct Token {
    pub secret: String,
    pub expires_at: DateTime<Utc>,
}

/// Boxed future so `TokenProvider` stays `dyn`-compatible — the connector holds an
/// `Arc<dyn TokenProvider>`, letting the static (test/fixture) and the real JWT-exchange providers
/// share one type without leaking a generic through `GithubConnection` into the dispatch enum.
pub type TokenFuture<'a> = Pin<Box<dyn Future<Output = Result<Token, SourceError>> + Send + 'a>>;

/// Mints/returns an installation access token. Infra caches + serializes refreshes per connection
/// (design §3A); a provider impl may itself cache, so `access_token` is cheap to call per poll.
pub trait TokenProvider: Send + Sync {
    fn access_token(&self) -> TokenFuture<'_>;
}

/// A fixed, pre-minted token — used in tests/fixtures (and any "I already have a token" path) so
/// the connector's poll/normalize chain runs without the App-key exchange. Never used in prod.
pub struct StaticTokenProvider {
    token: Token,
}

impl StaticTokenProvider {
    pub fn new(secret: impl Into<String>) -> Self {
        Self {
            token: Token {
                secret: secret.into(),
                // Far future: a static token never triggers the (not-yet-built) refresh path.
                expires_at: DateTime::<Utc>::MAX_UTC,
            },
        }
    }
}

impl TokenProvider for StaticTokenProvider {
    fn access_token(&self) -> TokenFuture<'_> {
        let token = self.token.clone();
        Box::pin(async move { Ok(token) })
    }
}

/// A token provider that never yields a token — it errors if asked. Used by the realtime-only
/// worker ([`super::GithubConnection::realtime_only`]): webhook normalization needs no token, so a
/// realtime-only worker has none. Calling `poll` on it is a bug, and this turns that into a clear
/// runtime error instead of a malformed request.
pub struct UnavailableToken;

impl TokenProvider for UnavailableToken {
    fn access_token(&self) -> TokenFuture<'_> {
        Box::pin(async {
            Err(SourceError::Request(
                "no token provider configured (realtime-only GitHub worker cannot poll)"
                    .to_string(),
            ))
        })
    }
}
