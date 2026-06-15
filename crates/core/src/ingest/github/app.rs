//! The real GitHub App → installation-token exchange (design §3A, tech §6 "Token management").
//!
//! GitHub App auth is two hops:
//! 1. Sign a short-lived **app JWT** (RS256 over the App's private key, `exp ≤ 10 min`) proving
//!    "I am App N".
//! 2. Exchange it at `POST /app/installations/{id}/access_tokens` for a ~1 h **installation token**
//!    — the bearer the REST calls actually carry. It is the [`Token`] the connector's
//!    [`TokenProvider`] yields.
//!
//! The App private key is the one persisted secret here; it's unsealed once at startup
//! ([`crate::secret`]) into an [`EncodingKey`] and never logged. The installation token is ephemeral
//! (in-memory, cached per connection, never persisted) — so a [`GithubAppTokens`] caches it and only
//! re-mints when it's near expiry, serialized by a mutex so a burst of polls makes one exchange.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::token::{Token, TokenFuture, TokenProvider};
use crate::ingest::SourceError;

/// Re-mint an installation token this long before it actually expires, so an in-flight poll never
/// races the expiry. (The token lives ~1 h; a minute of slack is ample.)
const EXPIRY_SKEW_SECS: i64 = 60;
/// App-JWT lifetime. GitHub caps it at 10 min and rejects a future `iat` past its own clock, so we
/// sign for 9 min and backdate `iat` 60 s to tolerate skew between us and GitHub.
const APP_JWT_TTL_SECS: i64 = 9 * 60;
const APP_JWT_BACKDATE_SECS: i64 = 60;

/// App-level GitHub credentials, loaded once at startup. Cheap to clone (the signing key is shared
/// behind an `Arc`), so it can seed the [`ConnectorCtx`](crate::ingest::ConnectorCtx) token factory
/// that mints a per-installation provider on demand.
#[derive(Clone)]
pub struct GithubApp {
    client: reqwest::Client,
    base_url: String,
    app_id: i64,
    encoding_key: Arc<EncodingKey>,
}

impl GithubApp {
    /// Build from the App id + its **PEM-encoded** RSA private key (PKCS#1 or PKCS#8 — GitHub issues
    /// PKCS#1 `BEGIN RSA PRIVATE KEY`). Errors only if the key isn't a usable RSA PEM. `base_url` is
    /// overridable for GitHub Enterprise / tests; pass `DEFAULT_API_BASE` for github.com.
    pub fn new(
        base_url: impl Into<String>,
        app_id: i64,
        private_key_pem: &[u8],
    ) -> Result<Self, SourceError> {
        let encoding_key = EncodingKey::from_rsa_pem(private_key_pem).map_err(|e| {
            SourceError::Request(format!(
                "GitHub App private key is not a valid RSA PEM: {e}"
            ))
        })?;
        Ok(Self {
            client: reqwest::Client::new(),
            base_url: base_url.into(),
            app_id,
            encoding_key: Arc::new(encoding_key),
        })
    }

    /// A [`TokenProvider`] scoped to one installation — what the connector drives. Each holds its own
    /// expiry-aware cache, so the dispatch enum sees one `Arc<dyn TokenProvider>` regardless of
    /// whether the token came from this real exchange or a test's static provider.
    pub fn installation_tokens(&self, installation_id: i64) -> Arc<dyn TokenProvider> {
        Arc::new(GithubAppTokens {
            app: self.clone(),
            installation_id,
            cache: Mutex::new(None),
        })
    }

    /// Sign a fresh app JWT (RS256). Short-lived and minted per refresh; never stored.
    fn app_jwt(&self) -> Result<String, SourceError> {
        let now = Utc::now().timestamp();
        let claims = AppClaims {
            iat: now - APP_JWT_BACKDATE_SECS,
            exp: now + APP_JWT_TTL_SECS,
            iss: self.app_id,
        };
        jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &self.encoding_key)
            .map_err(|e| SourceError::Request(format!("signing GitHub App JWT failed: {e}")))
    }
}

/// A per-installation installation-token source with an expiry-aware cache. Implements
/// [`TokenProvider`]; the connector calls [`access_token`](TokenProvider::access_token) every poll
/// and gets a cached token until it nears expiry, when one mint refreshes it under the mutex.
pub struct GithubAppTokens {
    app: GithubApp,
    installation_id: i64,
    cache: Mutex<Option<Token>>,
}

impl GithubAppTokens {
    async fn mint(&self) -> Result<Token, SourceError> {
        let jwt = self.app.app_jwt()?;
        let url = format!(
            "{}/app/installations/{}/access_tokens",
            self.app.base_url, self.installation_id
        );
        let resp = self
            .app
            .client
            .post(&url)
            .bearer_auth(jwt)
            .header(reqwest::header::ACCEPT, "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .header(reqwest::header::USER_AGENT, "bulletin")
            .send()
            .await
            .map_err(|e| SourceError::Request(e.to_string()))?;
        if !resp.status().is_success() {
            // Don't echo the body — an error response can carry sensitive context; the status is enough.
            return Err(SourceError::Request(format!(
                "installation token exchange: HTTP {}",
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| SourceError::Request(e.to_string()))?;
        let parsed: TokenResponse = serde_json::from_slice(&bytes)
            .map_err(|e| SourceError::Parse(format!("installation token response: {e}")))?;
        Ok(Token {
            secret: parsed.token,
            expires_at: parsed.expires_at,
        })
    }
}

impl TokenProvider for GithubAppTokens {
    fn access_token(&self) -> TokenFuture<'_> {
        Box::pin(async move {
            let mut cache = self.cache.lock().await;
            // Reuse the cached token until it's within the skew window of expiring.
            if let Some(token) = cache.as_ref() {
                if token.expires_at > Utc::now() + chrono::Duration::seconds(EXPIRY_SKEW_SECS) {
                    return Ok(token.clone());
                }
            }
            let token = self.mint().await?;
            *cache = Some(token.clone());
            Ok(token)
        })
    }
}

/// Registered claims for the app JWT. `iss` is the App id (GitHub accepts the numeric id).
#[derive(Serialize)]
struct AppClaims {
    iat: i64,
    exp: i64,
    iss: i64,
}

/// The relevant fields of the `access_tokens` response (it carries more — permissions, repos — that
/// we don't need here).
#[derive(Deserialize)]
struct TokenResponse {
    token: String,
    expires_at: DateTime<Utc>,
}
