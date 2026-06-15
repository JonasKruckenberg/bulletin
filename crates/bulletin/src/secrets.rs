//! Credential-at-rest wiring for the binary (M2 Phase 5): resolve the app's sealed secrets at
//! startup, and the `bulletin secrets …` operator tools that produce them.
//!
//! The flow an operator follows once:
//! 1. `bulletin secrets keygen` → a base64 master key, stored as `BULLETIN_MASTER_KEY` (sealed file
//!    / agenix / KMS-fronted — never the Nix store, never a log).
//! 2. `bulletin secrets seal < app-private-key.pem` → an envelope, stored as
//!    `BULLETIN_GITHUB_APP_PRIVATE_KEY`; likewise the webhook signing secret →
//!    `BULLETIN_GITHUB_WEBHOOK_SECRET_SEALED`.
//!
//! At runtime [`SecretConfig`] unseals those once with the master key into a [`ConnectorCtx`] (real
//! GitHub token minting) and the webhook verifier's secret bytes. With no GitHub App configured the
//! context stays `github = None` — RSS keeps working, GitHub polls skip with a clear log, exactly
//! the "plumbing now, secrets later" seam Phase 1 left.

use std::io::Read;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use secrecy::ExposeSecret;

use bulletin_core::ingest::github::app::GithubApp;
use bulletin_core::ingest::github::DEFAULT_API_BASE;
use bulletin_core::ingest::{ConnectorCtx, GithubCtx};
use bulletin_core::secret::{seal, MasterKey, SealedSecret};

/// At-rest secret configuration, flattened into the top-level CLI so every role + the `secrets`
/// tools share one source of truth (and one set of env vars).
#[derive(Args, Clone)]
pub struct SecretConfig {
    /// Base64-encoded 32-byte app master key. Unwraps every sealed secret (the GitHub App key, the
    /// sealed webhook secret). Generate with `bulletin secrets keygen`. Without it, sealed GitHub
    /// credentials can't load and GitHub stays disabled (RSS is unaffected).
    #[arg(long, env = "BULLETIN_MASTER_KEY")]
    master_key: Option<String>,

    /// GitHub App id — **not** a secret (design §3A). Required (with the private key + master key)
    /// to mint installation tokens.
    #[arg(long, env = "BULLETIN_GITHUB_APP_ID")]
    github_app_id: Option<i64>,

    /// Sealed GitHub App **private key** envelope: the base64 output of `bulletin secrets seal` over
    /// the App's RSA PEM. Unsealed once at startup with the master key, then held only as an
    /// in-memory signing key.
    #[arg(long, env = "BULLETIN_GITHUB_APP_PRIVATE_KEY")]
    github_app_private_key: Option<String>,

    /// GitHub REST base URL override (GitHub Enterprise / a test mock). Defaults to api.github.com.
    #[arg(long, env = "BULLETIN_GITHUB_API_BASE")]
    github_api_base: Option<String>,

    /// Sealed webhook signing secret envelope (preferred). Unsealed with the master key and fed to
    /// the edge HMAC verifier.
    #[arg(long, env = "BULLETIN_GITHUB_WEBHOOK_SECRET_SEALED")]
    github_webhook_secret_sealed: Option<String>,

    /// Plaintext webhook signing secret — a dev-only fallback used when no sealed secret is set.
    /// Sealing it (above) is preferred in production.
    #[arg(long, env = "BULLETIN_GITHUB_WEBHOOK_SECRET")]
    github_webhook_secret: Option<String>,
}

impl SecretConfig {
    /// Parse the master key if configured.
    fn master_key(&self) -> Result<Option<MasterKey>> {
        self.master_key
            .as_deref()
            .map(|s| {
                MasterKey::from_base64(s)
                    .context("BULLETIN_MASTER_KEY is not a valid 32-byte base64 key")
            })
            .transpose()
    }

    /// The webhook signing secret bytes for the edge verifier: the sealed form (unsealed with the
    /// master key) when present, else the plaintext dev fallback, else `None` (the catcher then
    /// fails closed and rejects every delivery).
    pub fn webhook_secret(&self) -> Result<Option<Vec<u8>>> {
        if let Some(sealed) = &self.github_webhook_secret_sealed {
            let master = self.master_key()?.context(
                "BULLETIN_GITHUB_WEBHOOK_SECRET_SEALED is set but BULLETIN_MASTER_KEY is not",
            )?;
            let opened = unseal_text(&master, sealed).context("unsealing the webhook secret")?;
            return Ok(Some(opened.expose_secret().to_vec()));
        }
        Ok(self
            .github_webhook_secret
            .as_ref()
            .map(|s| s.clone().into_bytes()))
    }

    /// Build the connector context. When the App id + sealed private key + master key are all set,
    /// unseal the key, construct a [`GithubApp`], and wire a token factory that mints a
    /// per-installation provider — turning on real GitHub polling. Otherwise `github = None`.
    pub fn connector_ctx(&self) -> Result<ConnectorCtx> {
        let (Some(app_id), Some(sealed_key)) =
            (self.github_app_id, self.github_app_private_key.as_deref())
        else {
            if self.github_app_id.is_some() || self.github_app_private_key.is_some() {
                tracing::warn!(
                    "GitHub App partially configured (need both --github-app-id and \
                     --github-app-private-key); GitHub polling stays disabled"
                );
            }
            return Ok(ConnectorCtx::default());
        };
        let master = self
            .master_key()?
            .context("a sealed GitHub App private key is set but BULLETIN_MASTER_KEY is not")?;
        let pem =
            unseal_text(&master, sealed_key).context("unsealing the GitHub App private key")?;
        let base_url = self
            .github_api_base
            .clone()
            .unwrap_or_else(|| DEFAULT_API_BASE.to_string());
        let app = GithubApp::new(base_url.clone(), app_id, pem.expose_secret())
            .map_err(|e| anyhow::anyhow!("{e}"))
            .context("loading the GitHub App credentials")?;
        tracing::info!(app_id, base_url = %base_url, "GitHub App credentials loaded; polling enabled");
        let token_factory =
            Arc::new(move |installation_id: i64| app.installation_tokens(installation_id));
        Ok(ConnectorCtx {
            github: Some(GithubCtx {
                base_url,
                token_factory,
            }),
        })
    }
}

/// Decode + unseal a base64 envelope string in one step.
fn unseal_text(master: &MasterKey, text: &str) -> Result<secrecy::SecretSlice<u8>> {
    let sealed = SealedSecret::decode(text).map_err(|e| anyhow::anyhow!("{e}"))?;
    bulletin_core::secret::unseal(master, &sealed).map_err(|e| anyhow::anyhow!("{e}"))
}

#[derive(Subcommand)]
pub enum SecretsCommand {
    /// Generate a fresh base64 master key. Store it as `BULLETIN_MASTER_KEY` (in a sealed file /
    /// secret store) — it is the root that unwraps every other secret.
    Keygen,
    /// Seal a secret read from **stdin** under the master key, printing the envelope to stdout.
    /// Pipe in the GitHub App PEM or the webhook signing secret; store the printed envelope as the
    /// corresponding `*_SEALED` / `BULLETIN_GITHUB_APP_PRIVATE_KEY` env var. A single trailing
    /// newline is trimmed so `echo secret | … seal` does the obvious thing.
    Seal,
}

/// Run a `secrets` subcommand. These need no database — they're offline key tooling.
pub fn run(cfg: &SecretConfig, command: SecretsCommand) -> Result<()> {
    match command {
        SecretsCommand::Keygen => {
            println!("{}", MasterKey::generate().to_base64());
        }
        SecretsCommand::Seal => {
            let master = cfg
                .master_key()?
                .context("`secrets seal` needs BULLETIN_MASTER_KEY (run `secrets keygen` first)")?;
            let mut buf = Vec::new();
            std::io::stdin()
                .read_to_end(&mut buf)
                .context("reading the secret from stdin")?;
            // Trim one trailing newline (and a CR) so a piped `echo` doesn't fold it into the secret.
            if buf.last() == Some(&b'\n') {
                buf.pop();
                if buf.last() == Some(&b'\r') {
                    buf.pop();
                }
            }
            let sealed = seal(&master, &buf).map_err(|e| anyhow::anyhow!("{e}"))?;
            buf.iter_mut().for_each(|b| *b = 0); // scrub the plaintext buffer
            println!("{}", sealed.encode());
        }
    }
    Ok(())
}
