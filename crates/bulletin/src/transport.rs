//! Email delivery, runtime side. `EmailConfig` is the clap-driven configuration; `Sender` is the
//! built file/SMTP transport it produces, and it implements `core::digest::Mailer` so the digest
//! flow can deliver without knowing about clap, the filesystem, or SMTP.

use anyhow::{Context, Result};
use bulletin_core::digest::{DigestContent, Mailer};
use lettre::{
    transport::smtp::authentication::Credentials, AsyncFileTransport, AsyncSmtpTransport,
    AsyncTransport, Message, Tokio1Executor,
};

/// Email delivery config. The default is the **file** transport — it writes `.eml` files to a
/// local directory, so the pipeline runs end-to-end with no external service. Point it at a real
/// server (`--email-transport smtp --smtp-host … --smtp-username … --smtp-password …`) for
/// delivery. Every value takes a long flag or an env var (`clap` `arg(long, env)`).
///
/// Deliberately not `#[derive(Debug)]`: `smtp_password` holds a live credential (a Proton SMTP
/// token), and we never want it landing in a log line via a struct dump.
#[derive(Clone, clap::Args)]
pub struct EmailConfig {
    /// "file" (write .eml locally, no external service) or "smtp" (send via a server).
    #[arg(
        long = "email-transport",
        env = "BULLETIN_EMAIL_TRANSPORT",
        default_value = "file"
    )]
    pub transport: String,
    /// From address for digest emails. For the smtp transport this must be an address the server
    /// is allowed to send as — with Proton, the custom-domain address paired with the SMTP token.
    #[arg(
        long = "email-from",
        env = "BULLETIN_EMAIL_FROM",
        default_value = "bulletin@localhost"
    )]
    pub from: String,
    /// Output directory for the file transport's `.eml` files.
    #[arg(
        long = "email-file-dir",
        env = "BULLETIN_EMAIL_FILE_DIR",
        default_value = "./outbox"
    )]
    pub file_dir: String,
    /// SMTP submission host, e.g. `smtp.protonmail.ch`.
    #[arg(long = "smtp-host", env = "BULLETIN_SMTP_HOST")]
    pub smtp_host: Option<String>,
    /// SMTP port. Omitted → 587 (`--smtp-tls starttls`) or 465 (`--smtp-tls implicit`).
    #[arg(long = "smtp-port", env = "BULLETIN_SMTP_PORT")]
    pub smtp_port: Option<u16>,
    /// SMTP username. With Proton this is the custom-domain address (e.g. `bulletin@your.domain`),
    /// passed verbatim — no URL-encoding of the `@`.
    #[arg(long = "smtp-username", env = "BULLETIN_SMTP_USERNAME")]
    pub smtp_username: Option<String>,
    /// SMTP password. With Proton this is the **SMTP token** (Settings → IMAP/SMTP → SMTP tokens),
    /// NOT your login password. Prefer the env var / an `EnvironmentFile` over the flag so it
    /// doesn't show up in the process list.
    #[arg(long = "smtp-password", env = "BULLETIN_SMTP_PASSWORD")]
    pub smtp_password: Option<String>,
    /// Connection security. Both modes enforce TLS, so the token is never sent in cleartext; they
    /// differ only in when TLS is negotiated. Proton accepts either.
    #[arg(
        long = "smtp-tls",
        env = "BULLETIN_SMTP_TLS",
        default_value = "starttls"
    )]
    pub smtp_tls: SmtpTls,
    /// Small-caps brand label at the top of the digest email.
    #[arg(
        long = "email-brand",
        env = "BULLETIN_EMAIL_BRAND",
        default_value = "Bulletin"
    )]
    pub brand: String,
    /// Serif masthead title beneath the brand label.
    #[arg(
        long = "email-title",
        env = "BULLETIN_EMAIL_TITLE",
        default_value = "Your Digest"
    )]
    pub title: String,
    /// Footer note beneath the items.
    #[arg(
        long = "email-footer",
        env = "BULLETIN_EMAIL_FOOTER",
        default_value = "You're receiving this digest from Bulletin, gathered from the sources you subscribed to."
    )]
    pub footer: String,
}

/// How the SMTP connection is secured. Both require TLS (lettre `Tls::Required` / `Tls::Wrapper`),
/// so a misconfigured server fails closed rather than leaking the credential over plaintext.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum SmtpTls {
    /// Connect on 587 in plaintext, then upgrade in-band via STARTTLS. The upgrade is *required* —
    /// lettre aborts before authenticating if the server refuses it, so there's no downgrade.
    Starttls,
    /// TLS-wrapped from the first byte on 465.
    Implicit,
}

/// A built transport plus the From address to render as — the binary's `Mailer`.
pub struct Sender {
    from: String,
    transport: Transport,
}

enum Transport {
    File(AsyncFileTransport<Tokio1Executor>),
    Smtp(AsyncSmtpTransport<Tokio1Executor>),
}

impl EmailConfig {
    /// Builds the delivery `Sender`. Swapping `file` ↔ `smtp` is a transport change behind one
    /// call site.
    pub fn build_sender(&self) -> Result<Sender> {
        let transport = match self.transport.as_str() {
            "file" => {
                std::fs::create_dir_all(&self.file_dir)
                    .with_context(|| format!("creating email outbox dir {}", self.file_dir))?;
                Transport::File(AsyncFileTransport::new(&self.file_dir))
            }
            "smtp" => Transport::Smtp(self.build_smtp()?),
            other => {
                anyhow::bail!("unknown --email-transport '{other}' (expected 'file' or 'smtp')")
            }
        };
        Ok(Sender {
            from: self.from.clone(),
            transport,
        })
    }

    /// The digest's configurable chrome, borrowed as the core renderer wants it. The branding
    /// (brand/title/footer) comes from the flags above; the lead and per-item summaries are now fed
    /// by the data model, leaving only `item_category` at its placeholder `DigestContent` default.
    pub fn content(&self) -> DigestContent<'_> {
        DigestContent {
            brand: &self.brand,
            title: &self.title,
            footer: &self.footer,
            ..DigestContent::default()
        }
    }

    /// Builds an authenticated, TLS-enforced SMTP transport from the explicit host/port/creds
    /// fields. Host, username, and password are all required for this transport. `starttls_relay`
    /// / `relay` pin TLS (`Tls::Required` / `Tls::Wrapper`) and the submission port default, so the
    /// credential always travels encrypted; an optional `--smtp-port` overrides the default.
    fn build_smtp(&self) -> Result<AsyncSmtpTransport<Tokio1Executor>> {
        let host = self
            .smtp_host
            .as_deref()
            .context("--smtp-host / BULLETIN_SMTP_HOST is required for the smtp transport")?;
        let username = self.smtp_username.as_deref().context(
            "--smtp-username / BULLETIN_SMTP_USERNAME is required for the smtp transport",
        )?;
        let password = self.smtp_password.as_deref().context(
            "--smtp-password / BULLETIN_SMTP_PASSWORD is required for the smtp transport",
        )?;

        let mut builder = match self.smtp_tls {
            SmtpTls::Starttls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host),
            SmtpTls::Implicit => AsyncSmtpTransport::<Tokio1Executor>::relay(host),
        }
        .with_context(|| format!("configuring TLS for smtp host {host}"))?;

        if let Some(port) = self.smtp_port {
            builder = builder.port(port);
        }

        Ok(builder
            .credentials(Credentials::new(username.to_owned(), password.to_owned()))
            .build())
    }
}

impl Mailer for Sender {
    fn from(&self) -> &str {
        &self.from
    }

    async fn send(&self, message: Message) -> Result<()> {
        match &self.transport {
            Transport::File(t) => {
                t.send(message)
                    .await
                    .context("file transport write failed")?;
            }
            Transport::Smtp(t) => {
                t.send(message).await.context("smtp send failed")?;
            }
        }
        Ok(())
    }
}
