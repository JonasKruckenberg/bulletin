use anyhow::{Context, Result};
use bulletin_store::digest::RenderItem;
use chrono::{DateTime, Utc};
use lettre::{
    message::header::ContentType, AsyncFileTransport, AsyncSmtpTransport, AsyncTransport, Message,
    Tokio1Executor,
};

/// Email delivery config. M1 default is the **file** transport — it writes `.eml` files to a
/// local directory, so the pipeline runs end-to-end with no external service. Point it at an
/// SMTP relay (`--email-transport smtp --smtp-url …`) for real delivery. Every value takes a
/// long flag or an env var (`clap` `arg(long, env)`).
#[derive(Clone, clap::Args)]
pub struct EmailConfig {
    /// "file" (write .eml locally, no external service) or "smtp" (send via a relay).
    #[arg(
        long = "email-transport",
        env = "BULLETIN_EMAIL_TRANSPORT",
        default_value = "file"
    )]
    pub transport: String,
    /// From address for digest emails.
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
    /// SMTP relay URL for the smtp transport, e.g. `smtp://user:pass@host:587`.
    #[arg(long = "smtp-url", env = "BULLETIN_SMTP_URL")]
    pub smtp_url: Option<String>,
}

/// A built transport. Swapping `file` ↔ `smtp` is a transport change behind one call site.
pub enum Sender {
    File(AsyncFileTransport<Tokio1Executor>),
    Smtp(AsyncSmtpTransport<Tokio1Executor>),
}

impl EmailConfig {
    pub fn build_sender(&self) -> Result<Sender> {
        match self.transport.as_str() {
            "file" => {
                std::fs::create_dir_all(&self.file_dir)
                    .with_context(|| format!("creating email outbox dir {}", self.file_dir))?;
                Ok(Sender::File(AsyncFileTransport::new(&self.file_dir)))
            }
            "smtp" => {
                let url = self
                    .smtp_url
                    .as_deref()
                    .context("--smtp-url / BULLETIN_SMTP_URL is required for the smtp transport")?;
                let transport = AsyncSmtpTransport::<Tokio1Executor>::from_url(url)
                    .context("invalid --smtp-url")?
                    .build();
                Ok(Sender::Smtp(transport))
            }
            other => {
                anyhow::bail!("unknown --email-transport '{other}' (expected 'file' or 'smtp')")
            }
        }
    }
}

impl Sender {
    pub async fn send(&self, message: Message) -> Result<()> {
        match self {
            Sender::File(t) => {
                t.send(message)
                    .await
                    .context("file transport write failed")?;
            }
            Sender::Smtp(t) => {
                t.send(message).await.context("smtp send failed")?;
            }
        }
        Ok(())
    }
}

/// Renders a plaintext digest. M1: a numbered list of selected items (title, link, source,
/// time) — one cluster per item (1 cluster = 1 story). HTML and the deep-link view come later.
pub fn render(
    from: &str,
    to: &str,
    window_end: DateTime<Utc>,
    items: &[RenderItem],
) -> Result<Message> {
    let plural = if items.len() == 1 { "" } else { "s" };
    let subject = format!("Bulletin: {} new item{plural}", items.len());

    let mut body = format!(
        "Your digest for the window ending {}\n\n",
        window_end.format("%Y-%m-%d %H:%M UTC")
    );
    for (i, item) in items.iter().enumerate() {
        body.push_str(&format!("{}. {}\n", i + 1, item.title));
        if let Some(link) = &item.link {
            body.push_str(&format!("   {link}\n"));
        }
        body.push_str(&format!(
            "   {} · {}\n\n",
            item.source.as_str(),
            item.last_event_time.format("%Y-%m-%d %H:%M UTC")
        ));
    }

    Message::builder()
        .from(
            from.parse()
                .with_context(|| format!("invalid from address: {from}"))?,
        )
        .to(to
            .parse()
            .with_context(|| format!("invalid to address: {to}"))?)
        .subject(subject)
        .header(ContentType::TEXT_PLAIN)
        .body(body)
        .context("building digest email")
}
