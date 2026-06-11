//! Rendering a digest to an email + the delivery seam. `core` builds the `Message` and hands it
//! to a `Mailer` the binary supplies (file or SMTP) — so the transport/config stays runtime-side
//! while the deliver *flow* lives here in the digest slice.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use lettre::{message::header::ContentType, Message};

use crate::digest::store::RenderItem;

/// The delivery seam: something that can send a rendered digest, and knows the From address to
/// render it as. The binary implements this over its file/SMTP transports.
pub trait Mailer {
    /// From address for rendered digests.
    fn from(&self) -> &str;
    /// Sends a rendered digest message.
    fn send(&self, message: Message) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// Renders a plaintext digest: a numbered list of selected items (title, link, source, time) —
/// one cluster per item. HTML and the deep-link view come later.
pub(crate) fn render(
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
