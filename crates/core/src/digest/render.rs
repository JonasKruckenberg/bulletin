//! Rendering a digest to an email + the delivery seam. `core` builds the `Message` and hands it
//! to a `Mailer` the binary supplies (file or SMTP) — so the transport/config stays runtime-side
//! while the deliver *flow* lives here in the digest slice.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use lettre::{message::MultiPart, Message};

use crate::digest::store::RenderItem;

/// The delivery seam: something that can send a rendered digest, and knows the From address to
/// render it as. The binary implements this over its file/SMTP transports.
pub trait Mailer {
    /// From address for rendered digests.
    fn from(&self) -> &str;
    /// Sends a rendered digest message.
    fn send(&self, message: Message) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// Renders a digest as a `multipart/alternative` email: an HTML view (a clean, editorial card of
/// the selected items) with a plaintext fallback for clients that can't — or won't — render HTML.
/// One cluster per item, in the frozen selection order.
pub(crate) fn render(
    from: &str,
    to: &str,
    window_end: DateTime<Utc>,
    items: &[RenderItem],
) -> Result<Message> {
    let plural = if items.len() == 1 { "" } else { "s" };
    let subject = format!("Bulletin: {} new item{plural}", items.len());

    let plain = render_plain(window_end, items);
    let html = render_html(window_end, items);

    Message::builder()
        .from(
            from.parse()
                .with_context(|| format!("invalid from address: {from}"))?,
        )
        .to(to
            .parse()
            .with_context(|| format!("invalid to address: {to}"))?)
        .subject(subject)
        .multipart(MultiPart::alternative_plain_html(plain, html))
        .context("building digest email")
}

/// Plaintext fallback: a numbered list of items (title, link, source, time). Kept deliberately
/// plain — this is what HTML-averse clients and screen-reader-friendly setups fall back to.
fn render_plain(window_end: DateTime<Utc>, items: &[RenderItem]) -> String {
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
    body
}

// --- The editorial palette & type, lifted from the reference digest design. -------------------
//
// A warm, paper-like aesthetic: cream background, dark-brown ink, a terracotta accent, and serif
// display/body type with a sans-serif for the small UI labels. Held as consts so the inline-CSS
// (the only kind email clients reliably honour) reads against names, not bare hex.

const BG: &str = "#fefbf3"; // warm cream backdrop
const SURFACE: &str = "#fffdf8"; // the card, a touch lighter than the page
const INK: &str = "#2c1810"; // headline ink
const INK_MUTED: &str = "#a0927b"; // captions, dates, footer
const ACCENT: &str = "#d35327"; // terracotta — brand, numbers, source labels
const BORDER: &str = "#ddd4c2"; // hairline rules between items
const SERIF: &str = "Georgia, 'Times New Roman', serif";
const SANS: &str = "'Helvetica Neue', Helvetica, Arial, sans-serif";

/// HTML view: a centered editorial card on warm paper — a small-caps brand label, a serif
/// masthead, a `── date ──` rule, then the selected items as a ruled list (a terracotta number,
/// the headline as a link, and a source · time caption), closed by a rule and footer.
///
/// Table-based, all-inline-CSS, single column — the "bulletproof" shape that survives the
/// patchwork of email clients. Every piece of feed-derived text (titles, links) is HTML-escaped,
/// since it's untrusted.
fn render_html(window_end: DateTime<Utc>, items: &[RenderItem]) -> String {
    let count = items.len();
    let plural = if count == 1 { "" } else { "s" };
    let date = window_end.format("%A, %B %-d, %Y");
    let preheader = format!("{count} new item{plural} in your digest");

    let mut rows = String::new();
    for (i, item) in items.iter().enumerate() {
        rows.push_str(&render_item_row(i + 1, item));
    }

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light">
<meta name="supported-color-schemes" content="light">
<title>Bulletin digest</title>
</head>
<body style="margin:0;padding:0;background-color:{BG};-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;">
<div style="display:none;max-height:0;overflow:hidden;opacity:0;mso-hide:all;">{preheader}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:{BG};">
<tr>
<td align="center" style="padding:28px 12px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="width:600px;max-width:100%;background-color:{SURFACE};border:1px solid {BORDER};border-radius:4px;">
<tr>
<td style="padding:40px 44px 0 44px;">
<div style="font-family:{SANS};font-size:12px;font-weight:600;letter-spacing:0.12em;text-transform:uppercase;color:{INK_MUTED};text-align:center;">Bulletin</div>
<h1 style="margin:14px 0 22px 0;font-family:{SERIF};font-size:34px;font-weight:700;line-height:1.2;color:{INK};text-align:center;">Your Digest</h1>
{date_rule}
<div style="font-family:{SERIF};font-size:15px;font-style:italic;font-weight:700;color:{ACCENT};margin:0 0 4px 0;">In this digest &middot; {count} item{plural}</div>
</td>
</tr>
{rows}<tr>
<td style="padding:28px 44px 40px 44px;border-top:1px solid {BORDER};">
<div style="font-family:{SANS};font-size:12px;line-height:1.7;color:{INK_MUTED};text-align:center;">You&#39;re receiving this digest from Bulletin,<br>gathered from the sources you subscribed to.</div>
</td>
</tr>
</table>
</td>
</tr>
</table>
</body>
</html>
"#,
        date_rule = date_rule(&date.to_string()),
    )
}

/// The signature `── DATE ──` divider: a centered, letter-spaced date flanked by hairline rules.
/// Built as a three-cell table (rule | text | rule) so it holds up where flexbox doesn't.
fn date_rule(date: &str) -> String {
    let date = escape(date);
    format!(
        r#"<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:0 0 8px 0;">
<tr>
<td width="40%" style="border-bottom:1px solid {BORDER};font-size:0;line-height:0;">&nbsp;</td>
<td style="padding:0 14px;white-space:nowrap;font-family:{SANS};font-size:12px;letter-spacing:0.1em;text-transform:uppercase;color:{INK_MUTED};text-align:center;">{date}</td>
<td width="40%" style="border-bottom:1px solid {BORDER};font-size:0;line-height:0;">&nbsp;</td>
</tr>
</table>"#
    )
}

/// One item: a terracotta number badge beside the headline (a link when the cluster has one) over
/// a small source · time caption, set off from the previous item by a hairline rule.
fn render_item_row(number: usize, item: &RenderItem) -> String {
    let title = escape(&item.title);
    let headline = match &item.link {
        Some(link) => format!(
            r#"<a href="{}" style="font-family:{SERIF};font-size:20px;font-weight:700;line-height:1.3;color:{INK};text-decoration:none;">{title}</a>"#,
            escape(link)
        ),
        None => format!(
            r#"<span style="font-family:{SERIF};font-size:20px;font-weight:700;line-height:1.3;color:{INK};">{title}</span>"#
        ),
    };

    let source = escape(item.source.as_str());
    let time = item.last_event_time.format("%b %-d, %H:%M UTC");

    format!(
        r#"<tr>
<td style="padding:20px 44px;border-top:1px solid {BORDER};">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
<tr>
<td valign="top" width="40" style="padding-top:3px;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0">
<tr>
<td width="26" height="26" align="center" valign="middle" style="width:26px;height:26px;border:1.5px solid {ACCENT};border-radius:50%;font-family:{SANS};font-size:11px;font-weight:700;color:{ACCENT};line-height:1;">{number}</td>
</tr>
</table>
</td>
<td valign="top">
{headline}
<div style="margin-top:7px;font-family:{SANS};font-size:12px;letter-spacing:0.02em;">
<span style="font-weight:700;text-transform:uppercase;letter-spacing:0.06em;color:{ACCENT};">{source}</span>
<span style="color:{INK_MUTED};">&nbsp;&middot;&nbsp;{time}</span>
</div>
</td>
</tr>
</table>
</td>
</tr>
"#
    )
}

/// Minimal HTML escaping for untrusted feed text, safe in both element-text and double-quoted
/// attribute contexts (the two places we interpolate).
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kind::SourceKind;
    use chrono::TimeZone;

    fn item(title: &str, link: Option<&str>, source: SourceKind) -> RenderItem {
        RenderItem {
            title: title.to_string(),
            link: link.map(str::to_string),
            source,
            last_event_time: Utc.with_ymd_and_hms(2026, 6, 13, 8, 30, 0).unwrap(),
        }
    }

    #[test]
    fn html_includes_titles_and_links() {
        let items = vec![
            item("Hello World", Some("https://example.com/a"), SourceKind::Rss),
            item("No Link Here", None, SourceKind::Github),
        ];
        let html = render_html(Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(), &items);

        assert!(html.contains("Hello World"));
        assert!(html.contains(r#"href="https://example.com/a""#));
        assert!(html.contains("No Link Here"));
        // The unlinked item must not be wrapped in an anchor.
        assert!(!html.contains(r#"<a href="" "#));
        // Source labels surface the kind.
        assert!(html.contains(">rss<"));
        assert!(html.contains(">github<"));
        // The item numbers render.
        assert!(html.contains(">1<"));
        assert!(html.contains(">2<"));
    }

    #[test]
    fn html_escapes_untrusted_feed_text() {
        let items = vec![item(
            "Tom & Jerry <script>alert(1)</script>",
            Some("https://example.com/?a=1&b=2\"></a>"),
            SourceKind::Rss,
        )];
        let html = render_html(Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(), &items);

        // No raw injection survives.
        assert!(!html.contains("<script>"));
        assert!(html.contains("Tom &amp; Jerry &lt;script&gt;"));
        // The attribute-breaking quote in the link is neutralised.
        assert!(!html.contains(r#"a=1&b=2"></a>"#));
        assert!(html.contains("&amp;b=2&quot;"));
    }

    #[test]
    fn plain_fallback_is_unchanged_shape() {
        let items = vec![item("Hello", Some("https://example.com"), SourceKind::Rss)];
        let plain = render_plain(Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(), &items);

        assert!(plain.contains("1. Hello"));
        assert!(plain.contains("https://example.com"));
        assert!(plain.contains("rss ·"));
    }
}
