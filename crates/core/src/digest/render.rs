//! Rendering a digest to an email + the delivery seam. `core` builds the `Message` and hands it
//! to a `Mailer` the binary supplies (file or SMTP) — so the transport/config stays runtime-side
//! while the deliver *flow* lives here in the digest slice.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use lettre::{message::MultiPart, Message};

use crate::digest::store::RenderItem;
use crate::identity::ConfidenceBand;

/// The delivery seam: something that can send a rendered digest, and knows the From address to
/// render it as. The binary implements this over its file/SMTP transports.
pub trait Mailer {
    /// From address for rendered digests.
    fn from(&self) -> &str;
    /// Sends a rendered digest message.
    fn send(&self, message: Message) -> impl std::future::Future<Output = Result<()>> + Send;
}

/// The configurable, non-item content of a digest email — everything that isn't the item list or
/// the per-digest greeting. Lets the caller supply the brand, masthead title, and footer (e.g. from
/// config) instead of baking them into the renderer, so the same layout can be re-skinned without
/// touching this module. The `summary` / `item_*` fields are stand-ins for reference-design
/// sections the data model doesn't feed yet; their defaults are lorem-ipsum and they're HTML-marked
/// for removal.
#[derive(Clone, Copy)]
pub struct DigestContent<'a> {
    /// Small-caps brand label at the very top (e.g. "Bulletin").
    pub brand: &'a str,
    /// Serif masthead headline beneath the brand label (e.g. "Your Digest").
    pub title: &'a str,
    /// Footer note rendered beneath the items.
    pub footer: &'a str,
    /// The "big picture" summary that follows the greeting in the lead. **Placeholder** until the
    /// digest produces one.
    pub summary: &'a str,
    /// Per-item category label (e.g. "Geopolitics/Diplomacy"). **Placeholder** until items carry one.
    pub item_category: &'a str,
    /// Per-item summary/TL;DR. **Placeholder** until items carry one.
    pub item_summary: &'a str,
}

impl Default for DigestContent<'_> {
    fn default() -> Self {
        Self {
            brand: "Bulletin",
            title: "Your Digest",
            footer: "You're receiving this digest from Bulletin, \
                     gathered from the sources you subscribed to.",
            // Lorem-ipsum stand-ins for sections the reference design has but our data model
            // doesn't feed yet. Wrapped in `<!-- PLACEHOLDER … -->` markers in the rendered HTML.
            summary: "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod \
                      tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, \
                      quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo.",
            item_category: "Lorem / Ipsum",
            item_summary: "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
                           eiusmod tempor incididunt ut labore et dolore magna aliqua.",
        }
    }
}

/// Renders a digest as a `multipart/alternative` email: an HTML view (a clean, editorial card of
/// the selected items) with a plaintext fallback for clients that can't — or won't — render HTML.
/// One cluster per item, in the frozen selection order. All the non-item chrome (brand, title,
/// footer) comes from `content`, so callers fully parametrize what's shown; the subject line is the
/// per-digest `greeting`, so the inbox preview matches the lead.
pub(crate) fn render(
    from: &str,
    to: &str,
    window_end: DateTime<Utc>,
    timezone: &str,
    items: &[RenderItem],
    greeting: &str,
    content: &DigestContent<'_>,
) -> Result<Message> {
    // The greeting doubles as the subject line, so the inbox preview opens in the same warm,
    // time-of-day voice as the digest's lead (and varies per window the same way).
    let subject = greeting;

    // Dates and times are shown in the subscriber's own zone so the masthead date matches when
    // they actually receive it. An unparseable name can't reach here (the DB rejects it on
    // signup/update), but fall back to UTC rather than panic if one ever does.
    let tz: Tz = timezone.parse().unwrap_or(Tz::UTC);
    let plain = render_plain(window_end, tz, greeting, items);
    let html = render_html(window_end, tz, greeting, items, content);

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

/// Renders the "nothing to report" digest: the same editorial card, but a cheerful *you're all
/// caught up* note where the items would go. Sent when a subscriber's window turns up empty —
/// rare enough that going silent reads as a broken pipeline, so we send a happy little nudge
/// instead. Same `multipart/alternative` shape and chrome (`content`) as [`render`].
pub(crate) fn render_empty(
    from: &str,
    to: &str,
    window_end: DateTime<Utc>,
    timezone: &str,
    salutation: &str,
    content: &DigestContent<'_>,
) -> Result<Message> {
    // Mirror the populated digest: the subject is the email's own opening line, so the inbox
    // preview reads in the same time-of-day voice as the body.
    let subject = format!("{salutation}. You're all caught up");

    // Show the window date in the subscriber's own zone, like the item digest does. An
    // unparseable name can't reach here (the DB rejects it on signup/update); fall back to UTC.
    let tz: Tz = timezone.parse().unwrap_or(Tz::UTC);
    let plain = render_empty_plain(window_end, tz, salutation);
    let html = render_empty_html(window_end, tz, salutation, content);

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
        .context("building empty digest email")
}

/// Plaintext fallback: a numbered list of items (title, link, source, time). Kept deliberately
/// plain — this is what HTML-averse clients and screen-reader-friendly setups fall back to.
fn render_plain(window_end: DateTime<Utc>, tz: Tz, greeting: &str, items: &[RenderItem]) -> String {
    let mut body = format!(
        "{greeting}\n\nYour digest for the window ending {}\n\n",
        window_end.with_timezone(&tz).format("%Y-%m-%d %H:%M %Z")
    );
    for (i, item) in items.iter().enumerate() {
        body.push_str(&format!("{}. {}\n", i + 1, item.title));
        if let Some(link) = &item.link {
            body.push_str(&format!("   {link}\n"));
        }
        body.push_str(&format!(
            "   {} · {}\n",
            item.source.as_str(),
            item.last_event_time
                .with_timezone(&tz)
                .format("%Y-%m-%d %H:%M %Z")
        ));
        // The cross-source connections fused into this story, each with why it belongs (§8.2).
        for conn in &item.connections {
            body.push_str(&format!("   ↳ {} [{}]", conn.title, conn.source.as_str()));
            if let Some(reason) = &conn.link_reason {
                body.push_str(&format!(" — {reason}"));
            }
            body.push('\n');
            if let Some(link) = &conn.link {
                body.push_str(&format!("     {link}\n"));
            }
        }
        body.push('\n');
    }
    body
}

/// Plaintext fallback for the empty digest: the cheerful counterpart to [`render_plain`], opened
/// with the time-of-day salutation so it matches the populated digest's voice.
fn render_empty_plain(window_end: DateTime<Utc>, tz: Tz, salutation: &str) -> String {
    format!(
        "{salutation}. You're all caught up!\n\n\
         No new items in the window ending {}.\n\
         Enjoy the quiet. \u{1F343}\n",
        window_end.with_timezone(&tz).format("%Y-%m-%d %H:%M %Z")
    )
}

// --- The editorial palette & type, lifted from the reference digest design. -------------------
//
// A warm, paper-like aesthetic: cream background, dark-brown ink, a terracotta accent, and serif
// display/body type with a sans-serif for the small UI labels. Held as consts so the inline-CSS
// (the only kind email clients reliably honour) reads against names, not bare hex.

const BG: &str = "#fefbf3"; // warm cream backdrop
const SURFACE: &str = "#fffdf8"; // the card, a touch lighter than the page
const INK: &str = "#2c1810"; // headline ink
const INK_BODY: &str = "#3d2e22"; // running body copy (summaries, lead)
const INK_MUTED: &str = "#a0927b"; // captions, dates, footer
const ACCENT: &str = "#d35327"; // terracotta — brand, numbers, source labels
const BORDER: &str = "#ddd4c2"; // hairline rules between items
const SERIF: &str = "Georgia, 'Times New Roman', serif";
const SANS: &str = "'Helvetica Neue', Helvetica, Arial, sans-serif";

// A deliberately off-palette "this is scaffolding" look for the debug block, so no reader mistakes
// the selection trace for finished editorial content. Monospace type and a dashed amber callout read
// as developer output, not design — and make the block obvious to strip later.
const MONO: &str = "'SF Mono', 'SFMono-Regular', Menlo, Consolas, 'Liberation Mono', monospace";
const DEBUG_BG: &str = "#fff8e1"; // pale amber notebook tint, foreign to the cream palette
const DEBUG_BORDER: &str = "#e0a800"; // amber, for the dashed "draft" border
const DEBUG_INK: &str = "#7a5c00"; // dark amber, the debug text colour

/// A small progressive-enhancement stylesheet. The layout is all inline-CSS and works without this
/// (most clients ignore `<style>`/media queries) — but the clients that *do* honour it, chiefly
/// modern mobile mail, get tighter side padding and a touch larger type on narrow screens, where the
/// 600px desktop card would otherwise feel cramped and small. Classes are layered over the inline
/// defaults; `!important` lets the media query win where it applies, inline holds everywhere else.
const MOBILE_CSS: &str = r#"<style>
@media only screen and (max-width:480px) {
  .bx { padding-left:24px !important; padding-right:24px !important; }
  .lead { font-size:18px !important; line-height:1.75 !important; }
  .headline { font-size:22px !important; }
  .meta { font-size:14px !important; }
  .related { font-size:16px !important; }
}
</style>"#;

/// HTML view: a centered editorial card on warm paper — a small-caps brand label, a serif
/// masthead, a `── date ──` rule, a short time-of-day greeting lead, then the selected items as a
/// ruled list (a terracotta number, the headline link, a category, a summary, and a source · time
/// caption), closed by a rule and footer.
///
/// The greeting stands in for the reference design's "big picture" summary until the digest
/// produces a real one. Per-item sections the data model doesn't feed yet (category/summary) render
/// parametric lorem-ipsum placeholders wrapped in `<!-- PLACEHOLDER … -->`; the source · time
/// caption is debug-only, wrapped in `<!-- DEBUG … -->` and styled as a distinct monospace amber
/// callout so it never reads as real content — both are easy to grep out later.
///
/// Table-based, all-inline-CSS, single column — the "bulletproof" shape that survives the
/// patchwork of email clients. The chrome comes from `content`; every piece of caller- or
/// feed-supplied text is HTML-escaped, since it's interpolated into markup.
fn render_html(
    window_end: DateTime<Utc>,
    tz: Tz,
    greeting: &str,
    items: &[RenderItem],
    content: &DigestContent<'_>,
) -> String {
    let count = items.len();
    let plural = if count == 1 { "" } else { "s" };
    let date = window_end.with_timezone(&tz).format("%A, %B %-d, %Y");
    let preheader = format!("{count} new item{plural} in your digest");
    let brand = escape(content.brand);
    let title = escape(content.title);
    let greeting = escape(greeting);
    let summary = escape(content.summary);
    let footer = escape(content.footer);

    // Per-item placeholders are identical across items today, so escape them once and reuse.
    let category = escape(content.item_category);
    let item_summary = escape(content.item_summary);

    // One divider, reused between every item and above the footer.
    let divider = soft_divider();
    let mut rows = String::new();
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            rows.push_str(&divider);
        }
        rows.push_str(&render_item_row(i, item, tz, &category, &item_summary));
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
{MOBILE_CSS}
</head>
<body style="margin:0;padding:0;background-color:{BG};-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;">
<div style="display:none;max-height:0;overflow:hidden;opacity:0;mso-hide:all;">{preheader}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:{BG};">
<tr>
<td align="center" style="padding:36px 12px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="width:600px;max-width:100%;background-color:{SURFACE};border:1px solid {BORDER};border-radius:4px;">
<tr>
<td class="bx" style="padding:52px 40px 0 40px;">
<div style="font-family:{SANS};font-size:12px;font-weight:600;letter-spacing:0.12em;text-transform:uppercase;color:{INK_MUTED};text-align:center;">{brand}</div>
<h1 style="margin:18px 0 30px 0;font-family:{SERIF};font-size:34px;font-weight:700;line-height:1.25;color:{INK};text-align:center;">{title}</h1>
{date_rule}
<div style="font-family:{SERIF};font-size:15px;font-style:italic;font-weight:700;color:{ACCENT};margin:34px 0 12px 0;">The big picture</div>
<!-- Lead: a time-of-day greeting (real) opens the paragraph, then the "big picture" summary. -->
<div class="lead" style="font-family:{SERIF};font-size:17px;line-height:1.75;color:{INK_BODY};margin:0;"><strong style="color:{INK};font-weight:700;">{greeting}</strong> <!-- PLACEHOLDER: "big picture" summary — remove or replace once the digest produces a real one -->{summary}<!-- /PLACEHOLDER --></div>
<div style="font-family:{SERIF};font-size:15px;font-style:italic;font-weight:700;color:{ACCENT};margin:40px 0 0 0;">In this digest &middot; {count} item{plural}</div>
</td>
</tr>
{rows}{soft_divider}<tr>
<td class="bx" style="padding:30px 40px 52px 40px;">
<div class="meta" style="font-family:{SANS};font-size:13px;line-height:1.7;color:{INK_MUTED};text-align:center;">{footer}</div>
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
        soft_divider = divider,
    )
}

/// HTML view of the empty digest: the same warm card, brand label, serif masthead and `── date ──`
/// rule as [`render_html`], but where the items would be there's a centered, happy *all caught up*
/// note — a big sparkle, a serif headline, and a reassuring line. No placeholder/debug sections,
/// since there are no items to stand in for. Same table-based, all-inline-CSS, escape-everything
/// shape.
fn render_empty_html(
    window_end: DateTime<Utc>,
    tz: Tz,
    salutation: &str,
    content: &DigestContent<'_>,
) -> String {
    let date = window_end.with_timezone(&tz).format("%A, %B %-d, %Y");
    let preheader = "Nothing new — you're all caught up";
    let brand = escape(content.brand);
    let title = escape(content.title);
    let salutation = escape(salutation);
    let footer = escape(content.footer);

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light">
<meta name="supported-color-schemes" content="light">
<title>Bulletin digest</title>
{MOBILE_CSS}
</head>
<body style="margin:0;padding:0;background-color:{BG};-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;">
<div style="display:none;max-height:0;overflow:hidden;opacity:0;mso-hide:all;">{preheader}</div>
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:{BG};">
<tr>
<td align="center" style="padding:36px 12px;">
<table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="width:600px;max-width:100%;background-color:{SURFACE};border:1px solid {BORDER};border-radius:4px;">
<tr>
<td class="bx" style="padding:52px 40px 0 40px;">
<div style="font-family:{SANS};font-size:12px;font-weight:600;letter-spacing:0.12em;text-transform:uppercase;color:{INK_MUTED};text-align:center;">{brand}</div>
<h1 style="margin:18px 0 30px 0;font-family:{SERIF};font-size:34px;font-weight:700;line-height:1.25;color:{INK};text-align:center;">{title}</h1>
{date_rule}
<div style="text-align:center;padding:46px 8px 18px 8px;">
<div style="font-size:52px;line-height:1;" aria-hidden="true">&#x1F343;</div>
<div style="margin:24px 0 0 0;font-family:{SERIF};font-size:28px;font-weight:700;line-height:1.3;color:{ACCENT};">You're all caught up</div>
<div style="margin:14px auto 0 auto;max-width:360px;font-family:{SERIF};font-size:17px;font-style:italic;line-height:1.7;color:{INK_BODY};">{salutation}. No new notifications this time. Sit back and enjoy the calm.</div>
</div>
</td>
</tr>
{soft_divider}<tr>
<td class="bx" style="padding:30px 40px 52px 40px;">
<div class="meta" style="font-family:{SANS};font-size:13px;line-height:1.7;color:{INK_MUTED};text-align:center;">{footer}</div>
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
        soft_divider = soft_divider(),
    )
}

/// A gentle separator between items: a short centered hairline rather than a full-width rule, so
/// it marks a break without slicing the column edge to edge. Echoes the date-divider's hairlines.
fn soft_divider() -> String {
    format!(
        r#"<tr>
<td align="center" style="padding:6px 40px;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" align="center">
<tr>
<td width="44" height="1" style="width:44px;height:1px;line-height:1px;font-size:0;background-color:{BORDER};">&nbsp;</td>
</tr>
</table>
</td>
</tr>
"#
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

/// One item: the headline (a link when the cluster has one), then a placeholder category and
/// summary, and the debug block. `category` and `item_summary` are pre-escaped placeholders; the
/// debug block is debug-only — a monospace, dashed-amber callout (with a `debug` tag so it reads as
/// scaffolding, not finished content) carrying the why-and-how of this row: its selection rank and
/// the recency key it was chosen on, the source, the raw link, and how many related sub-items it
/// fused. `position` is the 0-based render slot (== recency rank, since selection is recency-only).
/// All of it is marked `<!-- DEBUG … -->` for later removal. Items are separated by
/// [`soft_divider`], not a per-item rule.
fn render_item_row(
    position: usize,
    item: &RenderItem,
    tz: Tz,
    category: &str,
    item_summary: &str,
) -> String {
    let title = escape(&item.title);
    let headline = match &item.link {
        Some(link) => format!(
            r#"<a class="headline" href="{}" style="font-family:{SERIF};font-size:21px;font-weight:700;line-height:1.3;color:{INK};text-decoration:none;">{title}</a>"#,
            escape(link)
        ),
        None => format!(
            r#"<span class="headline" style="font-family:{SERIF};font-size:21px;font-weight:700;line-height:1.3;color:{INK};">{title}</span>"#
        ),
    };

    let source = escape(item.source.as_str());
    // Debug fields: the recency key selection ranked on (full date), the 1-based rank, the raw link,
    // and the count of fused sub-items — the answer to "why is this here, and what's inside it?".
    let rank = position + 1;
    let recency_key = item
        .last_event_time
        .with_timezone(&tz)
        .format("%Y-%m-%d %H:%M %Z");
    let link = item
        .link
        .as_deref()
        .map(escape)
        .unwrap_or_else(|| "—".into());
    let related = item.connections.len();
    let connections = render_connections(&item.connections);
    let thread_chip = render_thread_chip(item.thread.as_ref());

    // Decision-log fields (§10.2): the ranking basis is the Thread relevance term then recency — so a
    // nonzero relevance promoted this item above pure recency; the entity spine is what it scored on;
    // and the thread line records the assignment + its identity confidence band.
    let relevance = item.reason.relevance;
    let basis = if relevance > 0.0 {
        format!("relevance {relevance:.2}, then recency")
    } else {
        "recency".to_string()
    };
    let spine = if item.reason.entities.is_empty() {
        String::new()
    } else {
        format!(
            r#"<div style="margin-top:4px;word-break:break-word;">entities {}</div>"#,
            escape(&item.reason.entities.join(", "))
        )
    };
    let thread_trace = match &item.thread {
        Some(t) => format!(
            r#"<div style="margin-top:4px;">thread <span style="font-weight:700;">{}</span> &middot; identity {}</div>"#,
            escape(&t.label),
            t.confidence.as_str()
        ),
        None => String::new(),
    };

    format!(
        r#"<tr>
<td class="bx" style="padding:30px 40px;">
{thread_chip}{headline}
<!-- PLACEHOLDER: per-item category — remove or replace once items carry a category -->
<div class="meta" style="margin-top:9px;font-family:{SANS};font-size:13px;font-weight:500;letter-spacing:0.04em;color:{ACCENT};">{category}</div>
<!-- /PLACEHOLDER -->
<!-- PLACEHOLDER: per-item summary — remove or replace once items carry a summary -->
<div class="meta" style="margin-top:12px;font-family:{SERIF};font-size:16px;font-style:italic;line-height:1.65;color:{INK_MUTED};">{item_summary}</div>
<!-- /PLACEHOLDER -->
{connections}<!-- DEBUG: selection trace — debugging info, remove before launch -->
<div style="margin-top:18px;padding:9px 12px;background-color:{DEBUG_BG};border:1px dashed {DEBUG_BORDER};border-radius:3px;font-family:{MONO};font-size:12px;line-height:1.6;color:{DEBUG_INK};">
<span style="display:inline-block;padding:1px 6px;margin-right:8px;background-color:{DEBUG_BORDER};color:{DEBUG_BG};font-weight:700;text-transform:uppercase;letter-spacing:0.1em;border-radius:2px;">debug</span>
<span style="font-weight:700;">selected #{rank}</span> <span>by {basis}</span>
<div style="margin-top:4px;">key {recency_key} &middot; source <span style="font-weight:700;">{source}</span> &middot; related {related}</div>
{thread_trace}{spine}<div style="margin-top:4px;word-break:break-all;">link {link}</div>
</div>
<!-- /DEBUG -->
</td>
</tr>
"#
    )
}

/// The "Related" block: the cross-source clusters fused into a story (design §8.2), each a headline
/// (linked when it has a URL), a source tag, and the `link_reason` for why it belongs — the M3 value
/// made visible. Renders every fused sub-item (uncapped). Empty for a singleton story, so a lone item
/// renders unchanged.
fn render_connections(connections: &[crate::digest::store::Connection]) -> String {
    if connections.is_empty() {
        return String::new();
    }
    let mut rows = String::new();
    for c in connections {
        let title = escape(&c.title);
        let head = match &c.link {
            Some(link) => format!(
                r#"<a href="{}" style="color:{INK_BODY};text-decoration:none;font-weight:700;">{title}</a>"#,
                escape(link)
            ),
            None => format!(r#"<span style="color:{INK_BODY};font-weight:700;">{title}</span>"#),
        };
        let source = escape(c.source.as_str());
        let reason = c
            .link_reason
            .as_deref()
            .map(|r| {
                format!(
                    r#" <span style="color:{INK_MUTED};">— {}</span>"#,
                    escape(r)
                )
            })
            .unwrap_or_default();
        rows.push_str(&format!(
            r#"<div class="related" style="margin-top:8px;font-family:{SERIF};font-size:16px;line-height:1.5;color:{INK_BODY};">
<span style="color:{ACCENT};">&#8627;</span> {head} <span style="font-family:{SANS};font-size:12px;text-transform:uppercase;letter-spacing:0.06em;color:{ACCENT};">{source}</span>{reason}</div>
"#
        ));
    }
    format!(
        r#"<div style="margin-top:16px;padding-top:4px;">
<div class="meta" style="font-family:{SANS};font-size:12px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:{INK_MUTED};">Related</div>
{rows}</div>
"#
    )
}

/// The thread chip above a story's headline (design §5.2 thread-grouped render): the persistent
/// thread it advances, prefixed "possibly" when the thread's identity is only `Probable`/`Uncertain`
/// — confidence rendered as a product surface (§4). Empty when the story isn't assigned to a thread,
/// so an un-threaded item renders exactly as before.
fn render_thread_chip(thread: Option<&crate::digest::store::ThreadTag>) -> String {
    let Some(tag) = thread.filter(|t| !t.label.trim().is_empty()) else {
        return String::new();
    };
    let qualifier = match tag.confidence {
        ConfidenceBand::Confirmed => "",
        ConfidenceBand::Probable | ConfidenceBand::Uncertain => "possibly ",
    };
    format!(
        r#"<div style="margin-bottom:8px;font-family:{SANS};font-size:11px;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:{ACCENT};">&#9656;&nbsp;{qualifier}{}</div>
"#,
        escape(&tag.label)
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
            connections: Vec::new(),
            thread: None,
            reason: crate::digest::select::ItemReason::default(),
        }
    }

    #[test]
    fn html_includes_titles_and_links() {
        let items = vec![
            item(
                "Hello World",
                Some("https://example.com/a"),
                SourceKind::Rss,
            ),
            item("No Link Here", None, SourceKind::Github),
        ];
        let html = render_html(
            Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(),
            Tz::UTC,
            "Good morning. Here's your daily digest.",
            &items,
            &DigestContent::default(),
        );

        assert!(html.contains("Hello World"));
        assert!(html.contains(r#"href="https://example.com/a""#));
        assert!(html.contains("No Link Here"));
        // The unlinked item must not be wrapped in an anchor.
        assert!(!html.contains(r#"<a href="" "#));
        // Source labels surface the kind.
        assert!(html.contains(">rss<"));
        assert!(html.contains(">github<"));
    }

    #[test]
    fn html_escapes_untrusted_feed_text() {
        let items = vec![item(
            "Tom & Jerry <script>alert(1)</script>",
            Some("https://example.com/?a=1&b=2\"></a>"),
            SourceKind::Rss,
        )];
        let html = render_html(
            Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(),
            Tz::UTC,
            "Good morning. Here's your daily digest.",
            &items,
            &DigestContent::default(),
        );

        // No raw injection survives.
        assert!(!html.contains("<script>"));
        assert!(html.contains("Tom &amp; Jerry &lt;script&gt;"));
        // The attribute-breaking quote in the link is neutralised.
        assert!(!html.contains(r#"a=1&b=2"></a>"#));
        assert!(html.contains("&amp;b=2&quot;"));
    }

    #[test]
    fn placeholder_and_debug_sections_are_marked() {
        let items = vec![item(
            "Headline",
            Some("https://example.com"),
            SourceKind::Rss,
        )];
        let content = DigestContent {
            summary: "BIG_PICTURE_TEXT",
            item_category: "CAT_TEXT",
            item_summary: "ITEM_SUMMARY_TEXT",
            ..DigestContent::default()
        };
        let html = render_html(
            Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(),
            Tz::UTC,
            "GREETING_TEXT",
            &items,
            &content,
        );

        // The greeting opens the lead as real (non-placeholder) content, ahead of the summary.
        assert!(html.contains("GREETING_TEXT"));
        let greeting_at = html.find("GREETING_TEXT").unwrap();
        let summary_at = html.find("BIG_PICTURE_TEXT").unwrap();
        assert!(
            greeting_at < summary_at,
            "greeting must precede the summary"
        );
        // Parametric placeholder content renders...
        assert!(html.contains("BIG_PICTURE_TEXT"));
        assert!(html.contains("CAT_TEXT"));
        assert!(html.contains("ITEM_SUMMARY_TEXT"));
        // ...inside grep-able markers for later removal/replacement (summary + category + item).
        assert_eq!(html.matches("<!-- PLACEHOLDER:").count(), 3);
        assert!(html.contains("<!-- DEBUG: selection trace"));
        assert!(html.contains("<!-- /DEBUG -->"));
        // The debug block is styled to look unmistakably like scaffolding — a monospace,
        // dashed-amber callout carrying a visible `debug` tag — so it can't pass for real content.
        assert!(html.contains(MONO));
        assert!(html.contains(DEBUG_BG));
        assert!(html.contains(&format!("border:1px dashed {DEBUG_BORDER}")));
        assert!(html.contains(">debug</span>"));
        // ...and it carries the selection trace: rank, recency key, source, related count, link.
        assert!(html.contains("selected #1"));
        assert!(html.contains("by recency"));
        assert!(html.contains("related 0"));
        assert!(html.contains("link https://example.com"));
    }

    #[test]
    fn parametric_chrome_overrides_defaults() {
        let items = vec![item("Headline", None, SourceKind::Rss)];
        let content = DigestContent {
            brand: "ACME",
            title: "Weekly Roundup",
            footer: "Sent by ACME.",
            ..DigestContent::default()
        };
        let html = render_html(
            Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(),
            Tz::UTC,
            "Good morning. Here's your daily digest.",
            &items,
            &content,
        );

        assert!(html.contains("ACME"));
        assert!(html.contains("Weekly Roundup"));
        assert!(html.contains("Sent by ACME."));
        assert!(!html.contains("Your Digest"));
    }

    #[test]
    fn plain_fallback_is_unchanged_shape() {
        let items = vec![item("Hello", Some("https://example.com"), SourceKind::Rss)];
        let plain = render_plain(
            Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(),
            Tz::UTC,
            "Good morning. Here's your daily digest.",
            &items,
        );

        // The greeting opens the plaintext fallback, ahead of the item list.
        assert!(plain.starts_with("Good morning. Here's your daily digest."));
        assert!(plain.contains("1. Hello"));
        assert!(plain.contains("https://example.com"));
        assert!(plain.contains("rss ·"));
    }

    #[test]
    fn empty_html_is_caught_up_and_carries_chrome() {
        let content = DigestContent {
            brand: "ACME",
            title: "Weekly Roundup",
            footer: "Sent by ACME.",
            ..DigestContent::default()
        };
        let html = render_empty_html(
            Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(),
            Tz::UTC,
            "Good morning",
            &content,
        );

        // The cheerful "all caught up" copy is present, opened with the time-of-day salutation...
        assert!(html.contains("You're all caught up"));
        assert!(html.contains("Good morning. No new notifications this time."));
        // ...alongside the same caller-supplied chrome the real digest carries.
        assert!(html.contains("ACME"));
        assert!(html.contains("Weekly Roundup"));
        assert!(html.contains("Sent by ACME."));
        // No item-shaped placeholder/debug sections, since there are no items to stand in for.
        assert!(!html.contains("<!-- PLACEHOLDER:"));
        assert!(!html.contains("<!-- DEBUG:"));
    }

    #[test]
    fn empty_plain_is_caught_up() {
        let plain = render_empty_plain(
            Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(),
            Tz::UTC,
            "Good evening",
        );

        assert!(plain.starts_with("Good evening. You're all caught up!"));
        assert!(plain.contains("2026-06-13 09:00 UTC"));
    }
}
