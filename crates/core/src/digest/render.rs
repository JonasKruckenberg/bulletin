//! Rendering a digest to an email + the delivery seam. `core` builds the `Message` and hands it
//! to a `Mailer` the binary supplies (file or SMTP) — so the transport/config stays runtime-side
//! while the deliver *flow* lives here in the digest slice.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use lettre::{message::MultiPart, Message};

use crate::digest::select::{Format, ItemReason};
use crate::digest::store::RenderItem;
use crate::identity::ConfidenceBand;

/// The human-readable "why" for an item (design §10.2): its format, the richness phrase that chose
/// that format, and its relevance. Rendered as a caption on every item and in the plaintext fallback,
/// so "shown because…" is visible, not only in the debug trace.
fn reason_line(reason: &ItemReason) -> String {
    let format = match reason.format {
        Format::Story => "Story",
        Format::Note => "Note",
    };
    let richness = if reason.richness.is_empty() {
        "—"
    } else {
        &reason.richness
    };
    format!("{format} · {richness} · relevance {:.2}", reason.relevance)
}

/// The deterministic big-picture lead (`llm-summarization.md` §2.4/§3.1): one sentence composed at
/// fire time from the selected items' headlines — **no model call on the punctual path**. Leads with
/// the top-ranked item's headline (what dominated) and notes how much else moved. Falls back to the
/// raw cluster `title` per item (carried on [`RenderItem::headline`]), so it reads sensibly with the
/// summarization feature off too. Empty for an empty item list (the caller renders the empty digest).
fn compose_lead(items: &[RenderItem]) -> String {
    // The dominant line is the top-ranked item's headline (selection is in priority order); fall to
    // the next one only if it has no real content. The predicate is `trim_sentence_end(h)` non-empty,
    // not just `h` non-empty, so a punctuation-only title ("???", "...") is skipped rather than
    // producing an empty clause — and it guarantees the clause below (and `end_sentence`) gets text.
    // An all-blank set yields no lead. The "N more" count is over **all** rendered items, so it can
    // never disagree with the rows below (a blank/skipped headline still counts as a row).
    let Some(lead) = items
        .iter()
        .map(|i| i.headline.trim())
        .find(|h| !trim_sentence_end(h).is_empty())
    else {
        return String::new();
    };
    let rest = items.len() - 1;
    if rest == 0 {
        // One item: the headline *is* the lead sentence — keep its own terminal punctuation.
        format!("Leading this digest: {}", end_sentence(lead))
    } else {
        // Several: the headline is a mid-sentence clause before ", with …", so strip any terminator
        // and let the template supply the single closing period.
        let unit = if rest == 1 { "update" } else { "updates" };
        format!(
            "Leading this digest: {}, with {rest} more {unit} below.",
            trim_sentence_end(lead)
        )
    }
}

/// Finish a headline as a whole sentence: keep its own terminal punctuation (so an ellipsis or "!" is
/// preserved intact), appending a period only when it ends without one. Used for the single-item lead,
/// where the headline stands as the entire sentence.
fn end_sentence(s: &str) -> String {
    let s = s.trim_end();
    if s.ends_with(['.', '!', '?']) {
        s.to_string()
    } else {
        format!("{s}.")
    }
}

/// Strip any trailing terminator run from a headline used **mid-sentence** (before ", with …"), so the
/// clause reads cleanly and the lead template supplies the sentence's single closing period. Headlines
/// are abstractive and usually unterminated, but the deterministic baseline reuses the raw cluster
/// `title`, which may end in one (or several, e.g. an ellipsis).
fn trim_sentence_end(s: &str) -> &str {
    s.trim_end_matches(['.', '!', '?']).trim_end()
}

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
/// touching this module. The big-picture lead and per-item summaries are now fed by the data model
/// (composed deterministically from the items' summaries, `llm-summarization.md` §2.4/§6.1); the only
/// remaining stand-in is the per-item `item_category`, HTML-marked for removal.
#[derive(Clone, Copy)]
pub struct DigestContent<'a> {
    /// Small-caps brand label at the very top (e.g. "Bulletin").
    pub brand: &'a str,
    /// Serif masthead headline beneath the brand label (e.g. "Your Digest").
    pub title: &'a str,
    /// Footer note rendered beneath the items.
    pub footer: &'a str,
    /// Per-item category label (e.g. "Geopolitics/Diplomacy"). **Placeholder** until items carry one.
    pub item_category: &'a str,
}

impl Default for DigestContent<'_> {
    fn default() -> Self {
        Self {
            brand: "Bulletin",
            title: "Your Digest",
            footer: "You're receiving this digest from Bulletin, \
                     gathered from the sources you subscribed to.",
            // The last lorem-ipsum stand-in: a section the reference design has but our data model
            // doesn't feed yet. Wrapped in `<!-- PLACEHOLDER … -->` markers in the rendered HTML.
            item_category: "Lorem / Ipsum",
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
    let tz = subscriber_tz(timezone);
    // Compose the deterministic big-picture lead once (§2.4) and share it across both bodies, so the
    // plaintext and HTML views can never carry a different lead for the same digest.
    let lead = compose_lead(items);
    let plain = render_plain(window_end, tz, greeting, &lead, items);
    let html = render_html(window_end, tz, greeting, &lead, items, content);
    // The greeting doubles as the subject line, so the inbox preview opens in the same warm,
    // time-of-day voice as the digest's lead (and varies per window the same way).
    build_message(from, to, greeting, plain, html, "digest email")
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
    let tz = subscriber_tz(timezone);
    let plain = render_empty_plain(window_end, tz, salutation);
    let html = render_empty_html(window_end, tz, salutation, content);
    // Mirror the populated digest: the subject is the email's own opening line, so the inbox
    // preview reads in the same time-of-day voice as the body.
    let subject = format!("{salutation}. You're all caught up");
    build_message(from, to, &subject, plain, html, "empty digest email")
}

/// Resolves the subscriber's IANA zone, so the masthead date matches when they actually receive the
/// digest. An unparseable name can't reach here (the DB rejects it on signup/update), but fall back
/// to UTC rather than panic if one ever does.
fn subscriber_tz(timezone: &str) -> Tz {
    timezone.parse().unwrap_or(Tz::UTC)
}

/// Assembles the `multipart/alternative` message from a rendered plain+HTML pair. Shared by the
/// populated and empty digests, which differ only in their bodies and subject line. `what` names the
/// email for the error context.
fn build_message(
    from: &str,
    to: &str,
    subject: &str,
    plain: String,
    html: String,
    what: &str,
) -> Result<Message> {
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
        .with_context(|| format!("building {what}"))
}

/// Plaintext fallback: a numbered list of items (headline, grounded tldr, link, source, time). Kept
/// deliberately plain — this is what HTML-averse clients and screen-reader-friendly setups fall back
/// to. Opens with the same deterministic big-picture lead the HTML view carries (§2.4).
fn render_plain(
    window_end: DateTime<Utc>,
    tz: Tz,
    greeting: &str,
    lead: &str,
    items: &[RenderItem],
) -> String {
    // Join the lead onto the greeting only when there is one, so an empty lead leaves no dangling space.
    let opening = if lead.is_empty() {
        greeting.to_string()
    } else {
        format!("{greeting} {lead}")
    };
    let mut body = format!(
        "{opening}\n\nYour digest for the window ending {}\n\n",
        window_end.with_timezone(&tz).format("%Y-%m-%d %H:%M %Z"),
    );
    for (i, item) in items.iter().enumerate() {
        body.push_str(&format!(
            "{}. [{}] {}\n",
            i + 1,
            item.reason.format.as_str().to_uppercase(),
            item.headline
        ));
        // The grounded one-sentence tldr (§6.1 zone 3), when a summary has run; omitted otherwise.
        if !item.summary.trim().is_empty() {
            body.push_str(&format!("   {}\n", item.summary));
        }
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
        // The human-readable "why" (design §10.2): format, richness, relevance.
        body.push_str(&format!("   why: {}\n", reason_line(&item.reason)));
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
/// The lead opens with the time-of-day greeting, then the deterministic big-picture summary composed
/// from the selected items' headlines (§2.4). Per item, the headline is the representative cluster's
/// `summary.headline` (falling back to the raw `title`) and the summary line is its grounded `tldr`
/// (omitted when no summary has run). The remaining placeholder — the per-item category — renders
/// parametric lorem wrapped in `<!-- PLACEHOLDER … -->`; the source · time caption is debug-only,
/// wrapped in `<!-- DEBUG … -->` and styled as a distinct monospace amber callout so it never reads
/// as real content — both are easy to grep out later.
///
/// Table-based, all-inline-CSS, single column — the "bulletproof" shape that survives the
/// patchwork of email clients. The chrome comes from `content`; every piece of caller- or
/// feed-supplied text is HTML-escaped, since it's interpolated into markup.
fn render_html(
    window_end: DateTime<Utc>,
    tz: Tz,
    greeting: &str,
    lead: &str,
    items: &[RenderItem],
    content: &DigestContent<'_>,
) -> String {
    let count = items.len();
    let plural = if count == 1 { "" } else { "s" };
    let date = window_end
        .with_timezone(&tz)
        .format("%A, %B %-d, %Y")
        .to_string();
    let preheader = format!("{count} new item{plural} in your digest");
    let greeting = escape(greeting);
    // The big-picture lead is composed once by the caller (§2.4) — deterministic, no model, no lorem.
    // Prefix it with a single space only when present, so an empty lead leaves no stray gap after the
    // greeting (mirrors the plaintext join).
    let lead_html = if lead.is_empty() {
        String::new()
    } else {
        format!(" {}", escape(lead))
    };

    // The category placeholder is identical across items today, so escape it once and reuse.
    let category = escape(content.item_category);

    // One divider, reused between every item and above the footer.
    let divider = soft_divider();
    let mut rows = String::new();
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            rows.push_str(&divider);
        }
        // Format ≠ importance: items are in global priority order; a Story renders as a rich card, a
        // Note as a compact one-liner (design §8.4/§9.5). The format is purely a richness-driven
        // rendering difference.
        rows.push_str(&match item.reason.format {
            Format::Story => render_story_row(i, item, tz, &category),
            Format::Note => render_note_row(item, tz),
        });
    }

    let masthead = format!(
        r#"{head}<div style="font-family:{SERIF};font-size:15px;font-style:italic;font-weight:700;color:{ACCENT};margin:34px 0 12px 0;">The big picture</div>
<!-- Lead: a time-of-day greeting (real) opens the paragraph, then the deterministic big-picture summary composed from the selected items' headlines (§2.4). -->
<div class="lead" style="font-family:{SERIF};font-size:17px;line-height:1.75;color:{INK_BODY};margin:0;"><strong style="color:{INK};font-weight:700;">{greeting}</strong>{lead_html}</div>
<div style="font-family:{SERIF};font-size:15px;font-style:italic;font-weight:700;color:{ACCENT};margin:40px 0 0 0;">In this digest &middot; {count} item{plural}</div>
"#,
        head = masthead_head(content, &date),
    );
    document(&preheader, &masthead, &format!("{rows}{divider}"), content)
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
    let date = window_end
        .with_timezone(&tz)
        .format("%A, %B %-d, %Y")
        .to_string();
    let salutation = escape(salutation);

    let masthead = format!(
        r#"{head}<div style="text-align:center;padding:46px 8px 18px 8px;">
<div style="font-size:52px;line-height:1;" aria-hidden="true">&#x1F343;</div>
<div style="margin:24px 0 0 0;font-family:{SERIF};font-size:28px;font-weight:700;line-height:1.3;color:{ACCENT};">You're all caught up</div>
<div style="margin:14px auto 0 auto;max-width:360px;font-family:{SERIF};font-size:17px;font-style:italic;line-height:1.7;color:{INK_BODY};">{salutation}. No new notifications this time. Sit back and enjoy the calm.</div>
</div>
"#,
        head = masthead_head(content, &date),
    );
    document(
        "Nothing new — you're all caught up",
        &masthead,
        &soft_divider(),
        content,
    )
}

/// The shared HTML shell: doctype, `<head>` (+ the mobile stylesheet), the warm centered card, and
/// the footer. Both digests fill only `masthead` (the inner content of the top card cell — brand,
/// title, date rule, and their own lead) and `body` (what follows the masthead row: the item rows
/// for a populated digest, just a divider for an empty one). `preheader` is the hidden inbox preview.
fn document(preheader: &str, masthead: &str, body: &str, content: &DigestContent<'_>) -> String {
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
{masthead}</td>
</tr>
{body}<tr>
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
"#
    )
}

/// The top of the masthead shared by both digests: the small-caps brand label, the serif title, and
/// the `── date ──` rule. The populated and empty views append their own lead beneath it. Ends with
/// a trailing newline so the caller's lead slots straight on.
fn masthead_head(content: &DigestContent<'_>, date: &str) -> String {
    let brand = escape(content.brand);
    let title = escape(content.title);
    format!(
        r#"<div style="font-family:{SANS};font-size:12px;font-weight:600;letter-spacing:0.12em;text-transform:uppercase;color:{INK_MUTED};text-align:center;">{brand}</div>
<h1 style="margin:18px 0 30px 0;font-family:{SERIF};font-size:34px;font-weight:700;line-height:1.25;color:{INK};text-align:center;">{title}</h1>
{date_rule}
"#,
        date_rule = date_rule(date),
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

/// A **Story** row (design §8.4 richness → format): the rich editorial card — an optional thread
/// chip, the editor-grade `headline` (a link when the cluster has one), a placeholder category, the
/// grounded one-sentence summary (§6.1 zone 3 — the representative cluster's `tldr`, omitted when no
/// summary has run), the fused cross-source connections, a human-readable reason caption, then the
/// debug block. `category` is the last pre-escaped placeholder; the reason caption is the "why"
/// (design §10.2); the debug block is debug-only — a monospace, dashed-amber callout (a `debug` tag
/// so it reads as scaffolding) carrying the machine trace: priority position, ranking basis, recency
/// key, source, fused-item count, thread assignment, entity spine, and raw link. `position` is the
/// 0-based render slot (global priority order). Items are separated by [`soft_divider`], not a rule.
fn render_story_row(position: usize, item: &RenderItem, tz: Tz, category: &str) -> String {
    let headline_text = escape(&item.headline);
    let headline = match &item.link {
        Some(link) => format!(
            r#"<a class="headline" href="{}" style="font-family:{SERIF};font-size:21px;font-weight:700;line-height:1.3;color:{INK};text-decoration:none;">{headline_text}</a>"#,
            escape(link)
        ),
        None => format!(
            r#"<span class="headline" style="font-family:{SERIF};font-size:21px;font-weight:700;line-height:1.3;color:{INK};">{headline_text}</span>"#
        ),
    };

    // The grounded TL;DR sentence (§6.1): real content set in upright body serif (a confident
    // statement, not a muted italic aside). Omitted entirely until a summary has run — no placeholder.
    let summary = if item.summary.trim().is_empty() {
        String::new()
    } else {
        format!(
            r#"<div class="meta" style="margin-top:12px;font-family:{SERIF};font-size:16px;line-height:1.65;color:{INK_BODY};">{}</div>
"#,
            escape(&item.summary)
        )
    };

    let connections = render_connections(&item.connections);
    let thread_chip = render_thread_chip(item.thread.as_ref());
    let reason = escape(&reason_line(&item.reason));
    let debug = debug_trace_block(position, item, tz);

    format!(
        r#"<tr>
<td class="bx" style="padding:30px 40px;">
{thread_chip}{headline}
<!-- PLACEHOLDER: per-item category — remove or replace once items carry a category -->
<div class="meta" style="margin-top:9px;font-family:{SANS};font-size:13px;font-weight:500;letter-spacing:0.04em;color:{ACCENT};">{category}</div>
<!-- /PLACEHOLDER -->
{summary}{connections}<div class="meta" style="margin-top:16px;font-family:{SANS};font-size:12px;line-height:1.6;color:{INK_MUTED};"><span style="font-weight:600;color:{ACCENT};">Why</span> &middot; {reason}</div>
{debug}</td>
</tr>
"#
    )
}

/// The debug-only selection trace beneath a story card: a monospace, dashed-amber callout carrying
/// the machine trace (1-based rank, ranking basis, recency key, source, fused-item count, thread
/// assignment, entity spine, raw link — design §10.2). Styled to read as scaffolding, and isolated
/// in one function so it's a single block to strip before launch.
fn debug_trace_block(position: usize, item: &RenderItem, tz: Tz) -> String {
    let rank = position + 1;
    let source = escape(item.source.as_str());
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
    // priority is the ranking key (relevance + severity, recency-decayed); the entity spine is what
    // relevance scored on; the thread line records the assignment + its identity band.
    let basis = format!(
        "priority {:.3}, richness {}",
        item.reason.priority, item.reason.richness
    );
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
        r#"<!-- DEBUG: selection trace — debugging info, remove before launch -->
<div style="margin-top:12px;padding:9px 12px;background-color:{DEBUG_BG};border:1px dashed {DEBUG_BORDER};border-radius:3px;font-family:{MONO};font-size:12px;line-height:1.6;color:{DEBUG_INK};">
<span style="display:inline-block;padding:1px 6px;margin-right:8px;background-color:{DEBUG_BORDER};color:{DEBUG_BG};font-weight:700;text-transform:uppercase;letter-spacing:0.1em;border-radius:2px;">debug</span>
<span style="font-weight:700;">selected #{rank}</span> <span>by {basis}</span>
<div style="margin-top:4px;">key {recency_key} &middot; source <span style="font-weight:700;">{source}</span> &middot; related {related}</div>
{thread_trace}{spine}<div style="margin-top:4px;word-break:break-all;">link {link}</div>
</div>
<!-- /DEBUG -->
"#
    )
}

/// A **Note** row (design §8.4/§9.5): the compact one-liner for a thin, atomic item — an optional
/// thread chip, a terracotta `Note` tag, the headline (linked when it has a URL), a source · time
/// caption, the human reason, and any fused connections. No lorem placeholders or amber debug block;
/// a Note is deliberately terse. Format is purely a richness-driven rendering difference — a Note can
/// still out-rank a Story (it's interleaved in global priority order).
fn render_note_row(item: &RenderItem, tz: Tz) -> String {
    let headline_text = escape(&item.headline);
    let headline = match &item.link {
        Some(link) => format!(
            r#"<a href="{}" style="font-family:{SERIF};font-size:17px;font-weight:700;line-height:1.4;color:{INK};text-decoration:none;">{headline_text}</a>"#,
            escape(link)
        ),
        None => format!(
            r#"<span style="font-family:{SERIF};font-size:17px;font-weight:700;line-height:1.4;color:{INK};">{headline_text}</span>"#
        ),
    };
    let source = escape(item.source.as_str());
    let when = item
        .last_event_time
        .with_timezone(&tz)
        .format("%Y-%m-%d %H:%M %Z");
    let reason = escape(&reason_line(&item.reason));
    let connections = render_connections(&item.connections);
    let thread_chip = render_thread_chip(item.thread.as_ref());

    format!(
        r#"<tr>
<td class="bx" style="padding:18px 40px;">
{thread_chip}<span style="display:inline-block;margin-bottom:6px;font-family:{SANS};font-size:11px;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:{ACCENT};">Note</span>
<div>{headline}</div>
<div class="meta" style="margin-top:6px;font-family:{SANS};font-size:12px;line-height:1.6;color:{INK_MUTED};">{source} &middot; {when}</div>
<div class="meta" style="margin-top:4px;font-family:{SANS};font-size:12px;line-height:1.6;color:{INK_MUTED};"><span style="font-weight:600;color:{ACCENT};">Why</span> &middot; {reason}</div>
{connections}</td>
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
            // No summary ran: the headline degrades to the raw title and the tldr is empty (the
            // renderer then omits the summary line), matching a feature-off / not-yet-summarized item.
            headline: title.to_string(),
            summary: String::new(),
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
            &compose_lead(&items),
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
            &compose_lead(&items),
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
        // A summarized item carries an editor-grade headline + a grounded tldr; render them as real
        // content (no lorem placeholders), and compose the lead from the headline deterministically.
        let summarized = RenderItem {
            headline: "EDITOR_HEADLINE".to_string(),
            summary: "GROUNDED_TLDR_SENTENCE".to_string(),
            ..item("RAW_TITLE", Some("https://example.com"), SourceKind::Rss)
        };
        let items = vec![summarized];
        let content = DigestContent {
            item_category: "CAT_TEXT",
            ..DigestContent::default()
        };
        let html = render_html(
            Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(),
            Tz::UTC,
            "GREETING_TEXT",
            &compose_lead(&items),
            &items,
            &content,
        );

        // The greeting opens the lead as real content, ahead of the deterministic big-picture summary
        // — which is composed from the item's headline (no model, no lorem).
        assert!(html.contains("GREETING_TEXT"));
        let greeting_at = html.find("GREETING_TEXT").unwrap();
        let lead_at = html.find("Leading this digest: EDITOR_HEADLINE").unwrap();
        assert!(greeting_at < lead_at, "greeting must precede the lead");
        // The editor-grade headline (not the raw title) leads the item, and the grounded tldr renders
        // as real upright body content — no lorem, no big-picture placeholder.
        assert!(html.contains("EDITOR_HEADLINE"));
        assert!(html.contains("GROUNDED_TLDR_SENTENCE"));
        assert!(!html.contains("RAW_TITLE"));
        assert!(!html.contains("Lorem ipsum"));
        // The only remaining placeholder is the per-item category.
        assert!(html.contains("CAT_TEXT"));
        assert_eq!(html.matches("<!-- PLACEHOLDER:").count(), 1);
        assert!(html.contains("<!-- DEBUG: selection trace"));
        assert!(html.contains("<!-- /DEBUG -->"));
        // The debug block is styled to look unmistakably like scaffolding — a monospace,
        // dashed-amber callout carrying a visible `debug` tag — so it can't pass for real content.
        assert!(html.contains(MONO));
        assert!(html.contains(DEBUG_BG));
        assert!(html.contains(&format!("border:1px dashed {DEBUG_BORDER}")));
        assert!(html.contains(">debug</span>"));
        // ...and it carries the selection trace: rank, ranking basis, related count, link.
        assert!(html.contains("selected #1"));
        assert!(html.contains("by priority"));
        assert!(html.contains("related 0"));
        assert!(html.contains("link https://example.com"));
        // The human-readable reason caption (non-debug) names format + richness + relevance.
        assert!(html.contains(">Why</span>"));
    }

    #[test]
    fn note_renders_compact_without_placeholders_or_debug() {
        let note = RenderItem {
            reason: crate::digest::select::ItemReason {
                format: Format::Note,
                richness: "announcement".to_string(),
                ..Default::default()
            },
            ..item(
                "A small ping",
                Some("https://example.com/n"),
                SourceKind::Github,
            )
        };
        let items = vec![note];
        let html = render_html(
            Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(),
            Tz::UTC,
            "GREETING",
            &compose_lead(&items),
            &items,
            &DigestContent::default(),
        );
        // The Note tag + headline (degraded to the raw title) + reason are present...
        assert!(html.contains(">Note</span>"));
        assert!(html.contains("A small ping"));
        assert!(html.contains("Note · announcement"));
        // ...and with the big-picture lead now composed (no lorem) and a Note carrying no per-item
        // placeholders, there are no PLACEHOLDER markers at all and no amber debug block.
        assert_eq!(html.matches("<!-- PLACEHOLDER:").count(), 0);
        assert!(!html.contains("<!-- DEBUG:"));
    }

    #[test]
    fn lead_is_composed_from_headlines_deterministically() {
        // No items → empty lead (the empty-digest path renders its own copy).
        assert!(compose_lead(&[]).is_empty());

        // One item → leads with its headline; a trailing terminator is trimmed so it isn't doubled.
        let one = RenderItem {
            headline: "Auth outage traced to the deploy.".to_string(),
            ..item("raw", None, SourceKind::Rss)
        };
        assert_eq!(
            compose_lead(&[one]),
            "Leading this digest: Auth outage traced to the deploy."
        );

        // A single item's own terminal punctuation is preserved (no collapse, no doubled period):
        // an ellipsis stays an ellipsis, a "!" stays a "!".
        let mk = |h: &str| RenderItem {
            headline: h.to_string(),
            ..item("raw", None, SourceKind::Rss)
        };
        assert_eq!(
            compose_lead(&[mk("Talks continue...")]),
            "Leading this digest: Talks continue..."
        );
        assert_eq!(
            compose_lead(&[mk("Servers are back!")]),
            "Leading this digest: Servers are back!"
        );

        // Several → leads with the top-ranked headline + a count of the rest (pluralized). The top
        // headline is mid-sentence here, so its own trailing terminator is stripped before ", with".
        assert_eq!(
            compose_lead(&[mk("Top story."), mk("Second")]),
            "Leading this digest: Top story, with 1 more update below."
        );
        assert_eq!(
            compose_lead(&[mk("Top story"), mk("Second"), mk("Third")]),
            "Leading this digest: Top story, with 2 more updates below."
        );

        // The "N more" count is over *all* rendered items, so a blank-headline row still counts (the
        // lead text falls through to the first non-blank headline) — count never disagrees with rows.
        let blank = RenderItem {
            headline: "  ".to_string(),
            ..item("raw", None, SourceKind::Rss)
        };
        assert_eq!(
            compose_lead(&[mk("Top story"), blank]),
            "Leading this digest: Top story, with 1 more update below."
        );

        // A punctuation-only top headline ("???", "...") has no real content: it is skipped for the
        // lead text (falling through to the next), but still counts as a rendered row. It must never
        // produce an empty clause like "Leading this digest: , with …".
        assert_eq!(
            compose_lead(&[mk("???"), mk("Real headline")]),
            "Leading this digest: Real headline, with 1 more update below."
        );
        // ...and when it is the only item, there is simply no lead (not "Leading this digest: ???").
        assert!(compose_lead(&[mk("...")]).is_empty());
    }

    #[test]
    fn story_without_a_summary_omits_the_tldr_line() {
        // A not-yet-summarized story: the headline degrades to the raw title and no tldr is rendered.
        let items = vec![item(
            "Just a title",
            Some("https://example.com"),
            SourceKind::Rss,
        )];
        let html = render_html(
            Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(),
            Tz::UTC,
            "Hi.",
            &compose_lead(&items),
            &items,
            &DigestContent::default(),
        );

        assert!(html.contains("Just a title"));
        // The body-serif summary line only appears when the item carries a tldr — none here.
        let summary_style = format!("font-size:16px;line-height:1.65;color:{INK_BODY}");
        assert!(!html.contains(&summary_style));
        // The lead still composes from the (title-derived) headline.
        assert!(html.contains("Leading this digest: Just a title."));
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
            &compose_lead(&items),
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
            &compose_lead(&items),
            &items,
        );

        // The greeting opens the plaintext fallback, then the composed lead, ahead of the item list.
        assert!(plain.starts_with(
            "Good morning. Here's your daily digest. Leading this digest: Hello."
        ));
        assert!(plain.contains("1. [STORY] Hello"));
        assert!(plain.contains("https://example.com"));
        assert!(plain.contains("rss ·"));
        // The human reason line is carried in plaintext too.
        assert!(plain.contains("why: Story · "));
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
