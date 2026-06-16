//! Rendering a digest to an email + the delivery seam. `core` builds the `Message` and hands it
//! to a `Mailer` the binary supplies (file or SMTP) — so the transport/config stays runtime-side
//! while the deliver *flow* lives here in the digest slice.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use lettre::{message::MultiPart, Message};

use crate::digest::select::{Format, ItemReason};
use crate::digest::store::{RenderItem, ThreadTag};
use crate::identity::ConfidenceBand;
use crate::summarize::TldrRun;

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
/// touching this module. Every per-item slot is now fed by the data model (the §6.1 four-zone redesign
/// — context eyebrow, headline, grounded summary, provenance — all composed from the items'
/// cluster/story/thread summaries); **no lorem placeholders remain.** The per-item category kicker was
/// retired by the Phase-B context eyebrow (`llm-summarization.md` §1.1/§6.1).
#[derive(Clone, Copy)]
pub struct DigestContent<'a> {
    /// Small-caps brand label at the very top (e.g. "Bulletin").
    pub brand: &'a str,
    /// Serif masthead headline beneath the brand label (e.g. "Your Digest").
    pub title: &'a str,
    /// Footer note rendered beneath the items.
    pub footer: &'a str,
}

impl Default for DigestContent<'_> {
    fn default() -> Self {
        Self {
            brand: "Bulletin",
            title: "Your Digest",
            footer: "You're receiving this digest from Bulletin, \
                     gathered from the sources you subscribed to.",
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
            clean(&item.headline)
        ));
        // The context eyebrow (§6.1 zone 1): the thread label + delta flag, when threaded. Plaintext
        // shows the label regardless of band (no "possibly"/omit budgeting — that's a visual nicety).
        if let Some(tag) = item.thread.as_ref().filter(|t| !t.label.trim().is_empty()) {
            match tag.delta.as_deref().filter(|d| !d.trim().is_empty()) {
                Some(delta) => {
                    body.push_str(&format!("   ~ {} · {}\n", clean(&tag.label), clean(delta)))
                }
                None => body.push_str(&format!("   ~ {}\n", clean(&tag.label))),
            }
        }
        // The grounded one-sentence tldr (§6.1 zone 3), when a summary has run; omitted otherwise.
        if !item.summary.trim().is_empty() {
            body.push_str(&format!("   {}\n", clean(&item.summary)));
        }
        // Only print a link the HTML view would also make live (http/https/mailto) — an unsafe scheme is
        // dropped rather than shown as text, so the two views agree and no `javascript:`/`data:` URL ever
        // reaches the reader. Sanitized of control/bidi characters by `safe_href`.
        if let Some(link) = item.link.as_deref().and_then(safe_href) {
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
            body.push_str(&format!("   ↳ {} [{}]", clean(&conn.title), conn.source.as_str()));
            if let Some(reason) = &conn.link_reason {
                body.push_str(&format!(" — {}", clean(reason)));
            }
            body.push('\n');
            if let Some(link) = conn.link.as_deref().and_then(safe_href) {
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

// Inline entity-badge tints (§6.2): a CVE pill reads as a severity chip — a warm tint of the accent,
// distinct from running body copy but still on-palette.
const BADGE_CVE_BG: &str = "#fbe6dd"; // pale terracotta pill background
const BADGE_CVE_INK: &str = "#b23c12"; // deeper terracotta, the pill text

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
            Format::Story => render_story_row(i, item, tz),
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

/// A **Story** row, redesigned around the LLM summary (`llm-summarization.md` §6.1) into four quiet
/// zones, the scaffolding moved off the email:
///
/// 1. **Context eyebrow** — the thread label + a terse delta flag, on one clamped line ([`render_eyebrow`]);
///    omitted entirely for an un-threaded item.
/// 2. **Headline** — the editor-grade `headline` (a link when the cluster has one).
/// 3. **Summary** — the one grounded TL;DR sentence in upright body serif, with inline entity
///    **badges** rendered from the structured run-list ([`render_summary_runs`]); omitted when no
///    summary has run.
/// 4. **Provenance** — `Across <source> · <source> — <when>` (multi-source) or `<source> — <when>`
///    (single), the M3 cross-source value made calm ([`render_provenance`]) — this replaces *both* the
///    verbose "Related" list and the `Why · relevance` caption on the email.
///
/// The amber **debug block is kept** (and enriched): everything the four zones shed — the fused
/// connections, the relevance/priority "why", the thread/identity/delta trace, the summary provenance —
/// now lives in the [`debug_trace_block`] so the selection + summarization trace stays inspectable.
/// `position` is the 0-based render slot (global priority order). Items are separated by
/// [`soft_divider`], not a rule.
fn render_story_row(position: usize, item: &RenderItem, tz: Tz) -> String {
    let headline_text = escape(&item.headline);
    let headline = match item.link.as_deref().and_then(safe_href) {
        Some(link) => format!(
            r#"<a class="headline" href="{}" style="display:block;margin-top:10px;font-family:{SERIF};font-size:22px;font-weight:700;line-height:1.32;color:{INK};text-decoration:none;">{headline_text}</a>"#,
            escape(&link)
        ),
        None => format!(
            r#"<span class="headline" style="display:block;margin-top:10px;font-family:{SERIF};font-size:22px;font-weight:700;line-height:1.32;color:{INK};">{headline_text}</span>"#
        ),
    };

    // Zone 3 — the grounded TL;DR sentence (§6.1/§6.2): real content set in upright body serif (a
    // confident statement, not a muted italic aside), with inline entity badges from the run-list.
    // Omitted entirely until a summary has run — no placeholder.
    let summary = if item.summary_runs.is_empty() {
        String::new()
    } else {
        format!(
            r#"<div class="related" style="margin-top:11px;font-family:{SERIF};font-size:17px;line-height:1.65;color:{INK_BODY};">{}</div>
"#,
            render_summary_runs(&item.summary_runs)
        )
    };

    let eyebrow = render_eyebrow(item.thread.as_ref());
    let provenance = render_provenance(item, tz);
    let debug = debug_trace_block(position, item, tz);

    format!(
        r#"<tr>
<td class="bx" style="padding:30px 40px;">
{eyebrow}{headline}
{summary}{provenance}
{debug}</td>
</tr>
"#
    )
}

/// Zone 4 — the **provenance** line (§6.1): the distinct sources the story fused, made calm. `Across
/// <source> · <source> · <source> — <when>` for a multi-source story (surfacing the M3 cross-source
/// value as the confidence line), or just `<source> — <when>` for a single source. Replaces both the
/// verbose "Related" list and the machine "why" caption on the email (those move to the debug block).
/// Sources are the representative's plus each connection's, de-duplicated in first-seen order.
fn render_provenance(item: &RenderItem, tz: Tz) -> String {
    let mut sources: Vec<&str> = Vec::new();
    for s in std::iter::once(item.source.as_str())
        .chain(item.connections.iter().map(|c| c.source.as_str()))
    {
        if !sources.contains(&s) {
            sources.push(s);
        }
    }
    let when = item
        .last_event_time
        .with_timezone(&tz)
        .format("%Y-%m-%d %H:%M %Z");
    let tagged: Vec<String> = sources
        .iter()
        .map(|s| format!(r#"<span style="color:{ACCENT};">{}</span>"#, escape(s)))
        .collect();
    let body = if tagged.len() > 1 {
        format!("Across {}", tagged.join(" &middot; "))
    } else {
        tagged.join("")
    };
    format!(
        r#"<div class="meta" style="margin-top:14px;font-family:{SANS};font-size:12px;line-height:1.6;color:{INK_MUTED};">{body} &nbsp;&mdash;&nbsp; {when}</div>
"#
    )
}

/// Zone 3 inner HTML — render the structured TL;DR run-list (§6.2): plain `text` runs escaped inline,
/// and grounded entity `ref` runs as type-styled inline **badges** ([`render_entity_badge`]). The
/// model can only reference an entity in the closed grounded set (the schema enum), so a badge can
/// never name a thing that wasn't extracted from ground truth; an unrecognised namespace degrades to
/// plain `surface` text — never a broken badge (the plaintext view uses the flat `tldr_text`).
fn render_summary_runs(runs: &[TldrRun]) -> String {
    let mut out = String::new();
    for run in runs {
        match run {
            TldrRun::Text { text } => out.push_str(&escape(text)),
            TldrRun::Ref { entity, surface } => out.push_str(&render_entity_badge(entity, surface)),
        }
    }
    out
}

/// One inline entity badge, styled by the `ref` token's namespace (§6.2): a `repo:` dotted-underline
/// tag, a `cve:` severity-tinted pill, a `user:` person chip, anything else plain. Rendering owns the
/// treatment; the model only picks which grounded token to reference and its visible `surface` text.
/// Identity resolution + avatars are a later layer — for now the badge is namespace-styled and the
/// surface text is shown verbatim, degrading gracefully to plain text for an unknown namespace.
fn render_entity_badge(entity: &str, surface: &str) -> String {
    let s = escape(surface);
    match crate::identity::namespace(entity).map(|(ns, _)| ns) {
        Some("repo") => format!(
            r#"<span style="border-bottom:1px dotted {ACCENT};font-weight:600;color:{INK_BODY};">{s}</span>"#
        ),
        Some("cve") => format!(
            r#"<span style="font-family:{SANS};font-size:13px;font-weight:600;background:{BADGE_CVE_BG};color:{BADGE_CVE_INK};padding:1px 7px;border-radius:10px;">{s}</span>"#
        ),
        Some("user") => format!(r#"<span style="font-weight:600;color:{INK};">{s}</span>"#),
        _ => s,
    }
}

/// The debug-only selection + **summarization** trace beneath a story card: a monospace, dashed-amber
/// callout carrying the machine trace the four editorial zones (§6.1) deliberately shed, so the audit
/// detail stays inspectable. Kept (not deleted) and enriched for Phases B/C with:
///
/// - **selection** — 1-based rank, ranking basis (priority/richness), recency key, source, fused
///   count, entity spine, raw link (design §10.2);
/// - **summary** (Phase A/C) — the headline/tldr's provenance (story synthesis vs representative
///   cluster vs raw title) and its faithfulness `band`;
/// - **thread** (Phase B) — the assigned thread label, its identity band, and the §5.2 delta flag;
/// - **related** — every fused cross-source connection with its `link_reason` (the §8.2 value that was
///   on the email as the "Related" list, now here).
///
/// Styled to read as scaffolding and isolated in one function so it's a single block to strip later.
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
    // relevance scored on; the thread line records the assignment + its identity band + delta.
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

    // Summary provenance (Phase A/C): where the rendered headline/tldr came from, and its band. The
    // story cache (`item.synthesized`) may hold a true cross-source fusion *or* the representative
    // cluster summary it fell back to on a gate rejection — render can't tell them apart, so the label
    // is the honest "story.summary cache" rather than asserting a synthesis happened.
    let summary_origin = if item.summary_runs.is_empty() {
        "raw title (no summary)"
    } else if item.synthesized {
        "story.summary cache"
    } else {
        "cluster summary"
    };
    let summary_trace = format!(
        r#"<div style="margin-top:4px;">summary <span style="font-weight:700;">{summary_origin}</span> &middot; band {}</div>"#,
        item.summary_band.as_str(),
    );

    // Thread trace (Phase B): label · identity band · delta flag.
    let thread_trace = match &item.thread {
        Some(t) => {
            let delta = t
                .delta
                .as_deref()
                .filter(|d| !d.trim().is_empty())
                .map(|d| format!(" &middot; delta &ldquo;{}&rdquo;", escape(d)))
                .unwrap_or_default();
            format!(
                r#"<div style="margin-top:4px;">thread <span style="font-weight:700;">{}</span> &middot; identity {}{delta}</div>"#,
                escape(&t.label),
                t.confidence.as_str()
            )
        }
        None => String::new(),
    };

    // Related (§8.2): the fused cross-source connections + each one's link_reason and click-through
    // URL, moved off the editorial email into the trace (so the cross-source detail stays inspectable).
    let mut related_trace = String::new();
    for c in &item.connections {
        let reason = c
            .link_reason
            .as_deref()
            .map(|r| format!(" &mdash; {}", escape(r)))
            .unwrap_or_default();
        let link = c
            .link
            .as_deref()
            .map(|l| format!(" &middot; {}", escape(l)))
            .unwrap_or_default();
        related_trace.push_str(&format!(
            r#"<div style="margin-top:4px;word-break:break-word;">&#8627; {} [{}]{reason}{link}</div>"#,
            escape(&c.title),
            escape(c.source.as_str()),
        ));
    }

    format!(
        r#"<!-- DEBUG: selection + summarization trace — debugging info, remove before launch -->
<div style="margin-top:16px;padding:9px 12px;background-color:{DEBUG_BG};border:1px dashed {DEBUG_BORDER};border-radius:3px;font-family:{MONO};font-size:12px;line-height:1.6;color:{DEBUG_INK};">
<span style="display:inline-block;padding:1px 6px;margin-right:8px;background-color:{DEBUG_BORDER};color:{DEBUG_BG};font-weight:700;text-transform:uppercase;letter-spacing:0.1em;border-radius:2px;">debug</span>
<span style="font-weight:700;">selected #{rank}</span> <span>by {basis}</span>
<div style="margin-top:4px;">key {recency_key} &middot; source <span style="font-weight:700;">{source}</span> &middot; related {related}</div>
{summary_trace}{thread_trace}{spine}{related_trace}<div style="margin-top:4px;word-break:break-all;">link {link}</div>
</div>
<!-- /DEBUG -->
"#
    )
}

/// A **Note** row (design §8.4/§9.5): the compact one-liner for a thin, atomic item — the same
/// grammar as a Story but terser: an optional context eyebrow, a terracotta `Note` tag, the headline
/// (linked when it has a URL), then the provenance line. No summary sentence (a Note is thin by
/// definition) and no debug block. Format is purely a richness-driven rendering difference — a Note
/// can still out-rank a Story (it's interleaved in global priority order).
fn render_note_row(item: &RenderItem, tz: Tz) -> String {
    let headline_text = escape(&item.headline);
    let headline = match item.link.as_deref().and_then(safe_href) {
        Some(link) => format!(
            r#"<a href="{}" style="display:block;margin-top:8px;font-family:{SERIF};font-size:17px;font-weight:700;line-height:1.4;color:{INK};text-decoration:none;">{headline_text}</a>"#,
            escape(&link)
        ),
        None => format!(
            r#"<span style="display:block;margin-top:8px;font-family:{SERIF};font-size:17px;font-weight:700;line-height:1.4;color:{INK};">{headline_text}</span>"#
        ),
    };
    let eyebrow = render_eyebrow(item.thread.as_ref());
    let provenance = render_provenance(item, tz);

    format!(
        r#"<tr>
<td class="bx" style="padding:18px 40px;">
{eyebrow}<span style="display:inline-block;margin-top:6px;font-family:{SANS};font-size:11px;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:{ACCENT};">Note</span>
{headline}
{provenance}</td>
</tr>
"#
    )
}

/// Zone 1 — the **context eyebrow** (§6.1/§1.1): the assigned thread label + a terse delta flag, on a
/// single line that can never wrap (`white-space:nowrap; overflow:hidden; text-overflow:ellipsis`).
/// This consolidates the old standalone thread chip (one thread reference per item, not two). Identity
/// doubt is *budgeted* (§10.4): a `Probable` thread shows a quiet italic "possibly" before the label;
/// an `Uncertain` one omits the eyebrow entirely (the doubt isn't worth a line). Empty for an
/// un-threaded item — it simply renders no eyebrow, exactly as the mockup's item 3.
fn render_eyebrow(thread: Option<&ThreadTag>) -> String {
    let Some(tag) = thread.filter(|t| !t.label.trim().is_empty()) else {
        return String::new();
    };
    let qualifier = match tag.confidence {
        ConfidenceBand::Confirmed => String::new(),
        ConfidenceBand::Probable => {
            r#"<span style="font-style:italic;">possibly</span> "#.to_string()
        }
        // Budget the doubt: an Uncertain thread isn't worth an eyebrow line at all (§6.1/§10.4).
        ConfidenceBand::Uncertain => return String::new(),
    };
    let label = escape(&tag.label);
    let delta = match &tag.delta {
        Some(d) if !d.trim().is_empty() => {
            format!(r#" &nbsp;&middot;&nbsp; {}"#, escape(d))
        }
        _ => String::new(),
    };
    format!(
        r#"<div class="meta" style="font-family:{SANS};font-size:12px;letter-spacing:0.02em;color:{INK_MUTED};white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{qualifier}<span style="color:{ACCENT};font-weight:600;">{label}</span>{delta}</div>
"#
    )
}

/// Characters we drop from untrusted feed/model text *before* HTML-escaping it — a class that can
/// never legitimately appear in a calm editorial digest line and that "renders weirdly" (or worse) when
/// it does:
///
/// - **C0/C1 control codes** (`U+0000`–`U+001F` except tab/newline, `U+007F`–`U+009F`) — non-printing
///   bytes that can smuggle a scheme past a URL check or break a line mid-field;
/// - **bidi embeddings, overrides and isolates** (`U+202A`–`U+202E`, `U+2066`–`U+2069`) — the
///   Trojan-Source / spoofing family that visually reorders text so a headline reads as something it
///   isn't;
/// - **zero-width and join controls + BOM** (`U+200B`–`U+200F`, `U+2060`–`U+2064`, `U+FEFF`) — invisible
///   characters that hide content or split words the eye can't see.
///
/// Tab (`U+0009`) and newline (`U+000A`) are kept — they're legitimate whitespace that HTML collapses
/// harmlessly. Everything else printable passes through to [`escape`].
fn is_unsafe_char(c: char) -> bool {
    matches!(c,
        '\u{0000}'..='\u{0008}' | '\u{000B}'..='\u{001F}' | '\u{007F}'..='\u{009F}'
        | '\u{200B}'..='\u{200F}' | '\u{2060}'..='\u{2064}' | '\u{2066}'..='\u{2069}'
        | '\u{202A}'..='\u{202E}' | '\u{FEFF}'
    )
}

/// HTML escaping for untrusted feed/model text, safe in both element-text and double-quoted attribute
/// contexts (the two places we interpolate). First strips the [`is_unsafe_char`] class (control / bidi /
/// zero-width) so a hostile or malformed feed can't reorder, hide, or break the rendered line, then
/// escapes the five HTML-significant characters so nothing can break out of the markup.
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if is_unsafe_char(c) {
            continue;
        }
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

/// Strip the [`is_unsafe_char`] class from untrusted text for the **plaintext** view, where there is no
/// markup to escape but a bidi-override or control character would still reorder or mangle the line in a
/// terminal or plain-text mail client (the same Trojan-Source concern, minus the injection). The HTML
/// view gets this stripping for free inside [`escape`]; plaintext fields call this directly.
fn clean(s: &str) -> String {
    s.chars().filter(|c| !is_unsafe_char(*c)).collect()
}

/// A feed-supplied URL we are willing to emit as a live `href` / link target, or `None` to render the
/// item **unlinked**. Feed links are untrusted: a `javascript:`, `data:`, or `vbscript:` URL executes
/// (or renders attacker markup) in the mail clients that honour it, and HTML-escaping does not stop it —
/// escaping only neutralizes the quotes that would break the attribute, not the scheme inside it. So we
/// allowlist **only** the schemes a digest link can legitimately use — `http`, `https`, `mailto` —
/// matched case-insensitively after stripping the control characters that could hide a scheme
/// (`java\u{0009}script:`) and trimming surrounding whitespace. Anything else (another scheme, or a
/// relative/scheme-relative URL we can't vet) drops to `None`, and the caller shows plain text instead of
/// a live link. The returned string still carries no control characters; callers HTML-escape it as usual.
fn safe_href(url: &str) -> Option<String> {
    let cleaned: String = url.chars().filter(|c| !is_unsafe_char(*c)).collect();
    let trimmed = cleaned.trim();
    let scheme = trimmed.to_ascii_lowercase();
    if scheme.starts_with("http://")
        || scheme.starts_with("https://")
        || scheme.starts_with("mailto:")
    {
        Some(trimmed.to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kind::SourceKind;
    use crate::digest::store::Connection;
    use crate::summarize::Band;
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
            summary_runs: Vec::new(),
            summary_band: Band::Uncertain,
            synthesized: false,
        }
    }

    /// A render item carrying a grounded summary (the flat text + a single text run), for the
    /// summary-bearing render paths.
    fn summarized(
        title: &str,
        headline: &str,
        summary: &str,
        link: Option<&str>,
        source: SourceKind,
    ) -> RenderItem {
        RenderItem {
            headline: headline.to_string(),
            summary: summary.to_string(),
            summary_runs: vec![TldrRun::Text {
                text: summary.to_string(),
            }],
            summary_band: Band::Confirmed,
            ..item(title, link, source)
        }
    }

    /// A context-eyebrow thread tag with the given label, band, and optional delta.
    fn tag(label: &str, confidence: ConfidenceBand, delta: Option<&str>) -> ThreadTag {
        ThreadTag {
            label: label.to_string(),
            confidence,
            delta: delta.map(str::to_string),
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
    fn four_zones_render_with_no_lorem_and_a_kept_debug_block() {
        // A summarized item carries an editor-grade headline + a grounded tldr; render them as real
        // content (no lorem placeholders), and compose the lead from the headline deterministically.
        let summarized = summarized(
            "RAW_TITLE",
            "EDITOR_HEADLINE",
            "GROUNDED_TLDR_SENTENCE",
            Some("https://example.com"),
            SourceKind::Rss,
        );
        let items = vec![summarized];
        let html = render_html(
            Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(),
            Tz::UTC,
            "GREETING_TEXT",
            &compose_lead(&items),
            &items,
            &DigestContent::default(),
        );

        // The greeting opens the lead, ahead of the deterministic big-picture summary (no lorem).
        assert!(html.contains("GREETING_TEXT"));
        let greeting_at = html.find("GREETING_TEXT").unwrap();
        let lead_at = html.find("Leading this digest: EDITOR_HEADLINE").unwrap();
        assert!(greeting_at < lead_at, "greeting must precede the lead");
        // Zone 2 headline (the editor-grade one, not the raw title) + zone 3 grounded summary, as real
        // content. The lorem category placeholder is gone entirely (retired by the eyebrow).
        assert!(html.contains("EDITOR_HEADLINE"));
        assert!(html.contains("GROUNDED_TLDR_SENTENCE"));
        assert!(!html.contains("RAW_TITLE"));
        assert!(!html.contains("Lorem"));
        assert_eq!(html.matches("<!-- PLACEHOLDER:").count(), 0);
        // Zone 4 provenance: a single-source story shows "<source> — <when>", not "Across".
        assert!(html.contains(">rss<"));
        assert!(!html.contains("Across"));
        // The "Why ·" caption and the "Related" block are off the email (they move to the debug box).
        assert!(!html.contains(">Why</span>"));
        assert!(!html.contains(">Related</div>"));

        // The amber DEBUG block is KEPT (not deleted) — styled unmistakably as scaffolding...
        assert!(html.contains("<!-- DEBUG: selection + summarization trace"));
        assert!(html.contains("<!-- /DEBUG -->"));
        assert!(html.contains(MONO));
        assert!(html.contains(DEBUG_BG));
        assert!(html.contains(&format!("border:1px dashed {DEBUG_BORDER}")));
        assert!(html.contains(">debug</span>"));
        // ...and it carries the enriched trace: selection rank/basis/link + the summary provenance.
        assert!(html.contains("selected #1"));
        assert!(html.contains("by priority"));
        assert!(html.contains("related 0"));
        assert!(html.contains("link https://example.com"));
        assert!(html.contains("summary <span style=\"font-weight:700;\">cluster summary</span>"));
        assert!(html.contains("band confirmed"));
    }

    #[test]
    fn note_renders_compact_without_debug() {
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
        // The Note tag + headline + provenance are present...
        assert!(html.contains(">Note</span>"));
        assert!(html.contains("A small ping"));
        assert!(html.contains(">github<"));
        // ...and a Note is deliberately terse: no placeholders and no amber debug block.
        assert_eq!(html.matches("<!-- PLACEHOLDER:").count(), 0);
        assert!(!html.contains("<!-- DEBUG:"));
    }

    #[test]
    fn context_eyebrow_carries_thread_and_delta_with_budgeted_doubt() {
        // A Confirmed thread with a delta: the label + the delta flag on one (nowrap) line, no "possibly".
        let confirmed = render_eyebrow(Some(&tag(
            "Acme auth migration",
            ConfidenceBand::Confirmed,
            Some("staging cutover landed"),
        )));
        assert!(confirmed.contains("Acme auth migration"));
        assert!(confirmed.contains("staging cutover landed"));
        assert!(confirmed.contains("white-space:nowrap")); // the one-line clamp (§6.1)
        assert!(!confirmed.contains("possibly"));

        // A Probable thread: a quiet italic "possibly" qualifies the label.
        let probable = render_eyebrow(Some(&tag(
            "Billing rewrite",
            ConfidenceBand::Probable,
            Some("reactivated"),
        )));
        assert!(probable.contains("possibly"));
        assert!(probable.contains("Billing rewrite"));

        // An Uncertain thread: the doubt isn't worth a line — the eyebrow is omitted entirely (§10.4).
        let uncertain = render_eyebrow(Some(&tag(
            "Shaky thread",
            ConfidenceBand::Uncertain,
            Some("moved"),
        )));
        assert!(uncertain.is_empty());

        // ...but the label still appears in the kept debug block (the audit detail isn't budgeted away).
        let item = RenderItem {
            thread: Some(tag(
                "Shaky thread",
                ConfidenceBand::Uncertain,
                Some("moved"),
            )),
            ..summarized("t", "H", "S", None, SourceKind::Github)
        };
        let html = render_one(&item);
        assert!(html.contains("Shaky thread"));
        assert!(html.contains("delta &ldquo;moved&rdquo;"));
    }

    #[test]
    fn summary_runs_render_inline_entity_badges() {
        let runs = vec![
            TldrRun::Text {
                text: "A bad config broke ".to_string(),
            },
            TldrRun::Ref {
                entity: "repo:acme/auth".to_string(),
                surface: "acme/auth".to_string(),
            },
            TldrRun::Text {
                text: "; flagged in ".to_string(),
            },
            TldrRun::Ref {
                entity: "cve:CVE-2026-2200".to_string(),
                surface: "CVE-2026-2200".to_string(),
            },
            TldrRun::Text {
                text: ".".to_string(),
            },
        ];
        let item = RenderItem {
            summary: "A bad config broke acme/auth; flagged in CVE-2026-2200.".to_string(),
            summary_runs: runs,
            summary_band: Band::Confirmed,
            ..item("t", None, SourceKind::Github)
        };
        let html = render_one(&item);
        // The repo ref renders as a dotted-underline tag; the CVE ref as a severity pill.
        assert!(html.contains(&format!("border-bottom:1px dotted {ACCENT}")));
        assert!(html.contains("acme/auth"));
        assert!(html.contains(BADGE_CVE_BG));
        assert!(html.contains("CVE-2026-2200"));
    }

    #[test]
    fn provenance_lists_distinct_sources_for_a_multi_source_story() {
        let item = RenderItem {
            connections: vec![
                Connection {
                    title: "PR".to_string(),
                    link: None,
                    source: SourceKind::Github,
                    link_reason: Some("same incident".to_string()),
                },
                Connection {
                    title: "chatter".to_string(),
                    link: None,
                    source: SourceKind::Rss,
                    link_reason: None,
                },
            ],
            ..summarized("t", "H", "S", None, SourceKind::Github)
        };
        let html = render_one(&item);
        // Multi-source provenance leads with "Across" and lists the distinct sources (github once).
        assert!(html.contains("Across"));
        assert_eq!(html.matches(">github<").count(), 1 + 1); // provenance + the debug source line
        assert!(html.contains(">rss<"));
        // The connection + its link_reason live in the debug block now, not on the email body.
        assert!(html.contains("same incident"));
    }

    /// Render a single story item to HTML (a one-item digest), for the per-zone assertions.
    fn render_one(item: &RenderItem) -> String {
        let items = std::slice::from_ref(item);
        render_html(
            Utc.with_ymd_and_hms(2026, 6, 13, 9, 0, 0).unwrap(),
            Tz::UTC,
            "Hi.",
            &compose_lead(items),
            items,
            &DigestContent::default(),
        )
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
        // The body-serif summary line only appears when the item carries a tldr run-list — none here.
        let summary_style = format!("font-size:17px;line-height:1.65;color:{INK_BODY}");
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
        assert!(plain
            .starts_with("Good morning. Here's your daily digest. Leading this digest: Hello."));
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
