//! Shared HTML-fragment → plain-text rendering, used by both the RSS body extractor
//! ([`super::rss`]) and the full-article fetcher ([`super::fetch`]).
//!
//! Both turn attacker/feed-supplied HTML into the flat, whitespace-normalized prose the summarizer's
//! source corpus and the entity/number miners consume — not displayed markup. Keeping the render in
//! one place means the two paths can't drift on how markup is stripped or how a body is bounded.
//!
//! Uses `html2text`'s `plain_no_decorate` deliberately: it drops link/emphasis markup entirely rather
//! than emitting `[text][1]` footnotes, so the rendered prose never re-introduces the URLs the
//! summarizer's faithfulness gate would only have to strip back out (links are carried structurally in
//! `event.links`).

use std::borrow::Cow;

/// Wrap width handed to html2text. Immaterial to the result — the rendered line breaks are collapsed
/// straight back out below — but it must clear the renderer's internal minimum; a roomy value avoids
/// needless mid-text wraps.
const RENDER_WIDTH: usize = 200;

/// Render an HTML fragment to the plain text we store and summarize: strip the markup with
/// `html2text`, collapse all whitespace to single spaces, and cap the length on a word boundary.
/// `None` for empty/blank input (or markup that renders to nothing), so a content-less item keeps no
/// body.
///
/// `max_html_chars` bounds the *input* markup actually parsed (html2text tolerates a truncated
/// fragment): rendering work stays proportional to what we keep, not to the source size — a
/// multi-megabyte page is never parsed in full only to discard all but the leading `max_chars` of its
/// text. `max_chars` bounds the *output* text stored.
pub(crate) fn render(html: &str, max_html_chars: usize, max_chars: usize) -> Option<String> {
    if html.trim().is_empty() {
        return None;
    }
    // Bound the work before rendering: only allocate a truncated copy when the markup actually exceeds
    // the cap (byte length ≥ char count, so a shorter byte length needs no truncation).
    let bounded: Cow<str> = if html.len() > max_html_chars {
        Cow::Owned(html.chars().take(max_html_chars).collect())
    } else {
        Cow::Borrowed(html)
    };
    let rendered = html2text::config::plain_no_decorate()
        .string_from_read(bounded.as_bytes(), RENDER_WIDTH)
        .ok()?;
    // Collapse all runs of whitespace (incl. the wrapper's line breaks) to single spaces — the body is
    // grounding for the model and the entity/number miners, not displayed, so flat prose is cleanest.
    let normalized = rendered.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return None;
    }
    Some(truncate_on_word_boundary(normalized, max_chars))
}

/// Cap `s` to `max_chars`, cutting on a whitespace boundary so the stored body never ends in a split
/// token — a half-truncated number would otherwise be mined as a (bogus) grounded quantity. Falls back
/// to a hard char cut only when the truncated prefix holds no space at all (one pathological long
/// token). `s` is already whitespace-normalized to single spaces, so the split is on `' '`.
pub(crate) fn truncate_on_word_boundary(s: String, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s;
    }
    let truncated: String = s.chars().take(max_chars).collect();
    match truncated.rsplit_once(' ') {
        Some((head, _)) if !head.is_empty() => head.to_string(),
        _ => truncated,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_markup_and_normalizes_whitespace() {
        let out = render("<p>Hello   <b>world</b></p>\n<p>second</p>", 16_000, 2000).unwrap();
        assert_eq!(out, "Hello world second");
    }

    #[test]
    fn empty_or_blank_renders_to_none() {
        assert_eq!(render("", 16_000, 2000), None);
        assert_eq!(render("   \n  ", 16_000, 2000), None);
        assert_eq!(render("<p></p>", 16_000, 2000), None);
    }

    #[test]
    fn caps_output_on_word_boundary() {
        let html = "<p>alpha beta gamma delta epsilon</p>";
        let out = render(html, 16_000, 12).unwrap();
        // Cut at <=12 chars on a space, never mid-word.
        assert!(out.chars().count() <= 12);
        assert!(!out.ends_with(' '));
        assert_eq!(out, "alpha beta");
    }

    #[test]
    fn truncate_falls_back_to_hard_cut_without_spaces() {
        let s = "a".repeat(50);
        assert_eq!(truncate_on_word_boundary(s, 10).chars().count(), 10);
    }
}
