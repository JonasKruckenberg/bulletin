//! Shared HTML-fragment → plain-text rendering, used by both the RSS body extractor
//! ([`super::rss`]) and the full-article fetcher ([`super::fetch`]).
//!
//! Both turn attacker/feed-supplied HTML into the flat, whitespace-normalized prose the summarizer's
//! source corpus and the entity/number miners consume — not displayed markup. Keeping the render in
//! one place means the two paths can't drift on how markup is stripped or how a body is bounded.
//!
//! Uses `html2text`'s `plain_no_decorate` to avoid the `[text][1]` reference-footnotes the decorated
//! renderer appends (which would re-introduce the very URLs we strip — links are carried structurally in
//! `event.links`). It is not enough on its own, though: `plain_no_decorate` still wraps an inline link's
//! text in `[...]` and glues that bracket onto adjacent words when the source markup lacks surrounding
//! whitespace, and it prefixes headings with `##`. [`render`] cleans both up before normalizing, so the
//! stored grounding text is flat prose with neither stray decorations nor mashed-together tokens.

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
    // `plain_no_decorate` still leaks two decorations that pollute the grounding text the model and the
    // entity/number miners read — and that this module's whole premise (links carried structurally in
    // `event.links`, never as prose) means we don't want:
    //   1. It wraps an inline link's visible text in `[...]` and, when the source HTML has no whitespace
    //      around the tag, glues the bracket straight onto the neighbouring words —
    //      `coast.<a>tagesschau.de</a>The` -> `coast.[tagesschau.de]The`. That single mashed token then
    //      both reads as garbage and trips the summarizer's bare-domain faithfulness gate downstream.
    //   2. It prefixes a heading with a markdown `##` marker.
    // Split on the link brackets as well as whitespace — de-gluing the boundary while keeping the link's
    // visible text as plain content — and drop standalone heading markers, in the same tokenization pass
    // that collapses whitespace into clean single-spaced prose (so no extra full-string allocation just to
    // swap the brackets out first). Empty pieces from adjacent delimiters are filtered with the markers.
    let normalized = rendered
        .split(|c: char| c.is_whitespace() || c == '[' || c == ']')
        .filter(|tok| !tok.is_empty() && !tok.bytes().all(|b| b == b'#'))
        .collect::<Vec<_>>()
        .join(" ");
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

    #[test]
    fn inline_link_text_is_unglued_not_mashed_into_a_token() {
        // html2text wraps the anchor text in `[...]` and, with no whitespace around the tag in the
        // source, glues it to the neighbours: `coast.<a>tagesschau.de</a>The` would render as the single
        // token `coast.[tagesschau.de]The`. We de-glue it into clean, space-separated prose.
        let html =
            r#"<p>off the coast.<a href="https://tagesschau.de">tagesschau.de</a>The story continues.</p>"#;
        let out = render(html, 16_000, 2000).unwrap();
        assert_eq!(out, "off the coast. tagesschau.de The story continues.");
        // No mashed token survives.
        assert!(!out.contains("deThe"));
        assert!(!out.contains('['));
        assert!(!out.contains(']'));
    }

    #[test]
    fn heading_markdown_marker_is_dropped() {
        let out = render("<h2>Poland.</h2><p>The aid arrived.</p>", 16_000, 2000).unwrap();
        assert_eq!(out, "Poland. The aid arrived.");
    }

    #[test]
    fn real_hash_tokens_survive_the_heading_strip() {
        // Only a *standalone* run of `#` (a heading marker) is dropped — issue refs and `C#` stay.
        let out = render("<p>fixed in #123 for C# users</p>", 16_000, 2000).unwrap();
        assert_eq!(out, "fixed in #123 for C# users");
    }
}
