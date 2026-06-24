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

/// Render a *full fetched page* to the article's plain text — the full-article fetcher's entry point
/// ([`super::fetch`]), distinct from the generic [`render`] the RSS path uses on a small `<description>`
/// fragment. A whole-page [`render`] is the wrong tool for a modern news page: the article lives in a
/// `<script type="application/ld+json">` block html2text deliberately skips and/or in `<article>` markup
/// that sits *past* [`render`]'s input-size truncation, so a blind strip yields only the nav/footer
/// chrome (the observed `Hauptnavigation Nebennavigation Inhalt Fußzeile` boilerplate). This isolates
/// the main content first, in falling order of reliability:
///
///   1. **schema.org JSON-LD `articleBody`** — the clean, already-plain article text most news sites
///      embed for search engines. Immune to both the chrome and the truncation, so it is tried first.
///   2. **the `<article>` subtree**, rendered in isolation — drops surrounding nav/header/footer, and
///      (handed to [`render`] on its own) is no longer beyond the truncation horizon.
///   3. **the whole page** ([`render`]) — the legacy last resort for a page exposing neither signal.
///
/// `None` only when all three yield nothing usable (a genuinely content-less page). `max_html_chars` /
/// `max_chars` bound the parse/output exactly as in [`render`].
pub(crate) fn render_article(
    html: &str,
    max_html_chars: usize,
    max_chars: usize,
) -> Option<String> {
    if let Some(body) = jsonld_article_body(html) {
        // Route the (already-plain) JSON-LD body through `render` too, so it gets the same HTML-entity
        // decode (`&amp;` → `&`), whitespace collapse, and word-boundary cap as every other path — one
        // normalizer, no drift.
        if let Some(text) = render(&body, max_html_chars, max_chars) {
            return Some(text);
        }
    }
    if let Some(article) = first_article_subtree(html) {
        if let Some(text) = render(article, max_html_chars, max_chars) {
            return Some(text);
        }
    }
    render(html, max_html_chars, max_chars)
}

/// Extract the article text from a page's schema.org JSON-LD: scan each
/// `<script type="application/ld+json">` block, parse it, and return the first non-empty `articleBody`
/// found (walking nested objects / arrays / `@graph`, since the article node is often wrapped). `None`
/// when the page carries no JSON-LD or none with an `articleBody`.
///
/// The scan is a byte-safe, ASCII-case-insensitive walk over the raw HTML ([`find_ascii_ci`]) — no DOM
/// parser and no new dependency — which is sound because every offset it slices on (`<script`, the
/// tag-closing `>`, `</script>`) is ASCII, so it lands on a valid char boundary even amid multibyte
/// UTF-8 article text.
fn jsonld_article_body(html: &str) -> Option<String> {
    let mut pos = 0;
    while let Some(rel) = find_ascii_ci(&html[pos..], "<script") {
        let tag_open = pos + rel;
        // The opening tag ends at the first `>`; its content runs to the next `</script>`.
        let Some(gt_rel) = html[tag_open..].find('>') else {
            break;
        };
        let content_start = tag_open + gt_rel + 1;
        let open_tag = &html[tag_open..content_start];
        if find_ascii_ci(open_tag, "application/ld+json").is_some() {
            if let Some(close_rel) = find_ascii_ci(&html[content_start..], "</script>") {
                let json = &html[content_start..content_start + close_rel];
                if let Some(body) = parse_article_body(json) {
                    return Some(body);
                }
            }
        }
        // Advance past this opening tag (a boundary) and keep scanning for the next script block.
        pos = content_start;
    }
    None
}

/// Parse one JSON-LD script block and pull a non-empty `articleBody` out of it, if present.
fn parse_article_body(json: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json.trim()).ok()?;
    find_article_body(&value)
}

/// Recursively search a JSON-LD value for a non-empty `articleBody` string — the node may be a bare
/// object, an array of nodes (multiple `@type`s on one page), or wrapped in an `@graph`, so any
/// `articleBody` anywhere in the tree wins (the first found, depth-first).
fn find_article_body(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(body)) = map.get("articleBody") {
                let trimmed = body.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
            map.values().find_map(find_article_body)
        }
        serde_json::Value::Array(arr) => arr.iter().find_map(find_article_body),
        _ => None,
    }
}

/// The `<article>…</article>` subtree of `html` (the first `<article` open tag to the next
/// `</article>` close), or `None` when the page has no `<article>` element. A coarse but effective main-
/// content isolation for the common semantic-HTML page: it drops the nav/header/footer that a whole-page
/// strip would fold in. Byte-safe ASCII matching ([`find_ascii_ci`]), like [`jsonld_article_body`].
fn first_article_subtree(html: &str) -> Option<&str> {
    let open = find_ascii_ci(html, "<article")?;
    let close_rel = find_ascii_ci(&html[open..], "</article>")?;
    Some(&html[open..open + close_rel + "</article>".len()])
}

/// First byte offset of ASCII `needle` in `haystack`, case-insensitively, or `None`. ASCII-only
/// matching is byte-offset-safe even within multibyte UTF-8: every continuation/lead byte is `>= 0x80`
/// and so never equals an ASCII byte, so a match can only start (and end) on a char boundary.
fn find_ascii_ci(haystack: &str, needle: &str) -> Option<usize> {
    let hay = haystack.as_bytes();
    let nee = needle.as_bytes();
    if nee.is_empty() || hay.len() < nee.len() {
        return None;
    }
    (0..=hay.len() - nee.len()).find(|&i| hay[i..i + nee.len()].eq_ignore_ascii_case(nee))
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
        let html = r#"<p>off the coast.<a href="https://tagesschau.de">tagesschau.de</a>The story continues.</p>"#;
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

    #[test]
    fn jsonld_article_body_is_extracted_and_entity_decoded() {
        let html = r#"<html><head>
            <script type="application/ld+json">
            {"@context":"https://schema.org","@type":"NewsArticle",
             "headline":"x","articleBody":"The boom did not happen. Hotels &amp; bars stayed empty."}
            </script></head><body><p>nav</p></body></html>"#;
        let out = render_article(html, 200_000, 8000).unwrap();
        assert_eq!(out, "The boom did not happen. Hotels & bars stayed empty.");
    }

    #[test]
    fn jsonld_article_body_found_inside_a_graph_array() {
        // The article node is commonly wrapped in an `@graph` alongside breadcrumb/org nodes — the
        // recursive search must still find its `articleBody`.
        let html = r#"<script type="application/ld+json">
            {"@context":"https://schema.org","@graph":[
              {"@type":"BreadcrumbList","itemListElement":[]},
              {"@type":"Article","articleBody":"Real article text here."}
            ]}</script>"#;
        assert_eq!(
            render_article(html, 200_000, 8000).unwrap(),
            "Real article text here."
        );
    }

    #[test]
    fn regression_news_page_returns_article_not_nav_chrome() {
        // The shape that produced "FIFA announces a fan boom in the USA": a script/chrome-heavy page
        // whose only *visible* text near the top is the accessibility skip-links, with the real article
        // exposed via JSON-LD. A whole-page strip yields the nav boilerplate; `render_article` must pull
        // the JSON-LD body instead.
        let html = r##"<html><head>
            <script>var darkmode=1;</script>
            <script type="application/ld+json">
            {"@type":"NewsArticle","headline":"Fan-Boom bleibt aus",
             "articleBody":"Vor dem Turnier hatte die FIFA einen Boom versprochen. Doch die Rechnung geht bislang nicht auf."}
            </script></head>
            <body>
              <a href="#nav">Zur Hauptnavigation springen</a>
              <a href="#sub">Zur Nebennavigation</a>
              <a href="#content">Zum Inhalt</a>
              <a href="#footer">Zur Fußzeile</a>
            </body></html>"##;
        let out = render_article(html, 200_000, 8000).unwrap();
        assert!(
            out.starts_with("Vor dem Turnier hatte die FIFA einen Boom versprochen."),
            "expected the article body, got: {out}"
        );
        // The crucial negation that the title-only summary dropped is present in the grounding now.
        assert!(out.contains("Doch die Rechnung geht bislang nicht auf."));
        // And the nav skip-links are not what we returned.
        assert!(!out.contains("Hauptnavigation"), "nav chrome leaked: {out}");
    }

    #[test]
    fn article_subtree_is_used_when_no_jsonld() {
        // No JSON-LD: isolate the <article> subtree so the surrounding nav/footer is dropped.
        let html = r##"<html><body>
            <nav><a href="/">Home</a> Navigation menu</nav>
            <article><h1>Title.</h1><p>The body of the story.</p></article>
            <footer>Footer links here</footer>
            </body></html>"##;
        let out = render_article(html, 200_000, 8000).unwrap();
        assert_eq!(out, "Title. The body of the story.");
        assert!(!out.contains("Navigation"));
        assert!(!out.contains("Footer"));
    }

    #[test]
    fn falls_back_to_whole_page_without_jsonld_or_article() {
        // Neither signal present → legacy whole-page strip (unchanged behavior).
        let html = "<html><body><p>Just a plain page.</p></body></html>";
        assert_eq!(
            render_article(html, 200_000, 8000).unwrap(),
            "Just a plain page."
        );
    }

    #[test]
    fn find_ascii_ci_is_case_insensitive_and_utf8_safe() {
        // Case-insensitive match, and a byte offset that lands correctly past multibyte UTF-8.
        let s = "Fußball <SCRIPT TYPE=\"x\">";
        assert_eq!(find_ascii_ci(s, "<script"), s.find('<'));
        assert_eq!(find_ascii_ci("abc", "zzz"), None);
        assert_eq!(find_ascii_ci("", "x"), None);
    }
}
