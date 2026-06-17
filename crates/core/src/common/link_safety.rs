//! Link-shaped-text safety: keep URL- and bare-domain-shaped tokens out of editorial prose so no
//! receiving mail client auto-linkifies them.
//!
//! The subtle thing this guards against — and the reason escaping alone is not enough. HTML escaping
//! (`html_escape`) and href allow-listing (`url::Url` + a scheme check) only govern the renderer's
//! *own* `<a href>` output. They do nothing about the receiving client running its own linkifier over
//! the displayed text: Apple Mail, Gmail and others turn a bare `acme.com` — or a pasted
//! `https://acme.com` — into a live link on *their* side, entirely outside our control. So a
//! hallucinated domain the model wrote into a summary becomes a clickable link the moment the digest
//! is opened, no matter how carefully we escaped it. The only defence that holds — especially in the
//! plaintext part, which has no markup to escape at all — is that the text itself never *looks* like a
//! URL or a domain.
//!
//! This module is the single source of truth for "does this token look clickable?", used two ways:
//!
//! - the write-side **gate** ([`first_linkable_token`]): a summary whose prose carries such a token is
//!   rejected and replaced by a clean deterministic baseline (`summarize::faithful`), so a clean true
//!   line ships instead of a hallucinated link;
//! - the render-side **backstop** ([`defang`]): every editorial string is passed through a last-line
//!   pass that rewrites each clickable-looking token's dots to `[.]`, so anything the gate could not
//!   catch — a baseline built from a feed title, the composed lead, a future field — still can't
//!   auto-link in either the HTML or the plaintext body.
//!
//! The detection is deliberately aggressive (the operator asked to "clamp down totally"): it flags
//! bare host shapes, not just explicit URLs. The cost is the occasional defanged `main.rs` in prose;
//! the benefit is a hard guarantee that no link surface — real, hallucinated, or future — reaches a
//! reader's auto-linkifier through editorial text.

use std::borrow::Cow;

/// Does `tok` — a single token, already trimmed of surrounding punctuation — look like something a
/// mail client would turn into a link?
///
/// Flags the three explicit URL forms (a `://` scheme, or a `www.`/`mailto:` prefix) and, deliberately
/// aggressively, any **bare host shape**: dot-separated labels whose final label is purely alphabetic
/// and ≥2 chars (`acme.com`, `github.com/x`, `ops@example.com`, `main.rs`). A numeric or single-letter
/// final is spared, so versions, ratios and abbreviations (`v2.0`, `9.5`, `3.14`, `e.g`, `U.S`) never
/// trip — the tokens a developer digest is actually full of.
pub fn is_linkable_token(tok: &str) -> bool {
    if tok.contains("://") {
        return true;
    }
    let has_prefix = |p: &str| {
        tok.len() >= p.len() && tok.as_bytes()[..p.len()].eq_ignore_ascii_case(p.as_bytes())
    };
    if has_prefix("www.") || has_prefix("mailto:") {
        return true;
    }
    looks_like_host(tok)
}

/// The bare-domain test behind [`is_linkable_token`]: take the authority (everything before the first
/// `/`, `?` or `#`), drop any `user@` userinfo and `:port` suffix, then require ≥2 dot-separated
/// labels of letters/digits/hyphen whose last label (the would-be TLD) is alphabetic and ≥2 chars.
fn looks_like_host(tok: &str) -> bool {
    let authority = tok.split(['/', '?', '#']).next().unwrap_or(tok);
    let after_userinfo = authority.rsplit('@').next().unwrap_or(authority);
    let host = after_userinfo.split(':').next().unwrap_or(after_userinfo);

    let mut count = 0usize;
    let mut last = "";
    for label in host.split('.') {
        // Every label must be a non-empty run of letters/digits/hyphen.
        if label.is_empty()
            || !label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return false;
        }
        count += 1;
        last = label;
    }
    // Need at least one dot, and a TLD-looking final label (alphabetic, ≥2 chars).
    count >= 2 && last.len() >= 2 && last.bytes().all(|b| b.is_ascii_alphabetic())
}

/// Trim a raw whitespace-delimited word down to its core token by stripping leading/trailing
/// non-alphanumerics — sentence punctuation and wrapping brackets — while leaving the internal `://`,
/// `@`, `.` and `/` that make a URL a URL. `(https://acme.com).` -> `https://acme.com`.
fn core_token(word: &str) -> &str {
    word.trim_matches(|c: char| !c.is_alphanumeric())
}

/// The first clickable-looking token in `text`, or `None` for clean prose — the deterministic gate the
/// summarizer uses to reject (and baseline-replace) a summary whose prose leaked a URL or bare domain.
pub fn first_linkable_token(text: &str) -> Option<String> {
    text.split_whitespace()
        .map(core_token)
        .find(|tok| !tok.is_empty() && is_linkable_token(tok))
        .map(str::to_string)
}

/// Neutralize a single flagged token so no linkifier — or our own [`is_linkable_token`] — still sees a
/// link in it: bracket every dot (`acme.com` -> `acme[.]com`, breaking host parsing) and break the two
/// colon markers a scheme keys on (`://` -> `[:]//`, a leading `mailto:` -> `mailto[:]`). The result is
/// inert in every mail client and reads as the familiar `[.]` security defang.
fn neutralize(core: &str) -> String {
    let mut s = core.replace('.', "[.]").replace("://", "[:]//");
    if s.len() >= 7 && s[..7].eq_ignore_ascii_case("mailto:") {
        s = format!("{}[:]{}", &s[..6], &s[7..]);
    }
    s
}

/// Rewrite every clickable-looking token so no mail client linkifies it (see [`neutralize`]), leaving
/// all other text — and the surrounding punctuation of a defanged token — untouched. `see acme.com
/// today` -> `see acme[.]com today`; `https://x.io/p` -> `https[:]//x[.]io/p`. Borrows unchanged when
/// the text holds nothing clickable, so clean prose (the overwhelming common case) costs no allocation.
pub fn defang(text: &str) -> Cow<'_, str> {
    let any = text
        .split_whitespace()
        .map(core_token)
        .any(|tok| !tok.is_empty() && is_linkable_token(tok));
    if !any {
        return Cow::Borrowed(text);
    }

    let mut out = String::with_capacity(text.len() + 8);
    let mut rest = text;
    while !rest.is_empty() {
        // Emit any leading whitespace verbatim, preserving the original spacing.
        let ws_end = rest
            .find(|c: char| !c.is_whitespace())
            .unwrap_or(rest.len());
        out.push_str(&rest[..ws_end]);
        rest = &rest[ws_end..];
        if rest.is_empty() {
            break;
        }
        // Take the next non-whitespace run (a "word") and defang only its trimmed core's dots.
        let word_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let word = &rest[..word_end];
        rest = &rest[word_end..];

        let lead_len = word.len()
            - word
                .trim_start_matches(|c: char| !c.is_alphanumeric())
                .len();
        let trail_len = word.len() - word.trim_end_matches(|c: char| !c.is_alphanumeric()).len();
        let core = &word[lead_len..word.len() - trail_len];
        if !core.is_empty() && is_linkable_token(core) {
            out.push_str(&word[..lead_len]);
            out.push_str(&neutralize(core));
            out.push_str(&word[word.len() - trail_len..]);
        } else {
            out.push_str(word);
        }
    }
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_explicit_urls_and_bare_domains() {
        for s in [
            "https://acme.com/x",
            "HTTPS://example.com",
            "www.example.org",
            "WWW.example.com",
            "mailto:ops@example.com",
            "acme.com",
            "github.com/acme/auth",
            "status.claude.com",
            "Booking.com",
            "ops@example.com", // a bare email autolinks too
            "main.rs",         // .rs is a real TLD — clamp-down-totally catches it
        ] {
            assert!(is_linkable_token(core_token(s)), "should flag: {s}");
        }
    }

    #[test]
    fn spares_versions_ratios_and_abbreviations() {
        for s in [
            "v2.0", "9.5", "3.14", "1.2.3", "e.g.", "i.e.", "U.S.", "etc.", "24/7", "Node",
        ] {
            assert!(!is_linkable_token(core_token(s)), "should spare: {s}");
        }
    }

    #[test]
    fn first_linkable_token_finds_the_token() {
        assert_eq!(
            first_linkable_token("published on acme.com today").as_deref(),
            Some("acme.com")
        );
        assert_eq!(
            first_linkable_token("(see https://x.io/p).").as_deref(),
            Some("https://x.io/p")
        );
        assert_eq!(first_linkable_token("a calm true line about auth"), None);
    }

    #[test]
    fn defang_neutralizes_links_and_preserves_the_rest() {
        assert_eq!(defang("see acme.com today"), "see acme[.]com today");
        assert_eq!(defang("(see acme.com)."), "(see acme[.]com).");
        assert_eq!(
            defang("ping https://x.io/p now"),
            "ping https[:]//x[.]io/p now"
        );
        assert_eq!(defang("reach www.example.org"), "reach www[.]example[.]org");
        assert_eq!(
            defang("mail mailto:ops@example.com"),
            "mail mailto[:]ops@example[.]com"
        );
        // Multiple tokens, original spacing preserved.
        assert_eq!(defang("  a.com  and b.org "), "  a[.]com  and b[.]org ");
    }

    #[test]
    fn defang_borrows_clean_prose_unchanged() {
        let s = "Auth logins broke after the token-rotation deploy";
        assert!(matches!(defang(s), Cow::Borrowed(_)));
        // Versions/ratios are not links, so prose carrying them is left alone too.
        assert!(matches!(defang("rated 9.5/10 in v2.0"), Cow::Borrowed(_)));
    }

    #[test]
    fn defanged_output_holds_no_linkable_token() {
        // The backstop's contract: whatever goes in, nothing clickable-looking comes out.
        for s in [
            "see acme.com and https://x.io/p or www.q.net",
            "the Booking.com outage hit main.rs",
            "mailto:ops@example.com",
        ] {
            assert_eq!(first_linkable_token(&defang(s)), None, "leaked from: {s}");
        }
    }
}
