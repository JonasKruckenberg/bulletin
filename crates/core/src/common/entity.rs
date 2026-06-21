//! Entity extraction — the **blocking substrate** cross-source linking runs on (design §8.2).
//!
//! An entity is a namespaced token (`kind:value`) that two events might share: a CVE id, a URL, a
//! repo, a user. The namespace prefix is load-bearing — it both keeps unrelated values from
//! colliding (`user:rust` ≠ `repo:rust`) and classifies an entity as **strong** or **weak** for
//! linking ([`link_strength`]): a shared *strong* key (a CVE or an exact URL) is a near-certain
//! connection that may merge anything; a shared *weak* key (a domain, repo, person, place, or org)
//! only links when corroborated by other signals (design §8.2's asymmetric-merge guard against
//! single-linkage blobs).
//!
//! Three layers feed an event's entities:
//! - **structural**, per source — the connector knows a GitHub event is `repo:`/`user:` (kept where
//!   the source vocabulary lives, `ingest/github`).
//! - **derived**, shared — [`derive`] mines CVE ids and URLs/domains out of any event's title, body,
//!   and links, so every source gets the cross-source keys "for free" (design §8.2: "URLs and native
//!   ids carry the load"). Run once at the seal point ([`super::event::EventBuilder::finalize`]).
//! - **enriched**, shared — the Phase-2 best-effort LLM sweep ([`crate::enrich`]) mines grounded
//!   `place:`/`org:`/`person:`/`topic:` tokens out of the title/body *before* the item is clustered,
//!   so coverage of the same happening across publishers fuses on what it is ABOUT, not just a
//!   per-feed `domain:`. Added asynchronously, never at the seal point (it must never block ingest).
//!
//! Hand-rolled (no `regex` dep) to match the project's lean-deps ethos; the patterns are narrow
//! (CVE ids, `http(s)://` URLs) and unit-tested below.

/// How strongly two clusters sharing this entity should link (design §8.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkStrength {
    /// A shared `cve:`/`url:` — a near-certain connection that may merge anything (incl. two
    /// already-delivered stories, the retro-merge).
    Strong,
    /// A shared distinctive named entity (`repo:`/`user:`/`place:`/`org:`/`person:`) — links only when
    /// corroborated, and never collapses two established stories (the asymmetric-merge guard).
    Weak,
}

/// How an entity participates in linking, or `None` if it is too coarse to link on.
///
/// `domain:` and `topic:` are deliberately **non-linking**:
/// - `domain:` — every item from one feed shares its domain, so a shared domain is noise as an edge.
/// - `topic:` — a broad subject tag (the Phase-2 enrichment, [`crate::enrich`]). It must never fuse
///   stories on its own, or "everything about AI" collapses into one blob. Classified like `domain:`
///   here, but *unlike* `domain:` it stays on the thread spine (Phase 0 drops only `domain:`/`url:`),
///   so `topic:` shapes threads and affinity without ever forming a link.
///
/// Both still ride on the cluster for blocking/affinity (M4); they just never form a link by themselves.
///
/// The Phase-2 grounded named entities — `place:`/`org:`/`person:` — are **weak**, like `repo:`/`user:`:
/// they corroborate (two outlets naming the same place/org in the same window fuse), but a single shared
/// one can never collapse two already-delivered stories.
pub fn link_strength(entity: &str) -> Option<LinkStrength> {
    if entity.starts_with("cve:") || entity.starts_with("url:") {
        Some(LinkStrength::Strong)
    } else if let Some(login) = entity.strip_prefix("user:") {
        // A *bot* actor (`renovate[bot]`, `dependabot[bot]`, `github-actions[bot]`) touches a great many
        // unrelated repos, so a shared bot is noise as a link key — it fused every repo Renovate runs on
        // into one blob. Only a human actor corroborates a connection; a bot is non-linking (it still
        // rides on the cluster for display, it just never forms an edge by itself).
        if is_bot_login(login) {
            None
        } else {
            Some(LinkStrength::Weak)
        }
    } else if entity.starts_with("repo:")
        || entity.starts_with("place:")
        || entity.starts_with("org:")
        || entity.starts_with("person:")
    {
        Some(LinkStrength::Weak)
    } else {
        None
    }
}

/// Whether a `user:` login is an automation account rather than a person — GitHub's convention is a
/// trailing `[bot]` suffix (`renovate[bot]`, `dependabot[bot]`, `github-actions[bot]`). Matched on the
/// bracketed suffix so a human login that merely contains "bot" (`robot`, `botev`) is not caught.
fn is_bot_login(login: &str) -> bool {
    login.ends_with("[bot]")
}

/// Derive the shared, cross-source entities (CVE ids + URLs/domains) from an event's text and links.
/// Source-structural entities (`repo:`/`user:`) are added by the connector; this is the part every
/// source shares, so a GitHub PR and an RSS advisory that name the same CVE/URL block together.
/// Returns sorted, de-duplicated tokens (the cluster rollup unions these, so a stable order keeps the
/// stored array deterministic).
pub fn derive(title: &str, body: Option<&str>, links: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();

    // Structured links first — these are the cleanest URL signal (no parsing ambiguity).
    for link in links {
        push_url(&mut out, link);
    }
    // Then mine the free text: CVE ids and any inline URLs.
    for text in std::iter::once(title).chain(body.iter().copied()) {
        push_cves(&mut out, text);
        for token in text.split_whitespace() {
            push_url(&mut out, token.trim_end_matches(is_url_trailer));
        }
    }

    out.sort();
    out.dedup();
    out
}

/// Append the `url:` + `domain:` entities for one candidate URL string, if it parses as `http(s)://`.
/// A bare word that isn't a URL is silently ignored (the common case for free-text tokens).
fn push_url(out: &mut Vec<String>, candidate: &str) {
    let Some((url, domain)) = normalize_url(candidate) else {
        return;
    };
    out.push(format!("url:{url}"));
    out.push(format!("domain:{domain}"));
}

/// Normalize an `http(s)://` URL to `(canonical_url, domain)`:
/// scheme + host lowercased, any `#fragment` dropped, a trailing `/` trimmed — so the same resource
/// linked two slightly different ways still collides. `domain` drops a leading `www.`. Returns `None`
/// for anything that isn't an absolute `http(s)` URL with a host.
fn normalize_url(raw: &str) -> Option<(String, String)> {
    let (scheme, rest) = raw.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return None;
    }
    // Host runs up to the first '/', '?' or '#'; the path/query is everything before a '#'.
    let host_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let host = rest[..host_end].to_ascii_lowercase();
    if host.is_empty() {
        return None;
    }
    let after_host = &rest[host_end..];
    let path = after_host.split('#').next().unwrap_or("");
    let url = format!("{scheme}://{host}{path}");
    let url = url.trim_end_matches('/').to_string();

    let domain = host.strip_prefix("www.").unwrap_or(&host).to_string();
    Some((url, domain))
}

/// Trailing characters to strip off a free-text URL token — sentence punctuation and the brackets
/// that commonly wrap a link, none of which are part of the URL.
fn is_url_trailer(c: char) -> bool {
    matches!(
        c,
        '.' | ',' | ';' | ':' | ')' | ']' | '}' | '"' | '\'' | '>' | '!' | '?'
    )
}

/// Append `cve:CVE-YYYY-NNNN…` (canonical uppercase) for every CVE id in `text`. Matches the
/// official format — `CVE`, year (4+ digits), sequence (4+ digits) — case-insensitively, so a feed
/// that writes `cve-2026-1234` still links to a GitHub PR that writes `CVE-2026-1234`.
fn push_cves(out: &mut Vec<String>, text: &str) {
    let bytes = text.as_bytes();
    let lower = text.to_ascii_lowercase();
    let mut search_from = 0;
    while let Some(rel) = lower[search_from..].find("cve-") {
        let start = search_from + rel;
        // Word boundary before "cve" so "abccve-…" doesn't match.
        let preceded_ok = start == 0 || !bytes[start - 1].is_ascii_alphanumeric();
        if let (true, Some(end)) = (preceded_ok, cve_tail(&bytes[start + 4..])) {
            let id = text[start..start + 4 + end].to_ascii_uppercase();
            out.push(format!("cve:{id}"));
            search_from = start + 4 + end;
        } else {
            search_from = start + 4;
        }
    }
}

/// Given the bytes right after `CVE-`, return the length of a valid `YYYY-NNNN…` tail (year ≥4
/// digits, dash, sequence ≥4 digits), or `None`. The end is exclusive of any trailing non-digit.
fn cve_tail(rest: &[u8]) -> Option<usize> {
    let year = rest.iter().take_while(|b| b.is_ascii_digit()).count();
    if year < 4 || rest.get(year) != Some(&b'-') {
        return None;
    }
    let seq_start = year + 1;
    let seq = rest[seq_start..]
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .count();
    if seq < 4 {
        return None;
    }
    Some(seq_start + seq)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strong_vs_weak_classification() {
        use LinkStrength::*;
        assert_eq!(link_strength("cve:CVE-2026-1234"), Some(Strong));
        assert_eq!(link_strength("url:https://example.com/a"), Some(Strong));
        assert_eq!(link_strength("repo:acme/widget"), Some(Weak));
        assert_eq!(link_strength("user:alice"), Some(Weak));
        assert_eq!(link_strength("domain:example.com"), None);
    }

    #[test]
    fn bot_actors_are_not_link_keys() {
        use LinkStrength::*;
        // A human actor corroborates a link; a bot does not (it acts across unrelated repos).
        assert_eq!(link_strength("user:alice"), Some(Weak));
        assert_eq!(link_strength("user:renovate[bot]"), None);
        assert_eq!(link_strength("user:dependabot[bot]"), None);
        assert_eq!(link_strength("user:github-actions[bot]"), None);
        // A human login that merely contains "bot" is still a link key — only the `[bot]` suffix counts.
        assert_eq!(link_strength("user:robot"), Some(Weak));
        assert_eq!(link_strength("user:botev"), Some(Weak));
    }

    #[test]
    fn enriched_namespace_strengths() {
        use LinkStrength::*;
        // The Phase-2 grounded named entities corroborate but never collapse delivered stories.
        assert_eq!(link_strength("place:english-channel"), Some(Weak));
        assert_eq!(link_strength("org:royal-navy"), Some(Weak));
        assert_eq!(link_strength("person:jane-smith"), Some(Weak));
        // `topic:` is non-linking like `domain:` — it must never fuse stories on its own ("everything
        // about AI" must not become one blob). It still rides on the spine; it just forms no edge.
        assert_eq!(link_strength("topic:maritime-security"), None);
    }

    #[test]
    fn derives_url_and_domain_from_links() {
        let ents = derive(
            "title",
            None,
            &["https://www.Example.com/Path/".to_string()],
        );
        // Host lowercased, www. dropped for the domain, trailing slash trimmed on the url.
        assert!(ents.contains(&"url:https://www.example.com/Path".to_string()));
        assert!(ents.contains(&"domain:example.com".to_string()));
    }

    #[test]
    fn same_resource_two_ways_collides() {
        let a = derive("", None, &["https://example.com/x".to_string()]);
        let b = derive("", None, &["https://example.com/x#section".to_string()]);
        let url = "url:https://example.com/x".to_string();
        assert!(a.contains(&url) && b.contains(&url));
    }

    #[test]
    fn extracts_cve_case_insensitively() {
        let a = derive("Fixes cve-2026-1234 in parser", None, &[]);
        let b = derive("Advisory: CVE-2026-1234", Some("details"), &[]);
        let cve = "cve:CVE-2026-1234".to_string();
        assert!(a.contains(&cve), "{a:?}");
        assert!(b.contains(&cve), "{b:?}");
    }

    #[test]
    fn cve_requires_well_formed_id_and_boundary() {
        // Too-short sequence, embedded in a word, or missing parts → not an entity.
        assert!(derive("CVE-2026-12", None, &[]).is_empty());
        assert!(derive("xCVE-2026-1234", None, &[]).is_empty());
        assert!(derive("CVE-26", None, &[]).is_empty());
    }

    #[test]
    fn extracts_url_from_free_text_and_strips_trailing_punctuation() {
        let ents = derive("see https://example.com/a, also other", None, &[]);
        assert!(
            ents.contains(&"url:https://example.com/a".to_string()),
            "{ents:?}"
        );
        // The comma is not part of the URL.
        assert!(!ents.iter().any(|e| e.contains("a,")));
    }

    #[test]
    fn non_http_and_garbage_ignored() {
        let ents = derive(
            "ftp://nope just words here",
            None,
            &["mailto:x@y.z".to_string()],
        );
        assert!(ents.is_empty(), "{ents:?}");
    }

    #[test]
    fn output_is_sorted_and_deduped() {
        let ents = derive(
            "CVE-2026-1234 and CVE-2026-1234 again",
            None,
            &["https://a.com".to_string(), "https://a.com".to_string()],
        );
        let mut sorted = ents.clone();
        sorted.sort();
        assert_eq!(ents, sorted);
        let cve_count = ents.iter().filter(|e| e.starts_with("cve:")).count();
        assert_eq!(cve_count, 1);
    }
}
