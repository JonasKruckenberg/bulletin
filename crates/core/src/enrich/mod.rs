//! Phase 2 — **LLM entity/topic enrichment** (the early, grounded NER substrate).
//!
//! A best-effort sweep that runs *before* clustering: for each new item it asks a constrained local
//! LLM for the real-world entities the item is about — `place:`/`org:`/`person:`/`topic:` — validates
//! each against the source text (the grounding gate), and unions the surviving tokens onto the
//! event's `entities` set *before* the item becomes cluster-eligible. So three outlets covering the
//! same happening ("warning shots in the English Channel") — which today share only a per-publisher
//! `domain:` — come to share grounded tags (`place:english-channel`, `org:royal-navy`) and fuse into
//! one **story** on one thread, threaded around what the story is ABOUT rather than where it ran.
//!
//! **Never block, fall behind never wrong.** Enrichment is off the punctual send path entirely (it
//! runs in the background, ahead of the build). A failed/disabled/timed-out LLM call leaves the item
//! fully usable with the entities it already has (structural + derived): the cluster build's grace
//! deadline ages an un-enriched event into cluster-eligibility regardless, and it proceeds without the
//! grounded tags. Mirrors the comprehension/summarization plumbing ([`crate::summarize`]) — the same
//! constrained, grammar-shaped JSON over the same local sidecar.
//!
//! **Grounding is non-negotiable** (the LLM hallucination surface). This project never invents
//! entities; every extracted value MUST appear in the item's title/body (a normalized, whole-word
//! match) or it is dropped before it ever enters the entity set ([`ground_entities`]). The grammar
//! shapes the *shape* of the answer; the grounding gate shapes its *truth*.
//!
//! This module splits like [`crate::summarize`]:
//! - **The pure core (unit-tested without a sidecar):** the prompt/schema ([`ENRICH_SYSTEM_PROMPT`] /
//!   [`enrichment_schema`]), the extracted shape ([`Enrichment`]), and the deterministic grounding +
//!   canonicalization ([`ground_entities`]).
//! - **The model edge:** [`client`] (the local-sidecar call) and [`store`] / [`sweep_public`] (the DB
//!   sweep that walks the pending frontier and writes grounded tokens back onto events).

pub mod client;
pub mod store;

use serde::Deserialize;

use crate::common::db::{begin_scope, ScopeCtx};
use crate::summarize::SummarizationConfig;

/// The grounded entity namespaces this pass emits, paired with the `Enrichment` list each is drawn
/// from. `place:`/`org:`/`person:` are weak link keys; `topic:` is non-linking (see
/// [`crate::common::entity::link_strength`]). Iterated by [`ground_entities`].
const NAMESPACES: &[&str] = &["place", "org", "person", "topic"];

/// The constrained extraction output (§ mirrors [`crate::summarize::Comprehension`]): a short
/// free-text `analysis` scratchpad **first** (the "reason, then constrain" lever — named `analysis`
/// so llama.cpp's lexical property ordering generates it before the lists), then the four entity
/// lists. The model only *proposes* values here; nothing is trusted until [`ground_entities`] has
/// validated each against the source text.
///
/// Tolerant deserialize (every field defaulted): a missing/garbled field degrades to an empty list,
/// never an error — enrichment is best-effort.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Enrichment {
    /// The reasoning scratchpad. Named to sort *first* among the object's keys so it is generated
    /// before the lists (llama.cpp orders object properties lexically; `a…` guarantees scratchpad-first).
    #[serde(default)]
    pub analysis: String,
    /// Geographic places named in the text.
    #[serde(default)]
    pub places: Vec<String>,
    /// Organizations / companies / agencies / teams named in the text.
    #[serde(default)]
    pub orgs: Vec<String>,
    /// Specific named people in the text.
    #[serde(default)]
    pub people: Vec<String>,
    /// A few broad subject tags for what the item is about.
    #[serde(default)]
    pub topics: Vec<String>,
}

impl Enrichment {
    /// The proposed values for a namespace, for [`ground_entities`]'s uniform loop.
    fn list(&self, namespace: &str) -> &[String] {
        match namespace {
            "place" => &self.places,
            "org" => &self.orgs,
            "person" => &self.people,
            "topic" => &self.topics,
            _ => &[],
        }
    }
}

/// The grounding gate (non-negotiable, §). Turn a model [`Enrichment`] into the set of grounded,
/// canonicalized namespaced tokens to union onto the event's entities — **dropping every value that
/// does not actually appear in the source text**. For each proposed value:
/// 1. normalize it (lowercase, punctuation→spaces, drop a leading article) and require it to appear
///    as a whole-word run in the normalized title+body (so a hallucinated "NATO" the model added is
///    rejected, and "art" can't match inside "smart");
/// 2. canonicalize the surviving value to a slug token (`"the English Channel"` → `place:english-channel`)
///    through the shared identity machinery, so two outlets writing it two ways collide on one token.
///
/// Pure + deterministic (no model, no clock) — the returned tokens are sorted + de-duplicated.
pub fn ground_entities(title: &str, body: Option<&str>, e: &Enrichment) -> Vec<String> {
    // The source haystack, normalized once and space-padded so containment is whole-word: a needle
    // " royal navy " matches only on word boundaries, never inside another word.
    let mut source = normalize_text(title);
    if let Some(b) = body {
        if !b.is_empty() {
            source.push(' ');
            source.push_str(&normalize_text(b));
        }
    }
    let haystack = format!(" {source} ");

    let mut out: Vec<String> = Vec::new();
    for &namespace in NAMESPACES {
        for value in e.list(namespace) {
            if let Some(token) = grounded_token(namespace, value, &haystack) {
                out.push(token);
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Validate one proposed `value` against the space-padded normalized `haystack` and, if it is
/// grounded, return its canonical `kind:slug` token. `None` when the value is empty after
/// normalization or does not appear (as a whole-word run) in the source — the hallucination drop.
fn grounded_token(namespace: &str, value: &str, haystack: &str) -> Option<String> {
    let normalized = normalize_text(value);
    let needle = strip_article(&normalized);
    if needle.is_empty() {
        return None;
    }
    // Whole-word containment: the value's words must appear, in order, on word boundaries.
    if !haystack.contains(&format!(" {needle} ")) {
        return None;
    }
    let slug = needle.replace(' ', "-");
    if slug.is_empty() {
        return None;
    }
    // Reuse the identity canonicalizer (lower-cases the `kind:`, leaves the already-normalized value),
    // so the token form matches what the resolver/feedback path sees.
    Some(crate::identity::canonicalize(&format!(
        "{namespace}:{slug}"
    )))
}

/// Normalize free text for grounding: lower-case, map every run of non-alphanumeric characters to a
/// single space, and trim. Unicode-aware (`char::is_alphanumeric`), so accented names survive. Shared
/// by the haystack and the needle so they agree on token boundaries.
fn normalize_text(s: &str) -> String {
    let mut out = String::new();
    let mut prev_space = false;
    for c in s.chars() {
        if c.is_alphanumeric() {
            out.extend(c.to_lowercase());
            prev_space = false;
        } else if !prev_space && !out.is_empty() {
            out.push(' ');
            prev_space = true;
        }
    }
    // Trim a possible trailing space from the final non-alnum run.
    if out.ends_with(' ') {
        out.pop();
    }
    out
}

/// Drop a single leading article from a normalized value, so `"the English Channel"` and
/// `"English Channel"` slug to the same token. Leaves a bare article (`"the"`) alone.
fn strip_article(s: &str) -> &str {
    for article in ["the ", "an ", "a "] {
        if let Some(rest) = s.strip_prefix(article) {
            return rest;
        }
    }
    s
}

// ── The model edge: prompt + schema ──────────────────────────────────────────────────────────────

/// The enrichment system prompt — engineered for a 3–4B model exactly like the comprehension prompt
/// (short, imperative, one job, closed instructions, a worked example). A constant ⇒ prefix-cached.
/// The grounding rule is stated *and* enforced deterministically after (defense in depth).
pub const ENRICH_SYSTEM_PROMPT: &str = r#"You read one news or work item and tag the real-world entities it is about. Think first, then tag.

Fill these fields:
- analysis: 1-2 short sentences naming what the item is about.
- places: geographic places named in the text (countries, regions, cities, bodies of water).
- orgs: organizations, companies, agencies, or teams named in the text.
- people: specific named people in the text.
- topics: a few broad subject tags for what the item is about.

Rules:
- Use ONLY names that literally appear in the text. Never invent, expand, or infer an entity that is not written there. If unsure, leave it out.
- Copy each place/org/person from the text as written (you may drop a leading "the").
- Keep each list short - the few most central. Use an empty list if none apply.
- Output only the JSON the schema asks for. No preamble.

EXAMPLE
text: Royal Navy warships fired warning shots after a standoff in the English Channel on Tuesday, the Ministry of Defence said.
out: {"analysis":"A naval standoff in the English Channel involving the Royal Navy and the Ministry of Defence.","places":["English Channel"],"orgs":["Royal Navy","Ministry of Defence"],"people":[],"topics":["standoff"]}"#;

/// The per-item enrichment user prompt: the item's title (+ body when present) and the concrete ask.
/// Short and concrete, like the comprehension prompt.
pub fn enrich_user_prompt(title: &str, body: Option<&str>) -> String {
    let mut s = format!("title: {title}\n");
    if let Some(b) = body {
        let b = b.trim();
        if !b.is_empty() {
            s.push_str("body: ");
            s.push_str(b);
            s.push('\n');
        }
    }
    s.push_str(
        "\nTag the real-world entities this item is about: analysis first, then places, orgs, people, topics.",
    );
    s
}

/// The enrichment response schema for `response_format: json_schema` — llama.cpp's GBNF token-masking
/// turns this into a grammar guaranteeing structurally valid JSON. It bounds the *shape* (four string
/// arrays, capped length/count, plus the scratchpad); it cannot bound *truth* — that is the job of the
/// deterministic [`ground_entities`] gate. All five fields are required so the scratchpad is always
/// produced (and, being named `analysis`, produced first).
pub fn enrichment_schema() -> serde_json::Value {
    use serde_json::json;
    let value_list = json!({
        "type": "array",
        "maxItems": 8,
        "items": { "type": "string", "maxLength": 60 }
    });
    json!({
        "name": "enrichment",
        "strict": true,
        "schema": {
            "type": "object",
            "properties": {
                "analysis": { "type": "string", "maxLength": 400 },
                "places":   value_list,
                "orgs":     value_list,
                "people":   value_list,
                "topics":   value_list
            },
            "required": ["analysis", "places", "orgs", "people", "topics"],
            "additionalProperties": false
        }
    })
}

// ── The sweep (DB-bound orchestration) ────────────────────────────────────────────────────────────

/// What one enrichment pass did, for logs / metrics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct EnrichStats {
    /// Events the model answered for (and whose grounded tokens were written), counted whether or not
    /// any value survived grounding — the event is marked enriched either way, so it is not retried.
    pub enriched: usize,
    /// Events the model call failed for (down / timeout / malformed) — left un-marked to retry next
    /// pass (until the build's grace deadline ages them in regardless).
    pub failed: usize,
    /// Total grounded tokens unioned onto events this pass (richness signal).
    pub entities_added: usize,
}

/// The **public** enrichment sweep, hung off PublicBuild *before* the build (the pre-cluster point,
/// §): walk the frontier of public, not-yet-enriched, not-yet-built events; for each, call the
/// constrained model, ground the result, and union the surviving tokens onto the event's `entities`
/// (stamping `enriched_at`). Runs in the no-subscriber RLS context (public rows only), like the
/// public summarization sweep.
///
/// Best-effort throughout: a disabled sweep (`cfg.enrich == false`) is a no-op; a per-event model
/// failure is counted and the event left to retry; nothing here ever blocks or fails the build. Each
/// event's write is its own short transaction (committed right after its model call) so a mid-sweep
/// failure never rolls back earlier work and no transaction is held open across an LLM round-trip.
pub async fn sweep_public(
    pool: &sqlx::PgPool,
    cfg: &SummarizationConfig,
) -> anyhow::Result<EnrichStats> {
    if !cfg.enrich {
        return Ok(EnrichStats::default());
    }

    // Snapshot the pending frontier in one short read; the per-event writes follow individually.
    let events = {
        let mut tx = begin_scope(pool, ScopeCtx::NoSubscriber).await?;
        let evs = store::pending_public_events(&mut *tx, cfg.enrich_max_per_sweep).await?;
        tx.commit().await?;
        evs
    };
    if events.is_empty() {
        return Ok(EnrichStats::default());
    }

    // One client (and connection pool) for the whole pass, with the same generous per-call timeout the
    // summarizer uses — a slow call just defers that event, it never blocks the build.
    let http = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .build()?;

    let mut stats = EnrichStats::default();
    for ev in &events {
        match client::enrich_event(cfg, &http, ev).await {
            Some(tokens) => {
                let mut tx = begin_scope(pool, ScopeCtx::NoSubscriber).await?;
                store::apply_enrichment(&mut *tx, ev.id, &tokens).await?;
                tx.commit().await?;
                stats.enriched += 1;
                stats.entities_added += tokens.len();
            }
            None => stats.failed += 1,
        }
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enr(places: &[&str], orgs: &[&str], people: &[&str], topics: &[&str]) -> Enrichment {
        let v = |xs: &[&str]| xs.iter().map(|s| s.to_string()).collect();
        Enrichment {
            analysis: String::new(),
            places: v(places),
            orgs: v(orgs),
            people: v(people),
            topics: v(topics),
        }
    }

    #[test]
    fn extracts_grounded_place_and_org_as_namespaced_tokens() {
        // The motivating example: a place + org named in the text become weak link keys, so coverage
        // from another outlet naming the same English Channel / Royal Navy fuses onto one story.
        let title = "Royal Navy fired warning shots after a standoff in the English Channel";
        let e = enr(
            &["the English Channel"],
            &["Royal Navy"],
            &[],
            &["standoff"],
        );
        let tokens = ground_entities(title, None, &e);
        assert!(
            tokens.contains(&"place:english-channel".to_string()),
            "{tokens:?}"
        );
        assert!(tokens.contains(&"org:royal-navy".to_string()), "{tokens:?}");
        // The topic word "standoff" is in the title, so it grounds too — but as a non-linking tag.
        assert!(tokens.contains(&"topic:standoff".to_string()), "{tokens:?}");
        // Sorted + de-duplicated.
        let mut sorted = tokens.clone();
        sorted.sort();
        assert_eq!(tokens, sorted);
    }

    #[test]
    fn grounding_rejects_a_hallucinated_value() {
        // The model invents "NATO" and a "Vladimir Putin" that never appear in the text; both must be
        // dropped before they enter the entity set. The grounded "English Channel" survives.
        let title = "Royal Navy fired warning shots in the English Channel";
        let e = enr(
            &["English Channel", "Baltic Sea"],
            &["NATO"],
            &["Vladimir Putin"],
            &["maritime escalation"],
        );
        let tokens = ground_entities(title, None, &e);
        assert_eq!(
            tokens,
            vec!["place:english-channel".to_string()],
            "{tokens:?}"
        );
    }

    #[test]
    fn canonicalization_merges_the_definite_article_and_casing() {
        // "the English Channel" and "English Channel" must canonicalize to the SAME token, or the two
        // outlets' coverage won't fuse. Casing/punctuation differences collapse too.
        let title_a = "Tensions rise in the English Channel";
        let title_b = "English Channel sees naval standoff";
        let a = ground_entities(title_a, None, &enr(&["the English Channel"], &[], &[], &[]));
        let b = ground_entities(title_b, None, &enr(&["English Channel"], &[], &[], &[]));
        assert_eq!(a, vec!["place:english-channel".to_string()]);
        assert_eq!(a, b, "the two spellings must share one token");
    }

    #[test]
    fn whole_word_match_does_not_fire_inside_a_larger_word() {
        // A short value must not ground by appearing *inside* another word ("art" in "smart").
        let title = "A smart contract shipped today";
        let tokens = ground_entities(title, None, &enr(&[], &[], &[], &["art"]));
        assert!(tokens.is_empty(), "{tokens:?}");
    }

    #[test]
    fn grounds_against_the_body_too() {
        let title = "Quarterly results";
        let body = "Acme Corp announced layoffs across its Berlin office.";
        let e = enr(&["Berlin"], &["Acme Corp"], &[], &["layoffs"]);
        let tokens = ground_entities(title, Some(body), &e);
        assert!(tokens.contains(&"place:berlin".to_string()), "{tokens:?}");
        assert!(tokens.contains(&"org:acme-corp".to_string()), "{tokens:?}");
        assert!(tokens.contains(&"topic:layoffs".to_string()), "{tokens:?}");
    }

    #[test]
    fn empty_and_whitespace_values_are_dropped() {
        let title = "Anything at all";
        let tokens = ground_entities(title, None, &enr(&["", "   "], &[], &[], &[]));
        assert!(tokens.is_empty(), "{tokens:?}");
    }
}
