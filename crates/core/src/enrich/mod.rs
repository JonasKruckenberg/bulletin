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
//!   slugging gate ([`ground_entities`]).
//! - **The model edge:** [`client`] (the local-sidecar call) and [`store`] / [`sweep_public`] (the DB
//!   sweep that walks the pending frontier and writes grounded tokens back onto events).

pub mod client;
pub mod store;

use serde::Deserialize;

use crate::common::db::{with_scope, ScopeCtx};
use crate::summarize::{build_summarize_http, SummarizationConfig};

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
    /// How big a deal this item is — a closed [`salience`](crate::common::salience::IMPACT_VOCAB)
    /// class the model *classifies* (it never emits a number), turned into the priority-boosting
    /// importance score by [`crate::common::salience::impact_score`]. Re-validated there, so a
    /// missing/out-of-vocab value floors to `routine` (no boost).
    #[serde(default)]
    pub impact: String,
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
    /// Each grounded namespace paired with the proposed values it draws from — the single structural
    /// source of the name↔field mapping, so [`ground_entities`] iterates one place and a new namespace
    /// can't be added to one half without the other. `place:`/`org:`/`person:` are weak link keys;
    /// `topic:` is non-linking (see [`crate::common::entity::link_strength`]).
    fn namespaced(&self) -> [(&'static str, &[String]); 4] {
        [
            ("place", &self.places),
            ("org", &self.orgs),
            ("person", &self.people),
            ("topic", &self.topics),
        ]
    }
}

/// The grounding gate (non-negotiable, §). Turn a model [`Enrichment`] into the set of grounded,
/// canonicalized namespaced tokens to union onto the event's entities — **dropping every value that
/// does not actually appear in the source text**. For each proposed value:
/// 1. normalize it (lowercase, punctuation→spaces, drop a leading "the") and require it to appear
///    as a whole-word run in the normalized title *or* body (so a hallucinated "NATO" the model added
///    is rejected, and "art" can't match inside "smart");
/// 2. slug the surviving value to a `kind:value` token (`"the English Channel"` → `place:english-channel`),
///    canonical by construction, so two outlets writing it two ways collide on one token.
///
/// Pure + deterministic (no model, no clock) — the returned tokens are sorted + de-duplicated.
pub fn ground_entities(title: &str, body: Option<&str>, e: &Enrichment) -> Vec<String> {
    // Title and body are kept as *separate* padded haystacks (each normalized + space-padded so a
    // needle " royal navy " matches only on whole-word boundaries). Checking them independently — not
    // a single title‖body concatenation — is deliberate: a value must appear contiguously within one
    // field, so a phrase straddling the title-end/body-start boundary ("…Royal" + "Navy…") can't
    // falsely ground. The grounding gate is non-negotiable, so it must not invent boundary matches.
    let mut haystacks: Vec<String> = vec![format!(" {} ", normalize_text(title))];
    if let Some(b) = body {
        if !b.is_empty() {
            haystacks.push(format!(" {} ", normalize_text(b)));
        }
    }

    let mut out: Vec<String> = Vec::new();
    for (namespace, values) in e.namespaced() {
        for value in values {
            if let Some(token) = grounded_token(namespace, value, &haystacks) {
                out.push(token);
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Validate one proposed `value` against the space-padded normalized `haystacks` (title, body) and, if
/// it appears as a whole-word run in *any one* of them, return its `kind:slug` token. `None` when the
/// value is empty after normalization or appears in none of the haystacks — the hallucination drop.
fn grounded_token(namespace: &str, value: &str, haystacks: &[String]) -> Option<String> {
    let normalized = normalize_text(value);
    let needle = strip_article(&normalized);
    if needle.is_empty() {
        return None;
    }
    // Whole-word containment within a single field: the value's words must appear, in order, on word
    // boundaries somewhere in one haystack.
    let padded = format!(" {needle} ");
    if !haystacks.iter().any(|h| h.contains(&padded)) {
        return None;
    }
    // `namespace` is a lowercase literal and `needle` is already normalized (lowercased, alnum-only),
    // so the token is canonical by construction — no `identity::canonicalize` pass is needed (it would
    // be an identity transform here), and `needle` non-empty ⇒ the slug is non-empty.
    Some(format!("{namespace}:{}", needle.replace(' ', "-")))
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

/// Drop a leading **definite** article from a normalized value, so `"the English Channel"` and
/// `"English Channel"` slug to the same token. Only `"the "` is stripped — the indefinite articles
/// `"a "`/`"an "` are left in place, since (unlike "the") they are rarely a throwaway prefix on a
/// proper name and stripping them mangles legitimately article-leading names ("A Tribe Called Quest").
/// Leaves a bare `"the"` alone.
fn strip_article(s: &str) -> &str {
    s.strip_prefix("the ").unwrap_or(s)
}

// ── The model edge: prompt + schema ──────────────────────────────────────────────────────────────

/// The enrichment system prompt — engineered for a 3–4B model exactly like the comprehension prompt
/// (short, imperative, one job, closed instructions, a worked example). A constant ⇒ prefix-cached.
/// The grounding rule is stated *and* enforced deterministically after (defense in depth).
pub const ENRICH_SYSTEM_PROMPT: &str = r#"You read one news or work item and tag the real-world entities it is about, then judge how big a deal it is. Think first, then tag.

Fill these fields:
- analysis: 1-2 short sentences naming what the item is about.
- impact: how significant the item is, one of:
    - "major": a breaking, large-scale, or grave development — war or its escalation, mass casualties, a disaster, a critical security advisory, a major release or ruling.
    - "significant": a serious development — a major policy change, a notable incident, an important market or organizational move.
    - "notable": worth knowing but not dominating — a debate, a routine announcement or release, a local incident.
    - "routine": minor or everyday — a market wrap, a soft-news or culture note, chatter, a small update.
  Judge the EVENT itself, not how dramatic the wording is. When unsure, choose the lower level.
- places: geographic places named in the text (countries, regions, cities, bodies of water).
- orgs: organizations, companies, agencies, or teams named in the text.
- people: specific named people in the text.
- topics: a few broad subject tags for what the item is about.

Rules:
- Use ONLY names that literally appear in the text. Never invent, expand, or infer an entity that is not written there. If unsure, leave it out.
- Tag only entities that are part of the STORY. Ignore photo credits, image captions, bylines, and the publishing outlet itself: a photo agency (picture alliance, dpa, Reuters, Getty, imago), a photographer's or reporter's name in a credit line, and the source's own brand are NOT what the item is about. Leave them out.
- Tag the most SPECIFIC entity, not a broad container. Prefer a city, agency, or company over a whole country, and a specific person over a generic group.
- Copy each place/org/person from the text as written (you may drop a leading "the").
- Keep each list short - the few most central. Use an empty list if none apply.
- Output only the JSON the schema asks for. No preamble.

EXAMPLE
text: Royal Navy warships fired warning shots after a standoff in the English Channel on Tuesday, the Ministry of Defence said. (Photo: Jane Doe / picture alliance)
out: {"analysis":"A naval standoff in the English Channel involving the Royal Navy and the Ministry of Defence.","impact":"significant","places":["English Channel"],"orgs":["Royal Navy","Ministry of Defence"],"people":[],"topics":["standoff"]}"#;

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
        "\nTag the real-world entities this item is about: analysis first, then impact, places, orgs, people, topics.",
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
                // Mirrors `salience::IMPACT_VOCAB` (asserted by `schema_impact_enum_matches_vocab`).
                "impact":   { "type": "string", "enum": ["routine", "notable", "significant", "major"] },
                "places":   value_list,
                "orgs":     value_list,
                "people":   value_list,
                "topics":   value_list
            },
            "required": ["analysis", "impact", "places", "orgs", "people", "topics"],
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
    use anyhow::Context;

    if !cfg.enrich {
        return Ok(EnrichStats::default());
    }

    // Snapshot the pending frontier in one short scoped transaction; the per-event writes follow
    // individually (the same `with_scope`-per-DB-step shape the summarization sweep uses, so the model
    // calls are never held inside a transaction).
    let limit = cfg.enrich_max_per_sweep;
    let events = with_scope(pool, ScopeCtx::NoSubscriber, move |conn| {
        Box::pin(async move {
            store::pending_public_events(conn, limit)
                .await
                .context("load pending events for enrichment")
        })
    })
    .await?;
    if events.is_empty() {
        return Ok(EnrichStats::default());
    }

    // One client (and connection pool) for the whole pass, via the shared constructor so client setup
    // (and its error context) lives in one place with the summarizer's.
    let http = build_summarize_http(cfg, "enrich")?;

    let mut stats = EnrichStats::default();
    for ev in &events {
        match client::enrich_event(cfg, &http, ev).await {
            Some((tokens, salience)) => {
                let event_id = ev.id;
                let tokens_for_write = tokens.clone();
                with_scope(pool, ScopeCtx::NoSubscriber, move |conn| {
                    Box::pin(async move {
                        store::apply_enrichment(conn, event_id, &tokens_for_write, salience)
                            .await
                            .context("apply enrichment to event")
                    })
                })
                .await?;
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
            impact: String::new(),
            places: v(places),
            orgs: v(orgs),
            people: v(people),
            topics: v(topics),
        }
    }

    #[test]
    fn schema_impact_enum_matches_vocab() {
        // The schema's inline `impact` enum must stay in lockstep with the scoring vocabulary, or the
        // model could emit a class the scorer floors to routine.
        let schema = enrichment_schema();
        let enum_vals: Vec<String> = schema["schema"]["properties"]["impact"]["enum"]
            .as_array()
            .expect("impact enum present")
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(enum_vals, crate::common::salience::IMPACT_VOCAB);
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

    #[test]
    fn value_straddling_the_title_body_boundary_does_not_falsely_ground() {
        // "Royal" ends the title and "Navy" begins the body; "Royal Navy" appears contiguously in
        // NEITHER field, so grounding it against the title‖body seam would be a hallucination. The
        // separate-haystack check must reject it.
        let title = "Honours for the Royal";
        let body = "Navy budget cuts announced today.";
        let tokens = ground_entities(title, Some(body), &enr(&[], &["Royal Navy"], &[], &[]));
        assert!(
            !tokens.contains(&"org:royal-navy".to_string()),
            "boundary-straddling phrase must not ground: {tokens:?}"
        );
        // A value wholly inside one field still grounds (here, "Navy" alone is in the body).
        let ok = ground_entities(title, Some(body), &enr(&[], &["Navy"], &[], &[]));
        assert!(ok.contains(&"org:navy".to_string()), "{ok:?}");
    }

    #[test]
    fn only_the_definite_article_is_stripped() {
        // "the" is dropped so "the English Channel" == "English Channel" (the fusion case), but an
        // indefinite article that is part of a real name is preserved rather than mangled.
        let title = "the English Channel and the band A Tribe Called Quest";
        let tokens = ground_entities(
            title,
            None,
            &enr(
                &["the English Channel"],
                &["A Tribe Called Quest"],
                &[],
                &[],
            ),
        );
        assert!(
            tokens.contains(&"place:english-channel".to_string()),
            "{tokens:?}"
        );
        // "A " is kept, so the slug matches the real name rather than "tribe-called-quest".
        assert!(
            tokens.contains(&"org:a-tribe-called-quest".to_string()),
            "{tokens:?}"
        );
    }
}
