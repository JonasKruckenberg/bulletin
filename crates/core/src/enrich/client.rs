//! The local-sidecar enrichment call. Reuses the summarization client's grammar-constrained
//! chat-completion plumbing ([`crate::summarize::client::chat_json`]) — same local `llama-server`,
//! same OpenAI-compatible JSON-schema request, same no-egress invariant — to extract the item's
//! real-world entities, then hands the result to the deterministic grounding gate.

use crate::common::event::Event;
use crate::enrich::{
    enrich_user_prompt, enrichment_schema, ground_entities, Enrichment, ENRICH_SYSTEM_PROMPT,
};
use crate::summarize::SummarizationConfig;

/// POST one grammar-constrained enrichment completion and parse the proposed entities. Errors bubble
/// up to [`enrich_event`], which degrades to "no enrichment this pass" (the event retries later).
pub async fn call_enrichment(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    title: &str,
    body: Option<&str>,
) -> anyhow::Result<Enrichment> {
    crate::summarize::client::chat_json(
        cfg,
        http,
        "enrich",
        ENRICH_SYSTEM_PROMPT,
        enrich_user_prompt(title, body),
        cfg.enrich_max_tokens,
        enrichment_schema(),
    )
    .await
}

/// Enrich one event, best-effort: call the model, then **ground** every proposed value against the
/// event's own title + grounding text, returning the grounded, canonicalized entity tokens to union
/// onto its entities **and** the classified salience score (the priority-boost importance, validated
/// to `0..=3` — never a model-emitted number; see [`crate::common::salience`]). `None` on any model
/// failure (transport / non-2xx / malformed) — enrichment is best-effort, so the caller leaves the
/// event un-marked to retry, and the build's grace deadline ages it in regardless. A successful call
/// that grounds *nothing* still returns `Some((vec![], score))`, so the event is marked enriched and
/// not retried forever.
///
/// Grounds against [`Event::best_text`] — the Phase-1 fetched `full_text` when present, else the
/// connector's `body` snippet — so the same accessor the summarizer reads also bounds enrichment: the
/// model sees the richer article when it exists (extracting more entities), and every proposed value
/// is validated against that same text. Phase 2 is stronger with Phase 1's article text but never
/// depends on it (a title-only event still extracts from its title). The salience class is a judgment,
/// not a grounded span, so it isn't gated by the text-match — it's re-validated to the closed vocab.
pub async fn enrich_event(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    event: &Event,
) -> Option<(Vec<String>, i16)> {
    let text = event.best_text();
    match call_enrichment(cfg, http, &event.title, text).await {
        Ok(extracted) => {
            let tokens = ground_entities(&event.title, text, &extracted);
            let salience = crate::common::salience::impact_score(&extracted.impact);
            Some((tokens, salience))
        }
        Err(e) => {
            tracing::debug!(
                error = %format!("{e:#}"),
                event_id = %event.id,
                "enrichment call failed; leaving event un-enriched for retry"
            );
            None
        }
    }
}
