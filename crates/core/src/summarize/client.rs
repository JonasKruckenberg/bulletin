//! The local-sidecar summarization client (gated). Drives a `llama-server` (llama.cpp) over its
//! OpenAI-compatible `/chat/completions` with grammar-constrained JSON (`docs/local-ml-options.md`
//! §4–§5). 100% local ⇒ the §12 no-egress invariant holds: no content leaves the box.
//!
//! Implemented directly over the already-present `reqwest` rather than pulling in the heavier
//! `async-openai` crate — the request is a single JSON POST, this keeps the dependency surface flat
//! and offline-buildable, and it gives us exact control of the `response_format`/grammar payload.
//! (Handoff: swap to `async-openai` if richer client features — streaming, tool-calls — are wanted.)

use serde::Deserialize;

use crate::common::event::Event;
use crate::summarize::{
    apply_comprehension, baseline, comprehend_user_prompt, comprehension_schema, extract_facts,
    faithful, response_schema, source_corpus, user_prompt, Band, ClusterSummary, Comprehension,
    Facts, SummarizationConfig, TldrRun, COMPREHEND_SYSTEM_PROMPT, SYSTEM_PROMPT,
};

/// Summarize one cluster: extract-then-summarize (§3.2) — hand the model the pre-extracted facts +
/// budgeted source text and ask it to *rewrite* them, then run the §3.4 faithfulness gate.
///
/// Returns:
/// - `Some(summary)` on success **and** on a gate rejection (a deterministic, content-derived
///   [`baseline`] banded `uncertain`) — both are stable results worth caching;
/// - `None` when the model itself was unavailable (transport/HTTP error) — so the caller leaves the
///   cluster unsummarized and a later sweep retries once the sidecar recovers, rather than freezing it
///   at a baseline. Never panics — the digest's punctuality does not depend on the model.
///
/// `http` is the sweep's shared client (one connection pool for the whole pass).
pub async fn summarize_cluster(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    title: &str,
    events: &[Event],
) -> Option<ClusterSummary> {
    let mut facts = extract_facts(events);
    let source = source_corpus(events, cfg.max_source_chars);

    // Extract-then-summarize (§3.2): run the comprehension pass first so the summarizer's hedge rule
    // (§3.6) is a mechanical branch on `facts.certainty`/`state`, not an inference. Best-effort — a
    // failed/disabled comprehension leaves the neutral defaults (asserted, plain), the safe direction.
    if cfg.comprehend {
        if let Some(comp) = comprehend_cluster(cfg, http, &facts, &source).await {
            apply_comprehension(&mut facts, &comp);
        }
    }

    let candidate = match call_model(cfg, http, &facts, &source).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "summarization model call failed; leaving cluster unsummarized for retry");
            return None;
        }
    };

    // Stitch the grounding facts back on and derive the flat text from the structured runs.
    let mut summary = ClusterSummary {
        headline: candidate.headline.trim().to_string(),
        tldr: candidate.tldr,
        tldr_text: String::new(),
        facts: facts.clone(),
        band: Band::Confirmed,
    };
    summary.rebuild_tldr_text();

    if cfg.faithfulness_gate {
        if let Err(v) = faithful(&summary, &facts, &source) {
            tracing::debug!(violation = ?v, "faithfulness gate rejected summary; using baseline");
            return Some(baseline(
                title,
                events.len() as i32,
                &source_labels(events),
                facts,
            ));
        }
    }
    Some(summary)
}

/// Run the comprehension pass for one cluster (§3.2, `local-ml-options.md` §6): a constrained chat
/// completion that classifies `event_type` / `state` / `certainty` over the deterministically-extracted
/// grounding + source text. Reasoning is free (the `analysis` scratchpad), only the classification is
/// grammar-constrained (CRANE — avoid the reasoning "grammar tax").
///
/// Returns `None` on any failure (transport, non-2xx, malformed JSON) — comprehension is itself
/// best-effort, so the caller proceeds with the neutral facts rather than blocking the summary.
async fn comprehend_cluster(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    facts: &Facts,
    source: &str,
) -> Option<Comprehension> {
    match call_comprehension(cfg, http, facts, source).await {
        Ok(c) => Some(c),
        Err(e) => {
            tracing::debug!(error = %e, "comprehension call failed; summarizing with neutral facts");
            None
        }
    }
}

/// POST one grammar-constrained comprehension completion and parse the classified output. Errors
/// bubble up to [`comprehend_cluster`], which degrades to neutral facts.
async fn call_comprehension(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    facts: &Facts,
    source: &str,
) -> anyhow::Result<Comprehension> {
    use serde_json::json;

    let body = json!({
        "model": cfg.model,
        "messages": [
            { "role": "system", "content": COMPREHEND_SYSTEM_PROMPT },
            { "role": "user", "content": comprehend_user_prompt(facts, source) }
        ],
        "temperature": cfg.temperature,
        "seed": cfg.seed,
        "max_tokens": cfg.comprehension_max_tokens,
        "response_format": { "type": "json_schema", "json_schema": comprehension_schema() }
    });

    let resp = http
        .post(format!("{}/chat/completions", cfg.base_url))
        .header("content-type", "application/json")
        .body(serde_json::to_string(&body)?)
        .send()
        .await?;

    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("sidecar returned {status}: {text}");
    }

    let envelope: ChatResponse = serde_json::from_str(&text)?;
    let content = envelope
        .choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .ok_or_else(|| anyhow::anyhow!("sidecar response had no choices"))?;
    Ok(serde_json::from_str(&content)?)
}

/// The source-kind labels a cluster's events came from, in event order (the baseline tldr sorts +
/// dedups them, so no need to here).
fn source_labels(events: &[Event]) -> Vec<&'static str> {
    events.iter().map(|e| e.source.as_str()).collect()
}

/// The slice of the model's response we parse — the abstractive fields. The rest is reconstructed
/// locally: `tldr_text` from the runs, `facts`/`band` from grounding + the gate.
#[derive(Debug, Deserialize)]
struct ModelOutput {
    #[serde(default)]
    headline: String,
    #[serde(default)]
    tldr: Vec<TldrRun>,
}

/// POST one grammar-constrained chat completion to the local sidecar and parse the structured output.
/// Errors (transport, non-success status, malformed envelope/JSON) bubble up to the caller, which
/// degrades to baseline.
async fn call_model(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    facts: &Facts,
    source: &str,
) -> anyhow::Result<ModelOutput> {
    use serde_json::json;

    let body = json!({
        "model": cfg.model,
        "messages": [
            { "role": "system", "content": SYSTEM_PROMPT },
            { "role": "user", "content": user_prompt(facts, source) }
        ],
        "temperature": cfg.temperature,
        "seed": cfg.seed,
        "max_tokens": cfg.headline_max_tokens + cfg.tldr_max_tokens + 64,
        // llama.cpp turns this JSON schema into a GBNF grammar → structurally valid JSON guaranteed.
        "response_format": { "type": "json_schema", "json_schema": response_schema(&facts.entities) }
    });

    let resp = http
        .post(format!("{}/chat/completions", cfg.base_url))
        .header("content-type", "application/json")
        .body(serde_json::to_string(&body)?)
        .send()
        .await?;

    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("sidecar returned {status}: {text}");
    }

    // OpenAI-compatible envelope: choices[0].message.content is the JSON string the schema shaped.
    let envelope: ChatResponse = serde_json::from_str(&text)?;
    let content = envelope
        .choices
        .into_iter()
        .next()
        .map(|c| c.message.content)
        .ok_or_else(|| anyhow::anyhow!("sidecar response had no choices"))?;
    Ok(serde_json::from_str(&content)?)
}

#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    #[serde(default)]
    content: String,
}
