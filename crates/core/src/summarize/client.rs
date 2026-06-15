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
    baseline, extract_facts, faithful, response_schema, source_corpus, user_prompt, Band,
    ClusterSummary, Facts, SummarizationConfig, TldrRun, SYSTEM_PROMPT,
};

/// Summarize one cluster: extract-then-summarize (§3.2) — hand the model the pre-extracted facts +
/// budgeted source text and ask it to *rewrite* them, then run the §3.4 faithfulness gate. **Always
/// returns a usable summary**: on any model/transport error, or a gate rejection, it degrades to the
/// deterministic [`baseline`] banded `uncertain`. Never fails — the digest's punctuality does not
/// depend on the model.
pub async fn summarize_cluster(
    cfg: &SummarizationConfig,
    title: &str,
    events: &[Event],
) -> ClusterSummary {
    let facts = extract_facts(events);
    let source = source_corpus(events, cfg.max_source_chars);
    let sources = distinct_sources(events);
    let fallback = || baseline(title, events.len() as i32, &sources, facts.clone());

    let candidate = match call_model(cfg, &facts, &source).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "summarization model call failed; using baseline");
            return fallback();
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
            return fallback();
        }
    }
    summary
}

/// The distinct source-kind labels a cluster's events came from (the labels the baseline tldr names).
fn distinct_sources(events: &[Event]) -> Vec<&'static str> {
    let mut s: Vec<&'static str> = events.iter().map(|e| e.source.as_str()).collect();
    s.sort_unstable();
    s.dedup();
    s
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

    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .build()?;
    let resp = client
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
