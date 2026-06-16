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
    apply_comprehension, baseline, clean_delta, clean_label, comprehend_user_prompt,
    comprehension_schema, delta_schema, delta_user_prompt, extract_facts, faithful, label_schema,
    label_user_prompt, response_schema, source_corpus, story_member_corpus, story_user_prompt,
    synthesize_facts, user_prompt, Band, ClusterSummary, Comprehension, Facts, SummarizationConfig,
    TldrRun, COMPREHEND_SYSTEM_PROMPT, DELTA_SYSTEM_PROMPT, LABEL_SYSTEM_PROMPT,
    STORY_SYSTEM_PROMPT, SYSTEM_PROMPT,
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

/// Synthesize a story's cross-source summary (Phase C, §2.2): fuse the member clusters' precomputed
/// summaries into one headline + tldr for the whole happening, then run the §3.4 faithfulness gate
/// over the fused facts. `members` are the story's member-cluster summaries, **newest-first** (so
/// `members[0]` is the representative and the freshest lifecycle wins in [`synthesize_facts`]).
///
/// Returns, mirroring [`summarize_cluster`]:
/// - `Some(synthesis)` on success, or — on a gate rejection — the **representative member's own
///   summary** (already grounded and gate-passed when it was generated), banded down to its band;
///   both are stable, content-derived results worth caching.
/// - `None` when the model itself was unavailable, so the caller leaves the story un-synthesized and a
///   later pass retries once the sidecar recovers (rather than caching a degraded synthesis).
pub async fn synthesize_story(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    members: &[ClusterSummary],
    thread_label: Option<&str>,
) -> Option<ClusterSummary> {
    // Nothing to fuse — let the caller fall back to the representative cluster (cold-start, §2.2).
    let representative = members.first()?;
    // A lone member is not a cross-source synthesis: the representative summary already *is* the
    // answer, so reuse it verbatim rather than spending a model call to paraphrase one input.
    if members.len() == 1 {
        return Some(representative.clone());
    }

    let facts = synthesize_facts(members);
    let corpus = story_member_corpus(members);

    let candidate = match chat_json::<ModelOutput>(
        cfg,
        http,
        STORY_SYSTEM_PROMPT,
        story_user_prompt(&facts, &corpus, thread_label),
        cfg.headline_max_tokens + cfg.tldr_max_tokens + 64,
        response_schema(&facts.entities),
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "story synthesis call failed; leaving story un-synthesized for retry");
            return None;
        }
    };

    let mut summary = ClusterSummary {
        headline: candidate.headline.trim().to_string(),
        tldr: candidate.tldr,
        tldr_text: String::new(),
        facts: facts.clone(),
        band: Band::Confirmed,
    };
    summary.rebuild_tldr_text();

    if cfg.faithfulness_gate {
        if let Err(v) = faithful(&summary, &facts, &corpus) {
            tracing::debug!(violation = ?v, "story synthesis gate rejected; falling back to representative cluster summary");
            return Some(representative.clone());
        }
    }
    Some(summary)
}

/// Generate a readable thread **label** (Phase B, §2.3): upgrade the deterministic auto-label to a
/// short prose name from the thread's entity spine + a few recent headlines. Best-effort — `None` on
/// any transport failure *or* a gate rejection (over-length / hype), so the caller keeps the
/// deterministic `thread.label`. The §3.4 gate here is the lighter [`clean_label`] (voice + length;
/// a label is a name, not a grounded claim).
pub async fn label_thread(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    entities: &[String],
    recent_headlines: &[String],
) -> Option<String> {
    let out = chat_json::<LabelOutput>(
        cfg,
        http,
        LABEL_SYSTEM_PROMPT,
        label_user_prompt(entities, recent_headlines),
        cfg.headline_max_tokens + 16,
        label_schema(),
    )
    .await;
    match out {
        Ok(o) => clean_label(&o.label),
        Err(e) => {
            tracing::debug!(error = %e, "thread label call failed; keeping deterministic auto-label");
            None
        }
    }
}

/// Generate the thread **delta** flag (Phase B, §5.2): compress the new stories' headlines since the
/// watermark into a ≤6-word "what changed" tag. Best-effort — `None` on any transport failure *or* a
/// gate rejection ([`clean_delta`]: voice + length + word-count + no end punctuation), so the caller
/// keeps the deterministic count delta.
pub async fn delta_thread(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    label: &str,
    new_headlines: &[String],
) -> Option<String> {
    let out = chat_json::<DeltaOutput>(
        cfg,
        http,
        DELTA_SYSTEM_PROMPT,
        delta_user_prompt(label, new_headlines),
        cfg.headline_max_tokens + 16,
        delta_schema(),
    )
    .await;
    match out {
        Ok(o) => clean_delta(&o.delta),
        Err(e) => {
            tracing::debug!(error = %e, "thread delta call failed; keeping deterministic count delta");
            None
        }
    }
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
    chat_json(
        cfg,
        http,
        COMPREHEND_SYSTEM_PROMPT,
        comprehend_user_prompt(facts, source),
        cfg.comprehension_max_tokens,
        comprehension_schema(),
    )
    .await
}

/// The source-kind labels a cluster's events came from, in event order (the baseline tldr sorts +
/// dedups them, so no need to here).
fn source_labels(events: &[Event]) -> Vec<&'static str> {
    events.iter().map(|e| e.source.as_str()).collect()
}

/// The slice of the model's response we parse — the abstractive fields. The rest is reconstructed
/// locally: `tldr_text` from the runs, `facts`/`band` from grounding + the gate. Shared by the cluster
/// summarizer and the Phase-C story synthesis (both emit headline + tldr run-list).
#[derive(Debug, Deserialize)]
struct ModelOutput {
    #[serde(default)]
    headline: String,
    #[serde(default)]
    tldr: Vec<TldrRun>,
}

/// The Phase-B thread-label response (just the readable name); cleaned by [`clean_label`].
#[derive(Debug, Deserialize)]
struct LabelOutput {
    #[serde(default)]
    label: String,
}

/// The Phase-B thread-delta response (just the flag); cleaned by [`clean_delta`].
#[derive(Debug, Deserialize)]
struct DeltaOutput {
    #[serde(default)]
    delta: String,
}

/// POST one grammar-constrained summarization completion and parse the structured output. Errors
/// bubble up to the caller, which degrades to baseline.
async fn call_model(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    facts: &Facts,
    source: &str,
) -> anyhow::Result<ModelOutput> {
    chat_json(
        cfg,
        http,
        SYSTEM_PROMPT,
        user_prompt(facts, source),
        cfg.headline_max_tokens + cfg.tldr_max_tokens + 64,
        response_schema(&facts.entities),
    )
    .await
}

/// The shared chat-completion plumbing every summarization call routes through (the summarizer, the
/// comprehension pass, and Phase C's story synthesis later): build the OpenAI-compatible body with the
/// `response_format: json_schema` (llama.cpp's GBNF token-masking → structurally valid JSON), POST to
/// the local sidecar, and deserialize `choices[0].message.content` into `T`. Errors (transport,
/// non-success status, malformed envelope/JSON) bubble up to the caller, which degrades to its
/// deterministic fallback.
async fn chat_json<T: serde::de::DeserializeOwned>(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    system: &str,
    user: String,
    max_tokens: u32,
    schema: serde_json::Value,
) -> anyhow::Result<T> {
    let body = serde_json::json!({
        "model": cfg.model,
        "messages": [
            { "role": "system", "content": system },
            { "role": "user", "content": user }
        ],
        "temperature": cfg.temperature,
        "seed": cfg.seed,
        "max_tokens": max_tokens,
        "response_format": { "type": "json_schema", "json_schema": schema }
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
