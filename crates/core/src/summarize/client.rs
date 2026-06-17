//! The local-sidecar summarization client (gated). Drives a `llama-server` (llama.cpp) over its
//! OpenAI-compatible `/chat/completions` with grammar-constrained JSON (`docs/local-ml-options.md`
//! §4–§5). 100% local ⇒ the §12 no-egress invariant holds: no content leaves the box.
//!
//! Implemented directly over the already-present `reqwest` rather than pulling in the heavier
//! `async-openai` crate — the request is a single JSON POST, this keeps the dependency surface flat
//! and offline-buildable, and it gives us exact control of the `response_format`/grammar payload.
//! (Handoff: swap to `async-openai` if richer client features — streaming, tool-calls — are wanted.)

use std::time::{Duration, Instant};

use anyhow::Context;
use serde::Deserialize;

use super::metric;
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
            tracing::warn!(
                error = %format!("{e:#}"),
                kind = failure_kind(&e),
                base_url = %cfg.base_url,
                model = %cfg.model,
                "summarization model call failed; leaving cluster unsummarized for retry"
            );
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
            metric::gate_rejection("summarize", &v);
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
        "synthesize",
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
            metric::gate_rejection("synthesize", &v);
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
        "label",
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
        "delta",
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

/// Startup reachability gate for the local summarization sidecar (the fail-loud counterpart to the
/// per-call best-effort degradation). A `llm-summarization` build exists *to* summarize against the
/// sidecar; if it can't be reached when the worker boots, that is a deployment/config error — a wrong
/// `BULLETIN_LLM_BASE_URL`, a sidecar that never came up, or a model that failed to load — and we want
/// it surfaced **loudly** (a failed unit → deploy rollback) rather than silently shipping deterministic
/// baselines forever. This is intentionally distinct from the *running* contract: once past this gate,
/// a transient sidecar blip mid-sweep still degrades best-effort and retries (a digest is never blocked).
///
/// Probes the OpenAI-compatible `{base_url}/models` endpoint, retrying with capped exponential backoff
/// until `deadline` elapses, so a sidecar still mapping its GGUF at boot (llama-cpp is ordered before us
/// but only answers once the model is loaded) is given time rather than mistaken for an absent one.
/// Returns `Err` only after the whole window passes with no successful response.
pub async fn ensure_reachable(cfg: &SummarizationConfig, deadline: Duration) -> anyhow::Result<()> {
    // Short per-attempt timeout so one hung connect can't swallow the whole window in a single try.
    let attempt_timeout = Duration::from_secs(5).min(deadline);
    let http = reqwest::Client::builder()
        .timeout(attempt_timeout)
        .build()
        .context("build sidecar readiness http client")?;
    let url = format!("{}/models", cfg.base_url);

    let start = Instant::now();
    let mut backoff = Duration::from_secs(1);
    loop {
        let last_err = match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => format!("sidecar returned HTTP {}", resp.status()),
            Err(e) => format!("{e}"),
        };
        let remaining = deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            anyhow::bail!(
                "summarization sidecar at {} not reachable after {}s: {last_err}",
                cfg.base_url,
                deadline.as_secs(),
            );
        }
        // Never sleep past the deadline, then grow the backoff (capped) for the next try.
        tokio::time::sleep(backoff.min(remaining)).await;
        backoff = (backoff * 2).min(Duration::from_secs(5));
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
            tracing::debug!(
                error = %format!("{e:#}"),
                kind = failure_kind(&e),
                "comprehension call failed; summarizing with neutral facts"
            );
            None
        }
    }
}

/// Classify a failed sidecar call into a coarse, *static* reason for the logs, so an operator can tell
/// the sidecar being **down** (`connect`) from **slow** (`timeout`) from **erroring** (`status`) from a
/// **malformed response** (`response`) at a glance — without grepping the wrapped error text. The full
/// chain is still logged via `error`; this is just the field you filter/alert on. Best-effort: anything
/// we can't place lands in `transport`/`other`.
fn failure_kind(err: &anyhow::Error) -> &'static str {
    match err.downcast_ref::<reqwest::Error>() {
        Some(e) if e.is_timeout() => "timeout",
        Some(e) if e.is_connect() => "connect",
        Some(e) if e.is_status() => "status",
        Some(e) if e.is_decode() => "decode",
        Some(_) => "transport",
        // The explicit non-2xx `bail!` and the malformed-envelope / bad-JSON parse errors are plain
        // `anyhow` errors (not `reqwest`), so they land here: the sidecar answered, but unusably.
        None => "response",
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
        "comprehend",
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
        "summarize",
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
    phase: &'static str,
    system: &str,
    user: String,
    max_tokens: u32,
    schema: serde_json::Value,
) -> anyhow::Result<T> {
    // Time and tally every call at the single choke point all five phases route through. The latency
    // histogram is keyed on `phase` + `outcome`, so the percentiles can be read clean (filter
    // `outcome="ok"`) without losing the failure latencies — a fast `connect` failure and a 120s
    // `timeout` are recorded too, under their own outcome.
    let started = std::time::Instant::now();
    let result = chat_json_inner(cfg, http, phase, system, user, max_tokens, schema).await;
    let outcome = match &result {
        Ok(_) => "ok",
        Err(e) => failure_kind(e),
    };
    metric::llm_call(phase, outcome, started.elapsed());
    result
}

async fn chat_json_inner<T: serde::de::DeserializeOwned>(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    phase: &'static str,
    system: &str,
    user: String,
    max_tokens: u32,
    schema: serde_json::Value,
) -> anyhow::Result<T> {
    let mut body = serde_json::json!({
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
    // Turn off the model's native chain-of-thought for these constrained calls. A reasoning model
    // (e.g. Qwen3) left in "thinking" mode spends the small `max_tokens` budget on a `<think>` block
    // and then returns an **empty** `content` (which serde reports as the unhelpful "EOF while parsing
    // a value at line 1 column 0") — or blows the request timeout producing it. The grammar already
    // shapes the answer to JSON, so the reasoning buys nothing here. Honoured by a llama.cpp sidecar
    // run with `--jinja`; harmlessly ignored by servers/models that don't template on it.
    if cfg.disable_thinking {
        body["chat_template_kwargs"] = serde_json::json!({ "enable_thinking": false });
    }

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
    // A 2xx with an empty body is not parseable JSON — name it as exactly that, rather than letting
    // serde report a bare "EOF while parsing a value at line 1 column 0" the operator can't place.
    if text.trim().is_empty() {
        anyhow::bail!("sidecar returned {status} with an empty body");
    }

    // OpenAI-compatible envelope: choices[0].message.content is the JSON string the schema shaped.
    let envelope: ChatResponse = serde_json::from_str(&text)
        .with_context(|| format!("parse sidecar response envelope ({} bytes)", text.len()))?;
    // Token accounting from the `usage` block (present on the 2xx path), recorded before the content is
    // even validated: the tokens were spent regardless of whether the body parses or the gate later
    // rejects it.
    if let Some(usage) = envelope.usage {
        metric::llm_tokens(phase, usage.prompt_tokens, usage.completion_tokens);
    }
    let choice = envelope
        .choices
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("sidecar response had no choices"))?;

    // Drop a leading `<think>…</think>` block a reasoning model may still inline into `content` (the
    // llama.cpp `--reasoning-format none` shape), then require a non-empty remainder. When a reasoning
    // model is left thinking, llama.cpp's default `--reasoning-format auto`/`deepseek` instead routes
    // the thoughts to `message.reasoning_content` and leaves `content` empty — the exact source of the
    // downstream serde "EOF while parsing a value at line 1 column 0". Name which case it is so the
    // operator can act; the caller then degrades the cluster to baseline / retries it.
    let finish_reason = choice
        .finish_reason
        .unwrap_or_else(|| "unknown".to_string());
    let reasoning_len = choice
        .message
        .reasoning_content
        .as_deref()
        .map_or(0, |r| r.trim().len());
    let content = strip_reasoning(&choice.message.content);
    if content.is_empty() {
        if reasoning_len > 0 {
            anyhow::bail!(
                "sidecar returned only reasoning and no content (finish_reason: {finish_reason}, \
                 reasoning_content: {reasoning_len} bytes) — the model is thinking instead of \
                 answering. Disable thinking server-side (`--reasoning-budget 0`) or per-request \
                 (`chat_template_kwargs.enable_thinking=false`), or raise max_tokens"
            );
        }
        anyhow::bail!(
            "sidecar returned an empty completion (finish_reason: {finish_reason}); the model produced \
             no content to parse — raise max_tokens, or check the sidecar"
        );
    }
    serde_json::from_str(content)
        .with_context(|| format!("parse model JSON content: {}", snippet(content)))
}

/// Strip a single leading `<think>…</think>` reasoning block (some reasoning models/templates inline it
/// into `content` instead of a separate field), returning the JSON answer that follows. An
/// *unterminated* `<think>` — the whole completion was reasoning, cut off before any answer — yields
/// `""`, which the caller treats as an empty completion. Only a leading block is stripped; the grammar
/// would never emit a `<think>` inside the JSON, so there is nothing to scan for past the prefix.
fn strip_reasoning(content: &str) -> &str {
    let s = content.trim();
    let Some(rest) = s.strip_prefix("<think>") else {
        return s;
    };
    match rest.find("</think>") {
        Some(end) => rest[end + "</think>".len()..].trim_start(),
        None => "",
    }
}

/// A short, single-line excerpt of model output for an error message — enough to recognize a malformed
/// completion in the logs without dumping a whole response. Collapses newlines and caps at 120 chars.
fn snippet(s: &str) -> String {
    let one_line = s.replace(['\n', '\r'], " ");
    let trimmed = one_line.trim();
    if trimmed.chars().count() > 120 {
        format!("{}…", trimmed.chars().take(120).collect::<String>())
    } else {
        trimmed.to_string()
    }
}

#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
    /// The OpenAI-compatible token accounting llama.cpp returns alongside the choices. Optional — a
    /// non-conforming sidecar may omit it — so its absence degrades to "no token metric", never an error.
    #[serde(default)]
    usage: Option<Usage>,
}

/// The slice of the `usage` block we record. `Copy` so reading it doesn't disturb the `choices` move
/// that follows.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
struct Usage {
    #[serde(default)]
    prompt_tokens: u64,
    #[serde(default)]
    completion_tokens: u64,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
    /// Why generation stopped (`stop` / `length` / …) — surfaced in the empty-completion error so an
    /// operator can tell "ran out of tokens" (`length`) from "model emitted nothing".
    #[serde(default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    #[serde(default)]
    content: String,
    /// The thoughts a reasoning model emits when llama.cpp parses them out of `content` (the
    /// `--reasoning-format deepseek`/`auto` shape, per the server README). Captured only to tell an
    /// empty-`content` completion that *was* reasoning (the thinking-overran-the-budget case) from one
    /// that was genuinely empty — it is never used as the answer.
    #[serde(default)]
    reasoning_content: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn ensure_reachable_errors_when_sidecar_absent() {
        let cfg = SummarizationConfig {
            // Port 1 has nothing listening → connection refused → the gate must fail (and fast,
            // within the deadline), never hang. This is the no-sidecar deploy/config error path.
            base_url: "http://127.0.0.1:1/v1".to_string(),
            ..SummarizationConfig::default()
        };
        let err = ensure_reachable(&cfg, Duration::from_millis(200))
            .await
            .expect_err("an absent sidecar must be a hard error");
        let msg = format!("{err:#}");
        assert!(msg.contains("not reachable"), "unexpected error: {msg}");
    }

    #[test]
    fn strip_reasoning_drops_leading_think_block() {
        assert_eq!(
            strip_reasoning("<think>weighing the facts</think>{\"headline\":\"x\"}"),
            "{\"headline\":\"x\"}"
        );
        // Leading/trailing whitespace around the block and the answer is tolerated.
        assert_eq!(
            strip_reasoning("  <think>x</think>\n  {\"headline\":\"x\"}  "),
            "{\"headline\":\"x\"}"
        );
    }

    #[test]
    fn strip_reasoning_passes_plain_json_through() {
        assert_eq!(strip_reasoning("{\"a\":1}"), "{\"a\":1}");
        assert_eq!(strip_reasoning("  {\"a\":1}  "), "{\"a\":1}");
    }

    #[test]
    fn strip_reasoning_unterminated_think_is_empty() {
        // The whole completion was reasoning, cut off before any answer → caller treats as empty.
        assert_eq!(strip_reasoning("<think>cut off mid thought"), "");
        assert_eq!(strip_reasoning("   <think>still thinking"), "");
    }

    #[test]
    fn snippet_collapses_newlines_and_caps_length() {
        assert_eq!(snippet("a\nb\r c"), "a b  c");
        let long: String = "x".repeat(200);
        let s = snippet(&long);
        assert_eq!(s.chars().count(), 121); // 120 chars + the ellipsis
        assert!(s.ends_with('…'));
    }
}
