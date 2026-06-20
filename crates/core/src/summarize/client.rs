//! The local-sidecar summarization client. Drives a `llama-server` (llama.cpp) over its
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
use crate::common::kind::ContentKind;
use crate::summarize::{
    apply_comprehension, clean_delta, clean_label, clean_lead, comprehend_user_prompt,
    comprehension_schema, delta_schema, delta_user_prompt, extract_facts, faithful,
    headline_only_schema, headline_only_user_prompt, label_schema, label_user_prompt, lead_schema,
    lead_user_prompt, response_schema, source_corpus, story_member_corpus, story_user_prompt,
    synthesize_facts, user_prompt, Band, ClusterSummary, Comprehension, Facts, LeadOutcome,
    SummarizationConfig, SummaryFailure, SummaryOutcome, TldrRun, COMPREHEND_SYSTEM_PROMPT,
    DELTA_SYSTEM_PROMPT, LABEL_SYSTEM_PROMPT, LEAD_SYSTEM_PROMPT, STORY_SYSTEM_PROMPT,
    SYSTEM_PROMPT,
};

/// Summarize one cluster: extract-then-summarize (§3.2) — hand the model the pre-extracted facts +
/// budgeted source text and ask it to *rewrite* them, then run the §3.4 faithfulness gate.
///
/// Returns (§3.7, no baseline fallback):
/// - [`SummaryOutcome::Faithful`] when the model answered and the gate passed it — the only result that
///   ships in a digest;
/// - [`SummaryOutcome::Failed`] otherwise: [`SummaryFailure::Unavailable`] when the sidecar was
///   unreachable/erroring (a later sweep retries once it recovers), or [`SummaryFailure::Rejected`] when
///   the gate caught an ungrounded claim (a later sweep retries with an escalated seed). Either way the
///   cluster is left un-summarized and withheld from digests, never degraded to a baseline. Never panics.
///
/// `cfg` carries this attempt's seed/temperature (the caller passes a [`SummarizationConfig::for_attempt`]
/// clone); `http` is the sweep's shared client (one connection pool for the whole pass).
pub async fn summarize_cluster(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    events: &[Event],
) -> SummaryOutcome {
    let (summary, facts, source) = match generate_candidate(cfg, http, events).await {
        Ok(generated) => generated,
        Err(e) => {
            let kind = failure_kind(&e);
            tracing::warn!(
                error = %format!("{e:#}"),
                kind,
                base_url = %cfg.base_url,
                model = %cfg.model,
                "summarization model call failed; leaving cluster unsummarized for retry"
            );
            return SummaryOutcome::Failed(SummaryFailure::Unavailable(kind));
        }
    };

    if cfg.faithfulness_gate {
        if let Err(v) = faithful(&summary, &facts, &source) {
            tracing::debug!(violation = ?v, "faithfulness gate rejected summary; will retry with an escalated seed");
            metric::gate_rejection("summarize", &v);
            return SummaryOutcome::Failed(SummaryFailure::Rejected(v));
        }
    }
    SummaryOutcome::Faithful(summary)
}

/// The shared generation half of the cluster summarizer (§3.2), factored out so the production path
/// ([`summarize_cluster`], which gates → baseline) and the read-only eval ([`eval_cluster`], which
/// gates → verdict) measure the **exact same path** and can't drift: extract the grounding facts,
/// build the budgeted source corpus, run the (best-effort) comprehension pass, call the model, and
/// assemble the candidate [`ClusterSummary`] (facts stitched back on, `tldr_text` rebuilt from the
/// runs, banded `Confirmed` pending the §3.4 gate). Returns the candidate plus the `facts` and `source`
/// the gate checks against. `Err` only when the model itself was unavailable (transport/HTTP) — a
/// failed or disabled comprehension degrades to the neutral facts, never an error.
async fn generate_candidate(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    events: &[Event],
) -> anyhow::Result<(ClusterSummary, Facts, String)> {
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

    // Depth-gate the summary's richness (§5.1): a cluster is only as deep as its richest event
    // (`ContentKind` is `Ord`: Message < Announcement < Longform). Only a Longform cluster earns a
    // multi-sentence tldr (a Story); a thinner one gets a headline-only Note, sparing both the vague
    // headline-paraphrase and the tldr token budget. An empty cluster shouldn't reach here, but default
    // to Longform if it somehow does — don't regress a real cluster to a Note.
    let depth = events
        .iter()
        .map(|e| e.content_kind)
        .max()
        .unwrap_or(ContentKind::Longform);
    let want_tldr = depth == ContentKind::Longform;

    let candidate = call_model(cfg, http, &facts, &source, want_tldr).await?;

    // Stitch the grounding facts back on and derive the flat text from the structured runs.
    let mut summary = ClusterSummary {
        headline: candidate.headline.trim().to_string(),
        tldr: candidate.tldr,
        tldr_text: String::new(),
        facts: facts.clone(),
        band: Band::Confirmed,
    };
    summary.rebuild_tldr_text();
    Ok((summary, facts, source))
}

/// Read-only **faithfulness eval** of one cluster (§3.4/§7 — the `digest-explain` hook): run the exact
/// generation path [`summarize_cluster`] uses (the shared [`generate_candidate`]) but **return the
/// gate's verdict** instead of degrading to baseline, and **store nothing**. The whole point is to
/// measure how often the model's *raw* output is faithful — the Vectara-style entity/number accuracy
/// rate (`local-ml-options.md` §7) — before any of it ships to a delivered digest.
///
/// Deliberately bypasses the [`metric::gate_rejection`] counter the production path increments: an eval
/// is a measurement, not a real rejection, so it must not pollute the operational gate-rejection rate.
/// (The shared `chat_json` plumbing still records `llm_call`/`llm_tokens` for the calls it makes — a
/// call *was* made and tokens *were* spent — but a manual eval run installs no recorder, so those are
/// no-ops in practice.)
pub async fn eval_cluster(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    events: &[Event],
) -> super::EvalVerdict {
    use super::EvalVerdict;

    let (summary, facts, source) = match generate_candidate(cfg, http, events).await {
        Ok(generated) => generated,
        Err(e) => {
            tracing::debug!(
                error = %format!("{e:#}"),
                kind = failure_kind(&e),
                "eval: model call failed; cluster counted unavailable"
            );
            return EvalVerdict::Unavailable;
        }
    };

    match faithful(&summary, &facts, &source) {
        Ok(()) => EvalVerdict::Passed,
        Err(v) => EvalVerdict::Rejected(v),
    }
}

/// Synthesize a story's cross-source summary (Phase C, §2.2): fuse the member clusters' precomputed
/// summaries into one headline + tldr for the whole happening, then run the §3.4 faithfulness gate
/// over the fused facts. `members` are the story's member-cluster summaries, **newest-first** (so
/// `members[0]` is the representative and the freshest lifecycle wins in [`synthesize_facts`]).
///
/// Treated exactly like a cluster (§3.7) — and for the same reason: a multi-source story collapsed to one
/// member's single-source blurb reads as low quality, so a synthesis that can't be made faithful is a
/// tracked error the story tier *withholds and retries*, never a silent downgrade. One attempt per call
/// (the sweep escalates the seed across passes via the story's DB attempt counter, mirroring the cluster
/// sweep): returns [`SummaryOutcome::Faithful`] on a gate-passed cross-source rewrite, or
/// [`SummaryOutcome::Failed`] (`Unavailable` / `Rejected`) otherwise — there is no representative
/// fallback. (A single-member story is never synthesized; the sweep renders its one faithful cluster
/// summary directly, so it never reaches here.)
///
/// `members` are the story's member-cluster summaries, **newest-first** (so `members[0]` is the
/// representative and the freshest lifecycle wins in [`synthesize_facts`]). `cfg` carries this attempt's
/// seed/temperature (a [`SummarizationConfig::for_attempt`] clone).
pub async fn synthesize_story(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    members: &[ClusterSummary],
    thread_label: Option<&str>,
) -> SummaryOutcome {
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
            let kind = failure_kind(&e);
            tracing::warn!(error = %e, kind, "story synthesis call failed; leaving story un-synthesized for retry");
            return SummaryOutcome::Failed(SummaryFailure::Unavailable(kind));
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
            tracing::debug!(violation = ?v, "story synthesis gate rejected; will retry with an escalated seed");
            metric::gate_rejection("synthesize", &v);
            return SummaryOutcome::Failed(SummaryFailure::Rejected(v));
        }
    }
    SummaryOutcome::Faithful(summary)
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

/// Compose the **authored big-picture lead** (Phase D, §2.4/§3.1): an "editor's note" over the
/// selected items' `headlines` and the `threads` they advance, rephrasing them into one or two
/// sentences. This is the *one* summarization call that runs on the punctual path — so the caller wraps
/// it with [`SummarizationConfig::lead_deadline`](crate::summarize::SummarizationConfig::lead_deadline)
/// and retries it (with an escalated seed) rather than ship a digest without it (§3.7).
///
/// Returns a [`LeadOutcome`]: `Ready` with the composed sentence; `Rejected` when the model answered but
/// [`clean_lead`] (voice + length + URL + numeric grounding against the headlines/threads) caught it — a
/// deterministic miss the caller re-seeds past; or `Unavailable` on any transport failure — the caller
/// defers the digest so the job retries later. The headlines fed here are themselves already-gated
/// summaries (§4), so the lead never touches raw events.
pub async fn authored_lead(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    dominant: &[String],
    also: &[String],
    threads: &[String],
    total_items: usize,
) -> LeadOutcome {
    // The grounding the lead's numbers are checked against: the headlines it was actually shown (the
    // dominant + also tiers) + the thread labels it can name. A big-picture lead naturally cites a count
    // ("…and 6 other updates"), so the digest's own item counts — the FULL selected total and the
    // "N others" (total − 1) — are grounded too (the total is the whole selection, not just the tiers,
    // since the long tail the model never saw still counts toward "N others"); without them the §3.4
    // numeric gate would reject every count-bearing lead.
    let mut grounding = dominant.join("\n");
    grounding.push('\n');
    grounding.push_str(&also.join("\n"));
    grounding.push('\n');
    grounding.push_str(&threads.join("\n"));
    grounding.push_str(&format!(
        "\n{total_items} {}",
        total_items.saturating_sub(1)
    ));

    let out = chat_json::<LeadOutput>(
        cfg,
        http,
        "lead",
        LEAD_SYSTEM_PROMPT,
        lead_user_prompt(dominant, also, threads),
        cfg.headline_max_tokens + cfg.tldr_max_tokens,
        lead_schema(),
    )
    .await;
    match out {
        // `clean_lead` returns `None` when the model's sentence failed the voice/length/grounding lint —
        // a deterministic rejection the caller retries with a fresh seed.
        Ok(o) => match clean_lead(&o.lead, &grounding) {
            Some(lead) => LeadOutcome::Ready(lead),
            None => {
                tracing::debug!("digest lead rejected by the voice/grounding gate; retrying with an escalated seed");
                LeadOutcome::Rejected
            }
        },
        Err(e) => {
            tracing::debug!(error = %e, kind = failure_kind(&e), "digest lead call failed; deferring digest");
            LeadOutcome::Unavailable
        }
    }
}

/// Startup reachability gate for the local summarization sidecar. Summarization is a hard dependency of
/// the pipeline now (§3.7), so an unreachable sidecar at boot is a deployment/config error — a wrong
/// `BULLETIN_LLM_BASE_URL`, a sidecar that never came up, or a model that failed to load — and we want
/// it surfaced **loudly** (a failed unit → deploy rollback) rather than a worker that quietly quarantines
/// the entire corpus and defers every digest. This is distinct from the *running* contract: once past
/// this gate, a transient sidecar blip mid-sweep is a tracked per-cluster failure that retries (and a
/// digest waits on its lead) — the boot gate just refuses to start against a sidecar that was never there.
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

/// The Phase-D digest-lead response (just the big-picture sentence); cleaned by [`clean_lead`].
#[derive(Debug, Deserialize)]
struct LeadOutput {
    #[serde(default)]
    lead: String,
}

/// POST one grammar-constrained summarization completion and parse the structured output. Errors
/// bubble up to the caller, which degrades to baseline.
///
/// `want_tldr` gates the summary's depth (§5.1): `true` for a Longform cluster asks for the full
/// headline + multi-sentence tldr; `false` for a Note-depth cluster asks for a **headline only** (a
/// tighter schema/prompt and no tldr token budget), and the absent tldr deserializes to the empty
/// `ModelOutput.tldr` default — yielding a summary with an empty `tldr`/`tldr_text`.
async fn call_model(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    facts: &Facts,
    source: &str,
    want_tldr: bool,
) -> anyhow::Result<ModelOutput> {
    let (user, max_tokens, schema) = if want_tldr {
        (
            user_prompt(facts, source),
            cfg.headline_max_tokens + cfg.tldr_max_tokens + 64,
            response_schema(&facts.entities),
        )
    } else {
        (
            headline_only_user_prompt(facts, source),
            cfg.headline_max_tokens + 16,
            headline_only_schema(),
        )
    };
    chat_json(
        cfg,
        http,
        "summarize",
        SYSTEM_PROMPT,
        user,
        max_tokens,
        schema,
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
