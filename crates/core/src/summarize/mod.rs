//! LLM summarization (Phase A — the cluster foundation; `docs/llm-summarization.md`).
//!
//! Produces the **durable, content-hashed cluster summary** every higher surface composes from. The
//! governing constraint (`local-ml-options.md` §0, `thread-layer.md` §3.1): every model call is
//! write-side, best-effort, off the punctual path, behind a flag, and **degrades to a deterministic
//! baseline** — a missing or rejected summary costs a plainer email, never a late or wrong one.
//!
//! This module splits cleanly into two halves:
//!
//! - **The pure core (always compiled, unit-tested):** the data model ([`ClusterSummary`]), the
//!   content signature ([`summary_hash`]), the grounding-fact skeleton ([`extract_facts`]), the
//!   grammar/JSON schema + prompts ([`response_schema`] / [`SYSTEM_PROMPT`] / [`user_prompt`]), the
//!   deterministic [`faithfulness gate`](faithful), and the [`baseline`] fallback. None of it talks
//!   to a model or the DB, so it is exercised without a sidecar.
//! - **The gated edge (behind `feature = "llm-summarization"`):** [`client`] (the local-sidecar HTTP
//!   call) and the DB [`sweep`](sweep_public)/[`store`](store) that walk the work queue. Compiled out
//!   by default so the deterministic digest ships unchanged.

#[cfg(feature = "llm-summarization")]
pub mod client;
/// The `bulletin_llm_*` recorders for the summarization path; gated with the rest of the model edge.
#[cfg(feature = "llm-summarization")]
mod metric;
#[cfg(feature = "llm-summarization")]
pub(crate) mod store;

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::common::event::Event;

// ── Config + kill switch (docs/llm-summarization.md §2.5) ──────────────────────────────────────

/// Tuning surface for summarization, held as a struct like
/// [`thread::MaintenanceConfig`](crate::thread::MaintenanceConfig) — a `summarization_config` row
/// when per-deployment tuning bites. **There is no runtime kill switch here:** the *only* switch is
/// the compile-time `llm-summarization` feature (mirroring `thread-weighting`), so a build without it
/// has no summarization code at all. This struct is pure config — the sidecar address, the model, and
/// the generation knobs — never a guard.
#[derive(Debug, Clone)]
pub struct SummarizationConfig {
    /// The 100%-local sidecar's OpenAI-compatible base URL (no egress, §3.5), e.g.
    /// `http://127.0.0.1:8080/v1`. The summary request POSTs to `{base_url}/chat/completions`.
    pub base_url: String,
    /// The served model name, e.g. `qwen3.5-4b-instruct` (`local-ml-options.md` §7).
    pub model: String,
    /// Prompt version — bumped on any prompt/schema change so [`summary_model`](Self::summary_model)
    /// changes and the old corpus re-summarizes (`WHERE summary_model <> $current`, §2.1).
    pub prompt_version: u32,
    /// Per-task token ceilings (§3.3): short outputs cut latency *and* hallucination.
    pub headline_max_tokens: u32,
    pub tldr_max_tokens: u32,
    /// Token ceiling for the comprehension pass (§3.2, `local-ml-options.md` §6): a short
    /// `analysis` scratchpad + the three classified fields, so it needs a touch more room than a
    /// headline but stays small (it is off the hot path).
    pub comprehension_max_tokens: u32,
    /// Low temperature + fixed seed ⇒ a content-unchanged cluster re-summarizes identically, so the
    /// content-hash cache is meaningful (§3.3 idempotency).
    pub temperature: f32,
    pub seed: u32,
    /// Run the deterministic faithfulness gate (§3.4). Off only for eval/debugging — production keeps
    /// it on, since it is the real backstop against a hallucinated entity/number reaching a digest.
    pub faithfulness_gate: bool,
    /// Run the comprehension pass (§3.2, `local-ml-options.md` §6) ahead of the summarizer: a tiny
    /// constrained LLM call that fills `facts.event_type` / `state` / `certainty` so the summarizer's
    /// hedge rule (§3.6) is *looked up*, not inferred. Best-effort itself — when off, or when the call
    /// fails, the facts stay at their neutral defaults and the summarizer degrades to "state asserted
    /// facts plainly," the safe direction.
    pub comprehend: bool,
    /// Send `chat_template_kwargs: {enable_thinking: false}` on every call to suppress a reasoning
    /// model's native `<think>` block (§3.6 — these are short, grammar-constrained rephrases; the
    /// reasoning only wastes the token budget and, when it exhausts it, leaves an empty completion).
    /// Honoured by a llama.cpp sidecar run with `--jinja`; harmlessly ignored otherwise. On by default;
    /// the rare deployment whose model *needs* thinking can clear it via `BULLETIN_LLM_DISABLE_THINKING`.
    pub disable_thinking: bool,
    /// HTTP timeout for one sidecar call. Generous — it is off the punctual path; a timeout just
    /// degrades that cluster to baseline. A small model on CPU can be slow on a cold cache, so this is
    /// sized for the worst realistic single call; raise it for slower hardware via
    /// `BULLETIN_LLM_REQUEST_TIMEOUT_SECS`.
    pub request_timeout: Duration,
    /// Deadline for the **Phase-D authored big-picture lead** (§2.4/§3.1) — the *one* model call that
    /// runs on the punctual path (after selection, before send), so unlike every other summary it is
    /// deadline-bounded rather than merely best-effort: if the editor's note doesn't return within this
    /// window the digest ships the deterministic lead and goes out on time ("fall behind, never wrong").
    /// Deliberately well under [`request_timeout`](Self::request_timeout) so a slow box delays a send by
    /// at most this long; tune via `BULLETIN_LLM_LEAD_DEADLINE_SECS`.
    pub lead_deadline: Duration,
    /// Source-text budget per cluster (§7 long-context cliff): truncate the concatenated event
    /// title+body fed to the model so a small model stays in its faithful regime.
    pub max_source_chars: usize,
    /// Max clusters summarized per sweep — bounds one best-effort pass so a large backlog drains over
    /// several sweeps rather than one long-running job.
    pub max_per_sweep: i64,
}

impl Default for SummarizationConfig {
    fn default() -> Self {
        SummarizationConfig {
            base_url: "http://127.0.0.1:8080/v1".to_string(),
            model: "qwen3.5-4b-instruct".to_string(),
            // Bumped to 2 with the comprehension pass: facts now carry event_type/state/certainty, so
            // a re-summarize of the corpus picks up the richer (and hedge-aware) phrasing. Bumped to 3
            // with the "don't paste raw URLs" rule (+ the deterministic url_like_token gate), so the
            // corpus re-summarizes without the leaked raw source links. Bumped to 4 with the longer
            // tldr (2–4 sentences, wider token + gate budgets), so the corpus re-summarizes at length.
            // Bumped to 5 when the URL rule widened to bare domains too, so the corpus re-summarizes
            // without the bare-domain mentions a mail client would auto-linkify.
            prompt_version: 5,
            headline_max_tokens: 24,
            tldr_max_tokens: 144,
            comprehension_max_tokens: 256,
            temperature: 0.2,
            seed: 42,
            faithfulness_gate: true,
            comprehend: true,
            disable_thinking: true,
            request_timeout: Duration::from_secs(120),
            lead_deadline: Duration::from_secs(20),
            max_source_chars: 4000,
            max_per_sweep: 200,
        }
    }
}

impl SummarizationConfig {
    /// The `<model>@<prompt-version>` provenance string stamped on `cluster.summary_model`. A model or
    /// prompt upgrade changes it, which invalidates the whole corpus by a cheap `WHERE` sweep — no
    /// data migration (§2.1).
    pub fn summary_model(&self) -> String {
        format!("{}@{}", self.model, self.prompt_version)
    }

    /// Build a config from the `BULLETIN_LLM_*` environment (the binary's runtime config seam): the
    /// sidecar `BASE_URL`, `MODEL`, and `PROMPT_VERSION`, plus the operational knobs an operator needs
    /// to recover a degraded edge without a recompile — `REQUEST_TIMEOUT_SECS` (slow hardware),
    /// `COMPREHEND` (drop the extra per-cluster call to halve the load), and `DISABLE_THINKING` (the
    /// reasoning-model toggle). Everything else stays at the conservative defaults. These are *config,
    /// not a kill switch* — whether summarization runs at all is the compile-time feature's call (this
    /// code only exists in a feature build). Reached only from the gated worker step, so it never
    /// executes in a default build.
    pub fn from_env() -> Self {
        let mut cfg = SummarizationConfig::default();
        if let Ok(v) = std::env::var("BULLETIN_LLM_BASE_URL") {
            if !v.trim().is_empty() {
                cfg.base_url = v.trim().trim_end_matches('/').to_string();
            }
        }
        if let Ok(v) = std::env::var("BULLETIN_LLM_MODEL") {
            if !v.trim().is_empty() {
                cfg.model = v.trim().to_string();
            }
        }
        if let Ok(v) = std::env::var("BULLETIN_LLM_PROMPT_VERSION") {
            if let Ok(n) = v.trim().parse() {
                cfg.prompt_version = n;
            }
        }
        // A positive whole-second override only — a `0`/garbage value keeps the default rather than
        // setting a zero (i.e. effectively no) timeout.
        if let Ok(v) = std::env::var("BULLETIN_LLM_REQUEST_TIMEOUT_SECS") {
            if let Ok(secs) = v.trim().parse::<u64>() {
                if secs > 0 {
                    cfg.request_timeout = Duration::from_secs(secs);
                }
            }
        }
        // The Phase-D lead deadline (the one on-path call). A positive whole-second override only.
        if let Ok(v) = std::env::var("BULLETIN_LLM_LEAD_DEADLINE_SECS") {
            if let Ok(secs) = v.trim().parse::<u64>() {
                if secs > 0 {
                    cfg.lead_deadline = Duration::from_secs(secs);
                }
            }
        }
        if let Some(b) = env_bool("BULLETIN_LLM_COMPREHEND") {
            cfg.comprehend = b;
        }
        if let Some(b) = env_bool("BULLETIN_LLM_DISABLE_THINKING") {
            cfg.disable_thinking = b;
        }
        // The lead deadline only bounds the punctual send if it sits at or under the per-call HTTP
        // timeout — past that, `request_timeout` is the real cap and the "at most `lead_deadline`"
        // guarantee is silently false. Clamp so a misconfigured (or default-vs-lowered-timeout) value
        // can never exceed it.
        cfg.lead_deadline = cfg.lead_deadline.min(cfg.request_timeout);
        cfg
    }
}

/// Parse a boolean-ish env var (`1`/`0`, `true`/`false`, `yes`/`no`, `on`/`off`, case-insensitive).
/// `None` when unset or unrecognized, so the caller keeps its default rather than guessing — used by
/// the optional `BULLETIN_LLM_*` toggles in [`SummarizationConfig::from_env`].
fn env_bool(key: &str) -> Option<bool> {
    let v = std::env::var(key).ok()?;
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

// ── The data model (§2.1, §6.2) ────────────────────────────────────────────────────────────────

/// The source's epistemic stance on a fact (§2.1/§3.6). Decided once, in the comprehension/extraction
/// pass, and handed to the summarizer as a flag it *branches on* — never inferred by the small model.
/// `Asserted` ⇒ state it plainly; `Tentative` ⇒ keep the source's hedge ("suspected", "appears to").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Certainty {
    #[default]
    Asserted,
    Tentative,
}

/// The faithfulness verdict the gate (§3.4) stamps on a summary, carried to render as the §10.4
/// confidence surface. `Confirmed`/`Probable` are model output that passed the gate; `Uncertain` is
/// what a rejected (or never-generated) summary degrades to — the deterministic baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Band {
    Confirmed,
    Probable,
    #[default]
    Uncertain,
}

impl Band {
    /// The lowercase string form (matching the serde rename), for the render debug trace — one source
    /// of the token, mirroring [`ConfidenceBand::as_str`](crate::identity::ConfidenceBand::as_str).
    pub fn as_str(self) -> &'static str {
        match self {
            Band::Confirmed => "confirmed",
            Band::Probable => "probable",
            Band::Uncertain => "uncertain",
        }
    }
}

/// One run of the `tldr` (§6.2): either literal `text`, or a grounded entity `ref` whose token is
/// constrained (by the response grammar, §3.3) to the closed set of `facts.entities` — so the model
/// can *reference* a grounded entity (for an inline badge) but can never *name* one that wasn't
/// extracted from ground truth. `surface` is the visible display text; render resolves `ref` to a
/// badge (person/repo/CVE) and falls back to plain `surface` when unresolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TldrRun {
    /// A grounded entity reference. Deserialized first (it carries the discriminating `ref` key).
    Ref {
        #[serde(rename = "ref")]
        entity: String,
        surface: String,
    },
    /// A literal text run.
    Text { text: String },
}

impl TldrRun {
    /// The visible text of this run (literal text, or an entity ref's display surface) — the building
    /// block of the flat [`ClusterSummary::tldr_text`].
    pub fn surface(&self) -> &str {
        match self {
            TldrRun::Text { text } => text,
            TldrRun::Ref { surface, .. } => surface,
        }
    }
}

/// The "extract" half (§2.1) — the comprehension/extraction product that *grounds* the summary. Stored
/// on the cluster so the extract step runs once and feeds every higher tier. The summarizer rewrites
/// these facts; it never recalls them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Facts {
    /// The grounded entity set — the closed `enum` the tldr's refs are constrained to.
    #[serde(default)]
    pub entities: Vec<String>,
    /// Event type (`incident`/`release`/…), from the Phase-2 comprehension pass. `None` until it lands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    /// Lifecycle state (`detected`→`resolved`), from comprehension. `None` until it lands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    /// The source's stance — drives the §3.6 hedge rule. Neutral (`asserted`) until comprehension
    /// supplies it.
    #[serde(default)]
    pub certainty: Certainty,
    /// Numbers/quantities mined from the source — part of the faithfulness check.
    #[serde(default)]
    pub numbers: Vec<String>,
    /// Dates/times mined from the source — part of the faithfulness check.
    #[serde(default)]
    pub dates: Vec<String>,
}

/// The extract-then-summarize product for one cluster (§2.1) — the `cluster.summary` jsonb. The
/// inert default (`'{}'` ⇒ [`is_empty`](Self::is_empty)) means no pass has run; the renderer then
/// falls back to the deterministic cluster title.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ClusterSummary {
    /// Abstractive headline, ≤ ~90 chars (the schema's `maxLength`).
    #[serde(default)]
    pub headline: String,
    /// The structured 2–4 sentence tldr as a run-list of text + grounded entity refs (§6.2).
    #[serde(default)]
    pub tldr: Vec<TldrRun>,
    /// Flat concatenation of the tldr's runs — for the plaintext email + inbox preview (§6.2).
    #[serde(default)]
    pub tldr_text: String,
    /// The grounding facts (the extract half) — reused by every higher tier.
    #[serde(default)]
    pub facts: Facts,
    /// The faithfulness verdict (§3.4).
    #[serde(default)]
    pub band: Band,
}

impl ClusterSummary {
    /// True for the inert default — no summary has run, so the renderer uses the deterministic
    /// baseline (the cluster `title`). Both abstractive fields empty ⇒ nothing to render.
    pub fn is_empty(&self) -> bool {
        self.headline.trim().is_empty() && self.tldr_text.trim().is_empty()
    }

    /// Recompute [`tldr_text`](Self::tldr_text) from the run-list — the single source of truth for the
    /// flat text, so the plaintext fallback can never drift from the structured runs.
    pub fn rebuild_tldr_text(&mut self) {
        self.tldr_text = self.tldr.iter().map(TldrRun::surface).collect();
    }
}

// ── Content signature (§2.1 staleness gate) ──────────────────────────────────────────────────────

/// The content signature of a cluster's summary inputs: SHA-256 over each event's
/// `title‖body‖links‖entities`, in `(event_time, id)` order. The summary is recomputed **only when
/// this changes** — the cheap staleness gate that makes a unit summarized once per content change,
/// not once per fire or per subscriber. Order-independent of the caller (sorted defensively).
pub fn summary_hash(events: &[Event]) -> Vec<u8> {
    const FIELD: u8 = 0x00; // field separator
    const ITEM: u8 = 0x1f; // intra-field list separator (ASCII unit separator)

    let mut order: Vec<&Event> = events.iter().collect();
    order.sort_by(|a, b| a.event_time.cmp(&b.event_time).then(a.id.cmp(&b.id)));

    let mut h = Sha256::new();
    for e in order {
        h.update(e.title.as_bytes());
        h.update([FIELD]);
        if let Some(b) = &e.body {
            h.update(b.as_bytes());
        }
        h.update([FIELD]);
        for l in &e.links {
            h.update(l.as_bytes());
            h.update([ITEM]);
        }
        h.update([FIELD]);
        for ent in &e.entities {
            h.update(ent.as_bytes());
            h.update([ITEM]);
        }
        h.update([FIELD]);
    }
    h.finalize().to_vec()
}

// ── Grounding facts (§3.2 — the extract half) ────────────────────────────────────────────────────

/// Build the grounding [`Facts`] for a cluster from what the deterministic backbone *already*
/// extracted: the sorted, de-duplicated union of its events' `entities` (the §8.2 blocking substrate),
/// plus numbers/dates mined by a light scan over each event's title + body.
///
/// This is the deterministic *skeleton*. The richer comprehension output (`event_type`, `state`,
/// `certainty`) is filled by [`apply_comprehension`] from the tiny-LLM pass (`local-ml-options.md`
/// §6), wired into [`client::summarize_cluster`](crate::summarize::client::summarize_cluster) ahead of
/// the summarizer. When the comprehension pass is off or unavailable those fields stay at their
/// neutral defaults, and the summarizer degrades to "state asserted facts plainly," the safe
/// direction.
///
/// On the entity-span half of the design's GLiNER + tiny-LLM split: Bulletin already has a
/// deterministic NER substrate — M3's namespaced entity tokens (`repo:`/`user:`/`cve:`/…), rolled
/// onto every cluster — so `facts.entities` is sourced from ground truth here rather than from a
/// separate span model. The comprehension LLM only supplies the *reasoning* half (the event's type,
/// lifecycle state, and the source's stance).
pub fn extract_facts(events: &[Event]) -> Facts {
    let mut entities: Vec<String> = Vec::new();
    let mut numbers: Vec<String> = Vec::new();
    let mut dates: Vec<String> = Vec::new();
    for e in events {
        entities.extend(e.entities.iter().cloned());
        mine_numeric(&e.title, &mut numbers, &mut dates);
        if let Some(b) = &e.body {
            mine_numeric(b, &mut numbers, &mut dates);
        }
    }
    dedup_sorted(&mut entities);
    dedup_sorted(&mut numbers);
    dedup_sorted(&mut dates);
    Facts {
        entities,
        numbers,
        dates,
        ..Facts::default()
    }
}

/// Sort + dedup in place (the §2.1 stable, deterministic fact ordering).
fn dedup_sorted(v: &mut Vec<String>) {
    v.sort();
    v.dedup();
}

/// Light deterministic miner: pull digit-bearing tokens out of `text`, routing ISO-ish / clock-ish
/// tokens (containing `-` or `:`) to `dates` and the rest to `numbers`. Deliberately simple — the
/// real comprehension pass (Phase 2) will replace it; this only needs to seed the faithfulness check
/// with the numbers/dates that legitimately appear in the source.
fn mine_numeric(text: &str, numbers: &mut Vec<String>, dates: &mut Vec<String>) {
    for tok in tokenize_numeric(text) {
        if tok.contains('-') || tok.contains(':') {
            dates.push(tok);
        } else {
            numbers.push(tok);
        }
    }
}

/// Split `text` into maximal runs of "numeric token" characters (digits and the punctuation that
/// glues a quantity/timestamp together: `% . , : - / +`), keeping only runs that contain ≥1 digit.
/// Shared by the miner and the faithfulness gate so they agree on what a "number/date" token is.
fn tokenize_numeric(text: &str) -> Vec<String> {
    fn is_numeric_char(c: char) -> bool {
        c.is_ascii_digit() || matches!(c, '%' | '.' | ',' | ':' | '-' | '/' | '+')
    }
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut has_digit = false;
    for c in text.chars() {
        if is_numeric_char(c) {
            cur.push(c);
            has_digit |= c.is_ascii_digit();
        } else {
            push_numeric(&mut cur, &mut has_digit, &mut out);
        }
    }
    push_numeric(&mut cur, &mut has_digit, &mut out);
    out
}

/// Flush an accumulated numeric run into `out` (trimming glue punctuation off the ends) iff it held a
/// digit; reset the accumulator. Factored out so [`tokenize_numeric`] handles the in-loop and final
/// flush identically.
fn push_numeric(cur: &mut String, has_digit: &mut bool, out: &mut Vec<String>) {
    if *has_digit {
        let trimmed = cur.trim_matches(|c: char| matches!(c, '.' | ',' | ':' | '-' | '/' | '+'));
        if !trimmed.is_empty() {
            out.push(trimmed.to_string());
        }
    }
    cur.clear();
    *has_digit = false;
}

// ── The comprehension pass (§3.2, local-ml-options.md §6) — extract before summarize ──────────────

/// The closed event-type vocabulary the comprehension pass classifies into (the schema `enum`, and the
/// validation re-check). `other` is the always-available escape hatch a small model can fall back to;
/// it is treated as "no useful type" by [`apply_comprehension`] (left unset on the facts).
pub const EVENT_TYPES: &[&str] = &[
    "incident",
    "release",
    "advisory",
    "announcement",
    "discussion",
    "change",
    "other",
];

/// The closed lifecycle-state vocabulary. `none` means "no lifecycle applies" — [`apply_comprehension`]
/// leaves the fact's `state` unset for it (and for `other`), so a non-lifecycle event carries no
/// misleading state.
pub const STATES: &[&str] = &[
    "detected",
    "investigating",
    "resolved",
    "proposed",
    "in_progress",
    "merged",
    "published",
    "closed",
    "none",
];

/// The comprehension pass's output (§3.2): a short free-text `analysis` scratchpad **first** (the
/// CRANE "reason, then constrain" lever — `local-ml-options.md` §6), then the three classified fields
/// the summarizer branches on. The model never recalls names/numbers here — that is the deterministic
/// skeleton's job (`extract_facts`); this only judges *type / lifecycle / stance* once, so every
/// downstream summary call is a mechanical rephrase (§3.6).
///
/// Tolerant deserialize (every field defaulted): a missing/garbled field degrades to the neutral
/// default, and [`apply_comprehension`] re-validates the closed-vocab fields against [`EVENT_TYPES`] /
/// [`STATES`] regardless of the grammar (defense in depth, like the entity gate).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Comprehension {
    /// The reasoning scratchpad. Named to sort *first* among the object's keys so it is generated
    /// before the classification (serde_json has no `preserve_order` here, and llama.cpp orders object
    /// properties lexically — an `a…` name is the portable way to guarantee scratchpad-first).
    #[serde(default)]
    pub analysis: String,
    /// The source's stance — the §3.6 hedge driver. Validated by the schema `enum`; defaults to
    /// `asserted` (the safe, plain-spoken direction) when absent.
    #[serde(default)]
    pub certainty: Certainty,
    #[serde(default)]
    pub event_type: String,
    #[serde(default)]
    pub state: String,
}

/// Fold a comprehension result onto the deterministic [`Facts`] skeleton (§2.1). Re-validates the
/// closed-vocab fields against [`EVENT_TYPES`] / [`STATES`] — an out-of-vocab or "no useful value"
/// (`other` / `none`) classification leaves the field unset, so the summarizer only ever sees a
/// grounded type/state. `certainty` always applies (its only values are the safe `asserted` and the
/// hedging `tentative`). Pure + deterministic, so it is unit-tested without a model.
pub fn apply_comprehension(facts: &mut Facts, c: &Comprehension) {
    facts.certainty = c.certainty;
    if EVENT_TYPES.contains(&c.event_type.as_str()) && c.event_type != "other" {
        facts.event_type = Some(c.event_type.clone());
    }
    if STATES.contains(&c.state.as_str()) && c.state != "none" {
        facts.state = Some(c.state.clone());
    }
}

/// The comprehension system prompt — engineered for a 3–4B model exactly like [`SYSTEM_PROMPT`]
/// (§3.6 "built for a 3–4B model"): short, imperative, one job, with the closed vocab inline and two
/// worked few-shot pairs that *show* asserted→plain and tentative→hedged. A constant ⇒ prefix-cached.
pub const COMPREHEND_SYSTEM_PROMPT: &str = r#"You read one work event and classify it. Think first, then label.

Fill these fields:
- analysis: 1-2 short sentences. What happened, and does the source state it as settled fact or hedge it (suspected, appears to, proposed, under investigation)?
- event_type: one of incident, release, advisory, announcement, discussion, change, other.
- state: where it is in its lifecycle - one of detected, investigating, resolved, proposed, in_progress, merged, published, closed, none. Use none if no lifecycle applies.
- certainty: asserted if the source states it as settled fact; tentative if the source hedges.

Use only what the source says. Do not guess beyond it. Output only the JSON the schema asks for. No preamble.

EXAMPLES
source: A bad config in the 14:02 rollout broke token validation; ~12% of logins failed for 40m until a rollback.
out: {"analysis":"A deploy broke logins and was rolled back; the source states it as resolved fact.","certainty":"asserted","event_type":"incident","state":"resolved"}

source: A high-severity advisory appears to affect billing's PDF path; no patch yet, still under investigation.
out: {"analysis":"A security advisory that may affect billing; the source hedges and is still investigating.","certainty":"tentative","event_type":"advisory","state":"investigating"}"#;

/// Format a fact list for a prompt line: comma-joined, or the literal `(none)` for an empty list (so
/// the model is *told* a category is empty rather than left to infer it from a blank value). Shared by
/// the comprehension and summarization prompts.
fn list_or_none(items: &[String]) -> String {
    if items.is_empty() {
        "(none)".to_string()
    } else {
        items.join(", ")
    }
}

/// The per-cluster comprehension user prompt: the deterministically-extracted grounding (entities,
/// numbers, dates) + the budgeted source text + the concrete ask. Short and concrete over the §4
/// pre-distilled inputs, like [`user_prompt`].
pub fn comprehend_user_prompt(facts: &Facts, source_text: &str) -> String {
    format!(
        "entities: {}\n\
         numbers: {}\n\
         dates: {}\n\
         source:\n{source_text}\n\n\
         Classify this event: analysis first, then event_type, state, certainty.",
        list_or_none(&facts.entities),
        list_or_none(&facts.numbers),
        list_or_none(&facts.dates),
    )
}

/// The comprehension response schema (§3.2) for `response_format: json_schema`. Constrains the three
/// classified fields to their closed vocab so the small model can only emit a known type/state/stance,
/// while `analysis` is a free-text string (the scratchpad — deliberately *not* hard-constrained, the
/// `local-ml-options.md` §6 "grammar tax" caveat; only its length is capped). All four are required so
/// the scratchpad is always produced (and, being named `analysis`, produced first).
pub fn comprehension_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "name": "comprehension",
        "strict": true,
        "schema": {
            "type": "object",
            "properties": {
                "analysis":   { "type": "string", "maxLength": 600 },
                "certainty":  { "type": "string", "enum": ["asserted", "tentative"] },
                "event_type": { "type": "string", "enum": EVENT_TYPES },
                "state":      { "type": "string", "enum": STATES }
            },
            "required": ["analysis", "certainty", "event_type", "state"],
            "additionalProperties": false
        }
    })
}

// ── The faithfulness gate (§3.4) — ML never grounds alone ────────────────────────────────────────

/// Why a candidate summary failed the gate — surfaced in logs to explain a baseline fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateViolation {
    /// An entity `ref` in the tldr is not in the closed `facts.entities` set (a hallucinated entity).
    UngroundedEntity(String),
    /// A number/date token in the output appears in neither the facts nor the source text (invented).
    UngroundedNumber(String),
    /// A banned hype word or second-person address slipped through (§3.6 denylist).
    BannedWord(String),
    /// A clickable-looking token — an explicit URL (`://`, `www.`, `mailto:`) **or a bare domain**
    /// (`databricks.com`) — leaked into the prose. The model names sources; it doesn't write links (the
    /// interface carries them). Rejected because mail clients auto-linkify either form in displayed
    /// text, a link surface outside the renderer's control entirely — see [`crate::common::link_safety`].
    UrlInProse(String),
    /// The headline or tldr exceeds its length budget.
    TooLong,
}

/// Words the editorial voice forbids (§3.6): hype + second person. A small model *will* occasionally
/// slip one; the lint catches it and the candidate is rejected to baseline. Matched whole-word,
/// case-insensitively. `critical` is intentionally absent — it is allowed when it is in the source.
const BANNED_WORDS: &[&str] = &[
    "massive",
    "huge",
    "game-changing",
    "game changing",
    "exciting",
    "revolutionary",
    "you",
    "we",
    "your",
    "our",
];

/// Token-equality numeric grounding (§3.4), shared by the cluster/story faithfulness gate
/// ([`faithful`]) and the Phase-D lead gate ([`clean_lead`]) so the two can never drift on what counts
/// as a grounded number. Returns the first numeric/date token in `output` that does **not** appear (as
/// the same token, case-insensitively) anywhere in `grounding`, or `None` when every number in the
/// output is grounded. Both sides go through [`tokenize_numeric`], so they agree on token boundaries and
/// on unit-suffix stripping ("40m" → "40") — and token-equality (not substring) means an output "40" is
/// never falsely grounded by a `grounding` "4000".
fn first_ungrounded_number(output: &str, grounding: &str) -> Option<String> {
    let grounded: std::collections::HashSet<String> = tokenize_numeric(grounding)
        .into_iter()
        .map(|t| t.to_lowercase())
        .collect();
    tokenize_numeric(output)
        .into_iter()
        .find(|tok| !grounded.contains(&tok.to_lowercase()))
}

/// The deterministic faithfulness gate (§3.4): the model may *drop* a fact but never *add* one, and
/// must stay in the house voice. A cheap, post-generation check that
///
/// - every entity `ref` in the tldr is in `facts.entities` (the structural §6.2 guarantee — also
///   grammar-enforced, re-checked here in case the grammar was bypassed);
/// - every number/date-looking token in the headline/tldr appears in `facts` *or* verbatim in the
///   `source_text` (the model may phrase, never invent, a quantity);
/// - no banned hype/second-person word survives (§3.6 lint);
/// - the headline/tldr stay within their length budgets.
///
/// On `Err` the caller rejects the candidate and falls back to [`baseline`], banding it `uncertain` —
/// the digest **never ships an unverified hallucination**; the worst case is a plainer, true line.
pub fn faithful(
    summary: &ClusterSummary,
    facts: &Facts,
    source_text: &str,
) -> Result<(), GateViolation> {
    /// Headline budget (chars) — matches the schema `maxLength` (§3.3).
    const HEADLINE_MAX: usize = 90;
    /// tldr budget (chars) — 2–4 sentences.
    const TLDR_MAX: usize = 480;

    if summary.headline.chars().count() > HEADLINE_MAX
        || summary.tldr_text.chars().count() > TLDR_MAX
    {
        return Err(GateViolation::TooLong);
    }

    // Closed-enum entity refs (a hallucinated mention is structurally impossible, but verify).
    for run in &summary.tldr {
        if let TldrRun::Ref { entity, .. } = run {
            if !facts.entities.iter().any(|e| e == entity) {
                return Err(GateViolation::UngroundedEntity(entity.clone()));
            }
        }
    }

    // Numbers/dates: every numeric token in the output must be grounded — appear as the *same token*
    // in the facts' numbers/dates or the source text. Token-equality, not substring, so an output "40"
    // is never falsely grounded by a source "4000" (see [`first_ungrounded_number`]).
    let mut grounding: String = facts
        .numbers
        .iter()
        .chain(facts.dates.iter())
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    grounding.push(' ');
    grounding.push_str(source_text);
    let output = format!("{} {}", summary.headline, summary.tldr_text);
    if let Some(tok) = first_ungrounded_number(&output, &grounding) {
        return Err(GateViolation::UngroundedNumber(tok));
    }

    // House-voice lint (§3.6 denylist), whole-word + case-insensitive.
    if let Some(w) = banned_word_in(&output) {
        return Err(GateViolation::BannedWord(w));
    }

    // No web addresses in the prose: the model names sources and the interface carries the link, so a
    // leaked URL — or bare domain — is both an artifact and a client-autolink surface the renderer
    // can't keep inert (the client linkifies displayed text on its own). Reject; baseline is true.
    if let Some(u) = url_like_token(&output) {
        return Err(GateViolation::UrlInProse(u));
    }

    Ok(())
}

/// The §3.6 house-voice lint, factored out so the cluster gate, the story synthesis gate, and the
/// Phase-B label/delta cleaners all reject the same hype/second-person vocabulary (and `!`). Returns
/// the first banned token found (whole-word, case-insensitive), or `None` if the text is clean.
pub fn banned_word_in(text: &str) -> Option<String> {
    let lc = text.to_lowercase();
    for banned in BANNED_WORDS {
        if contains_word(&lc, banned) {
            return Some((*banned).to_string());
        }
    }
    if text.contains('!') {
        return Some("!".to_string());
    }
    None
}

/// The first clickable-looking token in `text`, or `None` for clean prose — the deterministic backstop
/// to the prompt's "no URLs, no bare domains" rule, shared by the cluster/story gate and the
/// label/delta cleaners.
///
/// Detection lives in [`crate::common::link_safety`], the single source of truth shared with the
/// renderer's defang backstop. It flags both explicit URLs (`://`, `www.`, `mailto:`) **and bare host
/// shapes** (`databricks.com`, `github.com/acme/auth`): the original "bare domains are fine, escaping
/// keeps them inert" reasoning was wrong — escaping governs *our* `<a>` tags, but the receiving mail
/// client auto-linkifies a bare domain in displayed text on its own, which is exactly how a
/// hallucinated `acme.com` became a live link. A flagged summary costs one deterministic baseline; the
/// numeric finals of versions/ratios (`v2.0`, `9.5`) are spared, so true clean lines survive.
pub fn url_like_token(text: &str) -> Option<String> {
    crate::common::link_safety::first_linkable_token(text)
}

/// Whole-word, case-insensitive containment: `needle` (already lowercase) bounded by non-alphanumeric
/// edges in `haystack` (already lowercase). So "your" matches "your" but not "yourself", and "we"
/// doesn't fire inside "week". Multi-word needles (e.g. "game changing") match as a phrase. Boundaries
/// are tested on *chars* (Unicode `is_alphanumeric`), not raw bytes, so an adjacent multibyte
/// character ("caféyou") is not mistaken for a word boundary.
fn contains_word(haystack: &str, needle: &str) -> bool {
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(needle) {
        let i = start + pos;
        let before_ok = haystack[..i]
            .chars()
            .next_back()
            .is_none_or(|c| !c.is_alphanumeric());
        let after = i + needle.len();
        let after_ok = haystack[after..]
            .chars()
            .next()
            .is_none_or(|c| !c.is_alphanumeric());
        if before_ok && after_ok {
            return true;
        }
        // Advance past this occurrence (needles are non-empty), keeping byte-boundary alignment.
        start = after;
    }
    false
}

// ── The deterministic baseline (§3.4 fallback) ───────────────────────────────────────────────────

/// The deterministic baseline a rejected or never-generated summary degrades to: the extractive
/// cluster `title` as the headline, a templated one-liner as the tldr, banded `uncertain`. Always
/// true, never a hallucination — the §3.4/§8 graceful-degradation guarantee. `facts` is carried
/// through so the grounding survives even when generation is skipped.
pub fn baseline(title: &str, event_count: i32, sources: &[&str], facts: Facts) -> ClusterSummary {
    let headline = title.trim().chars().take(90).collect::<String>();
    let tldr_text = baseline_tldr(event_count, sources);
    ClusterSummary {
        headline,
        tldr: vec![TldrRun::Text {
            text: tldr_text.clone(),
        }],
        tldr_text,
        facts,
        band: Band::Uncertain,
    }
}

/// The templated baseline tldr (§3.4): a true, plain count-and-sources line — e.g. "3 updates across
/// GitHub, Slack." A single update with one source reads "1 update from GitHub."
fn baseline_tldr(event_count: i32, sources: &[&str]) -> String {
    let n = event_count.max(1);
    let unit = if n == 1 { "update" } else { "updates" };
    let mut srcs: Vec<&str> = sources.to_vec();
    srcs.sort_unstable();
    srcs.dedup();
    match srcs.len() {
        0 => format!("{n} {unit}."),
        1 => format!("{n} {unit} from {}.", srcs[0]),
        _ => format!("{n} {unit} across {}.", srcs.join(", ")),
    }
}

// ── Schema + prompts (§3.3, §3.6) ────────────────────────────────────────────────────────────────

/// The shared, prefix-cached system prompt (§3.6) — calm, plain, grounded, honestly hedged, with the
/// few-shot exemplars that carry what rules can't for a 3–4B model. A constant ⇒ near-free per call.
pub const SYSTEM_PROMPT: &str = r#"You turn given facts into one short line for a work digest.
You rephrase the facts. You add nothing.

1. Use only the facts and source text given. Every name, number, and date you write
   must be in the input. Not given -> leave it out.
2. Refer to people, repos, services, and CVEs only by the entity ids listed. Nothing more.
3. Each fact has "certainty". tentative -> use a hedge verb (suspected, appears to,
   reportedly, proposed). asserted -> say it plainly. Never change a fact's certainty.
4. Plain words. Active voice. Do not use: massive, huge, critical (unless in the
   source), game-changing, exciting, "!", "you", "we".
5. No web addresses at all: no URLs ("http://...", "www...") and no bare domains
   ("acme.com", "github.com/x"). Name sources plainly; the reader's interface shows
   the link.
6. Output only the JSON the schema asks for. No preamble.

EXAMPLES
facts: {event: deploy broke logins, state: resolved, certainty: asserted,
        repo: acme/auth, numbers: [12%, 40m], cause: bad config}
out: {"headline":"Auth logins broke after the token-rotation deploy",
      "tldr":[{"text":"A bad config in the rollout broke token validation in "},
              {"ref":"repo:acme/auth","surface":"acme/auth"},
              {"text":"; ~12% of logins failed for 40m until a rollback."}]}

facts: {event: SSRF advisory, state: investigating, certainty: tentative,
        cve: CVE-2026-2200, severity: high}
out: {"headline":"Suspected SSRF in the invoice PDF renderer",
      "tldr":[{"text":"A high-severity advisory, "},
              {"ref":"cve:CVE-2026-2200","surface":"CVE-2026-2200"},
              {"text":", appears to affect billing's PDF path; no patch yet."}]}"#;

/// The per-cluster user prompt (§3.6): the extracted facts + the closed entity-id set + the budgeted
/// source text, with the concrete ask. Short and concrete, over the §4 pre-distilled inputs.
pub fn user_prompt(facts: &Facts, source_text: &str) -> String {
    let entity_list = list_or_none(&facts.entities);
    let facts_json = serde_json::to_string(facts).unwrap_or_else(|_| "{}".to_string());
    format!(
        "facts: {facts_json}\n\
         allowed entity ids (use only these for refs): {entity_list}\n\
         source:\n{source_text}\n\n\
         Write: headline (<= 90 chars): the one most important thing. \
         tldr (2-4 sentences): what happened, the impact, the current state."
    )
}

/// The response JSON schema (§3.3) for `response_format: json_schema` — llama.cpp's GBNF token-masking
/// turns this into a grammar that **guarantees structurally valid JSON**. It does real work beyond
/// validity: `maxLength` on the headline (length control), and the tldr's entity `ref` constrained to
/// an **`enum` of the input `facts.entities`** — so the model can reference but never invent an entity
/// (the §6.2 structural faithfulness guarantee). An empty entity set drops the `ref` arm entirely.
pub fn response_schema(allowed_entities: &[String]) -> serde_json::Value {
    use serde_json::json;

    // The text run: { "text": "..." }.
    let text_run = json!({
        "type": "object",
        "properties": { "text": { "type": "string", "maxLength": 240 } },
        "required": ["text"],
        "additionalProperties": false
    });

    // The run is a text run, plus — only when there are grounded entities to reference — an entity-ref
    // run whose `ref` is the closed enum of `facts.entities`.
    let run_schema = if allowed_entities.is_empty() {
        text_run
    } else {
        let ref_run = json!({
            "type": "object",
            "properties": {
                "ref": { "type": "string", "enum": allowed_entities },
                "surface": { "type": "string", "maxLength": 80 }
            },
            "required": ["ref", "surface"],
            "additionalProperties": false
        });
        json!({ "oneOf": [text_run, ref_run] })
    };

    json!({
        "name": "cluster_summary",
        "strict": true,
        "schema": {
            "type": "object",
            "properties": {
                "headline": { "type": "string", "maxLength": 90 },
                "tldr": { "type": "array", "items": run_schema, "minItems": 1, "maxItems": 8 }
            },
            "required": ["headline", "tldr"],
            "additionalProperties": false
        }
    })
}

/// Concatenate a cluster's events into the budgeted source corpus fed to the model (§4 — the only tier
/// that touches raw text). Title + body per event, separated by blank lines, truncated to
/// `max_chars` (the §7 long-context cliff). Events are taken newest-first so a truncation keeps the
/// most recent context.
pub fn source_corpus(events: &[Event], max_chars: usize) -> String {
    let mut order: Vec<&Event> = events.iter().collect();
    order.sort_by(|a, b| b.event_time.cmp(&a.event_time).then(b.id.cmp(&a.id)));
    let mut out = String::new();
    // Track the char count as we append (each piece counted once) rather than re-scanning the whole
    // buffer per event — the latter is O(n²) on a many-event cluster.
    let mut len = 0usize;
    for e in order {
        if len >= max_chars {
            break;
        }
        if !out.is_empty() {
            out.push_str("\n\n");
            len += 2;
        }
        let title = e.title.trim();
        out.push_str(title);
        len += title.chars().count();
        if let Some(b) = &e.body {
            let b = b.trim();
            if !b.is_empty() {
                out.push('\n');
                out.push_str(b);
                len += 1 + b.chars().count();
            }
        }
    }
    if len > max_chars {
        out = out.chars().take(max_chars).collect();
    }
    out
}

// ── Phase C — Story cross-source synthesis (§2.2) ────────────────────────────────────────────────

/// The **member signature** that caches a story's synthesis (§2.2): SHA-256 over the *sorted* set of
/// member-cluster `summary_hash`es. Stories are id-forwarded and stable across fires, so this sig is
/// stable until membership/content actually moves — the synthesis is reused across fires for free and
/// regenerated only when a source is added/dropped or a member's content changes. Sorting makes it
/// order-independent of the caller. A member with no summary hash yet still contributes (its empty
/// slot is part of the signature), so adding the missing summary later moves the sig and triggers a
/// re-synthesis.
///
/// (The design's §2.2 sig also folds in the assigned `thread_id`; we key on member content alone —
/// the thread context barely affects the synthesis and this keeps Phase C decoupled from fire-time
/// thread-assignment, so a story moving threads does not itself force a re-synthesis. See the
/// `sweep_stories` deviation note.)
pub fn story_summary_sig(member_hashes: &[Option<Vec<u8>>]) -> Vec<u8> {
    const FIELD: u8 = 0x00;
    const NONE: u8 = 0x01; // marks a member with no summary hash yet, so it can't collide with empty
    let mut sorted: Vec<&Option<Vec<u8>>> = member_hashes.iter().collect();
    sorted.sort();
    let mut h = Sha256::new();
    for m in sorted {
        match m {
            Some(bytes) => h.update(bytes),
            None => h.update([NONE]),
        }
        h.update([FIELD]);
    }
    h.finalize().to_vec()
}

/// Fuse the member clusters' grounding [`Facts`] into the story's facts (§3.2 one level up): the
/// sorted union of their entities/numbers/dates (the closed `enum` the synthesis tldr's refs are still
/// constrained to — a hallucinated entity stays structurally impossible). `certainty` is the *weakest*
/// (any member `tentative` ⇒ the fused stance is `tentative`, so the synthesis keeps the hedge), and
/// `event_type`/`state` take the first member that has one (callers pass members newest-first, so the
/// freshest lifecycle wins). Pure + deterministic, unit-tested without a model.
pub fn synthesize_facts(members: &[ClusterSummary]) -> Facts {
    let mut entities: Vec<String> = Vec::new();
    let mut numbers: Vec<String> = Vec::new();
    let mut dates: Vec<String> = Vec::new();
    let mut certainty = Certainty::Asserted;
    let mut event_type: Option<String> = None;
    let mut state: Option<String> = None;
    for m in members {
        entities.extend(m.facts.entities.iter().cloned());
        numbers.extend(m.facts.numbers.iter().cloned());
        dates.extend(m.facts.dates.iter().cloned());
        if m.facts.certainty == Certainty::Tentative {
            certainty = Certainty::Tentative;
        }
        if event_type.is_none() {
            event_type.clone_from(&m.facts.event_type);
        }
        if state.is_none() {
            state.clone_from(&m.facts.state);
        }
    }
    dedup_sorted(&mut entities);
    dedup_sorted(&mut numbers);
    dedup_sorted(&mut dates);
    Facts {
        entities,
        event_type,
        state,
        certainty,
        numbers,
        dates,
    }
}

/// The concatenated grounding source for a story synthesis call (§4): the member clusters' precomputed
/// `headline` + `tldr_text` — **a handful of short summaries, never their raw events again** (the §4
/// "short inputs win" rule that keeps a 3–4B model in its faithful regime). The fused `Facts` are also
/// passed; this is the prose the gate checks numbers/dates against, and what the model rephrases.
pub fn story_member_corpus(members: &[ClusterSummary]) -> String {
    let mut out = String::new();
    for m in members {
        if !out.is_empty() {
            out.push_str("\n\n");
        }
        out.push_str(m.headline.trim());
        let tldr = m.tldr_text.trim();
        if !tldr.is_empty() {
            out.push('\n');
            out.push_str(tldr);
        }
    }
    out
}

/// The story-synthesis system prompt (§3.6) — the cross-source sibling of [`SYSTEM_PROMPT`], engineered
/// for a 3–4B model: it rewrites the *given member summaries* into one headline + tldr for the whole
/// happening, fuses without listing the sources, and keeps the same grounding/voice rules. A constant
/// ⇒ prefix-cached.
pub const STORY_SYSTEM_PROMPT: &str = r#"You are given several short summaries that are all the SAME happening, seen across different sources.
You write ONE headline and ONE tldr for the whole thing. You add nothing.

1. These summaries describe one event from different angles. Fuse them into a single view.
2. Use only the facts and text given. Every name, number, and date you write must be in the input.
3. Refer to people, repos, services, and CVEs only by the entity ids listed. Nothing more.
4. Do NOT list or name the sources ("across GitHub and Slack"). The interface shows them.
5. Each fact has "certainty". tentative -> hedge (suspected, appears to). asserted -> say it plainly.
6. Plain words. Active voice. Do not use: massive, huge, critical (unless in the source),
   game-changing, exciting, "!", "you", "we".
7. Don't paste raw URLs (no "http://...", no "www...").
8. Output only the JSON the schema asks for. No preamble.

EXAMPLE
members:
- A CVE advisory affects the billing PDF renderer.
- An incident PR disables remote asset fetching in acme/billing.
- Slack: "PDF export is down for some tenants".
out: {"headline":"SSRF advisory forces a billing PDF mitigation",
      "tldr":[{"text":"A high-severity advisory in "},
              {"ref":"repo:acme/billing","surface":"acme/billing"},
              {"text":"'s PDF path is being mitigated by disabling remote asset fetching."}]}"#;

/// The per-story synthesis user prompt: the member summaries (the §4 short fan-in), the closed
/// allowed-entity ids, and the thread label for context, with the concrete ask. Short and concrete
/// over the pre-distilled inputs, like [`user_prompt`].
pub fn story_user_prompt(facts: &Facts, members_text: &str, thread_label: Option<&str>) -> String {
    let entity_list = list_or_none(&facts.entities);
    let facts_json = serde_json::to_string(facts).unwrap_or_else(|_| "{}".to_string());
    let context = match thread_label {
        Some(l) if !l.trim().is_empty() => format!("thread: {}\n", l.trim()),
        _ => String::new(),
    };
    format!(
        "{context}facts: {facts_json}\n\
         allowed entity ids (use only these for refs): {entity_list}\n\
         member summaries:\n{members_text}\n\n\
         These are the same happening across sources. Write one headline (<= 90 chars) and one \
         tldr (2-4 sentences) for the whole thing. Do not list the sources."
    )
}

// ── Phase B — Thread label + delta (the context eyebrow, §2.3/§6.1) ──────────────────────────────

/// The readable "state of this thread" the LLM upgrade writes onto `thread.summary` (§2.3): just the
/// human-readable **label** ("Acme auth migration") that supersedes the deterministic auto-label
/// ("acme/auth +3") at render. The inert default (`'{}'` ⇒ [`is_empty`](Self::is_empty)) means no
/// upgrade has run, so the renderer falls back to `thread.label` (the deterministic baseline).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ThreadSummary {
    /// The LLM-upgraded readable label. Empty ⇒ render uses the deterministic `thread.label`.
    #[serde(default)]
    pub label: String,
}

impl ThreadSummary {
    /// True for the inert default — no label upgrade has run.
    pub fn is_empty(&self) -> bool {
        self.label.trim().is_empty()
    }
}

/// The deterministic thread auto-label (§2.3 baseline): a readable-ish name derived from the thread's
/// entity spine — the highest-priority entity's display value plus a `+N` for the rest ("acme/auth
/// +2"). Written every maintenance pass (cheap, recomputable), so the context eyebrow lights up even
/// with the `llm-summarization` feature off; the gated label sweep upgrades it to a prose label. Empty
/// for an empty spine (the eyebrow is then omitted, §6.1).
pub fn auto_label(entities: &[String]) -> String {
    if entities.is_empty() {
        return String::new();
    }
    let mut sorted: Vec<&String> = entities.iter().collect();
    sorted.sort_by(|a, b| {
        entity_priority(a)
            .cmp(&entity_priority(b))
            .then_with(|| a.cmp(b))
    });
    let head = entity_display(sorted[0]);
    let rest = entities.len() - 1;
    if rest == 0 {
        head.to_string()
    } else {
        format!("{head} +{rest}")
    }
}

/// Render-order priority of an entity namespace for the auto-label head (lower = preferred): a repo or
/// CVE names a thread better than a bare user or url. Unknown namespaces sort last. Splits the token
/// through [`identity::namespace`](crate::identity::namespace), the one owner of the `kind:value` parse.
fn entity_priority(token: &str) -> u8 {
    match crate::identity::namespace(token).map(|(ns, _)| ns) {
        Some("repo") => 0,
        Some("cve") => 1,
        Some("user") => 2,
        Some("domain") => 3,
        Some("url") => 4,
        _ => 5,
    }
}

/// The display value of a namespaced entity token: the part after the first `:` (so `repo:acme/auth` →
/// `acme/auth`, `cve:CVE-2026-1234` → `CVE-2026-1234`), or the whole token if it carries no namespace.
/// Uses [`identity::namespace`](crate::identity::namespace) so the `kind:value` parse lives in one place.
fn entity_display(token: &str) -> &str {
    crate::identity::namespace(token).map_or(token, |(_, v)| v)
}

/// The deterministic delta baseline (§5.2): a terse count flag of the stories that newly moved on this
/// thread since the last summarized appearance — "3 updates" / "1 update". `None` for nothing new (the
/// eyebrow then carries the label alone). The gated delta sweep upgrades this to a readable flag
/// ("staging cutover landed"); this is the always-true fallback.
pub fn auto_delta(new_story_count: usize) -> Option<String> {
    match new_story_count {
        0 => None,
        1 => Some("1 update".to_string()),
        n => Some(format!("{n} updates")),
    }
}

/// Max chars for a readable thread label (the §6.1 one-line eyebrow head). Matches the schema
/// `maxLength`; the cleaner clamps to it as defense in depth.
const LABEL_MAX: usize = 48;
/// Max chars for a delta flag (a few words, §6.1). Matches the schema `maxLength`.
const DELTA_MAX: usize = 48;
/// Max words for a delta flag (§3.6 "≤6 words: a flag, not a sentence").
const DELTA_MAX_WORDS: usize = 6;
/// Max chars for the big-picture lead (§2.4/§3.6 "1–2 sentences"). Matches the schema `maxLength`; the
/// cleaner rejects output exceeding it (defense in depth behind the grammar, degrading to the
/// deterministic lead — it does not truncate a half-sentence).
const LEAD_MAX: usize = 320;

/// Clean + gate a model-produced thread **label** (§3.6 backstop): trim, reject empties, over-length
/// (> [`LABEL_MAX`]), and the house-voice denylist (hype / second-person / `!`). `None` ⇒ the caller
/// keeps the deterministic auto-label. A label is a *name*, so there is no entity/number grounding
/// check here — only voice + length (the resolver's confidence band carries the identity doubt, §6.1).
pub fn clean_label(raw: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() || s.chars().count() > LABEL_MAX {
        return None;
    }
    if banned_word_in(s).is_some() || url_like_token(s).is_some() {
        return None;
    }
    Some(s.to_string())
}

/// Clean + gate a model-produced **delta** flag (§3.6): trim, strip any trailing end punctuation
/// ("staging cutover landed." → "…landed", since the delta carries none), then reject empties,
/// over-length (> [`DELTA_MAX`] chars or > [`DELTA_MAX_WORDS`] words), and the house-voice denylist.
/// `None` ⇒ the caller keeps the deterministic count delta.
pub fn clean_delta(raw: &str) -> Option<String> {
    let s = raw
        .trim()
        .trim_end_matches(['.', '!', '?', ',', ';'])
        .trim();
    if s.is_empty()
        || s.chars().count() > DELTA_MAX
        || s.split_whitespace().count() > DELTA_MAX_WORDS
    {
        return None;
    }
    if banned_word_in(s).is_some() || url_like_token(s).is_some() {
        return None;
    }
    Some(s.to_string())
}

/// The thread-label system prompt (§3.6) — engineered for a 3–4B model: name the persistent thread of
/// someone's work life in a few words, from the entity spine + a couple of recent headlines. A
/// constant ⇒ prefix-cached.
pub const LABEL_SYSTEM_PROMPT: &str = r#"You name a recurring thread of someone's work life in a few words.
The name is a short noun phrase a colleague would recognize: "Acme auth migration", "On-call rotation", "Billing rewrite".

1. 2-5 words. Title-style, no trailing punctuation.
2. Base it on the entities and recent headlines given. Use only what is given.
3. Plain words. Do not use: massive, huge, game-changing, exciting, "!", "you", "we".
4. Output only the JSON the schema asks for. No preamble.

EXAMPLES
entities: repo:acme/auth, user:dlewis | recent: "Auth outage traced to the token rotation deploy"
out: {"label":"Acme auth migration"}
entities: service:pagerduty | recent: "You're on call from Friday 18:00"
out: {"label":"On-call rotation"}"#;

/// The thread-delta system prompt (§3.6 / §5.2) — engineered for a 3–4B model: compress what *newly*
/// changed on a known thread into a terse flag, not a sentence.
pub const DELTA_SYSTEM_PROMPT: &str = r#"You write a SHORT flag of what newly changed on a thread the reader already knows.
It is a tag, not a sentence: "staging cutover landed", "reactivated", "3 follow-ups", "patch merged".

1. <= 6 words. No end punctuation.
2. Base it only on the new updates given. Use only what is given.
3. Plain words. Do not use: massive, huge, game-changing, exciting, "!", "you", "we".
4. Output only the JSON the schema asks for. No preamble.

EXAMPLES
thread: Acme auth migration | new: "Staging cutover completed; two follow-up tickets opened"
out: {"delta":"staging cutover landed"}
thread: Billing rewrite | new: "The dormant invoice-PDF work picked back up after the advisory"
out: {"delta":"reactivated"}"#;

/// The thread-label user prompt: the entity spine + a few recent headlines on the thread + the ask.
pub fn label_user_prompt(entities: &[String], recent_headlines: &[String]) -> String {
    format!(
        "entities: {}\n\
         recent headlines:\n{}\n\n\
         Name this thread in 2-5 words.",
        list_or_none(entities),
        bullet_list(recent_headlines),
    )
}

/// The thread-delta user prompt: the thread label + the new stories' headlines since the watermark.
pub fn delta_user_prompt(label: &str, new_headlines: &[String]) -> String {
    format!(
        "thread: {label}\n\
         new updates:\n{}\n\n\
         In <= 6 words, what newly changed? A flag, not a sentence. No end punctuation.",
        bullet_list(new_headlines),
    )
}

/// A `- item` bullet list for a prompt, or `(none)` when empty — so the model is told a section is
/// empty rather than inferring it from a blank.
fn bullet_list(items: &[String]) -> String {
    if items.is_empty() {
        return "(none)".to_string();
    }
    items
        .iter()
        .map(|h| format!("- {}", h.trim()))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The thread-label response schema — a single length-capped `label` string (no enums; a label is a
/// free phrase, gated for voice by [`clean_label`]).
pub fn label_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "name": "thread_label",
        "strict": true,
        "schema": {
            "type": "object",
            "properties": { "label": { "type": "string", "maxLength": LABEL_MAX } },
            "required": ["label"],
            "additionalProperties": false
        }
    })
}

/// The thread-delta response schema — a single length-capped `delta` string, gated by [`clean_delta`].
pub fn delta_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "name": "thread_delta",
        "strict": true,
        "schema": {
            "type": "object",
            "properties": { "delta": { "type": "string", "maxLength": DELTA_MAX } },
            "required": ["delta"],
            "additionalProperties": false
        }
    })
}

// ── Phase D — the authored big-picture lead (§2.4/§3.1) ──────────────────────────────────────────

/// Clean + gate a model-produced **big-picture lead** (§3.6 backstop), the one summary surface whose
/// call sits on the punctual path. Trim, reject empties / over-length (> [`LEAD_MAX`]), the house-voice
/// denylist (hype / second-person / `!`), and any raw URL. Plus a numeric-grounding check, mirroring
/// the §3.4 cluster gate one level up: every number/date-looking token in the lead must appear (as the
/// same token) in `grounding` — the concatenated selected headlines + thread labels the lead is
/// composed from — so the editor's note can rephrase the selected items but never invent a quantity.
/// `None` ⇒ the caller keeps the deterministic Phase-A lead. (No entity-`ref` check: the lead is plain
/// prose with no run-list — it *names* threads, it doesn't badge entities.)
pub fn clean_lead(raw: &str, grounding: &str) -> Option<String> {
    let s = raw.trim();
    if s.is_empty() || s.chars().count() > LEAD_MAX {
        return None;
    }
    if banned_word_in(s).is_some() || url_like_token(s).is_some() {
        return None;
    }
    // Numbers must be grounded in the inputs (the shared §3.4 token-equality check), so a number that
    // wasn't in any selected headline / thread label (nor the item-count grounding the caller appends)
    // can't reach the lead.
    if first_ungrounded_number(s, grounding).is_some() {
        return None;
    }
    Some(s.to_string())
}

/// The digest-lead system prompt (§3.6) — engineered for a 3–4B model exactly like the others: write
/// the one "big picture" opening over the selected headlines, name a thread or two, don't list every
/// item. A constant ⇒ prefix-cached. (The grounding it rephrases — the headlines/thread labels — is in
/// the user prompt; those inputs are themselves already gate-passed summaries, §4.)
pub const LEAD_SYSTEM_PROMPT: &str = r#"You write the one-line "big picture" that opens a work digest.
It sits under a greeting, so do NOT greet. One or two short sentences.

1. Say what dominated, then what else moved. Name one or two threads by name.
2. Do not list every item. Do not number them. A reader sees the full list below.
3. Use only the headlines and threads given. Every name, number, and date you write must be in them.
4. Plain words. Active voice. Do not use: massive, huge, critical (unless given),
   game-changing, exciting, "!", "you", "we".
5. Don't paste raw URLs. Name sources plainly.
6. Output only the JSON the schema asks for. No preamble.

EXAMPLES
threads: Acme auth migration, Billing rewrite
headlines:
- Auth outage traced to the token-rotation deploy
- SSRF advisory forces a billing PDF mitigation
- Staging cutover completed for the payments service
out: {"lead":"An auth outage from the token-rotation deploy led the day on the Acme auth migration, while a suspected SSRF pushed a billing PDF mitigation."}

threads: On-call rotation
headlines:
- Pager handoff to the EU team completed
out: {"lead":"A quiet stretch on the on-call rotation: the pager handed off to the EU team."}"#;

/// The digest-lead user prompt: the selected items' headlines (newest/top-ranked first) + the thread
/// names they advance, with the concrete ask. Short and concrete over the §4 pre-distilled inputs (the
/// headlines are already-gated summaries, never raw events), like [`user_prompt`].
pub fn lead_user_prompt(headlines: &[String], threads: &[String]) -> String {
    format!(
        "threads: {}\n\
         headlines:\n{}\n\n\
         Write the big-picture opening: 1-2 sentences, what dominated and what else moved. \
         Name one or two threads. Do not greet, and do not list every item.",
        list_or_none(threads),
        bullet_list(headlines),
    )
}

/// The digest-lead response schema — a single length-capped `lead` string (no enums; the lead is free
/// prose over the already-grounded headlines, gated for voice/length/grounding by [`clean_lead`]).
pub fn lead_schema() -> serde_json::Value {
    use serde_json::json;
    json!({
        "name": "digest_lead",
        "strict": true,
        "schema": {
            "type": "object",
            "properties": { "lead": { "type": "string", "maxLength": LEAD_MAX } },
            "required": ["lead"],
            "additionalProperties": false
        }
    })
}

// ── The sweep (gated) — walk the work queue, summarize best-effort ───────────────────────────────

/// What a summarization sweep did, for logs / metrics.
#[derive(Debug, Default, Clone, Copy)]
pub struct SummarizeStats {
    /// Clusters whose summary was (re)written this pass.
    pub summarized: usize,
    /// Clusters skipped because their content hash was unchanged (the cache hit).
    pub skipped: usize,
    /// Clusters left unsummarized because the model was unavailable — *not* persisted, so a later
    /// sweep retries once the sidecar recovers (rather than sticking at a baseline).
    pub unavailable: usize,
}

/// Run a best-effort cluster-summarization sweep over **public** clusters, in the no-subscriber RLS
/// context (so it can only touch shared rows, §3.5). Public summaries are generated once and shared by
/// every subscriber (the §5 multiplier saving). Only exists in a `llm-summarization` build — the
/// feature is the kill switch.
///
/// Best-effort by contract (`thread-layer.md` §3.1): a per-cluster failure degrades that cluster to
/// its deterministic baseline and the sweep continues; nothing here ever blocks or fails a digest.
#[cfg(feature = "llm-summarization")]
pub async fn sweep_public(
    pool: &sqlx::PgPool,
    cfg: &SummarizationConfig,
) -> anyhow::Result<SummarizeStats> {
    sweep(pool, &Scope::Public, cfg).await
}

/// Run a best-effort cluster-summarization sweep over one subscriber's **private** clusters, in their
/// RLS context (per-unit, stateless — no cross-tenant content in one call, §3.5). Only exists in a
/// `llm-summarization` build — the feature is the kill switch.
#[cfg(feature = "llm-summarization")]
pub async fn sweep_private(
    pool: &sqlx::PgPool,
    subscriber_id: uuid::Uuid,
    cfg: &SummarizationConfig,
) -> anyhow::Result<SummarizeStats> {
    sweep(pool, &Scope::Private(subscriber_id), cfg).await
}

#[cfg(feature = "llm-summarization")]
use crate::common::{db::ScopeCtx, scope::Scope};

/// One HTTP client (connection pool / TLS cache / resolver) per sweep, reused across every model call
/// in the pass rather than rebuilt per item — shared by all three sweeps (cluster / story / thread) so
/// client construction lives in one place. `what` names the sweep for the error context.
#[cfg(feature = "llm-summarization")]
pub(crate) fn build_summarize_http(
    cfg: &SummarizationConfig,
    what: &str,
) -> anyhow::Result<reqwest::Client> {
    use anyhow::Context;
    reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .build()
        .with_context(|| format!("build {what} summarization http client"))
}

/// The shared sweep body for both scopes (mirrors the public/private build split): find the clusters
/// whose content changed since (or were never) summarized, recompute each one's hash, and — only if it
/// actually moved — generate + gate + store a fresh summary. The model/prompt provenance gates a
/// corpus-wide re-summarize after an upgrade. The RLS context is derived from the scope by
/// [`ScopeCtx::for_scope`] (public → no-subscriber, private → owner), the single source of that mapping.
///
/// **Each DB step runs in its own short scoped transaction, with the model calls held *between* them
/// (never inside one).** A single sidecar call can take seconds, and a sweep walks up to
/// `max_per_sweep` clusters — so wrapping the whole loop in one transaction (as this once did) would
/// pin a connection `idle in transaction` for minutes, holding locks the public build and every other
/// writer to `cluster` then queue behind. That is exactly what surfaces as a multi-minute "slow
/// statement" on the trivial `store_summary` `UPDATE`. The model calls touch no DB, so they sit
/// comfortably outside any transaction.
#[cfg(feature = "llm-summarization")]
async fn sweep(
    pool: &sqlx::PgPool,
    scope: &Scope,
    cfg: &SummarizationConfig,
) -> anyhow::Result<SummarizeStats> {
    use crate::common::db::with_scope;
    use anyhow::Context;
    use tracing::Instrument;

    let ctx = ScopeCtx::for_scope(scope);
    let model = cfg.summary_model();
    let http = build_summarize_http(cfg, "cluster")?;

    // The work queue, loaded in its own short transaction.
    let due = with_scope(pool, ctx, {
        let scope = scope.clone();
        let model = model.clone();
        let limit = cfg.max_per_sweep;
        move |conn| {
            Box::pin(async move {
                store::clusters_needing_summary(conn, &scope, &model, limit)
                    .await
                    .context("load clusters needing summary")
            })
        }
    })
    .await?;
    // Record which sidecar this sweep targets and how much work it found, so an operator can correlate
    // any per-cluster `connect`/`timeout` warnings below with the configured endpoint (a misconfigured
    // `BULLETIN_LLM_BASE_URL` looks exactly like a down sidecar otherwise).
    tracing::debug!(
        base_url = %cfg.base_url,
        model = %model,
        request_timeout_s = cfg.request_timeout.as_secs(),
        due = due.len(),
        "summarization sweep starting"
    );

    let mut stats = SummarizeStats::default();
    for c in due {
        let events = with_scope(pool, ctx, {
            let scope = scope.clone();
            let source = c.source;
            let group_key = c.group_key.clone();
            move |conn| {
                Box::pin(async move {
                    crate::cluster::store::list_group_events(conn, &scope, source, &group_key)
                        .await
                        .context("load cluster events for summary")
                })
            }
        })
        .await?;
        if events.is_empty() {
            continue;
        }
        let hash = summary_hash(&events);
        // The exact re-check behind the cheap SQL gate: content unchanged (and same model) ⇒ the
        // cached summary still holds, so just bump the watermark and skip the model call.
        if c.summary_hash.as_deref() == Some(hash.as_slice()) {
            metric::cache("cluster", true);
            let cid = c.id;
            let _ = with_scope(pool, ctx, move |conn| {
                Box::pin(async move {
                    store::touch_summarized(conn, cid)
                        .await
                        .context("touch summarized watermark")
                })
            })
            .await;
            stats.skipped += 1;
            continue;
        }
        metric::cache("cluster", false);
        // `None` ⇒ the model was unavailable: leave the cluster unsummarized (don't advance
        // `summarized_at`) so a later sweep retries once the sidecar recovers, rather than freezing it
        // at a baseline until its content next changes. A gate rejection still returns `Some(baseline)`
        // — that is a stable, content-derived result worth caching. A span so the best-effort warnings
        // inside `summarize_cluster` (model/comprehension call failed) carry *which* cluster they were
        // for — the failure is logged at the call site where the error type is known, but the identity
        // lives out here. The call holds no transaction.
        let span = tracing::debug_span!(
            "summarize_cluster",
            cluster_id = %c.id,
            source = c.source.as_str(),
        );
        match client::summarize_cluster(cfg, &http, &c.title, &events)
            .instrument(span)
            .await
        {
            Some(summary) => {
                let cid = c.id;
                let model = model.clone();
                with_scope(pool, ctx, move |conn| {
                    Box::pin(async move {
                        store::store_summary(conn, cid, &summary, &hash, &model)
                            .await
                            .context("store cluster summary")
                    })
                })
                .await?;
                stats.summarized += 1;
            }
            None => stats.unavailable += 1,
        }
    }
    Ok(stats)
}

/// Run a best-effort **story cross-source synthesis** pass (Phase C, §2.2) for one subscriber, in
/// their RLS context. Walks the stories whose membership/content changed, recomputes each one's member
/// signature from the member clusters' `summary_hash`es, and — only if it moved (or a model upgrade) —
/// synthesizes a fused summary from the **member cluster summaries** (never their raw events, §4),
/// cached by the signature so a stable story is reused for free across fires. Best-effort by contract:
/// a per-story failure leaves it un-synthesized (fire-time falls back to the representative cluster,
/// §2.2 cold-start) and the pass continues. Only exists in a `llm-summarization` build.
///
/// (Deviation from §2.2: the member signature is keyed on member content alone — `thread_id` is not
/// folded in, so a story moving threads doesn't itself force a re-synthesis. The synthesis quality
/// barely depends on the thread context, and this keeps Phase C decoupled from fire-time
/// thread-assignment. Revisit if cross-thread restatement proves valuable.)
#[cfg(feature = "llm-summarization")]
pub async fn sweep_stories(
    pool: &sqlx::PgPool,
    subscriber_id: uuid::Uuid,
    cfg: &SummarizationConfig,
) -> anyhow::Result<SummarizeStats> {
    use crate::common::db::with_scope;
    use anyhow::Context;

    let model = cfg.summary_model();
    let http = build_summarize_http(cfg, "story-synthesis")?;
    let ctx = ScopeCtx::Subscriber(subscriber_id);

    // Same transaction discipline as `sweep`: each DB step is its own short scoped transaction and the
    // synthesis model calls sit between them, so a slow sidecar never pins a connection in a long-held
    // transaction (the slow-`UPDATE` / lock-contention failure mode).
    let due = with_scope(pool, ctx, {
        let model = model.clone();
        let limit = cfg.max_per_sweep;
        move |conn| {
            Box::pin(async move {
                store::stories_needing_summary(conn, subscriber_id, &model, limit)
                    .await
                    .context("load stories needing synthesis")
            })
        }
    })
    .await?;

    let mut stats = SummarizeStats::default();
    for s in due {
        let members = with_scope(pool, ctx, {
            let cluster_ids = s.cluster_ids.clone();
            move |conn| {
                Box::pin(async move {
                    store::load_member_summaries(conn, &cluster_ids)
                        .await
                        .context("load story member summaries")
                })
            }
        })
        .await?;
        // A cross-source synthesis needs ≥2 members *and* at least one with a real cluster summary to
        // fuse; otherwise fire-time already renders the representative cluster identically — skip the
        // model call, just advance the watermark so it isn't re-flagged.
        let has_content = members.iter().any(|m| !m.summary.is_empty());
        if members.len() < 2 || !has_content {
            let sid = s.id;
            let model = model.clone();
            let _ = with_scope(pool, ctx, move |conn| {
                Box::pin(async move {
                    store::touch_story_summarized(conn, sid, &model)
                        .await
                        .context("touch story summarized")
                })
            })
            .await;
            stats.skipped += 1;
            continue;
        }

        let hashes: Vec<Option<Vec<u8>>> = members.iter().map(|m| m.summary_hash.clone()).collect();
        let sig = story_summary_sig(&hashes);
        // Exact re-check behind the cheap SQL gate: signature unchanged *and* same model ⇒ the cached
        // synthesis still holds, so just advance the watermark and skip the model call.
        if s.summary_sig.as_deref() == Some(sig.as_slice())
            && s.summary_model.as_deref() == Some(model.as_str())
        {
            metric::cache("story", true);
            let sid = s.id;
            let model = model.clone();
            let _ = with_scope(pool, ctx, move |conn| {
                Box::pin(async move {
                    store::touch_story_summarized(conn, sid, &model)
                        .await
                        .context("touch story summarized")
                })
            })
            .await;
            stats.skipped += 1;
            continue;
        }
        metric::cache("story", false);

        let summaries: Vec<ClusterSummary> = members.into_iter().map(|m| m.summary).collect();
        match client::synthesize_story(cfg, &http, &summaries, None).await {
            Some(summary) => {
                let sid = s.id;
                let model = model.clone();
                with_scope(pool, ctx, move |conn| {
                    Box::pin(async move {
                        store::store_story_summary(conn, sid, &summary, &sig, &model)
                            .await
                            .context("store story summary")
                    })
                })
                .await?;
                stats.summarized += 1;
            }
            None => stats.unavailable += 1,
        }
    }
    Ok(stats)
}

/// Run a best-effort **thread label + delta** pass (Phase B, §2.3/§5.2) for one subscriber, in their
/// RLS context. For each non-archived thread due for a pass, it upgrades the deterministic auto-label
/// to a readable one (stored on `thread.summary`, leaving `thread.label` as the baseline beneath) and
/// composes the §5.2 delta flag from the stories that newly landed since `delta_through` — both from
/// the precomputed story/cluster headlines, never raw events (§4). Best-effort: a per-thread failure
/// keeps the deterministic label/count-delta and the pass continues. Only exists in a
/// `llm-summarization` build.
#[cfg(feature = "llm-summarization")]
pub async fn sweep_thread_labels(
    pool: &sqlx::PgPool,
    subscriber_id: uuid::Uuid,
    cfg: &SummarizationConfig,
) -> anyhow::Result<SummarizeStats> {
    use crate::common::db::with_scope;
    use anyhow::Context;

    /// How far the per-thread story scan reaches — bounds the work *and* the accuracy of the delta's
    /// new-story count: a thread with more new stories than this saturates the count (rendered "N+").
    const STORY_SCAN_LIMIT: i64 = 30;
    /// How many headlines actually feed a model call (label or delta) — the §4 short-input fan-in cap,
    /// independent of the scan above.
    const FANIN_LIMIT: usize = 6;

    let model = cfg.summary_model();
    let http = build_summarize_http(cfg, "thread-label")?;
    let ctx = ScopeCtx::Subscriber(subscriber_id);

    // Same transaction discipline as `sweep`: each DB step is its own short scoped transaction and the
    // label/delta model calls sit between them, so a slow sidecar never pins a connection in a
    // long-held transaction (the slow-`UPDATE` / lock-contention failure mode).
    let due = with_scope(pool, ctx, {
        let model = model.clone();
        let limit = cfg.max_per_sweep;
        move |conn| {
            Box::pin(async move {
                store::threads_needing_summary(conn, subscriber_id, &model, limit)
                    .await
                    .context("load threads needing label/delta")
            })
        }
    })
    .await?;

    let mut stats = SummarizeStats::default();
    for t in due {
        // The recent stories on this thread's spine (newest-first): the label inputs, and (filtered by
        // the prior watermark) the delta's "what newly changed".
        let recent = with_scope(pool, ctx, {
            let entities = t.entities.clone();
            move |conn| {
                Box::pin(async move {
                    store::thread_recent_stories(conn, subscriber_id, &entities, STORY_SCAN_LIMIT)
                        .await
                        .context("load thread recent stories")
                })
            }
        })
        .await?;
        let since = t
            .delta_through
            .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC);
        // All stories newer than the watermark (up to the scan limit), so the count is accurate for the
        // deterministic delta; the LLM fan-in is capped separately to FANIN_LIMIT.
        let new: Vec<&store::ThreadStory> = recent
            .iter()
            .filter(|s| s.last_event_time > since)
            .collect();
        let new_count = new.len();
        // The count is exact unless *every* scanned story is new (then more may lie beyond the scan
        // window, so the deterministic delta renders "N+").
        let saturated = new_count as i64 == STORY_SCAN_LIMIT;
        let new_headlines: Vec<String> = new
            .iter()
            .take(FANIN_LIMIT)
            .map(|s| s.headline.clone())
            .collect();

        // Label: (re)generate when missing, after a model change, *or* when new stories landed — the
        // thread's subject can drift as its spine grows, so a label that never refreshes would go stale
        // against the (always-recomputed) deterministic baseline. Keep the prior readable label on a
        // gate/transport miss; never downgrade. The model call holds no transaction.
        let need_label = t.summary.is_empty()
            || t.summary_model.as_deref() != Some(model.as_str())
            || new_count > 0;
        let mut summary = t.summary.clone();
        if need_label {
            let label_headlines: Vec<String> = recent
                .iter()
                .take(FANIN_LIMIT)
                .map(|s| s.headline.clone())
                .collect();
            if !label_headlines.is_empty() {
                if let Some(label) =
                    client::label_thread(cfg, &http, &t.entities, &label_headlines).await
                {
                    summary = ThreadSummary { label };
                }
            }
        }

        // Delta: the LLM flag over the new stories, falling back to the deterministic count ("N+" when
        // the scan saturated). When *nothing* is new, keep the prior delta rather than clearing it — a
        // model-only re-fire (no new stories) must not wipe a valid flag.
        let delta = if new_headlines.is_empty() {
            t.delta.clone()
        } else {
            let label_for_delta = if summary.label.trim().is_empty() {
                auto_label(&t.entities)
            } else {
                summary.label.clone()
            };
            let count_delta = if saturated {
                Some(format!("{new_count}+ updates"))
            } else {
                auto_delta(new_count)
            };
            client::delta_thread(cfg, &http, &label_for_delta, &new_headlines)
                .await
                .or(count_delta)
        };

        // The watermark the delta now covers = the thread's last story time (so an unchanged thread
        // isn't re-flagged); stored as-is (including NULL) to keep the due-gate stable.
        let tid = t.id;
        let last_story_time = t.last_story_time;
        let model = model.clone();
        with_scope(pool, ctx, move |conn| {
            Box::pin(async move {
                store::store_thread_summary(
                    conn,
                    tid,
                    &summary,
                    delta.as_deref(),
                    last_story_time,
                    &model,
                )
                .await
                .context("store thread summary")
            })
        })
        .await?;
        stats.summarized += 1;
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::fingerprint::Fingerprint;
    use crate::common::kind::{ContentKind, SourceKind};
    use crate::common::scope::Scope;
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    fn ev(id: u128, secs: i64, title: &str, body: Option<&str>, entities: &[&str]) -> Event {
        Event {
            id: Uuid::from_u128(id),
            fingerprint: Fingerprint([0u8; 32]),
            source: SourceKind::Github,
            scope: Scope::Public,
            event_time: Utc.timestamp_opt(secs, 0).single().unwrap(),
            title: title.to_owned(),
            body: body.map(str::to_owned),
            links: vec!["https://example.com/x".to_owned()],
            group_key: "g".to_owned(),
            content_kind: ContentKind::Longform,
            entities: entities.iter().map(|s| s.to_string()).collect(),
            severity_hint: None,
            ingest_time: Utc.timestamp_opt(secs, 0).single().unwrap(),
            raw: None,
        }
    }

    #[test]
    fn hash_is_order_independent_and_content_sensitive() {
        // `Event` isn't `Clone`, so build fresh instances per call rather than reusing.
        let a = || ev(1, 100, "first", Some("body a"), &["repo:acme/auth"]);
        let b = || ev(2, 200, "second", Some("body b"), &["user:dlewis"]);
        let h1 = summary_hash(&[a(), b()]);
        let h2 = summary_hash(&[b(), a()]); // reversed input
        assert_eq!(h1, h2, "hash must not depend on caller order");

        let changed = ev(2, 200, "changed", Some("body b"), &["user:dlewis"]);
        let h3 = summary_hash(&[a(), changed]);
        assert_ne!(h1, h3, "a content change must move the hash");
    }

    #[test]
    fn extract_facts_unions_entities_and_mines_numbers_dates() {
        let events = [
            ev(
                1,
                100,
                "Auth broke at 14:02",
                Some("12% of logins failed for 40m"),
                &["repo:acme/auth"],
            ),
            ev(
                2,
                200,
                "Rollback on 2026-06-14",
                None,
                &["user:dlewis", "repo:acme/auth"],
            ),
        ];
        let facts = extract_facts(&events);
        assert_eq!(facts.entities, vec!["repo:acme/auth", "user:dlewis"]); // sorted + deduped
                                                                           // The miner keeps digit runs + glue punctuation (`%`), dropping unit-suffix letters: "40m" → "40".
        assert!(facts.numbers.contains(&"12%".to_string()));
        assert!(facts.numbers.contains(&"40".to_string()));
        assert!(facts.dates.contains(&"14:02".to_string())); // ':' → routed to dates
        assert!(facts.dates.contains(&"2026-06-14".to_string()));
        assert_eq!(facts.certainty, Certainty::Asserted); // neutral default until comprehension lands
    }

    #[test]
    fn gate_passes_a_grounded_summary() {
        let facts = Facts {
            entities: vec!["repo:acme/auth".to_string()],
            numbers: vec!["12%".to_string(), "40m".to_string()],
            ..Facts::default()
        };
        let mut s = ClusterSummary {
            headline: "Auth logins broke after the deploy".to_string(),
            tldr: vec![
                TldrRun::Text {
                    text: "A bad config broke validation in ".to_string(),
                },
                TldrRun::Ref {
                    entity: "repo:acme/auth".to_string(),
                    surface: "acme/auth".to_string(),
                },
                TldrRun::Text {
                    text: "; 12% of logins failed for 40m.".to_string(),
                },
            ],
            ..Default::default()
        };
        s.rebuild_tldr_text();
        assert!(faithful(&s, &facts, "").is_ok());
    }

    #[test]
    fn gate_rejects_ungrounded_entity_number_and_hype() {
        let facts = Facts {
            entities: vec!["repo:acme/auth".to_string()],
            numbers: vec!["12%".to_string()],
            ..Facts::default()
        };

        // Ungrounded entity ref.
        let mut bad_entity = ClusterSummary {
            headline: "x".to_string(),
            tldr: vec![TldrRun::Ref {
                entity: "user:eve".to_string(),
                surface: "Eve".to_string(),
            }],
            ..Default::default()
        };
        bad_entity.rebuild_tldr_text();
        assert!(matches!(
            faithful(&bad_entity, &facts, ""),
            Err(GateViolation::UngroundedEntity(_))
        ));

        // Ungrounded number not in facts nor source.
        let bad_num = ClusterSummary {
            headline: "99% of logins failed".to_string(),
            tldr_text: "99% of logins failed".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            faithful(&bad_num, &facts, "some source text"),
            Err(GateViolation::UngroundedNumber(_))
        ));

        // ...but the same number is fine when present in the source text.
        let ok_num = ClusterSummary {
            headline: "99% of logins failed".to_string(),
            tldr_text: "99% of logins failed".to_string(),
            ..Default::default()
        };
        assert!(faithful(&ok_num, &facts, "report: 99% of logins failed").is_ok());

        // Token-equality, not substring: "40" must NOT be grounded by a source/fact "4000".
        let substring_facts = Facts {
            numbers: vec!["4000".to_string()],
            ..Facts::default()
        };
        let sub = ClusterSummary {
            headline: "40 were affected".to_string(),
            tldr_text: "40 were affected".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            faithful(&sub, &substring_facts, "deployed 4000 servers"),
            Err(GateViolation::UngroundedNumber(_))
        ));

        // Banned hype word.
        let hype = ClusterSummary {
            headline: "A massive outage".to_string(),
            tldr_text: "A massive outage".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            faithful(&hype, &facts, ""),
            Err(GateViolation::BannedWord(_))
        ));
    }

    #[test]
    fn contains_word_is_whole_word() {
        assert!(contains_word("a massive outage", "massive"));
        assert!(!contains_word("the week ahead", "we")); // not inside a word
        assert!(!contains_word("yourself did it", "your"));
        assert!(contains_word("is this your fault", "your"));
        // A multibyte char adjacent to the needle is a real letter, not a word boundary.
        assert!(!contains_word("caféyou", "you"));
        assert!(contains_word("café you win", "you")); // standalone after the accented word
                                                       // Multi-word needle still matches as a phrase.
        assert!(contains_word("a game changing release", "game changing"));
    }

    #[test]
    fn url_like_token_catches_urls_and_bare_domains() {
        // Explicit URL forms: a scheme, or a www./mailto: prefix.
        assert!(url_like_token("see https://www.databricks.com/x for more").is_some());
        assert!(url_like_token("reach us at www.example.org").is_some());
        assert!(url_like_token("mail mailto:ops@example.com now").is_some());
        // Case-insensitive prefix; trailing punctuation doesn't hide a scheme.
        assert!(url_like_token("(see HTTPS://example.com).").is_some());
        assert!(url_like_token("WWW.example.com").is_some());

        // Bare domains are now caught too — a mail client auto-linkifies them in displayed text, so a
        // named source written as a domain is the very leak we're closing.
        for leak in [
            "published on databricks.com today",
            "at github.com/acme/auth/pull/1",
            "(see status.claude.com).",
            "the Booking.com outage",
            "the fix landed in main.rs after review",
        ] {
            assert!(
                url_like_token(leak).is_some(),
                "missed a bare domain in: {leak}"
            );
        }

        // Spared: versions, ratios, and abbreviations a developer digest is full of stay clean, so a
        // true line is never thrown away over a non-link.
        for ok in [
            "rated 9.5/10 by users",
            "a 24/7 on-call rotation",
            "shipped in v2.0 this week",
            "see, e.g., the changelog",
        ] {
            assert!(url_like_token(ok).is_none(), "false positive on: {ok}");
        }
    }

    #[test]
    fn gate_rejects_a_url_or_a_bare_domain() {
        let facts = Facts::default();
        let leaked = ClusterSummary {
            headline: "Databricks launches LTAP".to_string(),
            tldr_text: "Databricks announced LTAP. https://databricks.com/ltap".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            faithful(&leaked, &facts, "Databricks announced LTAP"),
            Err(GateViolation::UrlInProse(_))
        ));

        // A bare domain named in prose is now rejected too — the client would linkify it.
        let bare = ClusterSummary {
            headline: "Databricks launches LTAP".to_string(),
            tldr_text: "Databricks announced LTAP on databricks.com.".to_string(),
            ..Default::default()
        };
        assert!(matches!(
            faithful(&bare, &facts, "Databricks announced LTAP"),
            Err(GateViolation::UrlInProse(_))
        ));
    }

    #[test]
    fn baseline_is_true_and_uncertain() {
        let b = baseline("Title here", 3, &["github", "slack"], Facts::default());
        assert_eq!(b.headline, "Title here");
        assert_eq!(b.tldr_text, "3 updates across github, slack.");
        assert_eq!(b.band, Band::Uncertain);
        assert!(!b.is_empty());

        let single = baseline("T", 1, &["github"], Facts::default());
        assert_eq!(single.tldr_text, "1 update from github.");
    }

    #[test]
    fn empty_summary_round_trips_through_json() {
        // The '{}' jsonb default must deserialize to the inert empty summary.
        let s: ClusterSummary = serde_json::from_str("{}").unwrap();
        assert!(s.is_empty());
        assert_eq!(s.band, Band::Uncertain);
    }

    #[test]
    fn tldr_run_untagged_serde_round_trips() {
        let runs = vec![
            TldrRun::Text {
                text: "in ".to_string(),
            },
            TldrRun::Ref {
                entity: "repo:acme/auth".to_string(),
                surface: "acme/auth".to_string(),
            },
        ];
        let json = serde_json::to_string(&runs).unwrap();
        let back: Vec<TldrRun> = serde_json::from_str(&json).unwrap();
        assert_eq!(runs, back);
        // The ref run keeps its "ref" key (not "entity").
        assert!(json.contains("\"ref\":\"repo:acme/auth\""));
    }

    #[test]
    fn schema_constrains_refs_to_the_entity_enum() {
        let schema = response_schema(&["repo:acme/auth".to_string(), "cve:CVE-2026-1".to_string()]);
        let s = serde_json::to_string(&schema).unwrap();
        assert!(s.contains("cluster_summary"));
        assert!(s.contains("\"maxLength\":90")); // headline length control
        assert!(s.contains("repo:acme/auth")); // ref enum carries the grounded entities
                                               // With no entities, the ref arm is dropped (text-only runs).
        let empty = response_schema(&[]);
        let es = serde_json::to_string(&empty).unwrap();
        assert!(!es.contains("\"enum\""));
    }

    #[test]
    fn apply_comprehension_validates_and_folds() {
        // Valid, in-vocab classification folds onto the skeleton.
        let mut facts = Facts {
            entities: vec!["repo:acme/auth".to_string()],
            ..Facts::default()
        };
        apply_comprehension(
            &mut facts,
            &Comprehension {
                analysis: "deploy broke logins, resolved".to_string(),
                certainty: Certainty::Asserted,
                event_type: "incident".to_string(),
                state: "resolved".to_string(),
            },
        );
        assert_eq!(facts.event_type.as_deref(), Some("incident"));
        assert_eq!(facts.state.as_deref(), Some("resolved"));
        assert_eq!(facts.certainty, Certainty::Asserted);
        assert_eq!(facts.entities, vec!["repo:acme/auth"]); // skeleton untouched

        // `other`/`none` and out-of-vocab values leave the field unset (no misleading type/state), but
        // certainty (tentative) still applies — it drives the hedge.
        let mut neutral = Facts::default();
        apply_comprehension(
            &mut neutral,
            &Comprehension {
                analysis: String::new(),
                certainty: Certainty::Tentative,
                event_type: "other".to_string(),
                state: "bogus".to_string(),
            },
        );
        assert_eq!(neutral.event_type, None);
        assert_eq!(neutral.state, None);
        assert_eq!(neutral.certainty, Certainty::Tentative);
    }

    #[test]
    fn comprehension_schema_constrains_vocab_and_scratchpad_first() {
        let s = serde_json::to_string(&comprehension_schema()).unwrap();
        assert!(s.contains("comprehension"));
        // Closed vocab reaches the schema as enums.
        assert!(s.contains("incident") && s.contains("advisory"));
        assert!(s.contains("resolved") && s.contains("investigating"));
        assert!(s.contains("asserted") && s.contains("tentative"));
        // All four fields required (analysis must always be produced).
        assert!(s.contains("analysis"));
        // The scratchpad sorts first among the keys (serde_json has no preserve_order, and llama.cpp
        // orders object properties lexically): `analysis` precedes the classification keys.
        let a = s.find("analysis").unwrap();
        for key in ["certainty", "event_type", "state"] {
            assert!(a < s.find(key).unwrap(), "analysis must precede {key}");
        }
    }

    #[test]
    fn comprehension_output_deserializes_tolerantly() {
        let c: Comprehension = serde_json::from_str(
            r#"{"analysis":"x","certainty":"tentative","event_type":"advisory","state":"investigating"}"#,
        )
        .unwrap();
        assert_eq!(c.certainty, Certainty::Tentative);
        assert_eq!(c.event_type, "advisory");
        // Missing fields fall back to defaults rather than failing the parse.
        let empty: Comprehension = serde_json::from_str("{}").unwrap();
        assert_eq!(empty.certainty, Certainty::Asserted);
        assert!(empty.event_type.is_empty());
    }

    #[test]
    fn comprehend_user_prompt_carries_grounding() {
        let facts = Facts {
            entities: vec!["repo:acme/auth".to_string()],
            numbers: vec!["12%".to_string()],
            ..Facts::default()
        };
        let p = comprehend_user_prompt(&facts, "the source");
        assert!(p.contains("repo:acme/auth"));
        assert!(p.contains("12%"));
        assert!(p.contains("the source"));
    }

    #[test]
    fn source_corpus_is_budgeted_newest_first() {
        let events = [
            ev(1, 100, "OLD", Some("old body"), &[]),
            ev(2, 200, "NEW", Some("new body"), &[]),
        ];
        let full = source_corpus(&events, 4000);
        assert!(full.starts_with("NEW")); // newest first
        let clipped = source_corpus(&events, 3);
        assert_eq!(clipped.chars().count(), 3);
    }

    #[test]
    fn summary_model_string() {
        let cfg = SummarizationConfig::default();
        assert_eq!(cfg.summary_model(), "qwen3.5-4b-instruct@5");
    }

    // ── Phase C — story synthesis ────────────────────────────────────────────────────────────────

    fn member(
        headline: &str,
        entities: &[&str],
        numbers: &[&str],
        certainty: Certainty,
    ) -> ClusterSummary {
        let mut m = ClusterSummary {
            headline: headline.to_string(),
            tldr: vec![TldrRun::Text {
                text: format!("{headline} body."),
            }],
            facts: Facts {
                entities: entities.iter().map(|s| s.to_string()).collect(),
                numbers: numbers.iter().map(|s| s.to_string()).collect(),
                certainty,
                ..Facts::default()
            },
            band: Band::Confirmed,
            ..Default::default()
        };
        m.rebuild_tldr_text();
        m
    }

    #[test]
    fn story_sig_is_order_independent_and_member_sensitive() {
        let a = Some(vec![1u8, 2, 3]);
        let b = Some(vec![4u8, 5, 6]);
        // Member order does not matter (the sig sorts).
        assert_eq!(
            story_summary_sig(&[a.clone(), b.clone()]),
            story_summary_sig(&[b.clone(), a.clone()]),
        );
        // A member gaining its summary hash (None → Some) moves the sig → re-synthesis.
        assert_ne!(
            story_summary_sig(&[a.clone(), None]),
            story_summary_sig(&[a, b]),
        );
    }

    #[test]
    fn synthesize_facts_unions_and_weakens_certainty() {
        let members = [
            member(
                "Advisory",
                &["cve:CVE-2026-1", "repo:acme/billing"],
                &["high"],
                Certainty::Asserted,
            ),
            member(
                "Incident PR",
                &["repo:acme/billing"],
                &["12%"],
                Certainty::Tentative,
            ),
        ];
        let facts = synthesize_facts(&members);
        // Entities/numbers are the sorted, deduped union.
        assert_eq!(facts.entities, vec!["cve:CVE-2026-1", "repo:acme/billing"]);
        assert!(facts.numbers.contains(&"12%".to_string()));
        // Any tentative member ⇒ the fused stance is tentative (keeps the hedge).
        assert_eq!(facts.certainty, Certainty::Tentative);
    }

    #[test]
    fn story_member_corpus_and_prompt_carry_summaries() {
        let members = [
            member(
                "First headline",
                &["repo:acme/auth"],
                &[],
                Certainty::Asserted,
            ),
            member("Second headline", &[], &[], Certainty::Asserted),
        ];
        let corpus = story_member_corpus(&members);
        assert!(corpus.contains("First headline"));
        assert!(corpus.contains("Second headline"));
        let facts = synthesize_facts(&members);
        let p = story_user_prompt(&facts, &corpus, Some("Acme auth migration"));
        assert!(p.contains("Acme auth migration"));
        assert!(p.contains("repo:acme/auth"));
        assert!(p.contains("Do not list the sources"));
    }

    // ── Phase B — thread label + delta ───────────────────────────────────────────────────────────

    #[test]
    fn auto_label_picks_highest_priority_and_counts_rest() {
        // repo outranks user/cve; the head shows its display value, the rest a "+N".
        assert_eq!(
            auto_label(&[
                "user:dlewis".to_string(),
                "repo:acme/auth".to_string(),
                "cve:CVE-2026-1".to_string()
            ]),
            "acme/auth +2"
        );
        // A single entity: just its display value, no "+N".
        assert_eq!(auto_label(&["cve:CVE-2026-9".to_string()]), "CVE-2026-9");
        // Empty spine ⇒ empty label (eyebrow omitted).
        assert_eq!(auto_label(&[]), "");
    }

    #[test]
    fn auto_delta_is_a_count_flag() {
        assert_eq!(auto_delta(0), None);
        assert_eq!(auto_delta(1).as_deref(), Some("1 update"));
        assert_eq!(auto_delta(4).as_deref(), Some("4 updates"));
    }

    #[test]
    fn clean_label_gates_voice_and_length() {
        assert_eq!(
            clean_label("  Acme auth migration  ").as_deref(),
            Some("Acme auth migration")
        );
        assert!(clean_label("").is_none());
        assert!(clean_label("a massive incident sprawling thread").is_none()); // banned word
        assert!(clean_label(&"x".repeat(LABEL_MAX + 1)).is_none()); // over length
    }

    #[test]
    fn clean_delta_strips_punctuation_and_caps_words() {
        // Trailing punctuation is stripped (a delta carries none).
        assert_eq!(
            clean_delta("staging cutover landed.").as_deref(),
            Some("staging cutover landed")
        );
        assert_eq!(clean_delta("reactivated").as_deref(), Some("reactivated"));
        // > 6 words is a sentence, not a flag → rejected.
        assert!(clean_delta("the staging cutover finally landed after a long delay").is_none());
        // Hype / second person rejected.
        assert!(clean_delta("huge change").is_none());
        assert!(clean_delta("").is_none());
    }

    #[test]
    fn label_and_delta_schemas_constrain_length() {
        let l = serde_json::to_string(&label_schema()).unwrap();
        assert!(l.contains("thread_label") && l.contains("\"maxLength\":48"));
        let d = serde_json::to_string(&delta_schema()).unwrap();
        assert!(d.contains("thread_delta") && d.contains("\"maxLength\":48"));
        // The prompts carry the grounding inputs.
        let lp = label_user_prompt(
            &["repo:acme/auth".to_string()],
            &["Auth outage".to_string()],
        );
        assert!(lp.contains("repo:acme/auth") && lp.contains("Auth outage"));
        let dp = delta_user_prompt("Acme auth migration", &["Cutover landed".to_string()]);
        assert!(dp.contains("Acme auth migration") && dp.contains("Cutover landed"));
    }

    // ── Phase D — the authored big-picture lead ───────────────────────────────────────────────────

    #[test]
    fn clean_lead_gates_voice_length_url_and_grounding() {
        let grounding = "Auth outage traced to the token-rotation deploy; 12% of logins failed";
        // A grounded, in-voice lead passes (the 12% is present in the grounding).
        assert_eq!(
            clean_lead(
                "  An auth outage led the day; 12% of logins failed.  ",
                grounding
            )
            .as_deref(),
            Some("An auth outage led the day; 12% of logins failed.")
        );
        // Empty / over-length rejected.
        assert!(clean_lead("", grounding).is_none());
        assert!(clean_lead(&"x".repeat(LEAD_MAX + 1), grounding).is_none());
        // Hype / second-person / `!` rejected (the shared house-voice lint).
        assert!(clean_lead("A massive outage hit today.", grounding).is_none());
        assert!(clean_lead("You should read this.", grounding).is_none());
        // Raw URL rejected.
        assert!(clean_lead("See https://example.com for details.", grounding).is_none());
        // An ungrounded number (not in any selected headline) is rejected — no invented quantity.
        assert!(clean_lead("An auth outage hit 99% of logins.", grounding).is_none());

        // A count-bearing lead ("6 other updates") passes only when the count is in the grounding —
        // the contract `authored_lead` upholds by appending the digest's item counts (fix for the
        // gate rejecting every count-bearing big-picture lead).
        assert!(clean_lead(
            "An auth outage led the day; 6 other updates followed.",
            grounding
        )
        .is_none());
        let with_count = format!("{grounding}\n7 6");
        assert_eq!(
            clean_lead(
                "An auth outage led the day; 6 other updates followed.",
                &with_count
            )
            .as_deref(),
            Some("An auth outage led the day; 6 other updates followed.")
        );
    }

    #[test]
    fn lead_schema_and_prompt_carry_inputs() {
        let s = serde_json::to_string(&lead_schema()).unwrap();
        assert!(s.contains("digest_lead") && s.contains("\"maxLength\":320"));
        let p = lead_user_prompt(
            &[
                "Auth outage traced to the deploy".to_string(),
                "SSRF advisory forces a mitigation".to_string(),
            ],
            &["Acme auth migration".to_string()],
        );
        assert!(p.contains("Acme auth migration"));
        assert!(p.contains("Auth outage traced to the deploy"));
        assert!(p.contains("do not list every item"));
        // No headlines / no threads degrades to the explicit "(none)" rather than a blank.
        let empty = lead_user_prompt(&[], &[]);
        assert!(empty.contains("(none)"));
    }
}
