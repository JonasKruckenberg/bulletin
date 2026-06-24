//! The `bulletin_llm_*` metrics for the local-sidecar summarization path. Thin recorders so the
//! metric-name strings live in one place rather than scattered across the client and the sweep —
//! mirroring `bulletin`'s `metric.rs`. The Prometheus recorder is installed by the `bulletin` binary;
//! until then (e.g. in unit tests, or any non-`worker` role) these macros are no-ops, so recording is
//! always safe to call unconditionally.

use std::time::Duration;

use super::GateViolation;

/// One completed sidecar chat call: wall-time into the latency histogram, keyed by `phase` + `outcome`.
/// `phase` is the call site — `summarize` | `comprehend` | `synthesize` | `label` | `delta`; `outcome`
/// is `ok` or the [`failure_kind`](super::client) bucket (`timeout` | `connect` | `status` | `decode` |
/// `transport` | `response`). The `outcome` label is what keeps the latency view honest: a near-instant
/// `connect` failure and a 120s `timeout` are recorded too, so without it they would blend into the
/// generation-latency distribution — filter `outcome="ok"` for clean prompt latency. The histogram's
/// `_count{phase,outcome}` doubles as the per-outcome call total, so no separate counter is needed.
pub fn llm_call(phase: &'static str, outcome: &'static str, elapsed: Duration) {
    metrics::histogram!("bulletin_llm_call_duration_seconds", "phase" => phase, "outcome" => outcome)
        .record(elapsed.as_secs_f64());
}

/// The token usage the sidecar reported for one call (the OpenAI-compatible `usage` block), split into
/// `prompt` vs `completion` so throughput and budget pressure are both visible — a `completion` total
/// pinned against `max_tokens` is the "reasoning ran out the budget" signal the client guards against.
/// Only the 2xx path carries a usage block to report.
pub fn llm_tokens(phase: &'static str, prompt: u64, completion: u64) {
    metrics::counter!("bulletin_llm_tokens_total", "phase" => phase, "kind" => "prompt")
        .increment(prompt);
    metrics::counter!("bulletin_llm_tokens_total", "phase" => phase, "kind" => "completion")
        .increment(completion);
}

/// A model candidate the faithfulness gate rejected (`phase`: `summarize` | `synthesize`). `reason` is
/// the *variant* of the violation only — never its dynamic payload (the offending entity/number) — so
/// the label set stays bounded. For a cluster this is also a tracked [`summary_failed`] of kind
/// `rejected` that drives the §3.7 escalating-seed retry; this counter additionally breaks it down by
/// gate reason.
pub fn gate_rejection(phase: &'static str, violation: &GateViolation) {
    let reason = match violation {
        GateViolation::UngroundedEntity(_) => "ungrounded_entity",
        GateViolation::UngroundedNumber(_) => "ungrounded_number",
        GateViolation::BannedWord(_) => "banned_word",
        GateViolation::UrlInProse(_) => "url_in_prose",
        GateViolation::TooLong => "too_long",
        GateViolation::UnfaithfulRelation(_) => "unfaithful_relation",
    };
    metrics::counter!("bulletin_llm_gate_rejections_total", "phase" => phase, "reason" => reason)
        .increment(1);
}

/// A tracked summarization failure (§3.7): a unit (`unit`: `cluster` | `story`) that came out of an
/// attempt without a faithful summary. `kind` is the coarse reason — `unavailable` (sidecar/transport)
/// or `rejected` (the §3.4 gate said no) — so the down-sidecar rate and the hallucination-rejection rate
/// read apart. The escalating-seed retry (§3.7) draws against this counter.
pub fn summary_failed(unit: &'static str, kind: &'static str) {
    metrics::counter!("bulletin_llm_summary_failed_total", "unit" => unit, "kind" => kind)
        .increment(1);
}

/// A unit (`unit`: `cluster` | `story`) whose retry budget was exhausted and is now **quarantined** for
/// operator review (§3.7) — withheld from the sweep and from digests until a content change or a manual
/// clear. A rising count is the signal that the sidecar or the gate is systematically failing a slice of
/// the corpus, not just flapping.
pub fn quarantined(unit: &'static str) {
    metrics::counter!("bulletin_llm_quarantined_total", "unit" => unit).increment(1);
}

/// One content-hash cache decision in a sweep (`kind`: `cluster` | `story`): a `hit` reused the cached
/// summary and made no model call; a `miss` went on to spend one. The hit-rate across both is the
/// directest read on how much the §3.3 idempotency is actually saving in model calls.
pub fn cache(kind: &'static str, hit: bool) {
    let result = if hit { "hit" } else { "miss" };
    metrics::counter!("bulletin_llm_cache_total", "kind" => kind, "result" => result).increment(1);
}
