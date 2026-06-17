//! The `bulletin_llm_*` metrics for the local-sidecar summarization path. Thin recorders so the
//! metric-name strings live in one place rather than scattered across the client and the sweep —
//! mirroring `bulletin`'s `metric.rs`. Gated on `llm-summarization` (the only build that makes model
//! calls), and `metrics` is an optional dependency pulled in by that same feature, keeping the
//! deterministic build's dependency surface flat. The Prometheus recorder is installed by the
//! `bulletin` binary; until then (e.g. in unit tests, or any non-`worker` role) these macros are
//! no-ops, so recording is always safe to call unconditionally.

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

/// A model candidate the faithfulness gate rejected back to the deterministic baseline (`phase`:
/// `summarize` | `synthesize`). `reason` is the *variant* of the violation only — never its dynamic
/// payload (the offending entity/number) — so the label set stays bounded.
pub fn gate_rejection(phase: &'static str, violation: &GateViolation) {
    let reason = match violation {
        GateViolation::UngroundedEntity(_) => "ungrounded_entity",
        GateViolation::UngroundedNumber(_) => "ungrounded_number",
        GateViolation::BannedWord(_) => "banned_word",
        GateViolation::TooLong => "too_long",
    };
    metrics::counter!("bulletin_llm_gate_rejections_total", "phase" => phase, "reason" => reason)
        .increment(1);
}

/// One content-hash cache decision in a sweep (`kind`: `cluster` | `story`): a `hit` reused the cached
/// summary and made no model call; a `miss` went on to spend one. The hit-rate across both is the
/// directest read on how much the §3.3 idempotency is actually saving in model calls.
pub fn cache(kind: &'static str, hit: bool) {
    let result = if hit { "hit" } else { "miss" };
    metrics::counter!("bulletin_llm_cache_total", "kind" => kind, "result" => result).increment(1);
}
