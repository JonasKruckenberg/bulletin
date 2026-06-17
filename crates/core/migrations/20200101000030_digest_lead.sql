-- Phase D of LLM summarization (docs/llm-summarization.md §2.4): the Digest tier — the "big picture"
-- lead. The digest is the only summary unit that is both per-fire and per-subscriber and *cannot exist
-- before selection* (§1), so its lead is composed at fire time, never precomputed: a deterministic
-- string assembly over the selected items' headlines (Phase A, no model call) that an optional,
-- **deadline-bounded** best-effort "editor's note" (Phase D, behind the `llm-summarization` cargo
-- feature) may upgrade — falling back to the deterministic lead the instant it misses its deadline, so
-- the digest still sends on time ("fall behind, never wrong", thread-layer §3.1).
--
-- Persisted (nullable) for explainability parity with `digest.decisions` (migration 021) — so the lead
-- that shipped is reproducible in the debug trace. Additive and **inert by default** (NULL ⇒ no lead
-- was recorded for this digest — a pre-Phase-D / feature-off digest, which renders the deterministic
-- lead at fire time as before). A recomputable projection over the durable selection: lose it, the next
-- render recomposes it.

ALTER TABLE digest ADD COLUMN lead text;   -- the rendered "big picture" lead; NULL ⇒ none recorded (renders the deterministic lead at fire time)
