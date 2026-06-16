-- Phase C of LLM summarization (docs/llm-summarization.md §2.2): the Story tier — the fused,
-- cross-source rewrite ("a CVE advisory, an incident PR, and a Slack flurry are the same outage")
-- that a single cluster summary can't produce. Per subscriber, **cached by member-signature** and
-- written by a best-effort pass in `thread_maintenance`, *read* at fire time.
--
-- A story is a per-fire recompute (migration 018) with stable, id-forwarded ids (§8.2), so it cannot
-- host an authored-at-fire-time summary (an LLM call on the hot path is forbidden) — instead it hosts
-- a **cache** keyed by `summary_sig` (the sorted member-cluster summary hashes ‖ thread id). The sig
-- is stable until membership/content actually moves, so the synthesis is reused across fires for free
-- and regenerated only when a source is added/dropped or a member's content changes.
--
-- All additive and **inert by default** ('{}' ⇒ "no synthesis has run"): fire-time falls back to the
-- representative cluster's summary (always precomputed, Phase A), so the email is never empty — the
-- cross-source rewrite is a *quality upgrade* that lands on the next fire after a pass synthesizes it.
-- A recomputable cache over the durable cluster summaries: lose it, rebuild it.

ALTER TABLE story ADD COLUMN summary       jsonb NOT NULL DEFAULT '{}';  -- the fused cross-source item summary (headline + tldr run-list + facts + band), same shape as cluster.summary
ALTER TABLE story ADD COLUMN summary_sig   bytea;        -- member signature: hash of the sorted member cluster.summary_hash[] — the §2.2 staleness gate (thread_id is not folded in; see story_summary_sig)
ALTER TABLE story ADD COLUMN summary_model text;         -- "<model>@<prompt-version>" → a model/prompt upgrade re-synthesizes the corpus by a WHERE sweep, no data migration
ALTER TABLE story ADD COLUMN summarized_at timestamptz;  -- when the synthesis was last written: staleness + the "due" gate

-- The synthesis work queue, mirroring cluster_needs_summary (migration 027): the common "never
-- synthesized" scan stays cheap (a content change is found at sweep time by `updated_at > summarized_at`
-- — the upsert bumps `updated_at` only on a real change — plus an exact `summary_sig` re-check in Rust).
CREATE INDEX story_needs_summary ON story (subscriber_id, last_event_time)
  WHERE summarized_at IS NULL;
