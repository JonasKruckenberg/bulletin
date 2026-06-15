-- Phase A of LLM summarization (docs/llm-summarization.md §2.1): the cluster — the only content-graph
-- unit that is both durable and shared — gains a precomputed, content-hashed summary. It is the
-- foundation every higher summary surface (story / thread / digest, phases B–D) composes from,
-- never re-deriving from raw events.
--
-- All additive and **inert by default** ('{}' ⇒ "no summary has run"), exactly like the thread
-- layer's `subscriber.affinity '{}'` shadow-default: the deterministic digest is byte-for-byte
-- unchanged until a summarization pass — behind the `llm-summarization` cargo feature *and* a runtime
-- flag — has populated these columns. A recomputable cache over the cluster's events, like the rollup
-- columns beside it: lose it, rebuild it.

ALTER TABLE cluster ADD COLUMN summary       jsonb       NOT NULL DEFAULT '{}';  -- extract-then-summarize product (headline + tldr run-list + facts + band)
ALTER TABLE cluster ADD COLUMN summary_hash  bytea;        -- signature of the events that fed the summary (§2.1 staleness gate)
ALTER TABLE cluster ADD COLUMN summary_model text;         -- "<model>@<prompt-version>" → a model/prompt upgrade invalidates the corpus by a WHERE sweep, no data migration
ALTER TABLE cluster ADD COLUMN summarized_at timestamptz;  -- when the summary was last written: staleness + the "due" sweep

-- The summarizer work queue: clusters never summarized. A cluster whose *content* changed is found
-- at sweep time by `updated_at > summarized_at` (the build bumps `updated_at` on every recompute)
-- plus an exact `summary_hash` re-check in Rust; this partial index keeps the common "brand-new
-- cluster" scan cheap without indexing the whole table.
CREATE INDEX cluster_needs_summary ON cluster (last_event_time)
  WHERE summarized_at IS NULL;
