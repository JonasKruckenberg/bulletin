-- Phase B of LLM summarization (docs/llm-summarization.md §2.3): the Thread tier — a readable label
-- and the "what changed" delta that become the per-item **context eyebrow** (§1.1/§6.1). Per
-- subscriber, produced off the punctual path in `thread_maintenance`, composed from the tier below
-- (the new stories' summaries), never re-derived from raw events.
--
-- `thread.label` (migration 021) already carries the *deterministic* auto-label (top entities,
-- written every maintenance pass — the baseline). These columns add the **LLM upgrade** of that label
-- and the delta flag, additive and **inert by default** ('{}' / NULL ⇒ "no summary pass has run"),
-- exactly like the cluster tier: the deterministic eyebrow (auto-label, no delta) ships until a
-- summarization pass — behind the `llm-summarization` cargo feature — has populated them. A
-- recomputable cache over the durable story/feedback logs: lose it, rebuild it.

ALTER TABLE thread ADD COLUMN summary       jsonb NOT NULL DEFAULT '{}';  -- the readable "state of this thread" + LLM-upgraded label ({ "label": "Acme auth migration" })
ALTER TABLE thread ADD COLUMN delta         text;         -- the §5.2 delta flag ("staging cutover landed") — a few words, the eyebrow's terse tail
ALTER TABLE thread ADD COLUMN delta_through timestamptz;  -- watermark the delta covers (the thread's last summarized appearance): no new stories since ⇒ delta is current, skip
ALTER TABLE thread ADD COLUMN summary_model text;         -- "<model>@<prompt-version>" → a model/prompt upgrade re-summarizes the corpus by a WHERE sweep, no data migration
ALTER TABLE thread ADD COLUMN summarized_at timestamptz;  -- when the label/delta were last (re)written: staleness + the "due" gate
