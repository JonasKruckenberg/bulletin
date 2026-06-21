-- Step-function ranking: a salience (importance) decay knob + a per-thread diversity cap.
--
-- `severity_weight` (migration 023) already scales a story's `max_severity` into its priority, but no
-- v1 source emitted a hint, so the term was inert. Salience now populates `event.severity_hint`
-- (GitHub structurally at ingest; free-text via the enrichment LLM's classified impact), so the term
-- is live. Two new knobs make it land well:
--
--   * salience_half_life_days — the severity/importance term decays on its OWN cadence, deliberately
--     slower than base recency (a major story should linger a few days, not vanish overnight) yet
--     faster than an invested thread. Selection keeps the three decays explicit (recency / salience /
--     thread) so the importance boost can outlive recency without dragging the base term with it.
--
--   * thread_cap — the max stories from any one Thread a single digest may carry (design §8.4 /
--     thread-layer §5.2). Bounds within-topic repetition so a busy thread (or an added news feed's
--     dominant topic) can't take every slot; importance then ranks WITHIN each thread's allocation.
--     A generous value effectively disables the cap; 0 disables it outright.
ALTER TABLE digest_config
    ADD COLUMN salience_half_life_days double precision NOT NULL DEFAULT 7,
    ADD COLUMN thread_cap              integer          NOT NULL DEFAULT 2;
