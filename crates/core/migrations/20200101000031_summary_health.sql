-- The summarization pipeline redesign (docs/llm-summarization.md §3.7): summarization is no longer a
-- best-effort enrichment that silently degrades to a deterministic baseline — it is the deliverable. A
-- cluster ships in a digest only once it carries a faithful model summary (the §3.4 gate's
-- `confirmed`/`probable` band), and a digest never ships an authored lead it didn't compose. That makes
-- a *failed* summarization (a down sidecar, a faithfulness-gate rejection) a real, trackable error with
-- bounded retries — not a value to swallow. These columns are the per-cluster health record those
-- retries read and write.
--
-- All additive, all defaulting to "healthy / never failed", so existing rows are untouched: a cluster
-- with `summary_attempts = 0` and a NULL `summary_quarantined_at` is one that has either summarized
-- cleanly or not yet been tried — exactly today's state.

ALTER TABLE cluster ADD COLUMN summary_attempts      int NOT NULL DEFAULT 0;  -- consecutive failed summarization attempts since the last success (drives the §3.7 escalating-seed retry; reset to 0 on a faithful summary)
ALTER TABLE cluster ADD COLUMN summary_last_error    text;                    -- the most recent failure's coarse kind + message ("rejected: ungrounded_number …" / "unavailable: timeout"), for operator review
ALTER TABLE cluster ADD COLUMN summary_failed_at     timestamptz;             -- when the most recent attempt failed — NULL once a summary lands
ALTER TABLE cluster ADD COLUMN summary_quarantined_at timestamptz;            -- set when retries are exhausted: the cluster is flagged "bad", withheld from the sweep, and surfaced for operator review. Cleared by a content change (a fresh chance) or a later success.

-- The operator-review queue: clusters whose summarization the retry budget couldn't make faithful. A
-- partial index so "what's broken?" is a cheap scan of only the quarantined rows, not the whole table.
CREATE INDEX cluster_quarantined ON cluster (summary_failed_at)
  WHERE summary_quarantined_at IS NOT NULL;

-- The story tier (Phase C) gets the same treatment (§3.7): a multi-source story ships only with a
-- faithful cross-source *synthesis*, never collapsed to one member's single-source blurb (that reads as
-- low quality). So a failed synthesis is the same tracked error with the same bounded escalating retries
-- and quarantine — a story that can't be synthesized faithfully is withheld from the digest (it slips to
-- a later window) rather than degraded. (A single-member story has nothing to fuse and renders its one
-- faithful cluster summary directly, so it never reaches synthesis — these columns only bite multi-member
-- stories.)
ALTER TABLE story ADD COLUMN summary_attempts       int NOT NULL DEFAULT 0;
ALTER TABLE story ADD COLUMN summary_last_error     text;
ALTER TABLE story ADD COLUMN summary_failed_at      timestamptz;
ALTER TABLE story ADD COLUMN summary_quarantined_at timestamptz;

CREATE INDEX story_quarantined ON story (summary_failed_at)
  WHERE summary_quarantined_at IS NOT NULL;
