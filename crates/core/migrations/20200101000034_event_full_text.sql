-- Phase 1: real article text. Link-based sources (RSS) carry only a short snippet in `body`; the
-- summarizer grounds far better off the full article. A best-effort, off-hot-path fetch step
-- (`ingest::fetch`) resolves each event's link, extracts the readable text, and stores it here —
-- WITHOUT overwriting the original snippet, so provenance is clear and a fetch failure leaves the
-- event summarizable from what it already had.
--
-- `full_text`            the extracted readable article body (NULL until/unless a fetch succeeds).
-- `full_text_fetched_at` when the fetch landed (NULL while unfetched) — provenance + debugging.
-- `full_text_attempts`   consecutive fetch attempts so far; the work queue stops retrying past a cap
--                        so a permanently-unfetchable link doesn't burn the sweep every pass.
ALTER TABLE event ADD COLUMN full_text            text        NULL;
ALTER TABLE event ADD COLUMN full_text_fetched_at timestamptz NULL;
ALTER TABLE event ADD COLUMN full_text_attempts   smallint    NOT NULL DEFAULT 0;

-- The fetch work-queue gate (`due_events_for_fetch` / `events_needing_fetch_exist`): only events
-- that could still want a fetch. The partial predicate must match the work-queue WHERE clause's most
-- selective, never-changing terms so the recurring "is there work?" probe stays cheap as the log
-- grows: `source = 'rss'` excludes the GitHub/Slack rows (whose `full_text` is NULL forever because
-- they are never fetchable) from the index entirely — without it the index would bloat with exactly
-- the rows that can never match. (Keep `source = 'rss'` in sync with `SourceKind::fetchable_sources`;
-- a second fetchable source would widen this predicate. `full_text_attempts` is deliberately left out
-- — retry-exhausted RSS rows are few and bounded by RSS volume.)
CREATE INDEX event_fetch_pending ON event (ingest_time)
    WHERE full_text IS NULL AND source = 'rss';
