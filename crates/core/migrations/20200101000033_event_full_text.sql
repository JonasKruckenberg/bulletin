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
-- that still want a fetch. A partial index keeps the recurring "is there work?" probe cheap as the
-- event log grows — the overwhelming majority of rows (already fetched, or never fetchable) are
-- excluded from the index entirely.
CREATE INDEX event_fetch_pending ON event (ingest_time)
    WHERE full_text IS NULL;
