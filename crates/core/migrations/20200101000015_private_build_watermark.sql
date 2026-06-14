-- M2 Phase 3 follow-up (#4): a per-subscriber build watermark for private clusters.
--
-- PrivateBuild previously rebuilt ALL of a subscriber's private clusters on every digest (no
-- cursor): (a) cost grew with lifetime private history, and (b) every private cluster's `updated_at`
-- was stamped `now()` each run, so a quiet private cluster never aged out of the candidate floor the
-- way a public one does. This watermark mirrors the singleton `build_watermark`, keyed per
-- subscriber: PrivateBuild processes only events ingested since the owner's last build and advances
-- the cursor, so private clustering matches public semantics — bounded work, and stale clusters age
-- out (their `updated_at` is only bumped when a new event re-dirties their group).
--
-- The default floor is the epoch (not '-infinity', which chrono can't decode), earlier than any real
-- ingest_time; a missing row is treated as the epoch too, so a subscriber's first build covers all
-- of their private history. ON DELETE CASCADE drops the cursor with the subscriber.
CREATE TABLE private_build_watermark (
    subscriber_id uuid        NOT NULL PRIMARY KEY REFERENCES subscriber(id) ON DELETE CASCADE,
    built_through timestamptz NOT NULL DEFAULT 'epoch'
);
