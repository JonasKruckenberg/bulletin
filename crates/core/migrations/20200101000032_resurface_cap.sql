-- M4 follow-up: a hard cap on stale "still developing" re-surfaces per digest (design §9.4).
--
-- The `resurface_penalty` (migration 024) only damps a no-news re-surface in the *ranking*; on its own
-- it can't stop a quiet fire from backfilling the whole Note cap with recycled items. This caps the
-- number of demoted "still developing" notes a single digest may render, so a few carry the ongoing
-- thread and the rest fall to over-cap. Fresh content is never re-surfaced, so it is unaffected. A
-- generous value effectively disables the cap.
ALTER TABLE digest_config
    ADD COLUMN resurface_cap integer NOT NULL DEFAULT 5;
