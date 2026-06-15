-- M4: re-surface suppression (design §9.4) + its config knob.
--
-- A story may appear in more than one digest (ongoing-story continuity), but repetition is damped:
-- selection compares a story's current `last_event_time` against the snapshot on the most recent
-- prior `digest_item` for this subscriber. A story with no new events since it was last shown — and
-- not graduating Note → Story — fades to a compact "still developing" note and is priority-damped,
-- eventually ageing out; a genuinely new event (or a Note→Story graduation) re-surfaces it.
--
-- So the frozen `digest_item` records, per shown story, the recency anchor it was shown at and the
-- format it was shown as — the suppression key the next fire reads back. Stable story ids (forwarded
-- across recomputes, §8.2) make the lookup well-defined. Nullable: pre-M4 rows carry no snapshot.

ALTER TABLE digest_item
    ADD COLUMN story_last_event_time timestamptz,
    ADD COLUMN format                text;

-- The damping factor lives with the rest of the scoring config (singleton row from migration 023).
ALTER TABLE digest_config
    ADD COLUMN resurface_penalty double precision NOT NULL DEFAULT 0.25;
