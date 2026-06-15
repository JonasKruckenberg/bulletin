-- M4: the scoring config table (design §8.4).
--
-- Selection is a pure function over precomputed features; its thresholds/caps live in a config table
-- (design §8.4 "the relevance_floor, richness threshold, and caps live in a config table in v1"), so
-- they are tunable without a code change. A single global row for v1 — per-subscriber overrides are a
-- later refinement. No per-subscriber data, so (like `build_watermark`) it carries no RLS policy and
-- is readable by the runtime role in any context.
--
-- The per-item reason records (format/richness/priority + the thread relevance term) ride on the
-- existing `digest.decisions` log (added by the thread-layer migration) — extended in code, no schema
-- change here.

CREATE TABLE digest_config (
    id boolean NOT NULL DEFAULT true PRIMARY KEY,
    -- Gate: a story is included iff its relevance ≥ this floor (design §8.4). Default 0 so everything
    -- passes until feedback drives a thread relevance term negative ("don't care") — the gate is how
    -- feedback removes things; base relevance (1.0) keeps the un-tuned digest at recency behaviour.
    relevance_floor        double precision NOT NULL DEFAULT 0,
    -- Relevance bonus for a story that includes the subscriber's own private content (§8.3).
    scope_bonus            double precision NOT NULL DEFAULT 0.5,
    -- Priority boost per point of a story's max source-provided severity_hint (a small integer; no v1
    -- connector sets it yet, so this is forward-compatible and contributes 0 in practice).
    severity_weight        double precision NOT NULL DEFAULT 0.1,
    -- Priority recency decay: a story's priority halves every this-many days of age at read time
    -- (design §8.3 "aged by recency decay over now − last_event_time").
    recency_half_life_days double precision NOT NULL DEFAULT 3,
    -- The thread relevance term ages on a slower cadence than recency (halves every this-many days),
    -- so a story you've invested a thread in stays promoted for weeks but still eventually fades.
    thread_half_life_days  double precision NOT NULL DEFAULT 21,
    -- Per-format caps (design §8.4): Stories ~3–5, Notes ~15–25. A Note is never dropped for being a
    -- Note, only for losing the priority race within its own cap.
    story_cap              integer NOT NULL DEFAULT 5,
    note_cap               integer NOT NULL DEFAULT 20,

    CONSTRAINT digest_config_singleton CHECK (id)
);
INSERT INTO digest_config (id) VALUES (true);
