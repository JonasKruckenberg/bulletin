-- Phase 2 of the content graph: a within-source group of events sharing (source, group_key),
-- represented by its latest event. A recomputed batch artifact — durable state is the events;
-- this row is a rebuildable cache, so later milestones re-add columns (scope, rollups) for free.
-- M1 is public-only (private scope lands in M2), so there are no scope columns yet.
CREATE TABLE cluster (
    id              uuid        NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    source          text        NOT NULL,
    group_key       text        NOT NULL,
    title           text        NOT NULL,           -- representative: latest event's title
    link            text            NULL,           -- representative: latest event's primary link
    last_event_time timestamptz NOT NULL,           -- selection ordering key
    updated_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT cluster_identity UNIQUE (source, group_key)
);

-- Singleton watermark: every public event with ingest_time <= built_through has been
-- incorporated into a cluster. PublicBuild advances it; the digest sweep gates on it.
-- Floor is the epoch (not '-infinity', which chrono can't decode) — earlier than any real
-- ingest_time, so the first build's half-open range (epoch, now] still covers everything.
CREATE TABLE build_watermark (
    id            boolean     NOT NULL DEFAULT true PRIMARY KEY,
    built_through timestamptz NOT NULL DEFAULT 'epoch',

    CONSTRAINT build_watermark_singleton CHECK (id)
);
INSERT INTO build_watermark (id, built_through) VALUES (true, 'epoch');
