CREATE TABLE connection (
    id                   uuid        NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    source               text        NOT NULL,           -- 'rss' | 'github' | 'slack'
    status               text        NOT NULL DEFAULT 'active',  -- active | paused | errored
    config               jsonb       NOT NULL DEFAULT '{}',      -- e.g. {"url": "https://..."} for RSS
    cursor               jsonb           NULL,           -- opaque, source-private poll position
    poll_interval_secs   bigint      NOT NULL DEFAULT 900,       -- 15 min default
    next_poll_at         timestamptz NOT NULL DEFAULT now(),
    last_polled_at       timestamptz     NULL,
    consecutive_failures smallint    NOT NULL DEFAULT 0
);

CREATE INDEX due_connections ON connection (next_poll_at) WHERE status = 'active';
