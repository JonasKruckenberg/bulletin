CREATE TABLE subscriber (
    id            uuid        NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    email         text        NOT NULL,
    interval_days integer     NOT NULL DEFAULT 1,    -- digest cadence, in days (1 = daily, 7 = weekly)
    max_items     integer     NOT NULL DEFAULT 25,   -- selection cap N
    next_run_at   timestamptz NOT NULL DEFAULT now(),-- next digest boundary; the sweep's "due" watermark
    last_run_at   timestamptz     NULL               -- end of the last delivered window; selection lower bound
);

-- "due subscribers" sweep reads next_run_at; mirror of the connection due-index.
CREATE INDEX due_subscribers ON subscriber (next_run_at);
