-- A subscriber's digest for one scheduled window. Lifecycle is a single nullable timestamp:
-- delivered_at IS NULL = pending, set = delivered. The digest is created with its selected items
-- frozen in one transaction (born "built"), so there is exactly one transition — deliver.
-- window_end = scheduled boundary; UNIQUE(subscriber_id, window_end) collapses retries onto one
-- row so a crash mid-run resumes the same window without a duplicate digest (or send).
CREATE TABLE digest (
    id            uuid        NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    subscriber_id uuid        NOT NULL REFERENCES subscriber(id) ON DELETE CASCADE,
    window_start  timestamptz NOT NULL,
    window_end    timestamptz NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    delivered_at  timestamptz     NULL,

    CONSTRAINT digest_window_unique UNIQUE (subscriber_id, window_end)
);

-- The frozen selection: one selected cluster per row, in render order.
CREATE TABLE digest_item (
    digest_id  uuid    NOT NULL REFERENCES digest(id) ON DELETE CASCADE,
    cluster_id uuid    NOT NULL REFERENCES cluster(id),
    position   integer NOT NULL,

    PRIMARY KEY (digest_id, cluster_id)
);
