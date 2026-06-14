-- M3: the cluster rollup gains the linking substrate.
--
-- Per-subscriber linking (§8.2) blocks candidate cluster pairs on shared entities, so the build must
-- now roll the union of a group's event entities onto the cluster (the GIN index is the blocking
-- lookup). `first_event_time` rounds out the recency span the story rollup aggregates (the cluster
-- already cached `last_event_time`); both are free in the rollup fold.

ALTER TABLE cluster ADD COLUMN entities text[] NOT NULL DEFAULT '{}';

-- Existing rows pre-date per-event-time tracking on the cluster; seed first = last (a single-instant
-- span) so the NOT NULL holds. The build overwrites both on the next recompute of each group.
ALTER TABLE cluster ADD COLUMN first_event_time timestamptz NOT NULL DEFAULT now();
UPDATE cluster SET first_event_time = last_event_time;

-- Blocking index: candidate-pair generation unnests `entities` to find clusters sharing a key
-- (design §8.2 — "v1 unnests the rolled-up entities jsonb via GIN").
CREATE INDEX cluster_entities ON cluster USING gin (entities);
