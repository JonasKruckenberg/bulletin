-- Lookback selection: a digest is a freshness-scored view over the durable log, not a window
-- partition. The candidate set is clusters updated since a consideration floor, so:
--   * window_start is no longer meaningful (selection has no hard lower partition edge);
--   * the build->digest gate is gone (an unbuilt event simply isn't a candidate this fire and
--     rides the next one — never lost, since the floor is on cluster.updated_at, ingest/build
--     recency, which is bumped even by a backdated event).
-- See digest-system-design.md §9.4.

ALTER TABLE digest DROP COLUMN window_start;

-- The lookback filter scans clusters by ingest/build recency; index it.
CREATE INDEX cluster_updated ON cluster (updated_at);
