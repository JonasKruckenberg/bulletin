-- Lookback selection: a digest is a freshness-scored view over the durable log, not a window
-- partition. The candidate set is clusters updated since a consideration floor, so:
--   * window_start is no longer meaningful (selection has no hard lower partition edge);
--   * the build->digest gate is gone. An unbuilt event isn't a candidate this fire and is
--     re-considered on a later one (the floor is on cluster.updated_at — ingest/build recency,
--     bumped even by a backdated event — so it stays a candidate for the whole horizon). "Never
--     lost" is a property of the durable event log; selection by freshness may still leave a
--     low-ranked item unsurfaced (that is "not chosen", not "lost").
-- See digest-system-design.md §9.4.

ALTER TABLE digest DROP COLUMN window_start;

-- candidates_in_lookback filters by updated_at but returns rows ORDER BY last_event_time DESC; for
-- an active subscriber the floor (now - horizon) is non-selective, so the cost is the ordering.
-- Index last_event_time to serve it directly (backward scan), applying the updated_at floor as a
-- cheap filter.
CREATE INDEX cluster_recency ON cluster (last_event_time);
