-- M2 Phase 3 follow-up (#5): index the candidate lookback's actual predicate.
--
-- candidates_in_lookback filters `(scope_kind = 'public' OR scope_subscriber_id = $sub)
--   AND updated_at >= floor` (the freshness floor) and orders by last_event_time. The Phase-3
-- cluster_scope_recency index keyed on last_event_time, so it served neither the `updated_at` range
-- (the selective predicate) nor the scope OR — the query fell back to a full cluster scan.
--
-- Replace it with two predicate-aligned indexes the planner can bitmap-OR, one per arm of the OR:
--   * public arm  — a partial index over the freshness column for shared clusters;
--   * private arm — (owner, updated_at) so a subscriber's own clusters seek by owner then range.
-- The candidate set (clusters updated since the floor) is small, so the trailing sort on
-- last_event_time is cheap; the win is replacing the scan with two bounded index range scans.
DROP INDEX cluster_scope_recency;

CREATE INDEX cluster_public_recency
    ON cluster (updated_at)
    WHERE scope_kind = 'public';

CREATE INDEX cluster_private_recency
    ON cluster (scope_subscriber_id, updated_at);
