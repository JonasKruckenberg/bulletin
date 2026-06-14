-- M2 Phase 3: private scope becomes load-bearing.
--
-- The `event` table already carries scope (Phase 1). This migration extends scope to the two places
-- that decide *who sees what*: the `cluster` rollup (so a private repo's clusters are per-owner, not
-- shared) and the `connection` row (so `finalize` can bind a private event to its owning subscriber
-- — the adapter only ever reports an `is_private` bool, never a subscriber; design §12 risk #1).

-- ── cluster: scope columns (mirrors `event`) ───────────────────────────────
-- Existing rows are public; new private clusters are built just-in-time per subscriber.
ALTER TABLE cluster ADD COLUMN scope_kind          text NOT NULL DEFAULT 'public';
ALTER TABLE cluster ADD COLUMN scope_subscriber_id uuid     NULL;

ALTER TABLE cluster ADD CONSTRAINT cluster_scope_check CHECK (
    (scope_kind = 'public'  AND scope_subscriber_id IS NULL) OR
    (scope_kind = 'private' AND scope_subscriber_id IS NOT NULL)
);

-- Replace the public-only identity with a scope-aware one: a public and a private group with the
-- same (source, group_key) are now distinct clusters. NULLS NOT DISTINCT (PG15+) so the single NULL
-- subscriber on public rows still collapses to one cluster per (source, group_key) — without it the
-- public upsert's ON CONFLICT would never fire (NULLs compare distinct by default).
ALTER TABLE cluster DROP CONSTRAINT cluster_identity;
ALTER TABLE cluster ADD CONSTRAINT cluster_identity
    UNIQUE NULLS NOT DISTINCT (scope_kind, scope_subscriber_id, source, group_key);

-- Scope-aware recency index for the candidate lookback (`public ∪ own-private`, newest-first).
CREATE INDEX cluster_scope_recency
    ON cluster (scope_kind, scope_subscriber_id, last_event_time DESC);

-- ── connection: the owning subscriber ──────────────────────────────────────
-- NULL = a global/public source (RSS) with no owner. A private event from an owned connection
-- finalizes to Private(owner); `finalize` derives the owner from THIS row, never from the payload.
ALTER TABLE connection ADD COLUMN subscriber_id uuid NULL REFERENCES subscriber(id) ON DELETE CASCADE;
