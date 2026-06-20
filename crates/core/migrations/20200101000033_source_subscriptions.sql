-- M5: make "a subscriber subscribes to one or more sources" a load-bearing relation.
--
-- Until now "subscription" was implicit and global: the digest candidate set was *every* public
-- cluster ∪ the subscriber's own-private clusters (link::candidate_clusters' `scope_kind = 'public'
-- OR scope_subscriber_id = $1`), so every public source landed in every subscriber's digest with no
-- way to choose a subset. This migration ties subscriber ↔ source together so a subscriber picks the
-- sources that comprise their digest, and so deleting a subscriber reclaims their private cache.
--
-- Three parts: (1) attribute every event/cluster to the `connection` that produced it — the origin we
-- never tracked, and the key the per-source filter needs; (2) the `subscription` join itself; and
-- (3) close the deletion leak — private `event`/`cluster`/`entity_edge` rows carried a
-- `scope_subscriber_id` with *no* FK, so a deleted subscriber's private cache was orphaned, not
-- reclaimed.

-- ── (1) connection origin on the content tables ────────────────────────────
-- Nullable + ON DELETE CASCADE: a poll/webhook always knows its connection (set at ingest), so live
-- rows carry it; NULL is the fail-closed default (an unattributed public cluster matches no
-- subscription, so it simply never enters a digest). Deleting a connection (a removed feed, or an
-- owned source cascaded when its subscriber is deleted) reclaims its events/clusters with it.
ALTER TABLE event   ADD COLUMN connection_id uuid NULL REFERENCES connection(id) ON DELETE CASCADE;
ALTER TABLE cluster ADD COLUMN connection_id uuid NULL REFERENCES connection(id) ON DELETE CASCADE;

-- The per-source candidate filter probes `cluster (scope_kind, connection_id)`; the cascade probes
-- `event (connection_id)` on a connection delete.
CREATE INDEX cluster_public_connection ON cluster (connection_id) WHERE scope_kind = 'public';
CREATE INDEX event_connection           ON event   (connection_id);

-- ── (2) the subscription join: subscriber ↔ connection ─────────────────────
-- A subscriber's chosen sources. Owning a connection (private sources) implies a subscription —
-- `insert_connection` seeds one for the owner — so the candidate query reads this one table. Both FKs
-- cascade: dropping either side drops the subscription. Idempotent membership via the composite PK.
CREATE TABLE subscription (
    subscriber_id uuid        NOT NULL REFERENCES subscriber(id)  ON DELETE CASCADE,
    connection_id uuid        NOT NULL REFERENCES connection(id)  ON DELETE CASCADE,
    created_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subscriber_id, connection_id)
);

-- The PK (subscriber_id, connection_id) serves the per-subscriber read; the connection-delete cascade
-- probes by connection_id (the PK's trailing column → no index), so it needs its own.
CREATE INDEX subscription_connection ON subscription (connection_id);

-- ── (3) reclaim private cache on subscriber delete ─────────────────────────
-- These three tables carry a private owner with no FK (clusters/events are a "rebuildable cache", so
-- the columns were added scope-first in 0002/0012/0021). A bare `scope_subscriber_id` left a deleted
-- subscriber's private rows orphaned-but-RLS-hidden — durable storage that never came back. The FK
-- (NULL on public rows, so they're unaffected) makes the delete cascade reclaim them.
ALTER TABLE event       ADD CONSTRAINT event_scope_subscriber_fk
    FOREIGN KEY (scope_subscriber_id) REFERENCES subscriber(id) ON DELETE CASCADE;
ALTER TABLE cluster     ADD CONSTRAINT cluster_scope_subscriber_fk
    FOREIGN KEY (scope_subscriber_id) REFERENCES subscriber(id) ON DELETE CASCADE;
ALTER TABLE entity_edge ADD CONSTRAINT entity_edge_scope_subscriber_fk
    FOREIGN KEY (scope_subscriber_id) REFERENCES subscriber(id) ON DELETE CASCADE;

-- Index the new cascades' probe column so deleting a subscriber doesn't seq-scan the (large) event
-- log and the edge table. Partial on the private rows — public rows have a NULL scope_subscriber_id a
-- cascade never probes. (cluster already has `cluster_private_recency (scope_subscriber_id, …)`.)
CREATE INDEX event_scope_subscriber       ON event       (scope_subscriber_id) WHERE scope_kind = 'private';
CREATE INDEX entity_edge_scope_subscriber ON entity_edge (scope_subscriber_id) WHERE scope_kind = 'private';

-- ── RLS: subscription is per-subscriber control-plane (mirrors 0020) ───────
-- `admin OR own`: the digest fire reads the subscriber's own rows in their context (the candidate
-- filter), operator/debug manage them as admin. No-subscriber context is denied (fail-closed).
ALTER TABLE subscription ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscription FORCE ROW LEVEL SECURITY;
CREATE POLICY subscription_scope ON subscription FOR ALL
   USING (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   )
   WITH CHECK (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );
