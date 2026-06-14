-- M2 Phase 3 follow-up (#1): a private-capable source must have an owner (fail-closed).
--
-- `finalize` maps (is_private, owner) -> Scope; with no owner there is no subscriber to bind a
-- private item to. Rather than silently downgrading such an item to the shared public scope (a
-- confidentiality leak — fail-open), require that any source able to emit private events carries an
-- owning subscriber. Only RSS is public-only (a feed URL is global); everything else (GitHub today,
-- Slack later) must be owned. The FK's ON DELETE CASCADE then guarantees the owner exists for the
-- connection's whole life, so the ownerless-private case is structurally unreachable.
--
-- Mirrors `SourceKind::can_emit_private` (only `rss` is public-only) — keep the two in sync.
ALTER TABLE connection ADD CONSTRAINT connection_private_source_owned
    CHECK (source = 'rss' OR subscriber_id IS NOT NULL);
