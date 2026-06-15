-- digest.candidates: the frozen `select` input for one digest — its candidate features, the read-time
-- clock, and `max_items` (a serialized `ReplaySnapshot`). Persisted so a delivered digest can be
-- re-scored under a *trial* scoring config offline (the eval config sweep — local-ml-options.md §0.1),
-- turning the manual explain→run→eval loop into a real A/B over history.
--
-- Additive + nullable: pre-existing digests have NULL and are simply not replayable (expand/contract
-- safe — an older binary ignores the column). It inherits `digest`'s row-level security: the snapshot
-- is the subscriber's own candidate spine, readable only in their context.
ALTER TABLE digest ADD COLUMN candidates jsonb;
