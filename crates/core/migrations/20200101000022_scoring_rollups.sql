-- M4 Phase A: the scoring substrate — richer rollups on `cluster` and `story`.
--
-- M3 rolled the union of a group's entities + its recency span onto the cluster (the linking
-- blocking substrate). M4's scoring (design §8.3–§8.4) needs the *content* signals too:
--   * richness (Story-vs-Note format) = breadth (event_count, source_diversity) + depth (content_depth);
--   * priority = relevance + a `max_severity` boost, aged by recency decay.
-- These are derived from the same group fold the build already does — free to cache here, and
-- selection stays a pure function over precomputed features (design §8.4). Durable truth is still the
-- events; both tables are rebuildable caches, so this is purely additive.

-- ── cluster: per-group content signals (a cluster is one within-source group) ─
-- event_count + content_depth feed richness; max_severity feeds priority. A cluster spans one source,
-- so source_diversity is a *story*-level aggregate (distinct member sources) — not stored here.
ALTER TABLE cluster
    ADD COLUMN event_count   integer  NOT NULL DEFAULT 1,
    -- max content_kind over the group (Message < Announcement < Longform), the depth signal. Text,
    -- decoded via `ContentKind` exactly like `event.content_kind`; 'longform' is the M1/M2 default.
    ADD COLUMN content_depth text     NOT NULL DEFAULT 'longform',
    -- max source-provided severity_hint over the group, or NULL when no event carried one.
    ADD COLUMN max_severity  smallint     NULL;

-- ── story: cross-source rollups aggregated over the member clusters ──────────
-- The story is the unit selection scores (design §5.3/§8.4). These mirror the cluster signals,
-- folded across the component's members: event_count = Σ, source_diversity = |distinct sources|
-- (the literal "across sources" breadth signal), content_depth = max, max_severity = max. Recomputed
-- and rewritten by `persist_assignment` every generate, so the defaults only seed the column add.
ALTER TABLE story
    ADD COLUMN event_count      integer NOT NULL DEFAULT 0,
    ADD COLUMN source_diversity integer NOT NULL DEFAULT 0,
    ADD COLUMN content_depth    text    NOT NULL DEFAULT 'longform',
    ADD COLUMN max_severity     smallint    NULL;
