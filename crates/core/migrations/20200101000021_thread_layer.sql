-- The Thread layer & tiered identity (`digest-thread-layer.md`).
--
-- The cross-time weave that sits on top of M3 linking: persistent per-subscriber `Thread`s, a
-- feedback-driven `entity_edge` identity graph, and a projected entity-weight map the digest's
-- relevance term reads. **Additive and shadow by default** — nothing on the digest hot path consumes
-- these until a `thread_maintenance` pass has run, and the fire-time consumption is behind the
-- `thread-weighting` cargo feature. `thread_maintenance` is the *sole writer* of thread/identity
-- structure; the rows are a recomputable cache over the durable event + feedback logs (like
-- `cluster`/`story`), so losing them costs only a rebuild.
--
-- Entities are M3's namespaced tokens (`repo:`/`user:`/`url:`/`cve:`/`domain:`, already rolled onto
-- `cluster.entities`); the thread layer consumes them directly — there is no separate
-- `canonical_entities` on the cluster.

-- ── thread: the persistent "thread of a user's life" (always Private, one owner) ───────────────
CREATE TABLE thread (
    id                  uuid PRIMARY KEY DEFAULT uuidv7(),
    subscriber_id       uuid NOT NULL REFERENCES subscriber(id) ON DELETE CASCADE,
    origin              text NOT NULL CHECK (origin IN ('declared','emergent')),
    label               text,                              -- user-set, else auto (top entities)
    state               text NOT NULL DEFAULT 'active'
                          CHECK (state IN ('active','dormant','archived')),
    pinned              boolean NOT NULL DEFAULT false,    -- declared threads: never auto-merged/archived
    merged_into         uuid REFERENCES thread(id),        -- id-forwarding on re-clustering (§8.2 trick)
    entities            jsonb NOT NULL DEFAULT '[]',       -- the thread's resolved entity spine
    confidence          text NOT NULL DEFAULT 'confirmed'  -- identity band (weakest spine alias merge)
                          CHECK (confidence IN ('confirmed','probable','uncertain')),
    affinity            real NOT NULL DEFAULT 0,           -- feedback + engagement weight (decays)
    story_count         int  NOT NULL DEFAULT 0,
    source_diversity    int  NOT NULL DEFAULT 0,
    baseline_rate       real NOT NULL DEFAULT 0,           -- stories/day baseline → novelty/burst term
    first_seen          timestamptz,
    last_story_time     timestamptz                        -- dormancy + reactivation salience
);
CREATE INDEX thread_active   ON thread (subscriber_id, state, last_story_time DESC);
CREATE INDEX thread_entities ON thread USING gin (entities);   -- fire-time story→thread match
CREATE INDEX thread_merged   ON thread (merged_into) WHERE merged_into IS NOT NULL;

-- ── entity_edge: feedback-driven identity (NOT a 1:1 alias map) ─────────────────────────────────
-- A canonical identity is a connected component over the positive edges (computed in
-- thread_maintenance, id-forwarded). A `must_link` is a positive feedback edge; a `cannot_link` is a
-- veto, materialized here as a row with negative confidence so identity is reconstructible from the
-- graph alone (the resolver never re-merges a vetoed pair). `'public'` edges are shared; `'private'`
-- edges are per-subscriber. (Lexical/embedding graded edge sources slot in later as more rows.)
CREATE TABLE entity_edge (
    scope_kind          text NOT NULL,                     -- 'public' shared; 'private' per-subscriber
    scope_subscriber_id uuid,                              -- null iff public
    a                   text NOT NULL,                     -- entity token
    b                   text NOT NULL,
    confidence          real NOT NULL,                     -- ≥0 positive equivalence; <0 a cannot-link veto
    source              text NOT NULL,                     -- exact_id | normalized | embedding | feedback
    evidence            jsonb NOT NULL DEFAULT '{}',
    PRIMARY KEY (scope_kind, scope_subscriber_id, a, b)
);

-- ── the fire-time relevance input: per-subscriber entity_weight map ─────────────────────────────
-- A `{ entity_token: weight }` jsonb map (normalize on trigger, design §6/§8). thread_maintenance is
-- its sole writer; the digest hot path reads it to add the Thread relevance term. Default '{}' means
-- "no thread weighting" — so the layer is inert until a maintenance pass has run.
ALTER TABLE subscriber ADD COLUMN affinity jsonb NOT NULL DEFAULT '{}';

-- ── digest_item: the assigned thread, for thread-grouped render + the delivered-thread history ──
ALTER TABLE digest_item ADD COLUMN thread_id uuid;

-- ── digest.decisions: the full per-digest decision log (design §10.2 reason records) ────────────
-- A jsonb array of every candidate story considered for this digest with its verdict (selected OR
-- over-cap) and the reasoning behind its rank (the Thread relevance term + the entity spine it scored
-- on). Recording the *drops* too is what lets a later explain UI answer "why was X *not* in my
-- digest?". Surfaced in the digest's debug trace; queryable later. jsonb (normalize-on-trigger to a
-- `digest_decision` table if it gets hot); default '[]' = no log recorded (a pre-thread digest).
ALTER TABLE digest ADD COLUMN decisions jsonb NOT NULL DEFAULT '[]';

-- ── feedback: append-only correction log (design §10.3, thread-layer §4) ────────────────────────
-- Drives entity must/cannot-link (→ entity_edge) and thread care-more/less. Append-only and
-- per-subscriber; thread_maintenance folds it in on its next pass — nothing shared to mutate, so a
-- correction takes effect in that subscriber's next recompute only.
CREATE TABLE feedback (
    id            uuid PRIMARY KEY DEFAULT uuidv7(),
    subscriber_id uuid NOT NULL REFERENCES subscriber(id) ON DELETE CASCADE,
    target_type   text NOT NULL CHECK (target_type IN ('entity','thread','story')),
    target_id     text NOT NULL,                           -- entity token / thread id / story id (as text)
    signal        text NOT NULL
                    CHECK (signal IN ('care_more','care_less','done','must_link','cannot_link')),
    payload       jsonb NOT NULL DEFAULT '{}',             -- e.g. must_link/cannot_link: { "other": "<token>" }
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX feedback_subscriber ON feedback (subscriber_id, created_at DESC);

-- ── thread_maintenance watermark (per subscriber, due-gated cadence) ────────────────────────────
-- `built_through` is the incremental feedback cursor (so each care nudge folds in once); `ran_at`
-- drives the due query (`ran_at + cadence <= now`), mirroring how `due_subscribers` gates digests —
-- so the tick enqueues only subscribers actually due for a pass, not a full scan every minute.
CREATE TABLE thread_maintenance_watermark (
    subscriber_id uuid        NOT NULL PRIMARY KEY REFERENCES subscriber(id) ON DELETE CASCADE,
    built_through timestamptz NOT NULL DEFAULT 'epoch',
    ran_at        timestamptz NOT NULL DEFAULT 'epoch'
);

-- ── Two-context RLS (migrations 019/020 set the model) ──────────────────────────────────────────
-- The thread layer's tables join the same FORCE-RLS regime as the content/delivery path, so a logic
-- bug that drops a `subscriber_id` predicate still can't leak across tenants. `thread_maintenance`
-- runs in the subscriber's context (own threads/edges/feedback/watermark); the tick's due-sweep runs
-- as `admin` (the `*` sentinel) to enumerate watermarks across subscribers; the digest fire-time
-- consumption runs in the subscriber's context.

-- thread / feedback / watermark: per-subscriber control-plane rows → `admin OR own` (like `story`).
ALTER TABLE thread ENABLE ROW LEVEL SECURITY;
ALTER TABLE thread FORCE ROW LEVEL SECURITY;
CREATE POLICY thread_scope ON thread FOR ALL
   USING (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   )
   WITH CHECK (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );

ALTER TABLE feedback ENABLE ROW LEVEL SECURITY;
ALTER TABLE feedback FORCE ROW LEVEL SECURITY;
CREATE POLICY feedback_scope ON feedback FOR ALL
   USING (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   )
   WITH CHECK (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );

ALTER TABLE thread_maintenance_watermark ENABLE ROW LEVEL SECURITY;
ALTER TABLE thread_maintenance_watermark FORCE ROW LEVEL SECURITY;
CREATE POLICY thread_maintenance_watermark_scope ON thread_maintenance_watermark FOR ALL
   USING (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   )
   WITH CHECK (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );

-- entity_edge: scope-bearing identity *content* (public shared ∪ private per-subscriber) → same shape
-- as `event`/`cluster`: read public-or-own; write a private edge only as its owner, a public edge
-- only in the no-subscriber context. (No `admin` reach into another tenant's private edges.)
ALTER TABLE entity_edge ENABLE ROW LEVEL SECURITY;
ALTER TABLE entity_edge FORCE ROW LEVEL SECURITY;
CREATE POLICY entity_edge_select ON entity_edge FOR SELECT
   USING (
      scope_kind = 'public'
      OR scope_subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );
CREATE POLICY entity_edge_insert ON entity_edge FOR INSERT
   WITH CHECK (
      (scope_kind = 'public' AND nullif(current_setting('app.subscriber_id', true), '') IS NULL)
      OR (scope_kind = 'private'
            AND scope_subscriber_id::text = nullif(current_setting('app.subscriber_id', true), ''))
   );
CREATE POLICY entity_edge_update ON entity_edge FOR UPDATE
   USING (
      (scope_kind = 'public' AND nullif(current_setting('app.subscriber_id', true), '') IS NULL)
      OR (scope_kind = 'private'
            AND scope_subscriber_id::text = nullif(current_setting('app.subscriber_id', true), ''))
   )
   WITH CHECK (
      (scope_kind = 'public' AND nullif(current_setting('app.subscriber_id', true), '') IS NULL)
      OR (scope_kind = 'private'
            AND scope_subscriber_id::text = nullif(current_setting('app.subscriber_id', true), ''))
   );
