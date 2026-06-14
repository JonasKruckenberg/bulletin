-- M2 Phase 4 (cont.): extend RLS across the whole content→delivery path + the control-plane tables.
--
-- Migration 0017 put FORCE RLS on the scope-bearing *content* tables (`event`, `cluster`) and left
-- `connection`/`subscriber`/`digest`/`digest_item`/`private_build_watermark` merely granted to the
-- runtime role. That was a partial fix — and a partially-applied isolation boundary is worse than
-- none, because it invites reliance on a guarantee that doesn't hold (a digest and its frozen items
-- are exactly "subscriber A's content"; an unpoliced `digest`/`digest_item` is a leak path the
-- typed `Scope` never touches). This migration closes it: the full path
-- `event → cluster → digest_item → digest → delivery`, plus the per-subscriber build cursor, is now
-- DB-isolated, and these tables are **fail-closed** — the default (no-subscriber) context can't
-- read or write them at all.
--
-- Three contexts (set via the `app.subscriber_id` GUC through `with_scope`/`begin_scope`):
--   * no-subscriber (empty)  → DENIED on every table below;
--   * subscriber (a UUID)    → only that subscriber's own rows;
--   * admin (the `*` sentinel) → all rows — the explicit control-plane reach the cron sweeps,
--                                `status`, the poll/webhook connection lookups, and operator/debug
--                                commands opt into. No real UUID is `*`, so admin never matches an
--                                "own row" comparison on the content tables (0017) — there is still
--                                no path to another tenant's private *content*, only its control-plane
--                                metadata, which the trusted worker legitimately operates globally.
--
-- One permissive `FOR ALL` policy per table = `admin OR own` (USING and WITH CHECK alike), so reads,
-- inserts, updates, and deletes all obey the same boundary. `nullif(..., '')` collapses unset/empty
-- to NULL so the no-subscriber context's "own" comparison is NULL (→ denied), not a false match.

-- Helper shape, inlined per table (Postgres has no policy macros):
--   admin:  current_setting('app.subscriber_id', true) = '*'
--   own:    <subscriber key>::text = nullif(current_setting('app.subscriber_id', true), '')

-- ── connection (operator/control-plane: feeds + their owners) ───────────────
-- Owned by a subscriber (private-capable sources) or NULL (global RSS). A subscriber context sees
-- only its own connections; the worker's poll/webhook/tick/debug paths run as admin.
ALTER TABLE connection ENABLE ROW LEVEL SECURITY;
ALTER TABLE connection FORCE ROW LEVEL SECURITY;
CREATE POLICY connection_scope ON connection FOR ALL
   USING (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   )
   WITH CHECK (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );

-- ── subscriber (identity, schedule, PII) ────────────────────────────────────
-- A subscriber context sees only its own row (generate's load + the post-delivery schedule advance);
-- the tick's due-sweep, status, and operator/debug run as admin.
ALTER TABLE subscriber ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscriber FORCE ROW LEVEL SECURITY;
CREATE POLICY subscriber_scope ON subscriber FOR ALL
   USING (
      current_setting('app.subscriber_id', true) = '*'
      OR id::text = nullif(current_setting('app.subscriber_id', true), '')
   )
   WITH CHECK (
      current_setting('app.subscriber_id', true) = '*'
      OR id::text = nullif(current_setting('app.subscriber_id', true), '')
   );

-- ── digest (a subscriber's per-window digest) ───────────────────────────────
-- The delivery record: created/frozen/marked-delivered in the owner's context by generate; read in
-- bulk (status, debug list) as admin.
ALTER TABLE digest ENABLE ROW LEVEL SECURITY;
ALTER TABLE digest FORCE ROW LEVEL SECURITY;
CREATE POLICY digest_scope ON digest FOR ALL
   USING (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   )
   WITH CHECK (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );

-- ── digest_item (the frozen selection — a subscriber's chosen clusters) ──────
-- No subscriber column of its own; visibility derives from its parent digest, which is itself
-- RLS-scoped — so the EXISTS is evaluated under the *same* context and resolves to "is this my
-- digest?". (FK integrity checks against `cluster` bypass RLS, as Postgres always does for RI.)
ALTER TABLE digest_item ENABLE ROW LEVEL SECURITY;
ALTER TABLE digest_item FORCE ROW LEVEL SECURITY;
CREATE POLICY digest_item_scope ON digest_item FOR ALL
   USING (
      current_setting('app.subscriber_id', true) = '*'
      OR EXISTS (SELECT 1 FROM digest d WHERE d.id = digest_item.digest_id)
   )
   WITH CHECK (
      current_setting('app.subscriber_id', true) = '*'
      OR EXISTS (SELECT 1 FROM digest d WHERE d.id = digest_item.digest_id)
   );

-- ── private_build_watermark (per-subscriber build cursor) ────────────────────
-- Touched only by private-build, in the owner's context. Fail-closed elsewhere.
ALTER TABLE private_build_watermark ENABLE ROW LEVEL SECURITY;
ALTER TABLE private_build_watermark FORCE ROW LEVEL SECURITY;
CREATE POLICY private_build_watermark_scope ON private_build_watermark FOR ALL
   USING (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   )
   WITH CHECK (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );

-- ── story (M3: the per-subscriber, cross-source recompute the digest freezes) ─
-- A story is always Private-scoped to its owner (design §4/§5.3) and sits squarely on the delivery
-- path (cluster → story → digest_item → digest), so it is isolated like the rest. Recomputed and
-- read in the owner's context by `link::store`; admin only for any cross-tenant operator view.
ALTER TABLE story ENABLE ROW LEVEL SECURITY;
ALTER TABLE story FORCE ROW LEVEL SECURITY;
CREATE POLICY story_scope ON story FOR ALL
   USING (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   )
   WITH CHECK (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );

-- `build_watermark` (the singleton public cursor) holds no per-subscriber data — it stays
-- un-policied (granted to the runtime role), read/written by PublicBuild in the no-subscriber
-- context. The apalis queue schema is likewise control-plane infra, not tenant content.
