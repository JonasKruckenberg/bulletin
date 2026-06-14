-- M2 Phase 4: two-context row-level security (design §12, tech §6).
--
-- Up to now scope isolation has been a *query convention* — every cluster/event read carries a
-- `scope_kind = 'public' OR scope_subscriber_id = $sub` predicate (Phase 3). This migration makes
-- the database physically enforce it, so a logic bug that drops the predicate still cannot leak one
-- tenant's private content to another: Postgres refuses the row.
--
-- Run by the **owner/migration role** (it owns the DDL and the tables). The app logs in as a
-- separate, least-privilege **runtime role** (`bulletin_app`) on a second connection string: a
-- non-owner, non-superuser role with **no BYPASSRLS** — the prerequisite without which FORCE RLS is
-- theatre (a superuser or the table owner sans FORCE would sail straight through). The privilege
-- boundary is at the *credential* level (a separate login), which `SET ROLE`/`RESET ROLE` can't undo.
--
-- Scope of enforcement: the two **scope-bearing content tables**, `event` and `cluster`, where the
-- typed `Scope` lives and where cross-tenant leakage of *content* is catastrophic. The control-plane
-- tables (`connection`, `subscriber`, `digest`, `digest_item`, the watermarks, the apalis queue) are
-- granted to the runtime role without row policies: the cron tick must enumerate *all* due
-- connections/subscribers across owners to schedule work, and a `digest_item` only references a
-- `cluster_id` whose content is itself RLS-protected. Tightening those behind SECURITY DEFINER
-- enqueue/metrics functions is a later-milestone refinement; the content tables are the leak surface.

-- ── The least-privilege runtime role ───────────────────────────────────────
-- Idempotent: in production the deployment provisions the role first (e.g. NixOS `ensureUsers`, so a
-- non-superuser owner needn't hold CREATEROLE) and this block no-ops; in the test harness the
-- superuser migration creates it. Either way it ends up non-superuser, no BYPASSRLS. How it
-- *authenticates* (a password, or unix-socket peer auth) is a deployment concern set outside the
-- migration.
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'bulletin_app') THEN
      CREATE ROLE bulletin_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
   END IF;
END
$$;

-- ── event: FORCE RLS + two-context policies ────────────────────────────────
-- FORCE so the policies apply even to the table owner (a superuser still bypasses — which is why the
-- runtime role must not be one). `nullif(current_setting('app.subscriber_id', true), '')` collapses
-- both "unset" and "empty" to NULL = the no-subscriber context; comparing on `::text` avoids a cast
-- error if the GUC ever holds a non-UUID (it just fails to match).
ALTER TABLE event ENABLE ROW LEVEL SECURITY;
ALTER TABLE event FORCE ROW LEVEL SECURITY;

-- Read: public always; own-private only when this subscriber's context is set.
CREATE POLICY event_select ON event FOR SELECT
   USING (
      scope_kind = 'public'
      OR scope_subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );

-- Write (append-only log → INSERT): a public row only in the no-subscriber context; a private row
-- only as its owner. A subscriber context can never inject into the shared public pool, and can
-- never write another tenant's private row — the directional public→private invariant, enforced.
CREATE POLICY event_insert ON event FOR INSERT
   WITH CHECK (
      (scope_kind = 'public'
         AND nullif(current_setting('app.subscriber_id', true), '') IS NULL)
      OR
      (scope_kind = 'private'
         AND scope_subscriber_id::text = nullif(current_setting('app.subscriber_id', true), ''))
   );

-- ── cluster: FORCE RLS + two-context policies (same shape; +UPDATE for the upsert) ─
ALTER TABLE cluster ENABLE ROW LEVEL SECURITY;
ALTER TABLE cluster FORCE ROW LEVEL SECURITY;

CREATE POLICY cluster_select ON cluster FOR SELECT
   USING (
      scope_kind = 'public'
      OR scope_subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );

-- The build upserts (`INSERT … ON CONFLICT DO UPDATE`), so both INSERT and UPDATE need a policy.
CREATE POLICY cluster_insert ON cluster FOR INSERT
   WITH CHECK (
      (scope_kind = 'public'
         AND nullif(current_setting('app.subscriber_id', true), '') IS NULL)
      OR
      (scope_kind = 'private'
         AND scope_subscriber_id::text = nullif(current_setting('app.subscriber_id', true), ''))
   );

CREATE POLICY cluster_update ON cluster FOR UPDATE
   USING (
      (scope_kind = 'public'
         AND nullif(current_setting('app.subscriber_id', true), '') IS NULL)
      OR
      (scope_kind = 'private'
         AND scope_subscriber_id::text = nullif(current_setting('app.subscriber_id', true), ''))
   )
   WITH CHECK (
      (scope_kind = 'public'
         AND nullif(current_setting('app.subscriber_id', true), '') IS NULL)
      OR
      (scope_kind = 'private'
         AND scope_subscriber_id::text = nullif(current_setting('app.subscriber_id', true), ''))
   );

-- Table/sequence GRANTs to bulletin_app are applied (and re-applied for later schema, incl. apalis)
-- by `grant_runtime_role`, run after migrations + queue setup — see crates/core/src/common/db.rs.
