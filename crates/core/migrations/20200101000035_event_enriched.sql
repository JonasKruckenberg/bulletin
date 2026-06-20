-- Phase 2: LLM entity/topic enrichment — grounded named entities/topics mined per item, EARLY
-- (before clustering/linking), so related coverage across publishers fuses on what a story is
-- ABOUT (place:/org:/person:/topic:) rather than only a per-publisher domain:.
--
-- `enriched_at` marks an event the best-effort enrichment sweep has processed (NULL = pending). The
-- sweep unions the grounded tokens onto `event.entities` and stamps `enriched_at = now()` so a clean
-- pass is never re-attempted; a failed/down LLM leaves it NULL to retry next sweep, until the
-- cluster build's grace deadline ages the event past the watermark and it is clustered with the
-- entities it already has (structural + derived) — the "fall behind, never wrong" contract.
ALTER TABLE event ADD COLUMN enriched_at timestamptz NULL;

-- The sweep scans the small frontier of public, not-yet-enriched events. A partial index keeps that
-- scan cheap as the append-only log grows (only the pending NULL rows are indexed).
CREATE INDEX event_enrich_pending ON event (ingest_time)
   WHERE enriched_at IS NULL AND scope_kind = 'public';

-- The RLS migration (20200101000019) gave `event` only SELECT/INSERT policies — it was an
-- append-only log. Enrichment writes grounded entities back onto an event before it is clustered, so
-- the runtime role now needs to UPDATE it. Same two-context shape as `cluster_update`: a public row
-- only in the no-subscriber context, a private row only as its owner — the directional public→private
-- isolation invariant is preserved (a subscriber context can never touch the shared public pool, and
-- can never write another tenant's private row).
CREATE POLICY event_update ON event FOR UPDATE
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
