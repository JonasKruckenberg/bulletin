-- M2 Phase 3 follow-up: make the dedup identity scope-aware.
--
-- The fingerprint is SHA-256 over (source, stable_id) — a *content* identity, deliberately
-- scope-free so a poll and a webhook for the same activity collapse (§5.2). But `event` deduped on
-- `UNIQUE(fingerprint)` alone, which is global: if two subscribers each own a connection over the
-- *same* private repo (a shared org repo), both produce the same fingerprint, and the second
-- owner's event is silently dropped as a duplicate of the first — cross-tenant data loss.
--
-- Fix: an event's identity is its content-identity *within its scope*. Dedup on
-- (fingerprint, scope_kind, scope_subscriber_id). The fingerprint stays pure content identity
-- (unchanged), so poll↔webhook dedup within one scope still collapses; only cross-scope collisions
-- become distinct rows. NULLS NOT DISTINCT keeps public rows (NULL subscriber) collapsing on
-- fingerprint alone — without it two public events with the same fingerprint would no longer dedup.
ALTER TABLE event DROP CONSTRAINT event_fingerprint_unique;
ALTER TABLE event ADD CONSTRAINT event_fingerprint_unique
    UNIQUE NULLS NOT DISTINCT (fingerprint, scope_kind, scope_subscriber_id);
