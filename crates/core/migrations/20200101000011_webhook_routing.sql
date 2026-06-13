-- Webhook routing + the credential-ref indirection (M2 Phase 2).
--
-- `provider_account_id` is the source-side identity a webhook delivery routes to — for GitHub the
-- App installation_id (NOT a secret, design §3A). The `ProcessWebhook` job resolves OUR connection
-- row by (source, provider_account_id) and derives subscriber/scope from it, never from the payload
-- (IDOR defense). RSS connections have no such id (NULL).
--
-- `creds_ref` scaffolds the per-connection secret indirection (a pointer to a wrapped DEK) so
-- Phase 5's credential-at-rest and a later managed-KMS swap are *backend* changes, not migrations.
-- NULL for now.
--
-- `status` is free text, so a lifecycle webhook may set 'suspended'/'revoked' on a GitHub App
-- suspend/uninstall with no constraint change. Any non-'active' value pauses polling (the
-- `due_connections` predicate is `status = 'active'`).
ALTER TABLE connection ADD COLUMN provider_account_id text;
ALTER TABLE connection ADD COLUMN creds_ref           text;

-- The webhook routing key is unique per source (one connection per GitHub installation). Partial so
-- the many RSS rows with a NULL provider_account_id don't collide.
CREATE UNIQUE INDEX connection_provider_account
    ON connection (source, provider_account_id)
    WHERE provider_account_id IS NOT NULL;
