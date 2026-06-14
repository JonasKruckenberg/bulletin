-- M3: the `story` table — the per-subscriber, cross-source unit selection ranks (§5.3, design §8.2).
--
-- A story is one connected component of a subscriber's candidate clusters (public ∪ own-private),
-- fused across sources. It is a *per-subscriber recomputed read-model*, not durable truth: each
-- GenerateDigest recomputes the components from scratch and forwards stable ids onto them via the
-- membership recorded here (the prior assignment). Always Private-scoped to its owner — a public
-- cluster is a read-only input that may belong to many subscribers' stories, so membership lives on
-- the story (`clusters`), never as a back-pointer on the shared cluster (§5.3).
CREATE TABLE story (
    id               uuid        NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    subscriber_id    uuid        NOT NULL REFERENCES subscriber(id) ON DELETE CASCADE,
    -- Set when a retro-merge forwards this id to its survivor (the oldest id wins, §8.2); the row
    -- becomes a tombstone (empty `clusters`) that redirects a stale deep-link. NULL = a live story.
    merged_into      uuid            NULL REFERENCES story(id),
    -- Membership + per-member rationale: [{cluster_id, link_reason}] (design §10.2). This *is* the
    -- persisted prior assignment the next recompute reads to forward stable ids.
    clusters         jsonb       NOT NULL DEFAULT '[]',
    -- Cross-source recency span (aggregated over the member clusters); `last_event_time` is the
    -- selection ordering key, mirroring the cluster rollup the digest used pre-M3.
    first_event_time timestamptz NOT NULL,
    last_event_time  timestamptz NOT NULL,
    -- Stamped when a digest carrying this story is delivered. Gates the asymmetric-merge rule: only a
    -- *strong* edge may merge two already-delivered stories, so a weak link can't silently collapse
    -- two stories the subscriber has already seen as distinct (§8.2 single-linkage guard).
    last_delivered_at timestamptz    NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- The recompute loads a subscriber's live stories (the prior assignment) by owner.
CREATE INDEX story_subscriber ON story (subscriber_id) WHERE merged_into IS NULL;

-- The frozen selection unit becomes the Story (was the Cluster). digest_item is a rebuildable
-- projection artifact (recreated on each generate's freeze), so swap its shape outright rather than
-- expand-contract a column migration. A story's events are still reached by walking its `clusters`.
DROP TABLE digest_item;
CREATE TABLE digest_item (
    digest_id uuid    NOT NULL REFERENCES digest(id) ON DELETE CASCADE,
    story_id  uuid    NOT NULL REFERENCES story(id),
    position  integer NOT NULL,

    PRIMARY KEY (digest_id, story_id)
);
