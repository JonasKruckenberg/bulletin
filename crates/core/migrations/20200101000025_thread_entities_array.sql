-- Store `thread.entities` as `text[]`, matching `cluster.entities`.
--
-- The thread layer's entity spine is the same namespaced-token list M3 already rolls onto
-- `cluster.entities` as a native `text[]`. Storing it as jsonb forced the thread store to round-trip
-- every read/write through serde_json and express the fire-time story→thread match with
-- `jsonb_array_elements_text` / `jsonb_exists_any` gymnastics. A `text[]` column gets sqlx's native
-- array (de)coding for free and lets `assign_thread` use the GIN-served array-overlap operator
-- (`t.entities && $keys`), exactly like the cluster blocking lookup. The data shape is unchanged.

-- `ALTER COLUMN ... TYPE ... USING` forbids a subquery in the transform expression, and there's no
-- scalar jsonb→text[] cast — so convert via a temp column whose UPDATE *can* unnest the jsonb array,
-- then swap. Dropping the old column also drops its (jsonb_ops) GIN index; recreate it for text[].
ALTER TABLE thread ADD COLUMN entities_arr text[] NOT NULL DEFAULT '{}';
UPDATE thread
   SET entities_arr = ARRAY(SELECT value FROM jsonb_array_elements_text(entities) AS value);
ALTER TABLE thread DROP COLUMN entities;
ALTER TABLE thread RENAME COLUMN entities_arr TO entities;

CREATE INDEX thread_entities ON thread USING gin (entities);   -- fire-time story→thread match
