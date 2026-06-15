-- Store `thread.entities` as `text[]`, matching `cluster.entities`.
--
-- The thread layer's entity spine is the same namespaced-token list M3 already rolls onto
-- `cluster.entities` as a native `text[]`. Storing it as jsonb forced the thread store to round-trip
-- every read/write through serde_json and express the fire-time story→thread match with
-- `jsonb_array_elements_text` / `jsonb_exists_any` gymnastics. A `text[]` column gets sqlx's native
-- array (de)coding for free and lets `assign_thread` use the GIN-served array-overlap operator
-- (`t.entities && $keys`), exactly like the cluster blocking lookup. The data shape is unchanged.

-- The GIN op class differs between jsonb (jsonb_ops) and text[] (array_ops), so drop and recreate.
DROP INDEX thread_entities;

ALTER TABLE thread
    ALTER COLUMN entities DROP DEFAULT,
    ALTER COLUMN entities TYPE text[] USING ARRAY(SELECT jsonb_array_elements_text(entities)),
    ALTER COLUMN entities SET DEFAULT '{}';

CREATE INDEX thread_entities ON thread USING gin (entities);   -- fire-time story→thread match
