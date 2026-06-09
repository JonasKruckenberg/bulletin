CREATE TABLE event (
    id                  uuid        NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    fingerprint         bytea       NOT NULL,
    source              text        NOT NULL,
    scope_kind          text        NOT NULL,  -- 'public' | 'private'
    scope_subscriber_id uuid            NULL,  -- set iff scope_kind = 'private'
    event_time          timestamptz NOT NULL,
    title               text        NOT NULL,
    body                text            NULL,
    links               text[]      NOT NULL DEFAULT '{}',
    group_key           text        NOT NULL,
    entities            text[]      NOT NULL DEFAULT '{}',
    content_kind        text        NOT NULL,
    severity_hint       smallint        NULL,
    ingest_time         timestamptz NOT NULL DEFAULT now(),
    raw                 bytea           NULL,

    CONSTRAINT event_fingerprint_unique UNIQUE (fingerprint),
    CONSTRAINT event_scope_check CHECK (
        (scope_kind = 'public'  AND scope_subscriber_id IS NULL) OR
        (scope_kind = 'private' AND scope_subscriber_id IS NOT NULL)
    )
);
