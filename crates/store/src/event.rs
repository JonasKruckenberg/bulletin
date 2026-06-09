use bulletin_core::{
    event::{Event, NewEvent},
    fingerprint::Fingerprint,
    id::Id,
    kind::{ContentKind, SourceKind},
    scope::Scope,
};
use sqlx::{postgres::PgRow, PgPool, Row};
use uuid::Uuid;

/// Inserts `ev` into the `event` table, deduplicating on fingerprint.
/// Returns `Some(event)` if inserted, `None` if the fingerprint already existed.
pub async fn insert_event(pool: &PgPool, ev: &NewEvent) -> Result<Option<Event>, sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = match &ev.scope {
        Scope::Public => ("public", None::<Uuid>),
        Scope::Private(sub_id) => ("private", Some(sub_id.as_uuid())),
    };

    sqlx::query(
        r#"
        INSERT INTO event (
            fingerprint, source, scope_kind, scope_subscriber_id,
            event_time, title, body, links, group_key, entities,
            content_kind, severity_hint, raw
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (fingerprint) DO NOTHING
        RETURNING
            id, fingerprint, source, scope_kind, scope_subscriber_id,
            event_time, title, body, links, group_key, entities,
            content_kind, severity_hint, ingest_time, raw
        "#,
    )
    .bind(&ev.fingerprint.0[..])
    .bind(ev.source.as_str())
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(ev.event_time)
    .bind(&ev.title)
    .bind(ev.body.as_deref())
    .bind(&ev.links)
    .bind(&ev.group_key)
    .bind(&ev.entities)
    .bind(ev.content_kind.as_str())
    .bind(ev.severity_hint)
    .bind(ev.raw.as_deref())
    .map(row_to_event)
    .fetch_optional(pool)
    .await
}

fn row_to_event(row: PgRow) -> Event {
    let fp_bytes: Vec<u8> = row.get("fingerprint");
    let mut fp = [0u8; 32];
    fp.copy_from_slice(&fp_bytes);

    let scope_kind: String = row.get("scope_kind");
    let scope_subscriber_id: Option<Uuid> = row.get("scope_subscriber_id");
    let scope = match scope_kind.as_str() {
        "private" => Scope::Private(Id::new(
            scope_subscriber_id.expect("private scope_subscriber_id must be set"),
        )),
        _ => Scope::Public,
    };

    let source: String = row.get("source");
    let source = SourceKind::try_from(source.as_str())
        .unwrap_or_else(|_| panic!("unknown source in event row: {source}"));

    let content_kind: String = row.get("content_kind");
    let content_kind = ContentKind::try_from(content_kind.as_str())
        .unwrap_or_else(|_| panic!("unknown content_kind in event row: {content_kind}"));

    Event {
        id: Id::new(row.get("id")),
        fingerprint: Fingerprint(fp),
        source,
        scope,
        event_time: row.get("event_time"),
        title: row.get("title"),
        body: row.get("body"),
        links: row.get("links"),
        group_key: row.get("group_key"),
        entities: row.get("entities"),
        content_kind,
        severity_hint: row.get("severity_hint"),
        ingest_time: row.get("ingest_time"),
        raw: row.get("raw"),
    }
}
