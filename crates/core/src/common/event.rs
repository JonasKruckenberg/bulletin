use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, Row};
use uuid::Uuid;

use super::{fingerprint::Fingerprint, kind::ContentKind, kind::SourceKind, scope::Scope};

/// Connector-side event builder. Holds everything the connector knows; `source` is fixed
/// at construction. Infra seals it by calling `finalize(owner)`, which stamps the scope
/// boundary and computes the fingerprint — neither can be touched by a connector.
pub struct EventBuilder {
    source: SourceKind,
    stable_id: String,
    event_time: DateTime<Utc>,
    title: String,
    group_key: String,
    content_kind: ContentKind,
    body: Option<String>,
    links: Vec<String>,
    entities: Vec<String>,
    severity_hint: Option<i16>,
    raw: Option<Vec<u8>>,
    /// Structural visibility of the source item (e.g. GitHub's `repo.private`). The adapter reports
    /// only this bool — it can name no subscriber and construct no `Scope` (design §12 risk #1).
    /// `finalize` combines it with the connection's owner to decide the actual `Scope`.
    is_private: bool,
}

impl EventBuilder {
    pub fn new(
        source: SourceKind,
        stable_id: impl Into<String>,
        event_time: DateTime<Utc>,
        title: impl Into<String>,
        group_key: impl Into<String>,
    ) -> Self {
        Self {
            source,
            stable_id: stable_id.into(),
            event_time,
            title: title.into(),
            group_key: group_key.into(),
            // Longform is the conservative default (matches M1's behavior, where every event was
            // longform); connectors override via `content_kind` where source semantics differ.
            content_kind: ContentKind::Longform,
            body: None,
            links: Vec::new(),
            entities: Vec::new(),
            severity_hint: None,
            raw: None,
            is_private: false,
        }
    }

    /// Sets the depth signal (`message` / `announcement` / `longform`). Defaults to `longform`.
    pub fn content_kind(mut self, kind: ContentKind) -> Self {
        self.content_kind = kind;
        self
    }

    /// Marks the item as coming from a private source object (e.g. a private GitHub repo). Defaults
    /// to `false` (public). The adapter sets only this bool; the subscriber binding is `finalize`'s.
    pub fn private(mut self, is_private: bool) -> Self {
        self.is_private = is_private;
        self
    }

    pub fn body(mut self, body: impl Into<String>) -> Self {
        self.body = Some(body.into());
        self
    }

    pub fn links(mut self, links: Vec<String>) -> Self {
        self.links = links;
        self
    }

    pub fn entities(mut self, entities: Vec<String>) -> Self {
        self.entities = entities;
        self
    }

    pub fn severity_hint(mut self, hint: i16) -> Self {
        self.severity_hint = Some(hint);
        self
    }

    pub fn raw(mut self, raw: Vec<u8>) -> Self {
        self.raw = Some(raw);
        self
    }

    /// Stamps the scope and computes the fingerprint. Infra calls this with the connection's owning
    /// subscriber (`None` for a global/public source like RSS); connectors never do. The scope is
    /// derived here, *not* by the adapter: a private item from an owned connection is bound to that
    /// owner, everything else stays public. This is the one place a subscriber binding is created
    /// from a `(is_private, owner)` pair — an adapter can neither name a subscriber nor force a
    /// private scope onto a shared (ownerless) source (design §12 risk #1).
    ///
    /// The `(is_private, None) => Public` arm is *not* a fallback for private items: a private-capable
    /// source is required to have an owner at the connection layer (`SourceKind::can_emit_private` +
    /// the `connection_private_source_owned` CHECK, with the FK's ON DELETE CASCADE keeping it owned
    /// for life), so an ownerless private item is structurally unreachable. The arm only ever maps a
    /// genuinely public item (`is_private == false`) from an ownerless or owned connection.
    pub fn finalize(self, owner: Option<Uuid>) -> NewEvent {
        let scope = match (self.is_private, owner) {
            (true, Some(subscriber_id)) => Scope::Private(subscriber_id),
            _ => Scope::Public,
        };
        let fingerprint = Fingerprint::compute(self.source.as_str(), &self.stable_id);

        // Enrich the connector's structural entities (`repo:`/`user:`) with the cross-source keys
        // (`cve:`/`url:`/`domain:`) mined from this event's text + links, in one place so every
        // source gets them uniformly — they are the blocking substrate M3 linking runs on (§8.2).
        // Entities are *not* folded into the fingerprint, so enrichment never disturbs dedup.
        let mut entities = self.entities;
        entities.extend(super::entity::derive(
            &self.title,
            self.body.as_deref(),
            &self.links,
        ));
        entities.sort();
        entities.dedup();

        NewEvent {
            source: self.source,
            scope,
            fingerprint,
            event_time: self.event_time,
            title: self.title,
            body: self.body,
            links: self.links,
            group_key: self.group_key,
            content_kind: self.content_kind,
            entities,
            severity_hint: self.severity_hint,
            raw: self.raw,
        }
    }
}

/// Post-finalize, pre-DB — scope and fingerprint stamped, ready to INSERT.
pub struct NewEvent {
    pub source: SourceKind,
    pub scope: Scope,
    pub fingerprint: Fingerprint,
    pub event_time: DateTime<Utc>,
    pub title: String,
    pub body: Option<String>,
    pub links: Vec<String>,
    pub group_key: String,
    pub content_kind: ContentKind,
    pub entities: Vec<String>,
    pub severity_hint: Option<i16>,
    pub raw: Option<Vec<u8>>,
}

/// Full event from the DB — `id` (UUIDv7) and `ingest_time` filled by the DB.
pub struct Event {
    pub id: Uuid,
    pub fingerprint: Fingerprint,
    pub source: SourceKind,
    pub scope: Scope,
    pub event_time: DateTime<Utc>,
    pub title: String,
    pub body: Option<String>,
    pub links: Vec<String>,
    pub group_key: String,
    pub content_kind: ContentKind,
    pub entities: Vec<String>,
    pub severity_hint: Option<i16>,
    pub ingest_time: DateTime<Utc>,
    pub raw: Option<Vec<u8>>,
}

fn decode_err(msg: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> sqlx::Error {
    sqlx::Error::Decode(msg.into())
}

/// Decodes one `event` row into an `Event`. The canonical row mapper, shared by the ingest store
/// (append/list) and the cluster store (group reads) — both `SELECT` the same column set.
pub fn from_row(row: PgRow) -> Result<Event, sqlx::Error> {
    let fp_bytes: Vec<u8> = row.get("fingerprint");
    let mut fp = [0u8; 32];
    fp.copy_from_slice(&fp_bytes);

    let scope_kind: String = row.get("scope_kind");
    let scope =
        Scope::from_columns(&scope_kind, row.get("scope_subscriber_id")).map_err(decode_err)?;

    Ok(Event {
        id: row.get("id"),
        fingerprint: Fingerprint(fp),
        source: row.try_get("source")?,
        scope,
        event_time: row.get("event_time"),
        title: row.get("title"),
        body: row.get("body"),
        links: row.get("links"),
        group_key: row.get("group_key"),
        content_kind: row.try_get("content_kind")?,
        entities: row.get("entities"),
        severity_hint: row.get("severity_hint"),
        ingest_time: row.get("ingest_time"),
        raw: row.get("raw"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use proptest::prelude::*;

    fn t0() -> DateTime<Utc> {
        Utc.timestamp_opt(0, 0).single().unwrap()
    }

    fn arb_source() -> impl Strategy<Value = SourceKind> {
        prop_oneof![
            Just(SourceKind::Rss),
            Just(SourceKind::Github),
            Just(SourceKind::Slack),
        ]
    }

    proptest! {
        // Re-polling the same item (same source + stable_id) with different content must
        // produce the same fingerprint so ON CONFLICT DO NOTHING collapses the duplicate.
        #[test]
        fn content_does_not_affect_fingerprint(
            source in arb_source(),
            stable_id in "[a-z0-9]{1,64}",
            title_a in ".*",
            title_b in ".*",
        ) {
            let a = EventBuilder::new(source, stable_id.clone(), t0(), title_a, "g")
                .finalize(None);
            let b = EventBuilder::new(source, stable_id, t0(), title_b, "g")
                .finalize(None);
            prop_assert_eq!(a.fingerprint, b.fingerprint);
        }

        // Different stable_ids from the same source must never share a fingerprint.
        #[test]
        fn different_stable_ids_collide_never(
            source in arb_source(),
            id_a in "[a-z0-9]{1,32}",
            id_b in "[a-z0-9]{1,32}",
        ) {
            prop_assume!(id_a != id_b);
            let a = EventBuilder::new(source, id_a, t0(), "", "").finalize(None);
            let b = EventBuilder::new(source, id_b, t0(), "", "").finalize(None);
            prop_assert_ne!(a.fingerprint, b.fingerprint);
        }
    }
}
