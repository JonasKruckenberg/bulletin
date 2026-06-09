use chrono::{DateTime, Utc};

use crate::{
    fingerprint::Fingerprint,
    id::Id,
    kind::{ContentKind, SourceKind},
    scope::Scope,
};

/// Connector-side event builder. Holds everything the connector knows; `source` is fixed
/// at construction. Infra seals it by calling `finalize(scope)`, which stamps the scope
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
}

impl EventBuilder {
    pub fn new(
        source: SourceKind,
        stable_id: impl Into<String>,
        event_time: DateTime<Utc>,
        title: impl Into<String>,
        group_key: impl Into<String>,
        content_kind: ContentKind,
    ) -> Self {
        Self {
            source,
            stable_id: stable_id.into(),
            event_time,
            title: title.into(),
            group_key: group_key.into(),
            content_kind,
            body: None,
            links: Vec::new(),
            entities: Vec::new(),
            severity_hint: None,
            raw: None,
        }
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

    /// Stamps `scope` and computes the fingerprint. Infra calls this; connectors never do.
    pub fn finalize(self, scope: Scope) -> NewEvent {
        let fingerprint = Fingerprint::compute(self.source.as_str(), &self.stable_id);
        NewEvent {
            source: self.source,
            scope,
            fingerprint,
            event_time: self.event_time,
            title: self.title,
            body: self.body,
            links: self.links,
            group_key: self.group_key,
            entities: self.entities,
            content_kind: self.content_kind,
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
    pub entities: Vec<String>,
    pub content_kind: ContentKind,
    pub severity_hint: Option<i16>,
    pub raw: Option<Vec<u8>>,
}

/// Full event from the DB — `id` (UUIDv7) and `ingest_time` filled by the DB.
pub struct Event {
    pub id: Id<Event>,
    pub fingerprint: Fingerprint,
    pub source: SourceKind,
    pub scope: Scope,
    pub event_time: DateTime<Utc>,
    pub title: String,
    pub body: Option<String>,
    pub links: Vec<String>,
    pub group_key: String,
    pub entities: Vec<String>,
    pub content_kind: ContentKind,
    pub severity_hint: Option<i16>,
    pub ingest_time: DateTime<Utc>,
    pub raw: Option<Vec<u8>>,
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

    fn arb_content_kind() -> impl Strategy<Value = ContentKind> {
        prop_oneof![
            Just(ContentKind::Message),
            Just(ContentKind::Announcement),
            Just(ContentKind::Longform),
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
            kind_a in arb_content_kind(),
            kind_b in arb_content_kind(),
        ) {
            let a = EventBuilder::new(source, stable_id.clone(), t0(), title_a, "g", kind_a)
                .finalize(Scope::Public);
            let b = EventBuilder::new(source, stable_id, t0(), title_b, "g", kind_b)
                .finalize(Scope::Public);
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
            let a = EventBuilder::new(source, id_a, t0(), "", "", ContentKind::Longform)
                .finalize(Scope::Public);
            let b = EventBuilder::new(source, id_b, t0(), "", "", ContentKind::Longform)
                .finalize(Scope::Public);
            prop_assert_ne!(a.fingerprint, b.fingerprint);
        }
    }
}
