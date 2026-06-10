use chrono::{DateTime, Utc};

use crate::event::Event;

/// Marker for `Id<Cluster>`. A cluster is a within-source group of events sharing
/// `(source, group_key)`; the persisted row lives in the store layer.
pub struct Cluster;

/// The recomputed rollup PublicBuild upserts onto a cluster row. M1 keeps only what the digest
/// reads — the latest event's title/link and the group's recency. Richer rollups (counts,
/// severity, entities) re-add in M3/M4, when something consumes them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterRollup {
    /// Representative title — the latest event's title.
    pub title: String,
    /// Representative link — the latest event's primary link, if any.
    pub link: Option<String>,
    /// Most recent event_time in the group — the selection ordering key.
    pub last_event_time: DateTime<Utc>,
}

/// Pure rollup: fold a within-source group of events into a cluster rollup. The representative
/// is the latest event by `event_time` (ties → last in input order, deterministic given the
/// store loads a group ordered by `(event_time, id)`). Returns `None` for an empty group.
pub fn rollup(events: &[Event]) -> Option<ClusterRollup> {
    let mut representative = events.first()?;
    for ev in events {
        if ev.event_time >= representative.event_time {
            representative = ev;
        }
    }
    Some(ClusterRollup {
        title: representative.title.clone(),
        link: representative.links.first().cloned(),
        last_event_time: representative.event_time,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fingerprint::Fingerprint,
        id::Id,
        kind::{ContentKind, SourceKind},
        scope::Scope,
    };
    use chrono::TimeZone;
    use proptest::prelude::*;
    use uuid::Uuid;

    fn ev(secs: i64, title: &str) -> Event {
        Event {
            id: Id::new(Uuid::nil()),
            fingerprint: Fingerprint([0u8; 32]),
            source: SourceKind::Rss,
            scope: Scope::Public,
            event_time: Utc.timestamp_opt(secs, 0).single().unwrap(),
            title: title.to_owned(),
            body: None,
            links: vec![format!("https://example.com/{title}")],
            group_key: "g".to_owned(),
            entities: Vec::new(),
            content_kind: ContentKind::Longform,
            severity_hint: None,
            ingest_time: Utc.timestamp_opt(secs, 0).single().unwrap(),
            raw: None,
        }
    }

    #[test]
    fn empty_group_has_no_cluster() {
        assert!(rollup(&[]).is_none());
    }

    #[test]
    fn representative_is_latest_event() {
        let events = vec![ev(100, "old"), ev(300, "newest"), ev(200, "middle")];
        let r = rollup(&events).unwrap();
        assert_eq!(r.title, "newest");
        assert_eq!(r.link.as_deref(), Some("https://example.com/newest"));
        assert_eq!(r.last_event_time.timestamp(), 300);
    }

    proptest! {
        // last_event_time is always the group's max event_time.
        #[test]
        fn last_event_time_is_max(times in prop::collection::vec(0i64..1_000_000, 1..20)) {
            let events: Vec<Event> = times.iter().map(|&t| ev(t, "x")).collect();
            let r = rollup(&events).unwrap();
            prop_assert_eq!(r.last_event_time.timestamp(), *times.iter().max().unwrap());
        }
    }
}
