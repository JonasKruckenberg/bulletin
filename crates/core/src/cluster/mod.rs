//! The clustering flow: drain the event log (via the build watermark cursor) and fold each
//! dirtied within-source group into a representative `cluster` row. Consumer of the
//! ingest→clustering seam, producer of the clustering→digest seam.

pub mod store;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::common::event::Event;

/// The recomputed rollup the build upserts onto a cluster row. M1 keeps only what the digest
/// reads — the latest event's title/link and the group's recency. Richer rollups (counts,
/// severity, entities) re-add later, when something consumes them.
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

pub struct BuildStats {
    pub dirty_groups: usize,
    pub built_through: DateTime<Utc>,
}

/// PublicBuild: drain new public events into clusters and advance the build watermark.
///
/// Runs in a single transaction holding a transaction-level advisory lock, so concurrent builds
/// serialize (the loser returns `Ok(None)`) and the whole pass is atomic — a crash rolls back
/// without advancing the watermark, leaving the events still due next tick. Processes the
/// half-open ingest range `(built_through, now()]`: finds the groups it dirtied, recomputes each
/// cluster's rollup over *all* its events, upserts, then advances to `now()`.
pub async fn build(pool: &PgPool) -> Result<Option<BuildStats>> {
    let mut tx = pool.begin().await.context("begin build txn")?;

    if !store::try_build_lock(&mut *tx)
        .await
        .context("acquire build lock")?
    {
        tracing::debug!("public build already in progress; skipping");
        return Ok(None);
    }

    let (built_through, hwm) = store::build_bounds(&mut *tx)
        .await
        .context("read build bounds")?;
    let groups = store::dirty_public_groups(&mut *tx, built_through, hwm)
        .await
        .context("find dirty groups")?;

    for (source, group_key) in &groups {
        let events = store::list_public_group_events(&mut *tx, *source, group_key)
            .await
            .context("load group events")?;
        if let Some(r) = rollup(&events) {
            store::upsert_cluster(&mut *tx, *source, group_key, &r)
                .await
                .context("upsert cluster")?;
        }
    }

    store::advance_build_watermark(&mut *tx, hwm)
        .await
        .context("advance watermark")?;
    tx.commit().await.context("commit build txn")?;

    Ok(Some(BuildStats {
        dirty_groups: groups.len(),
        built_through: hwm,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{fingerprint::Fingerprint, kind::SourceKind, scope::Scope};
    use chrono::TimeZone;
    use proptest::prelude::*;
    use uuid::Uuid;

    fn ev(secs: i64, title: &str) -> Event {
        Event {
            id: Uuid::nil(),
            fingerprint: Fingerprint([0u8; 32]),
            source: SourceKind::Rss,
            scope: Scope::Public,
            event_time: Utc.timestamp_opt(secs, 0).single().unwrap(),
            title: title.to_owned(),
            body: None,
            links: vec![format!("https://example.com/{title}")],
            group_key: "g".to_owned(),
            entities: Vec::new(),
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
