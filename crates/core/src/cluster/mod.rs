//! The clustering flow: drain the event log (via the build watermark cursor) and fold each
//! dirtied within-source group into a representative `cluster` row. Consumer of the
//! ingest→clustering seam, producer of the clustering→digest seam.

pub mod store;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::{event::Event, kind::SourceKind, scope::Scope};

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

/// A cluster's scope-aware identity — the in-code mirror of the DB constraint
/// `UNIQUE(scope_kind, scope_subscriber_id, source, group_key)`. **Scope is part of the key**, so a
/// public and a private event can never fold into the same cluster: this is the typed primary
/// isolation defense, alongside the RLS that lands in Phase 4. Used by the pure scope-invariant
/// proptests below.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClusterKey {
    pub scope: Scope,
    pub source: SourceKind,
    pub group_key: String,
}

/// The cluster a finalized event belongs to. Pure; mirrors the build's grouping.
pub fn cluster_key(event: &Event) -> ClusterKey {
    ClusterKey {
        scope: event.scope.clone(),
        source: event.source,
        group_key: event.group_key.clone(),
    }
}

/// Whether a cluster of the given `scope` may appear in `subscriber`'s digest candidate set: public
/// is shared, private is owner-only. The in-code mirror of the `candidates_in_lookback` predicate
/// (`scope_kind = 'public' OR scope_subscriber_id = $sub`) — the isolation invariant the proptests
/// pin so a logic change can't silently leak another subscriber's private cluster.
pub fn visible_to(scope: &Scope, subscriber: Uuid) -> bool {
    match scope {
        Scope::Public => true,
        Scope::Private(owner) => *owner == subscriber,
    }
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
            store::upsert_cluster(&mut *tx, &Scope::Public, *source, group_key, &r)
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

/// PrivateBuild: (re)build one subscriber's private clusters just-in-time, ahead of their digest.
///
/// Unlike [`build`] this carries **no watermark and no advisory lock**: a subscriber's private
/// volume is small and the rollup is idempotent, so `GenerateDigest` simply recomputes the owner's
/// private clusters each run, then selects over `public ∪ own-private` (design §9.1). Every private
/// event is isolated by `scope_subscriber_id = subscriber_id` at the store boundary, so this can
/// only ever touch the caller's own clusters. (Phase 4 runs it inside the subscriber RLS context,
/// making that a DB-enforced guarantee rather than a query convention.)
pub async fn build_private(pool: &PgPool, subscriber_id: Uuid) -> Result<usize> {
    let groups = store::dirty_private_groups(pool, subscriber_id)
        .await
        .context("find private groups")?;

    for (source, group_key) in &groups {
        let events = store::list_private_group_events(pool, subscriber_id, *source, group_key)
            .await
            .context("load private group events")?;
        if let Some(r) = rollup(&events) {
            store::upsert_cluster(pool, &Scope::Private(subscriber_id), *source, group_key, &r)
                .await
                .context("upsert private cluster")?;
        }
    }
    Ok(groups.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{
        fingerprint::Fingerprint,
        kind::{ContentKind, SourceKind},
        scope::Scope,
    };
    use chrono::TimeZone;
    use proptest::prelude::*;
    use std::collections::{HashMap, HashSet};
    use uuid::Uuid;

    fn ev(secs: i64, title: &str) -> Event {
        ev_scoped(Scope::Public, SourceKind::Rss, "g", secs, title)
    }

    /// An `Event` with an explicit scope + group, for the isolation proptests.
    fn ev_scoped(
        scope: Scope,
        source: SourceKind,
        group_key: &str,
        secs: i64,
        title: &str,
    ) -> Event {
        Event {
            id: Uuid::nil(),
            fingerprint: Fingerprint([0u8; 32]),
            source,
            scope,
            event_time: Utc.timestamp_opt(secs, 0).single().unwrap(),
            title: title.to_owned(),
            body: None,
            links: vec![format!("https://example.com/{title}")],
            group_key: group_key.to_owned(),
            content_kind: ContentKind::Longform,
            entities: Vec::new(),
            severity_hint: None,
            ingest_time: Utc.timestamp_opt(secs, 0).single().unwrap(),
            raw: None,
        }
    }

    // Two distinct subscribers for the scope-invariant proptests.
    fn sub_a() -> Uuid {
        Uuid::from_u128(0xA)
    }
    fn sub_b() -> Uuid {
        Uuid::from_u128(0xB)
    }

    /// Maps a small Debug-able tag (`0`/`1`/`2`) to a scope drawn from `{public, private(a),
    /// private(b)}`. The proptests generate tags (which print on failure), not `Event`s.
    fn scope_of(tag: u8) -> Scope {
        match tag {
            0 => Scope::Public,
            1 => Scope::Private(sub_a()),
            _ => Scope::Private(sub_b()),
        }
    }

    /// Build a mixed multi-tenant event set from `(scope_tag, group, secs)` specs.
    fn events_of(specs: &[(u8, String, i64)]) -> Vec<Event> {
        specs
            .iter()
            .map(|(tag, group, secs)| {
                ev_scoped(scope_of(*tag), SourceKind::Github, group, *secs, "x")
            })
            .collect()
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

        // ── Scope-invariant properties (a primary isolation defense, design §12) ──

        // The public build reads only public events (`scope_kind = 'public'`) and writes Public
        // clusters. Modelled by grouping the public subset on `cluster_key`: every cluster it
        // produces is keyed Public and holds only public events — a private event can never land in
        // a public cluster, because scope is part of the cluster identity.
        #[test]
        fn public_build_never_clusters_a_private_event(
            specs in prop::collection::vec((0u8..3, "[a-c]", 0i64..1000), 0..40),
        ) {
            let events = events_of(&specs);
            let mut clusters: HashMap<ClusterKey, Vec<&Event>> = HashMap::new();
            for ev in &events {
                if matches!(ev.scope, Scope::Public) {
                    clusters.entry(cluster_key(ev)).or_default().push(ev);
                }
            }
            for (key, members) in &clusters {
                prop_assert_eq!(&key.scope, &Scope::Public);
                for m in members {
                    prop_assert!(matches!(m.scope, Scope::Public));
                }
            }
        }

        // A subscriber's digest candidate set (`public ∪ own-private`) never contains another
        // subscriber's private cluster, and never hides their own — `visible_to` is exactly the
        // `candidates_in_lookback` predicate, pinned here so a query change can't silently leak.
        #[test]
        fn candidate_set_isolates_private_clusters(
            specs in prop::collection::vec((0u8..3, "[a-c]", 0i64..1000), 0..40),
        ) {
            let events = events_of(&specs);
            let keys: HashSet<ClusterKey> = events.iter().map(cluster_key).collect();
            for key in &keys {
                match &key.scope {
                    // Public clusters are shared with everyone.
                    Scope::Public => {
                        prop_assert!(visible_to(&key.scope, sub_a()));
                        prop_assert!(visible_to(&key.scope, sub_b()));
                    }
                    // A private cluster is visible to its owner and to no one else.
                    Scope::Private(owner) => {
                        prop_assert!(visible_to(&key.scope, *owner));
                        let other = if *owner == sub_a() { sub_b() } else { sub_a() };
                        prop_assert!(!visible_to(&key.scope, other));
                    }
                }
            }
        }
    }
}
