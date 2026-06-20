//! The clustering flow: drain the event log (via the build watermark cursor) and fold each
//! dirtied within-source group into a representative `cluster` row. Consumer of the
//! ingest→clustering seam, producer of the clustering→digest seam.

pub mod store;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::{
    db::{begin_scope, ScopeCtx},
    event::Event,
    kind::{ContentKind, SourceKind},
    scope::Scope,
};

/// The recomputed rollup the build upserts onto a cluster row: the latest event's title/link, the
/// group's recency span, the union of its events' `entities` (the blocking substrate M3 linking runs
/// on, §8.2), and the M4 scoring signals — `event_count` + `content_depth` (richness) and
/// `max_severity` (priority). All folded from the same group scan, so the richer signals are free
/// (design §8.3, M3-handoff seam #1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterRollup {
    /// Representative title — the latest event's title.
    pub title: String,
    /// Representative link — the latest event's primary link, if any.
    pub link: Option<String>,
    /// Earliest event_time in the group — the start of its recency span.
    pub first_event_time: DateTime<Utc>,
    /// Most recent event_time in the group — the selection ordering key.
    pub last_event_time: DateTime<Utc>,
    /// Sorted, de-duplicated union of the group's event entities — the linking blocking key.
    pub entities: Vec<String>,
    /// Number of events folded into this cluster — the breadth half of richness (design §8.3).
    pub event_count: i32,
    /// Max `content_kind` over the group (`Message < Announcement < Longform`) — the depth signal.
    pub content_depth: ContentKind,
    /// Max source-provided `severity_hint` over the group, or `None` — a priority boost input.
    pub max_severity: Option<i16>,
    /// The representative event's originating `connection` — the source-subscription key a public
    /// cluster is filtered by. `None` for a group of pre-attribution events (no connection stamped).
    pub connection_id: Option<Uuid>,
}

/// Pure rollup: fold a within-source group of events into a cluster rollup. The representative
/// is the latest event by `event_time` (ties → last in input order, deterministic given the
/// store loads a group ordered by `(event_time, id)`). Returns `None` for an empty group.
pub fn rollup(events: &[Event]) -> Option<ClusterRollup> {
    let mut representative = events.first()?;
    let mut first = representative.event_time;
    let mut entities: Vec<String> = Vec::new();
    // Depth is the max content_kind; the conservative floor is the lowest variant (Message), lifted
    // by every event. Severity is the max over the events that carried a hint (None if none did).
    let mut content_depth = ContentKind::Message;
    let mut max_severity: Option<i16> = None;
    for ev in events {
        if ev.event_time >= representative.event_time {
            representative = ev;
        }
        first = first.min(ev.event_time);
        entities.extend(ev.entities.iter().cloned());
        content_depth = content_depth.max(ev.content_kind);
        max_severity = max_severity.max(ev.severity_hint);
    }
    entities.sort();
    entities.dedup();
    Some(ClusterRollup {
        title: representative.title.clone(),
        link: representative.links.first().cloned(),
        first_event_time: first,
        last_event_time: representative.event_time,
        entities,
        event_count: events.len() as i32,
        content_depth,
        max_severity,
        // The representative (latest) event's source — a within-source group shares it; for the
        // single-article public RSS cluster this is exactly the feed the subscriber subscribes to.
        connection_id: representative.connection_id,
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
/// is shared, private is owner-only. The in-code mirror of the candidate-cluster predicate
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
    // PublicBuild runs in the no-subscriber RLS context: it can read and write only public rows, so
    // it physically cannot pull a private event into a public cluster (design §12).
    let mut tx = begin_scope(pool, ScopeCtx::NoSubscriber)
        .await
        .context("begin build txn")?;

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
    let groups = store::dirty_groups(&mut *tx, &Scope::Public, built_through, hwm)
        .await
        .context("find dirty groups")?;

    build_groups(&mut tx, &Scope::Public, &groups).await?;

    store::advance_build_watermark(&mut *tx, hwm)
        .await
        .context("advance watermark")?;
    tx.commit().await.context("commit build txn")?;

    Ok(Some(BuildStats {
        dirty_groups: groups.len(),
        built_through: hwm,
    }))
}

/// The per-group build step both [`build`] and [`build_private`] run: recompute each dirtied group's
/// rollup over *all* its (scoped) events and upsert the cluster. Only the scope, lock, bounds, and
/// watermark differ between the two builds; this is the shared body, so a change to how a group is
/// folded lands in one place. Reads and writes are both fenced by `scope`, so a private build can
/// only touch its owner's clusters.
async fn build_groups(
    conn: &mut sqlx::PgConnection,
    scope: &Scope,
    groups: &[(SourceKind, String)],
) -> Result<()> {
    for (source, group_key) in groups {
        let events = store::list_group_events(&mut *conn, scope, *source, group_key)
            .await
            .context("load group events")?;
        if let Some(r) = rollup(&events) {
            store::upsert_cluster(&mut *conn, scope, *source, group_key, &r)
                .await
                .context("upsert cluster")?;
        }
    }
    Ok(())
}

/// PrivateBuild: drain one subscriber's new private events into their private clusters, ahead of
/// their digest. The per-subscriber counterpart to [`build`], with the same shape — a transaction
/// holding a per-subscriber advisory lock (concurrent builds for the same subscriber serialize; the
/// loser returns `Ok(0)`), the half-open ingest range `(built_through, now()]` from the subscriber's
/// own `private_build_watermark`, recompute each dirtied group's rollup, upsert, advance the cursor.
///
/// Watermark-bounded so it scales with *new* private activity, not lifetime history, and a quiet
/// private cluster ages out of the candidate floor exactly like a public one (its `updated_at` is
/// only bumped when a new event re-dirties its group). Every read/write is fenced by
/// `scope_subscriber_id = subscriber_id`, so this can only touch the caller's own clusters — Phase 4
/// makes that a DB-enforced (RLS) guarantee rather than a query convention. Returns the number of
/// groups rebuilt this pass.
pub async fn build_private(pool: &PgPool, subscriber_id: Uuid) -> Result<usize> {
    // PrivateBuild runs in the owner's subscriber RLS context: it reads only their private events and
    // writes only their private clusters — the DB enforces the per-tenant boundary the query
    // predicates assert.
    let mut tx = begin_scope(pool, ScopeCtx::Subscriber(subscriber_id))
        .await
        .context("begin private build txn")?;

    if !store::try_private_build_lock(&mut *tx, subscriber_id)
        .await
        .context("acquire private build lock")?
    {
        tracing::debug!(%subscriber_id, "private build already in progress; skipping");
        return Ok(0);
    }

    let (built_through, hwm) = store::private_build_bounds(&mut *tx, subscriber_id)
        .await
        .context("read private build bounds")?;
    let scope = Scope::Private(subscriber_id);
    let groups = store::dirty_groups(&mut *tx, &scope, built_through, hwm)
        .await
        .context("find dirty private groups")?;

    build_groups(&mut tx, &scope, &groups).await?;

    store::advance_private_build_watermark(&mut *tx, subscriber_id, hwm)
        .await
        .context("advance private watermark")?;
    tx.commit().await.context("commit private build txn")?;

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
            connection_id: None,
            full_text: None,
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
    fn rollup_folds_scoring_signals() {
        // event_count = group size; content_depth = max content_kind; max_severity = max over hints.
        let mut a = ev(100, "a");
        a.content_kind = ContentKind::Message;
        a.severity_hint = Some(2);
        let mut b = ev(200, "b");
        b.content_kind = ContentKind::Longform; // the depth ceiling
        let mut c = ev(150, "c");
        c.content_kind = ContentKind::Announcement;
        c.severity_hint = Some(5); // the severity ceiling
        let r = rollup(&[a, b, c]).unwrap();
        assert_eq!(r.event_count, 3);
        assert_eq!(r.content_depth, ContentKind::Longform);
        assert_eq!(r.max_severity, Some(5));
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
        // candidate-cluster predicate, pinned here so a query change can't silently leak.
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
