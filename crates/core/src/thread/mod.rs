//! Threads — durable, recomputable, per-subscriber state: the persistent weave that runs the full
//! height of time (the Acme migration over months; an on-call rotation), the system's *memory*
//! (design `digest-thread-layer.md` §2).
//!
//! This module owns the **pure** maintenance algorithms — everything `thread_maintenance` computes
//! off the punctual path, all DB-free and deterministic so they can be proptested in isolation:
//!
//! - [`co_occurrence`] — build the engaged co-occurrence graph over a rolling window (nodes =
//!   canonical entities; edges = co-occurrence within a cluster / delivered item, weighted by
//!   frequency × recency).
//! - [`label_propagation`] — near-linear community detection, deterministic with a fixed node order
//!   and a stable canonical-id tie-break (Louvain is the documented quality-upgrade lever, §5.1).
//! - [`map_communities_to_threads`] — communities → threads with **stable id-forwarding** (oldest-id
//!   wins on merge; a new id on split), so deep-links and feedback targets stay stable.
//! - [`ThreadState::transition`] — the active → dormant → archived state machine.
//! - [`decay_affinity`] — per-thread affinity decay so "cared in Q1" doesn't weigh forever.
//! - [`project_weights`] — distribute each active thread's affinity across its canonical entities
//!   into the subscriber's `entity_weight` map (the fire-time relevance input).
//!
//! The DB seam (reading the co-occurrence sources, writing thread rows + the weight map) lives in
//! [`store`]; the orchestrated flow in [`maintain`].

mod maintain;
pub mod store;

pub use maintain::{maintain, MaintenanceConfig, MaintenanceStats};

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::identity::CanonicalId;

/// Whether a thread was explicitly declared by the user or emerged from community detection.
/// Declared threads are pinned-by-policy: never auto-merged or auto-archived.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThreadOrigin {
    Declared,
    Emergent,
}

impl ThreadOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            ThreadOrigin::Declared => "declared",
            ThreadOrigin::Emergent => "emergent",
        }
    }
}

/// A thread's lifecycle state. `Dormant` threads are retained and reactivatable (a story landing on
/// one earns a salience bump); `Archived` threads are excluded from active weight projection but
/// kept for reactivation, unless pinned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThreadState {
    Active,
    Dormant,
    Archived,
}

impl ThreadState {
    pub fn as_str(self) -> &'static str {
        match self {
            ThreadState::Active => "active",
            ThreadState::Dormant => "dormant",
            ThreadState::Archived => "archived",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "active" => Some(ThreadState::Active),
            "dormant" => Some(ThreadState::Dormant),
            "archived" => Some(ThreadState::Archived),
            _ => None,
        }
    }

    /// The state machine (design §5.1 step 6): a thread with no story since `dormancy_horizon` goes
    /// `dormant`; past `archive_horizon` and not pinned it goes `archived`. `pinned` (declared)
    /// threads never auto-archive — they hold `active`/`dormant` but stay in projection. A fresh
    /// story (so `last_story_time` is recent) reactivates a dormant/archived thread to `active`.
    pub fn transition(
        last_story_time: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
        pinned: bool,
        horizons: &Horizons,
    ) -> ThreadState {
        let Some(last) = last_story_time else {
            // No story ever — a freshly declared/seeded thread is active.
            return ThreadState::Active;
        };
        let age = now.signed_duration_since(last);
        if age < horizons.dormancy {
            ThreadState::Active
        } else if pinned || age < horizons.archive {
            ThreadState::Dormant
        } else {
            ThreadState::Archived
        }
    }
}

/// Dormancy / archive horizons + the affinity decay half-life — all tuning knobs (design §10), held
/// as a struct so a config table can supply them later without touching call sites.
#[derive(Debug, Clone, Copy)]
pub struct Horizons {
    pub dormancy: Duration,
    pub archive: Duration,
    /// Half-life of the per-thread affinity decay.
    pub affinity_half_life: Duration,
}

impl Default for Horizons {
    fn default() -> Self {
        Horizons {
            dormancy: Duration::days(21),
            archive: Duration::days(90),
            affinity_half_life: Duration::days(30),
        }
    }
}

/// One unit of co-occurrence evidence: the canonical entities that appeared together in a single
/// cluster or delivered digest item, with the item's recency. The maintenance flow feeds one of
/// these per own-private/engaged-public cluster and per delivered item over the rolling window.
#[derive(Debug, Clone)]
pub struct CoOccurrenceItem {
    pub entities: Vec<CanonicalId>,
    pub at: DateTime<Utc>,
}

/// A weighted, undirected co-occurrence graph keyed by canonical id. Symmetric: `weight(a,b) ==
/// weight(b,a)`. Self-edges are not stored. Node degree / presence is the union of edge endpoints
/// plus any singleton nodes that appeared alone.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CoGraph {
    pub nodes: BTreeSet<CanonicalId>,
    /// Canonical undirected key `(min, max)` → accumulated weight.
    pub edges: BTreeMap<(CanonicalId, CanonicalId), f32>,
}

impl CoGraph {
    fn add_node(&mut self, n: &CanonicalId) {
        self.nodes.insert(n.clone());
    }

    fn add_edge(&mut self, a: &CanonicalId, b: &CanonicalId, w: f32) {
        if a == b {
            return;
        }
        let key = if a < b {
            (a.clone(), b.clone())
        } else {
            (b.clone(), a.clone())
        };
        *self.edges.entry(key).or_insert(0.0) += w;
    }
}

/// Build the engaged co-occurrence graph over `items` (design §5.1 step 2). Each item contributes a
/// clique over its entities, every edge weighted by `frequency × recency` — recency is an
/// exponential decay of the item's age relative to `now` with the given `half_life`, so a topic that
/// was hot months ago fades against this week's. Resolution should already have folded aliases (feed
/// canonical-component reps as the entities), so "Acme" and "acme.com" co-occur as one node.
pub fn co_occurrence(
    items: &[CoOccurrenceItem],
    now: DateTime<Utc>,
    half_life: Duration,
) -> CoGraph {
    let hl_secs = half_life.num_seconds().max(1) as f64;
    let mut g = CoGraph::default();
    for item in items {
        // Distinct, sorted entities so a duplicated entity in one item can't double-count, and the
        // clique is built deterministically.
        let ents: Vec<&CanonicalId> = {
            let set: BTreeSet<&CanonicalId> = item.entities.iter().collect();
            set.into_iter().collect()
        };
        let age_secs = now.signed_duration_since(item.at).num_seconds().max(0) as f64;
        let recency = 0.5_f64.powf(age_secs / hl_secs) as f32;
        for e in &ents {
            g.add_node(e);
        }
        for i in 0..ents.len() {
            for j in (i + 1)..ents.len() {
                g.add_edge(ents[i], ents[j], recency);
            }
        }
    }
    g
}

/// Deterministic label propagation (design §5.1 step 4). Each node starts in its own community; each
/// pass, in fixed ascending node order, a node adopts the community with the greatest summed edge
/// weight among its neighbors **plus its own current label** (a self-weight stabilizer so a node
/// whose own community isn't represented among its neighbors doesn't thrash), ties broken by the
/// **smallest community label** — so the result is independent of hash ordering and reproducible.
/// Returns `node → community-representative id`; an isolated node is its own community.
///
/// Builds the adjacency list once (O(V+E)) and reuses it across passes, so the whole run is
/// O((V+E)·iters) — not the O(V·E·iters) a per-node edge scan would cost. Asynchronous (in-place)
/// updates converge in a handful of passes; `max_iters` caps it.
pub fn label_propagation(graph: &CoGraph, max_iters: usize) -> BTreeMap<CanonicalId, CanonicalId> {
    let mut label: BTreeMap<CanonicalId, CanonicalId> =
        graph.nodes.iter().map(|n| (n.clone(), n.clone())).collect();

    // Adjacency list, built once: node → [(neighbor, weight)]. Every edge endpoint is also seeded
    // into `label` (an edge may name a node not in `graph.nodes`).
    let mut adj: BTreeMap<CanonicalId, Vec<(CanonicalId, f32)>> = BTreeMap::new();
    for ((a, b), &w) in &graph.edges {
        label.entry(a.clone()).or_insert_with(|| a.clone());
        label.entry(b.clone()).or_insert_with(|| b.clone());
        adj.entry(a.clone()).or_default().push((b.clone(), w));
        adj.entry(b.clone()).or_default().push((a.clone(), w));
    }

    // A self-edge weight that lets a node hold its own label against weakly-tied neighbors. Small
    // relative to a real co-occurrence edge, so it only breaks otherwise-ties.
    const SELF_WEIGHT: f32 = 1e-3;

    let ordered: Vec<CanonicalId> = label.keys().cloned().collect();
    for _ in 0..max_iters {
        let mut changed = false;
        for n in &ordered {
            let Some(neighbors) = adj.get(n) else {
                continue; // isolated node keeps its own label
            };
            // Sum edge weight per candidate label, seeded with the node's own label (the stabilizer).
            let mut score: BTreeMap<CanonicalId, f32> = BTreeMap::new();
            *score.entry(label[n].clone()).or_insert(0.0) += SELF_WEIGHT;
            for (nb, w) in neighbors {
                *score.entry(label[nb].clone()).or_insert(0.0) += w;
            }
            // Pick max weight; tie-break by smallest label (BTreeMap iterates ascending).
            let best = score
                .iter()
                .fold(None::<(&CanonicalId, f32)>, |acc, (lbl, &w)| match acc {
                    Some((_, bw)) if bw >= w => acc,
                    _ => Some((lbl, w)),
                })
                .map(|(lbl, _)| lbl.clone());
            if let Some(best) = best {
                if label[n] != best {
                    label.insert(n.clone(), best);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    label
}

/// A candidate thread distilled from a community: its entity spine, sorted. Communities below the
/// minimum size are dropped (a lone entity is not a "thread of a life").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateThread {
    pub entities: Vec<CanonicalId>,
}

/// Group a label-propagation assignment into candidate threads, dropping communities smaller than
/// `min_size`. Deterministic: communities and their entities come out sorted.
pub fn communities_to_candidates(
    labels: &BTreeMap<CanonicalId, CanonicalId>,
    min_size: usize,
) -> Vec<CandidateThread> {
    let mut by_label: BTreeMap<CanonicalId, BTreeSet<CanonicalId>> = BTreeMap::new();
    for (node, label) in labels {
        by_label
            .entry(label.clone())
            .or_default()
            .insert(node.clone());
    }
    by_label
        .into_values()
        .filter(|members| members.len() >= min_size)
        .map(|members| CandidateThread {
            entities: members.into_iter().collect(),
        })
        .collect()
}

/// An existing thread's identity-relevant fields, for [`map_communities_to_threads`] id-forwarding.
#[derive(Debug, Clone, PartialEq)]
pub struct ExistingThread {
    pub id: Uuid,
    pub entities: Vec<CanonicalId>,
    pub pinned: bool,
}

/// The outcome of matching one candidate community to the prior thread set.
#[derive(Debug, Clone, PartialEq)]
pub enum ThreadMapping {
    /// The candidate matched an existing thread — keep its id, refresh its entity spine.
    Keep {
        id: Uuid,
        entities: Vec<CanonicalId>,
    },
    /// No sufficient match — a new emergent thread (the store mints the id).
    New { entities: Vec<CanonicalId> },
    /// The candidate absorbed several existing threads — oldest id wins, the rest forward into it.
    Merge {
        winner: Uuid,
        merged: Vec<Uuid>,
        entities: Vec<CanonicalId>,
    },
}

/// Map candidate communities onto the prior thread set with **stable id-forwarding** (design §5.1
/// step 5). A candidate is matched to existing threads by entity-spine overlap (Jaccard ≥
/// `match_threshold`); the matched thread keeps its id. When a candidate overlaps several existing
/// threads, they merge — the **oldest id wins** (UUIDv7 is time-ordered, so the smallest id is
/// oldest) and the rest are reported as `merged` for `merged_into` forwarding. Unmatched candidates
/// are `New`. Deterministic; pinned existing threads are never merged away (they may match, never
/// be a loser).
pub fn map_communities_to_threads(
    candidates: &[CandidateThread],
    existing: &[ExistingThread],
    match_threshold: f32,
) -> Vec<ThreadMapping> {
    let mut mappings = Vec::with_capacity(candidates.len());
    let mut claimed: BTreeSet<Uuid> = BTreeSet::new();

    for cand in candidates {
        let cand_set: BTreeSet<&CanonicalId> = cand.entities.iter().collect();
        // All existing threads with sufficient overlap, not already claimed by another candidate.
        let mut matches: Vec<&ExistingThread> = existing
            .iter()
            .filter(|e| !claimed.contains(&e.id))
            .filter(|e| jaccard(&cand_set, &e.entities) >= match_threshold)
            .collect();
        // Oldest id first (UUIDv7 time-ordered), so the winner is deterministic and stable.
        matches.sort_by_key(|e| e.id);

        let mapping = match matches.as_slice() {
            [] => ThreadMapping::New {
                entities: cand.entities.clone(),
            },
            [only] => {
                claimed.insert(only.id);
                ThreadMapping::Keep {
                    id: only.id,
                    entities: cand.entities.clone(),
                }
            }
            many => {
                // Oldest non-pinned wins if any are pinned we still keep the oldest overall as winner
                // but never list a pinned thread among the merged losers (it stays independent).
                let winner = many[0].id;
                let merged: Vec<Uuid> = many[1..]
                    .iter()
                    .filter(|e| !e.pinned)
                    .map(|e| e.id)
                    .collect();
                for e in many {
                    if e.id == winner || !e.pinned {
                        claimed.insert(e.id);
                    }
                }
                ThreadMapping::Merge {
                    winner,
                    merged,
                    entities: cand.entities.clone(),
                }
            }
        };
        mappings.push(mapping);
    }
    mappings
}

fn jaccard(a: &BTreeSet<&CanonicalId>, b: &[CanonicalId]) -> f32 {
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    let b_set: BTreeSet<&CanonicalId> = b.iter().collect();
    let inter = a.intersection(&b_set).count();
    let union = a.union(&b_set).count();
    if union == 0 {
        0.0
    } else {
        inter as f32 / union as f32
    }
}

/// Decay a thread's prior affinity toward zero by an exponential half-life over the elapsed time,
/// then add the period's fresh signal (design §5.1 step 7): `affinity = decay(prior) + delta`,
/// bounded to `[0, max]`. Per-thread decay is what fixes "cared in Q1, weighted forever."
pub fn decay_affinity(
    prior: f32,
    elapsed: Duration,
    half_life: Duration,
    delta: f32,
    max: f32,
) -> f32 {
    let hl = half_life.num_seconds().max(1) as f64;
    let e = elapsed.num_seconds().max(0) as f64;
    let decayed = prior as f64 * 0.5_f64.powf(e / hl);
    ((decayed as f32) + delta).clamp(0.0, max)
}

/// Project active threads' affinity onto a per-entity weight map (design §5.1 step 8): each active
/// thread distributes its affinity across its canonical entities (evenly — a uniform prior; a
/// frequency-weighted split is a later refinement), and an entity shared by several threads
/// accumulates their contributions. Archived threads are excluded; dormant threads contribute at a
/// reduced rate so a reactivation still has some pull. The result is the fire-time `entity_weight`
/// map the relevance term sums over.
pub fn project_weights(
    threads: &[(ThreadState, f32, Vec<CanonicalId>)],
) -> BTreeMap<CanonicalId, f32> {
    let mut weights: BTreeMap<CanonicalId, f32> = BTreeMap::new();
    for (state, affinity, entities) in threads {
        let factor = match state {
            ThreadState::Active => 1.0,
            ThreadState::Dormant => 0.25,
            ThreadState::Archived => continue,
        };
        if entities.is_empty() || *affinity <= 0.0 {
            continue;
        }
        let share = affinity * factor / entities.len() as f32;
        for e in entities {
            *weights.entry(e.clone()).or_insert(0.0) += share;
        }
    }
    weights
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use proptest::prelude::*;

    fn ids(xs: &[&str]) -> Vec<CanonicalId> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    fn at(day: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(day * 86_400, 0).single().unwrap()
    }

    #[test]
    fn co_occurrence_builds_cliques_with_recency() {
        let now = at(100);
        let items = vec![
            CoOccurrenceItem {
                entities: ids(&["a", "b", "c"]),
                at: at(100), // fresh
            },
            CoOccurrenceItem {
                entities: ids(&["a", "b"]),
                at: at(70), // 30d old → ~half weight at 30d half-life
            },
        ];
        let g = co_occurrence(&items, now, Duration::days(30));
        // a-b got contributions from both items; a-c only from the fresh one.
        let ab = g.edges[&("a".to_string(), "b".to_string())];
        let ac = g.edges[&("a".to_string(), "c".to_string())];
        assert!(ab > ac, "a-b ({ab}) should outweigh a-c ({ac})");
        assert!(g.nodes.contains("c"));
    }

    #[test]
    fn label_propagation_separates_two_cliques() {
        // Two dense triangles joined by nothing → two communities.
        let items = vec![
            CoOccurrenceItem {
                entities: ids(&["a", "b", "c"]),
                at: at(100),
            },
            CoOccurrenceItem {
                entities: ids(&["x", "y", "z"]),
                at: at(100),
            },
        ];
        let g = co_occurrence(&items, at(100), Duration::days(30));
        let labels = label_propagation(&g, 20);
        // a,b,c share a label; x,y,z share another; the two differ.
        assert_eq!(labels["a"], labels["b"]);
        assert_eq!(labels["b"], labels["c"]);
        assert_eq!(labels["x"], labels["y"]);
        assert_ne!(labels["a"], labels["x"]);

        let cands = communities_to_candidates(&labels, 2);
        assert_eq!(cands.len(), 2);
    }

    #[test]
    fn state_machine_dormant_then_archived() {
        let h = Horizons::default();
        let now = at(200);
        // 5 days since last story → active.
        assert_eq!(
            ThreadState::transition(Some(at(195)), now, false, &h),
            ThreadState::Active
        );
        // 30 days → past dormancy (21d), before archive (90d) → dormant.
        assert_eq!(
            ThreadState::transition(Some(at(170)), now, false, &h),
            ThreadState::Dormant
        );
        // 120 days, not pinned → archived.
        assert_eq!(
            ThreadState::transition(Some(at(80)), now, false, &h),
            ThreadState::Archived
        );
        // 120 days but pinned → never archives, holds dormant.
        assert_eq!(
            ThreadState::transition(Some(at(80)), now, true, &h),
            ThreadState::Dormant
        );
    }

    #[test]
    fn id_forwarding_keeps_existing_and_merges_oldest_wins() {
        let cand = CandidateThread {
            entities: ids(&["a", "b", "c", "d"]),
        };
        let old = Uuid::from_u128(1);
        let newer = Uuid::from_u128(2);
        let existing = vec![
            ExistingThread {
                id: old,
                entities: ids(&["a", "b"]),
                pinned: false,
            },
            ExistingThread {
                id: newer,
                entities: ids(&["c", "d"]),
                pinned: false,
            },
        ];
        let mappings = map_communities_to_threads(&[cand], &existing, 0.4);
        assert_eq!(mappings.len(), 1);
        match &mappings[0] {
            ThreadMapping::Merge { winner, merged, .. } => {
                assert_eq!(*winner, old); // oldest id wins
                assert_eq!(merged, &vec![newer]);
            }
            other => panic!("expected merge, got {other:?}"),
        }
    }

    #[test]
    fn unmatched_candidate_is_new() {
        let cand = CandidateThread {
            entities: ids(&["p", "q"]),
        };
        let existing = vec![ExistingThread {
            id: Uuid::from_u128(1),
            entities: ids(&["x", "y"]),
            pinned: false,
        }];
        let mappings = map_communities_to_threads(&[cand], &existing, 0.4);
        assert!(matches!(mappings[0], ThreadMapping::New { .. }));
    }

    #[test]
    fn affinity_decays_and_accumulates_bounded() {
        // After one half-life, prior 1.0 decays to ~0.5; +0.3 delta → ~0.8.
        let a = decay_affinity(1.0, Duration::days(30), Duration::days(30), 0.3, 10.0);
        assert!((a - 0.8).abs() < 0.05, "got {a}");
        // Clamped to max.
        let capped = decay_affinity(100.0, Duration::zero(), Duration::days(30), 50.0, 10.0);
        assert_eq!(capped, 10.0);
    }

    #[test]
    fn project_weights_distributes_and_excludes_archived() {
        let threads = vec![
            (ThreadState::Active, 1.0, ids(&["a", "b"])),
            (ThreadState::Archived, 5.0, ids(&["c"])),
            (ThreadState::Dormant, 1.0, ids(&["a"])),
        ];
        let w = project_weights(&threads);
        // Active thread: 1.0 / 2 = 0.5 each; dormant adds 0.25 * 1.0 / 1 = 0.25 to "a".
        assert!((w["a"] - 0.75).abs() < 1e-6);
        assert!((w["b"] - 0.5).abs() < 1e-6);
        // Archived entity contributes nothing.
        assert!(!w.contains_key("c"));
    }

    proptest! {
        // Label propagation is deterministic: same graph in, same labels out, regardless of how many
        // (capped) iterations we allow beyond convergence.
        #[test]
        fn label_propagation_is_deterministic(
            edges in prop::collection::vec((0u8..6, 0u8..6), 0..18),
        ) {
            let items: Vec<CoOccurrenceItem> = edges
                .iter()
                .filter(|(a, b)| a != b)
                .map(|(a, b)| CoOccurrenceItem {
                    entities: ids(&[&format!("n{a}"), &format!("n{b}")]),
                    at: at(100),
                })
                .collect();
            let g = co_occurrence(&items, at(100), Duration::days(30));
            let r1 = label_propagation(&g, 50);
            let r2 = label_propagation(&g, 50);
            prop_assert_eq!(r1, r2);
        }

        // co_occurrence is symmetric and never stores a self-edge.
        #[test]
        fn co_graph_symmetric_no_self_edges(
            groups in prop::collection::vec(prop::collection::vec(0u8..5, 0..4), 0..8),
        ) {
            let items: Vec<CoOccurrenceItem> = groups
                .iter()
                .map(|g| CoOccurrenceItem {
                    entities: g.iter().map(|n| format!("n{n}")).collect(),
                    at: at(100),
                })
                .collect();
            let graph = co_occurrence(&items, at(100), Duration::days(30));
            for (a, b) in graph.edges.keys() {
                prop_assert!(a < b, "edge key must be canonically ordered");
            }
        }
    }
}
