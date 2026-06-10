use chrono::{DateTime, Utc};

use crate::{cluster::Cluster, id::Id};

/// The precomputed features a selection decision reads. M1 carries a `relevance` stub
/// (always 1.0); M3/M4 thicken this — affinity, entity match, scope bonus — without
/// changing the call site (design §8.4: pure over precomputed features from day one).
pub struct Candidate {
    pub cluster_id: Id<Cluster>,
    pub last_event_time: DateTime<Utc>,
    pub relevance: f32,
}

/// Selection knobs. M1: floor 0.0 (everything recent passes), cap from the subscriber.
pub struct Selection {
    pub relevance_floor: f32,
    pub max_items: usize,
}

/// Why a candidate did or didn't make the digest — the selection's audit trail. `select()`
/// keeps only the surviving ids; `select_explained()` keeps the verdict for *every* candidate,
/// so "why is X in (or not in) the digest?" is answerable. M1 only ever yields `Selected`/
/// `OverCap` (floor 0.0, relevance stub 1.0); `BelowFloor` lights up once M4 fills relevance in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Made the cut — rendered at this 0-based position.
    Selected { position: usize },
    /// Passed the floor but ranked past the cap; `rank` is its 0-based recency rank.
    OverCap { rank: usize },
    /// Relevance below the floor — gated out before ordering.
    BelowFloor,
}

/// A candidate paired with its verdict and the features that drove it.
#[derive(Debug, Clone)]
pub struct Decision {
    pub cluster_id: Id<Cluster>,
    pub last_event_time: DateTime<Utc>,
    pub relevance: f32,
    pub verdict: Verdict,
}

/// The recency tiebreak-stable ordering selection imposes: newest first, ties broken by
/// `cluster_id` for a stable total order. Shared by the gate-and-cap below.
fn by_recency(a: &Candidate, b: &Candidate) -> std::cmp::Ordering {
    b.last_event_time
        .cmp(&a.last_event_time)
        .then_with(|| a.cluster_id.cmp(&b.cluster_id))
}

/// Pure selection that records *why* each candidate was kept or dropped: same gate / order /
/// cap as [`select`], but **total** over the input — every candidate comes back with a
/// [`Verdict`]. No I/O, deterministic, proptestable. Output order is the digest's render order
/// (selected by ascending position), then the near-misses (over-cap by ascending rank), then
/// below-floor (input order) — i.e. read top-down to see exactly where the cut fell.
pub fn select_explained(candidates: Vec<Candidate>, cfg: &Selection) -> Vec<Decision> {
    let (mut eligible, ineligible): (Vec<Candidate>, Vec<Candidate>) = candidates
        .into_iter()
        .partition(|c| c.relevance >= cfg.relevance_floor);
    eligible.sort_by(by_recency);

    let decide = |c: Candidate, verdict: Verdict| Decision {
        cluster_id: c.cluster_id,
        last_event_time: c.last_event_time,
        relevance: c.relevance,
        verdict,
    };

    let mut decisions = Vec::with_capacity(eligible.len() + ineligible.len());
    for (rank, c) in eligible.into_iter().enumerate() {
        let verdict = if rank < cfg.max_items {
            Verdict::Selected { position: rank }
        } else {
            Verdict::OverCap { rank }
        };
        decisions.push(decide(c, verdict));
    }
    decisions.extend(
        ineligible
            .into_iter()
            .map(|c| decide(c, Verdict::BelowFloor)),
    );
    decisions
}

/// Pure selection: the cluster ids that make the digest, in render order. A thin projection of
/// [`select_explained`] — one source of truth for the gate / order / cap.
pub fn select(candidates: Vec<Candidate>, cfg: &Selection) -> Vec<Id<Cluster>> {
    select_explained(candidates, cfg)
        .into_iter()
        .filter_map(|d| matches!(d.verdict, Verdict::Selected { .. }).then_some(d.cluster_id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use proptest::prelude::*;
    use uuid::Uuid;

    fn cand(id: u128, secs: i64, relevance: f32) -> Candidate {
        Candidate {
            cluster_id: Id::new(Uuid::from_u128(id)),
            last_event_time: Utc.timestamp_opt(secs, 0).single().unwrap(),
            relevance,
        }
    }

    #[test]
    fn orders_newest_first_and_caps() {
        let cands = vec![cand(1, 100, 1.0), cand(2, 300, 1.0), cand(3, 200, 1.0)];
        let cfg = Selection {
            relevance_floor: 0.0,
            max_items: 2,
        };
        let out = select(cands, &cfg);
        assert_eq!(
            out,
            vec![Id::new(Uuid::from_u128(2)), Id::new(Uuid::from_u128(3))]
        );
    }

    #[test]
    fn gates_below_floor() {
        let cands = vec![cand(1, 100, 0.0), cand(2, 200, 1.0)];
        let cfg = Selection {
            relevance_floor: 0.5,
            max_items: 10,
        };
        let out = select(cands, &cfg);
        assert_eq!(out, vec![Id::new(Uuid::from_u128(2))]);
    }

    #[test]
    fn explains_every_candidate_with_a_verdict() {
        // newest→oldest: id2(300), id3(200), id1(100); id4 is below floor.
        let cands = vec![
            cand(1, 100, 1.0),
            cand(2, 300, 1.0),
            cand(3, 200, 1.0),
            cand(4, 400, 0.0),
        ];
        let cfg = Selection {
            relevance_floor: 0.5,
            max_items: 2,
        };
        let out = select_explained(cands, &cfg);

        // Every candidate accounted for, render order first.
        assert_eq!(out.len(), 4);
        assert_eq!(out[0].cluster_id, Id::new(Uuid::from_u128(2)));
        assert_eq!(out[0].verdict, Verdict::Selected { position: 0 });
        assert_eq!(out[1].cluster_id, Id::new(Uuid::from_u128(3)));
        assert_eq!(out[1].verdict, Verdict::Selected { position: 1 });
        assert_eq!(out[2].cluster_id, Id::new(Uuid::from_u128(1)));
        assert_eq!(out[2].verdict, Verdict::OverCap { rank: 2 });
        assert_eq!(out[3].cluster_id, Id::new(Uuid::from_u128(4)));
        assert_eq!(out[3].verdict, Verdict::BelowFloor);
    }

    proptest! {
        // `select` is exactly the `Selected` projection of `select_explained` (one source of truth).
        #[test]
        fn select_is_the_selected_projection(
            specs in prop::collection::vec((0u128..1000, 0i64..100_000, 0.0f32..1.0), 0..50),
            floor in 0.0f32..1.0,
            cap in 0usize..30,
        ) {
            let cands: Vec<Candidate> = specs.iter().map(|&(id, t, r)| cand(id, t, r)).collect();
            let explained = select_explained(
                specs.iter().map(|&(id, t, r)| cand(id, t, r)).collect(),
                &Selection { relevance_floor: floor, max_items: cap },
            );
            let projected: Vec<Id<Cluster>> = explained
                .into_iter()
                .filter_map(|d| matches!(d.verdict, Verdict::Selected { .. }).then_some(d.cluster_id))
                .collect();
            let direct = select(cands, &Selection { relevance_floor: floor, max_items: cap });
            prop_assert_eq!(direct, projected);
        }

        // Output never exceeds the cap, and is a subset of the above-floor inputs.
        #[test]
        fn respects_cap_and_floor(
            specs in prop::collection::vec((0u128..1000, 0i64..100_000, 0.0f32..1.0), 0..50),
            floor in 0.0f32..1.0,
            cap in 0usize..30,
        ) {
            let cands: Vec<Candidate> = specs.iter().map(|&(id, t, r)| cand(id, t, r)).collect();
            let eligible = specs.iter().filter(|&&(_, _, r)| r >= floor).count();
            let cfg = Selection { relevance_floor: floor, max_items: cap };
            let out = select(cands, &cfg);
            prop_assert!(out.len() <= cap);
            prop_assert_eq!(out.len(), eligible.min(cap));
        }
    }
}
