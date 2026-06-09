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

/// Pure selection: gate on `relevance_floor`, order by recency (newest first), cap at
/// `max_items`. Total over precomputed features — no I/O, deterministic, proptestable.
/// Ties on `last_event_time` break by `cluster_id` for a stable total order.
pub fn select(mut candidates: Vec<Candidate>, cfg: &Selection) -> Vec<Id<Cluster>> {
    candidates.retain(|c| c.relevance >= cfg.relevance_floor);
    candidates.sort_by(|a, b| {
        b.last_event_time
            .cmp(&a.last_event_time)
            .then_with(|| a.cluster_id.cmp(&b.cluster_id))
    });
    candidates.truncate(cfg.max_items);
    candidates.into_iter().map(|c| c.cluster_id).collect()
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
        let cfg = Selection { relevance_floor: 0.0, max_items: 2 };
        let out = select(cands, &cfg);
        assert_eq!(out, vec![Id::new(Uuid::from_u128(2)), Id::new(Uuid::from_u128(3))]);
    }

    #[test]
    fn gates_below_floor() {
        let cands = vec![cand(1, 100, 0.0), cand(2, 200, 1.0)];
        let cfg = Selection { relevance_floor: 0.5, max_items: 10 };
        let out = select(cands, &cfg);
        assert_eq!(out, vec![Id::new(Uuid::from_u128(2))]);
    }

    proptest! {
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
