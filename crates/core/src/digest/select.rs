use chrono::{DateTime, Utc};
use uuid::Uuid;

/// A candidate for a digest: an id (a **story** id since M3 — a fused cross-source unit) and the
/// recency key selection orders by. Selection is generic over what the id names; pre-M3 it was a
/// cluster, now it is a story (one or more clusters linked together).
pub struct Candidate {
    pub id: Uuid,
    pub last_event_time: DateTime<Utc>,
}

/// Why a candidate did or didn't make the digest. M1 ranks purely by recency and caps at
/// `max_items`, so a candidate is either `Selected` (rendered at `position`) or `OverCap`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Made the cut — rendered at this 0-based position.
    Selected { position: usize },
    /// Ranked past the cap; `rank` is its 0-based recency rank.
    OverCap { rank: usize },
}

/// A candidate paired with its verdict — the selection's audit trail.
#[derive(Debug, Clone)]
pub struct Decision {
    pub id: Uuid,
    pub last_event_time: DateTime<Utc>,
    pub verdict: Verdict,
}

/// Recency-ordered, tiebreak-stable: newest first, ties broken by `id`.
fn by_recency(a: &Candidate, b: &Candidate) -> std::cmp::Ordering {
    b.last_event_time
        .cmp(&a.last_event_time)
        .then_with(|| a.id.cmp(&b.id))
}

/// Pure selection: order candidates newest-first and cap at `max_items`, returning a verdict for
/// *every* candidate so "why is X in (or not in) the digest?" is answerable. No I/O, deterministic.
/// Output is render order — `Selected` by ascending position, then `OverCap` by ascending rank.
/// `digest::run` keeps the `Selected` ids; `debug digest-explain` shows the whole trace.
pub fn select(candidates: Vec<Candidate>, max_items: usize) -> Vec<Decision> {
    let mut candidates = candidates;
    candidates.sort_by(by_recency);
    candidates
        .into_iter()
        .enumerate()
        .map(|(rank, c)| {
            let verdict = if rank < max_items {
                Verdict::Selected { position: rank }
            } else {
                Verdict::OverCap { rank }
            };
            Decision {
                id: c.id,
                last_event_time: c.last_event_time,
                verdict,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use proptest::prelude::*;

    fn cand(id: u128, secs: i64) -> Candidate {
        Candidate {
            id: Uuid::from_u128(id),
            last_event_time: Utc.timestamp_opt(secs, 0).single().unwrap(),
        }
    }

    fn selected_ids(decisions: &[Decision]) -> Vec<Uuid> {
        decisions
            .iter()
            .filter_map(|d| matches!(d.verdict, Verdict::Selected { .. }).then_some(d.id))
            .collect()
    }

    #[test]
    fn orders_newest_first_and_caps() {
        let out = select(vec![cand(1, 100), cand(2, 300), cand(3, 200)], 2);
        assert_eq!(
            selected_ids(&out),
            vec![Uuid::from_u128(2), Uuid::from_u128(3)]
        );
    }

    #[test]
    fn explains_every_candidate_with_a_verdict() {
        // newest→oldest: id2(300), id3(200), id1(100); cap 2 → id1 is over-cap.
        let out = select(vec![cand(1, 100), cand(2, 300), cand(3, 200)], 2);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].id, Uuid::from_u128(2));
        assert_eq!(out[0].verdict, Verdict::Selected { position: 0 });
        assert_eq!(out[1].id, Uuid::from_u128(3));
        assert_eq!(out[1].verdict, Verdict::Selected { position: 1 });
        assert_eq!(out[2].id, Uuid::from_u128(1));
        assert_eq!(out[2].verdict, Verdict::OverCap { rank: 2 });
    }

    proptest! {
        // Every candidate is accounted for; exactly min(n, cap) are selected, the rest over-cap.
        #[test]
        fn respects_cap(
            specs in prop::collection::vec((0u128..1000, 0i64..100_000), 0..50),
            cap in 0usize..30,
        ) {
            let cands: Vec<Candidate> = specs.iter().map(|&(id, t)| cand(id, t)).collect();
            let n = cands.len();
            let out = select(cands, cap);
            prop_assert_eq!(out.len(), n);
            let selected = out
                .iter()
                .filter(|d| matches!(d.verdict, Verdict::Selected { .. }))
                .count();
            prop_assert_eq!(selected, n.min(cap));
        }
    }
}
