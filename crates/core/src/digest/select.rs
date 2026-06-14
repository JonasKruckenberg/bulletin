use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::identity::CanonicalId;

/// The persisted **decision log** for one selected story (design §10.2 reason record): the reasoning
/// behind its rank that isn't otherwise recoverable from the frozen row — the Thread relevance term
/// and the resolved entity spine it scored on. Combined at render time with the item's thread
/// assignment + cross-source connections for the full "why is this here?" trace, and queryable later
/// (a future explain UI / feedback). Stored as `digest_item.reasons` jsonb.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ItemReason {
    /// The Thread relevance term that (co-)ordered it; `0` ⇒ ranked by pure recency.
    pub relevance: f32,
    /// The story's resolved entity spine — what the relevance term + thread matching keyed on.
    pub entities: Vec<CanonicalId>,
}

/// A candidate for a digest: a **story** id (a fused cross-source unit, M3), the recency key, the
/// story's entity spine (for the Thread relevance term), and the computed `relevance`. `relevance`
/// is `0` until [`apply_thread_weights`] fills it, so a digest with no thread weighting (the default,
/// or before `thread_maintenance` has run) ranks by pure recency exactly as M1–M3 did.
pub struct Candidate {
    pub id: Uuid,
    pub last_event_time: DateTime<Utc>,
    pub entities: Vec<CanonicalId>,
    pub relevance: f32,
}

impl Candidate {
    /// A candidate with no thread relevance yet — the shape linking produces.
    pub fn new(id: Uuid, last_event_time: DateTime<Utc>, entities: Vec<CanonicalId>) -> Self {
        Candidate {
            id,
            last_event_time,
            entities,
            relevance: 0.0,
        }
    }
}

/// The Thread relevance term (design `docs/thread-layer.md` §5.2): set each candidate's `relevance`
/// to `Σ_{e ∈ entities} entity_weight[e]` over the weights `thread_maintenance` projected — the
/// *rescue for the missed-because-split* case (a story advancing a thread you've invested in clears
/// the bar). Purely additive: an empty `weights` map leaves every relevance at `0`, so selection
/// degrades to pure recency.
pub fn apply_thread_weights(candidates: &mut [Candidate], weights: &BTreeMap<CanonicalId, f32>) {
    if weights.is_empty() {
        return;
    }
    for c in candidates.iter_mut() {
        c.relevance = c.entities.iter().filter_map(|e| weights.get(e)).sum();
    }
}

/// Why a candidate did or didn't make the digest: `Selected` (rendered at `position`) or `OverCap`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    Selected { position: usize },
    OverCap { rank: usize },
}

/// One entry of a digest's persisted **decision log** (design §10.2): a candidate story, its verdict
/// (selected or over-cap — so the log records the *drops* too, answering "why was X *not* in my
/// digest?"), and the reasoning behind its rank. The full `Vec<DecisionRecord>` is stored on the
/// `digest` row and is the queryable record a future explain UI / feedback loop reads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionRecord {
    pub story_id: Uuid,
    pub verdict: Verdict,
    pub reason: ItemReason,
}

/// A candidate paired with its verdict — the selection's audit trail. Carries the `relevance` that
/// ordered it, so `digest-explain` can show *why* a thread-invested story out-ranked a fresher one.
#[derive(Debug, Clone)]
pub struct Decision {
    pub id: Uuid,
    pub last_event_time: DateTime<Utc>,
    pub relevance: f32,
    pub verdict: Verdict,
}

/// Score-ordered, tiebreak-stable: highest **relevance** first, then newest, then by `id`. With every
/// relevance at `0` (no thread weighting) this is exactly the prior recency order, so the thread term
/// only ever *promotes* an invested story — it never reshuffles an un-weighted digest.
fn by_score(a: &Candidate, b: &Candidate) -> std::cmp::Ordering {
    b.relevance
        .total_cmp(&a.relevance)
        .then_with(|| b.last_event_time.cmp(&a.last_event_time))
        .then_with(|| a.id.cmp(&b.id))
}

/// Pure selection: order candidates by (relevance, recency) and cap at `max_items`, returning a
/// verdict for *every* candidate so "why is X in (or not in) the digest?" is answerable. No I/O,
/// deterministic. Output is render order — `Selected` by ascending position, then `OverCap` by
/// ascending rank.
pub fn select(candidates: Vec<Candidate>, max_items: usize) -> Vec<Decision> {
    let mut candidates = candidates;
    candidates.sort_by(by_score);
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
                relevance: c.relevance,
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
        Candidate::new(
            Uuid::from_u128(id),
            Utc.timestamp_opt(secs, 0).single().unwrap(),
            Vec::new(),
        )
    }

    fn cand_ent(id: u128, secs: i64, entities: &[&str]) -> Candidate {
        Candidate::new(
            Uuid::from_u128(id),
            Utc.timestamp_opt(secs, 0).single().unwrap(),
            entities.iter().map(|s| s.to_string()).collect(),
        )
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
        let out = select(vec![cand(1, 100), cand(2, 300), cand(3, 200)], 2);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].id, Uuid::from_u128(2));
        assert_eq!(out[0].verdict, Verdict::Selected { position: 0 });
        assert_eq!(out[2].id, Uuid::from_u128(1));
        assert_eq!(out[2].verdict, Verdict::OverCap { rank: 2 });
    }

    #[test]
    fn thread_weight_promotes_an_invested_but_older_story() {
        // id1 is oldest but sits on an invested thread; cap 1 must rescue it.
        let mut cands = vec![
            cand_ent(1, 100, &["repo:acme"]),
            cand_ent(2, 300, &["repo:misc"]),
            cand_ent(3, 200, &[]),
        ];
        let weights: BTreeMap<CanonicalId, f32> =
            [("repo:acme".to_string(), 5.0)].into_iter().collect();
        apply_thread_weights(&mut cands, &weights);
        assert_eq!(selected_ids(&select(cands, 1)), vec![Uuid::from_u128(1)]);
    }

    #[test]
    fn empty_weights_leave_pure_recency_order() {
        let mut cands = vec![cand_ent(1, 100, &["x"]), cand_ent(2, 300, &["x"])];
        apply_thread_weights(&mut cands, &BTreeMap::new());
        assert_eq!(
            selected_ids(&select(cands, 2)),
            vec![Uuid::from_u128(2), Uuid::from_u128(1)]
        );
    }

    proptest! {
        #[test]
        fn respects_cap(
            specs in prop::collection::vec((0u128..1000, 0i64..100_000), 0..50),
            cap in 0usize..30,
        ) {
            let cands: Vec<Candidate> = specs.iter().map(|&(id, t)| cand(id, t)).collect();
            let n = cands.len();
            let out = select(cands, cap);
            prop_assert_eq!(out.len(), n);
            let selected = out.iter().filter(|d| matches!(d.verdict, Verdict::Selected { .. })).count();
            prop_assert_eq!(selected, n.min(cap));
        }

        // With no weights, selection is non-increasing by recency (ties by ascending id) — identical
        // to the pre-thread pure-recency order.
        #[test]
        fn no_weights_equals_recency(specs in prop::collection::vec((0u128..50, 0i64..1000), 0..30)) {
            let cands: Vec<Candidate> = specs.iter().map(|&(id, t)| cand(id, t)).collect();
            let out = select(cands, 1000);
            for w in out.windows(2) {
                prop_assert!(
                    w[0].last_event_time > w[1].last_event_time
                        || (w[0].last_event_time == w[1].last_event_time && w[0].id <= w[1].id)
                );
            }
        }
    }
}
