//! Selection-quality evaluation (the eval harness, design §10.3 / `local-ml-options.md` §0.1 Phase 0):
//! a **pure** read over the persisted decision log (`digest.decisions`, §10.2) + the story-level
//! `feedback` log. It is the keystone the doctrine rests on — *nothing below is tunable without it* —
//! so it ships before any ML, gives immediate value (volume/structure metrics to tune the scorer's
//! still-guessed constants — `relevance_floor`, the caps, the decay half-lives — against real digests),
//! and auto-fills the feedback-based metrics (precision, nDCG) the moment a feedback surface exists.
//!
//! Like [`super::select::select`], it is a pure function over precomputed records — no I/O, fixture-
//! testable — so a config sweep can replay it offline. Honest limit (design §10.3): feedback only
//! exists on *shown* items, so this measures the **precision family**, never recall; a story we wrongly
//! *dropped* leaves no feedback to catch it. Recall waits on consented audit-digests / the entropy
//! budget (design §14).

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::digest::select::{
    select, DecisionRecord, DropCause, Format, ItemReason, ReplaySnapshot, ScoringConfig, Verdict,
};

/// Re-score a persisted [`ReplaySnapshot`] under a trial config — the offline config-sweep primitive.
/// Pure: runs the *same* [`select`] the live path runs, over the frozen candidate input, returning a
/// fresh decision log to feed [`evaluate`]. This is what lets an operator A/B a `digest_config` change
/// over real history without a deploy (and how a later ML signal proves its marginal lift). Entities
/// are dropped from the replayed reason (metrics don't read them); the rest of the reason is faithful.
pub fn replay(snapshot: &ReplaySnapshot, cfg: &ScoringConfig) -> Vec<DecisionRecord> {
    select(
        snapshot.candidates.clone(),
        cfg,
        snapshot.max_items,
        snapshot.now,
    )
    .into_iter()
    .map(|d| DecisionRecord {
        story_id: d.id,
        verdict: d.verdict,
        reason: ItemReason {
            relevance: d.relevance,
            format: d.format,
            richness: d.richness,
            priority: d.priority,
            entities: Vec::new(),
        },
    })
    .collect()
}

/// A user's verdict on a *shown* story, derived from the `feedback` log. Entity-level
/// `must_link`/`cannot_link` are not story grades (they act on the identity graph) and map to `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grade {
    /// `care_more` — a true positive (and a "rank this higher" signal).
    Up,
    /// `care_less` / `done` — a false positive (shouldn't have shown / shown too high).
    Down,
}

impl Grade {
    /// Map a `feedback.signal` to a story grade, or `None` for the entity-link signals.
    pub fn from_signal(signal: &str) -> Option<Grade> {
        match signal {
            "care_more" => Some(Grade::Up),
            "care_less" | "done" => Some(Grade::Down),
            _ => None,
        }
    }

    /// nDCG gain. An ungraded shown item sits between the two (see [`UNGRADED_GAIN`]).
    fn gain(self) -> f64 {
        match self {
            Grade::Up => 2.0,
            Grade::Down => 0.0,
        }
    }
}

/// nDCG gain for a *shown but un-reacted-to* story — between `Down` (0) and `Up` (2), so ranking a
/// `care_less` above an ungraded item, or an ungraded above a `care_more`, both cost nDCG.
const UNGRADED_GAIN: f64 = 1.0;

/// The computed metrics over a window of digests. The structural block needs no feedback (useful from
/// day one for tuning volume/format balance); the feedback block is `None`/0 until feedback flows.
#[derive(Debug, Clone, PartialEq)]
pub struct Metrics {
    // ── structure / volume (no feedback required) ───────────────────────────
    /// Digests evaluated.
    pub digests: usize,
    /// Σ candidate stories considered (every decision, drops included).
    pub candidates: usize,
    /// Σ stories rendered (verdict `Selected`).
    pub selected: usize,
    /// Σ stories that lost a cap race (verdict `OverCap`).
    pub over_cap: usize,
    /// Σ stories gated out below the relevance floor.
    pub dropped_below_floor: usize,
    /// Selected stories classified as a Story / as a Note (catches an all-Story or all-Note digest).
    pub selected_stories: usize,
    pub selected_notes: usize,
    /// Digests that rendered nothing.
    pub empty_digests: usize,
    /// Digests that hit a cap (≥1 `OverCap`) — the dial for "am I capping too hard?".
    pub cap_limited_digests: usize,
    /// Selected ÷ digests.
    pub mean_items: f64,
    /// Selected-Story share `selected_stories / selected` — the all-Notes/all-Stories balance.
    pub story_share: f64,
    // ── feedback (None / 0 until a feedback surface exists) ──────────────────
    /// Distinct selected stories that received feedback.
    pub graded: usize,
    pub care_more: usize,
    pub care_less: usize,
    /// `care_more / (care_more + care_less)` — precision among reacted-to stories. `None` if no
    /// feedback yet.
    pub precision: Option<f64>,
    /// Mean per-digest nDCG over the render order (graded gain 2/0, ungraded 1), across digests with
    /// ≥1 graded story. `None` if no feedback yet. Measures *ranking* quality, not just inclusion.
    pub ndcg: Option<f64>,
}

/// Evaluate a window of decision logs against the story grades. Pure over `(digests, grades)`.
///
/// `digests` is one `Vec<DecisionRecord>` per digest (the persisted `digest.decisions`, newest-first
/// or any order — metrics are order-independent except each digest's own render positions). `grades`
/// maps a story id to the user's latest grade on it; a story selected across several digests counts
/// once toward the feedback tallies but contributes to each digest's nDCG.
pub fn evaluate(digests: &[Vec<DecisionRecord>], grades: &HashMap<Uuid, Grade>) -> Metrics {
    let mut m = Metrics {
        digests: digests.len(),
        candidates: 0,
        selected: 0,
        over_cap: 0,
        dropped_below_floor: 0,
        selected_stories: 0,
        selected_notes: 0,
        empty_digests: 0,
        cap_limited_digests: 0,
        mean_items: 0.0,
        story_share: 0.0,
        graded: 0,
        care_more: 0,
        care_less: 0,
        precision: None,
        ndcg: None,
    };
    let mut ndcg_sum = 0.0;
    let mut ndcg_digests = 0usize;
    // A story shown in several digests must only count once toward care_more/care_less.
    let mut counted: HashSet<Uuid> = HashSet::new();

    for decisions in digests {
        m.candidates += decisions.len();

        // The selected stories, in render-position order (the order nDCG scores).
        let mut sel: Vec<&DecisionRecord> = decisions
            .iter()
            .filter(|d| matches!(d.verdict, Verdict::Selected { .. }))
            .collect();
        sel.sort_by_key(|d| match d.verdict {
            Verdict::Selected { position } => position,
            _ => usize::MAX,
        });

        if sel.is_empty() {
            m.empty_digests += 1;
        }
        m.selected += sel.len();

        for d in decisions {
            match d.verdict {
                Verdict::OverCap { .. } => m.over_cap += 1,
                Verdict::Dropped {
                    cause: DropCause::BelowFloor,
                } => m.dropped_below_floor += 1,
                Verdict::Selected { .. } => {}
            }
        }
        if decisions
            .iter()
            .any(|d| matches!(d.verdict, Verdict::OverCap { .. }))
        {
            m.cap_limited_digests += 1;
        }

        for d in &sel {
            match d.reason.format {
                Format::Story => m.selected_stories += 1,
                Format::Note => m.selected_notes += 1,
            }
        }

        // Per-digest nDCG over the render order, only where ≥1 selected story has a grade.
        if sel.iter().any(|d| grades.contains_key(&d.story_id)) {
            let rels: Vec<f64> = sel
                .iter()
                .map(|d| grades.get(&d.story_id).map_or(UNGRADED_GAIN, |g| g.gain()))
                .collect();
            if let Some(nd) = ndcg(&rels) {
                ndcg_sum += nd;
                ndcg_digests += 1;
            }
        }

        // Feedback tallies — once per distinct selected story.
        for d in &sel {
            if let Some(g) = grades.get(&d.story_id) {
                if counted.insert(d.story_id) {
                    match g {
                        Grade::Up => m.care_more += 1,
                        Grade::Down => m.care_less += 1,
                    }
                }
            }
        }
    }

    m.graded = m.care_more + m.care_less;
    if m.digests > 0 {
        m.mean_items = m.selected as f64 / m.digests as f64;
    }
    if m.selected > 0 {
        m.story_share = m.selected_stories as f64 / m.selected as f64;
    }
    if m.graded > 0 {
        m.precision = Some(m.care_more as f64 / m.graded as f64);
    }
    if ndcg_digests > 0 {
        m.ndcg = Some(ndcg_sum / ndcg_digests as f64);
    }
    m
}

/// nDCG of a ranking given per-position gains in render order: `DCG / IDCG`, where `IDCG` is the gain
/// vector sorted descending. `None` for an empty ranking or an all-zero ideal (nothing to rank).
fn ndcg(rels: &[f64]) -> Option<f64> {
    if rels.is_empty() {
        return None;
    }
    let dcg = |xs: &[f64]| -> f64 {
        xs.iter()
            .enumerate()
            .map(|(i, r)| r / (i as f64 + 2.0).log2())
            .sum()
    };
    let actual = dcg(rels);
    let mut ideal = rels.to_vec();
    ideal.sort_by(|a, b| b.partial_cmp(a).expect("gains are finite"));
    let ideal = dcg(&ideal);
    (ideal > 0.0).then(|| actual / ideal)
}

impl std::fmt::Display for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "digests             {}", self.digests)?;
        writeln!(f, "candidates          {}", self.candidates)?;
        writeln!(
            f,
            "selected            {}   (story {} / note {}, {:.1}% story)",
            self.selected,
            self.selected_stories,
            self.selected_notes,
            self.story_share * 100.0
        )?;
        writeln!(f, "  mean items/digest {:.2}", self.mean_items)?;
        writeln!(f, "  empty digests     {}", self.empty_digests)?;
        writeln!(f, "  cap-limited       {}", self.cap_limited_digests)?;
        writeln!(f, "over-cap drops      {}", self.over_cap)?;
        writeln!(f, "below-floor drops   {}", self.dropped_below_floor)?;
        match (self.precision, self.ndcg) {
            (Some(p), ndcg) => {
                writeln!(
                    f,
                    "feedback            {}   (care_more {} / care_less {})",
                    self.graded, self.care_more, self.care_less
                )?;
                writeln!(f, "precision           {p:.3}")?;
                match ndcg {
                    Some(n) => writeln!(f, "nDCG                {n:.3}")?,
                    None => writeln!(f, "nDCG                n/a")?,
                }
            }
            (None, _) => {
                writeln!(
                    f,
                    "feedback            0   (no story feedback yet — precision/nDCG pending)"
                )?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest::select::ItemReason;
    use proptest::prelude::*;

    /// A decision record with the given verdict + format (the only fields the harness reads).
    fn rec(id: u128, verdict: Verdict, format: Format) -> DecisionRecord {
        DecisionRecord {
            story_id: Uuid::from_u128(id),
            verdict,
            reason: ItemReason {
                format,
                ..Default::default()
            },
        }
    }
    fn sel(id: u128, position: usize, format: Format) -> DecisionRecord {
        rec(id, Verdict::Selected { position }, format)
    }

    #[test]
    fn structural_counts_without_any_feedback() {
        // One digest: 2 selected (a Story + a Note), 1 over-cap, 1 below-floor drop.
        let digest = vec![
            sel(1, 0, Format::Story),
            sel(2, 1, Format::Note),
            rec(3, Verdict::OverCap { rank: 2 }, Format::Story),
            rec(
                4,
                Verdict::Dropped {
                    cause: DropCause::BelowFloor,
                },
                Format::Note,
            ),
        ];
        let m = evaluate(&[digest], &HashMap::new());
        assert_eq!(m.digests, 1);
        assert_eq!(m.candidates, 4);
        assert_eq!(m.selected, 2);
        assert_eq!(m.selected_stories, 1);
        assert_eq!(m.selected_notes, 1);
        assert_eq!(m.over_cap, 1);
        assert_eq!(m.dropped_below_floor, 1);
        assert_eq!(m.cap_limited_digests, 1);
        assert_eq!(m.empty_digests, 0);
        assert!((m.mean_items - 2.0).abs() < 1e-9);
        assert!((m.story_share - 0.5).abs() < 1e-9);
        assert_eq!(m.precision, None, "no feedback → no precision");
        assert_eq!(m.ndcg, None);
    }

    #[test]
    fn replay_rescoring_responds_to_a_trial_cap() {
        use crate::digest::select::{Candidate, ReplaySnapshot, ScoringConfig};
        use chrono::TimeZone;
        let at = |s: i64| chrono::Utc.timestamp_opt(s, 0).single().unwrap();
        // Three longform single-event stories → all classify Story; only the cap differs.
        let candidates = vec![
            Candidate::new(Uuid::from_u128(1), at(300), vec![]),
            Candidate::new(Uuid::from_u128(2), at(200), vec![]),
            Candidate::new(Uuid::from_u128(3), at(100), vec![]),
        ];
        let snap = ReplaySnapshot {
            now: at(300),
            max_items: 1000,
            candidates,
        };
        let tight = ScoringConfig {
            story_cap: 1,
            ..Default::default()
        };
        let loose = ScoringConfig {
            story_cap: 5,
            ..Default::default()
        };
        let m_tight = evaluate(&[replay(&snap, &tight)], &HashMap::new());
        let m_loose = evaluate(&[replay(&snap, &loose)], &HashMap::new());
        assert_eq!(m_tight.selected, 1, "tight cap selects one");
        assert_eq!(m_loose.selected, 3, "loose cap selects all three");
        assert_eq!(m_tight.over_cap, 2);
    }

    #[test]
    fn empty_digest_is_counted() {
        let m = evaluate(&[vec![]], &HashMap::new());
        assert_eq!(m.empty_digests, 1);
        assert_eq!(m.selected, 0);
    }

    #[test]
    fn precision_is_care_more_over_reacted() {
        let digest = vec![
            sel(1, 0, Format::Story),
            sel(2, 1, Format::Story),
            sel(3, 2, Format::Story),
        ];
        let grades = HashMap::from([
            (Uuid::from_u128(1), Grade::Up),
            (Uuid::from_u128(2), Grade::Down),
            // story 3 ungraded → not counted toward precision
        ]);
        let m = evaluate(&[digest], &grades);
        assert_eq!(m.graded, 2);
        assert_eq!(m.care_more, 1);
        assert_eq!(m.care_less, 1);
        assert_eq!(m.precision, Some(0.5));
    }

    #[test]
    fn a_story_shown_twice_counts_once_for_precision() {
        let d1 = vec![sel(1, 0, Format::Story)];
        let d2 = vec![sel(1, 0, Format::Story)];
        let grades = HashMap::from([(Uuid::from_u128(1), Grade::Up)]);
        let m = evaluate(&[d1, d2], &grades);
        assert_eq!(m.care_more, 1, "deduped across digests");
        assert_eq!(m.precision, Some(1.0));
    }

    #[test]
    fn ndcg_rewards_ranking_care_less_low() {
        // Same items, opposite orders: care_less first scores worse than care_less last.
        let bad = vec![sel(1, 0, Format::Story), sel(2, 1, Format::Story)]; // down at top
        let good = vec![sel(2, 0, Format::Story), sel(1, 1, Format::Story)]; // up at top
        let grades = HashMap::from([
            (Uuid::from_u128(1), Grade::Down),
            (Uuid::from_u128(2), Grade::Up),
        ]);
        let m_bad = evaluate(&[bad], &grades).ndcg.unwrap();
        let m_good = evaluate(&[good], &grades).ndcg.unwrap();
        assert!(m_good > m_bad, "{m_good} should beat {m_bad}");
        assert!((m_good - 1.0).abs() < 1e-9, "best order is ideal");
    }

    proptest! {
        // Rates stay in [0,1]; the verdict tallies never exceed the candidate count.
        #[test]
        fn metrics_are_well_formed(
            specs in prop::collection::vec(
                prop::collection::vec((0u128..30, 0usize..3, any::<bool>()), 0..12), 0..6),
            up_ids in prop::collection::vec(0u128..30, 0..10),
        ) {
            let digests: Vec<Vec<DecisionRecord>> = specs.into_iter().map(|digest| {
                let mut seen = HashSet::new();
                digest.into_iter().filter(|(id,_,_)| seen.insert(*id)).enumerate()
                    .map(|(pos, (id, verdict_kind, is_note))| {
                        let fmt = if is_note { Format::Note } else { Format::Story };
                        let v = match verdict_kind {
                            0 => Verdict::Selected { position: pos },
                            1 => Verdict::OverCap { rank: pos },
                            _ => Verdict::Dropped { cause: DropCause::BelowFloor },
                        };
                        rec(id, v, fmt)
                    }).collect()
            }).collect();
            let grades: HashMap<Uuid, Grade> =
                up_ids.into_iter().map(|id| (Uuid::from_u128(id), Grade::Up)).collect();

            let m = evaluate(&digests, &grades);
            prop_assert!(m.selected + m.over_cap + m.dropped_below_floor <= m.candidates);
            prop_assert_eq!(m.selected_stories + m.selected_notes, m.selected);
            if let Some(p) = m.precision { prop_assert!((0.0..=1.0).contains(&p)); }
            if let Some(n) = m.ndcg { prop_assert!((0.0..=1.0 + 1e-9).contains(&n)); }
            prop_assert_eq!(m.graded, m.care_more + m.care_less);
        }
    }
}
