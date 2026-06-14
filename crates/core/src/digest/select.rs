//! Scoring & selection — the pure heart of M4's "relevant & explainable" half (design §8.3–§8.4),
//! layered on top of the thread layer's relevance term.
//!
//! [`select`] is a **pure function over precomputed features** (a story's cached rollups + the
//! thread relevance term + a config row), so it is fixture-testable and swappable, with no I/O and no
//! ambient clock — `now` is injected. It implements the design's three-signal model:
//!
//! 1. **Relevance gates** (`relevance ≥ relevance_floor`) — *do you care?* A story's relevance is
//!    `base + scope_bonus (own-private) + the thread term` ([`apply_thread_weights`] — Σ of the
//!    entity weights `thread_maintenance` projected from your feedback/affinity). The candidate set is
//!    already `public ∪ own-private` (what you're subscribed to), so subscription match is implicit;
//!    the thread term is additive, so an explicit source-subscription filter slots in later without
//!    reshaping selection. "Don't care" feedback drives a term negative, dropping the story below the
//!    floor — the gate is how feedback removes things.
//! 2. **Richness classifies** format — Story (multi-event OR multi-source OR a substantive `longform`
//!    single item) vs Note (atomic/thin). **Purely a rendering difference from the candidate's
//!    complexity/richness** — it never gates inclusion and never affects rank.
//! 3. **Priority orders + caps** — relevance-led, boosted by `max_severity`, aged by recency decay at
//!    read time; Stories and Notes have separate caps, but the final order is global priority, so a
//!    high-priority Note can outrank a Story (**format ≠ importance**).
//!
//! Every candidate gets a [`Decision`] whose [`ItemReason`] (the thread layer's reason record, design
//! §10.2, extended here with format/richness/priority) is persisted on `digest.decisions` — drops
//! included — so "why is this in / out, and why this format?" is answerable. The render path reads it
//! back; `digest-explain` shows the whole trace.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::kind::ContentKind;
use crate::identity::CanonicalId;

/// Story (rich, multi-faceted) vs Note (atomic, thin) — a **rendering** classification from the
/// candidate's richness, not importance (design §8.4: a high-priority Note can sit above a
/// lower-priority Story).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    #[default]
    Story,
    Note,
}

impl Format {
    pub fn as_str(self) -> &'static str {
        match self {
            Format::Story => "story",
            Format::Note => "note",
        }
    }
}

/// The tunable scoring knobs — the `digest_config` row, lifted into a pure value so selection is
/// testable against fixtures (design §8.4: "the relevance_floor, richness threshold, and caps live in
/// a config table in v1"). [`Default`] mirrors the migration defaults so a fixture needn't hit the DB.
#[derive(Debug, Clone, Copy)]
pub struct ScoringConfig {
    /// Inclusion gate: a story is kept iff `relevance ≥ relevance_floor`.
    pub relevance_floor: f32,
    /// Relevance bonus when a story includes the subscriber's own private content.
    pub scope_bonus: f32,
    /// Priority boost per point of a story's `max_severity`.
    pub severity_weight: f32,
    /// Priority halves every this-many days of age (recency decay at read time).
    pub recency_half_life_days: f64,
    /// Max Stories rendered (design §8.4: ~3–5).
    pub story_cap: usize,
    /// Max Notes rendered (~15–25).
    pub note_cap: usize,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            relevance_floor: 0.0,
            scope_bonus: 0.5,
            severity_weight: 0.1,
            recency_half_life_days: 3.0,
            story_cap: 5,
            note_cap: 20,
        }
    }
}

/// The persisted **decision record** for one story (design §10.2 reason record), extended for M4: the
/// thread layer's `relevance` term + resolved entity spine, plus the M4 scoring outcome — the render
/// `format`, the human `richness` phrase that chose it, and the computed `priority`. Combined at
/// render time into the "why is this here?" trace, and queryable later. Stored inside
/// `digest.decisions`. The M4 fields are `#[serde(default)]` so a pre-M4 decision log still decodes.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ItemReason {
    /// The story's relevance — `base + scope_bonus + thread term`; ranks it, gates it.
    pub relevance: f32,
    /// The story's resolved entity spine — what the relevance term + thread matching keyed on.
    pub entities: Vec<CanonicalId>,
    /// The render format chosen by richness (design §8.4).
    #[serde(default)]
    pub format: Format,
    /// Human richness phrase ("multi-source", "longform", "announcement", …) — why this format.
    #[serde(default)]
    pub richness: String,
    /// The computed priority that ordered it (relevance, severity-boosted, recency-decayed).
    #[serde(default)]
    pub priority: f32,
}

/// A candidate for a digest: a **story** id (a fused cross-source unit, M3), the recency key, the
/// story's entity spine (for the thread relevance term), the computed thread `relevance`, and the M4
/// scoring features (its cross-source rollups). `relevance` is `0` until [`apply_thread_weights`]
/// fills it, so a digest with no thread weighting ranks by recency-decayed base relevance.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub id: Uuid,
    pub last_event_time: DateTime<Utc>,
    pub entities: Vec<CanonicalId>,
    pub relevance: f32,
    /// Σ member event counts — breadth (richness).
    pub event_count: i32,
    /// Distinct member sources — the "across sources" breadth signal (richness + multi-source).
    pub source_diversity: i32,
    /// Max member depth — richness (a `longform` single item is still a Story).
    pub content_depth: ContentKind,
    /// Max member severity, or `None` — a priority boost.
    pub max_severity: Option<i16>,
    /// Whether the story includes the subscriber's own private content — the scope bonus.
    pub has_private: bool,
}

impl Candidate {
    /// A candidate with no thread relevance yet and neutral rollups — the simple `(id, time,
    /// entities)` shape for tests. The digest flow builds the full struct (with the story's rollups)
    /// directly.
    pub fn new(id: Uuid, last_event_time: DateTime<Utc>, entities: Vec<CanonicalId>) -> Self {
        Candidate {
            id,
            last_event_time,
            entities,
            relevance: 0.0,
            event_count: 1,
            source_diversity: 1,
            content_depth: ContentKind::Longform,
            max_severity: None,
            has_private: false,
        }
    }
}

/// The Thread relevance term (design `digest-thread-layer.md` §5.2): set each candidate's `relevance`
/// to `Σ_{e ∈ entities} entity_weight[e]` over the weights `thread_maintenance` projected — the
/// *rescue for the missed-because-split* case (a story advancing a thread you've invested in clears
/// the bar). Purely additive: an empty `weights` map leaves every relevance at `0`, so selection
/// degrades to recency-decayed base relevance.
pub fn apply_thread_weights(candidates: &mut [Candidate], weights: &BTreeMap<CanonicalId, f32>) {
    if weights.is_empty() {
        return;
    }
    for c in candidates.iter_mut() {
        c.relevance = c.entities.iter().filter_map(|e| weights.get(e)).sum();
    }
}

/// Why a story dropped out before ranking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DropCause {
    /// Relevance fell below `relevance_floor` (design §8.4 gate; a "don't care" pushes it here).
    BelowFloor,
}

/// Why a candidate did or didn't make the digest (design §8.4). `Dropped` is new in M4 — the relevance
/// gate can now exclude a story before ranking, recorded so "why was X *not* in my digest?" answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    Selected { position: usize },
    OverCap { rank: usize },
    Dropped { cause: DropCause },
}

/// A candidate paired with its full scoring outcome — the selection's audit trail. Carries the
/// computed `relevance` / `priority` / `format` that ordered and classified it (so `digest-explain`
/// can show *why* a thread-invested story out-ranked a fresher one, and why it's a Story or a Note).
#[derive(Debug, Clone)]
pub struct Decision {
    pub id: Uuid,
    pub last_event_time: DateTime<Utc>,
    pub relevance: f32,
    pub priority: f32,
    pub format: Format,
    pub richness: String,
    pub verdict: Verdict,
}

/// One entry of a digest's persisted **decision log** (design §10.2): a candidate story, its verdict
/// (selected, over-cap, or dropped — so the log records *every* outcome, answering "why was X *not*
/// in my digest?"), and the reasoning behind its rank. The full `Vec<DecisionRecord>` is stored on
/// the `digest` row and is the queryable record a later explain UI / feedback loop reads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionRecord {
    pub story_id: Uuid,
    pub verdict: Verdict,
    pub reason: ItemReason,
}

/// Recency decay in `(0, 1]`: 1.0 at age 0, halving every `half_life_days`. A non-positive half-life
/// disables decay (constant 1.0). Age is floored at 0 (v1 is retrospective-only — design §8.5 — so a
/// clock skew can't boost a future-stamped story). Underflows to 0 for very old stories, which then
/// fall to the recency tiebreak — they have aged out of relevance, exactly the §9.4 fade.
fn recency_decay(now: DateTime<Utc>, t: DateTime<Utc>, half_life_days: f64) -> f64 {
    if half_life_days <= 0.0 {
        return 1.0;
    }
    let age_days = ((now - t).num_seconds() as f64 / 86_400.0).max(0.0);
    0.5_f64.powf(age_days / half_life_days)
}

/// A story's relevance: `base + scope_bonus (own-private) + the thread term`. Base 1.0 makes
/// subscription match implicit (everything in the candidate set is something you subscribed to), so a
/// default floor of 0 admits everything until feedback drives a thread term negative.
fn relevance(c: &Candidate, cfg: &ScoringConfig) -> f32 {
    1.0 + if c.has_private { cfg.scope_bonus } else { 0.0 } + c.relevance
}

/// Priority — relevance-led, boosted by `max_severity`, aged by recency decay at read time
/// (design §8.3).
fn priority(c: &Candidate, relevance: f32, cfg: &ScoringConfig, now: DateTime<Utc>) -> f32 {
    let sev = c.max_severity.unwrap_or(0) as f32;
    let decay = recency_decay(now, c.last_event_time, cfg.recency_half_life_days) as f32;
    (relevance + cfg.severity_weight * sev) * decay
}

/// Richness → render format + a human phrase (design §8.4). Story when multi-source, multi-event, or a
/// substantive `longform` single item; Note otherwise (a thin announcement/message). A pure function
/// of the candidate's complexity — it never gates inclusion and never affects rank.
fn richness(c: &Candidate) -> (Format, &'static str) {
    if c.source_diversity > 1 {
        (Format::Story, "multi-source")
    } else if c.event_count > 1 {
        (Format::Story, "multi-event")
    } else if c.content_depth == ContentKind::Longform {
        (Format::Story, "longform")
    } else {
        match c.content_depth {
            ContentKind::Announcement => (Format::Note, "announcement"),
            _ => (Format::Note, "message"),
        }
    }
}

/// A gated (relevance ≥ floor) candidate, carried through ranking.
struct Gated {
    id: Uuid,
    last_event_time: DateTime<Utc>,
    relevance: f32,
    priority: f32,
    format: Format,
    richness: &'static str,
}

/// Pure scoring + selection (design §8.4): gate by relevance, rank by priority, classify richness into
/// Story/Note, cap each format, and order the result by global priority (format ≠ importance). Returns
/// a [`Decision`] for **every** candidate — selected (by render position), then over-cap (by rank),
/// then dropped — so the trace is complete. `now` is injected (no ambient clock), keeping it pure.
pub fn select(
    candidates: Vec<Candidate>,
    cfg: &ScoringConfig,
    now: DateTime<Utc>,
) -> Vec<Decision> {
    // 1. Gate. Dropped candidates get a terminal Decision now; the rest carry forward to ranking.
    let mut gated: Vec<Gated> = Vec::new();
    let mut dropped: Vec<Decision> = Vec::new();
    for c in &candidates {
        let (format, richness_phrase) = richness(c);
        let r = relevance(c, cfg);
        if r < cfg.relevance_floor {
            dropped.push(Decision {
                id: c.id,
                last_event_time: c.last_event_time,
                relevance: r,
                priority: 0.0,
                format,
                richness: richness_phrase.to_string(),
                verdict: Verdict::Dropped {
                    cause: DropCause::BelowFloor,
                },
            });
        } else {
            gated.push(Gated {
                id: c.id,
                last_event_time: c.last_event_time,
                relevance: r,
                priority: priority(c, r, cfg, now),
                format,
                richness: richness_phrase,
            });
        }
    }

    // 2. Rank by priority desc, then recency, then id — deterministic. With no thread term and equal
    //    base relevance this is exactly recency order (priority is monotonic in recency via decay).
    gated.sort_by(|a, b| {
        b.priority
            .total_cmp(&a.priority)
            .then(b.last_event_time.cmp(&a.last_event_time))
            .then(a.id.cmp(&b.id))
    });

    // 3. Cap per format, assigning render positions in the global priority order (Stories and Notes
    //    interleave by priority). A Note is never dropped *for being a Note* — only for losing the
    //    Note cap race; same for Stories.
    let (mut stories, mut notes, mut position) = (0usize, 0usize, 0usize);
    let mut selected: Vec<Decision> = Vec::new();
    let mut over_cap: Vec<Decision> = Vec::new();
    for (rank, g) in gated.into_iter().enumerate() {
        let (count, cap) = match g.format {
            Format::Story => (&mut stories, cfg.story_cap),
            Format::Note => (&mut notes, cfg.note_cap),
        };
        let verdict = if *count < cap {
            *count += 1;
            let pos = position;
            position += 1;
            Verdict::Selected { position: pos }
        } else {
            Verdict::OverCap { rank }
        };
        let decision = Decision {
            id: g.id,
            last_event_time: g.last_event_time,
            relevance: g.relevance,
            priority: g.priority,
            format: g.format,
            richness: g.richness.to_string(),
            verdict,
        };
        match verdict {
            Verdict::Selected { .. } => selected.push(decision),
            _ => over_cap.push(decision),
        }
    }

    // Output order: selected (render order) ++ over-cap (priority rank) ++ dropped (by id).
    dropped.sort_by_key(|d| d.id);
    selected.extend(over_cap);
    selected.extend(dropped);
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use proptest::prelude::*;

    fn at(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).single().unwrap()
    }

    /// A bare longform single-event public story — classifies as a Story, base relevance.
    fn story(id: u128, secs: i64) -> Candidate {
        Candidate::new(Uuid::from_u128(id), at(secs), Vec::new())
    }

    fn selected_ids(decisions: &[Decision]) -> Vec<Uuid> {
        decisions
            .iter()
            .filter_map(|d| matches!(d.verdict, Verdict::Selected { .. }).then_some(d.id))
            .collect()
    }

    #[test]
    fn richness_classifies_story_vs_note() {
        let s = story(1, 100);
        assert_eq!(richness(&s).0, Format::Story); // longform single

        let mut note = story(2, 100);
        note.content_depth = ContentKind::Announcement;
        assert_eq!(richness(&note).0, Format::Note); // thin announcement

        let mut multi = note;
        multi.source_diversity = 2; // multi-source rescues it to a Story regardless of depth
        assert_eq!(richness(&multi).0, Format::Story);
    }

    #[test]
    fn scope_bonus_lifts_relevance_and_thread_term_adds() {
        let cfg = ScoringConfig::default();
        let mut c = story(1, 100);
        c.has_private = true;
        c.relevance = 2.0; // a thread term from apply_thread_weights
        assert_eq!(relevance(&c, &cfg), 3.5); // base 1.0 + scope 0.5 + thread 2.0
    }

    #[test]
    fn relevance_floor_gates_inclusion() {
        let cfg = ScoringConfig {
            relevance_floor: 1.2, // above a bare story's 1.0, below a private story's 1.5
            ..Default::default()
        };
        let bare = story(1, 100);
        let mut private = story(2, 100);
        private.has_private = true;
        let out = select(vec![bare, private], &cfg, at(100));
        assert_eq!(selected_ids(&out), vec![Uuid::from_u128(2)]);
        let dropped = out.iter().find(|d| d.id == Uuid::from_u128(1)).unwrap();
        assert!(matches!(
            dropped.verdict,
            Verdict::Dropped {
                cause: DropCause::BelowFloor
            }
        ));
    }

    #[test]
    fn negative_thread_term_drops_below_floor() {
        // "Don't care" drives a thread term negative; base 1.0 + (−1.5) < 0 floor → dropped.
        let cfg = ScoringConfig::default();
        let mut c = story(1, 100);
        c.relevance = -1.5;
        let out = select(vec![c], &cfg, at(100));
        assert!(matches!(out[0].verdict, Verdict::Dropped { .. }));
    }

    #[test]
    fn recency_decay_orders_fresher_first() {
        let cfg = ScoringConfig::default();
        let older = story(1, 0);
        let newer = story(2, 10 * 86_400);
        let out = select(vec![older, newer], &cfg, at(10 * 86_400));
        assert_eq!(out[0].id, Uuid::from_u128(2));
    }

    #[test]
    fn thread_weight_promotes_an_invested_but_similar_age_story() {
        // id1 is a touch older but sits on an invested thread; cap 1 must rescue it (the relevance
        // term outweighs the small recency-decay gap). `now` near the events keeps decay meaningful.
        let cfg = ScoringConfig {
            story_cap: 1,
            ..Default::default()
        };
        let mut invested = story(1, 100);
        invested.relevance = 5.0;
        let fresher = story(2, 300);
        let out = select(vec![invested, fresher], &cfg, at(300));
        assert_eq!(selected_ids(&out), vec![Uuid::from_u128(1)]);
    }

    #[test]
    fn caps_are_per_format_and_order_is_global_priority() {
        let cfg = ScoringConfig {
            story_cap: 1,
            note_cap: 1,
            recency_half_life_days: 0.0, // disable decay so priority == relevance for clarity
            ..Default::default()
        };
        let mk = |id, secs, is_note: bool| {
            let mut c = story(id, secs);
            if is_note {
                c.content_depth = ContentKind::Announcement;
            }
            c
        };
        let out = select(
            vec![
                mk(1, 100, false), // Story
                mk(2, 90, false),  // Story (over the story cap)
                mk(3, 80, true),   // Note
                mk(4, 70, true),   // Note (over the note cap)
            ],
            &cfg,
            at(1000),
        );
        let sel = selected_ids(&out);
        assert_eq!(sel.len(), 2, "one Story + one Note within the caps");
        assert!(sel.contains(&Uuid::from_u128(1)));
        assert!(sel.contains(&Uuid::from_u128(3)));
        let over = out
            .iter()
            .filter(|d| matches!(d.verdict, Verdict::OverCap { .. }))
            .count();
        assert_eq!(over, 2);
    }

    #[test]
    fn high_priority_note_outranks_a_story() {
        // A fresh Note vs a stale Story: format ≠ importance, the Note renders first.
        let cfg = ScoringConfig::default();
        let mut note = story(1, 100 * 86_400);
        note.content_depth = ContentKind::Announcement;
        let stale_story = story(2, 0);
        let out = select(vec![stale_story, note], &cfg, at(100 * 86_400));
        assert_eq!(out[0].id, Uuid::from_u128(1));
        assert_eq!(out[0].format, Format::Note);
    }

    fn arb_candidate() -> impl Strategy<Value = Candidate> {
        (1u128..50, 0i64..1000, 1i32..4, 1i32..3, 0usize..3).prop_map(
            |(id, secs, ev, srcs, depth)| {
                let mut c = story(id, secs);
                c.event_count = ev;
                c.source_diversity = srcs;
                c.content_depth = [
                    ContentKind::Message,
                    ContentKind::Announcement,
                    ContentKind::Longform,
                ][depth];
                c
            },
        )
    }

    proptest! {
        // Every candidate is accounted for exactly once, and the per-format caps are never exceeded.
        #[test]
        fn accounts_for_every_candidate_and_respects_caps(
            specs in prop::collection::vec(arb_candidate(), 0..40),
            story_cap in 0usize..8,
            note_cap in 0usize..8,
        ) {
            let mut seen = std::collections::BTreeSet::new();
            let cands: Vec<Candidate> = specs.into_iter().filter(|c| seen.insert(c.id)).collect();
            let n = cands.len();
            let cfg = ScoringConfig { story_cap, note_cap, ..Default::default() };
            let out = select(cands, &cfg, at(1000));

            prop_assert_eq!(out.len(), n);
            let mut ids: Vec<Uuid> = out.iter().map(|d| d.id).collect();
            ids.sort();
            ids.dedup();
            prop_assert_eq!(ids.len(), n, "no candidate lost or doubled");

            let sel_stories = out.iter().filter(|d|
                matches!(d.verdict, Verdict::Selected { .. }) && d.format == Format::Story).count();
            let sel_notes = out.iter().filter(|d|
                matches!(d.verdict, Verdict::Selected { .. }) && d.format == Format::Note).count();
            prop_assert!(sel_stories <= story_cap);
            prop_assert!(sel_notes <= note_cap);
        }

        // With no thread weighting and a 0 floor, every story is kept and ordered newest-first
        // (priority is base × decay, monotonic in recency) — the pre-M4 recency behavior.
        #[test]
        fn no_weights_keeps_recency_order(specs in prop::collection::vec((0u128..50, 0i64..1000), 0..30)) {
            let mut seen = std::collections::BTreeSet::new();
            let cands: Vec<Candidate> = specs.into_iter()
                .filter(|(id, _)| seen.insert(*id))
                .map(|(id, t)| story(id, t))
                .collect();
            let n = cands.len();
            let cfg = ScoringConfig { story_cap: 1000, note_cap: 1000, ..Default::default() };
            let out = select(cands, &cfg, at(1000));
            let sel: Vec<_> = out.iter().filter(|d| matches!(d.verdict, Verdict::Selected { .. })).collect();
            prop_assert_eq!(sel.len(), n, "0 floor admits everything");
            for w in sel.windows(2) {
                prop_assert!(
                    w[0].last_event_time > w[1].last_event_time
                        || (w[0].last_event_time == w[1].last_event_time && w[0].id <= w[1].id)
                );
            }
        }
    }
}
