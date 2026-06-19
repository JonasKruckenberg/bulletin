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
use crate::link::LinkedStory;

// ── Default scoring tunables ─────────────────────────────────────────────────
// The code-side defaults backing [`ScoringConfig::default`], named here so they are easy to find and
// tune in one place. The *runtime* values come from the `digest_config` table; these mirror its
// migration defaults and back tests + off-DB reasoning. The two decay half-lives are the main dials:
// recency fades a story over days, the thread term over weeks (so an invested thread lingers longer).

/// Inclusion gate (design §8.4): admit everything until feedback drives a thread term negative.
const RELEVANCE_FLOOR: f32 = 0.0;
/// Relevance bonus for a story including the subscriber's own private content (design §8.3).
const SCOPE_BONUS: f32 = 0.5;
/// Priority boost per point of a story's `max_severity`.
const SEVERITY_WEIGHT: f32 = 0.1;
/// Recency decay half-life (days): the recency-bound priority halves every this many days of age.
const RECENCY_HALF_LIFE_DAYS: f64 = 3.0;
/// Thread-term decay half-life (days): deliberately ≫ recency, so an invested thread stays promoted
/// for weeks but still eventually ages out (design §8.3 + §9.4).
const THREAD_HALF_LIFE_DAYS: f64 = 21.0;
/// Max Stories per digest (design §8.4: ~3–5).
const STORY_CAP: usize = 5;
/// Max Notes per digest (~15–25).
const NOTE_CAP: usize = 20;
/// Re-surface damping (design §9.4): a no-news re-surface keeps this fraction of its priority.
const RESURFACE_PENALTY: f32 = 0.25;
/// Max stale "still developing" re-surfaces per digest. Damping only sinks a no-news re-surface in the
/// *ranking*; on its own it can't stop a quiet period from backfilling the whole Note cap with recycled
/// items (the "15 still-developing notes" padding). This is the hard budget that does: a few carry the
/// thread, the rest fall to over-cap. Fresh content is never affected — it isn't re-surfaced.
const RESURFACE_CAP: usize = 5;

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

impl TryFrom<&str> for Format {
    type Error = &'static str;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "story" => Ok(Format::Story),
            "note" => Ok(Format::Note),
            _ => Err("unknown format"),
        }
    }
}

/// The tunable scoring knobs — the `digest_config` row, lifted into a pure value so selection is
/// testable against fixtures (design §8.4: "the relevance_floor, richness threshold, and caps live in
/// a config table in v1"). [`Default`] mirrors the migration defaults so a fixture needn't hit the DB.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ScoringConfig {
    /// Inclusion gate: a story is kept iff `relevance ≥ relevance_floor`.
    pub relevance_floor: f32,
    /// Relevance bonus when a story includes the subscriber's own private content.
    pub scope_bonus: f32,
    /// Priority boost per point of a story's `max_severity`.
    pub severity_weight: f32,
    /// Priority halves every this-many days of age (recency decay at read time).
    pub recency_half_life_days: f64,
    /// The **thread** relevance term ages on a slower cadence than recency — it halves every
    /// this-many days (typically ≫ `recency_half_life_days`), so a story you've invested a thread in
    /// stays promoted for weeks but still eventually fades (design §8.3 + §9.4).
    pub thread_half_life_days: f64,
    /// Max Stories rendered (design §8.4: ~3–5).
    pub story_cap: usize,
    /// Max Notes rendered (~15–25).
    pub note_cap: usize,
    /// Priority multiplier for a story re-surfaced with no new events since it was last shown
    /// (design §9.4 re-surface suppression) — it fades to a "still developing" note and eventually
    /// out. `1.0` disables the penalty.
    pub resurface_penalty: f32,
    /// Max stale "still developing" re-surfaces rendered per digest — the hard cap on recycled-note
    /// padding the priority damping alone can't enforce. Fresh content isn't re-surfaced, so it's
    /// untouched by this; a generous value effectively disables the cap.
    pub resurface_cap: usize,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            relevance_floor: RELEVANCE_FLOOR,
            scope_bonus: SCOPE_BONUS,
            severity_weight: SEVERITY_WEIGHT,
            recency_half_life_days: RECENCY_HALF_LIFE_DAYS,
            thread_half_life_days: THREAD_HALF_LIFE_DAYS,
            story_cap: STORY_CAP,
            note_cap: NOTE_CAP,
            resurface_penalty: RESURFACE_PENALTY,
            resurface_cap: RESURFACE_CAP,
        }
    }
}

/// A snapshot of a story as it was last shown to this subscriber (the most recent prior
/// `digest_item`) — the re-surface suppression key (design §9.4). A story whose `last_event_time`
/// hasn't advanced past `last_event_time` here, and which hasn't graduated `Note → Story`, is a stale
/// re-surface to be damped.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Shown {
    pub last_event_time: DateTime<Utc>,
    pub format: Format,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// The story as last shown to this subscriber, if ever — the re-surface suppression input
    /// (design §9.4). `None` = never shown (a fresh story); stable story ids make this well-defined.
    pub last_shown: Option<Shown>,
}

impl Candidate {
    /// A candidate with no thread relevance yet and neutral rollups — the simple `(id, time,
    /// entities)` shape for tests.
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
            last_shown: None,
        }
    }

    /// Build a candidate from a linked story's precomputed rollups + its resolved entity spine and
    /// last-shown snapshot — the digest flow's mapping, in one place so adding a scoring feature
    /// touches a single site. `relevance` is `0` until [`apply_thread_weights`] fills the thread term.
    pub fn from_story(
        story: &LinkedStory,
        entities: Vec<CanonicalId>,
        last_shown: Option<Shown>,
    ) -> Self {
        Candidate {
            id: story.id,
            last_event_time: story.last_event_time,
            entities,
            relevance: 0.0,
            event_count: story.event_count,
            source_diversity: story.source_diversity,
            content_depth: story.content_depth,
            max_severity: story.max_severity,
            has_private: story.has_private,
            last_shown,
        }
    }
}

/// The exact input to one [`select`] call, persisted (as `digest.candidates` jsonb) so a delivered
/// digest can be **re-scored under a trial `ScoringConfig`** offline — the eval config sweep
/// (`local-ml-options.md` §0.1). `candidates` is the post-thread-weighting candidate set, `now` the
/// read-time clock its recency decay used; with `max_items` they make `select` a deterministic replay,
/// so an operator can A/B a `digest_config` change (or a later ML signal) over real history with no
/// deploy. Pre-snapshot digests store nothing and are simply not replayable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaySnapshot {
    pub now: DateTime<Utc>,
    pub max_items: usize,
    pub candidates: Vec<Candidate>,
}

/// The Thread relevance term (design `docs/thread-layer.md` §5.2): set each candidate's `relevance`
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
    /// The natural richness classification — what to freeze on `digest_item` for the next fire's
    /// re-surface/graduation check (unaffected by a re-surface demotion of `format`).
    pub natural_format: Format,
    /// The format to render — `natural_format`, or `Note` when re-surface-damped to "still developing".
    pub format: Format,
    pub richness: String,
    pub verdict: Verdict,
}

impl Decision {
    /// Whether this candidate made the cut — as opposed to falling to over-cap or being dropped.
    pub fn is_selected(&self) -> bool {
        matches!(self.verdict, Verdict::Selected { .. })
    }
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

/// The recency-bound part of relevance: `base (1.0) + scope_bonus (own-private)`. Base 1.0 makes
/// subscription match implicit (everything in the candidate set is something you subscribed to). This
/// is the part that ages on the recency cadence; the thread term (`c.relevance`) ages slower.
fn base_relevance(c: &Candidate, cfg: &ScoringConfig) -> f32 {
    1.0 + if c.has_private { cfg.scope_bonus } else { 0.0 }
}

/// A story's relevance — `base_relevance + the thread term` — the gate key and primary ranking input.
/// A default floor of 0 admits everything until feedback drives a thread term negative.
fn relevance(c: &Candidate, cfg: &ScoringConfig) -> f32 {
    base_relevance(c, cfg) + c.relevance
}

/// Priority — relevance-led, boosted by `max_severity`, aged by recency decay at read time
/// (design §8.3). The recency-bound part (`base_relevance` + severity) decays on the recency
/// half-life; the **thread** term (`c.relevance`) decays on the slower `thread_half_life_days`, so an
/// invested thread stays promoted for weeks yet still eventually ages out (§9.4). The two components
/// are kept explicit (no reconstructing one from the total), so adding a relevance term later can't
/// silently mis-split the decay. With no thread term this is the single-decay form.
fn priority(c: &Candidate, cfg: &ScoringConfig, now: DateTime<Utc>) -> f32 {
    let sev = c.max_severity.unwrap_or(0) as f32;
    let recency = recency_decay(now, c.last_event_time, cfg.recency_half_life_days) as f32;
    let thread_decay = recency_decay(now, c.last_event_time, cfg.thread_half_life_days) as f32;
    (base_relevance(c, cfg) + cfg.severity_weight * sev) * recency + c.relevance * thread_decay
}

/// Richness → render format + a human phrase (design §8.4). Story when multi-source, multi-event, or a
/// substantive `longform` single item; Note otherwise (a thin announcement/message). A pure function
/// of the candidate's complexity — it never gates inclusion and never affects rank.
fn richness(c: &Candidate) -> (Format, &'static str) {
    if c.source_diversity > 1 {
        (Format::Story, "multi-source")
    } else if c.event_count > 1 {
        (Format::Story, "multi-event")
    } else {
        match c.content_depth {
            ContentKind::Longform => (Format::Story, "longform"),
            ContentKind::Announcement => (Format::Note, "announcement"),
            ContentKind::Message => (Format::Note, "message"),
        }
    }
}

/// A gated (relevance ≥ floor) candidate, carried through ranking.
struct Gated {
    id: Uuid,
    last_event_time: DateTime<Utc>,
    relevance: f32,
    priority: f32,
    /// The richness classification (the "natural" format) — snapshotted for the next fire's
    /// graduation check, independent of any re-surface demotion.
    natural_format: Format,
    /// The format actually rendered — `natural_format`, or `Note` when re-surface-demoted.
    format: Format,
    richness: &'static str,
    /// True when this is a stale, no-news re-surface (demoted to "still developing") — so the cap pass
    /// can hold it to the small `resurface_cap` budget instead of letting it pad the Note cap.
    resurfaced: bool,
}

impl Gated {
    /// Seals a ranked candidate into its terminal [`Decision`] once the cap pass has settled its
    /// verdict — the single place the `Gated` → `Decision` field move lives.
    fn into_decision(self, verdict: Verdict) -> Decision {
        Decision {
            id: self.id,
            last_event_time: self.last_event_time,
            relevance: self.relevance,
            priority: self.priority,
            natural_format: self.natural_format,
            format: self.format,
            richness: self.richness.to_string(),
            verdict,
        }
    }
}

/// Pure scoring + selection (design §8.4): gate by relevance, rank by priority, classify richness into
/// Story/Note, cap each format, and order the result by global priority (format ≠ importance). Returns
/// a [`Decision`] for **every** candidate — selected (by render position), then over-cap (by rank),
/// then dropped — so the trace is complete. `now` is injected (no ambient clock), keeping it pure.
///
/// `max_items` is the subscriber's overall ceiling, applied *on top of* the per-format caps: a digest
/// never renders more than `min(max_items, story_cap + note_cap)` items, the lowest-priority overflow
/// falling to `OverCap`.
pub fn select(
    candidates: Vec<Candidate>,
    cfg: &ScoringConfig,
    max_items: usize,
    now: DateTime<Utc>,
) -> Vec<Decision> {
    // 1. Gate. Dropped candidates get a terminal Decision now; the rest carry forward to ranking.
    let mut gated: Vec<Gated> = Vec::new();
    let mut dropped: Vec<Decision> = Vec::new();
    for c in &candidates {
        // The natural richness classification — what the story *is*. Re-surface may demote what's
        // rendered, but the snapshot must record the natural format so graduation isn't fooled by its
        // own demotion (otherwise a damped Story would "graduate" back next fire, oscillating).
        let (natural_format, natural_phrase) = richness(c);
        let r = relevance(c, cfg);
        if r < cfg.relevance_floor {
            dropped.push(Decision {
                id: c.id,
                last_event_time: c.last_event_time,
                relevance: r,
                priority: 0.0,
                natural_format,
                format: natural_format,
                richness: natural_phrase.to_string(),
                verdict: Verdict::Dropped {
                    cause: DropCause::BelowFloor,
                },
            });
            continue;
        }
        let mut p = priority(c, cfg, now);
        let mut format = natural_format;
        let mut richness_phrase = natural_phrase;
        let mut resurfaced = false;
        // Re-surface suppression (design §9.4): a story already shown to this subscriber with no new
        // events since — and not graduating Note → Story (natural richness grew) — fades to a compact
        // "still developing" note and is priority-damped, so it sinks and eventually ages out. A
        // genuinely new event (or a graduation) re-surfaces it at full weight.
        if let Some(shown) = c.last_shown {
            let new_events = c.last_event_time > shown.last_event_time;
            let graduated = shown.format == Format::Note && natural_format == Format::Story;
            if !new_events && !graduated {
                format = Format::Note;
                richness_phrase = "still developing";
                p *= cfg.resurface_penalty;
                resurfaced = true;
            }
        }
        gated.push(Gated {
            id: c.id,
            last_event_time: c.last_event_time,
            relevance: r,
            priority: p,
            natural_format,
            format,
            richness: richness_phrase,
            resurfaced,
        });
    }

    // 2. Rank by priority desc, then recency, then id — deterministic. With no thread term and equal
    //    base relevance this is exactly recency order (priority is monotonic in recency via decay).
    gated.sort_by(|a, b| {
        b.priority
            .total_cmp(&a.priority)
            .then(b.last_event_time.cmp(&a.last_event_time))
            .then(a.id.cmp(&b.id))
    });

    // 3. Cap per format AND by the subscriber's overall `max_items`, assigning render positions in the
    //    global priority order (Stories and Notes interleave by priority). A Note is never dropped
    //    *for being a Note* — only for losing the Note cap race; same for Stories. `position` doubles
    //    as the count of items selected so far, so it enforces the overall ceiling.
    let (mut stories, mut notes, mut resurfaced, mut position) = (0usize, 0usize, 0usize, 0usize);
    let mut selected: Vec<Decision> = Vec::new();
    let mut over_cap: Vec<Decision> = Vec::new();
    for (rank, g) in gated.into_iter().enumerate() {
        let (count, cap) = match g.format {
            Format::Story => (&mut stories, cfg.story_cap),
            Format::Note => (&mut notes, cfg.note_cap),
        };
        // A stale "still developing" re-surface also has to win a slot in the small re-surface budget,
        // on top of its format cap — so a quiet fire can't backfill the digest with recycled notes.
        let resurface_ok = !g.resurfaced || resurfaced < cfg.resurface_cap;
        let verdict = if *count < cap && resurface_ok && position < max_items {
            *count += 1;
            if g.resurfaced {
                resurfaced += 1;
            }
            let pos = position;
            position += 1;
            Verdict::Selected { position: pos }
        } else {
            Verdict::OverCap { rank }
        };
        let decision = g.into_decision(verdict);
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
            .filter_map(|d| d.is_selected().then_some(d.id))
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
    fn stale_resurface_fades_to_a_still_developing_note() {
        // A longform Story shown before with no new events since → demoted to a "still developing"
        // Note and priority-damped (design §9.4).
        let cfg = ScoringConfig::default();
        let mut c = story(1, 100);
        c.last_shown = Some(Shown {
            last_event_time: at(100),
            format: Format::Story,
        });
        let out = select(vec![c], &cfg, 1000, at(100));
        assert_eq!(out[0].format, Format::Note);
        assert_eq!(out[0].richness, "still developing");
    }

    #[test]
    fn damped_resurface_snapshots_its_natural_format_not_the_demotion() {
        // A longform Story re-surfaced with no news renders as a Note, but its *natural* format stays
        // Story — so the snapshot frozen for next time won't spuriously "graduate" it (no oscillation).
        let cfg = ScoringConfig::default();
        let mut c = story(1, 100);
        c.last_shown = Some(Shown {
            last_event_time: at(100),
            format: Format::Story,
        });
        let out = select(vec![c], &cfg, 1000, at(100));
        assert_eq!(
            out[0].format,
            Format::Note,
            "rendered as a still-developing note"
        );
        assert_eq!(
            out[0].natural_format,
            Format::Story,
            "but the snapshotted natural format is unchanged"
        );
    }

    #[test]
    fn new_event_resurfaces_at_full_weight() {
        let cfg = ScoringConfig::default();
        let mut c = story(1, 200); // a newer event than when last shown
        c.last_shown = Some(Shown {
            last_event_time: at(100),
            format: Format::Story,
        });
        let out = select(vec![c], &cfg, 1000, at(200));
        assert_eq!(out[0].format, Format::Story);
        assert_ne!(out[0].richness, "still developing");
    }

    #[test]
    fn note_to_story_graduation_resurfaces_despite_no_new_events() {
        let cfg = ScoringConfig::default();
        let mut c = story(1, 100);
        c.source_diversity = 2; // richness grew → now a Story
        c.last_shown = Some(Shown {
            last_event_time: at(100),
            format: Format::Note, // last shown as a Note, no new events
        });
        let out = select(vec![c], &cfg, 1000, at(100));
        assert_eq!(out[0].format, Format::Story);
        assert_ne!(out[0].richness, "still developing");
    }

    #[test]
    fn stale_resurface_sinks_below_a_fresh_peer() {
        let cfg = ScoringConfig::default();
        let mut stale = story(1, 100);
        stale.last_shown = Some(Shown {
            last_event_time: at(100),
            format: Format::Story,
        });
        let fresh = story(2, 100); // same recency, never shown
        let out = select(vec![stale, fresh], &cfg, 1000, at(100));
        // The fresh Story out-ranks the damped "still developing" note.
        assert_eq!(out[0].id, Uuid::from_u128(2));
        assert_eq!(out[0].format, Format::Story);
    }

    #[test]
    fn resurface_cap_holds_back_recycled_note_padding() {
        // A quiet fire of nothing-but-stale re-surfaces: damping ranks them but can't bound them, so
        // without the cap they'd backfill the Note slots. With `resurface_cap`, only that many render;
        // the rest fall to over-cap rather than padding the digest.
        let cfg = ScoringConfig {
            resurface_cap: 3,
            ..Default::default()
        };
        let stale: Vec<Candidate> = (0..10)
            .map(|i| {
                let mut c = story(i + 1, 100 + i as i64); // distinct recency for a stable order
                c.last_shown = Some(Shown {
                    last_event_time: c.last_event_time,
                    format: Format::Story,
                });
                c
            })
            .collect();
        let out = select(stale, &cfg, 1000, at(1000));
        let selected: Vec<&Decision> = out.iter().filter(|d| d.is_selected()).collect();
        assert_eq!(selected.len(), 3, "only resurface_cap stale notes render");
        assert!(selected.iter().all(|d| d.richness == "still developing"));
        // The rest aren't dropped (still above the floor) — they're held over-cap.
        let over = out
            .iter()
            .filter(|d| matches!(d.verdict, Verdict::OverCap { .. }))
            .count();
        assert_eq!(over, 7);
    }

    #[test]
    fn fresh_notes_are_untouched_by_the_resurface_cap() {
        // The cap is specific to stale re-surfaces — fresh (never-shown) notes fill the Note cap as
        // before, even with a tiny resurface budget.
        let cfg = ScoringConfig {
            resurface_cap: 0,
            note_cap: 5,
            ..Default::default()
        };
        let fresh: Vec<Candidate> = (0..5)
            .map(|i| {
                let mut c = story(i + 1, 100 + i as i64);
                c.content_depth = ContentKind::Announcement; // thin → Note
                c
            })
            .collect();
        let out = select(fresh, &cfg, 1000, at(1000));
        assert_eq!(out.iter().filter(|d| d.is_selected()).count(), 5);
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
        let out = select(vec![bare, private], &cfg, 1000, at(100));
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
        let out = select(vec![c], &cfg, 1000, at(100));
        assert!(matches!(out[0].verdict, Verdict::Dropped { .. }));
    }

    #[test]
    fn recency_decay_orders_fresher_first() {
        let cfg = ScoringConfig::default();
        let older = story(1, 0);
        let newer = story(2, 10 * 86_400);
        let out = select(vec![older, newer], &cfg, 1000, at(10 * 86_400));
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
        let out = select(vec![invested, fresher], &cfg, 1000, at(300));
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
            1000,
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
    fn max_items_caps_the_total_below_the_per_format_caps() {
        // Per-format caps are generous (5/20), but the subscriber's overall ceiling is 2 → only the
        // top-2-by-priority select, the rest fall to OverCap.
        let cfg = ScoringConfig::default();
        let out = select(
            vec![story(1, 400), story(2, 300), story(3, 200), story(4, 100)],
            &cfg,
            2,
            at(400),
        );
        assert_eq!(
            selected_ids(&out),
            vec![Uuid::from_u128(1), Uuid::from_u128(2)]
        );
        assert_eq!(
            out.iter()
                .filter(|d| matches!(d.verdict, Verdict::OverCap { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn thread_term_ages_out_but_slower_than_recency() {
        // At 10 days old an invested story would lose to a fresh trivial one under a single decay, but
        // the slower thread half-life keeps it promoted; pushed far enough out it still ages away.
        let cfg = ScoringConfig::default();
        let invested = |secs| {
            let mut c = story(1, secs);
            c.relevance = 4.0; // a strong thread term
            c
        };
        let ten_days = 10 * 86_400;
        let now = at(ten_days);
        let out = select(vec![invested(0), story(2, ten_days)], &cfg, 1, now);
        assert_eq!(
            selected_ids(&out),
            vec![Uuid::from_u128(1)],
            "the invested story stays promoted at 10 days"
        );

        // 90 days on, the thread term has decayed away and the fresh story wins.
        let ninety = 90 * 86_400;
        let out = select(vec![invested(0), story(2, ninety)], &cfg, 1, at(ninety));
        assert_eq!(
            selected_ids(&out),
            vec![Uuid::from_u128(2)],
            "eventually it ages out"
        );
    }

    #[test]
    fn high_priority_note_outranks_a_story() {
        // A fresh Note vs a stale Story: format ≠ importance, the Note renders first.
        let cfg = ScoringConfig::default();
        let mut note = story(1, 100 * 86_400);
        note.content_depth = ContentKind::Announcement;
        let stale_story = story(2, 0);
        let out = select(vec![stale_story, note], &cfg, 1000, at(100 * 86_400));
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
            let out = select(cands, &cfg, 1000, at(1000));

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
            let out = select(cands, &cfg, 1000, at(1000));
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
