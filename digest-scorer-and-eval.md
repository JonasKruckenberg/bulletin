# Digest — the deterministic scorer & the eval harness

**Status:** SPEC (designed; targets roadmap **M4**, "Relevance & trust"). Not yet built.
**Last updated:** 2026-06-14
**Reads against:** `digest-system-design.md` (§8.3 scoring, §8.4 selection, §8.5 prospective-deferred,
§10.2 reason records, §10.3 feedback), `digest-technical-architecture.md` (§5.3 the content-graph
types, §5.5 the pure selection function, §6 Observability/Reliability, §11 open questions),
`digest-local-ml-options.md` §0 (method doctrine), `IMPLEMENTATION-ROADMAP.md` §M4 / §5.

This spec turns two product-level decisions into a buildable component design:

1. **The deterministic scorer** — the rule-based pre-ranker over four ground-truth signals
   (**relevance · severity · recency · corroboration**) that gates and orders a subscriber's
   candidates with **no model in the loop** (design doctrine, system-design §1 line 21).
2. **The eval harness over the feedback log** — how the append-only `feedback` log becomes the
   *measurement* of whether the scorer is working: **story precision** and **false-positive rate**,
   plus an **offline counterfactual replay** that tunes the scorer's weights without shipping a
   single bad digest (system-design §10.3; roadmap §5 / tech §11 open item).

The two are one loop: the scorer *decides*, reason records make every decision *legible*, the
feedback log *labels* those decisions, and the harness *scores the scorer* — closing the
"earn trust" half of the thesis.

---

## 0. Where this sits in what already exists

Today (M1) selection is recency-only. `digest::select` (`crates/core/src/digest/select.rs`) is a
pure function:

```rust
pub fn select(candidates: Vec<Candidate>, max_items: usize) -> Vec<Decision>
//             Candidate { cluster_id, last_event_time }
//             Decision  { cluster_id, last_event_time, verdict }
//             Verdict   { Selected { position } | OverCap { rank } }
```

It sorts newest-first, tie-breaks by `cluster_id`, caps at `max_items`, and **emits a verdict for
every candidate** so "why is X in/out?" is answerable (`debug digest-explain`, `log_selection`).
That last property — *every candidate carries its rationale* — is the seam this whole spec builds on.

The scorer **generalizes `select` in place**: same call site (`digest::mod::select_over_lookback`),
same "verdict for every candidate" contract, same purity/`now`-injection. It only changes the
*inputs* (recency-only → four signals over precomputed features) and *enriches* the verdict (so the
reason record and the eval harness fall out for free). M1's recency-only behaviour stays reachable
as the degenerate config (§3.6) — a golden-equivalence test pins it.

**What is M1-shaped and stays:** `select` is pure, `now` is injected, the candidate set comes from
the freshness-scored lookback (`candidates_in_lookback`), and selection is *frozen* into the digest
on first run (idempotent re-run reads the frozen view).

**What this spec needs that M1 doesn't have yet** (dependencies, §6):
- **Cluster/Story rollups** — `event_count`, `max_severity`, `content_depth`, `entities`
  (tech §5.3; the `cluster` table is bare today). Severity & corroboration read these.
- **Per-subscriber relevance inputs** — `subscriber.affinity` (entity weights) + `subscriber.filters`
  (sources / mutes / keywords) (system-design §6 data model). Today's `subscriber` has neither.
- **A `feedback` table** (system-design §6/§10.3) — append-only; does not exist yet.
- **A scoring config table** — weights/floor/half-life/caps (system-design §8.4 "live in a config
  table").
- **A candidate feature freeze** — the one genuinely new piece, load-bearing for *offline* eval
  (§5.3).

---

## PART I — THE DETERMINISTIC SCORER

## 1. Doctrine & non-negotiables

From the method doctrine (system-design §1, `digest-local-ml-options.md` §0) — these are
constraints, not preferences:

- **Ground-truth-first, no model in the loop.** Every term is a deterministic function of
  structured, precomputed features. No embedding, no classifier, no LLM gates or ranks here. (ML, if
  ever added, is *one more signal into this scorer*, banded and off the hot path — never a gate.)
- **Degrades to baseline.** Disable any signal (weight → 0, or its feature absent) and the digest
  degrades gracefully — ultimately to M1 recency-only. The scorer must be *correct* with only
  recency available and *better* as the other features arrive (so it can ship before Stories/affinity
  exist and improve monotonically).
- **Pure over precomputed features; `now` injected** (tech §5.5, §6 Reliability). No I/O, no
  ambient clock. This is what makes it fixture-testable *and* what makes the offline harness (§5)
  possible at all — the harness re-runs *this exact function*.
- **Relevance gates; volume never gates** (system-design §8.3). Folding volume into the inclusion
  bar would bury a high-relevance/low-volume item (one album drop). Volume only ever classifies
  *format* (Story vs Note, §4) or boosts *priority* (corroboration), never inclusion.
- **Every decision is a reason record** (system-design §10.2) — free, because the reason *is* the
  signals already computed. This is non-optional: the eval harness reads it.

## 2. The four signals

The scorer computes, per candidate, four signals in `[0, 1]` (relevance also carries hard-zero /
hard-filter semantics). Three of the four already have product definitions in system-design §8.3;
this spec pins their *shape* (the exact arithmetic is config — §3.5 — and is what the harness tunes).

### 2.1 Relevance — *do you care?* (per-subscriber; the gate **and** the primary key)

The only signal that gates inclusion. Inputs (system-design §8.3):

| Input | Source | Effect |
|---|---|---|
| **Subscription match** | `subscriber.filters.sources` vs candidate `source` | **hard filter** — no match ⇒ relevance 0 (dropped, not scored) |
| **Mutes** | `subscriber.filters.mutes` (source / entity / keyword) | **hard zero** — any mute hit ⇒ relevance 0 |
| **Entity affinity** | `subscriber.affinity` (jsonb `entity → weight`) ∩ candidate `entities` | additive: `Σ affinity[e]` over matched entities |
| **Keyword match** | `subscriber.filters.keywords` vs `title`/`entities` | additive bump |
| **Scope bonus** | candidate `scope == Private(self)` | additive: your own private content is inherently relevant |

Shape:

```
relevance =
  if !subscription_match || muted        -> 0.0          (hard filter / hard zero)
  else clamp01( base_affinity
                + Σ affinity_weight(e) for e in entities ∩ tracked
                + keyword_bonus
                + scope_bonus )
```

`base_affinity` (a small positive floor for a subscribed-but-untracked source) is config: set it
≥ `relevance_floor` to mean "subscribed ⇒ shown unless muted" (the M1/cold-start posture), or below
the floor to mean "subscribed but must earn its way in via affinity" (the warmed-up posture). The
eval harness (§5) is exactly how you choose between these.

> **Deferred — the Thread term** (system-design §8.6, `digest-thread-layer.md`): relevance also sums
> the precomputed weight of the active **threads** a story advances — the rescue for the
> missed-because-split case (a story whose pieces each fall below the floor still clears it by
> advancing a thread you've invested in). It re-adds as **one more additive term in the `clamp01`
> above**, fed from a cached per-story thread-weight — no structural change. Seam: keep relevance an
> additive sum so the term slots in.

### 2.2 Severity — a priority **boost** (global; never gates)

Input: `Cluster.max_severity` / `Story.max_severity` = `max(event.severity_hint)` (tech §5.3;
`event.severity_hint smallint NULL` already exists). Source-provided importance (a `critical`
dependabot alert, a P1 incident label).

```
severity_norm = match max_severity {
    None    => 0.0,                          // absent ⇒ neutral, NOT a penalty
    Some(s) => clamp01( s as f / severity_scale_max ),   // e.g. 0..=4 → 0.0..=1.0
}
```

`severity_scale_max` is config (the source taxonomy's top rung). `None` is **neutral** (most v1
events have no severity hint), so severity can only ever *lift* an item, never sink one — it is a
boost, faithful to "boosted by `max_severity`" (§8.3) and "orders; never gates" (tech §5.2).

### 2.3 Recency — a priority **decay** (read-time; deterministic given `now`)

Replaces M1's raw `last_event_time` sort with a continuous decay over `age = now − last_event_time`:

```
recency = 0.5 ^ ( age_seconds / recency_half_life_seconds )      // exponential, ∈ (0, 1]
```

Exponential half-life (config `recency_half_life`, e.g. 36 h for daily, scaled for weekly) — smooth,
bounded, monotone-decreasing in age, and **deterministic because `now` is injected** (so it is
replayable in the harness and testable to the second). System-design §8.5 notes this is the
*retrospective* special case of a future-general `salience(Δ)` curve; the signed-gap generalization
(prospective events) is deferred and re-adds as a swap of this one term — keep recency a single
pluggable function of one time delta.

### 2.4 Corroboration — a priority **boost** for independent confirmation (global)

*Independent sources lighting up about the same thing is importance, not a render hint.* Input:
`source_diversity` = distinct `source` across a story's clusters (`Story.source_diversity`, tech §5.3
— "the across-sources value, free"). On a bare M1 **cluster** (one source) this is `1` and the term
is a **no-op** — exactly the graceful-degradation contract: corroboration contributes nothing until
cross-source linking (M3) produces multi-source stories, then it switches on with zero code change.

```
corroboration = 1 - 1 / (1 + corr_k * (source_diversity - 1))      // ∈ [0, 1), saturating
//  source_diversity = 1 -> 0     (single source: nothing to corroborate)
//  grows with each independent source, with diminishing returns (config corr_k)
```

> **Reconciliation with system-design §8.3.** The product doc currently sequences the
> *priority-corroboration* term "with Threads" (§8.6, deferred). This spec **pulls the deterministic
> corroboration term forward** as one of the four ground-truth pre-ranking signals — the framing of
> the method-doctrine line (§1, line 21: "rule-based pre-ranking — relevance, severity, recency,
> corroboration"). It is cheap, model-free, and reads an already-cached rollup (`source_diversity`),
> so there is no reason to gate it behind Threads. It is *defined* now and *inert* until M3 linking
> gives `source_diversity > 1`. (The Threads-era **reactivation/novelty** term, §8.6, stays deferred
> — that one genuinely needs Thread state.)

## 3. Combination, gate, and the selection pipeline

### 3.1 Priority — relevance-led, modulated

Priority must be **relevance-led** (a low-relevance item can never be hoisted into the digest by
severity or corroboration alone) and **aged by recency**. A multiplicative envelope gives exactly
that, with clean monotonicity for proptests:

```
priority = relevance
         * (1 + w_severity * severity_norm + w_corroboration * corroboration)   // boosts lift within the relevance envelope
         * recency                                                              // decay ages everything
```

- **Relevance dominates**: `priority ≤ relevance * (1 + w_sev + w_corr)`, and `relevance = 0 ⇒
  priority = 0`. Boosts re-rank *within* a relevance band; they cannot rescue a below-floor item.
- **Recency** multiplies, so a stale high-relevance item fades past a fresh one of equal relevance —
  the M1 behaviour, now continuous.
- All weights default-tunable; the **shape contract** (the monotonicity invariants, §3.7) is what
  the implementation guarantees, not the literal arithmetic — the harness (§5) is free to discover
  better weights, even a different *combination form*, as long as the invariants hold.

### 3.2 The pipeline (gate → score → rank → classify → cap → order)

Exactly system-design §8.4, with the scorer feeding step 2:

1. **Gate** — drop candidates with `relevance < relevance_floor` (one bar, both tiers). Muted /
   unsubscribed are already `relevance = 0` ⇒ dropped here. Emit a **drop reason** (§3.4).
2. **Score** — compute `priority` (§3.1) for survivors.
3. **Rank** — order by `priority` desc; **tie-break by `cluster_id`/`story_id`** (preserves M1's
   stable, deterministic order — `select.rs::by_recency` already does this for the recency key).
4. **Classify format** (rendering, *not* importance — system-design §8.4): **Story** if richness is
   high (multi-event OR multi-source OR `content_depth == Longform`), else **Note**. Richness is
   `combine(breadth = f(event_count, source_diversity), depth = content_depth)` — derived from the
   same cached rollups, **not** a stored column, and explicitly **not** part of `priority`.
5. **Cap by tier** — Stories at `max_stories` (~3–5), Notes at `max_notes` (~15–25). A Note is never
   dropped *for being a Note* — only for losing the priority race within its tier. (Deferred
   thread-diversity cap, §8.6, slots in here.)
6. **Order** the final digest by `priority` with format per item — a high-priority Note can sit above
   a lower-priority Story (**format ≠ importance**).

### 3.3 Types (extends `digest/select.rs`)

```rust
/// Precomputed, per-candidate features the scorer is pure over. Sourced from the cluster/story
/// rollups (severity, corroboration, recency) + the subscriber's affinity/filters (relevance).
/// Story-shaped; an M1 cluster is the degenerate one-source story (source_diversity = 1).
pub struct Candidate {
    pub id:               Uuid,                 // cluster_id (M1) / story_id (M3)
    pub last_event_time:  DateTime<Utc>,
    pub max_severity:     Option<i16>,
    pub source_diversity: i16,                  // = 1 for a bare cluster
    pub event_count:      i32,
    pub content_depth:    ContentKind,
    pub entities:         Vec<String>,
    pub scope_is_own:     bool,                 // candidate is this subscriber's own private content
    pub source_subscribed: bool,               // subscription hard-filter input
    pub muted:            bool,                 // any mute hit (source/entity/keyword)
}

/// The four signals + derived priority + format — the reason record, computed once, serialized free.
pub struct Signals {
    pub relevance:     f64,
    pub severity:      f64,
    pub recency:       f64,
    pub corroboration: f64,
    pub priority:      f64,
    pub format:        Format,                  // Story | Note (classify)
}

pub enum Verdict {
    Selected { position: usize, signals: Signals },   // made the cut, render order
    OverCap  { rank: usize,     signals: Signals },    // scored in, lost the tier cap
    Dropped  { reason: DropReason },                   // failed the gate (below floor / muted / unsubscribed)
}

pub enum DropReason {
    BelowFloor { relevance: f64, floor: f64 },
    Muted      { what: String },                       // "source=hackernews" | "entity=acme"
    Unsubscribed,
}

/// Pure: features in, fully-explained decisions out. `now` injected; no I/O.
pub fn score_and_select(
    candidates: Vec<Candidate>,
    affinity:   &Affinity,        // entity -> weight, from subscriber.affinity
    config:     &ScoringConfig,   // weights/floor/half-life/caps (§3.5)
    now:        DateTime<Utc>,
) -> Vec<Decision>;               // Decision { id, signals?/drop, verdict } — verdict for EVERY candidate
```

`score_and_select` is the in-place generalization of today's `select`; `digest::mod` keeps the
`Selected` ids and freezes `Signals` into `digest_item.reasons`.

### 3.4 Reason records — free, and load-bearing for eval

Each `Verdict` carries either its `Signals` (selected/over-cap) or its `DropReason`. On the first
fire these freeze into `digest_item.reasons` (selected) and the **candidate feature log** (§5.3,
which also captures drops + over-caps). This is system-design §10.2 verbatim:

```
story: {relevance: 0.9, severity: 0.0, recency: 0.71, corroboration: 0.5,
        priority: 1.20, richness: high(multi-source) -> Story, rank: 1}
note:  {relevance: 0.8, severity: 0.0, recency: 0.66, corroboration: 0.0,
        priority: 0.53, richness: low(announcement) -> Note,  rank: 3}
drop:  {below relevance_floor: 0.12 < 0.30} | {muted: source=hackernews}
```

Emitted as a structured `tracing` span event (`selection decision`, the M1 trace already does this
per candidate — tech §6) and persisted as `jsonb`. **The render contract is identical to the eval
contract** — one serialization serves both the human ("shown because you follow acme-corp; grouped
because 3 sources referenced the same CVE") and the harness.

### 3.5 Config — `scoring_config` table

System-design §8.4: "the `relevance_floor`, richness threshold, and caps live in a config table."
This spec widens that to the full weight vector:

```
scoring_config(
  id                    -- singleton 'default' in v1; per-subscriber override row later
  relevance_floor       double  -- the gate
  base_affinity         double  -- subscribed-but-untracked floor (§2.1)
  w_severity            double
  w_corroboration       double
  corr_k                double
  recency_half_life_s   bigint
  severity_scale_max    smallint
  richness_*            -- breadth/depth thresholds for Story-vs-Note
  max_stories           int
  max_notes             int
  updated_at
)
```

v1 is a **single global row** (the M4 "hand-tuned floor/thresholds/caps" posture, roadmap §M4). The
schema admits a per-subscriber override row later (the split-trigger: when one global config can't
serve everyone) with **no migration** — same indirection discipline as `creds_ref`. The harness
(§5) treats a `ScoringConfig` as a *value* it can swap, which is the whole point of putting it in a
table: counterfactual replay is "load row → mutate value → re-run pure scorer."

### 3.6 Graceful degradation (the baseline ladder)

The scorer is *correct* at each rung and *better* as features arrive — so it ships before its inputs
all exist:

| Available | Behaviour |
|---|---|
| recency only (M1 today) | `w_*=0`, `relevance_floor=0`, `base_affinity=1` ⇒ **bit-identical to M1's recency sort** (golden test, §3.7) |
| + severity rollup | severity boost switches on |
| + affinity/filters (M4) | relevance gates & ranks; mutes/subscriptions apply |
| + cross-source stories (M3) | `source_diversity > 1` ⇒ corroboration switches on |
| + Threads (deferred) | relevance Thread term + reactivation/novelty re-add as terms |

Each rung is a config/feature change, **not** a rewrite of `score_and_select`.

### 3.7 Determinism & tests (pure `core`, no DB)

`proptest` invariants (mirroring the existing `select.rs` proptests):
- **Relevance monotonicity**: raising `relevance` (others fixed) never lowers `priority`.
- **Boost monotonicity**: raising `severity_norm` or `corroboration` never lowers `priority`;
  raising `age` never raises `recency` (hence never raises `priority`).
- **Relevance-led**: `relevance = 0 ⇒ priority = 0 ⇒ Dropped` (never Selected/OverCap).
- **Gate correctness**: every muted/unsubscribed/below-floor candidate is `Dropped`; no Dropped
  candidate appears in render order.
- **Cap correctness** (carried over from M1): exactly `min(eligible_in_tier, tier_cap)` Selected per
  tier; everyone else accounted for as `OverCap` or `Dropped` — *verdict for every candidate*.
- **Tie-break stability**: equal `priority` ⇒ ordered by id; output is a deterministic function of
  inputs (same inputs + same `now` ⇒ same bytes).
- **Baseline equivalence** (golden): the §3.6 degenerate config reproduces M1 `select` exactly.

Plus `insta` snapshots of `Signals`/reason records over the existing pipeline fixtures.

---

## PART II — THE EVAL HARNESS OVER THE FEEDBACK LOG

## 4. What we are measuring, and the honest limits

System-design §10.3: the feedback log "is also the **eval signal**: story precision and
false-positive rate are how you measure whether overwhelm is actually decreasing." The harness makes
that operational.

### 4.1 The feedback log (the ground truth)

System-design §6/§10.3 — append-only, processed async, never blocks delivery:

```
feedback(
  id            uuid pk
  subscriber_id uuid
  target_type   text       -- 'story' | 'cluster' | 'entity' | 'source' | ('thread' deferred)
  target_id     text       -- story/cluster id, or the entity/source string
  kind          text       -- 'care_more' | 'care_less' | 'wrong_aggregation'  (+ 'thread_*' deferred)
  payload       jsonb      -- e.g. wrong_aggregation: {split_off: [cluster_id,...]}
  created_at    timestamptz
  processed_at  timestamptz null   -- when the next-tick loop consumed it (affinity/edge update)
)
```

Two consumers, kept separate: the **affinity/edge update loop** (system-design §10.3 — `care_more/less`
→ `affinity` deltas; `wrong_aggregation` → per-subscriber cannot-link edge) *acts on* the digest;
the **eval harness** (this part) *measures* it. The harness is strictly read-only.

### 4.2 The label model — and what silence is **not**

Each delivered `digest_item` is a **positive prediction** (we chose to show it). Feedback labels it:

| Signal | Label on the item it surfaced |
|---|---|
| `care_more` (later: open/click engagement) | **true positive** — wanted |
| `care_less` / mute | **false positive** — noise we shipped |
| `wrong_aggregation` | **aggregation error** (a *linking* fault, scored separately — §4.3.4) |
| *no feedback* | **unlabeled** — counted as neither TP nor FP |

The discipline that keeps the metric honest (system-design §10.3 "processed async … the eval
signal", and the implicit-feedback pitfall): **silence is not a label.** We never count an
un-touched item as a positive (it would inflate precision toward 1.0) or as a negative. Precision is
computed over **labeled** deliveries only, and we always report **label coverage** beside it
(§4.3.5) so a precision number is never read without knowing how much of the digest it speaks for.

**The structural limit — missing negatives.** We directly observe what we *showed*; we rarely
observe what we *should have shown and didn't* (the items we gated out). So:
- **Precision is directly measurable.** Of what we surfaced and got signal on, what fraction was
  wanted.
- **Recall / false-negatives are only *proxied*** (§4.3.3) — there is no ground truth for "you'd have
  wanted this thing you never saw." The harness must state this limit in its output, not paper over it.

### 4.3 The KPIs

#### 4.3.1 Story precision (the headline)
```
story_precision = TP / (TP + FP)        over labeled delivered items
```
"Of the items we surfaced *and* got a signal on, how many were wanted." The roadmap-§M4 / tech-§11
headline KPI.

#### 4.3.2 False-positive rate
```
fp_rate = FP / labeled_delivered
```
How much *labeled* noise we still ship — the direct read on "is overwhelm decreasing?" Tracked as a
time series; the goal is monotone-down as affinity warms.

#### 4.3.3 Drop-regret (the proxy false-negative)
The only honest signal about misses: a `care_more` / un-mute on an entity or source that the scorer
had **dropped** for this subscriber in the same window.
```
drop_regret = | { dropped candidates whose entity/source the subscriber later asked to care_more about } |
```
A **lower bound** on false-negatives (we only catch the misses the user happened to notice and
correct elsewhere). Reported as a count + examples, never as a recall denominator.

#### 4.3.4 Aggregation error rate (linking, not relevance)
```
aggregation_error_rate = wrong_aggregation_feedback / delivered_stories
```
Isolated from precision because a `wrong_aggregation` is a *linking* fault (§8.2), not a *scoring*
fault — conflating them would make the scorer look bad for the linker's mistakes. Feeds M3 tuning.

#### 4.3.5 Operational / hygiene
- **Label coverage** = `labeled_delivered / delivered` — the trust qualifier on every precision
  number.
- **Feedback latency** = `created_at − digest.delivered_at` distribution (how fast users react).
- **Volume relief** = items delivered per digest over time (the blunt "is the digest getting
  shorter / more focused" read).

All KPIs are sliceable **per-subscriber** and **global**, and **windowed** (default trailing 30 d).

## 5. The two modes

### 5.1 Mode A — online monitoring (the dashboard)

Aggregate the live feedback log + delivered digests into the §4.3 KPIs and emit them as **metrics**
— *gauges*, per tech §6 ("**Product KPIs** … derive from the feedback log", and metrics-primary, not
traces):

```
bulletin_story_precision{window}            gauge
bulletin_digest_fp_rate{window}             gauge
bulletin_aggregation_error_rate{window}     gauge
bulletin_drop_regret_total                  counter
bulletin_feedback_total{kind}               counter
bulletin_label_coverage{window}             gauge
```

Refreshed by the existing once-a-minute cron tick's gauge-refresh path (same place
`bulletin_queue_depth` et al. are recomputed — README "Metrics"), so this is a *wiring* addition, not
new infrastructure. Importable into the existing Grafana overview (`ops/grafana`).

### 5.2 Mode B — offline counterfactual replay (the tuning loop)

The reason the scorer is a pure function. **Replay historical digests under a *different*
`ScoringConfig` and measure how the KPIs *would* have changed against the labels we already have** —
so you tune `relevance_floor` / weights / half-life and *see the precision/FP trade* before shipping.

```
for each historical digest D in the eval window:
    features  = load_frozen_candidate_features(D)        // §5.3 — the features AS AT fire time
    decisions = score_and_select(features, D.affinity_snapshot, candidate_config, D.window_end)
    labels    = join_feedback_to(D's targets)            // §4.2
    accumulate(decisions ⋈ labels)
report EvalReport { precision, fp_rate, drop_regret, agg_error, coverage, confusion, deltas-vs-actual }
```

Because `score_and_select` is **the same pure function** the live path runs, replay is *faithful* —
no separate "eval model" to drift from production. The harness can sweep a grid of configs and report
the Pareto front (precision ↑ vs items-shipped ↓).

**The counterfactual-identity test** (the proof the freeze is sufficient): replaying a digest with
the **exact config that produced it** must reproduce its **exact** selection. If it doesn't, the
frozen features are lossy — a hard CI gate.

### 5.3 The one new schema requirement — the candidate feature freeze

`digest_item` stores only **selected** items. That is enough for *precision on what we shipped*, but
**not** for counterfactual replay: to evaluate a config that would have *included something we
dropped* (e.g. "did lowering the floor recover a TP?"), we need the **features of the dropped /
over-cap candidates too**, *as they were at fire time* (clusters get recomputed — today's rollup ≠
the one that fired).

So the harness requires freezing, per digest, the `Candidate` features + `Verdict` for the **whole
candidate set** (selected + over-cap + dropped), plus the **affinity snapshot** used:

```
digest_candidate(
  digest_id      uuid
  candidate_id   uuid       -- cluster/story id
  features       jsonb      -- the Candidate struct (§3.3) as at fire time
  verdict        jsonb      -- Selected{pos,signals} | OverCap{rank,signals} | Dropped{reason}
  primary key (digest_id, candidate_id)
)
-- + digest.affinity_snapshot jsonb : the Affinity used for this fire
```

Notes:
- **It is the reason record, persisted for the full candidate set** — `digest_item.reasons` is the
  selected-subset of exactly this. So the marginal cost is the dropped/over-cap rows, bounded by the
  lookback candidate count (small — tens, per §11). Optionally **sample** drops (keep all
  selected/over-cap + the top-K near-miss drops) if volume bites; the counterfactual is then exact
  for re-ranking and approximate only for deep-floor recoveries — state that in the report.
- **Privacy/retention** (system-design §12, tech §13): this table holds `entities`/titles ⇒ it is
  **scoped data** — same RLS treatment as `digest`/`digest_item` (M2 Phase 4), and it joins the
  **GDPR per-subscriber deletion cascade** (tech §13) and a retention horizon (default = eval
  window + margin). It is a *cache for eval*, droppable.

This is the single load-bearing design decision of Part II: **without the frozen candidate set,
offline tuning can only ever re-rank what was shown; with it, you can ask the counterfactual that
actually matters — "what would a different scorer have done?"**

## 6. Harness shape (pure core + I/O shell), and the CLI

Same architecture discipline as the scorer:

```rust
// PURE core — fixture-testable, deterministic, no DB:
pub struct LabeledDigest { /* frozen candidates + verdicts + the feedback labels joined in */ }
pub struct EvalReport {
    pub precision: f64, pub fp_rate: f64, pub drop_regret: u64,
    pub aggregation_error_rate: f64, pub label_coverage: f64,
    pub confusion: Confusion, pub per_subscriber: Vec<(Uuid, EvalReport)>,
    pub n_labeled: u64, pub n_delivered: u64,
}
pub fn evaluate(digests: &[LabeledDigest]) -> EvalReport;                       // Mode A math
pub fn evaluate_counterfactual(digests: &[LabeledDigest], cfg: &ScoringConfig,
                               now_of: impl Fn(&LabeledDigest)->DateTime<Utc>)  // Mode B (re-runs score_and_select)
    -> EvalReport;

// I/O shell (the bulletin binary):
pub async fn run(pool, window, config: Option<ScoringConfig>, subscriber: Option<Uuid>) -> Result<EvalReport>;
```

### CLI — `bulletin debug eval`

Mirrors `digest-explain`'s ergonomics (read-only, safe to re-run — README "Iteration loop" Tier-0/1):

```
bulletin debug eval                          # Mode A: KPIs over the trailing 30d, global
  [--window 30d] [--subscriber <id>]         # slice
  [--config <json|file>]                     # Mode B: counterfactual with an alternate ScoringConfig
  [--compare]                                # current config vs --config, side by side (precision/FP delta)
  [--sweep <param=lo..hi:step>]              # grid-sweep one knob, print the Pareto front
  [--json]                                   # machine-readable EvalReport
```

`--compare` / `--sweep` are the actual tuning loop: run against a `pg_dump` restore locally (Tier-0,
zero prod risk), find a config that drops FPs without losing TPs, then promote it into
`scoring_config`. **Read-only, idempotent, no send** — like `digest-explain`, it never advances a
watermark or writes.

### Determinism & tests (pure `core`)
- `evaluate` proptests: `precision, fp_rate, coverage ∈ [0,1]`; adding a TP never lowers precision;
  an all-correct labeled set ⇒ precision `1.0`; empty labels ⇒ precision reported as `None`/`n/a`
  (never a divide-by-zero `1.0`).
- **Counterfactual identity** (§5.2): `evaluate_counterfactual(d, config_that_made_d)` reproduces the
  recorded selection exactly — the proof the freeze is lossless.
- `insta` snapshot of an `EvalReport` over a fixture digest+feedback set.

## 7. Module layout & sequencing

```
crates/core/src/digest/
  select.rs     -- score_and_select replaces/extends select; Candidate, Signals, Verdict, DropReason
  score.rs      -- the four signal fns + combination (§2,§3.1); pure; proptested
  config.rs     -- ScoringConfig + load from scoring_config
crates/core/src/feedback/
  mod.rs        -- append-only log writes + the async affinity/edge update loop (§4.1) [feedback-processing, sibling work]
crates/core/src/eval/
  mod.rs        -- evaluate / evaluate_counterfactual (pure, §6)
  store.rs      -- load frozen candidates + join feedback (I/O shell)
crates/bulletin/src/debug.rs  -- `debug eval` subcommand
```

**Build order (within M4):**
1. **Rollups + scorer** — cluster/story rollup columns (dep, §0) → `score.rs`/`select.rs` →
   `scoring_config` → reason records into `digest_item.reasons`. Ships the scorer; degrades to M1
   baseline (§3.6) until affinity/stories exist.
2. **Feedback log + affinity loop** — `feedback` table; `care_more/less` → affinity (sibling to this
   spec; the scorer just *consumes* `affinity`).
3. **Candidate feature freeze** — `digest_candidate` + `affinity_snapshot` (§5.3). The enabler for
   Mode B.
4. **Eval harness** — `eval` module (Modes A & B), KPI gauges, `debug eval` CLI.

**Dependencies / cross-refs:** rollups & per-subscriber linking (M3) make corroboration & richness
non-trivial; RLS (M2 Phase 4) governs `digest_candidate`; the affinity/edge *update* loop is
feedback-processing (system-design §10.3), specced here only as the scorer's input and the harness's
label source.

## 8. Open tuning questions (for the harness to answer, not to guess)

These are exactly the §15 / tech-§11 open items this component surfaces — and the harness is the tool
that resolves them empirically rather than by argument:

- `relevance_floor`, `base_affinity` posture (subscribed⇒shown vs must-earn), richness/Story
  threshold, and the `max_stories`/`max_notes` caps — sweep against precision/FP (§5.2).
- The weight vector `w_severity`, `w_corroboration`, `corr_k`, `recency_half_life` — and whether the
  multiplicative combination (§3.1) beats an additive one (the harness can A/B *combination forms*,
  since it re-runs the whole pure scorer).
- Whether to fold **engagement** (open/click) into the label model as additional positive signal —
  re-adds as more `feedback.kind`s, no schema change (§4.2).
- Drop sampling vs full freeze for `digest_candidate` (§5.3) — decided by measured row volume.
