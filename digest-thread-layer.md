# Digest System — The Thread Layer & Tiered Identity

**Status:** Draft (design conversation, 14 June 2026)
**Companion to:** `digest-system-design.md` (§4 scopes, §6 data model, §8 aggregation, §9–§10),
`digest-technical-architecture.md` (§2 topology, §5 type modeling).
**What it owns:** the extension that turns the aggregator from a *stateless topical filter* into a
*stateful model of the user* — **Threads** (the persistent threads of a user's life), the **tiered
probabilistic identity** layer that feeds them, and **confidence as a first-class signal** that flows
all the way to rendering and back through feedback.

This is a **designed-for, deferred** layer: it lands *after* per-subscriber linking (roadmap M3) and
relevance/trust (M4), and every piece below is schema-additive with split-triggers, in the same spirit
as the embedding / shared-public-story-cache deferrals.

---

## 1. Motivation — the streams metaphor

Sources are **horizontal streams** of events flowing through time. The existing content graph already
stitches them locally:

```
Event → Cluster → Story → Thread
(atom)  (within-  (cross-  (cross-TIME weave: many stories/clusters over
        stream)   stream)   weeks/months — the persistent "thread of a life")
```

- A **`Cluster`** is a *local* stitch within one stream (one PR, one Slack thread).
- A **`Story`** is a *vertical* stitch across streams at one moment — one *happening* (the incident
  this week). It is the unit of a single digest item.
- A **`Thread`** is the vertical weave that runs the *full height of time* — the persistent thing a
  happening belongs to (the Acme migration over months; the on-call rotation; the relationship with a
  report). It is the **memory and context** a Story is scored and rendered against.

The current design (§8) reduces "meaning" to entity co-occurrence: grouping is key-equality, linking is
"these clusters share an entity / URL / timestamp." That is a topical filter with no model of the
*user*. "Understand the user's life and happenings" needs a **stateful, evolving model of a person** —
what they're working on, who matters, what's unresolved, what changed since they last looked. Threads
are that model; Stories remain the per-fire synthesis that hangs from them.

---

## 2. `Thread` — durable, recomputable, per-subscriber state

Three properties pin it down, each preserving an existing invariant:

1. **Always Private, one owner.** A Thread is `subscriber_id`-scoped exactly like `story` (§4). Public
   *entities* may be canonicalized globally; Thread *membership and affinity are never shared*. No new
   cross-tenant read path — risk #1 (§12) is untouched. `thread_maintenance` runs in the subscriber RLS
   context.
2. **Cache over the durable log, not a new source of truth.** A Thread's durable *inputs* are the event
   log + the `feedback` log + explicit user declarations (all already append-only). The Thread rows are
   a **recomputable cache**, same status as `cluster`/`story`: lose them and `thread_maintenance`
   rebuilds them from history. The CQRS guarantee (§3.0) — "the durable log is the only truth" — still
   holds.
3. **Formed in the background, read on the hot path.** Threads evolve on the **world's clock** (write
   side, best-effort); projection only ever *reads* their precomputed product. This is the same
   decoupling `public-build` already uses — under load it falls *behind*, never *wrong*.

This is the change that flips the system from "filter" to "model of the user" **without** touching the
fire-time purity that makes digests punctual.

---

## 3. Tiered identity resolution — the prerequisite

Threads are worthless if `acme-corp`, `Acme Corp`, `acme.com`, and the GitHub org `acme` are four
different nodes. But a **rigid 1:1 canonical map is a dead end**: identity is not a thing you commit
once. The fix is *not* "fuzzy replaces exact" — it is a **tiered resolver producing probabilistic,
revisable equivalence edges**, where exactness is the high-confidence floor, not the whole thing.

### 3.1 Two kinds of identity evidence

- **Authority-minted identifiers** — GitHub org/node id, Slack `U0123`, email address, DOI, CVE id,
  normalized URL. These are exact *by construction* (a join on a key an authority minted), never wrong,
  and free. Discarding them for cosine similarity would *manufacture* false merges. Keep them as the
  certain backbone.
- **Surface forms / display names** — "Acme Corp", "@dlewis", "the auth service". No authority, no key;
  exact match both over- and under-merges. This is the residual where graded, probabilistic matching is
  mandatory.

So: *for authority ids, exactness is real and must be exploited; for natural-language references, it is
impossible in principle and must not be forced.*

### 3.2 Identity as a probabilistic edge graph (the §8.2 trick, one level down)

Replace the alias table with **equivalence edges with confidence and provenance**. A canonical identity
is a **connected component over edges above a context threshold, with stable id-forwarding** — the same
union-find-with-`merged_into` machinery already used for stories and threads.

```
entity_edge(scope, a, b, confidence, evidence, source)   -- NOT a 1:1 map
  source = exact_id   → 1.0    authoritative key join (hard merge)
         = normalized → ~0.99  case-fold, domain/handle canonicalization
         = lexical    → 0.5–0.9 string similarity, shared context
         = embedding  → later, additive ("same meaning, no shared token")
identity = connected components over edges ≥ θ, ids forwarded across recompute
```

Three properties make this robust where the rigid map rots:

- **Soft edges weight, they don't collapse.** A 0.7 "Dana"↔"Dana Lewis" edge does *not* unify the two
  identities; it raises their co-thread probability and blocking recall, then is **confirmed or denied
  by accumulating evidence** (a conflicting authoritative id is a `cannot-link` veto). Only
  confidence-1.0 edges hard-merge. This is "graded identity that degrades gracefully" vs "a wrong guess
  that silently corrupts a thread."
- **Revisable, via the feedback channel that already exists.** The §10.3 "wrong aggregation"
  must-link / cannot-link signal extends straight down to the entity level — one click drops or adds an
  edge in *that subscriber's* graph, effective next recompute, nothing shared to mutate.
- **No embeddings in v1, but forward-compatible.** Exact ids + normalization + cheap lexical similarity
  cover most identity in a dev/work context; an embedding-similarity edge source slots in later as "one
  more edge source with a confidence," mirroring §8.2's "one more blocking source."

### 3.3 Why not fuzzy-first

Fuzzy-as-*replacement* is actively worse: false merges sit **upstream of relevance**, so they compound
(wrong entity → wrong thread → wrong weights → wrong inclusion) and are nearly unexplainable. And
embedding every entity from day one is the "reach for the vector DB" move the design already rejects for
clusters. Exact-id joins are free, certain, and explainable; let them carry the 80%, and let the graded
layer handle only the residual — with its confidence surfaced.

---

## 4. Confidence as a first-class, rendered signal

Confidence is not only an internal scoring knob — it is a **product surface**, which lines up exactly
with the trust thesis (§1, §10.2): a system that renders its own doubt is the honest version of
"transparency." The resolver's tier flows all the way to the frontend and back.

- **Frontend sees a *band*, not a float.** The resolver emits `Confirmed | Probable | Uncertain`;
  rendering maps tier → treatment (real avatar / avatar + badge / question-mark placeholder). The raw
  score stays server-side. One confidence vocabulary spans **identity *and* edges** — a story→thread
  assignment or a cluster→cluster link renders "possibly part of *the Acme migration*" with the same
  visual grammar as "possibly Dana."
- **Avatar provenance must equal identity provenance** *(the one real footgun)*. Rendering a real
  profile picture asserts "this specific person"; a false-confident avatar is worse than a question mark
  — it misleads and can attach the wrong face to private content (a §12 hazard). Rule: **show the
  guaranteed avatar only when the image comes from the same authority that minted the exact id** (the
  Slack `U0123` *carries* its avatar). Avatar provenance tracks identity provenance, full stop.
- **The question mark is also the correction affordance** — that's the flywheel. The "?" is the click
  target for the §10.3 entity-level feedback: tap → "yes, this is Dana Lewis" / "no, different person" →
  confirm or veto the edge → the graph improves for every future digest. The *render of doubt is the
  resolution UI*.

Two guardrails:

- **The weight and the render come from the same number.** If a 0.6 identity edge contributed 0.6× to a
  thread's relevance (attenuate, never gate), it must *also* render as Uncertain. One confidence value,
  two consumers — never a soft merge that silently inflates relevance while the UI implies certainty.
- **Budget the doubt.** A digest that is a sea of question marks reads as "unsure of everything." Cap
  visible "?"s and aggregate the tail ("…and 3 possibly-related items"). Confident-by-default,
  uncertain-when-it-matters.

The resolver's output to rendering is, per entity/edge reference:
`{ display_name, canonical_id?, confidence_band, evidence, avatar_ref? (authoritative-only) }` — which
is just the §10.2 reason-record reshaped for the frontend, so it is free.

---

## 5. Runtime placement — one new background job, two cheap fire-time steps

The read/write split (§3.0/§3.1) extends by exactly one job:

```
MATERIALIZATION (write side · world's clock · best-effort · durable cache)
  public-build         (unchanged)
  private-build        (unchanged) — now also writes canonical_entities (resolved at build time)
  thread_maintenance   NEW · per-subscriber · relaxed cadence (post-digest + nightly sweep, coalescing)
                       entity-edge resolution + community detection → thread rows + projected weights

PROJECTION (read side · subscriber's wall clock · punctual · pure over snapshot)
  generate:
    link → STORIES                              (unchanged)
    + thread-assign  story → thread_id          NEW · bounded GIN lookup (reuses cluster-blocking)
    gate · rank · classify                      now read thread-projected entity weights
    + thread-diversity cap, thread-grouped render   NEW · in-memory over already-selected items
```

### 5.1 `thread_maintenance(subscriber)` — background, subscriber RLS context

1. **Resolve identity:** materialize the subscriber's `entity_edge` graph (exact-id + normalized +
   lexical sources) → connected components ≥ θ → canonical ids (id-forwarded). (A sub-pass of the same
   co-occurrence graph below — equivalence is its tightest-confidence subset.)
2. **Build the engaged co-occurrence graph** over a rolling window W (≈90d): nodes = canonical ids in
   own-private clusters + public clusters the subscriber engages; edges = co-occurrence within a cluster
   or a delivered story (durable `digest_item` history), weighted by frequency × recency.
3. **Anchor pinned/declared threads** as fixed seed communities.
4. **Community detection = label propagation** (near-linear, deterministic with fixed seed + stable
   tie-break by canonical id; Louvain is the quality upgrade lever). Communities → candidate threads.
5. **Map communities → threads with id-forwarding** (the §8.2 trick): keep the id the entities mostly
   carried; merge → oldest-id-wins + `merged_into`; split → new id. Stable ids → stable deep-links and
   feedback targets.
6. **State machine:** `last_story_time` past `dormancy_horizon` → `dormant`; past `archive_horizon` and
   `!pinned` → `archived` (retained for reactivation, excluded from active weight projection).
7. **Affinity update:** `affinity = decay(prior) + Σ feedback_on_thread + engagement`, bounded. Per-thread
   decay fixes "cared in Q1, weighted forever." Recompute `baseline_rate` for novelty.
8. **Project weights:** distribute each active thread's affinity across its canonical entities → refresh
   the subscriber's `entity_weight` map (the hot-path relevance input).

Cost is bounded by the subscriber's entity count (hundreds to low thousands — *a life, not the
firehose*). Milliseconds; relaxed cadence; best-effort; coalescing. Never blocks a fire.

### 5.2 Projection (fire-time) changes

- **Thread-assign:** for each freshly-linked story, GIN-lookup threads sharing ≥ k canonical entities,
  pick best by `overlap × affinity`, set `story.thread_id`. Bounded: few stories × few candidate
  threads — the *same* pattern as `cluster_entities` blocking.
- **Relevance gains a Thread term** — *the rescue for the missed-because-split case*:
  `relevance += Σ_{e ∈ story.canonical_entities} entity_weight[e]`. Same unnest-and-sum already done for
  `affinity`; the weights now encode thread membership. A story whose individual pieces each fall below
  `relevance_floor` clears the bar because it **advances a thread you've invested in** — precisely the
  signal that was split across sources and therefore missed.
- **Priority gains two ML-free terms:** **corroboration** (`source_diversity` now feeds *priority*, not
  only richness/format — independent sources lighting up is importance, not a render hint); and
  **reactivation/novelty** (a story landing on a `dormant` thread, or a thread spiking over its
  `baseline_rate`, gets a salience bump).
- **Thread-diversity cap:** within the existing caps (§8.4), bound stories-per-thread (≈ ≤2) so one busy
  thread can't monopolize — forces breadth across the user's life.
- **Render grouped by Thread with a delta line:** "**Acme migration** — staging cutover landed; 2
  follow-ups assigned to you." The delta is computed exactly like the recently-surfaced damping (§9.4):
  stories on this thread with new events since the thread last appeared in a *delivered* `digest_item`.

---

## 6. Performance accounting

The hot path today (§11) is per-subscriber generation fan-out. Threads add **nothing asymptotic** to
it:

| Fire-time step | Cost | Why it's free |
|---|---|---|
| Thread-aware relevance | same `O(entities)` unnest-and-sum already done for affinity | weights precomputed by `thread_maintenance`; richer table, identical op |
| Story→thread assign | few stories × few candidate threads, GIN lookup | reuses the `cluster_entities` blocking pattern |
| Corroboration / novelty / reactivation | O(1) per story | reads cached `source_diversity`, `baseline_rate`, thread `state` |
| Thread-diversity cap + grouped render | in-memory over already-selected items | no new I/O |

All genuine work — identity resolution, community detection, decay, weight projection, novelty
baselines — lives in `thread_maintenance`, which is per-subscriber, bounded by the (small) entity graph,
best-effort, coalescing, and **never on the punctual path**. It mirrors `public-build`'s "fall behind,
never wrong" contract. Two amortization levers in reserve (consistent with the deferred *shared
public-story cache*): a **shared public co-occurrence baseline** (the public slice of the entity graph
is identical across subscribers — precompute once) and the `relevance_weight` table split-trigger.

---

## 7. Invariants preserved

- **Scope isolation (§12 risk #1):** Threads/entity resolution are per-subscriber, run in the subscriber
  RLS context, never span tenants. New job, same two-context enforcement.
- **Determinism / idempotency:** fire-time assignment is a pure function of (snapshot, current thread
  state); `thread_maintenance` is the sole writer of thread/identity structure and is idempotent
  (recompute + id-forwarding). `generate` stays idempotent; `digest`/`digest_item` keys unchanged.
- **Durability / recomputability:** thread rows and entity components are caches over the durable event
  + feedback + declaration logs; reconstructible from truth. No-loss guarantee untouched.
- **Explainability & trust (§10.2):** every thread/identity decision emits a reason record (with
  confidence). Pin/declare/mute and the "?"-affordance give the user direct control of their own threads
  and merges.

---

## 8. Schema (additive; §6 style, with split-triggers)

```sql
CREATE TABLE thread (                                   -- the persistent "thread of a user's life"
  id                  uuid PRIMARY KEY DEFAULT uuidv7(),
  subscriber_id       uuid NOT NULL REFERENCES subscriber(id),  -- always Private-scoped, one owner (§4)
  origin              text NOT NULL CHECK (origin IN ('declared','emergent')),
  label               text,                             -- user-set, else auto (top entities; LLM summary later)
  state               text NOT NULL DEFAULT 'active'
                        CHECK (state IN ('active','dormant','archived')),
  pinned              boolean NOT NULL DEFAULT false,   -- declared threads: never auto-merged/archived
  merged_into         uuid REFERENCES thread(id),       -- id-forwarding on re-clustering (the §8.2 trick)
  canonical_entities  jsonb NOT NULL DEFAULT '[]',      -- the thread's resolved entity spine
  affinity            real NOT NULL DEFAULT 0,          -- weight from feedback + engagement (decays)
  story_count         int  NOT NULL DEFAULT 0,
  source_diversity    int  NOT NULL DEFAULT 0,
  baseline_rate       real NOT NULL DEFAULT 0,          -- events/day baseline → novelty/burst term
  first_seen          timestamptz,
  last_story_time     timestamptz                       -- dormancy + reactivation salience
);
CREATE INDEX thread_active   ON thread (subscriber_id, state, last_story_time DESC);
CREATE INDEX thread_entities ON thread USING gin (canonical_entities);   -- fire-time story→thread match
CREATE INDEX thread_merged   ON thread (merged_into) WHERE merged_into IS NOT NULL;

CREATE TABLE entity_edge (                              -- probabilistic identity (NOT a 1:1 alias map)
  scope_kind          text NOT NULL,                   -- 'public' shared; 'private' per-subscriber
  scope_subscriber_id uuid,                            -- null iff public
  a                   text NOT NULL,                   -- surface form / id
  b                   text NOT NULL,
  confidence          real NOT NULL,                   -- 1.0 exact_id … 0.5 lexical
  source              text NOT NULL,                   -- exact_id | normalized | lexical | embedding | feedback
  evidence            jsonb NOT NULL DEFAULT '{}',
  PRIMARY KEY (scope_kind, scope_subscriber_id, a, b)
);
-- canonical identity = connected components over edges ≥ θ (computed in thread_maintenance, id-forwarded)

ALTER TABLE cluster ADD COLUMN canonical_entities jsonb NOT NULL DEFAULT '[]';  -- resolved, build-maintained
ALTER TABLE story   ADD COLUMN canonical_entities jsonb NOT NULL DEFAULT '[]';
ALTER TABLE story   ADD COLUMN thread_id uuid REFERENCES thread(id);            -- primary thread (0..1 in v1)
ALTER TABLE digest_item ADD COLUMN thread_id uuid;                              -- thread-grouped render + delta
```

The fire-time relevance input (active-thread affinity projected onto entities) lives in
`subscriber.affinity` jsonb as an `entity_weight` map for v1 (consistent with §6 "jsonb now, normalize
on trigger"); **split-trigger:** the keyed lookup gets hot → promote to a
`relevance_weight(subscriber_id, canonical_id, weight)` table. `thread_maintenance` is its sole writer.

**Split-triggers:** `story.thread_id` (single primary) → a `story_thread` join table when a happening
must belong to several threads; `entity_edge` jsonb-evidence → normalized when resolution gets hot; the
shared public co-occurrence baseline when per-subscriber `thread_maintenance` cost bites.

### Rust types (mirror tech §5.3)

```rust
pub enum ConfidenceBand { Confirmed, Probable, Uncertain }   // the render/scoring contract

pub struct Thread {
    pub id:               Id<Thread>,
    pub subscriber_id:    Id<Subscriber>,     // always Private-scoped
    pub origin:           ThreadOrigin,        // Declared | Emergent
    pub label:            Option<String>,
    pub state:            ThreadState,          // Active | Dormant | Archived
    pub pinned:           bool,
    pub merged_into:      Option<Id<Thread>>,
    pub canonical_entities: Vec<CanonicalId>,
    pub affinity:         f32,
    pub story_count:      i32,
    pub source_diversity: i16,
    pub baseline_rate:    f32,
    pub first_seen:       OffsetDateTime,
    pub last_story_time:  OffsetDateTime,
}
```

New apalis job kind: `thread_maintenance` alongside
`poll_connection | process_webhook | public_build | generate_digest`.

---

## 9. Phased plan — shippable, independently evaluable, performance-safe at each step

Each phase is gated behind config and evaluated **read-only** via the existing `digest-explain` harness
before it touches a real digest. The layer as a whole lands **after** roadmap M3 (linking) and M4
(relevance).

- **Phase 0 — Tiered identity.** `entity_edge` graph + tiered sources (exact-id, normalized, lexical) +
  soft-merge + connected-components-with-forwarding; `canonical_entities` written at build time; the
  entity-level extension of the §10.3 feedback signal. No digest behavior change yet — verify resolution
  quality and confidence bands offline.
- **Phase 1 — Threads in shadow mode.** `thread` table + `thread_maintenance` (community detection +
  id-forwarding + state/decay). Declared/pinned threads. Threads built but unused by digests — eval
  coherence via `digest-explain` with zero delivery risk.
- **Phase 2 — Thread-weighted relevance.** Project weights into `entity_weight`; relevance reads them
  behind a flag. The missed-because-split case starts firing. A/B the floor via `digest-explain`.
- **Phase 3 — Assignment, salience, render.** Fire-time `thread_id`; corroboration / novelty /
  reactivation priority terms; thread-diversity cap; thread-grouped digest with delta lines; confidence
  bands + avatar/question-mark in the rendered output.
- **Phase 4 — Thread feedback + canonicalization growth.** `target_type='thread'` care-more/less/done;
  the "?"-driven entity merge/split confirmations; feedback-driven alias growth; (optional) LLM thread
  labels/summaries under the §12 consent rules.

---

## 10. Open questions (tuning surface)

- LPA vs Louvain; `k` for story→thread overlap; `θ` per edge source — need real per-subscriber graphs.
- `dormancy_horizon` / `archive_horizon`, affinity decay rate, reactivation bump magnitude.
- Single primary `thread_id` vs many-to-many `story_thread` (a happening can touch two threads). Ship
  single-primary; join table is the documented split-trigger.
- Cold-start: a new subscriber has no threads → bootstrap from declarations + first N digests'
  co-occurrence; until then relevance falls back to today's affinity (the thread term is purely
  additive, so this degrades gracefully).
- Confidence-band thresholds and the visible-doubt budget per digest.
- Where the public co-occurrence baseline gets shared vs recomputed per subscriber.
