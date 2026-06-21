//! Per-subscriber linking — fuse a subscriber's candidate clusters into cross-source **stories**
//! (design §8.2, the product's reason to exist: "the connections you would have missed").
//!
//! This module is the **pure core**: [`link`] is a deterministic function of
//! `(clusters, prior assignment, id minter)` with no I/O and no ambient clock — so it is exhaustively
//! property-tested for determinism and id-stability ([`store`] handles persistence, and the digest
//! flow wires it in). The pipeline runs it per subscriber inside `GenerateDigest`, because a story
//! can fuse public clusters with that subscriber's *own* private clusters and so can't be a global
//! precompute (design §4/§9.1).
//!
//! The algorithm is textbook entity-resolution, in four stages (design §8.2):
//! 1. **Blocking** — an inverted index over `entities` yields only the candidate pairs that share a
//!    key, the O(n²) guard. We never compare two clusters with nothing in common.
//! 2. **Scoring** — each candidate pair gets a weighted score (a **weighted** entity Jaccard +
//!    temporal closeness). Entities are *graded* by namespace prior × corpus IDF × feedback multiplier
//!    ([`entity_weight`]), so a rare, specific shared token corroborates far more than a common one (a
//!    broad place, a bot, a stock agency). A pair is promoted to a **strong** edge when it shares a
//!    *strong* key (a CVE/URL — `entity::link_strength`).
//! 3. **Components** — connected components (union-find) over the edges; each is a story. Computed
//!    fresh every run from the full candidate set, so it is order-independent (no arrival-order bias)
//!    and late retro-connections form automatically. **Strong edges merge anything; a weak edge may
//!    not collapse two already-delivered stories** — the asymmetric guard against single-linkage
//!    blobs (the well-known chaining failure of transitive closure).
//! 4. **Forwarding** — map each component back onto a story id from the prior assignment so ids stay
//!    stable for the subscriber across recomputes; a retro-merge keeps the oldest id and tombstones
//!    the loser (`merged_into`).

pub mod store;

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::common::entity::{link_strength, LinkStrength};
use crate::common::kind::{ContentKind, SourceKind};

/// A candidate cluster for linking: its identity, the blocking substrate (`entities`), the recency
/// span the story rollup aggregates, and the M4 scoring signals it folds onto the story (source for
/// `source_diversity`, `event_count`/`content_depth` for richness, `max_severity` for priority, and
/// whether it is the subscriber's own private content for the scope bonus). The pure input — no DB
/// types leak in beyond the source/depth enums.
#[derive(Debug, Clone)]
pub struct LinkCluster {
    pub id: Uuid,
    pub entities: Vec<String>,
    pub first_event_time: DateTime<Utc>,
    pub last_event_time: DateTime<Utc>,
    pub source: SourceKind,
    pub event_count: i32,
    pub content_depth: ContentKind,
    pub max_severity: Option<i16>,
    /// True when this cluster is the subscriber's own private content (vs a shared public one) — the
    /// scope-bonus input (design §8.3). Always the caller's own, by the candidate-set scope.
    pub is_own_private: bool,
}

/// One cluster's membership in the *prior* assignment, read back to forward stable ids. `delivered`
/// (the story's `last_delivered_at` is set) gates the asymmetric-merge rule.
#[derive(Debug, Clone)]
pub struct PriorMember {
    pub cluster_id: Uuid,
    pub story_id: Uuid,
    pub delivered: bool,
}

/// A member of a linked story: the cluster and *why* it belongs (design §10.2). The reason is its
/// strongest link to a sibling; `None` for a lone cluster (a singleton story has no connection to
/// explain) — which renders exactly like a pre-M3 one-cluster digest item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterRef {
    pub cluster_id: Uuid,
    pub link_reason: Option<String>,
}

/// A linked story: a connected component of clusters with a (stable, forwarded) id, plus the
/// cross-source rollups M4 scoring reads (design §8.3–§8.4). The rollups are aggregated over the
/// component's members in [`forward_ids`], so the story is the single home for its scoring features
/// (M3-handoff seam #1). The story's *entity spine* (for the thread relevance term) is derived by the
/// digest flow from the member clusters' entities — not duplicated here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkedStory {
    pub id: Uuid,
    pub clusters: Vec<ClusterRef>,
    pub first_event_time: DateTime<Utc>,
    pub last_event_time: DateTime<Utc>,
    /// Σ of member `event_count` — breadth (richness).
    pub event_count: i32,
    /// Number of distinct member sources — the "across sources" breadth signal (richness).
    pub source_diversity: i32,
    /// Max member `content_depth` — depth (richness).
    pub content_depth: ContentKind,
    /// Max member `max_severity`, or `None` — a priority boost.
    pub max_severity: Option<i16>,
    /// Whether any member is the subscriber's own private content — the scope-bonus trigger.
    pub has_private: bool,
}

/// A retro-merge: a prior story whose clusters now fall inside another component, so its id is
/// forwarded to the (older) `survivor` and its row tombstoned (`merged_into`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Merge {
    pub loser: Uuid,
    pub survivor: Uuid,
}

/// The recompute result: the subscriber's current stories plus the retro-merges to record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Assignment {
    pub stories: Vec<LinkedStory>,
    pub merges: Vec<Merge>,
}

// ── Tunables ───────────────────────────────────────────────────────────────
// Initial edge weights/thresholds. Deliberately conservative; design §15 moves these to a config
// table in M4 once there are real digests to tune against. Strong edges bypass the weak threshold
// entirely (a shared CVE/URL is a near-certain link).

/// Weight on entity Jaccard in a weak edge's score.
const W_JACCARD: f64 = 0.7;
/// Weight on temporal closeness in a weak edge's score.
const W_TEMPORAL: f64 = 0.3;
/// A weak edge forms only at or above this score — corroboration beyond a single shared weak token.
const WEAK_EDGE_THRESHOLD: f64 = 0.35;
/// A weak edge also requires at least this much **weighted** entity overlap (the weighted Jaccard over
/// the distinctive linkable entities, [`build_edge`]). Temporal proximity **corroborates** an edge; it
/// must not be able to *carry* one. Without this floor two same-window items linked on a single broad
/// shared token — `place:germany`, a shared stock-photo agency — because the temporal term
/// (`W_TEMPORAL`) alone nearly clears the threshold, chaining unrelated coverage into one blob (the
/// reported "wildly inaccurate" grouping). Tuned so a genuine same-story pair (several shared
/// distinctive entities, or one dominant high-weight one) still links, while a lone broad coincidence —
/// already IDF-demoted to a small weight — does not.
const WEAK_MIN_JACCARD: f64 = 0.2;
/// A weighted Jaccard at/above this is **saturated**: the shared entities are (essentially) the whole of
/// *both* clusters' linkable sets, so the weight cancels in the ratio (`w/(w+w−w)=1`) and `wj` reports a
/// perfect match no matter how IDF-demoted the shared token is. This is the one case where IDF can't bite
/// — two entity-sparse items sharing a single broad token (`place:germany` and nothing else linkable).
const SATURATED_WJ: f64 = 0.999;
/// For a **saturated** weak edge ([`SATURATED_WJ`]) the ratio is uninformative, so the shared mass must
/// clear this **absolute** weight instead — restoring IDF's say-so. A single broad, IDF-demoted token
/// (high df ⇒ small weight) falls below it and can't fuse two sparse items, while a rare/specific shared
/// token, or several shared tokens (their weights sum), clears it. Picked so a `place:`/`org:` that recurs
/// across a large fraction of the candidate set is rejected while a distinctive single share passes; like
/// the other thresholds this is conservative and lands in the §15 config table once tuned on real digests.
const MIN_SATURATED_SHARED_WEIGHT: f64 = 1.2;
/// Temporal closeness decays linearly to zero across this many days apart.
const TEMPORAL_WINDOW_DAYS: f64 = 14.0;

/// Per-entity **linking-weight override** (the feedback/affinity seam, §10.3): a multiplier on an
/// entity's graded weight, `1.0` for any entity absent. This is where a subscriber's care-more /
/// care-less affinity flows *into linking* once wired — "weight connections through what I care about
/// more, and through what I've muted less". The thread layer already produces a per-entity affinity map
/// of exactly this shape ([`thread::store::load_entity_weights`](crate::thread::store::load_entity_weights)),
/// so wiring it is a one-line change at the [`link_with`] call site. Empty today ⇒ every multiplier 1.0.
pub type EntityWeights = std::collections::BTreeMap<String, f64>;

/// Pairwise **must-link / cannot-link** corrections over *cluster ids* — the §8.7 "this aggregation is
/// wrong / right" feedback, applied as an edge added / dropped in the subscriber's next recompute.
/// `cannot_link` drops the direct edge between two clusters (they never fuse on it); `must_link` forces
/// a strong edge (they always fuse). Stored normalized `(min, max)` so the pair is order-independent.
/// Empty today — the seam for the story-level feedback the [`crate::feedback`] log already records
/// (`TargetType::Story`), pending the projection that turns a "wrong story" correction into a
/// cluster-pair constraint here.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LinkConstraints {
    pub must_link: BTreeSet<(Uuid, Uuid)>,
    pub cannot_link: BTreeSet<(Uuid, Uuid)>,
}

impl LinkConstraints {
    /// Normalize a cluster-id pair to `(min, max)` so membership is order-independent.
    fn key(a: Uuid, b: Uuid) -> (Uuid, Uuid) {
        if a <= b {
            (a, b)
        } else {
            (b, a)
        }
    }

    /// Record a must-link ("these belong together") between two clusters.
    pub fn must(&mut self, a: Uuid, b: Uuid) {
        self.must_link.insert(Self::key(a, b));
    }

    /// Record a cannot-link ("these don't belong together") between two clusters.
    pub fn cannot(&mut self, a: Uuid, b: Uuid) {
        self.cannot_link.insert(Self::key(a, b));
    }
}

/// The per-subscriber tuning [`link_with`] applies — the feedback seams, defaulting to neutral. Held as
/// one struct so a new seam is added in one place and every caller threads it unchanged. `Default` is
/// "no feedback": empty weight overrides (every multiplier 1.0) and no constraints, i.e. exactly the
/// behavior of the bare [`link`].
#[derive(Debug, Default, Clone)]
pub struct LinkTuning {
    pub entity_weights: EntityWeights,
    pub constraints: LinkConstraints,
}

/// Link a subscriber's candidate `clusters` into stories, forwarding ids from the `prior` assignment.
/// `mint` allocates a fresh id for a genuinely new component; it is injected (not an ambient
/// `Uuid::now_v7`) so the function stays pure and tests are deterministic. The pipeline passes
/// `Uuid::now_v7`. Equivalent to [`link_with`] under the neutral [`LinkTuning::default`] (no feedback).
pub fn link(
    clusters: &[LinkCluster],
    prior: &[PriorMember],
    mint: impl FnMut() -> Uuid,
) -> Assignment {
    link_with(clusters, prior, &LinkTuning::default(), mint)
}

/// Link with explicit per-subscriber [`LinkTuning`] — the feedback-aware entry point. Identical to
/// [`link`] but for the `tuning` it threads into edge scoring (the graded entity-weight overrides) and
/// component formation (the must/cannot-link constraints). The production path calls this so feedback,
/// once collected, flows in by populating `tuning` — no change to the algorithm below.
pub fn link_with(
    clusters: &[LinkCluster],
    prior: &[PriorMember],
    tuning: &LinkTuning,
    mut mint: impl FnMut() -> Uuid,
) -> Assignment {
    let mut edges = score_edges(clusters, &tuning.entity_weights);
    apply_constraints(&mut edges, clusters, &tuning.constraints);
    let components = components(clusters, &edges, prior);
    let reasons = member_reasons(clusters, &edges, &components);
    forward_ids(clusters, &components, &reasons, prior, &mut mint)
}

// ── Stage 2: edge scoring (over blocked candidate pairs) ─────────────────────

/// A scored link between two clusters (by index into `clusters`, `a < b`).
struct Edge {
    a: usize,
    b: usize,
    strong: bool,
    score: f64,
    reason: String,
}

/// What one candidate pair accumulates as each shared entity is scattered onto it (see [`score_edges`]):
/// the running sum of shared-entity *weights*, the strongest shared key (a `cve:`/`url:` → a strong
/// edge), and the highest-weight shared *weak* entity (its `link_reason`). A `'a` borrow of the index
/// keys — the entity strings live on the clusters for the whole pass.
#[derive(Default)]
struct PairAcc<'a> {
    /// Σ of the graded weights of the entities this pair shares — the numerator of the weighted Jaccard.
    shared_weight: f64,
    /// A shared near-unique id (`cve:`/`url:`), if any — promotes the pair to a strong edge. Lexically
    /// first (the index iterates in sorted order), for a deterministic reason string.
    strong_key: Option<&'a str>,
    /// The highest-weight shared *weak* entity (weight, token) — the most distinctive shared key, used
    /// as the edge's `link_reason`. Ties break to the lexically-first token (strict `>` on update).
    best_weak: Option<(f64, &'a str)>,
}

/// Blocking + **graded** scoring: build the candidate pairs via an inverted index over the *linkable*
/// entities, then score each as a weighted overlap. Every shared entity contributes its *weight* —
/// namespace prior × corpus IDF × feedback multiplier ([`entity_weight`]) — so a rare, specific shared
/// token (a CVE, a named person) corroborates a link far more than one that recurs across the candidate
/// set (a broad place, a bot, a stock-photo agency). Returns the edges that clear the bar: a shared
/// strong key (`cve:`/`url:`) unconditionally, else a weighted-Jaccard + recency blend over
/// [`WEAK_EDGE_THRESHOLD`] with at least [`WEAK_MIN_JACCARD`] overlap. Deterministic: the index, the
/// per-entity weights, and the pair accumulator are all built in a fixed (sorted) order.
fn score_edges(clusters: &[LinkCluster], entity_weights: &EntityWeights) -> Vec<Edge> {
    let n = clusters.len();
    // Inverted index entity → cluster indices, over *linkable* entities only (a `domain:`/`topic:` is
    // noise as an edge, so it never seeds a candidate pair). It doubles as the **document-frequency**
    // table: `members.len()` is df(entity), the IDF input. BTree keeps iteration stable.
    let mut index: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (i, c) in clusters.iter().enumerate() {
        for e in c.entities.iter().filter(|e| link_strength(e).is_some()) {
            index.entry(e.as_str()).or_default().push(i);
        }
    }

    // The graded weight of each linkable entity, computed once from its df. This is the "grade" — how
    // much *sharing this token* is worth — pre-seeded by the namespace prior and the live corpus IDF,
    // and nudged by any feedback override; no labelled data or denylist required.
    let weight: BTreeMap<&str, f64> = index
        .iter()
        .map(|(&e, members)| (e, entity_weight(e, members.len(), n, entity_weights)))
        .collect();

    // Per-cluster Σ of its linkable entities' weights — the |A|/|B| terms of the weighted Jaccard.
    let mut cluster_weight = vec![0.0f64; n];
    for (&e, members) in &index {
        let w = weight[e];
        for &i in members {
            cluster_weight[i] += w;
        }
    }

    // One scatter pass over the index: each entity adds its weight to every pair of clusters it
    // co-occurs in, and records whether it is a strong key or the pair's best weak reason — so a pair's
    // shared weight, strong-promotion, and reason all fall out without re-intersecting two clusters per
    // pair. The cost is Σ C(df,2) — the blocking work the old pair enumeration already paid.
    let mut acc: BTreeMap<(usize, usize), PairAcc> = BTreeMap::new();
    for (&e, members) in &index {
        let w = weight[e];
        let strong = link_strength(e) == Some(LinkStrength::Strong);
        for (oi, &i) in members.iter().enumerate() {
            for &j in &members[oi + 1..] {
                let p = acc.entry((i.min(j), i.max(j))).or_default();
                p.shared_weight += w;
                if strong {
                    p.strong_key.get_or_insert(e);
                } else if p.best_weak.is_none_or(|(bw, _)| w > bw) {
                    p.best_weak = Some((w, e));
                }
            }
        }
    }

    acc.into_iter()
        .filter_map(|((a, b), p)| build_edge(a, b, &p, clusters, &cluster_weight))
        .collect()
}

/// Turn one accumulated candidate pair into an [`Edge`], or `None` if it doesn't clear the bar. A shared
/// strong key is a strong edge at score 1.0; otherwise the weighted Jaccard (`shared / union` of the
/// entity weights) blended with temporal closeness must clear [`WEAK_EDGE_THRESHOLD`] *and* the overlap
/// itself must reach [`WEAK_MIN_JACCARD`] — temporal proximity corroborates a weak edge, it can't carry
/// one (the single-broad-token chaining guard).
fn build_edge(
    a: usize,
    b: usize,
    p: &PairAcc,
    clusters: &[LinkCluster],
    cluster_weight: &[f64],
) -> Option<Edge> {
    if let Some(key) = p.strong_key {
        return Some(Edge {
            a,
            b,
            strong: true,
            score: 1.0,
            reason: format!("shared {key}"),
        });
    }
    let union = cluster_weight[a] + cluster_weight[b] - p.shared_weight;
    let wj = if union > 0.0 {
        p.shared_weight / union
    } else {
        0.0
    };
    let score = W_JACCARD * wj + W_TEMPORAL * temporal_closeness(&clusters[a], &clusters[b]);
    if score < WEAK_EDGE_THRESHOLD || wj < WEAK_MIN_JACCARD {
        return None;
    }
    // Saturation guard: when `wj` is saturated (the shared entities are the whole of both clusters'
    // linkable sets) the ratio is a no-op and IDF can't demote a broad shared token, so fall back to an
    // absolute floor on the shared weight — a single IDF-demoted token can't fuse two sparse items, a
    // distinctive one (or several shared) still can. Non-saturated pairs are unaffected (the ratio
    // already carries the corroboration signal).
    if wj >= SATURATED_WJ && p.shared_weight < MIN_SATURATED_SHARED_WEIGHT {
        return None;
    }
    let (_, key) = p.best_weak?;
    Some(Edge {
        a,
        b,
        strong: false,
        score,
        reason: format!("shared {key}"),
    })
}

/// The graded weight of a shared linkable entity: **namespace prior × corpus IDF × feedback multiplier**.
/// The prior pre-seeds the kind's specificity (a named person outweighs a broad place); the IDF demotes
/// whatever recurs across *this* candidate set (a feed's house entities, a bot, a stock agency, the
/// repo owner shared by all of one account's repos), so quality emerges from the corpus rather than a
/// hand-maintained denylist; the `overrides` multiplier (default 1.0) is the feedback/affinity seam.
fn entity_weight(entity: &str, df: usize, n: usize, overrides: &EntityWeights) -> f64 {
    let mult = overrides.get(entity).copied().unwrap_or(1.0);
    namespace_prior(entity) * idf(df, n) * mult
}

/// The static **namespace prior** — the specificity grade of an entity *kind*, before the corpus IDF
/// adjusts for how common a particular token is. A unique-ish id (`cve:`/`url:`) and a named person are
/// the most specific; an `org:` or `repo:` is solid; a `place:` is often broad (a country); a non-linking
/// `domain:`/`topic:`/unknown is `0.0` (it never reaches scoring anyway, the index excludes it). These
/// are deliberately coarse — the IDF carries the within-kind discrimination — and are the natural knob a
/// future calibration (or the §15 config table) tunes against real digests.
fn namespace_prior(entity: &str) -> f64 {
    match crate::identity::namespace(entity).map(|(ns, _)| ns) {
        Some("cve") | Some("url") => 1.0,
        Some("person") => 1.0,
        Some("repo") | Some("user") => 0.9,
        Some("org") => 0.8,
        Some("place") => 0.7,
        _ => 0.0,
    }
}

/// Smoothed **inverse document frequency** over the candidate set: `ln((n+1)/(df+1)) + 1`. The `+1`
/// floor keeps a token that appears in *every* cluster at idf 1 rather than 0 — corroborated coverage
/// of one happening (three outlets, one shared place) must still link — while a rarer token scores
/// strictly higher. The smoothing also makes the small-corpus / cold-start case well-behaved (df is not
/// yet meaningful when `n` is tiny), so no separate minimum-`n` guard is needed.
fn idf(df: usize, n: usize) -> f64 {
    ((n as f64 + 1.0) / (df as f64 + 1.0)).ln() + 1.0
}

/// Apply the per-subscriber must/cannot-link [`LinkConstraints`] to the scored edges (the §8.7 feedback
/// seam): drop any edge a `cannot_link` forbids, then force a strong edge for each `must_link` pair
/// (upgrading an existing edge in place, else adding one). A no-op when both sets are empty — the
/// default, until story-level feedback is projected into cluster-pair constraints. Acts on the *direct*
/// edge between the two clusters (matching the design's "an edge dropped/added"); a cannot-link doesn't
/// chase a transitive merge through a third cluster.
fn apply_constraints(edges: &mut Vec<Edge>, clusters: &[LinkCluster], c: &LinkConstraints) {
    if c.must_link.is_empty() && c.cannot_link.is_empty() {
        return;
    }
    let pair_id = |e: &Edge| LinkConstraints::key(clusters[e.a].id, clusters[e.b].id);
    if !c.cannot_link.is_empty() {
        edges.retain(|e| !c.cannot_link.contains(&pair_id(e)));
    }
    for &(id1, id2) in &c.must_link {
        let (Some(i), Some(j)) = (index_of(clusters, id1), index_of(clusters, id2)) else {
            continue; // a constraint naming a cluster outside this candidate set is inert
        };
        if i == j {
            continue;
        }
        let (a, b) = (i.min(j), i.max(j));
        if let Some(e) = edges.iter_mut().find(|e| e.a == a && e.b == b) {
            e.strong = true;
            e.score = 1.0;
            e.reason = "must-link (feedback)".to_string();
        } else {
            edges.push(Edge {
                a,
                b,
                strong: true,
                score: 1.0,
                reason: "must-link (feedback)".to_string(),
            });
        }
    }
}

/// The candidate-set index of the cluster with id `id`, or `None` if it isn't in this set. Linear, but
/// only walked per must-link constraint (a handful), so it stays off the hot path.
fn index_of(clusters: &[LinkCluster], id: Uuid) -> Option<usize> {
    clusters.iter().position(|c| c.id == id)
}

/// Temporal closeness in `0..=1`: 1.0 for same-instant clusters, decaying linearly to 0 across
/// [`TEMPORAL_WINDOW_DAYS`]. Compares `last_event_time` (the freshness anchor).
fn temporal_closeness(a: &LinkCluster, b: &LinkCluster) -> f64 {
    let delta_days = (a.last_event_time - b.last_event_time).num_seconds().abs() as f64 / 86_400.0;
    (1.0 - delta_days / TEMPORAL_WINDOW_DAYS).max(0.0)
}

// ── Stage 3: connected components (union-find) with the asymmetric guard ─────

/// Disjoint-set forest over cluster indices, carrying per-component the set of **already-delivered
/// prior story ids** it contains — the state the asymmetric-merge guard needs. A set (not a bool):
/// the guard must distinguish "re-linking two clusters of the *same* delivered story" (allowed) from
/// "merging two *different* delivered stories" (weak edges may not).
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<u8>,
    delivered: Vec<BTreeSet<Uuid>>,
}

impl UnionFind {
    fn new(delivered: Vec<BTreeSet<Uuid>>) -> Self {
        let n = delivered.len();
        Self {
            parent: (0..n).collect(),
            rank: vec![0; n],
            delivered,
        }
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]]; // path halving
            x = self.parent[x];
        }
        x
    }

    /// Union the sets of `x` and `y` (no-op if already joined); the delivered-story sets merge onto
    /// the surviving root.
    fn union(&mut self, x: usize, y: usize) {
        let (mut rx, mut ry) = (self.find(x), self.find(y));
        if rx == ry {
            return;
        }
        if self.rank[rx] < self.rank[ry] {
            std::mem::swap(&mut rx, &mut ry);
        }
        self.parent[ry] = rx;
        if self.rank[rx] == self.rank[ry] {
            self.rank[rx] += 1;
        }
        let absorbed = std::mem::take(&mut self.delivered[ry]);
        self.delivered[rx].extend(absorbed);
    }

    /// Whether a weak edge between `a` and `b` would merge **two different** delivered stories — the
    /// case the guard forbids. False when either side carries no delivered story (a fresh cluster may
    /// attach) or when they share one (re-linking the same story, not a cross-story merge).
    fn merges_distinct_delivered(&mut self, a: usize, b: usize) -> bool {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return false;
        }
        let (da, db) = (&self.delivered[ra], &self.delivered[rb]);
        !da.is_empty() && !db.is_empty() && da.is_disjoint(db)
    }
}

/// Compute connected components as a map root-index → sorted member indices.
///
/// Two passes encode the asymmetric guard (design §8.2): **strong edges first** (a shared CVE/URL may
/// merge anything, including two delivered stories — this is the intended retro-merge), then **weak
/// edges**, each skipped if it would merge two *different* already-delivered stories — so weak
/// entity-overlap can attach a fresh cluster, and re-link clusters of the same story, but never
/// collapse two stories the subscriber has already seen as distinct. Weak edges are processed
/// strongest-first so a fresh cluster attaches to its best connection; ties broken by index, keeping
/// the result a pure function of the inputs.
fn components(
    clusters: &[LinkCluster],
    edges: &[Edge],
    prior: &[PriorMember],
) -> BTreeMap<usize, Vec<usize>> {
    let prior_story: BTreeMap<Uuid, Uuid> =
        prior.iter().map(|p| (p.cluster_id, p.story_id)).collect();
    let delivered_stories: BTreeSet<Uuid> = prior
        .iter()
        .filter(|p| p.delivered)
        .map(|p| p.story_id)
        .collect();
    // Per cluster: the delivered prior story it belongs to (a singleton set), else empty.
    let delivered: Vec<BTreeSet<Uuid>> = clusters
        .iter()
        .map(|c| {
            prior_story
                .get(&c.id)
                .filter(|s| delivered_stories.contains(s))
                .into_iter()
                .copied()
                .collect()
        })
        .collect();

    let mut uf = UnionFind::new(delivered);

    for e in edges.iter().filter(|e| e.strong) {
        uf.union(e.a, e.b);
    }

    let mut weak: Vec<&Edge> = edges.iter().filter(|e| !e.strong).collect();
    // Strongest weak edge first; deterministic tie-break by endpoints.
    weak.sort_by(|x, y| {
        y.score
            .partial_cmp(&x.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then((x.a, x.b).cmp(&(y.a, y.b)))
    });
    for e in weak {
        if uf.merges_distinct_delivered(e.a, e.b) {
            continue; // would collapse two already-delivered stories — only a strong edge may
        }
        uf.union(e.a, e.b);
    }

    let mut comps: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for i in 0..clusters.len() {
        let r = uf.find(i);
        comps.entry(r).or_default().push(i);
    }
    for members in comps.values_mut() {
        members.sort_unstable();
    }
    comps
}

/// Each cluster's `link_reason`: the strongest edge incident to it **within its own story** — a
/// strong edge outranks any weak one, then higher score wins. Edges the asymmetric guard skipped (a
/// weak edge across two delivered stories) are excluded by the same-component check, so a member never
/// cites a cluster it isn't actually grouped with. A cluster with no intra-story edge (a singleton
/// story) gets `None`, rendering exactly like a pre-M3 one-cluster item.
fn member_reasons(
    clusters: &[LinkCluster],
    edges: &[Edge],
    components: &BTreeMap<usize, Vec<usize>>,
) -> Vec<Option<String>> {
    // cluster index → its component root, to keep only edges realized within one story.
    let mut root = vec![0usize; clusters.len()];
    for (&r, members) in components {
        for &m in members {
            root[m] = r;
        }
    }

    // Per cluster, the best incident edge ranked by (strong, score).
    let mut best: Vec<Option<(bool, f64)>> = vec![None; clusters.len()];
    let mut reason: Vec<Option<String>> = vec![None; clusters.len()];
    for e in edges.iter().filter(|e| root[e.a] == root[e.b]) {
        let rank = (e.strong, e.score);
        for &i in &[e.a, e.b] {
            let beats = match best[i] {
                None => true,
                Some((s, sc)) => (rank.0 && !s) || (rank.0 == s && rank.1 > sc),
            };
            if beats {
                best[i] = Some(rank);
                reason[i] = Some(e.reason.clone());
            }
        }
    }
    reason
}

// ── Stage 4: stable-id forwarding ────────────────────────────────────────────

/// Map each component onto a story id from the prior assignment, keeping ids stable for the
/// subscriber: a component carrying prior story ids keeps the **oldest** (uuidv7 is time-ordered, so
/// `min` = oldest = "the id its clusters mostly carried, oldest wins on a retro-merge", §8.2); a
/// component with none mints a fresh id. A prior id is **claimed by at most one** component, so if a
/// prior story ever *splits* across components, only one keeps the id and the others fall through to
/// their next-oldest prior id (or a fresh mint) — no two stories can collide on one id. Every prior
/// id that is absorbed (present in a component but kept by no one) is a retro-merge loser →
/// `merged_into` its component's survivor. Components are walked in cluster-id order so claiming and
/// minting are reproducible.
fn forward_ids(
    clusters: &[LinkCluster],
    components: &BTreeMap<usize, Vec<usize>>,
    reasons: &[Option<String>],
    prior: &[PriorMember],
    mint: &mut impl FnMut() -> Uuid,
) -> Assignment {
    let prior_story: BTreeMap<Uuid, Uuid> =
        prior.iter().map(|p| (p.cluster_id, p.story_id)).collect();

    // Order components by their smallest cluster id so id claiming/minting is reproducible.
    let mut ordered: Vec<&Vec<usize>> = components.values().collect();
    ordered.sort_by_key(|members| members.iter().map(|&i| clusters[i].id).min());

    // The prior ids each component carries (oldest first), computed once.
    let component_prior_ids: Vec<Vec<Uuid>> = ordered
        .iter()
        .map(|members| {
            let mut ids: Vec<Uuid> = members
                .iter()
                .filter_map(|&i| prior_story.get(&clusters[i].id).copied())
                .collect();
            ids.sort_unstable();
            ids.dedup();
            ids
        })
        .collect();

    // Pass 1: claim a survivor id per component (oldest unclaimed prior id, else a fresh mint).
    let mut claimed: BTreeSet<Uuid> = BTreeSet::new();
    let survivors: Vec<Uuid> = component_prior_ids
        .iter()
        .map(|prior_ids| {
            let survivor = match prior_ids.iter().find(|id| !claimed.contains(id)) {
                Some(&id) => id,
                None => mint(),
            };
            claimed.insert(survivor);
            survivor
        })
        .collect();

    // Pass 2: any prior id present in a component but kept by *no* component was absorbed → a merge.
    let mut merges = Vec::new();
    for (prior_ids, &survivor) in component_prior_ids.iter().zip(&survivors) {
        for &id in prior_ids {
            if id != survivor && !claimed.contains(&id) {
                merges.push(Merge {
                    loser: id,
                    survivor,
                });
            }
        }
    }

    let mut stories = Vec::new();
    for (members, &id) in ordered.iter().zip(&survivors) {
        // Members in cluster-id order; representative recency span is the component's min/max.
        let mut clusters_out: Vec<ClusterRef> = members
            .iter()
            .map(|&i| ClusterRef {
                cluster_id: clusters[i].id,
                link_reason: reasons[i].clone(),
            })
            .collect();
        clusters_out.sort_by_key(|r| r.cluster_id);

        let first = members
            .iter()
            .map(|&i| clusters[i].first_event_time)
            .min()
            .unwrap();
        let last = members
            .iter()
            .map(|&i| clusters[i].last_event_time)
            .max()
            .unwrap();

        // Cross-source rollups, folded over the component's members (design §8.3): counts sum,
        // depth/severity take the max, source_diversity is the distinct member sources, and the
        // story is "own private" if any member is.
        let event_count = members.iter().map(|&i| clusters[i].event_count).sum();
        let source_diversity = members
            .iter()
            .map(|&i| clusters[i].source)
            .collect::<BTreeSet<_>>()
            .len() as i32;
        let content_depth = members
            .iter()
            .map(|&i| clusters[i].content_depth)
            .max()
            .unwrap();
        let max_severity = members
            .iter()
            .filter_map(|&i| clusters[i].max_severity)
            .max();
        let has_private = members.iter().any(|&i| clusters[i].is_own_private);

        stories.push(LinkedStory {
            id,
            clusters: clusters_out,
            first_event_time: first,
            last_event_time: last,
            event_count,
            source_diversity,
            content_depth,
            max_severity,
            has_private,
        });
    }

    // Stable output order: newest story first (matches selection), tie-break by id.
    stories.sort_by(|x, y| {
        y.last_event_time
            .cmp(&x.last_event_time)
            .then(x.id.cmp(&y.id))
    });
    merges.sort_by_key(|m| (m.loser, m.survivor));
    Assignment { stories, merges }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use proptest::prelude::*;
    use std::collections::BTreeMap as Map;

    fn t(day: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(day * 86_400, 0).single().unwrap()
    }

    fn cluster(id: u128, entities: &[&str], day: i64) -> LinkCluster {
        LinkCluster {
            id: Uuid::from_u128(id),
            entities: entities.iter().map(|s| s.to_string()).collect(),
            first_event_time: t(day),
            last_event_time: t(day),
            source: SourceKind::Rss,
            event_count: 1,
            content_depth: ContentKind::Longform,
            max_severity: None,
            is_own_private: false,
        }
    }

    /// A deterministic id minter for tests — fresh components get `0xF...n` so they never collide
    /// with the small cluster ids above.
    fn minter() -> impl FnMut() -> Uuid {
        let mut n: u128 = 0xF000;
        move || {
            n += 1;
            Uuid::from_u128(n)
        }
    }

    /// Which story each cluster landed in: cluster_id → story_id.
    fn placement(a: &Assignment) -> Map<Uuid, Uuid> {
        a.stories
            .iter()
            .flat_map(|s| s.clusters.iter().map(move |c| (c.cluster_id, s.id)))
            .collect()
    }

    /// Turn an assignment into the prior-member input for the next recompute.
    fn prior_of(a: &Assignment, delivered: bool) -> Vec<PriorMember> {
        a.stories
            .iter()
            .flat_map(|s| {
                s.clusters.iter().map(move |c| PriorMember {
                    cluster_id: c.cluster_id,
                    story_id: s.id,
                    delivered,
                })
            })
            .collect()
    }

    #[test]
    fn shared_strong_key_fuses_across_sources() {
        // A GitHub PR and an RSS advisory naming the same CVE → one story.
        let clusters = vec![
            cluster(1, &["repo:acme/api", "cve:CVE-2026-1234"], 1),
            cluster(2, &["url:https://nvd/x", "cve:CVE-2026-1234"], 1),
        ];
        let a = link(&clusters, &[], minter());
        assert_eq!(a.stories.len(), 1);
        let story = &a.stories[0];
        assert_eq!(story.clusters.len(), 2);
        // Each member carries the strong reason naming the shared CVE.
        for m in &story.clusters {
            assert_eq!(m.link_reason.as_deref(), Some("shared cve:CVE-2026-1234"));
        }
    }

    #[test]
    fn disjoint_clusters_are_singleton_stories() {
        let clusters = vec![
            cluster(1, &["repo:a/one"], 1),
            cluster(2, &["repo:b/two"], 1),
        ];
        let a = link(&clusters, &[], minter());
        assert_eq!(a.stories.len(), 2);
        // A singleton has no connection to explain.
        for s in &a.stories {
            assert_eq!(s.clusters.len(), 1);
            assert_eq!(s.clusters[0].link_reason, None);
        }
    }

    #[test]
    fn shared_grounded_place_fuses_but_shared_topic_alone_does_not() {
        // Phase 2: three outlets covering the same happening, each carrying only a per-publisher
        // domain plus the SAME grounded place — they fuse into one story on the weak place edge (same
        // day → corroborated). This is the motivating "warning shots in the English Channel" case. The
        // candidate set carries unrelated filler so the place is *distinctive* (low df → high IDF →
        // weight above the saturated floor): three of nine clusters share it, the realistic shape of one
        // incident in a digest, not a degenerate 3-cluster universe.
        let mut same_place = vec![
            cluster(1, &["domain:bbc.com", "place:english-channel"], 1),
            cluster(2, &["domain:reuters.com", "place:english-channel"], 1),
            cluster(3, &["domain:guardian.com", "place:english-channel"], 1),
        ];
        // Six unrelated filler clusters (each a distinct repo, sharing nothing) — they form no edges,
        // they just make `place:english-channel` rare across the corpus.
        for i in 0..6 {
            same_place.push(cluster(10 + i, &[&format!("repo:filler/r{i}")], 1));
        }
        let stories = link(&same_place, &[], minter()).stories;
        // The three channel items fuse into one story; the six filler items stay singletons (7 total).
        assert_eq!(
            stories.len(),
            7,
            "a distinctive shared place is a weak link key — corroborated coverage must fuse"
        );
        assert!(
            stories.iter().any(|s| s.clusters.len() == 3),
            "the three English-Channel items must be the one 3-member story"
        );

        // But two items sharing ONLY a broad `topic:` must NOT fuse — topic is non-linking
        // (link_strength == None), or "everything about AI" collapses into one blob. They never even
        // become a candidate pair (the blocking index skips non-linkable entities).
        let same_topic = vec![
            cluster(1, &["domain:bbc.com", "topic:artificial-intelligence"], 1),
            cluster(
                2,
                &["domain:reuters.com", "topic:artificial-intelligence"],
                1,
            ),
        ];
        assert_eq!(
            link(&same_topic, &[], minter()).stories.len(),
            2,
            "a shared topic must never fuse stories on its own"
        );
    }

    #[test]
    fn pervasive_place_does_not_fuse_sparse_items() {
        // The saturated-Jaccard gap: items whose ONLY linkable entity is a broad place share it at
        // wj=1.0 (the weight cancels in the ratio), so IDF can't demote it through the ratio alone. The
        // absolute saturated-weight floor catches it: a place that recurs across the candidate set is
        // IDF-demoted below the floor, so five same-day items sharing only `place:germany` stay five
        // separate stories instead of collapsing into one blob.
        let pervasive: Vec<LinkCluster> = (0..5)
            .map(|i| {
                cluster(
                    i as u128 + 1,
                    &[&format!("domain:p{i}.com"), "place:germany"],
                    1,
                )
            })
            .collect();
        assert_eq!(
            link(&pervasive, &[], minter()).stories.len(),
            5,
            "a broad place that is the items' only linkable token must not fuse them"
        );
    }

    #[test]
    fn weak_edge_links_when_corroborated_but_not_when_stale() {
        // Shared weak entity + same day → linked. Same shared entity but far apart → separate.
        let close = vec![
            cluster(1, &["repo:acme/api", "user:alice"], 1),
            cluster(2, &["repo:acme/api", "user:bob"], 1),
        ];
        assert_eq!(link(&close, &[], minter()).stories.len(), 1);

        let stale = vec![
            cluster(1, &["repo:acme/api", "user:alice"], 1),
            cluster(2, &["repo:acme/api", "user:bob"], 60), // far outside the temporal window
        ];
        assert_eq!(link(&stale, &[], minter()).stories.len(), 2);
    }

    #[test]
    fn one_broad_shared_entity_in_a_rich_set_does_not_fuse() {
        // Two same-day items that share a single broad entity (`place:germany`) but are otherwise about
        // different things must NOT fuse: temporal proximity corroborates, it can't carry the edge. With
        // ~5 distinctive entities each and only one shared, the Jaccard sits under `WEAK_MIN_JACCARD`.
        let broad_only = vec![
            cluster(
                1,
                &[
                    "place:germany",
                    "org:bundestag",
                    "org:spd",
                    "person:alice",
                    "topic:pensions",
                    "domain:tagesschau.de",
                ],
                1,
            ),
            cluster(
                2,
                &[
                    "place:germany",
                    "org:dwd",
                    "place:cottbus",
                    "person:bob",
                    "topic:weather",
                    "domain:tagesschau.de",
                ],
                1,
            ),
        ];
        assert_eq!(
            link(&broad_only, &[], minter()).stories.len(),
            2,
            "a lone broad shared entity (same day) must not fuse two otherwise-distinct items"
        );

        // But a genuine same-story pair — several shared distinctive entities — still fuses on the day.
        let same_story = vec![
            cluster(
                1,
                &[
                    "place:strait-of-hormuz",
                    "org:iran",
                    "org:usa",
                    "domain:a.com",
                ],
                1,
            ),
            cluster(
                2,
                &[
                    "place:strait-of-hormuz",
                    "org:iran",
                    "org:usa",
                    "domain:b.com",
                ],
                1,
            ),
        ];
        assert_eq!(
            link(&same_story, &[], minter()).stories.len(),
            1,
            "several shared distinctive entities is a real connection — it must fuse"
        );
    }

    #[test]
    fn idf_demotes_a_corpus_wide_entity() {
        // The graded weight of a token shared by *every* cluster (idf floored at 1) is strictly below
        // that of a rare one (idf > 1), at equal namespace prior — quality emerges from the corpus, no
        // denylist. `org:` at df=5/5 vs df=1/5.
        let n = 5;
        let pervasive = entity_weight("org:acme", 5, n, &EntityWeights::new());
        let rare = entity_weight("org:acme", 1, n, &EntityWeights::new());
        assert!(
            rare > pervasive,
            "rare {rare} should outweigh pervasive {pervasive}"
        );
        // ...and the feedback override scales it (the affinity seam): care-less halves, care-more doubles.
        let mut w = EntityWeights::new();
        w.insert("org:acme".to_string(), 0.5);
        let muted = entity_weight("org:acme", 1, n, &w);
        assert!(muted < rare, "a care-less override must reduce the weight");
    }

    #[test]
    fn pervasive_owner_entity_does_not_bridge_distinct_clusters() {
        // The reported GitHub blob: one account's repos all carry a shared `org:<owner>` (here mined
        // onto every cluster). With it in *every* cluster its IDF collapses, so it can't by itself fuse
        // two clusters that otherwise share nothing — each repo's own activity stays its own story.
        let owner = "org:acme-corp";
        let clusters = vec![
            cluster(1, &[owner, "repo:acme-corp/api", "user:alice"], 1),
            cluster(2, &[owner, "repo:acme-corp/web", "user:bob"], 1),
            cluster(3, &[owner, "repo:acme-corp/cli", "user:carol"], 1),
            cluster(4, &[owner, "repo:acme-corp/docs", "user:dave"], 1),
        ];
        assert_eq!(
            link(&clusters, &[], minter()).stories.len(),
            4,
            "a corpus-wide owner entity must not bridge otherwise-distinct repos"
        );
    }

    #[test]
    fn feedback_constraints_force_and_block_links() {
        // cannot-link overrides even a strong shared key: two clusters sharing a CVE would always fuse,
        // but a cannot-link correction keeps them apart.
        let shared_cve = vec![
            cluster(1, &["cve:CVE-2026-1", "repo:a/one"], 1),
            cluster(2, &["cve:CVE-2026-1", "repo:b/two"], 1),
        ];
        let mut tuning = LinkTuning::default();
        tuning
            .constraints
            .cannot(shared_cve[0].id, shared_cve[1].id);
        assert_eq!(
            link_with(&shared_cve, &[], &tuning, minter()).stories.len(),
            2,
            "a cannot-link correction must keep two clusters apart despite a shared CVE"
        );

        // must-link fuses two clusters that share nothing (no candidate pair would otherwise form).
        let disjoint = vec![
            cluster(1, &["repo:a/one"], 1),
            cluster(2, &["repo:b/two"], 1),
        ];
        let mut tuning = LinkTuning::default();
        tuning.constraints.must(disjoint[0].id, disjoint[1].id);
        let a = link_with(&disjoint, &[], &tuning, minter());
        assert_eq!(
            a.stories.len(),
            1,
            "a must-link correction must fuse two otherwise-unconnected clusters"
        );
        assert_eq!(a.stories[0].clusters.len(), 2);
    }

    #[test]
    fn retro_merge_keeps_oldest_id_and_tombstones_loser() {
        // Two already-delivered single-cluster stories; a new strong edge now connects them.
        let older = Uuid::from_u128(0x10);
        let newer = Uuid::from_u128(0x20);
        let clusters = vec![
            cluster(1, &["cve:CVE-2026-1"], 1),
            cluster(2, &["cve:CVE-2026-1"], 2),
        ];
        let prior = vec![
            PriorMember {
                cluster_id: clusters[0].id,
                story_id: older,
                delivered: true,
            },
            PriorMember {
                cluster_id: clusters[1].id,
                story_id: newer,
                delivered: true,
            },
        ];
        let a = link(&clusters, &prior, minter());
        // One surviving story, carrying the OLDER id; the newer story is forwarded to it.
        assert_eq!(a.stories.len(), 1);
        assert_eq!(a.stories[0].id, older);
        assert_eq!(
            a.merges,
            vec![Merge {
                loser: newer,
                survivor: older
            }]
        );
    }

    #[test]
    fn asymmetric_guard_blocks_weak_merge_of_delivered_stories() {
        // Two already-delivered stories sharing only a *weak* entity (same day): must NOT merge.
        let s1 = Uuid::from_u128(0x10);
        let s2 = Uuid::from_u128(0x20);
        let clusters = vec![
            cluster(1, &["repo:acme/api", "user:alice"], 1),
            cluster(2, &["repo:acme/api", "user:bob"], 1),
        ];
        let prior = vec![
            PriorMember {
                cluster_id: clusters[0].id,
                story_id: s1,
                delivered: true,
            },
            PriorMember {
                cluster_id: clusters[1].id,
                story_id: s2,
                delivered: true,
            },
        ];
        let a = link(&clusters, &prior, minter());
        assert_eq!(
            a.stories.len(),
            2,
            "weak edge must not collapse two delivered stories"
        );
        assert!(a.merges.is_empty());
    }

    #[test]
    fn weak_linked_delivered_story_survives_recompute() {
        // A delivered story formed by a *weak* edge (shared repo, same day). On recompute both
        // clusters are delivered+established — the guard must NOT split them: they are the same
        // story, not two different ones.
        let s = Uuid::from_u128(0x10);
        let clusters = vec![
            cluster(1, &["repo:acme/api", "user:alice"], 1),
            cluster(2, &["repo:acme/api", "user:bob"], 1),
        ];
        let prior = vec![
            PriorMember {
                cluster_id: clusters[0].id,
                story_id: s,
                delivered: true,
            },
            PriorMember {
                cluster_id: clusters[1].id,
                story_id: s,
                delivered: true,
            },
        ];
        let a = link(&clusters, &prior, minter());
        assert_eq!(
            a.stories.len(),
            1,
            "a weak-linked delivered story must not split"
        );
        assert_eq!(a.stories[0].id, s);
        assert_eq!(a.stories[0].clusters.len(), 2);
    }

    #[test]
    fn weak_edge_attaches_fresh_cluster_to_delivered_story() {
        // A delivered story plus a brand-new cluster sharing a weak key: the fresh one attaches.
        let delivered = Uuid::from_u128(0x10);
        let clusters = vec![
            cluster(1, &["repo:acme/api", "user:alice"], 1),
            cluster(2, &["repo:acme/api", "user:carol"], 1), // fresh (no prior)
        ];
        let prior = vec![PriorMember {
            cluster_id: clusters[0].id,
            story_id: delivered,
            delivered: true,
        }];
        let a = link(&clusters, &prior, minter());
        assert_eq!(a.stories.len(), 1);
        assert_eq!(a.stories[0].id, delivered); // keeps the established id
        assert_eq!(a.stories[0].clusters.len(), 2);
    }

    #[test]
    fn link_reason_never_cites_a_cluster_in_another_story() {
        // A,B form delivered story S1 (weak repo link). C is delivered story S2. A's *strongest*
        // incident edge is the guard-skipped A–C (shared user:carol, higher score than A–B) — but
        // that edge crosses stories, so A's reason must come from its intra-story A–B edge.
        let s1 = Uuid::from_u128(0x10);
        let s2 = Uuid::from_u128(0x20);
        let clusters = vec![
            cluster(1, &["repo:acme/api", "user:carol"], 1), // A
            cluster(2, &["repo:acme/api", "user:bob"], 1),   // B
            cluster(3, &["user:carol"], 1),                  // C
        ];
        let prior = vec![
            PriorMember {
                cluster_id: clusters[0].id,
                story_id: s1,
                delivered: true,
            },
            PriorMember {
                cluster_id: clusters[1].id,
                story_id: s1,
                delivered: true,
            },
            PriorMember {
                cluster_id: clusters[2].id,
                story_id: s2,
                delivered: true,
            },
        ];
        let a = link(&clusters, &prior, minter());
        let s1_story = a.stories.iter().find(|s| s.id == s1).unwrap();
        assert_eq!(s1_story.clusters.len(), 2);
        for m in &s1_story.clusters {
            // The reason must be the intra-story repo link, never the cross-story user:carol.
            assert_eq!(m.link_reason.as_deref(), Some("shared repo:acme/api"));
        }
    }

    // ── Strategy + invariants (the load-bearing reliability guarantees, §6) ──

    fn arb_clusters() -> impl Strategy<Value = Vec<LinkCluster>> {
        // A small entity pool (mix of strong + weak) so collisions — and thus edges — actually occur.
        let pool = [
            "cve:CVE-2026-1",
            "url:https://a/x",
            "repo:acme/api",
            "user:alice",
            "domain:example.com",
        ];
        prop::collection::vec(
            (
                1u128..40,
                prop::collection::vec(0usize..pool.len(), 0..4),
                0i64..30,
            ),
            0..12,
        )
        .prop_map(move |specs| {
            // De-dup by cluster id (the store guarantees unique ids).
            let mut seen = std::collections::BTreeSet::new();
            specs
                .into_iter()
                .filter(|(id, _, _)| seen.insert(*id))
                .map(|(id, ents, day)| {
                    let mut entities: Vec<String> =
                        ents.into_iter().map(|i| pool[i].to_string()).collect();
                    entities.sort();
                    entities.dedup();
                    LinkCluster {
                        id: Uuid::from_u128(id),
                        entities,
                        first_event_time: t(day),
                        last_event_time: t(day),
                        source: SourceKind::Rss,
                        event_count: 1,
                        content_depth: ContentKind::Longform,
                        max_severity: None,
                        is_own_private: false,
                    }
                })
                .collect()
        })
    }

    proptest! {
        // Determinism: same inputs (and the same minting sequence) → identical assignment.
        #[test]
        fn linking_is_deterministic(clusters in arb_clusters()) {
            let a = link(&clusters, &[], minter());
            let b = link(&clusters, &[], minter());
            prop_assert_eq!(a, b);
        }

        // Id-stability: re-running over the same clusters, fed the prior assignment, preserves every
        // story id (no spurious churn) — the "stable deep-link / feedback target" guarantee (§5.3).
        #[test]
        fn ids_are_stable_across_recompute(clusters in arb_clusters()) {
            let first = link(&clusters, &[], minter());
            // Not delivered, so the asymmetric guard doesn't change the partition between runs.
            let prior = prior_of(&first, false);
            let second = link(&clusters, &prior, minter());

            prop_assert_eq!(placement(&first), placement(&second));
            prop_assert!(second.merges.is_empty(), "stable recompute must not retro-merge");
        }

        // Every candidate cluster lands in exactly one story (a partition — none lost, none doubled).
        #[test]
        fn clusters_partition_into_stories(clusters in arb_clusters()) {
            let a = link(&clusters, &[], minter());
            let mut members: Vec<Uuid> = a
                .stories
                .iter()
                .flat_map(|s| s.clusters.iter().map(|c| c.cluster_id))
                .collect();
            members.sort();
            let mut expected: Vec<Uuid> = clusters.iter().map(|c| c.id).collect();
            expected.sort();
            prop_assert_eq!(members, expected);
        }
    }
}
