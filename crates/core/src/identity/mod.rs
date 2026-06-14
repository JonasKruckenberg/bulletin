//! Tiered, probabilistic entity-identity resolution for the Thread layer (design
//! `docs/thread-layer.md` §3).
//!
//! M3 gives every event **namespaced exact tokens** (`repo:`/`user:`/`url:`/`cve:`/`domain:`,
//! `common::entity`) — exact *by construction*, the certain backbone. This module adds the **graded,
//! revisable** layer the design centres on: equivalence edges that fold two tokens into one identity,
//! from
//! - **lexical** similarity (`user:dlewis` ~ `user:dana-lewis`) — graded, confidence = the measure;
//! - **feedback** `must_link` — a user-confirmed equivalence (confidence 1.0);
//! - (later) **embedding** similarity — one more edge source, same shape.
//!
//! A canonical identity is a connected component over edges **≥ θ**, with a [`ConfidenceBand`] (the
//! bottleneck of the strongest path that holds it together — so a redundant weak edge can't downgrade
//! an otherwise-certain merge) and **stable id-forwarding** so deep-links/feedback targets survive a
//! recompute. `cannot_link` is the dual: a **veto** the resolver never merges across, materialized in
//! the same `entity_edge` graph (negative confidence) so identity is reconstructible from the graph
//! alone. The band flows to rendering ("possibly part of …") — confidence is a product surface (§4).
//!
//! Everything here is pure and deterministic (no I/O, no clock), so it is exhaustively proptested;
//! the DB seam is in [`store`].

pub mod store;

use std::collections::{BTreeMap, BTreeSet, HashMap};

/// A resolved entity identity — a namespaced token (`kind:value`). `String` for v1 (jsonb storage).
pub type CanonicalId = String;

/// The render/scoring contract: confidence collapsed to a *band*, never a raw float (the frontend
/// maps band → treatment; the raw score stays server-side). One vocabulary spans identity *and* a
/// story→thread assignment, so "possibly Dana" and "possibly part of the Acme migration" share a
/// visual grammar (design §4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConfidenceBand {
    Confirmed,
    Probable,
    Uncertain,
}

impl ConfidenceBand {
    /// Band cutoffs (a tuning surface, design §10): ≥ 0.99 authoritative/normalized ⇒ `Confirmed`;
    /// ≥ 0.75 ⇒ `Probable`; else `Uncertain`.
    pub const CONFIRMED_FLOOR: f32 = 0.99;
    pub const PROBABLE_FLOOR: f32 = 0.75;

    pub fn from_score(score: f32) -> Self {
        if score >= Self::CONFIRMED_FLOOR {
            ConfidenceBand::Confirmed
        } else if score >= Self::PROBABLE_FLOOR {
            ConfidenceBand::Probable
        } else {
            ConfidenceBand::Uncertain
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ConfidenceBand::Confirmed => "confirmed",
            ConfidenceBand::Probable => "probable",
            ConfidenceBand::Uncertain => "uncertain",
        }
    }

    /// Parse the stored text form (defaults to `Confirmed` on an unknown value — a missing band
    /// should read as "certain", never as spurious doubt).
    pub fn parse(s: &str) -> Self {
        match s {
            "probable" => ConfidenceBand::Probable,
            "uncertain" => ConfidenceBand::Uncertain,
            _ => ConfidenceBand::Confirmed,
        }
    }
}

/// Where an equivalence edge came from, with its nominal confidence. Exactness is the high floor; the
/// graded sources sit above it. `Embedding` is reserved for the deferred "same meaning, no shared
/// token" source — it slots in as one more edge source with a confidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EdgeSource {
    ExactId,
    Normalized,
    Lexical,
    Embedding,
    Feedback,
}

impl EdgeSource {
    /// The confidence an edge from this source carries by default. Lexical edges carry their own
    /// measured similarity instead (see [`Edge::lexical`]); this is the nominal value.
    pub fn default_confidence(self) -> f32 {
        match self {
            EdgeSource::ExactId | EdgeSource::Feedback => 1.0,
            EdgeSource::Normalized => 0.99,
            EdgeSource::Embedding => 0.85,
            EdgeSource::Lexical => 0.5,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            EdgeSource::ExactId => "exact_id",
            EdgeSource::Normalized => "normalized",
            EdgeSource::Lexical => "lexical",
            EdgeSource::Embedding => "embedding",
            EdgeSource::Feedback => "feedback",
        }
    }

    /// Parse the stored text form, co-located with [`as_str`](Self::as_str) so the pair can't drift
    /// (a round-trip proptest pins them).
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "exact_id" => EdgeSource::ExactId,
            "normalized" => EdgeSource::Normalized,
            "lexical" => EdgeSource::Lexical,
            "embedding" => EdgeSource::Embedding,
            "feedback" => EdgeSource::Feedback,
            _ => return None,
        })
    }
}

/// A probabilistic equivalence edge between two entity tokens. Undirected; `resolve` treats `(a,b)`
/// and `(b,a)` identically. Confidence is the edge's own (lexical edges carry a measured score).
#[derive(Debug, Clone, PartialEq)]
pub struct Edge {
    pub a: CanonicalId,
    pub b: CanonicalId,
    pub confidence: f32,
    pub source: EdgeSource,
}

impl Edge {
    /// A lexical edge carrying its *measured* similarity as the confidence (clamped to the lexical
    /// band so a fluke high score can't masquerade as authoritative).
    pub fn lexical(a: impl Into<CanonicalId>, b: impl Into<CanonicalId>, similarity: f32) -> Self {
        Edge {
            a: a.into(),
            b: b.into(),
            confidence: similarity.clamp(0.0, 0.95),
            source: EdgeSource::Lexical,
        }
    }

    /// An edge from a non-lexical source at its default confidence.
    pub fn from_source(
        a: impl Into<CanonicalId>,
        b: impl Into<CanonicalId>,
        source: EdgeSource,
    ) -> Self {
        Edge {
            a: a.into(),
            b: b.into(),
            confidence: source.default_confidence(),
            source,
        }
    }
}

/// Normalize a raw entity string to its stored token form — used on the feedback path so a
/// user-supplied token lands on the same node the resolver sees. Lower-cases the `kind:` prefix and
/// trims; the value is left as the source emitted it (M3 already normalizes per-kind). Idempotent.
pub fn canonicalize(raw: &str) -> CanonicalId {
    let trimmed = raw.trim();
    match trimmed.split_once(':') {
        Some((kind, value))
            if !kind.is_empty() && kind.chars().all(|c| c.is_ascii_alphanumeric()) =>
        {
            format!("{}:{}", kind.to_ascii_lowercase(), value)
        }
        _ => trimmed.to_ascii_lowercase(),
    }
}

/// Split a namespaced token into `(kind, value)`, or `None` if unprefixed. Lexical similarity is only
/// proposed *within* a namespace (a `user:` is never the same identity as a `repo:`), and on the
/// value — the `kind:` prefix is shared boilerplate that would inflate every score.
pub fn namespace(token: &str) -> Option<(&str, &str)> {
    token.split_once(':')
}

/// A canonical undirected pair key, so `(a,b)` and `(b,a)` collapse (veto set / edge dedup).
pub fn pair(a: &str, b: &str) -> (CanonicalId, CanonicalId) {
    if a <= b {
        (a.to_owned(), b.to_owned())
    } else {
        (b.to_owned(), a.to_owned())
    }
}

/// A prior resolution's component representatives, for **id-forwarding**: an unchanged (or growing)
/// component keeps the id it carried before. A set suffices — the rep is otherwise the
/// lexicographically-smallest member.
pub type PriorReps = BTreeSet<CanonicalId>;

/// The connected-component structure of the identity graph, with stable representatives and a
/// confidence band per component.
#[derive(Debug, Clone, PartialEq)]
pub struct Resolution {
    /// member token → its component's representative id.
    pub rep: BTreeMap<CanonicalId, CanonicalId>,
    /// representative id → the component's confidence band.
    pub band: BTreeMap<CanonicalId, ConfidenceBand>,
}

impl Resolution {
    /// The representative every member of `id`'s component forwards to (`id` itself if unseen).
    pub fn representative<'a>(&'a self, id: &'a CanonicalId) -> &'a CanonicalId {
        self.rep.get(id).unwrap_or(id)
    }

    /// The confidence band of `id`'s identity (`Confirmed` for an unseen singleton).
    pub fn band_of(&self, id: &CanonicalId) -> ConfidenceBand {
        self.band
            .get(self.representative(id))
            .copied()
            .unwrap_or(ConfidenceBand::Confirmed)
    }
}

/// Resolve canonical identities = connected components over `edges` with confidence **≥ θ**,
/// **skipping any pair in `vetoes`** (a `cannot_link`), with **stable id-forwarding** from `prior`.
/// Pure and deterministic.
///
/// The per-component band is the **bottleneck of the maximum spanning forest** — the weakest edge on
/// the strongest set of links that actually holds the component together. So an authoritative 1.0
/// merge stays `Confirmed` even if a redundant 0.6 edge also spans the same pair (that edge is never
/// chosen into the spanning forest), while a component reachable only through a 0.8 lexical hop
/// renders `Probable`. Singletons are `Confirmed` (a token is certainly itself).
///
/// A veto prevents a *direct* merge; a transitive path (a–c–b) is the hard correlation-clustering
/// case left for later (design flags identity tuning as open).
pub fn resolve(
    nodes: &[CanonicalId],
    edges: &[Edge],
    vetoes: &BTreeSet<(CanonicalId, CanonicalId)>,
    theta: f32,
    prior: &PriorReps,
) -> Resolution {
    let mut uf = UnionFind::default();
    for n in nodes {
        uf.touch(n);
    }
    // Eligible edges, strongest first (Kruskal max-spanning-forest), with a deterministic tiebreak.
    let mut sorted: Vec<&Edge> = edges
        .iter()
        .filter(|e| e.confidence >= theta && !vetoes.contains(&pair(&e.a, &e.b)))
        .collect();
    sorted.sort_by(|x, y| {
        y.confidence
            .total_cmp(&x.confidence)
            .then_with(|| x.a.cmp(&y.a))
            .then_with(|| x.b.cmp(&y.b))
    });
    // Union the strongest first; an edge that *creates* a merge is a spanning-forest edge — record it
    // so we can take each component's bottleneck (min spanning-forest-edge confidence).
    let mut forest: Vec<(usize, f32)> = Vec::new(); // (one endpoint's index, confidence)
    for e in &sorted {
        let ia = uf.touch(&e.a);
        uf.touch(&e.b);
        if uf.union(&e.a, &e.b) {
            forest.push((ia, e.confidence));
        }
    }

    // Group members by root; pick the stable representative.
    let mut by_root: BTreeMap<usize, Vec<CanonicalId>> = BTreeMap::new();
    for (id, &idx) in &uf.index {
        by_root
            .entry(uf.find_const(idx))
            .or_default()
            .push(id.clone());
    }
    // Bottleneck (weakest spanning-forest edge) per final root.
    let mut bottleneck: BTreeMap<usize, f32> = BTreeMap::new();
    for (idx, conf) in &forest {
        let root = uf.find_const(*idx);
        bottleneck
            .entry(root)
            .and_modify(|w| *w = w.min(*conf))
            .or_insert(*conf);
    }

    let mut rep = BTreeMap::new();
    let mut band = BTreeMap::new();
    for (root, mut members) in by_root {
        members.sort();
        let representative = members
            .iter()
            .find(|m| prior.contains(*m))
            .cloned()
            .unwrap_or_else(|| members[0].clone());
        let component_band = bottleneck
            .get(&root)
            .map_or(ConfidenceBand::Confirmed, |&w| {
                ConfidenceBand::from_score(w)
            });
        for m in members {
            rep.insert(m, representative.clone());
        }
        band.insert(representative, component_band);
    }
    Resolution { rep, band }
}

/// A cheap, symmetric lexical similarity in `[0,1]`: token-set Jaccard for multi-word values, a
/// character-bigram Dice coefficient for single tokens. Callers pass the **value** part of
/// same-namespace tokens (see [`namespace`]). Deterministic.
pub fn lexical_similarity(a: &str, b: &str) -> f32 {
    if a == b {
        return 1.0;
    }
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let ta: BTreeSet<&str> = a
        .split([' ', '-', '_', '/'])
        .filter(|s| !s.is_empty())
        .collect();
    let tb: BTreeSet<&str> = b
        .split([' ', '-', '_', '/'])
        .filter(|s| !s.is_empty())
        .collect();
    if ta.len() > 1 || tb.len() > 1 {
        let inter = ta.intersection(&tb).count();
        let union = ta.union(&tb).count();
        return if union == 0 {
            0.0
        } else {
            inter as f32 / union as f32
        };
    }
    dice_bigram(a, b)
}

/// Sørensen–Dice over character bigrams — a forgiving single-token similarity.
fn dice_bigram(a: &str, b: &str) -> f32 {
    let bigrams = |s: &str| -> Vec<[char; 2]> {
        let chars: Vec<char> = s.chars().collect();
        chars.windows(2).map(|w| [w[0], w[1]]).collect()
    };
    let (ba, bb) = (bigrams(a), bigrams(b));
    if ba.is_empty() || bb.is_empty() {
        return 0.0;
    }
    let mut counts: HashMap<[char; 2], i32> = HashMap::new();
    for g in &ba {
        *counts.entry(*g).or_default() += 1;
    }
    let mut shared = 0usize;
    for g in &bb {
        let c = counts.entry(*g).or_default();
        if *c > 0 {
            *c -= 1;
            shared += 1;
        }
    }
    2.0 * shared as f32 / (ba.len() + bb.len()) as f32
}

/// A tiny string-keyed union-find with path-halving. Internal to [`resolve`]; deterministic because
/// callers feed it nodes/edges in sorted order.
#[derive(Default)]
struct UnionFind {
    index: HashMap<CanonicalId, usize>,
    parent: Vec<usize>,
    rank: Vec<u32>,
}

impl UnionFind {
    fn touch(&mut self, id: &str) -> usize {
        if let Some(&i) = self.index.get(id) {
            return i;
        }
        let i = self.parent.len();
        self.parent.push(i);
        self.rank.push(0);
        self.index.insert(id.to_owned(), i);
        i
    }

    fn find(&mut self, mut i: usize) -> usize {
        while self.parent[i] != i {
            self.parent[i] = self.parent[self.parent[i]]; // path halving
            i = self.parent[i];
        }
        i
    }

    /// Root without mutation — used once the structure is frozen (grouping pass).
    fn find_const(&self, mut i: usize) -> usize {
        while self.parent[i] != i {
            i = self.parent[i];
        }
        i
    }

    /// Union; returns `true` iff it merged two distinct sets (so the caller can record a
    /// spanning-forest edge).
    fn union(&mut self, a: &str, b: &str) -> bool {
        let (ia, ib) = (self.touch(a), self.touch(b));
        let (ra, rb) = (self.find(ia), self.find(ib));
        if ra == rb {
            return false;
        }
        let (lo, hi) = match self.rank[ra].cmp(&self.rank[rb]) {
            std::cmp::Ordering::Less => (ra, rb),
            std::cmp::Ordering::Greater => (rb, ra),
            std::cmp::Ordering::Equal if ra < rb => (rb, ra),
            std::cmp::Ordering::Equal => (ra, rb),
        };
        self.parent[lo] = hi;
        if self.rank[lo] == self.rank[hi] {
            self.rank[hi] += 1;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn ids(xs: &[&str]) -> Vec<CanonicalId> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    fn no_vetoes() -> BTreeSet<(CanonicalId, CanonicalId)> {
        BTreeSet::new()
    }

    #[test]
    fn canonicalize_lowercases_kind_keeps_value() {
        assert_eq!(canonicalize("Repo:Acme/Widgets"), "repo:Acme/Widgets");
        assert_eq!(canonicalize("  USER:dlewis "), "user:dlewis");
    }

    #[test]
    fn edge_source_round_trips() {
        for src in [
            EdgeSource::ExactId,
            EdgeSource::Normalized,
            EdgeSource::Lexical,
            EdgeSource::Embedding,
            EdgeSource::Feedback,
        ] {
            assert_eq!(EdgeSource::parse(src.as_str()), Some(src));
        }
        assert_eq!(EdgeSource::parse("nope"), None);
    }

    #[test]
    fn singletons_are_confirmed_and_self_representing() {
        let r = resolve(&ids(&["user:a"]), &[], &no_vetoes(), 0.8, &PriorReps::new());
        assert_eq!(r.representative(&"user:a".to_string()), "user:a");
        assert_eq!(r.band_of(&"user:a".to_string()), ConfidenceBand::Confirmed);
    }

    #[test]
    fn lexical_edge_merges_as_probable() {
        let edges = vec![Edge::lexical("user:dlewis", "user:dana", 0.82)];
        let r = resolve(
            &ids(&["user:dlewis", "user:dana"]),
            &edges,
            &no_vetoes(),
            0.8,
            &PriorReps::new(),
        );
        let rep = r.representative(&"user:dlewis".to_string()).clone();
        assert_eq!(r.representative(&"user:dana".to_string()), &rep);
        assert_eq!(r.band[&rep], ConfidenceBand::Probable);
    }

    #[test]
    fn weak_edge_below_theta_does_not_merge() {
        let edges = vec![Edge::lexical("user:a", "user:b", 0.6)];
        let r = resolve(
            &ids(&["user:a", "user:b"]),
            &edges,
            &no_vetoes(),
            0.8,
            &PriorReps::new(),
        );
        assert_ne!(
            r.representative(&"user:a".to_string()),
            r.representative(&"user:b".to_string())
        );
    }

    #[test]
    fn redundant_weak_edge_does_not_downgrade_a_confirmed_merge() {
        // An authoritative feedback merge (1.0) plus a redundant lexical 0.8 edge over the same pair.
        let edges = vec![
            Edge::from_source("user:a", "user:b", EdgeSource::Feedback),
            Edge::lexical("user:a", "user:b", 0.8),
        ];
        let r = resolve(
            &ids(&["user:a", "user:b"]),
            &edges,
            &no_vetoes(),
            0.75,
            &PriorReps::new(),
        );
        let rep = r.representative(&"user:a".to_string()).clone();
        // The spanning forest uses the 1.0 edge; the redundant 0.8 is never the bottleneck.
        assert_eq!(r.band[&rep], ConfidenceBand::Confirmed);
    }

    #[test]
    fn veto_blocks_a_direct_merge() {
        let edges = vec![Edge::from_source("user:a", "user:b", EdgeSource::Feedback)];
        let mut vetoes = BTreeSet::new();
        vetoes.insert(pair("user:a", "user:b"));
        let r = resolve(
            &ids(&["user:a", "user:b"]),
            &edges,
            &vetoes,
            0.8,
            &PriorReps::new(),
        );
        assert_ne!(
            r.representative(&"user:a".to_string()),
            r.representative(&"user:b".to_string())
        );
    }

    #[test]
    fn id_forwarding_keeps_prior_rep() {
        let edges = vec![Edge::from_source("b", "c", EdgeSource::Feedback)];
        let mut prior = PriorReps::new();
        prior.insert("c".to_string());
        let r = resolve(&ids(&["b", "c"]), &edges, &no_vetoes(), 0.8, &prior);
        assert_eq!(r.representative(&"b".to_string()), "c");
    }

    #[test]
    fn lexical_similarity_is_graded() {
        assert!(lexical_similarity("dlewis", "dlewis") > 0.99);
        assert!(lexical_similarity("dana-lewis", "dana lewis") > 0.5); // shared token "dana"... + "lewis"
        assert!(lexical_similarity("alice", "zzzzz") < 0.3);
    }

    proptest! {
        // resolve is order-independent: shuffling nodes/edges yields the same rep + band.
        #[test]
        fn resolve_is_order_independent(
            pairs in prop::collection::vec((0u8..6, 0u8..6, 75u32..100), 0..20),
        ) {
            let nodes: Vec<CanonicalId> = (0u8..6).map(|n| format!("n{n}")).collect();
            let edges: Vec<Edge> = pairs
                .iter()
                .filter(|(a, b, _)| a != b)
                .map(|(a, b, c)| Edge::lexical(format!("n{a}"), format!("n{b}"), *c as f32 / 100.0))
                .collect();
            let prior = PriorReps::new();
            let fwd = resolve(&nodes, &edges, &no_vetoes(), 0.8, &prior);
            let mut rn = nodes.clone();
            rn.reverse();
            let mut re = edges.clone();
            re.reverse();
            let rev = resolve(&rn, &re, &no_vetoes(), 0.8, &prior);
            prop_assert_eq!(fwd.rep, rev.rep);
            prop_assert_eq!(fwd.band, rev.band);
        }

        #[test]
        fn lexical_similarity_symmetric_bounded(a in "[a-c]{0,6}", b in "[a-c]{0,6}") {
            let ab = lexical_similarity(&a, &b);
            prop_assert!((ab - lexical_similarity(&b, &a)).abs() < 1e-6);
            prop_assert!((0.0..=1.0).contains(&ab));
        }
    }
}
