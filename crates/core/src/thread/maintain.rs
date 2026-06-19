//! `thread_maintenance(subscriber)` — the one new background job (design §5.1). Per-subscriber, off
//! the punctual path: it resolves identity (folding the user's `must_link`s, honouring `cannot_link`
//! vetoes), builds the engaged co-occurrence graph over the subscriber's stories, detects
//! communities, id-forwards them onto the prior thread set, decays affinity, runs the state machine,
//! and projects the per-entity weight map the fire-time relevance term reads. Best-effort and
//! due-gated; it mirrors `public-build`'s "fall behind, never wrong" contract and never blocks a fire.
//!
//! Everything genuinely expensive lives here, bounded by the subscriber's (small) entity graph — a
//! life, not the firehose. The pure algorithms it composes are in the parent module; this file is the
//! DB-bound orchestration.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::db::{with_scope, ScopeCtx};
use crate::feedback;
use crate::identity::{self, CanonicalId, ConfidenceBand, Edge, PriorReps, Resolution};
use crate::thread::store::{self, StorySource, ThreadRow, ThreadUpsert};
use crate::thread::{
    co_occurrence, communities_to_candidates, decay_affinity, label_propagation,
    map_communities_to_threads, project_weights, CoOccurrenceItem, Horizons, ThreadMapping,
    ThreadOrigin, ThreadState,
};

/// Tuning surface for a maintenance pass (design §10 open questions) — held as a struct so a config
/// table can supply per-subscriber values later. Defaults are deliberately conservative.
#[derive(Debug, Clone, Copy)]
pub struct MaintenanceConfig {
    /// Rolling co-occurrence window — how far back the graph reaches.
    pub window: Duration,
    /// Identity-merge threshold θ: only edges ≥ this collapse two tokens into one identity. Sits at
    /// or below `lexical_threshold` so graded (lexical/embedding) edges actually merge.
    pub theta: f32,
    /// Minimum lexical similarity to *propose* an equivalence edge between two same-namespace tokens.
    pub lexical_threshold: f32,
    /// Smallest community that becomes a thread (a lone entity is not a thread of a life).
    pub min_community_size: usize,
    /// Entity-overlap Jaccard for id-forwarding a community onto an existing thread.
    pub match_threshold: f32,
    /// Label-propagation iteration cap.
    pub max_iters: usize,
    /// Affinity gained per *new* engaged story on a thread this pass.
    pub engagement_weight: f32,
    /// Affinity ceiling.
    pub affinity_max: f32,
    pub horizons: Horizons,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        MaintenanceConfig {
            window: Duration::days(90),
            theta: 0.8,
            lexical_threshold: 0.8,
            min_community_size: 2,
            match_threshold: 0.3,
            max_iters: 30,
            engagement_weight: 0.5,
            affinity_max: 20.0,
            horizons: Horizons::default(),
        }
    }
}

/// What a maintenance pass did, for logs / metrics.
#[derive(Debug, Default)]
pub struct MaintenanceStats {
    pub sources: usize,
    pub entities: usize,
    pub communities: usize,
    pub threads_written: usize,
    pub weighted_entities: usize,
}

/// Run one maintenance pass for `subscriber_id` as of `now`. Idempotent over a stable snapshot:
/// re-running with the same inputs converges to the same thread set (id-forwarding keeps ids stable)
/// and the same weight map. A failure leaves the prior thread state untouched (best-effort).
pub async fn maintain(
    pool: &PgPool,
    subscriber_id: Uuid,
    now: DateTime<Utc>,
    cfg: &MaintenanceConfig,
) -> Result<MaintenanceStats> {
    let cfg = *cfg;
    let window_start = now - cfg.window;
    // The whole pass is one Subscriber-scoped transaction: every read/write is RLS-fenced to this
    // subscriber (own threads/edges/feedback/watermark; public ∪ own clusters/stories) and the writes
    // commit atomically. Best-effort — a failure rolls back, leaving the prior thread state intact.
    with_scope(pool, ScopeCtx::Subscriber(subscriber_id), move |conn| {
        Box::pin(async move {
            let since = store::feedback_cursor(&mut *conn, subscriber_id)
                .await
                .context("read maintenance watermark")?;

            // ── inputs ────────────────────────────────────────────────────────────
            let sources = store::co_occurrence_sources(&mut *conn, subscriber_id, window_start)
                .await
                .context("load co-occurrence sources")?;
            let edges = identity::store::load_edges(&mut *conn, subscriber_id)
                .await
                .context("load identity edges")?;
            let veto_pairs = identity::store::load_vetoes(&mut *conn, subscriber_id)
                .await
                .context("load identity vetoes")?;
            let care = feedback::thread_care_since(&mut *conn, subscriber_id, since)
                .await
                .context("load thread care feedback")?;
            let prior = store::load_threads(&mut *conn, subscriber_id)
                .await
                .context("load prior threads")?;

            // ── identity resolution (graded lexical + feedback must_link, honour cannot_link) ────────────
            let mut nodes: BTreeSet<CanonicalId> = BTreeSet::new();
            for s in &sources {
                nodes.extend(s.entities.iter().cloned());
            }
            let node_vec: Vec<CanonicalId> = nodes.into_iter().collect();
            let vetoes: BTreeSet<(CanonicalId, CanonicalId)> = veto_pairs
                .iter()
                .map(|(a, b)| identity::pair(a, b))
                .collect();
            // Durable feedback edges + freshly-proposed lexical edges (same namespace, value similarity ≥
            // threshold, not vetoed). Lexical edges are derived each pass from the current entity set, so they
            // aren't persisted; feedback edges are the durable graph.
            let mut edges = edges;
            edges.extend(lexical_edges(&node_vec, &vetoes, cfg.lexical_threshold));
            let resolution =
                identity::resolve(&node_vec, &edges, &vetoes, cfg.theta, &PriorReps::new());

            // ── co-occurrence over resolved (component-rep) entities ────────────────
            let items: Vec<CoOccurrenceItem> = sources
                .iter()
                .map(|s| CoOccurrenceItem {
                    entities: s
                        .entities
                        .iter()
                        .map(|e| resolution.representative(e).clone())
                        .collect(),
                    at: s.last_event_time,
                })
                .collect();
            let graph = co_occurrence(&items, now, cfg.horizons.affinity_half_life);

            // ── community detection → candidates → id-forwarding onto prior threads ──
            let labels = label_propagation(&graph, cfg.max_iters);
            let candidates = communities_to_candidates(&labels, cfg.min_community_size);
            let existing: Vec<_> = prior.iter().map(ThreadRow::as_existing).collect();
            let mappings = map_communities_to_threads(&candidates, &existing, cfg.match_threshold);

            // Per-thread care delta (incremental — only feedback since the last pass).
            let mut care_by_thread: BTreeMap<Uuid, f32> = BTreeMap::new();
            for c in &care {
                *care_by_thread.entry(c.thread_id).or_insert(0.0) += c.delta;
            }
            let prior_by_id: BTreeMap<Uuid, &ThreadRow> = prior.iter().map(|t| (t.id, t)).collect();

            let mut upserts: Vec<ThreadUpsert> = Vec::new();
            let mut claimed: BTreeSet<Uuid> = BTreeSet::new();

            for m in &mappings {
                let (id, absorb, entities) = match m {
                    ThreadMapping::New { entities } => (None, Vec::new(), entities),
                    ThreadMapping::Keep { id, entities } => (Some(*id), Vec::new(), entities),
                    ThreadMapping::Merge {
                        winner,
                        merged,
                        entities,
                    } => (Some(*winner), merged.clone(), entities),
                };
                if let Some(id) = id {
                    claimed.insert(id);
                }
                claimed.extend(absorb.iter().copied());

                let prior_row = id.and_then(|id| prior_by_id.get(&id).copied());
                // Care folded in for the winner *and every thread it absorbs* — so a nudge on a thread that
                // merges away this pass isn't lost (it advances onto the survivor).
                let care_delta = id
                    .map_or(0.0, |id| care_by_thread.get(&id).copied().unwrap_or(0.0))
                    + absorb
                        .iter()
                        .filter_map(|l| care_by_thread.get(l))
                        .sum::<f32>();
                upserts.push(build_upsert(
                    id,
                    absorb,
                    entities,
                    prior_row,
                    &sources,
                    &resolution,
                    care_delta,
                    since,
                    now,
                    &cfg,
                ));
            }

            // Carry-forward: a prior thread no mapping claimed still gets a full re-score through the same
            // `build_upsert` — so if engaged stories still overlap its spine it stays active (its engagement
            // and last_story_time are recomputed), and only a genuinely quiet thread decays toward archived.
            for t in &prior {
                if claimed.contains(&t.id) {
                    continue;
                }
                let care_delta = care_by_thread.get(&t.id).copied().unwrap_or(0.0);
                upserts.push(build_upsert(
                    Some(t.id),
                    Vec::new(),
                    &t.entities,
                    Some(t),
                    &sources,
                    &resolution,
                    care_delta,
                    since,
                    now,
                    &cfg,
                ));
            }

            // ── persist + project weights ───────────────────────────────────────────
            store::save_threads(&mut *conn, subscriber_id, &upserts)
                .await
                .context("save threads")?;
            let projection: Vec<(ThreadState, f32, Vec<CanonicalId>)> = upserts
                .iter()
                .map(|u| (u.state, u.affinity, u.entities.clone()))
                .collect();
            let weights = project_weights(&projection);
            store::save_entity_weights(&mut *conn, subscriber_id, &weights)
                .await
                .context("save entity weights")?;
            store::advance_watermark(&mut *conn, subscriber_id, now)
                .await
                .context("advance maintenance watermark")?;

            Ok(MaintenanceStats {
                sources: sources.len(),
                entities: node_vec.len(),
                communities: candidates.len(),
                threads_written: upserts.len(),
                weighted_entities: weights.len(),
            })
        })
    })
    .await
}

/// Same-namespace lexical equivalence edges: for every pair of tokens sharing a `kind:` namespace
/// whose *values* are similar enough, propose a graded edge (skipping vetoed pairs). O(n²) within a
/// namespace, bounded by the subscriber's (small) entity set. These are derived each pass, not
/// persisted — the durable graph is the feedback edges.
fn lexical_edges(
    nodes: &[CanonicalId],
    vetoes: &BTreeSet<(CanonicalId, CanonicalId)>,
    threshold: f32,
) -> Vec<Edge> {
    let mut out = Vec::new();
    for i in 0..nodes.len() {
        let Some((ka, va)) = identity::namespace(&nodes[i]) else {
            continue;
        };
        for b in &nodes[i + 1..] {
            let Some((kb, vb)) = identity::namespace(b) else {
                continue;
            };
            if ka != kb || vetoes.contains(&identity::pair(&nodes[i], b)) {
                continue;
            }
            let sim = identity::lexical_similarity(va, vb);
            if sim >= threshold {
                out.push(Edge::lexical(nodes[i].clone(), b.clone(), sim));
            }
        }
    }
    out
}

/// Compute the persisted state of one thread: re-score affinity (decay prior + new engagement +
/// care), recompute window metrics (story_count / source_diversity / baseline_rate) from the stories
/// overlapping its spine, the **identity confidence** (the weakest band among its spine entities — a
/// thread held together by an uncertain alias merge renders uncertain), run the state machine, and
/// preserve `first_seen` / advance `last_story_time`. Used for both matched/new and carried-forward
/// threads, so the lifecycle is computed in exactly one place.
#[allow(clippy::too_many_arguments)]
fn build_upsert(
    id: Option<Uuid>,
    absorb: Vec<Uuid>,
    entities: &[CanonicalId],
    prior: Option<&ThreadRow>,
    sources: &[StorySource],
    resolution: &Resolution,
    care_delta: f32,
    since: DateTime<Utc>,
    now: DateTime<Utc>,
    cfg: &MaintenanceConfig,
) -> ThreadUpsert {
    let spine: BTreeSet<&CanonicalId> = entities.iter().collect();
    let mut story_count = 0i32;
    let mut new_stories = 0i32;
    let mut sources_seen: HashSet<&str> = HashSet::new();
    let mut last_story_time = prior.and_then(|p| p.last_story_time);
    let mut first_seen = prior.and_then(|p| p.first_seen);
    for s in sources {
        if !s.entities.iter().any(|e| spine.contains(e)) {
            continue;
        }
        story_count += 1;
        sources_seen.extend(s.sources.iter().map(String::as_str));
        if s.last_event_time > since {
            new_stories += 1;
        }
        last_story_time =
            Some(last_story_time.map_or(s.last_event_time, |t| t.max(s.last_event_time)));
        first_seen = Some(first_seen.map_or(s.last_event_time, |t| t.min(s.last_event_time)));
    }

    let elapsed = (now - since).max(Duration::zero());
    let delta = care_delta + cfg.engagement_weight * new_stories as f32;
    let affinity = decay_affinity(
        prior.map_or(0.0, |p| p.affinity),
        elapsed,
        cfg.horizons.affinity_half_life,
        delta,
        cfg.affinity_max,
    );
    let pinned = prior.is_some_and(|p| p.pinned);
    let origin = prior.map_or(ThreadOrigin::Emergent, |p| p.origin);
    let state = ThreadState::transition(last_story_time, now, pinned, &cfg.horizons);
    let window_days = (cfg.window.num_seconds() as f32 / 86_400.0).max(1.0);
    // Identity confidence: the weakest band among the spine's entities (the band reaches rendering as
    // "possibly part of …"). `Confirmed` for an empty spine or entities unseen this pass.
    let confidence = entities
        .iter()
        .map(|e| resolution.band_of(e))
        .max_by_key(|b| match b {
            ConfidenceBand::Confirmed => 0,
            ConfidenceBand::Probable => 1,
            ConfidenceBand::Uncertain => 2,
        })
        .unwrap_or(ConfidenceBand::Confirmed);

    ThreadUpsert {
        id,
        origin,
        pinned,
        // The deterministic auto-label (top entities), recomputed from the spine every pass — the
        // readable context eyebrow until the best-effort Phase-B sweep upgrades it onto `thread.summary`
        // (`llm-summarization.md` §2.3); the thread label stays best-effort (cosmetic, not a delivery gate).
        label: crate::summarize::auto_label(entities),
        entities: entities.to_vec(),
        affinity,
        state,
        confidence,
        story_count,
        source_diversity: sources_seen.len() as i32,
        baseline_rate: story_count as f32 / window_days,
        first_seen: first_seen.or(Some(now)),
        last_story_time,
        absorb,
    }
}
