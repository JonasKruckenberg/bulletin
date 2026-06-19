//! The digest flow (projection / read side): take a freshness-scored lookback over the cluster
//! cache, select by recency, freeze the selection, render, and deliver — advancing the subscriber's
//! schedule on delivery. A pure read of the materialization side's snapshot (design §3.0, §9.4).

pub mod eval;
mod greeting;
mod render;
pub mod select;
pub mod store;
pub mod subscriber;

pub use render::{DigestContent, Mailer};

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::db::{with_scope, ScopeCtx};
use crate::digest::select::{
    select, Candidate, Decision, DecisionRecord, ItemReason, ReplaySnapshot, ScoringConfig, Verdict,
};
use crate::digest::store::{
    build_render_item, cluster_cards, create_with_items, last_shown, load_config, mark_delivered,
    record_decisions, render_items, render_items_for_stories, story_owner, story_timeline,
    FrozenItem, RenderItem, TimelineEntry,
};
use crate::digest::subscriber::{load_subscriber, SubscriberRow};
use crate::link::{self, LinkedStory};

/// How far back a digest's candidate lookback reaches for *context*, beyond the guaranteed
/// reach-back to the last delivery (design §9.4). Generous on purpose: it must exceed the longest
/// cadence (weekly) plus any plausible outage so nothing ages out unconsidered. Config table later.
const CONTEXT_HORIZON_DAYS: i32 = 30;

#[derive(Debug)]
pub enum DigestOutcome {
    /// Delivered a digest with `items` entries (surfaced via `Debug` in logs / the debug CLI).
    Delivered {
        #[allow(dead_code)]
        items: usize,
    },
    /// Window had nothing to report; sent an "all caught up" note and advanced the watermark.
    Empty,
    /// Already delivered for this window (idempotent re-run).
    AlreadyDelivered,
    /// The boundary moved into the future between enqueue and run — a preference change deferred
    /// this send. Nothing delivered; the next tick fires it at the corrected boundary.
    NotYetDue,
    /// The digest had selected items but its **authored lead couldn't be composed** this run (the sidecar
    /// was down, or every re-seeded draw failed the gate). Per the §3.7 contract a digest with items never
    /// ships without an LLM lead, so nothing was delivered and the watermark did **not** advance: the
    /// worker errors so apalis retries this window. Whether a still-deferred lead warrants an operator
    /// alert is the worker's call (it owns the apalis attempt count and the retry budget).
    LeadDeferred,
}

/// Loads a subscriber or errors if it's gone — the shared first step of every flow below.
async fn load_required(pool: &PgPool, subscriber_id: Uuid) -> Result<SubscriberRow> {
    load_subscriber(pool, subscriber_id)
        .await
        .context("load subscriber")?
        .ok_or_else(|| anyhow!("subscriber {subscriber_id} not found"))
}

/// Link this subscriber's candidate clusters into stories, then rank them — the shared core of the
/// scheduled digest, the ad-hoc dispatch, and `explain`, so all three link and rank identically
/// (design §8.2). Returns the linked stories and a `Decision` per story.
///
/// The candidate set is scoped `public ∪ own-private` (never another subscriber's), and `link` runs
/// per subscriber because a story can fuse public clusters with this subscriber's own private ones.
/// `persist` writes the new assignment (the scheduled path, so stable ids carry forward and stories
/// can be frozen); the dry-run paths (`dispatch`/`explain`) pass `false` and keep the result
/// in-memory. The caller decides the lookback floor: `last_run`/`CONTEXT_HORIZON_DAYS` on schedule,
/// `None`/an explicit lookback off-schedule.
async fn link_and_select(
    pool: &PgPool,
    sub: &SubscriberRow,
    last_run: Option<DateTime<Utc>>,
    horizon_days: i32,
    shown_before: DateTime<Utc>,
    persist: bool,
) -> Result<(
    Vec<LinkedStory>,
    Vec<Decision>,
    HashMap<Uuid, Vec<String>>,
    ReplaySnapshot,
)> {
    let sub_id = sub.id;
    // Read the candidate clusters, the prior story assignment, and the per-story "last shown"
    // snapshots in the subscriber's RLS context: the candidate set is `public ∪ own-private` (and the
    // prior assignment + snapshots are the subscriber's own), never another tenant's — the query says
    // so, and now the DB enforces it (design §12).
    let (clusters, prior, shown) = with_scope(pool, ScopeCtx::Subscriber(sub_id), move |conn| {
        Box::pin(async move {
            // Strict summary gate (§3.7): withhold any cluster that doesn't yet carry a gate-passed
            // model summary (`band` confirmed/probable) — it slips to a later digest rather than shipping
            // without one. A cluster still being (re)summarized or quarantined for operator review simply
            // doesn't appear here; it never blocks the digest, it just isn't in it yet.
            let clusters = link::store::candidate_clusters(
                &mut *conn,
                sub_id,
                last_run,
                horizon_days,
                true,
            )
            .await
            .context("collect candidate clusters")?;
            let prior = link::store::load_prior_members(&mut *conn, sub_id)
                .await
                .context("load prior story assignment")?;
            let shown = last_shown(&mut *conn, sub_id, shown_before, CONTEXT_HORIZON_DAYS)
                .await
                .context("load last-shown snapshots")?;
            Ok((clusters, prior, shown))
        })
    })
    .await?;

    let assignment = link::link(&clusters, &prior, Uuid::now_v7);
    if persist {
        // Writes the subscriber's own stories → its RLS context (self-scoped inside the store fn).
        link::store::persist_assignment(pool, sub_id, &assignment)
            .await
            .context("persist story assignment")?;
    }

    // Each story's entity spine = the union of its member clusters' entities (already in memory from
    // the candidate load — no extra query). This feeds both the Thread relevance term and fire-time
    // thread-assignment.
    let cluster_entities: HashMap<Uuid, &[String]> = clusters
        .iter()
        .map(|c| (c.id, c.entities.as_slice()))
        .collect();
    let story_entities: HashMap<Uuid, Vec<String>> = assignment
        .stories
        .iter()
        .map(|s| (s.id, story_spine(s, &cluster_entities)))
        .collect();

    // The §3.7 story gate: a **multi-member** story may only be a candidate once its cross-source
    // synthesis is gate-passed (`band` confirmed/probable) — otherwise it's withheld and slips to a later
    // window rather than collapsing to one member's single-source blurb. Single-member stories have
    // nothing to fuse and render their one (already-gated) cluster summary, so they're always eligible.
    // Only multi-member stories consult the synthesis gate, so skip the DB round-trip entirely when there
    // are none (the common case for a low-volume subscriber).
    let multi_member_ids: Vec<Uuid> = assignment
        .stories
        .iter()
        .filter(|s| s.clusters.len() > 1)
        .map(|s| s.id)
        .collect();
    let faithful_stories: HashSet<Uuid> = if multi_member_ids.is_empty() {
        HashSet::new()
    } else {
        with_scope(pool, ScopeCtx::Subscriber(sub_id), move |conn| {
            Box::pin(async move {
                link::store::faithful_story_ids(&mut *conn, &multi_member_ids)
                    .await
                    .context("load faithful story summaries")
            })
        })
        .await?
        .into_iter()
        .collect()
    };
    let mut candidates: Vec<Candidate> = assignment
        .stories
        .iter()
        .filter(|s| s.clusters.len() == 1 || faithful_stories.contains(&s.id))
        .map(|s| Candidate::from_story(s, story_entities[&s.id].clone(), shown.get(&s.id).copied()))
        .collect();
    // Add the Thread relevance term before ranking (compiled out when the feature is off; a no-op
    // until thread_maintenance has projected weights) — it folds into the M4 relevance score.
    apply_weighting(pool, sub.id, &mut candidates).await?;
    // M4 scoring + selection (design §8.4): relevance gates, richness classifies Story/Note, priority
    // (relevance + severity, recency-decayed) orders + per-format caps, bounded by the subscriber's
    // overall `max_items`. `now` is read-time so the decay reflects when the digest fires; config is
    // the global `digest_config` row.
    let cfg = load_config(pool).await.context("load scoring config")?;
    // `.max(0)` guards the `i32 → usize` cast: a stray non-positive max_items yields an empty digest
    // (the safe direction), never a sign-wrapped, effectively-unbounded ceiling.
    let max_items = sub.max_items.max(0) as usize;
    // Capture the read-time clock once, and snapshot the candidate set *before* `select` consumes it,
    // so a delivered digest can be re-scored under a trial config later (the eval sweep, §0.1).
    let now = Utc::now();
    let snapshot = ReplaySnapshot {
        now,
        max_items,
        candidates: candidates.clone(),
    };
    let decisions = select(candidates, &cfg, max_items, now);
    Ok((assignment.stories, decisions, story_entities, snapshot))
}

/// The deduplicated, sorted union of a story's member-cluster entities.
fn story_spine(story: &LinkedStory, cluster_entities: &HashMap<Uuid, &[String]>) -> Vec<String> {
    let mut spine: Vec<String> = story
        .clusters
        .iter()
        .filter_map(|c| cluster_entities.get(&c.cluster_id))
        .flat_map(|ents| ents.iter().cloned())
        .collect();
    spine.sort();
    spine.dedup();
    spine
}

/// Add the Thread relevance term to the candidates (design §5.2). Compiled in only with the
/// `thread-weighting` feature — the compile-time kill switch that takes the whole consumption path
/// off the build when disabled. Even when on it's a no-op until `thread_maintenance` has projected a
/// weight map (an empty map leaves selection at pure recency).
#[cfg(feature = "thread-weighting")]
async fn apply_weighting(
    pool: &PgPool,
    subscriber_id: Uuid,
    candidates: &mut [Candidate],
) -> Result<()> {
    // `subscriber.affinity` lives on the (RLS-fenced) subscriber row, so read it in the subscriber's
    // own context — the no-subscriber context is denied the control-plane tables outright.
    let weights = with_scope(pool, ScopeCtx::Subscriber(subscriber_id), move |conn| {
        Box::pin(async move {
            crate::thread::store::load_entity_weights(&mut *conn, subscriber_id)
                .await
                .context("load entity weights")
        })
    })
    .await?;
    crate::digest::select::apply_thread_weights(candidates, &weights);
    Ok(())
}

#[cfg(not(feature = "thread-weighting"))]
async fn apply_weighting(_: &PgPool, _: Uuid, _: &mut [Candidate]) -> Result<()> {
    Ok(())
}

/// Stamp each selected story with the thread it advances (design §5.2). **Best-effort**: a DB error
/// is logged and swallowed, never propagated — the punctual digest must send regardless (the
/// assignment is render metadata, not the deliverable). Compiled out without the feature.
#[cfg(feature = "thread-weighting")]
async fn assign_threads(
    pool: &PgPool,
    digest_id: Uuid,
    subscriber_id: Uuid,
    selected: &[Uuid],
    story_entities: &HashMap<Uuid, Vec<String>>,
) {
    /// Minimum shared entities for a story→thread assignment (a single strong shared token suffices).
    const MIN_OVERLAP: i64 = 1;
    // All of `thread` and `digest_item` is RLS-fenced, so run in the subscriber's own context. The
    // whole thing is best-effort: any error is logged and swallowed so the digest still sends.
    let selected = selected.to_vec();
    let story_entities = story_entities.clone();
    let result = with_scope(pool, ScopeCtx::Subscriber(subscriber_id), move |conn| {
        Box::pin(async move {
            let mut assignments = Vec::with_capacity(selected.len());
            for id in &selected {
                let entities = story_entities.get(id).cloned().unwrap_or_default();
                let thread_id = crate::thread::store::assign_thread(
                    &mut *conn,
                    subscriber_id,
                    &entities,
                    MIN_OVERLAP,
                )
                .await?;
                assignments.push((*id, thread_id));
            }
            crate::thread::store::assign_thread_ids(&mut *conn, digest_id, &assignments).await?;
            Ok(())
        })
    })
    .await;
    if let Err(e) = result {
        tracing::warn!(error = %e, "thread assignment failed (non-fatal); digest unaffected");
    }
}

#[cfg(not(feature = "thread-weighting"))]
async fn assign_threads(_: &PgPool, _: Uuid, _: Uuid, _: &[Uuid], _: &HashMap<Uuid, Vec<String>>) {}

/// One candidate's `ItemReason` — the M4 scoring outcome (relevance, format + the richness phrase that
/// chose it, priority) plus the entity spine it scored on (design §10.2).
fn reason_of(d: &Decision, story_entities: &HashMap<Uuid, Vec<String>>) -> ItemReason {
    ItemReason {
        relevance: d.relevance,
        entities: story_entities.get(&d.id).cloned().unwrap_or_default(),
        format: d.format,
        richness: d.richness.clone(),
        priority: d.priority,
    }
}

/// The digest's full decision log (design §10.2): a structured record per candidate — *including the
/// over-cap drops* — with its verdict and reasoning. Persisted on the `digest` row; the foundation a
/// later explain UI / feedback reads.
fn decision_log(
    decisions: &[Decision],
    story_entities: &HashMap<Uuid, Vec<String>>,
) -> Vec<DecisionRecord> {
    decisions
        .iter()
        .map(|d| DecisionRecord {
            story_id: d.id,
            verdict: d.verdict,
            reason: reason_of(d, story_entities),
        })
        .collect()
}

/// The per-item reasons for the *selected* stories, keyed by story id — the render-facing slice of
/// the decision log (the dry-run paths build it in-memory; the frozen path re-reads it from storage).
fn selected_reasons(
    decisions: &[Decision],
    story_entities: &HashMap<Uuid, Vec<String>>,
) -> HashMap<Uuid, ItemReason> {
    decisions
        .iter()
        .filter(|d| d.is_selected())
        .map(|d| (d.id, reason_of(d, story_entities)))
        .collect()
}

/// Persist the digest's decision log (best-effort; never blocks the send) as structured records on
/// the `digest` row. Recorded regardless of the thread-weighting feature.
async fn record_decision_log(
    pool: &PgPool,
    digest_id: Uuid,
    subscriber_id: Uuid,
    log: Vec<DecisionRecord>,
) {
    let result = with_scope(pool, ScopeCtx::Subscriber(subscriber_id), move |conn| {
        Box::pin(async move {
            record_decisions(&mut *conn, digest_id, &log)
                .await
                .map_err(Into::into)
        })
    })
    .await;
    if let Err(e) = result {
        tracing::warn!(error = %e, "recording decision log failed (non-fatal)");
    }
}

/// Persist the digest's replay snapshot (best-effort; never blocks the send) onto the `digest` row,
/// so it can be re-scored under a trial config (the eval sweep). Subscriber-scoped (own candidates).
async fn record_candidate_snapshot(
    pool: &PgPool,
    digest_id: Uuid,
    subscriber_id: Uuid,
    snapshot: ReplaySnapshot,
) {
    let result = with_scope(pool, ScopeCtx::Subscriber(subscriber_id), move |conn| {
        Box::pin(async move {
            store::record_candidates(&mut *conn, digest_id, &snapshot)
                .await
                .map_err(Into::into)
        })
    })
    .await;
    if let Err(e) = result {
        tracing::warn!(error = %e, "recording candidate snapshot failed (non-fatal)");
    }
}

/// How many times, within a single `generate` run, the authored lead is re-attempted with an escalated
/// seed past a deterministic gate rejection (§3.7) before the digest defers to a later job attempt. A
/// down sidecar is *not* re-tried in-process (re-seeding can't reach a dead box) — that returns `None`
/// immediately and the whole job retries later, where the box may be back.
const LEAD_SEED_RETRIES: i32 = 3;

/// Compose the digest's big-picture lead (`llm-summarization.md` §2.4/§3.1, Phase D) and persist it onto
/// the `digest` row. This is the *one* summarization model call on the punctual path, and — per the §3.7
/// contract — the digest never ships without it: returns `Some(lead)` when one was authored, or `None`
/// when it couldn't be (the caller then defers the whole digest, watermark unmoved, for a later retry).
/// `job_attempt` is the apalis attempt index, folded into the seed so successive job retries also draw
/// fresh leads, not the same rejected one.
async fn digest_lead(
    pool: &PgPool,
    digest_id: Uuid,
    subscriber_id: Uuid,
    items: &[RenderItem],
    job_attempt: u32,
) -> Option<String> {
    let lead = authored_lead(items, job_attempt).await?;
    // Persist the authored lead (for the debug trace) — best-effort, never blocks the send.
    let to_store = lead.clone();
    let result = with_scope(pool, ScopeCtx::Subscriber(subscriber_id), move |conn| {
        Box::pin(async move {
            store::store_lead(&mut *conn, digest_id, &to_store)
                .await
                .map_err(Into::into)
        })
    })
    .await;
    if let Err(e) = result {
        tracing::warn!(error = %e, "recording digest lead failed (non-fatal)");
    }
    Some(lead)
}

/// The deadline-bounded **authored big-picture lead** (Phase D, §3.1). `Some(lead)` on success; `None`
/// when it couldn't be composed within this run's budget — the caller defers the digest (§3.7). There is
/// no deterministic fallback: a digest with items either carries an authored lead or waits for one.
///
/// A gate rejection is deterministic, so it is retried *in process* up to [`LEAD_SEED_RETRIES`] times,
/// each with a hotter seed (the `job_attempt` offsets the seed base so a later job retry doesn't repeat
/// this run's draws). A down sidecar or a blown deadline is not re-tried here — re-seeding can't revive
/// a dead box — so it returns `None` at once and lets the whole job retry later.
async fn authored_lead(items: &[RenderItem], job_attempt: u32) -> Option<String> {
    use crate::summarize::{self, LeadOutcome, SummarizationConfig};

    let headlines: Vec<String> = items
        .iter()
        .map(|i| i.headline.trim().to_string())
        .filter(|h| !h.is_empty())
        .collect();
    if headlines.is_empty() {
        return None; // nothing to author from (the empty digest never reaches here anyway)
    }
    // The threads the selected items advance, deduped in first-seen (rank) order — the §3.6 "name 1–2
    // threads" inputs.
    let mut threads: Vec<String> = Vec::new();
    for label in items
        .iter()
        .filter_map(|i| i.thread.as_ref())
        .map(|t| t.label.trim())
        .filter(|l| !l.is_empty())
    {
        if !threads.iter().any(|t| t == label) {
            threads.push(label.to_string());
        }
    }

    let base = SummarizationConfig::from_env();
    let http = match summarize::build_summarize_http(&base, "digest-lead") {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!(error = %e, "failed to build lead http client; deferring digest");
            return None;
        }
    };
    // Offset the seed base so each in-process retry *and* each job retry draws a distinct lead. The
    // apalis attempt index is 1-based, so subtract 1: the very first attempt starts at offset 0, i.e.
    // `for_attempt(0)` (the deterministic base seed) is exercised on a healthy first try before any
    // escalation, matching the rest of the pipeline.
    let seed_base = job_attempt.saturating_sub(1).saturating_mul(LEAD_SEED_RETRIES as u32) as i32;
    for i in 0..LEAD_SEED_RETRIES {
        let cfg = base.for_attempt(seed_base + i);
        match tokio::time::timeout(
            cfg.lead_deadline,
            summarize::client::authored_lead(&cfg, &http, &headlines, &threads),
        )
        .await
        {
            Ok(LeadOutcome::Ready(lead)) => return Some(lead),
            // Deterministic voice/grounding miss — re-seed and try again within this run.
            Ok(LeadOutcome::Rejected) => continue,
            // A dead box won't be revived by re-seeding; defer the whole digest to a later job attempt.
            Ok(LeadOutcome::Unavailable) => return None,
            Err(_) => {
                tracing::debug!(
                    deadline_s = cfg.lead_deadline.as_secs(),
                    "digest lead exceeded its deadline; deferring digest"
                );
                return None;
            }
        }
    }
    tracing::debug!(
        retries = LEAD_SEED_RETRIES,
        "digest lead still rejected after re-seeding; deferring digest"
    );
    None
}

/// The story ids that made the cut, in render order.
fn selected_ids(decisions: &[Decision]) -> Vec<Uuid> {
    decisions
        .iter()
        .filter(|d| d.is_selected())
        .map(|d| d.id)
        .collect()
}

/// The selected stories as `FrozenItem`s — story id + the recency anchor + format to freeze on each
/// `digest_item` (the re-surface snapshot, design §9.4), in render order.
fn frozen_items(decisions: &[Decision]) -> Vec<FrozenItem> {
    decisions
        .iter()
        .filter(|d| d.is_selected())
        .map(|d| FrozenItem {
            story_id: d.id,
            last_event_time: d.last_event_time,
            // Snapshot the *natural* richness format (not a re-surface demotion), so the next fire's
            // graduation check compares like with like and a damped story doesn't oscillate.
            format: d.natural_format,
        })
        .collect()
}

/// GenerateDigest for one subscriber: select the window's candidate clusters, freeze them into a
/// digest, render, and deliver via `mailer` — advancing the subscriber's watermark on delivery.
/// Idempotent and resumable: the `(subscriber, window_end)` row is created with its items in one
/// transaction, and a re-run finds the frozen selection (and skips a second send once delivered).
pub async fn generate(
    pool: &PgPool,
    mailer: &impl Mailer,
    subscriber_id: Uuid,
    content: &DigestContent<'_>,
    job_attempt: u32,
) -> Result<DigestOutcome> {
    // The lookback reads the cluster cache as of ~now; on delivery this instant becomes the new
    // last_run_at (the next digest's consideration floor). Captured before the read so the floor
    // can't sit *after* it — a cluster updated mid-read is re-considered next fire, never dropped.
    let snapshot_at = Utc::now();
    let sub = load_required(pool, subscriber_id).await?;

    // A preference change (timezone/digest_time/freq) can push next_run_at into the future after this
    // job was enqueued for the old, due boundary. Don't deliver early: bail (before the candidate
    // scan) and let the next tick fire it. This is what makes update_preferences safe mid-flight.
    if sub.next_run_at > Utc::now() {
        return Ok(DigestOutcome::NotYetDue);
    }

    let window_end = sub.next_run_at; // the digest's identity (UNIQUE(subscriber_id, window_end))

    // Build this subscriber's private clusters just-in-time so the candidate set is
    // `public ∪ own-private` (design §9.1). PublicBuild stays public-only; private is per-owner.
    crate::cluster::build_private(pool, sub.id)
        .await
        .context("build private clusters")?;

    // Link the candidate clusters into stories (persisting the assignment so ids stay stable), then
    // rank the stories by recency. The story is the unit the digest freezes and renders (§8.2).
    // `window_end` is the re-surface cutoff: a story is "stale" only against digests *before* this
    // one, so an idempotent re-run of the same window doesn't shadow-suppress its own selection.
    let (_, decisions, story_entities, snapshot) = link_and_select(
        pool,
        &sub,
        sub.last_run_at,
        CONTEXT_HORIZON_DAYS,
        window_end,
        true,
    )
    .await?;
    log_selection(sub.id, &decisions);
    let selected = selected_ids(&decisions);

    let digest = create_with_items(pool, sub.id, window_end, &frozen_items(&decisions))
        .await
        .context("create digest")?;

    if digest.delivered_at.is_some() {
        return Ok(DigestOutcome::AlreadyDelivered);
    }

    // Persist the per-item decision log onto the frozen items (always; best-effort), then thread-assign
    // them (best-effort, compiled out without the feature) — both metadata for the debug trace +
    // thread-grouped render, neither ever fails the digest (the email is the deliverable).
    record_decision_log(
        pool,
        digest.id,
        sub.id,
        decision_log(&decisions, &story_entities),
    )
    .await;
    // Persist the frozen `select` input alongside the decision log, so this digest is replayable under
    // a trial config later (the eval sweep). Best-effort — never fails the send.
    record_candidate_snapshot(pool, digest.id, sub.id, snapshot).await;
    assign_threads(pool, digest.id, sub.id, &selected, &story_entities).await;

    let digest_id = digest.id;
    let items = with_scope(pool, ScopeCtx::Subscriber(sub.id), move |conn| {
        Box::pin(async move {
            render_items(&mut *conn, digest_id)
                .await
                .context("load render items")
        })
    })
    .await?;
    if items.is_empty() {
        // Empty windows are rare — going silent reads as a broken pipeline. Send a cheerful
        // "you're all caught up" note instead, opened with the same time-of-day salutation as a
        // full digest, then advance the schedule so the subscriber isn't perpetually due.
        let message = render::render_empty(
            mailer.from(),
            &sub.email,
            window_end,
            &sub.timezone,
            &greeting::salutation(sub.digest_time, sub.name.as_deref()),
            content,
        )?;
        mailer.send(message).await?;
        mark_delivered(pool, digest.id, sub.id, snapshot_at)
            .await
            .context("mark delivered")?;
        return Ok(DigestOutcome::Empty);
    }

    // A warm lead keyed to the subscriber's local time-of-day and cadence; seeded from the digest's
    // identity so a re-render of this same window yields the same line.
    let greeting = greeting::greeting(
        sub.digest_time,
        sub.recurrence,
        greeting::seed_for(sub.id, window_end),
        sub.name.as_deref(),
    );
    // The big-picture lead (Phase D, `llm-summarization.md` §2.4/§3.1) — the one model call on the
    // punctual path, and the one summary the digest can't ship without (§3.7). `None` means it couldn't
    // be authored within this run's budget (sidecar down, or every re-seeded draw failed the gate): we
    // do **not** send and do **not** advance the watermark — the job errors so apalis retries this same
    // window later (the digest row stays frozen and idempotent). A subscriber waits for a good lead
    // rather than receiving a partial digest.
    let Some(lead) = digest_lead(pool, digest.id, sub.id, &items, job_attempt).await else {
        tracing::warn!(
            subscriber_id = %sub.id,
            %window_end,
            job_attempt,
            "digest lead unavailable; deferring delivery (no digest ships without an LLM lead)"
        );
        // Nothing delivered, watermark unmoved — the worker errors so apalis retries this window, and
        // decides (from the apalis attempt count + its own budget) when a still-deferred lead warrants
        // an operator alert. Keeping that threshold at the trigger layer keeps core free of retry policy.
        return Ok(DigestOutcome::LeadDeferred);
    };
    let message = render::render(
        mailer.from(),
        &sub.email,
        window_end,
        &sub.timezone,
        &items,
        &greeting,
        Some(lead.as_str()),
        content,
    )?;
    mailer.send(message).await?;
    mark_delivered(pool, digest.id, sub.id, snapshot_at)
        .await
        .context("mark delivered")?;

    Ok(DigestOutcome::Delivered { items: items.len() })
}

/// Ad-hoc dispatch: render and send a one-off digest for `subscriber_id` over the **last
/// `lookback_days`**, *without* touching the subscriber's schedule or freezing a scheduled digest.
/// It bypasses the due check and the `(subscriber, window_end)` freeze — purely a manual
/// preview/send (the `debug digest-dispatch` command), so it never disturbs the subscriber's real
/// cadence, `last_run_at`, or the de-dup history. Because it records nothing, a manual dispatch can
/// duplicate a concurrently-firing scheduled digest — acceptable for a debug tool. Returns `Empty`
/// if the lookback yields nothing.
pub async fn dispatch_now(
    pool: &PgPool,
    mailer: &impl Mailer,
    subscriber_id: Uuid,
    lookback_days: i32,
    content: &DigestContent<'_>,
) -> Result<DigestOutcome> {
    let sub = load_required(pool, subscriber_id).await?;

    // Build the subscriber's private clusters so a manual preview includes their own-private items.
    crate::cluster::build_private(pool, sub.id)
        .await
        .context("build private clusters")?;

    // Explicit lookback floor = now − lookback_days (last_run_at is ignored — this is off-schedule).
    // A preview links in-memory but persists nothing (`false`): it must not disturb the real story
    // cache, schedule, or de-dup history.
    let (stories, decisions, story_entities, _) =
        link_and_select(pool, &sub, None, lookback_days, Utc::now(), false).await?;
    log_selection(sub.id, &decisions);
    let selected = selected_ids(&decisions);

    // Reassemble the selected stories (in render order) from the in-memory assignment, rendering
    // their cluster cards in the subscriber's RLS context. The decision log is built in-memory (the
    // preview persists nothing) so the debug trace reads identically to a delivered digest.
    let by_id: HashMap<Uuid, &LinkedStory> = stories.iter().map(|s| (s.id, s)).collect();
    let selected_stories: Vec<LinkedStory> = selected
        .iter()
        .filter_map(|id| by_id.get(id).map(|s| (*s).clone()))
        .collect();
    let reasons = selected_reasons(&decisions, &story_entities);
    let items = with_scope(pool, ScopeCtx::Subscriber(sub.id), move |conn| {
        Box::pin(async move {
            render_items_for_stories(&mut *conn, &selected_stories, &reasons)
                .await
                .context("load render items")
        })
    })
    .await?;
    if items.is_empty() {
        return Ok(DigestOutcome::Empty);
    }
    // The rendered date header uses now() — this digest isn't tied to a scheduled boundary.
    let now = Utc::now();
    // The greeting still reflects the subscriber's *preferred* local time-of-day and cadence, so a
    // preview reads like the real thing regardless of when the dispatch is run.
    let greeting = greeting::greeting(
        sub.digest_time,
        sub.recurrence,
        greeting::seed_for(sub.id, now),
        sub.name.as_deref(),
    );
    // A manual preview renders the deterministic Phase-A lead (`None`): it persists nothing, so there is
    // no digest row to record an authored lead onto, and a debug dispatch shouldn't spend an on-path
    // model call. The scheduled `generate` is where the Phase-D authored lead lives.
    let message = render::render(
        mailer.from(),
        &sub.email,
        now,
        &sub.timezone,
        &items,
        &greeting,
        None,
        content,
    )?;
    mailer.send(message).await?;
    Ok(DigestOutcome::Delivered { items: items.len() })
}

/// Emits the selection audit trail: a one-line summary at INFO, then a per-candidate line at
/// DEBUG (`RUST_LOG=bulletin=debug`) so "why is this cluster in/out of the digest?" is answerable
/// from the worker logs. Mirrors `debug digest-explain` (which dry-runs it).
fn log_selection(subscriber_id: Uuid, decisions: &[Decision]) {
    let count = |f: fn(&Verdict) -> bool| decisions.iter().filter(|d| f(&d.verdict)).count();
    tracing::info!(
        %subscriber_id,
        candidates = decisions.len(),
        selected = count(|v| matches!(v, Verdict::Selected { .. })),
        over_cap = count(|v| matches!(v, Verdict::OverCap { .. })),
        dropped = count(|v| matches!(v, Verdict::Dropped { .. })),
        "selection complete"
    );
    for d in decisions {
        tracing::debug!(
            story_id = %d.id,
            last_event_time = %d.last_event_time,
            format = d.format.as_str(),
            relevance = d.relevance,
            priority = d.priority,
            verdict = ?d.verdict,
            "selection decision"
        );
    }
}

/// One candidate **story**'s selection verdict joined to its assembled render item — a row of
/// `digest-explain`. `item` is the same representative + connections the email would show (`None` if
/// the story resolves to no cluster), so the dry-run reflects exactly what would be rendered.
pub struct ExplainRow {
    pub verdict: Verdict,
    pub story_id: Uuid,
    pub last_event_time: DateTime<Utc>,
    /// The structured scoring rationale (design §10.2) — present even for a dropped/empty story.
    pub reason: ItemReason,
    pub item: Option<RenderItem>,
}

/// Dry-run of linking + selection for a subscriber: every candidate story paired with its verdict
/// and its assembled render item, with **no writes and no send** (it links in-memory but persists
/// nothing). Runs the exact same pure `link` + `select` the real digest does, over the subscriber's
/// scheduled lookback — so it explains both *why a story is in/out* and *why its clusters fused*.
pub async fn explain(pool: &PgPool, subscriber_id: Uuid) -> Result<Vec<ExplainRow>> {
    let sub = load_required(pool, subscriber_id).await?;
    crate::cluster::build_private(pool, sub.id)
        .await
        .context("build private clusters")?;

    let (stories, decisions, story_entities, _) = link_and_select(
        pool,
        &sub,
        sub.last_run_at,
        CONTEXT_HORIZON_DAYS,
        sub.next_run_at,
        false,
    )
    .await?;

    let by_id: HashMap<Uuid, &LinkedStory> = stories.iter().map(|s| (s.id, s)).collect();
    let ids: Vec<Uuid> = stories
        .iter()
        .flat_map(|s| s.clusters.iter().map(|c| c.cluster_id))
        .collect();
    let cards = with_scope(pool, ScopeCtx::Subscriber(sub.id), move |conn| {
        Box::pin(async move {
            cluster_cards(&mut *conn, &ids)
                .await
                .context("load cluster cards")
        })
    })
    .await?;

    // Attach the decision log to every candidate (including the over-cap drops), so the dry-run
    // explains the full reasoning — relevance term + entity spine — not just what was selected.
    Ok(decisions
        .into_iter()
        .map(|d| {
            let reason = reason_of(&d, &story_entities);
            // Explain is a dry-run over the in-memory linking: no persisted `story.summary` to read,
            // so render falls back to the representative cluster summary (Phase A), same as a digest
            // whose story synthesis hasn't run yet.
            let item = by_id.get(&d.id).and_then(|s| {
                build_render_item(&s.clusters, &cards, None).map(|mut item| {
                    item.reason = reason.clone();
                    item
                })
            });
            ExplainRow {
                verdict: d.verdict,
                story_id: d.id,
                last_event_time: d.last_event_time,
                reason,
                item,
            }
        })
        .collect())
}

/// "Show the data behind this story" (design §10.1): the event timeline of one story, oldest-first
/// (source + link + time per event). Resolves the story's owning subscriber via the admin
/// control-plane context, then walks `story.clusters → events` in *that subscriber's* RLS scope, so
/// the story's private events are visible to exactly their owner. Empty for an unknown story. The
/// `digest-provenance` debug command renders it.
pub async fn provenance(pool: &PgPool, story_id: Uuid) -> Result<Vec<TimelineEntry>> {
    let owner = with_scope(pool, ScopeCtx::Admin, move |conn| {
        Box::pin(async move {
            story_owner(&mut *conn, story_id)
                .await
                .context("resolve story owner")
        })
    })
    .await?;
    let Some(owner) = owner else {
        return Ok(Vec::new());
    };
    with_scope(pool, ScopeCtx::Subscriber(owner), move |conn| {
        Box::pin(async move {
            story_timeline(&mut *conn, story_id)
                .await
                .context("load story timeline")
        })
    })
    .await
}

/// Eval harness (design §10.3): score a subscriber's recent selection quality from the persisted
/// decision logs + their story feedback — read-only, no writes, no send. Reports structure/volume
/// (useful immediately, for tuning the scorer's config against real digests) and, once a feedback
/// surface populates the log, precision + nDCG. Per-subscriber by design: feedback and the decision
/// log's entity spines are the subscriber's own, so both reads run in *their* RLS context (an admin
/// cross-tenant read would touch private content). The `debug eval` command renders the result.
pub async fn eval_report(pool: &PgPool, subscriber_id: Uuid, limit: i64) -> Result<eval::Metrics> {
    load_required(pool, subscriber_id).await?;
    let (logs, feedback) = with_scope(pool, ScopeCtx::Subscriber(subscriber_id), move |conn| {
        Box::pin(async move {
            let logs = store::load_decision_logs(&mut *conn, subscriber_id, limit)
                .await
                .context("load decision logs")?;
            let feedback = store::story_feedback(&mut *conn, subscriber_id)
                .await
                .context("load story feedback")?;
            Ok((logs, feedback))
        })
    })
    .await?;

    Ok(eval::evaluate(&logs, &grades_from(feedback)))
}

/// Config sweep: re-score a subscriber's recent digests under both the **current** config and a
/// **trial** config, over the identical frozen candidate snapshots, and return `(baseline, trial)`
/// metrics — a true A/B that isolates the config change (vs `eval_report`, which scores the *frozen*
/// historical outcome). Only digests fired since the snapshot column landed are replayable; the rest
/// are silently skipped. Subscriber-scoped, like `eval_report`.
pub async fn eval_sweep(
    pool: &PgPool,
    subscriber_id: Uuid,
    limit: i64,
    trial: ScoringConfig,
) -> Result<(eval::Metrics, eval::Metrics)> {
    load_required(pool, subscriber_id).await?;
    let live = store::load_config(pool).await.context("load live config")?;
    let (snapshots, feedback) =
        with_scope(pool, ScopeCtx::Subscriber(subscriber_id), move |conn| {
            Box::pin(async move {
                let snapshots = store::load_candidate_snapshots(&mut *conn, subscriber_id, limit)
                    .await
                    .context("load candidate snapshots")?;
                let feedback = store::story_feedback(&mut *conn, subscriber_id)
                    .await
                    .context("load story feedback")?;
                Ok((snapshots, feedback))
            })
        })
        .await?;

    let grades = grades_from(feedback);
    let score = |cfg: &ScoringConfig| {
        let logs: Vec<Vec<DecisionRecord>> =
            snapshots.iter().map(|s| eval::replay(s, cfg)).collect();
        eval::evaluate(&logs, &grades)
    };
    Ok((score(&live), score(&trial)))
}

/// The latest grade per story from the story-feedback rows (newest-first, first occurrence wins).
fn grades_from(feedback: Vec<(Uuid, String)>) -> HashMap<Uuid, eval::Grade> {
    let mut grades: HashMap<Uuid, eval::Grade> = HashMap::new();
    for (story_id, signal) in feedback {
        if let Some(g) = eval::Grade::from_signal(&signal) {
            grades.entry(story_id).or_insert(g);
        }
    }
    grades
}

/// Read the singleton scoring config (`debug config`). The table is global, so no scope is needed.
pub async fn get_config(pool: &PgPool) -> Result<ScoringConfig> {
    store::load_config(pool)
        .await
        .context("load scoring config")
}

/// Overwrite the singleton scoring config (`debug config-set`). The CLI loads the current config,
/// applies the operator's provided overrides, and passes the merged value here.
pub async fn set_config(pool: &PgPool, cfg: ScoringConfig) -> Result<()> {
    store::update_config(pool, &cfg)
        .await
        .context("update scoring config")
}
