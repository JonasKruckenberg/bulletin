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

use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::db::{with_scope, ScopeCtx};
use crate::digest::select::{select, Candidate, Decision, DecisionRecord, ItemReason, Verdict};
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
) -> Result<(Vec<LinkedStory>, Vec<Decision>, HashMap<Uuid, Vec<String>>)> {
    let sub_id = sub.id;
    // Read the candidate clusters, the prior story assignment, and the per-story "last shown"
    // snapshots in the subscriber's RLS context: the candidate set is `public ∪ own-private` (and the
    // prior assignment + snapshots are the subscriber's own), never another tenant's — the query says
    // so, and now the DB enforces it (design §12).
    let (clusters, prior, shown) = with_scope(pool, ScopeCtx::Subscriber(sub_id), move |conn| {
        Box::pin(async move {
            let clusters =
                link::store::candidate_clusters(&mut *conn, sub_id, last_run, horizon_days)
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

    let mut candidates: Vec<Candidate> = assignment
        .stories
        .iter()
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
    let decisions = select(candidates, &cfg, max_items, Utc::now());
    Ok((assignment.stories, decisions, story_entities))
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
        .filter(|d| matches!(d.verdict, Verdict::Selected { .. }))
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

/// The story ids that made the cut, in render order.
fn selected_ids(decisions: &[Decision]) -> Vec<Uuid> {
    decisions
        .iter()
        .filter(|d| matches!(d.verdict, Verdict::Selected { .. }))
        .map(|d| d.id)
        .collect()
}

/// The selected stories as `FrozenItem`s — story id + the recency anchor + format to freeze on each
/// `digest_item` (the re-surface snapshot, design §9.4), in render order.
fn frozen_items(decisions: &[Decision]) -> Vec<FrozenItem> {
    decisions
        .iter()
        .filter(|d| matches!(d.verdict, Verdict::Selected { .. }))
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
    let (_, decisions, story_entities) = link_and_select(
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
    let message = render::render(
        mailer.from(),
        &sub.email,
        window_end,
        &sub.timezone,
        &items,
        &greeting,
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
    let (stories, decisions, story_entities) =
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
    let message = render::render(
        mailer.from(),
        &sub.email,
        now,
        &sub.timezone,
        &items,
        &greeting,
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

    let (stories, decisions, story_entities) = link_and_select(
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
            let item = by_id.get(&d.id).and_then(|s| {
                build_render_item(&s.clusters, &cards).map(|mut item| {
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

    // Latest grade per story (the rows arrive newest-first, so the first occurrence wins).
    let mut grades: HashMap<Uuid, eval::Grade> = HashMap::new();
    for (story_id, signal) in feedback {
        if let Some(g) = eval::Grade::from_signal(&signal) {
            grades.entry(story_id).or_insert(g);
        }
    }
    Ok(eval::evaluate(&logs, &grades))
}
