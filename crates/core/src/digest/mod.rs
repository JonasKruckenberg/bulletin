//! The digest flow (projection / read side): take a freshness-scored lookback over the cluster
//! cache, select by recency, freeze the selection, render, and deliver — advancing the subscriber's
//! schedule on delivery. A pure read of the materialization side's snapshot (design §3.0, §9.4).

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
use crate::digest::select::{select, Candidate, Decision, Verdict};
use crate::digest::store::{
    build_render_item, cluster_cards, create_with_items, mark_delivered, render_items,
    render_items_for_stories, RenderItem,
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
    persist: bool,
) -> Result<(Vec<LinkedStory>, Vec<Decision>)> {
    let sub_id = sub.id;
    // Read the candidate clusters + the prior story assignment in the subscriber's RLS context: the
    // candidate set is `public ∪ own-private` (and the prior assignment is the subscriber's own
    // stories), never another tenant's — the query says so, and now the DB enforces it (design §12).
    let (clusters, prior) = with_scope(pool, ScopeCtx::Subscriber(sub_id), move |conn| {
        Box::pin(async move {
            let clusters =
                link::store::candidate_clusters(&mut *conn, sub_id, last_run, horizon_days)
                    .await
                    .context("collect candidate clusters")?;
            let prior = link::store::load_prior_members(&mut *conn, sub_id)
                .await
                .context("load prior story assignment")?;
            Ok((clusters, prior))
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

    let candidates = assignment
        .stories
        .iter()
        .map(|s| Candidate {
            id: s.id,
            last_event_time: s.last_event_time,
        })
        .collect();
    let decisions = select(candidates, sub.max_items as usize);
    Ok((assignment.stories, decisions))
}

/// The story ids that made the cut, in render order.
fn selected_ids(decisions: &[Decision]) -> Vec<Uuid> {
    decisions
        .iter()
        .filter(|d| matches!(d.verdict, Verdict::Selected { .. }))
        .map(|d| d.id)
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
    let (_, decisions) =
        link_and_select(pool, &sub, sub.last_run_at, CONTEXT_HORIZON_DAYS, true).await?;
    log_selection(sub.id, sub.max_items as usize, &decisions);
    let selected = selected_ids(&decisions);

    let digest = create_with_items(pool, sub.id, window_end, &selected)
        .await
        .context("create digest")?;

    if digest.delivered_at.is_some() {
        return Ok(DigestOutcome::AlreadyDelivered);
    }

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
    let (stories, decisions) = link_and_select(pool, &sub, None, lookback_days, false).await?;
    log_selection(sub.id, sub.max_items as usize, &decisions);
    let selected = selected_ids(&decisions);

    // Reassemble the selected stories (in render order) from the in-memory assignment, rendering
    // their cluster cards in the subscriber's RLS context.
    let by_id: HashMap<Uuid, &LinkedStory> = stories.iter().map(|s| (s.id, s)).collect();
    let selected_stories: Vec<LinkedStory> = selected
        .iter()
        .filter_map(|id| by_id.get(id).map(|s| (*s).clone()))
        .collect();
    let items = with_scope(pool, ScopeCtx::Subscriber(sub.id), move |conn| {
        Box::pin(async move {
            render_items_for_stories(&mut *conn, &selected_stories)
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
fn log_selection(subscriber_id: Uuid, cap: usize, decisions: &[Decision]) {
    let count = |f: fn(&Verdict) -> bool| decisions.iter().filter(|d| f(&d.verdict)).count();
    tracing::info!(
        %subscriber_id,
        candidates = decisions.len(),
        selected = count(|v| matches!(v, Verdict::Selected { .. })),
        over_cap = count(|v| matches!(v, Verdict::OverCap { .. })),
        cap,
        "selection complete"
    );
    for d in decisions {
        tracing::debug!(
            story_id = %d.id,
            last_event_time = %d.last_event_time,
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

    let (stories, decisions) =
        link_and_select(pool, &sub, sub.last_run_at, CONTEXT_HORIZON_DAYS, false).await?;

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

    Ok(decisions
        .into_iter()
        .map(|d| ExplainRow {
            verdict: d.verdict,
            story_id: d.id,
            last_event_time: d.last_event_time,
            item: by_id
                .get(&d.id)
                .and_then(|s| build_render_item(&s.clusters, &cards)),
        })
        .collect())
}
