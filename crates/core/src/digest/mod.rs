//! The digest flow (projection / read side): take a freshness-scored lookback over the cluster
//! cache, select by recency, freeze the selection, render, and deliver — advancing the subscriber's
//! schedule on delivery. A pure read of the materialization side's snapshot (design §3.0, §9.4).

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

use crate::common::kind::SourceKind;
use crate::digest::select::{select, Decision, Verdict};
use crate::digest::store::{
    candidates_in_lookback, cluster_display, create_with_items, mark_delivered, render_items,
    render_items_for_clusters,
};
use crate::digest::subscriber::{load_subscriber, SubscriberRow};

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
    /// Window had nothing to report; watermark advanced without sending.
    Empty,
    /// Already delivered for this window (idempotent re-run).
    AlreadyDelivered,
    /// The boundary moved into the future between enqueue and run — a preference change deferred
    /// this send. Nothing delivered; the next tick fires it at the corrected boundary.
    NotYetDue,
}

/// Loads a subscriber and runs the pure selection over its lookback candidate set — the shared
/// front half of [`generate`] (which then freezes, renders, delivers) and [`explain`] (which only
/// reports). Both observe the same lookback, so they can't disagree.
async fn plan(pool: &PgPool, subscriber_id: Uuid) -> Result<(SubscriberRow, Vec<Decision>)> {
    let sub = load_subscriber(pool, subscriber_id)
        .await
        .context("load subscriber")?
        .ok_or_else(|| anyhow!("subscriber {subscriber_id} not found"))?;
    let candidates = candidates_in_lookback(pool, sub.last_run_at, CONTEXT_HORIZON_DAYS)
        .await
        .context("collect candidates")?;
    let decisions = select(candidates, sub.max_items as usize);
    Ok((sub, decisions))
}

/// The cluster ids that made the cut, in render order.
fn selected_ids(decisions: &[Decision]) -> Vec<Uuid> {
    decisions
        .iter()
        .filter(|d| matches!(d.verdict, Verdict::Selected { .. }))
        .map(|d| d.cluster_id)
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
    let (sub, decisions) = plan(pool, subscriber_id).await?;

    // A preference change (timezone/digest_time/freq) can push next_run_at into the future after this
    // job was enqueued for the old, due boundary. Don't deliver early: bail and let the next tick
    // fire it at the corrected boundary. This is what makes update_preferences safe mid-flight.
    if sub.next_run_at > Utc::now() {
        return Ok(DigestOutcome::NotYetDue);
    }

    let window_end = sub.next_run_at; // the digest's identity (UNIQUE(subscriber_id, window_end))

    log_selection(sub.id, sub.max_items as usize, &decisions);
    let selected = selected_ids(&decisions);

    let digest = create_with_items(pool, sub.id, window_end, &selected)
        .await
        .context("create digest")?;

    if digest.delivered_at.is_some() {
        return Ok(DigestOutcome::AlreadyDelivered);
    }

    let items = render_items(pool, digest.id)
        .await
        .context("load render items")?;
    if items.is_empty() {
        // Nothing to send, but advance so the subscriber isn't perpetually due.
        mark_delivered(pool, digest.id, sub.id, snapshot_at)
            .await
            .context("mark delivered")?;
        return Ok(DigestOutcome::Empty);
    }

    let message = render::render(
        mailer.from(),
        &sub.email,
        window_end,
        &sub.timezone,
        &items,
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
/// cadence, `last_run_at`, or the de-dup history. Returns `Empty` if the lookback yields nothing.
pub async fn dispatch_now(
    pool: &PgPool,
    mailer: &impl Mailer,
    subscriber_id: Uuid,
    lookback_days: i32,
    content: &DigestContent<'_>,
) -> Result<DigestOutcome> {
    let sub = load_subscriber(pool, subscriber_id)
        .await
        .context("load subscriber")?
        .ok_or_else(|| anyhow!("subscriber {subscriber_id} not found"))?;

    // Explicit lookback floor = now − lookback_days (last_run_at is ignored — this is off-schedule).
    let candidates = candidates_in_lookback(pool, None, lookback_days)
        .await
        .context("collect candidates")?;
    let decisions = select(candidates, sub.max_items as usize);
    log_selection(sub.id, sub.max_items as usize, &decisions);
    let selected = selected_ids(&decisions);
    if selected.is_empty() {
        return Ok(DigestOutcome::Empty);
    }

    let items = render_items_for_clusters(pool, &selected)
        .await
        .context("load render items")?;
    // The rendered date header uses now() — this digest isn't tied to a scheduled boundary.
    let message =
        render::render(mailer.from(), &sub.email, Utc::now(), &sub.timezone, &items, content)?;
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
            cluster_id = %d.cluster_id,
            last_event_time = %d.last_event_time,
            verdict = ?d.verdict,
            "selection decision"
        );
    }
}

/// One candidate's selection verdict joined to its display fields — a row of `digest-explain`.
pub struct ExplainRow {
    pub verdict: Verdict,
    pub cluster_id: Uuid,
    pub source: Option<SourceKind>,
    pub title: Option<String>,
    pub last_event_time: DateTime<Utc>,
}

/// Dry-run of selection for a subscriber: every candidate cluster paired with its verdict and a
/// human-readable title, with **no writes and no send**. Runs the exact same pure `select` the
/// real digest does, over the subscriber's current window.
pub async fn explain(pool: &PgPool, subscriber_id: Uuid) -> Result<Vec<ExplainRow>> {
    let (_sub, decisions) = plan(pool, subscriber_id).await?;

    let ids: Vec<Uuid> = decisions.iter().map(|d| d.cluster_id).collect();
    let display: HashMap<Uuid, _> = cluster_display(pool, &ids)
        .await
        .context("load cluster display")?
        .into_iter()
        .map(|c| (c.id, c))
        .collect();

    Ok(decisions
        .into_iter()
        .map(|d| {
            let disp = display.get(&d.cluster_id);
            ExplainRow {
                verdict: d.verdict,
                cluster_id: d.cluster_id,
                source: disp.map(|c| c.source),
                title: disp.map(|c| c.title.clone()),
                last_event_time: d.last_event_time,
            }
        })
        .collect())
}
