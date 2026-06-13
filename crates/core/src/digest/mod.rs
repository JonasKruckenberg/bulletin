//! The digest flow: drain the clusters a subscriber gained since its last run, select by recency,
//! freeze the selection, render, and deliver — advancing the subscriber's watermark on delivery.
//! Consumer of the clustering→digest seam.

mod render;
pub mod select;
pub mod store;
pub mod subscriber;

pub use render::{DigestContent, Mailer};

use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::kind::SourceKind;
use crate::digest::select::{select, Decision, Verdict};
use crate::digest::store::{
    candidates_in_window, cluster_display, create_with_items, mark_delivered, render_items,
};
use crate::digest::subscriber::{load_subscriber, SubscriberRow};

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

/// Loads a subscriber and runs the pure selection over its current window — the shared front
/// half of [`generate`] (which then freezes, renders, delivers) and [`explain`] (which only
/// reports). Both observe the same `(last_run_at, next_run_at]` window, so they can't disagree.
async fn plan(pool: &PgPool, subscriber_id: Uuid) -> Result<(SubscriberRow, Vec<Decision>)> {
    let sub = load_subscriber(pool, subscriber_id)
        .await
        .context("load subscriber")?
        .ok_or_else(|| anyhow!("subscriber {subscriber_id} not found"))?;
    let candidates = candidates_in_window(pool, sub.last_run_at, sub.next_run_at)
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
    let (sub, decisions) = plan(pool, subscriber_id).await?;

    // A preference change (timezone/digest_time) can push next_run_at into the future after this
    // job was enqueued for the old, due boundary. Don't deliver early: bail and let the next tick
    // fire it at the corrected boundary. This is what makes update_preferences safe mid-flight.
    if sub.next_run_at > Utc::now() {
        return Ok(DigestOutcome::NotYetDue);
    }

    let window_end = sub.next_run_at;
    let window_start = sub
        .last_run_at
        .unwrap_or_else(|| window_end - Duration::days(sub.interval_days as i64));

    log_selection(sub.id, sub.max_items as usize, &decisions);
    let selected = selected_ids(&decisions);

    let digest = create_with_items(pool, sub.id, window_start, window_end, &selected)
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
        mark_delivered(pool, digest.id, sub.id, window_end)
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
    mark_delivered(pool, digest.id, sub.id, window_end)
        .await
        .context("mark delivered")?;

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
