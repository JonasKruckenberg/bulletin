use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use bulletin_core::{
    cluster::Cluster,
    id::Id,
    kind::SourceKind,
    select::{select_explained, Decision, Selection, Verdict},
};
use bulletin_store::{
    cluster::{candidates_in_window, cluster_display},
    digest::{create_with_items, mark_delivered, render_items},
    subscriber::load_subscriber,
};

use crate::email::{self, EmailConfig};

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
}

/// GenerateDigest for one subscriber: select the window's candidate clusters, freeze them into a
/// digest, render, and deliver — advancing the subscriber's watermark on delivery. Idempotent and
/// resumable: the `(subscriber, window_end)` row is created with its items in one transaction, and
/// a re-run finds the frozen selection (and skips a second send once `delivered_at` is set).
pub async fn run(pool: &PgPool, email: &EmailConfig, subscriber_id: Uuid) -> Result<DigestOutcome> {
    let sub = load_subscriber(pool, subscriber_id)
        .await
        .context("load subscriber")?
        .ok_or_else(|| anyhow!("subscriber {subscriber_id} not found"))?;

    let window_end = sub.next_run_at;
    let window_start = sub
        .last_run_at
        .unwrap_or_else(|| window_end - Duration::days(sub.interval_days as i64));

    // Selection is pure over the window's candidates; only the first attempt persists it.
    let candidates = candidates_in_window(pool, sub.last_run_at, window_end)
        .await
        .context("collect candidates")?;
    let cfg = Selection {
        relevance_floor: 0.0,
        max_items: sub.max_items as usize,
    };
    let decisions = select_explained(candidates, &cfg);
    log_selection(sub.id, &cfg, &decisions);
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

    let message = email::render(&email.from, &sub.email, window_end, &items)?;
    email.build_sender()?.send(message).await?;
    mark_delivered(pool, digest.id, sub.id, window_end)
        .await
        .context("mark delivered")?;

    metrics::counter!("bulletin_digests_delivered_total").increment(1);
    Ok(DigestOutcome::Delivered { items: items.len() })
}

/// The cluster ids that made the cut, in render order (a `Selected` projection of the trace).
fn selected_ids(decisions: &[Decision]) -> Vec<Id<Cluster>> {
    decisions
        .iter()
        .filter(|d| matches!(d.verdict, Verdict::Selected { .. }))
        .map(|d| d.cluster_id)
        .collect()
}

/// Emits the selection audit trail: a one-line summary at INFO, then a per-candidate line at
/// DEBUG (`RUST_LOG=bulletin=debug`) so "why is this cluster in/out of the digest?" is
/// answerable straight from the worker logs. Mirrors `debug digest-explain` (which dry-runs it).
fn log_selection(subscriber_id: Uuid, cfg: &Selection, decisions: &[Decision]) {
    let count = |f: fn(&Verdict) -> bool| decisions.iter().filter(|d| f(&d.verdict)).count();
    tracing::info!(
        %subscriber_id,
        candidates = decisions.len(),
        selected = count(|v| matches!(v, Verdict::Selected { .. })),
        over_cap = count(|v| matches!(v, Verdict::OverCap { .. })),
        below_floor = count(|v| matches!(v, Verdict::BelowFloor)),
        cap = cfg.max_items,
        floor = cfg.relevance_floor,
        "selection complete"
    );
    for d in decisions {
        tracing::debug!(
            cluster_id = %d.cluster_id.as_uuid(),
            last_event_time = %d.last_event_time,
            relevance = d.relevance,
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
    pub relevance: f32,
}

/// Dry-run of selection for a subscriber: every candidate cluster paired with its verdict and a
/// human-readable title, with **no writes and no send**. This is the "why did the digest pick
/// what it picked?" inspector — it runs the exact same pure `select_explained` the real digest
/// does, over the subscriber's current window.
pub async fn explain(pool: &PgPool, subscriber_id: Uuid) -> Result<Vec<ExplainRow>> {
    let sub = load_subscriber(pool, subscriber_id)
        .await
        .context("load subscriber")?
        .ok_or_else(|| anyhow!("subscriber {subscriber_id} not found"))?;

    let candidates = candidates_in_window(pool, sub.last_run_at, sub.next_run_at)
        .await
        .context("collect candidates")?;
    let cfg = Selection {
        relevance_floor: 0.0,
        max_items: sub.max_items as usize,
    };
    let decisions = select_explained(candidates, &cfg);

    let ids: Vec<Uuid> = decisions.iter().map(|d| d.cluster_id.as_uuid()).collect();
    let display: HashMap<Uuid, _> = cluster_display(pool, &ids)
        .await
        .context("load cluster display")?
        .into_iter()
        .map(|c| (c.id, c))
        .collect();

    Ok(decisions
        .into_iter()
        .map(|d| {
            let id = d.cluster_id.as_uuid();
            let disp = display.get(&id);
            ExplainRow {
                verdict: d.verdict,
                cluster_id: id,
                source: disp.map(|c| c.source),
                title: disp.map(|c| c.title.clone()),
                last_event_time: d.last_event_time,
                relevance: d.relevance,
            }
        })
        .collect())
}
