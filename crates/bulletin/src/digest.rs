use anyhow::{anyhow, Context, Result};
use chrono::Duration;
use sqlx::PgPool;
use uuid::Uuid;

use bulletin_core::select::{select, Selection};
use bulletin_store::{
    cluster::candidates_in_window,
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
    let cfg = Selection { relevance_floor: 0.0, max_items: sub.max_items as usize };
    let selected = select(candidates, &cfg);

    let digest = create_with_items(pool, sub.id, window_start, window_end, &selected)
        .await
        .context("create digest")?;

    if digest.delivered_at.is_some() {
        return Ok(DigestOutcome::AlreadyDelivered);
    }

    let items = render_items(pool, digest.id).await.context("load render items")?;
    if items.is_empty() {
        // Nothing to send, but advance so the subscriber isn't perpetually due.
        mark_delivered(pool, digest.id, sub.id, window_end).await.context("mark delivered")?;
        return Ok(DigestOutcome::Empty);
    }

    let message = email::render(&email.from, &sub.email, window_end, &items)?;
    email.build_sender()?.send(message).await?;
    mark_delivered(pool, digest.id, sub.id, window_end).await.context("mark delivered")?;

    Ok(DigestOutcome::Delivered { items: items.len() })
}
