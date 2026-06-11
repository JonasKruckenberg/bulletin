use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use bulletin_core::cluster::rollup;
use bulletin_store::{cluster, event};

pub struct BuildStats {
    pub dirty_groups: usize,
    pub built_through: DateTime<Utc>,
}

/// PublicBuild: group new public events into clusters and advance the build watermark.
///
/// Runs in a single transaction holding a transaction-level advisory lock, so concurrent
/// builds serialize (the loser returns `Ok(None)`) and the whole pass is atomic — a crash
/// rolls back without advancing the watermark, leaving the events still due next tick.
/// Processes the half-open ingest range `(built_through, now()]`: finds the groups it dirtied,
/// recomputes each cluster's rollup over *all* its events, upserts, then advances to `now()`.
pub async fn run(pool: &PgPool) -> Result<Option<BuildStats>> {
    let mut tx = pool.begin().await.context("begin build txn")?;

    if !cluster::try_build_lock(&mut *tx)
        .await
        .context("acquire build lock")?
    {
        tracing::debug!("public build already in progress; skipping");
        return Ok(None);
    }

    let (built_through, hwm) = cluster::build_bounds(&mut *tx)
        .await
        .context("read build bounds")?;
    let groups = cluster::dirty_public_groups(&mut *tx, built_through, hwm)
        .await
        .context("find dirty groups")?;

    for (source, group_key) in &groups {
        let events = event::list_public_group_events(&mut *tx, *source, group_key)
            .await
            .context("load group events")?;
        if let Some(r) = rollup(&events) {
            cluster::upsert_cluster(&mut *tx, *source, group_key, &r)
                .await
                .context("upsert cluster")?;
        }
    }

    cluster::advance_build_watermark(&mut *tx, hwm)
        .await
        .context("advance watermark")?;
    tx.commit().await.context("commit build txn")?;

    Ok(Some(BuildStats {
        dirty_groups: groups.len(),
        built_through: hwm,
    }))
}
