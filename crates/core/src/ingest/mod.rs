//! The ingest flow: poll a connection's source, normalize items to events, and append them to
//! the event log (`store::insert_event`). Producer side of the ingest→clustering seam.

pub mod rss;
pub mod store;

use anyhow::Result;
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::{event::EventBuilder, kind::SourceKind, scope::Scope};

pub struct Batch<I, C> {
    pub items: Vec<I>,
    pub cursor: C,
}

#[derive(Debug)]
pub enum SourceError {
    Request(String),
    Parse(String),
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceError::Request(msg) => write!(f, "request error: {msg}"),
            SourceError::Parse(msg) => write!(f, "parse error: {msg}"),
        }
    }
}

impl std::error::Error for SourceError {}

/// Per-tenant live worker for one `connection` row. Every connector implements this.
pub trait Connection: Send + Sync {
    /// Opaque, source-private incremental-fetch position. Infra persists as JSON, never reads it.
    type Cursor: serde::Serialize + serde::de::DeserializeOwned + Default + Send + Sync;
    /// One unit of content from the source, complete after a poll.
    type Item: Send;

    fn poll(
        &self,
        cursor: Self::Cursor,
    ) -> impl std::future::Future<Output = Result<Batch<Self::Item, Self::Cursor>, SourceError>> + Send;

    /// Pure normalization: source-specific item → connector-side event builders. Infra calls
    /// `finalize(scope)` on each builder to stamp the scope boundary and fingerprint.
    fn to_events(&self, item: Self::Item) -> Vec<EventBuilder>;
}

/// Connection config we know how to build a live connection from. M1: RSS only.
#[derive(Deserialize)]
struct RssConfig {
    url: String,
}

/// What one `poll` did — the trigger layer turns this into metrics. `Failed` means the connector
/// errored and backoff was already recorded; `Skipped` means there was nothing to poll.
pub enum PollOutcome {
    Polled {
        source: SourceKind,
        inserted: usize,
        deduplicated: usize,
    },
    Skipped,
    Failed {
        source: SourceKind,
    },
}

/// Ingest one connection: load it, fetch its source, append new events to the log, then advance
/// the cursor (or back off on failure). Events commit before the cursor advances — the
/// crash-safety invariant: a re-poll re-fetches, and fingerprint dedup collapses the overlap.
pub async fn poll(pool: &PgPool, connection_id: Uuid) -> Result<PollOutcome> {
    let conn_row = match store::load_connection(pool, connection_id).await? {
        Some(r) => r,
        None => {
            tracing::warn!(%connection_id, "connection not found");
            return Ok(PollOutcome::Skipped);
        }
    };
    if conn_row.status != "active" {
        tracing::debug!(%connection_id, status = %conn_row.status, "skipping non-active connection");
        return Ok(PollOutcome::Skipped);
    }

    let source = conn_row.source;
    // M1: RSS only. GitHub/Slack land in later milestones.
    let conn = match source {
        SourceKind::Rss => match serde_json::from_value::<RssConfig>(conn_row.config.clone()) {
            Ok(cfg) => rss::RssConnection::new(cfg.url),
            Err(e) => {
                tracing::error!(%connection_id, error = %e, "invalid RSS config");
                return Ok(PollOutcome::Skipped);
            }
        },
        other => {
            tracing::error!(%connection_id, source = ?other, "unsupported source in M1");
            return Ok(PollOutcome::Skipped);
        }
    };

    let cursor: rss::RssCursor = conn_row
        .cursor
        .clone()
        .map(|v| serde_json::from_value(v).unwrap_or_default())
        .unwrap_or_default();

    match conn.poll(cursor).await {
        Ok(batch) => {
            let new_cursor =
                serde_json::to_value(&batch.cursor).expect("RssCursor always serializes");
            let builders: Vec<_> = batch
                .items
                .into_iter()
                .flat_map(|item| conn.to_events(item))
                .collect();
            let total = builders.len();
            let mut inserted = 0usize;
            for builder in builders {
                if store::insert_event(pool, &builder.finalize(Scope::Public))
                    .await?
                    .is_some()
                {
                    inserted += 1;
                }
            }
            let deduplicated = total - inserted;
            tracing::info!(
                connection_id = %conn_row.id,
                source = source.as_str(),
                inserted,
                deduplicated,
                "poll complete"
            );
            store::advance_cursor(pool, conn_row.id, new_cursor).await?;
            Ok(PollOutcome::Polled {
                source,
                inserted,
                deduplicated,
            })
        }
        Err(e) => {
            tracing::warn!(%connection_id, error = %e, "poll failed");
            store::record_failure(pool, conn_row.id).await?;
            Ok(PollOutcome::Failed { source })
        }
    }
}
