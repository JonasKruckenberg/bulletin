use anyhow::{Context, Result};
use apalis::prelude::*;
use apalis_cron::{CronStream, Tick};
use apalis_postgres::PostgresStorage;
use chrono::Utc;
use cron::Schedule;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

use bulletin_connectors::rss::RssConnection;
use bulletin_core::{connector::Connection, kind::SourceKind, scope::Scope};
use bulletin_store::{
    connection::{advance_cursor, due_connections, load_connection, record_failure, ConnectionRow},
    event::insert_event,
};

pub async fn setup_storage(pool: &PgPool) -> Result<()> {
    let mut m = PostgresStorage::<(), (), ()>::migrations();
    m.ignore_missing = true;
    m.run(pool).await.context("apalis migrations failed")?;
    Ok(())
}

// ── PollConnection job ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollConnectionJob {
    pub connection_id: Uuid,
}

/// M1 dispatch: RSS only. Becomes a full enum when GitHub lands in M2.
enum ConnDispatch {
    Rss(RssConnection),
}

#[derive(Deserialize)]
struct RssConfig {
    url: String,
}

fn build_dispatch(row: &ConnectionRow) -> Result<ConnDispatch, Box<dyn std::error::Error>> {
    match row.source {
        SourceKind::Rss => {
            let cfg: RssConfig = serde_json::from_value(row.config.clone())?;
            Ok(ConnDispatch::Rss(RssConnection::new(cfg.url)))
        }
        _ => Err(format!("unsupported source {:?} in M1", row.source).into()),
    }
}

async fn handle_poll_connection(
    job: PollConnectionJob,
    pool: Data<PgPool>,
) -> Result<(), BoxDynError> {
    let conn_row = match load_connection(&*pool, job.connection_id).await? {
        Some(r) => r,
        None => {
            tracing::warn!(connection_id = %job.connection_id, "connection not found");
            return Ok(());
        }
    };

    if conn_row.status != "active" {
        tracing::debug!(connection_id = %job.connection_id, status = %conn_row.status, "skipping non-active connection");
        return Ok(());
    }

    let dispatch = match build_dispatch(&conn_row) {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(connection_id = %job.connection_id, error = %e, "build_dispatch failed");
            return Ok(());
        }
    };

    // Cursor/creds erase to serde_json::Value at the arm boundary; typed within each arm.
    let result = match dispatch {
        ConnDispatch::Rss(conn) => {
            let cursor = conn_row
                .cursor
                .clone()
                .map(|v| serde_json::from_value(v).unwrap_or_default())
                .unwrap_or_default();
            conn.poll(cursor).await.map(|b| {
                let builders = b
                    .items
                    .into_iter()
                    .flat_map(|item| conn.to_events(item))
                    .collect::<Vec<_>>();
                let new_cursor =
                    serde_json::to_value(&b.cursor).expect("RssCursor always serializes");
                (builders, new_cursor)
            })
        }
    };

    match result {
        Ok((builders, new_cursor)) => {
            let total = builders.len();
            let mut inserted = 0usize;
            // Events committed before cursor advance — crash-safety invariant (arch §3).
            for builder in builders {
                let ev = builder.finalize(Scope::Public);
                if insert_event(&*pool, &ev).await?.is_some() {
                    inserted += 1;
                }
            }
            tracing::info!(
                connection_id = %conn_row.id,
                source = %conn_row.source.as_str(),
                inserted,
                deduplicated = total - inserted,
                "poll complete"
            );
            advance_cursor(&*pool, conn_row.id, new_cursor).await?;
        }
        Err(e) => {
            tracing::warn!(connection_id = %job.connection_id, error = %e, "poll failed");
            record_failure(&*pool, conn_row.id).await?;
        }
    }

    Ok(())
}

// ── Cron tick ──────────────────────────────────────────────────────────

async fn handle_tick(_: Tick<Utc>, pool: Data<PgPool>) -> Result<(), BoxDynError> {
    let due = due_connections(&*pool).await?;
    if due.is_empty() {
        tracing::debug!("tick: no connections due");
        return Ok(());
    }
    tracing::info!(count = due.len(), "tick: dispatching due connections");
    let mut storage: PostgresStorage<PollConnectionJob> = PostgresStorage::new(&*pool);
    for row in due {
        storage.push(PollConnectionJob { connection_id: row.id }).await?;
    }
    Ok(())
}

pub async fn start(pool: PgPool) -> Result<()> {
    setup_storage(&pool).await?;

    let schedule = Schedule::from_str("0 * * * * *").context("invalid cron expression")?;

    let pool_tick = pool.clone();
    let pool_poll = pool.clone();

    Monitor::new()
        .register(move |_| {
            let pool = pool_tick.clone();
            WorkerBuilder::new("bulletin-tick")
                .backend(CronStream::new(schedule.clone()))
                .data(pool)
                .build(handle_tick)
        })
        .register(move |_| {
            let pool = pool_poll.clone();
            let storage: PostgresStorage<PollConnectionJob> = PostgresStorage::new(&pool);
            WorkerBuilder::new("bulletin-poll-connection")
                .backend(storage)
                .data(pool)
                .build(handle_poll_connection)
        })
        .run()
        .await
        .context("worker monitor exited with error")?;

    Ok(())
}
