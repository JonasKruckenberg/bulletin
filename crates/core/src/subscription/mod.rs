//! The subscriber ↔ source (`connection`) relation: the sources a subscriber chose to comprise their
//! digest. This is the explicit join the digest candidate scope filters on
//! ([`link::store::candidate_clusters`](crate::link::store::candidate_clusters)) — a public cluster
//! enters a subscriber's digest only when they subscribe to the connection that produced it.
//!
//! Owning a connection implies a subscription (`ingest::store::insert_connection` seeds one for the
//! owner); ownerless public sources (RSS) require an explicit subscribe. Deleting either side drops
//! the row (both FKs `ON DELETE CASCADE`), so a deleted subscriber's subscriptions — and, via the
//! private-scope cascade added alongside, their private clusters/events — are reclaimed.
//!
//! Operator/control-plane operations, run in the `Admin` scope like the rest of the connection
//! management surface; the `subscription` RLS policy admits admin (and a subscriber to its own rows).

use sqlx::PgPool;
use uuid::Uuid;

use crate::common::db::{begin_scope, ScopeCtx};
use crate::ingest::store::{row_to_connection, ConnectionRow, CONNECTION_COLUMNS};

/// Subscribes `subscriber_id` to `connection_id`. Idempotent — re-subscribing is a no-op (the
/// composite PK collapses the duplicate). Returns `true` if a new row was created.
pub async fn subscribe(
    pool: &PgPool,
    subscriber_id: Uuid,
    connection_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let result = sqlx::query(
        "INSERT INTO subscription (subscriber_id, connection_id) VALUES ($1, $2)
         ON CONFLICT DO NOTHING",
    )
    .bind(subscriber_id)
    .bind(connection_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    let created = result.rows_affected() > 0;
    // Only count/announce a real change; a redundant re-subscribe is a no-op, not an edit.
    if created {
        metrics::counter!("bulletin_subscription_changes_total", "op" => "subscribe").increment(1);
    }
    tracing::info!(%subscriber_id, %connection_id, created, "subscribe");
    Ok(created)
}

/// Unsubscribes `subscriber_id` from `connection_id`. Returns `true` if a row was removed. The
/// connection's clusters simply stop entering this subscriber's candidate set on the next digest.
pub async fn unsubscribe(
    pool: &PgPool,
    subscriber_id: Uuid,
    connection_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let result =
        sqlx::query("DELETE FROM subscription WHERE subscriber_id = $1 AND connection_id = $2")
            .bind(subscriber_id)
            .bind(connection_id)
            .execute(&mut *tx)
            .await?;
    tx.commit().await?;
    let removed = result.rows_affected() > 0;
    if removed {
        metrics::counter!("bulletin_subscription_changes_total", "op" => "unsubscribe")
            .increment(1);
    }
    tracing::info!(%subscriber_id, %connection_id, removed, "unsubscribe");
    Ok(removed)
}

/// The connections a subscriber is subscribed to — the sources comprising their digest — ordered for
/// a stable listing.
pub async fn list_subscriptions(
    pool: &PgPool,
    subscriber_id: Uuid,
) -> Result<Vec<ConnectionRow>, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    // Membership via an IN-subquery rather than a JOIN: `connection` and `subscription` both carry a
    // `subscriber_id` column (the connection's owner vs the subscription's subscriber), so a joined
    // `SELECT {CONNECTION_COLUMNS}` would be an ambiguous-column error. The subquery keeps the
    // projection unambiguously on `connection`, reusing the shared column list + its row mapper.
    let rows = sqlx::query(&format!(
        "SELECT {CONNECTION_COLUMNS}
         FROM connection
         WHERE id IN (SELECT connection_id FROM subscription WHERE subscriber_id = $1)
         ORDER BY next_poll_at"
    ))
    .bind(subscriber_id)
    .try_map(row_to_connection)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    tracing::debug!(%subscriber_id, count = rows.len(), "list subscriptions");
    Ok(rows)
}
