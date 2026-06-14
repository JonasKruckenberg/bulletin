//! The append-only feedback log (design §10.3, thread-layer §4) — the channel through which a user
//! corrects relevance and aggregation. It is per-subscriber and revisable: a correction is logged,
//! and the identity-graph effect of a must/cannot-link is **materialized into `entity_edge` in the
//! same transaction**, so identity stays a function of the graph alone (reconstructible without
//! replaying the log) and the correction takes effect on the next `thread_maintenance` recompute.
//!
//! Two families:
//! - **Entity-level** `must_link` / `cannot_link` (thread-layer §3.2): `must_link` writes a positive
//!   equivalence edge ("yes, this is Dana Lewis"); `cannot_link` writes a durable **veto** row ("no,
//!   different person") the resolver consults to never merge the pair.
//! - **Thread-level** `care_more` / `care_less` / `done` — fold into a thread's affinity delta on the
//!   next maintenance pass.

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::{postgres::PgRow, PgExecutor, PgPool, Row};
use uuid::Uuid;

use crate::common::db::{begin_scope, ScopeCtx};
use crate::common::scope::Scope;
use crate::identity::{canonicalize, store as identity_store, Edge, EdgeSource};

/// What a feedback signal targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetType {
    Entity,
    Thread,
    Story,
}

impl TargetType {
    pub fn as_str(self) -> &'static str {
        match self {
            TargetType::Entity => "entity",
            TargetType::Thread => "thread",
            TargetType::Story => "story",
        }
    }
}

/// The correction itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    CareMore,
    CareLess,
    /// This thread is resolved — stop surfacing it (a strong care-less).
    Done,
    /// These two entities are the same (confirm an equivalence edge).
    MustLink,
    /// These two entities are *not* the same (veto the edge).
    CannotLink,
}

impl Signal {
    pub fn as_str(self) -> &'static str {
        match self {
            Signal::CareMore => "care_more",
            Signal::CareLess => "care_less",
            Signal::Done => "done",
            Signal::MustLink => "must_link",
            Signal::CannotLink => "cannot_link",
        }
    }

    /// The affinity nudge a thread-level signal contributes when maintenance folds it in. Entity-link
    /// signals contribute none (they act on the identity graph, not affinity).
    pub fn affinity_delta(self) -> f32 {
        match self {
            Signal::CareMore => 1.0,
            Signal::CareLess => -1.0,
            Signal::Done => -5.0,
            Signal::MustLink | Signal::CannotLink => 0.0,
        }
    }
}

/// Submit a feedback correction: append it to the log and, for an entity-level `must_link` /
/// `cannot_link`, materialize the identity-graph effect in the **same transaction** (so the log and
/// the graph can't disagree across a crash). `other` is required for the link signals — the other
/// token the target is (or isn't) the same as; both are canonicalized exactly like the resolver's
/// node ids so the edge/veto lands on the right nodes.
pub async fn submit(
    pool: &PgPool,
    subscriber_id: Uuid,
    target_type: TargetType,
    target_id: &str,
    signal: Signal,
    other: Option<&str>,
) -> Result<()> {
    // Writes this subscriber's own rows (a private `entity_edge` + the `feedback` log) → its RLS
    // context, atomically.
    let mut tx = begin_scope(pool, ScopeCtx::Subscriber(subscriber_id)).await?;

    if matches!(target_type, TargetType::Entity) {
        let scope = Scope::Private(subscriber_id);
        let a = canonicalize(target_id);
        let b = other.map(canonicalize).filter(|b| !b.is_empty());
        if !a.is_empty() {
            match (signal, b) {
                (Signal::MustLink, Some(b)) => {
                    let edge = Edge::from_source(a, b, EdgeSource::Feedback);
                    identity_store::upsert_must_link(&mut *tx, &scope, &edge).await?;
                }
                (Signal::CannotLink, Some(b)) => {
                    identity_store::upsert_veto(&mut *tx, &scope, &a, &b).await?;
                }
                _ => {}
            }
        }
    }

    let payload = match other {
        Some(o) => serde_json::json!({ "other": o }),
        None => serde_json::json!({}),
    };
    sqlx::query(
        "INSERT INTO feedback (subscriber_id, target_type, target_id, signal, payload)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(subscriber_id)
    .bind(target_type.as_str())
    .bind(target_id)
    .bind(signal.as_str())
    .bind(payload)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

/// A thread-level care signal since the last maintenance pass — `(thread_id, affinity_delta)`.
pub struct ThreadCare {
    pub thread_id: Uuid,
    pub delta: f32,
}

/// Thread-level care/less/done feedback newer than `since` (the maintenance watermark), so each
/// correction folds into affinity exactly once. Entity-link signals are excluded (they already
/// materialized into the graph at submit time). The caller sums by thread.
pub async fn thread_care_since(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    since: DateTime<Utc>,
) -> Result<Vec<ThreadCare>, sqlx::Error> {
    sqlx::query(
        "SELECT target_id, signal
         FROM feedback
         WHERE subscriber_id = $1 AND target_type = 'thread'
           AND signal IN ('care_more','care_less','done')
           AND created_at > $2",
    )
    .bind(subscriber_id)
    .bind(since)
    .try_map(|row: PgRow| {
        let thread_id = Uuid::parse_str(&row.get::<String, _>("target_id"))
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        let delta = match row.get::<String, _>("signal").as_str() {
            "care_more" => Signal::CareMore.affinity_delta(),
            "care_less" => Signal::CareLess.affinity_delta(),
            _ => Signal::Done.affinity_delta(),
        };
        Ok(ThreadCare { thread_id, delta })
    })
    .fetch_all(executor)
    .await
}
