//! The persistence seam for the `entity_edge` identity graph (design `docs/thread-layer.md` §3, §8).
//!
//! The graph holds both directions of the feedback channel as durable rows, so identity is
//! reconstructible from the graph alone: a `must_link` is a positive row (confidence ≥ 0), a
//! `cannot_link` is a **veto** row (confidence < 0) the resolver consults to never merge the pair.
//! Scope mirrors the rest of the system: `'public'` edges are shared; `'private'` edges are fenced to
//! one subscriber, read as `public ∪ own-private`.

use sqlx::{postgres::PgRow, PgExecutor, Row};
use uuid::Uuid;

use crate::common::scope::Scope;
use crate::identity::{Edge, EdgeSource};

/// Load the positive equivalence edges visible to `subscriber_id` (`public ∪ own-private`), each with
/// its stored confidence. Veto rows (confidence < 0) are excluded here and read via [`load_vetoes`].
pub async fn load_edges(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
) -> Result<Vec<Edge>, sqlx::Error> {
    sqlx::query(
        "SELECT a, b, confidence, source FROM entity_edge
         WHERE (scope_kind = 'public' OR scope_subscriber_id = $1) AND confidence >= 0",
    )
    .bind(subscriber_id)
    .try_map(|row: PgRow| {
        let source = EdgeSource::parse(&row.get::<String, _>("source"))
            .ok_or_else(|| sqlx::Error::Decode("unknown edge source".into()))?;
        Ok(Edge {
            a: row.get("a"),
            b: row.get("b"),
            confidence: row.get("confidence"),
            source,
        })
    })
    .fetch_all(executor)
    .await
}

/// Load the `cannot_link` veto pairs visible to `subscriber_id` (confidence < 0) — the pairs the
/// resolver must never merge. Order-insensitive `(a, b)` as stored.
pub async fn load_vetoes(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    sqlx::query(
        "SELECT a, b FROM entity_edge
         WHERE (scope_kind = 'public' OR scope_subscriber_id = $1) AND confidence < 0",
    )
    .bind(subscriber_id)
    .try_map(|row: PgRow| Ok((row.get("a"), row.get("b"))))
    .fetch_all(executor)
    .await
}

/// Upsert one row in `scope`, keyed by `(scope, a, b)` — re-asserting overwrites rather than
/// duplicating. A `must_link`/positive edge and a `cannot_link` veto share the key, so the latest
/// assertion wins (asserting one direction replaces the other). Endpoints are stored as given
/// (callers pass already-canonicalized tokens).
async fn upsert(
    executor: impl PgExecutor<'_>,
    scope: &Scope,
    a: &str,
    b: &str,
    confidence: f32,
    source: EdgeSource,
) -> Result<(), sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = scope.to_columns();
    sqlx::query(
        "INSERT INTO entity_edge (scope_kind, scope_subscriber_id, a, b, confidence, source)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (scope_kind, scope_subscriber_id, a, b) DO UPDATE SET
            confidence = EXCLUDED.confidence, source = EXCLUDED.source",
    )
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(a)
    .bind(b)
    .bind(confidence)
    .bind(source.as_str())
    .execute(executor)
    .await?;
    Ok(())
}

/// Assert a positive equivalence edge (a `must_link`): confidence 1.0, source `feedback`.
pub async fn upsert_must_link(
    executor: impl PgExecutor<'_>,
    scope: &Scope,
    edge: &Edge,
) -> Result<(), sqlx::Error> {
    upsert(executor, scope, &edge.a, &edge.b, 1.0, edge.source).await
}

/// Materialize a `cannot_link` veto: a durable negative-confidence row the resolver consults, so the
/// veto survives an `entity_edge` rebuild (identity is a function of the graph, not a replayed log).
pub async fn upsert_veto(
    executor: impl PgExecutor<'_>,
    scope: &Scope,
    a: &str,
    b: &str,
) -> Result<(), sqlx::Error> {
    upsert(executor, scope, a, b, -1.0, EdgeSource::Feedback).await
}
