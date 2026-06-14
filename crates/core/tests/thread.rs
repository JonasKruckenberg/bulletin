//! Integration coverage for the Thread layer & tiered identity (`docs/thread-layer.md`) against a
//! real Postgres, post-M3: that `thread_maintenance` turns a subscriber's stories into threads and a
//! projected entity-weight map, that fire-time thread-assignment finds the right thread, and that
//! must/cannot-link feedback materializes into the identity graph. The pure algorithms (resolve,
//! select, label propagation) are unit/proptested in `core`. Requires Docker + `postgres:18-alpine`.

use bulletin_core::cluster::build_private;
use bulletin_core::feedback::{self, Signal, TargetType};
use bulletin_core::ingest::store::insert_event;
use bulletin_core::link;
use bulletin_core::thread::{self, store as tstore, MaintenanceConfig};
use bulletin_core::{
    connect,
    digest::subscriber::{insert_subscriber, Recurrence},
    event::EventBuilder,
    identity::store as identity_store,
    kind::SourceKind,
    migrate,
};
use chrono::{Duration, NaiveTime, Utc};
use sqlx::{PgPool, Row};
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

async fn setup() -> (PgPool, testcontainers::ContainerAsync<Postgres>) {
    let pg = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .expect("failed to start postgres — requires Docker and postgres:18-alpine");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    (pool, pg)
}

async fn new_subscriber(pool: &PgPool) -> Uuid {
    insert_subscriber(
        pool,
        "me@example.com",
        None,
        Recurrence::Daily,
        "UTC",
        NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    )
    .await
    .unwrap()
}

/// Insert a private GitHub-shaped event owning `subscriber`, with explicit namespaced `entities`.
/// `secs_ago` ages the event relative to *now* so it lands inside `thread_maintenance`'s rolling
/// window (a fixed 1970 epoch would fall outside it and yield no co-occurrence sources).
async fn insert_private(
    pool: &PgPool,
    subscriber: Uuid,
    stable_id: &str,
    group_key: &str,
    entities: &[&str],
    secs_ago: i64,
) {
    let ev = EventBuilder::new(
        SourceKind::Github,
        stable_id,
        Utc::now() - Duration::seconds(secs_ago),
        "Private activity",
        group_key,
    )
    .entities(entities.iter().map(|s| s.to_string()).collect())
    .private(true)
    .finalize(Some(subscriber));
    insert_event(pool, &ev).await.unwrap();
}

/// Build the subscriber's private clusters and link+persist their stories (what `generate` does,
/// minus rendering) — so `thread_maintenance` has stories to read.
async fn build_stories(pool: &PgPool, subscriber: Uuid) {
    build_private(pool, subscriber).await.unwrap();
    let clusters = link::store::candidate_clusters(pool, subscriber, None, 3650)
        .await
        .unwrap();
    let prior = link::store::load_prior_members(pool, subscriber)
        .await
        .unwrap();
    let assignment = link::link(&clusters, &prior, Uuid::now_v7);
    link::store::persist_assignment(pool, subscriber, &assignment)
        .await
        .unwrap();
}

#[tokio::test]
async fn maintenance_builds_threads_and_projects_weights() {
    let (pool, _pg) = setup().await;
    let sub = new_subscriber(&pool).await;

    // Two private clusters co-occurring the same entity pair → a community → a thread.
    insert_private(
        &pool,
        sub,
        "e1",
        "g1",
        &["repo:acme/widgets", "user:dlewis"],
        100,
    )
    .await;
    insert_private(
        &pool,
        sub,
        "e2",
        "g2",
        &["repo:acme/widgets", "user:dlewis"],
        200,
    )
    .await;
    build_stories(&pool, sub).await;

    let stats = thread::maintain(&pool, sub, Utc::now(), &MaintenanceConfig::default())
        .await
        .unwrap();
    assert!(
        stats.threads_written >= 1,
        "expected a thread, got {stats:?}"
    );

    let thread_count: i64 =
        sqlx::query("SELECT count(*) AS n FROM thread WHERE subscriber_id = $1")
            .bind(sub)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get("n");
    assert!(thread_count >= 1);

    let weights = tstore::load_entity_weights(&pool, sub).await.unwrap();
    assert!(
        weights.get("repo:acme/widgets").copied().unwrap_or(0.0) > 0.0,
        "weights: {weights:?}"
    );
}

#[tokio::test]
async fn thread_assignment_finds_the_thread() {
    let (pool, _pg) = setup().await;
    let sub = new_subscriber(&pool).await;

    insert_private(
        &pool,
        sub,
        "e1",
        "g1",
        &["repo:acme/widgets", "user:dlewis"],
        100,
    )
    .await;
    insert_private(
        &pool,
        sub,
        "e2",
        "g2",
        &["repo:acme/widgets", "user:dlewis"],
        200,
    )
    .await;
    build_stories(&pool, sub).await;
    thread::maintain(&pool, sub, Utc::now(), &MaintenanceConfig::default())
        .await
        .unwrap();

    let hit = tstore::assign_thread(&pool, sub, &["repo:acme/widgets".to_string()], 1)
        .await
        .unwrap();
    assert!(hit.is_some(), "expected an assignment");
    let miss = tstore::assign_thread(&pool, sub, &["repo:unrelated".to_string()], 1)
        .await
        .unwrap();
    assert!(miss.is_none(), "unrelated entity must not match");
}

#[tokio::test]
async fn must_link_writes_edge_and_cannot_link_writes_veto() {
    let (pool, _pg) = setup().await;
    let sub = new_subscriber(&pool).await;

    feedback::submit(
        &pool,
        sub,
        TargetType::Entity,
        "user:dlewis",
        Signal::MustLink,
        Some("user:dana"),
    )
    .await
    .unwrap();
    feedback::submit(
        &pool,
        sub,
        TargetType::Entity,
        "user:alice",
        Signal::CannotLink,
        Some("user:alicia"),
    )
    .await
    .unwrap();

    let edges = identity_store::load_edges(&pool, sub).await.unwrap();
    assert!(
        edges
            .iter()
            .any(|e| (e.a == "user:dlewis" && e.b == "user:dana")
                || (e.a == "user:dana" && e.b == "user:dlewis")),
        "expected a must_link edge, got {edges:?}"
    );
    // The veto is NOT a positive edge...
    assert!(
        !edges
            .iter()
            .any(|e| e.a.contains("alice") || e.b.contains("alice")),
        "cannot_link must not appear as a positive edge"
    );
    // ...it's a durable veto row.
    let vetoes = identity_store::load_vetoes(&pool, sub).await.unwrap();
    assert!(
        vetoes
            .iter()
            .any(|(a, b)| (a == "user:alice" && b == "user:alicia")
                || (a == "user:alicia" && b == "user:alice")),
        "expected a cannot_link veto, got {vetoes:?}"
    );
}

#[tokio::test]
async fn must_link_merges_entities_in_maintenance() {
    let (pool, _pg) = setup().await;
    let sub = new_subscriber(&pool).await;

    // The same human under two handles, co-occurring with a repo across two stories.
    insert_private(
        &pool,
        sub,
        "e1",
        "g1",
        &["repo:acme/widgets", "user:dlewis"],
        100,
    )
    .await;
    insert_private(
        &pool,
        sub,
        "e2",
        "g2",
        &["repo:acme/widgets", "user:dana"],
        200,
    )
    .await;
    feedback::submit(
        &pool,
        sub,
        TargetType::Entity,
        "user:dlewis",
        Signal::MustLink,
        Some("user:dana"),
    )
    .await
    .unwrap();
    build_stories(&pool, sub).await;
    thread::maintain(&pool, sub, Utc::now(), &MaintenanceConfig::default())
        .await
        .unwrap();

    // The two handles resolved to one identity, so exactly one of them carries the projected weight
    // (the component representative) — the merge collapsed them rather than weighting both halves.
    let weights = tstore::load_entity_weights(&pool, sub).await.unwrap();
    let weighted_handles = ["user:dlewis", "user:dana"]
        .iter()
        .filter(|h| weights.contains_key(**h))
        .count();
    assert_eq!(
        weighted_handles, 1,
        "must_link should collapse the two handles to one: {weights:?}"
    );
}
