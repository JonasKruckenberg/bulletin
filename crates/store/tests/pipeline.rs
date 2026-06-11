//! Integration coverage for the M1 tick DAG's store contract: PublicBuild grouping, the
//! candidate-window query, the PublicBuild → GenerateDigest dependency gate, and the digest
//! delivery lifecycle. Mirrors the orchestration in the `bulletin` binary against a real Postgres.

use bulletin_core::{
    cluster::rollup,
    event::EventBuilder,
    kind::{ContentKind, SourceKind},
    scope::Scope,
    select::{select, Selection},
};
use bulletin_store::{
    cluster::{
        advance_build_watermark, build_bounds, candidates_in_window, dirty_public_groups,
        upsert_cluster,
    },
    connect,
    digest::{create_with_items, mark_delivered, render_items},
    event::{insert_event, list_public_group_events},
    migrate,
    subscriber::{due_subscribers, insert_subscriber, load_subscriber},
};
use chrono::{Duration, TimeZone, Utc};
use sqlx::{PgPool, Row};
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;

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

/// Inserts a public event. `secs` is the event_time (epoch seconds).
async fn insert_public(pool: &PgPool, stable_id: &str, group_key: &str, title: &str, secs: i64) {
    let ev = EventBuilder::new(
        SourceKind::Rss,
        stable_id,
        Utc.timestamp_opt(secs, 0).single().unwrap(),
        title,
        group_key,
        ContentKind::Longform,
    )
    .links(vec![format!("https://example.com/{stable_id}")])
    .finalize(Scope::Public);
    insert_event(pool, &ev).await.unwrap();
}

/// Runs PublicBuild's core loop inline (the binary's `build::run`, minus the advisory lock).
async fn build_all(pool: &PgPool) {
    let (lo, hi) = build_bounds(pool).await.unwrap();
    let groups = dirty_public_groups(pool, lo, hi).await.unwrap();
    for (source, group_key) in &groups {
        let events = list_public_group_events(pool, *source, group_key)
            .await
            .unwrap();
        if let Some(r) = rollup(&events) {
            upsert_cluster(pool, *source, group_key, &r).await.unwrap();
        }
    }
    advance_build_watermark(pool, hi).await.unwrap();
}

async fn cluster_count(pool: &PgPool) -> i64 {
    sqlx::query("SELECT count(*) AS n FROM cluster")
        .fetch_one(pool)
        .await
        .unwrap()
        .get("n")
}

// Each distinct group_key becomes its own cluster; events sharing a group_key collapse into one,
// represented by the latest.
#[tokio::test]
async fn build_groups_events_into_clusters() {
    let (pool, _pg) = setup().await;

    insert_public(&pool, "a", "a", "Article A", 100).await;
    insert_public(&pool, "b", "b", "Article B", 300).await;
    insert_public(&pool, "c1", "shared", "Shared first", 200).await;
    insert_public(&pool, "c2", "shared", "Shared latest", 400).await;

    build_all(&pool).await;

    // 3 groups → 3 clusters (the two "shared" events collapse into one).
    assert_eq!(cluster_count(&pool).await, 3);

    // The shared cluster is represented by its latest event.
    let shared = list_public_group_events(&pool, SourceKind::Rss, "shared")
        .await
        .unwrap();
    assert_eq!(rollup(&shared).unwrap().title, "Shared latest");

    let window_end = Utc::now() + Duration::hours(1);
    let candidates = candidates_in_window(&pool, None, window_end).await.unwrap();
    assert_eq!(candidates.len(), 3);

    // Pure selection caps and orders newest-first.
    let cfg = Selection {
        relevance_floor: 0.0,
        max_items: 2,
    };
    assert_eq!(select(candidates, &cfg).len(), 2);
}

// Re-building a dirtied group upserts the same cluster row in place, never a duplicate.
#[tokio::test]
async fn rebuild_upserts_cluster_in_place() {
    let (pool, _pg) = setup().await;

    insert_public(&pool, "x1", "g", "First", 100).await;
    build_all(&pool).await;
    assert_eq!(cluster_count(&pool).await, 1);

    // A new event in the same group re-dirties it; the next build updates, not inserts.
    insert_public(&pool, "x2", "g", "Second", 200).await;
    build_all(&pool).await;
    assert_eq!(cluster_count(&pool).await, 1);

    let title: String = sqlx::query("SELECT title FROM cluster WHERE group_key = 'g'")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("title");
    assert_eq!(title, "Second"); // representative advanced to the latest event
}

// The dependency gate: a subscriber whose window contains an unbuilt public event is withheld
// until PublicBuild catches up. Events are inserted *before* the subscriber, so their ingest_time
// precedes next_run_at and the gate's NOT EXISTS clause fires.
#[tokio::test]
async fn digest_waits_for_public_build() {
    let (pool, _pg) = setup().await;

    insert_public(&pool, "a", "a", "Pre-existing", 100).await;
    let sub_id = insert_subscriber(&pool, "me@example.com", 1).await.unwrap();

    // Due by the clock, but its window has an unbuilt event → not yet selectable.
    let due = due_subscribers(&pool).await.unwrap();
    assert!(
        due.iter().all(|s| s.id != sub_id),
        "subscriber must wait for build"
    );

    build_all(&pool).await;

    let due = due_subscribers(&pool).await.unwrap();
    assert!(
        due.iter().any(|s| s.id == sub_id),
        "subscriber due once built"
    );
}

// The digest freezes its selection, delivers once, advances the subscriber watermark, and is
// idempotent: re-creating the same window returns the same row with items intact.
#[tokio::test]
async fn digest_delivery_and_idempotency() {
    let (pool, _pg) = setup().await;

    insert_public(&pool, "a", "a", "Article A", 100).await;
    insert_public(&pool, "b", "b", "Article B", 200).await;
    let sub_id = insert_subscriber(&pool, "me@example.com", 1).await.unwrap();
    build_all(&pool).await;

    let sub = load_subscriber(&pool, sub_id).await.unwrap().unwrap();
    let window_end = sub.next_run_at;
    let window_start = window_end - Duration::days(1);

    let candidates = candidates_in_window(&pool, sub.last_run_at, window_end)
        .await
        .unwrap();
    let cfg = Selection {
        relevance_floor: 0.0,
        max_items: sub.max_items as usize,
    };
    let selected = select(candidates, &cfg);
    assert_eq!(selected.len(), 2);

    let digest = create_with_items(&pool, sub_id, window_start, window_end, &selected)
        .await
        .unwrap();
    assert!(digest.delivered_at.is_none());

    let items = render_items(&pool, digest.id).await.unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].source, SourceKind::Rss);

    mark_delivered(&pool, digest.id, sub_id, window_end)
        .await
        .unwrap();

    // Re-creating the same window returns the same delivered row; items unchanged (no duplicates).
    let again = create_with_items(&pool, sub_id, window_start, window_end, &selected)
        .await
        .unwrap();
    assert_eq!(again.id, digest.id);
    assert!(again.delivered_at.is_some());
    assert_eq!(render_items(&pool, again.id).await.unwrap().len(), 2);

    // Watermark advanced one interval; the just-delivered boundary is now last_run_at.
    let sub = load_subscriber(&pool, sub_id).await.unwrap().unwrap();
    assert_eq!(sub.last_run_at, Some(window_end));
    assert_eq!(sub.next_run_at, window_end + Duration::days(1));
}
