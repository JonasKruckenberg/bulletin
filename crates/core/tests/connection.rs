use bulletin_core::ingest::store::{
    advance_cursor, due_connections, load_connection, record_failure,
};
use bulletin_core::kind::SourceKind;
use bulletin_core::{connect, migrate};
use serde_json::json;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

async fn setup() -> (sqlx::PgPool, testcontainers::ContainerAsync<Postgres>) {
    let pg = Postgres::default()
        .with_tag("18-alpine")
        .start()
        .await
        .expect("failed to start postgres container — requires Docker and postgres:18-alpine");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgresql://postgres:postgres@127.0.0.1:{port}/postgres");
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    (pool, pg)
}

async fn insert_rss(pool: &sqlx::PgPool, url: &str) -> Uuid {
    let row: (Uuid,) = sqlx::query_as(
        "INSERT INTO connection (source, config, next_poll_at)
         VALUES ('rss', $1::jsonb, now() - interval '1 second')
         RETURNING id",
    )
    .bind(json!({ "url": url }).to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    row.0
}

#[tokio::test]
async fn load_connection_returns_row() {
    let (pool, _pg) = setup().await;
    let id = insert_rss(&pool, "https://example.com/feed.rss").await;

    let row = load_connection(&pool, id)
        .await
        .unwrap()
        .expect("row should exist");

    assert_eq!(row.id, id);
    assert_eq!(row.source, SourceKind::Rss);
    assert_eq!(row.status, "active");
    assert!(row.cursor.is_none());
    assert_eq!(row.consecutive_failures, 0);
}

#[tokio::test]
async fn load_connection_returns_none_for_unknown_id() {
    let (pool, _pg) = setup().await;
    let result = load_connection(&pool, Uuid::new_v4()).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn due_connections_returns_overdue_active_rows() {
    let (pool, _pg) = setup().await;
    let id = insert_rss(&pool, "https://example.com/feed.rss").await;

    let due = due_connections(&pool).await.unwrap();

    assert_eq!(due.len(), 1);
    assert_eq!(due[0].id, id);
}

#[tokio::test]
async fn due_connections_excludes_future_next_poll() {
    let (pool, _pg) = setup().await;
    // Insert with next_poll_at in the future.
    sqlx::query(
        "INSERT INTO connection (source, config, next_poll_at)
         VALUES ('rss', '{\"url\":\"https://example.com\"}'::jsonb, now() + interval '1 hour')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let due = due_connections(&pool).await.unwrap();
    assert!(due.is_empty());
}

#[tokio::test]
async fn advance_cursor_stores_cursor_and_schedules_next_poll() {
    let (pool, _pg) = setup().await;
    let id = insert_rss(&pool, "https://example.com/feed.rss").await;

    let cursor = json!({ "etag": "\"v1\"", "last_modified": null });
    advance_cursor(&pool, id, cursor.clone()).await.unwrap();

    let row = load_connection(&pool, id).await.unwrap().unwrap();

    assert_eq!(row.cursor, Some(cursor));
    assert_eq!(row.consecutive_failures, 0);
    // next_poll_at must now be in the future (rescheduled by poll_interval_secs).
    assert!(row.next_poll_at > chrono::Utc::now());
    assert!(row.last_polled_at.is_some());
}

#[tokio::test]
async fn advance_cursor_resets_consecutive_failures() {
    let (pool, _pg) = setup().await;
    let id = insert_rss(&pool, "https://example.com/feed.rss").await;

    // Simulate prior failures.
    record_failure(&pool, id).await.unwrap();
    record_failure(&pool, id).await.unwrap();

    advance_cursor(&pool, id, json!({})).await.unwrap();

    let row = load_connection(&pool, id).await.unwrap().unwrap();
    assert_eq!(
        row.consecutive_failures, 0,
        "advance_cursor must clear failure count"
    );
}

#[tokio::test]
async fn record_failure_increments_failure_count() {
    let (pool, _pg) = setup().await;
    let id = insert_rss(&pool, "https://example.com/feed.rss").await;

    record_failure(&pool, id).await.unwrap();

    let row = load_connection(&pool, id).await.unwrap().unwrap();
    assert_eq!(row.consecutive_failures, 1);
    assert_eq!(row.status, "active");
}

#[tokio::test]
async fn record_failure_applies_exponential_backoff() {
    let (pool, _pg) = setup().await;
    let id = insert_rss(&pool, "https://example.com/feed.rss").await;

    record_failure(&pool, id).await.unwrap();
    let after_1 = load_connection(&pool, id)
        .await
        .unwrap()
        .unwrap()
        .next_poll_at;

    record_failure(&pool, id).await.unwrap();
    let after_2 = load_connection(&pool, id)
        .await
        .unwrap()
        .unwrap()
        .next_poll_at;

    // Second failure should schedule further out than the first (doubling interval).
    assert!(
        after_2 > after_1,
        "backoff should increase with each failure"
    );
}

#[tokio::test]
async fn record_failure_flips_to_errored_at_fifth_failure() {
    let (pool, _pg) = setup().await;
    let id = insert_rss(&pool, "https://example.com/feed.rss").await;

    for _ in 0..4 {
        record_failure(&pool, id).await.unwrap();
        let row = load_connection(&pool, id).await.unwrap().unwrap();
        assert_eq!(
            row.status, "active",
            "status must remain active before 5 failures"
        );
    }

    record_failure(&pool, id).await.unwrap();
    let row = load_connection(&pool, id).await.unwrap().unwrap();
    assert_eq!(row.status, "errored");
    assert_eq!(row.consecutive_failures, 5);
}

#[tokio::test]
async fn errored_connections_are_excluded_from_due_connections() {
    let (pool, _pg) = setup().await;
    let id = insert_rss(&pool, "https://example.com/feed.rss").await;

    for _ in 0..5 {
        record_failure(&pool, id).await.unwrap();
    }
    let row = load_connection(&pool, id).await.unwrap().unwrap();
    assert_eq!(row.status, "errored");

    let due = due_connections(&pool).await.unwrap();
    assert!(
        due.is_empty(),
        "errored connections must not appear in due_connections"
    );
}
