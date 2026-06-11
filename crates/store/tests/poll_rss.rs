use axum::{routing::get, Router};
use bulletin_connectors::rss::{RssConnection, RssCursor};
use bulletin_core::{connector::Connection, scope::Scope};
use bulletin_store::{
    connect,
    connection::{advance_cursor, load_connection},
    event::insert_event,
    migrate,
};
use serde_json::json;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio::net::TcpListener;

const RSS_XML: &str = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <link>https://example.com</link>
    <description>A test feed</description>
    <item>
      <guid>https://example.com/post-1</guid>
      <title>Hello World</title>
      <link>https://example.com/post-1</link>
      <pubDate>Mon, 09 Jun 2025 10:00:00 +0000</pubDate>
    </item>
    <item>
      <guid>https://example.com/post-2</guid>
      <title>Second Post</title>
      <link>https://example.com/post-2</link>
      <pubDate>Mon, 09 Jun 2025 11:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>"#;

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

/// Binds a local server, returns its URL. Feed body is static.
async fn serve_feed(body: &'static str) -> String {
    let app = Router::new().route("/feed", get(move || async move { body }));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://127.0.0.1:{port}/feed")
}

async fn insert_connection(pool: &sqlx::PgPool, feed_url: &str) -> uuid::Uuid {
    let row: (uuid::Uuid,) = sqlx::query_as(
        "INSERT INTO connection (source, config, next_poll_at)
         VALUES ('rss', $1::jsonb, now() - interval '1 second')
         RETURNING id",
    )
    .bind(json!({ "url": feed_url }).to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    row.0
}

/// Mirrors the worker's handle_poll_connection logic without apalis scaffolding.
async fn run_poll_cycle(pool: &sqlx::PgPool, connection_id: uuid::Uuid) -> usize {
    let row = load_connection(pool, connection_id).await.unwrap().unwrap();
    assert_eq!(row.status, "active");

    let feed_url: String = row.config["url"].as_str().unwrap().into();
    let conn = RssConnection::new(&feed_url);

    let cursor: RssCursor = row
        .cursor
        .map(|v| serde_json::from_value(v).unwrap_or_default())
        .unwrap_or_default();

    let batch = conn.poll(cursor).await.expect("poll should succeed");
    let item_count = batch.items.len();

    // Events committed before cursor advance — crash-safety invariant.
    for item in batch.items {
        let ev = conn
            .to_events(item)
            .into_iter()
            .next()
            .unwrap()
            .finalize(Scope::Public);
        insert_event(pool, &ev).await.unwrap();
    }

    let new_cursor = serde_json::to_value(&batch.cursor).unwrap();
    advance_cursor(pool, connection_id, new_cursor)
        .await
        .unwrap();

    item_count
}

async fn event_count(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM event")
        .fetch_one(pool)
        .await
        .unwrap()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn first_poll_inserts_all_items() {
    let (pool, _pg) = setup().await;
    let feed_url = serve_feed(RSS_XML).await;
    let id = insert_connection(&pool, &feed_url).await;

    let inserted = run_poll_cycle(&pool, id).await;

    assert_eq!(inserted, 2);
    assert_eq!(event_count(&pool).await, 2);
}

#[tokio::test]
async fn second_poll_deduplicates_events() {
    let (pool, _pg) = setup().await;
    let feed_url = serve_feed(RSS_XML).await;
    let id = insert_connection(&pool, &feed_url).await;

    run_poll_cycle(&pool, id).await;
    run_poll_cycle(&pool, id).await;

    // Feed hasn't changed; fingerprints match → no new rows inserted.
    assert_eq!(
        event_count(&pool).await,
        2,
        "re-poll must not duplicate events"
    );
}

#[tokio::test]
async fn poll_advances_cursor_and_reschedules() {
    let (pool, _pg) = setup().await;
    let feed_url = serve_feed(RSS_XML).await;
    let id = insert_connection(&pool, &feed_url).await;

    run_poll_cycle(&pool, id).await;

    let row = load_connection(&pool, id).await.unwrap().unwrap();
    assert!(row.last_polled_at.is_some(), "last_polled_at should be set");
    assert!(
        row.next_poll_at > chrono::Utc::now(),
        "next_poll_at should be in the future"
    );
    assert_eq!(row.consecutive_failures, 0);
}

#[tokio::test]
async fn events_have_correct_fields_after_full_cycle() {
    let (pool, _pg) = setup().await;
    let feed_url = serve_feed(RSS_XML).await;
    let id = insert_connection(&pool, &feed_url).await;

    run_poll_cycle(&pool, id).await;

    let titles: Vec<String> = sqlx::query_scalar("SELECT title FROM event ORDER BY event_time")
        .fetch_all(&pool)
        .await
        .unwrap();

    assert_eq!(titles, ["Hello World", "Second Post"]);
}
