use bulletin_core::ingest::store::insert_event;
use bulletin_core::{connect, event::EventBuilder, kind::SourceKind, migrate, scope::Scope};
use chrono::Utc;
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;

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

fn rss(stable_id: &str, title: &str) -> EventBuilder {
    EventBuilder::new(
        SourceKind::Rss,
        stable_id,
        Utc::now(),
        title,
        format!("feed/{stable_id}"),
    )
    .links(vec![format!("https://example.com/{stable_id}")])
}

// First insert returns the full event with all fields round-tripped correctly.
#[tokio::test]
async fn insert_returns_event() {
    let (pool, _pg) = setup().await;

    let new_ev = rss("item-1", "Hello world").finalize(None);
    let event = insert_event(&pool, &new_ev)
        .await
        .unwrap()
        .expect("first insert should return event");

    assert_eq!(event.source, SourceKind::Rss);
    assert_eq!(event.scope, Scope::Public);
    assert_eq!(event.title, "Hello world");
    assert_eq!(event.links, vec!["https://example.com/item-1"]);
    assert_eq!(event.fingerprint, new_ev.fingerprint);
}

// Re-polling the same stable_id with changed content must return None — the edit
// collapses on the fingerprint rather than inserting a new event.
#[tokio::test]
async fn repoll_same_item_is_deduped() {
    let (pool, _pg) = setup().await;

    let first = rss("item-1", "Original title").finalize(None);
    assert!(insert_event(&pool, &first).await.unwrap().is_some());

    let second = EventBuilder::new(
        SourceKind::Rss,
        "item-1",
        Utc::now(),
        "Edited title",
        "feed/item-1",
    )
    .body("body added on edit")
    .entities(vec!["Rust".into()])
    .finalize(None);

    assert_eq!(
        first.fingerprint, second.fingerprint,
        "same stable_id → same fingerprint"
    );
    assert!(
        insert_event(&pool, &second).await.unwrap().is_none(),
        "re-poll of same item must not insert a new row",
    );
}

// Two items with distinct stable_ids insert independently.
#[tokio::test]
async fn distinct_stable_ids_both_insert() {
    let (pool, _pg) = setup().await;

    let ea = insert_event(&pool, &rss("item-a", "Item A").finalize(None))
        .await
        .unwrap()
        .expect("item-a should insert");
    let eb = insert_event(&pool, &rss("item-b", "Item B").finalize(None))
        .await
        .unwrap()
        .expect("item-b should insert");

    assert_ne!(ea.id, eb.id);
    assert_ne!(ea.fingerprint, eb.fingerprint);
}
