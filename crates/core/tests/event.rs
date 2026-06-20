use bulletin_core::ingest::store::insert_event;
use bulletin_core::{connect, event::EventBuilder, kind::SourceKind, migrate, scope::Scope};
use chrono::Utc;
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

/// Inserts a minimal `subscriber` row with the given id (everything but `email` defaults), so a
/// private event can finalize to it — `event.scope_subscriber_id` is an FK to `subscriber`.
async fn seed_subscriber(pool: &sqlx::PgPool, id: Uuid) {
    sqlx::query("INSERT INTO subscriber (id, email) VALUES ($1, $2)")
        .bind(id)
        .bind(format!("{id}@example.com"))
        .execute(pool)
        .await
        .unwrap();
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

// Dedup is scoped per owner: the same content-identity (same fingerprint) seen by two different
// private owners are two distinct events — a shared private repo can't collapse one tenant's
// activity into another's. Within a single owner's scope a re-observation still dedups.
#[tokio::test]
async fn fingerprint_dedup_is_scoped_per_owner() {
    let (pool, _pg) = setup().await;
    let alice = Uuid::new_v4();
    let bob = Uuid::new_v4();
    // Private events bind to these owners, which must exist — scope_subscriber_id is an FK.
    seed_subscriber(&pool, alice).await;
    seed_subscriber(&pool, bob).await;

    let a = rss("issue:999", "Alice's view")
        .private(true)
        .finalize(Some(alice));
    let b = rss("issue:999", "Bob's view")
        .private(true)
        .finalize(Some(bob));

    // Same content → same fingerprint (the fingerprint is scope-free, by design)...
    assert_eq!(a.fingerprint, b.fingerprint);

    // ...but the (fingerprint, scope) identity differs, so both insert — no cross-tenant collapse.
    assert!(insert_event(&pool, &a).await.unwrap().is_some());
    assert!(
        insert_event(&pool, &b).await.unwrap().is_some(),
        "a second owner seeing the same private activity must not be dropped as a duplicate"
    );

    // A re-observation within the same owner's scope (e.g. poll after webhook) still dedups.
    assert!(insert_event(&pool, &a).await.unwrap().is_none());

    // And a public event with that same fingerprint is its own identity too.
    let shared = rss("issue:999", "public view").finalize(None);
    assert_eq!(shared.fingerprint, a.fingerprint);
    assert!(insert_event(&pool, &shared).await.unwrap().is_some());
}
