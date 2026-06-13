//! Webhook ingest, DB-backed (requires Docker + postgres:18-alpine): connection resolution by
//! `provider_account_id`, end-to-end `process_webhook` ingest, dedup against a prior poll-shaped
//! event (fingerprint collapse), the unrouted-delivery drop (IDOR defense), and a lifecycle status
//! transition. Mirrors `tests/connection.rs::setup`.

use bulletin_core::ingest::github::event_map;
use bulletin_core::ingest::store::{insert_event, resolve_connection_by_provider};
use bulletin_core::ingest::{process_webhook, WebhookOutcome};
use bulletin_core::kind::SourceKind;
use bulletin_core::scope::Scope;
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

/// Seed a GitHub connection routed by its installation id (the webhook routing key).
async fn seed_github(pool: &sqlx::PgPool, installation_id: i64) -> Uuid {
    let row: (Uuid,) = sqlx::query_as(
        "INSERT INTO connection (source, config, provider_account_id)
         VALUES ('github', $1::jsonb, $2)
         RETURNING id",
    )
    .bind(json!({ "installation_id": installation_id }).to_string())
    .bind(installation_id.to_string())
    .fetch_one(pool)
    .await
    .unwrap();
    row.0
}

async fn count_events(pool: &sqlx::PgPool) -> i64 {
    let row: (i64,) = sqlx::query_as("SELECT count(*) FROM event")
        .fetch_one(pool)
        .await
        .unwrap();
    row.0
}

async fn connection_status(pool: &sqlx::PgPool, id: Uuid) -> String {
    let row: (String,) = sqlx::query_as("SELECT status FROM connection WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
    row.0
}

fn issue_body(installation_id: i64) -> Vec<u8> {
    json!({
        "action": "opened",
        "issue": {
            "id": 555, "number": 42, "title": "A bug",
            "html_url": "https://github.com/octo/repo/issues/42",
            "updated_at": "2026-06-10T10:00:00Z"
        },
        "repository": {"full_name": "octo/repo", "private": false},
        "sender": {"login": "alice"},
        "installation": {"id": installation_id}
    })
    .to_string()
    .into_bytes()
}

#[tokio::test]
async fn resolve_connection_by_provider_finds_the_seeded_row() {
    let (pool, _pg) = setup().await;
    let id = seed_github(&pool, 12345).await;

    let found = resolve_connection_by_provider(&pool, SourceKind::Github, "12345")
        .await
        .unwrap()
        .expect("connection should resolve");
    assert_eq!(found.id, id);

    // A foreign installation routes to nothing.
    assert!(
        resolve_connection_by_provider(&pool, SourceKind::Github, "99999")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn process_webhook_ingests_an_issue_event() {
    let (pool, _pg) = setup().await;
    seed_github(&pool, 12345).await;

    let outcome = process_webhook(
        &pool,
        SourceKind::Github,
        "issues",
        "delivery-1",
        &issue_body(12345),
    )
    .await
    .unwrap();

    assert!(matches!(
        outcome,
        WebhookOutcome::Ingested {
            inserted: 1,
            deduplicated: 0,
            ..
        }
    ));
    assert_eq!(count_events(&pool).await, 1);
}

// A webhook for an activity already ingested by the poll collapses on the fingerprint — no dup.
#[tokio::test]
async fn process_webhook_dedups_against_a_prior_poll() {
    let (pool, _pg) = setup().await;
    seed_github(&pool, 12345).await;

    // Simulate the poll having already ingested the same issue (REST events-feed shape).
    let polled = event_map::to_builder(
        serde_json::from_value(json!({
            "id": "300", "type": "IssuesEvent", "actor": {"login": "alice"},
            "repo": {"name": "octo/repo"}, "created_at": "2026-06-10T10:00:00Z",
            "payload": {"action": "opened", "issue": {
                "id": 555, "number": 42, "title": "A bug",
                "html_url": "https://github.com/octo/repo/issues/42"
            }}
        }))
        .unwrap(),
    )
    .finalize(Scope::Public);
    insert_event(&pool, &polled)
        .await
        .unwrap()
        .expect("first insert");
    assert_eq!(count_events(&pool).await, 1);

    let outcome = process_webhook(
        &pool,
        SourceKind::Github,
        "issues",
        "delivery-1",
        &issue_body(12345),
    )
    .await
    .unwrap();

    assert!(matches!(
        outcome,
        WebhookOutcome::Ingested {
            inserted: 0,
            deduplicated: 1,
            ..
        }
    ));
    assert_eq!(count_events(&pool).await, 1, "no duplicate event row");
}

// IDOR defense: a delivery whose installation matches no connection of ours is dropped.
#[tokio::test]
async fn process_webhook_drops_an_unrouted_delivery() {
    let (pool, _pg) = setup().await;
    seed_github(&pool, 12345).await;

    let outcome = process_webhook(
        &pool,
        SourceKind::Github,
        "issues",
        "delivery-x",
        &issue_body(99999), // foreign installation
    )
    .await
    .unwrap();

    assert!(matches!(outcome, WebhookOutcome::Unrouted { .. }));
    assert_eq!(count_events(&pool).await, 0);
}

#[tokio::test]
async fn process_webhook_revokes_on_installation_deleted() {
    let (pool, _pg) = setup().await;
    let id = seed_github(&pool, 12345).await;
    assert_eq!(connection_status(&pool, id).await, "active");

    let body = json!({"action": "deleted", "installation": {"id": 12345}})
        .to_string()
        .into_bytes();
    let outcome = process_webhook(&pool, SourceKind::Github, "installation", "d", &body)
        .await
        .unwrap();

    assert!(matches!(
        outcome,
        WebhookOutcome::Lifecycle {
            status: "revoked",
            ..
        }
    ));
    assert_eq!(connection_status(&pool, id).await, "revoked");
}
