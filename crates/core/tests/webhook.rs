//! Webhook ingest, DB-backed (requires Docker + postgres:18-alpine): connection resolution by
//! `provider_account_id`, end-to-end `process_webhook` ingest, dedup against a prior poll-shaped
//! event (fingerprint collapse), the unrouted-delivery drop (IDOR defense), and a lifecycle status
//! transition. Mirrors `tests/connection.rs::setup`.

use bulletin_core::digest::subscriber::{insert_subscriber, Recurrence};
use bulletin_core::ingest::github::event_map;
use bulletin_core::ingest::store::{insert_event, resolve_connection_by_provider};
use bulletin_core::ingest::{process_webhook, WebhookOutcome};
use bulletin_core::kind::SourceKind;
use bulletin_core::{connect, migrate};
use chrono::NaiveTime;
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

/// Seed a GitHub connection routed by its installation id (the webhook routing key). GitHub is a
/// private-capable source, so it must be owned; these tests exercise public-repo deliveries, so the
/// owner is incidental — `seed_github_owned` is used where the owner's scope is the point.
async fn seed_github(pool: &sqlx::PgPool, installation_id: i64) -> Uuid {
    let owner = seed_subscriber(pool, &format!("owner-{installation_id}@example.com")).await;
    seed_github_owned(pool, installation_id, owner).await
}

/// Seed a GitHub connection owned by `owner` — its private-repo deliveries finalize to that scope.
async fn seed_github_owned(pool: &sqlx::PgPool, installation_id: i64, owner: Uuid) -> Uuid {
    let row: (Uuid,) = sqlx::query_as(
        "INSERT INTO connection (source, config, provider_account_id, subscriber_id)
         VALUES ('github', $1::jsonb, $2, $3)
         RETURNING id",
    )
    .bind(json!({ "installation_id": installation_id }).to_string())
    .bind(installation_id.to_string())
    .bind(owner)
    .fetch_one(pool)
    .await
    .unwrap();
    row.0
}

/// Insert a bare subscriber to own a connection.
async fn seed_subscriber(pool: &sqlx::PgPool, email: &str) -> Uuid {
    insert_subscriber(
        pool,
        email,
        None,
        Recurrence::Daily,
        "UTC",
        NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
    )
    .await
    .unwrap()
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
    .finalize(None);
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

// A private-repo webhook (`repository.private:true`) on an *owned* connection ingests as a private
// event bound to that owner — derived from OUR connection row, never the payload (IDOR defense).
#[tokio::test]
async fn process_webhook_scopes_private_repo_to_owner() {
    let (pool, _pg) = setup().await;
    let owner = seed_subscriber(&pool, "owner@example.com").await;
    seed_github_owned(&pool, 12345, owner).await;

    let body = json!({
        "action": "opened",
        "issue": {
            "id": 999, "number": 7, "title": "secret bug",
            "updated_at": "2026-06-10T10:00:00Z"
        },
        "repository": {"full_name": "octo/secret", "private": true},
        "sender": {"login": "alice"},
        "installation": {"id": 12345}
    })
    .to_string()
    .into_bytes();

    let outcome = process_webhook(&pool, SourceKind::Github, "issues", "d-priv", &body)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        WebhookOutcome::Ingested { inserted: 1, .. }
    ));

    // The event row is scoped to the connection owner — not public, not another subscriber.
    let (scope_kind, scope_sub): (String, Option<Uuid>) =
        sqlx::query_as("SELECT scope_kind, scope_subscriber_id FROM event LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(scope_kind, "private");
    assert_eq!(scope_sub, Some(owner));
}
