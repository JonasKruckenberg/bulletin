//! Integration coverage for the tick's store contract: PublicBuild grouping, the lookback
//! candidate query, the (now decoupled) build/digest relationship, recurrence scheduling, and the
//! digest delivery lifecycle. Mirrors the orchestration in the `bulletin` binary against a real
//! Postgres.

use bulletin_core::cluster::store::{
    advance_build_watermark, build_bounds, dirty_public_groups, list_public_group_events,
    upsert_cluster,
};
use bulletin_core::digest::select::{select, Verdict};
use bulletin_core::digest::store::{
    candidates_in_lookback, create_with_items, mark_delivered, render_items,
};
use bulletin_core::digest::subscriber::{
    advance_after_delivery, due_subscribers, insert_subscriber, load_subscriber,
    update_preferences, Recurrence,
};
use bulletin_core::ingest::store::insert_event;
use bulletin_core::{
    cluster::rollup, connect, event::EventBuilder, kind::SourceKind, migrate, scope::Scope,
};
use chrono::{Duration, NaiveTime, TimeZone, Utc};
use sqlx::{PgPool, Row};
use testcontainers::{runners::AsyncRunner, ImageExt};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

/// 09:00 — the default local digest time, spelled out for test readability.
fn nine_am() -> NaiveTime {
    NaiveTime::from_hms_opt(9, 0, 0).unwrap()
}

/// Force a freshly-inserted subscriber due *now*. Signup schedules the first digest at the next
/// local digest time (in the future); delivery-path tests want it immediately due.
async fn force_due(pool: &PgPool, id: Uuid) {
    sqlx::query("UPDATE subscriber SET next_run_at = now() WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

/// The next_run_at's local time-of-day in `tz`, read back through Postgres (avoids a chrono-tz
/// test dependency) — used to assert a digest landed at the chosen wall-clock hour.
async fn local_run_time(pool: &PgPool, id: Uuid, tz: &str) -> NaiveTime {
    sqlx::query("SELECT (next_run_at AT TIME ZONE $2)::time AS lt FROM subscriber WHERE id = $1")
        .bind(id)
        .bind(tz)
        .fetch_one(pool)
        .await
        .unwrap()
        .get("lt")
}

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

    let candidates = candidates_in_lookback(&pool, None, 30).await.unwrap();
    assert_eq!(candidates.len(), 3);

    // Pure selection caps and orders newest-first.
    let selected = select(candidates, 2)
        .into_iter()
        .filter(|d| matches!(d.verdict, Verdict::Selected { .. }))
        .count();
    assert_eq!(selected, 2);
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
// No build gate: a due subscriber is dispatched even with unbuilt public events. Those events just
// aren't candidates until clustered — they ride the next fire (never lost), so build and digest are
// decoupled rather than gated.
#[tokio::test]
async fn no_build_gate_unbuilt_events_ride_next_fire() {
    let (pool, _pg) = setup().await;

    insert_public(&pool, "a", "a", "Pre-existing", 100).await;
    let sub_id = insert_subscriber(
        &pool,
        "me@example.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();
    force_due(&pool, sub_id).await;

    // Due regardless of build state — no gate.
    let due = due_subscribers(&pool).await.unwrap();
    assert!(
        due.iter().any(|s| s.id == sub_id),
        "subscriber is due even with unbuilt events"
    );
    // ...but the unbuilt event is not yet a candidate.
    assert!(
        candidates_in_lookback(&pool, None, 30)
            .await
            .unwrap()
            .is_empty(),
        "unbuilt event is not a candidate"
    );

    build_all(&pool).await;

    // Once clustered it becomes a candidate.
    assert_eq!(
        candidates_in_lookback(&pool, None, 30).await.unwrap().len(),
        1,
        "built cluster is now a candidate"
    );
}

// The digest freezes its selection, delivers once, advances the subscriber watermark, and is
// idempotent: re-creating the same window returns the same row with items intact.
#[tokio::test]
async fn digest_delivery_and_idempotency() {
    let (pool, _pg) = setup().await;

    insert_public(&pool, "a", "a", "Article A", 100).await;
    insert_public(&pool, "b", "b", "Article B", 200).await;
    let sub_id = insert_subscriber(
        &pool,
        "me@example.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();
    force_due(&pool, sub_id).await;
    build_all(&pool).await;

    let sub = load_subscriber(&pool, sub_id).await.unwrap().unwrap();
    let window_end = sub.next_run_at;

    let candidates = candidates_in_lookback(&pool, sub.last_run_at, 30)
        .await
        .unwrap();
    let selected: Vec<_> = select(candidates, sub.max_items as usize)
        .into_iter()
        .filter(|d| matches!(d.verdict, Verdict::Selected { .. }))
        .map(|d| d.cluster_id)
        .collect();
    assert_eq!(selected.len(), 2);

    let digest = create_with_items(&pool, sub_id, window_end, &selected)
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
    let again = create_with_items(&pool, sub_id, window_end, &selected)
        .await
        .unwrap();
    assert_eq!(again.id, digest.id);
    assert!(again.delivered_at.is_some());
    assert_eq!(render_items(&pool, again.id).await.unwrap().len(), 2);

    // Watermark advanced one cadence; the just-delivered boundary is now last_run_at, and
    // next_run_at snapped to the next 09:00 UTC slot strictly after it (DST-safe wall-clock grid,
    // not a blind +24h).
    let sub = load_subscriber(&pool, sub_id).await.unwrap().unwrap();
    assert_eq!(sub.last_run_at, Some(window_end));
    assert!(sub.next_run_at > window_end);
    assert!(sub.next_run_at <= window_end + Duration::days(1));
    assert_eq!(local_run_time(&pool, sub_id, "UTC").await, nine_am());
}

// Signup schedules the first digest at the next occurrence of the local digest time in the
// subscriber's own zone — in the future, at the chosen wall-clock hour (not "due immediately").
#[tokio::test]
async fn insert_schedules_next_local_digest_time() {
    let (pool, _pg) = setup().await;

    let id = insert_subscriber(
        &pool,
        "ny@example.com",
        None,
        Recurrence::Daily,
        "America/New_York",
        NaiveTime::from_hms_opt(7, 30, 0).unwrap(),
    )
    .await
    .unwrap();

    let sub = load_subscriber(&pool, id).await.unwrap().unwrap();
    assert!(
        sub.next_run_at > Utc::now(),
        "first digest is in the future"
    );
    assert_eq!(
        local_run_time(&pool, id, "America/New_York").await,
        NaiveTime::from_hms_opt(7, 30, 0).unwrap(),
        "lands at the chosen local time-of-day"
    );
}

// An optional display name round-trips through signup, and a blank/whitespace one normalizes to
// absent (NULL) rather than an empty string the greeting would awkwardly splice in.
#[tokio::test]
async fn insert_stores_optional_name() {
    let (pool, _pg) = setup().await;

    let named = insert_subscriber(
        &pool,
        "alice@example.com",
        Some("  Alice  "),
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();
    // Stored trimmed.
    assert_eq!(
        load_subscriber(&pool, named).await.unwrap().unwrap().name,
        Some("Alice".to_string())
    );

    let blank = insert_subscriber(
        &pool,
        "blank@example.com",
        Some("   "),
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();
    assert_eq!(
        load_subscriber(&pool, blank).await.unwrap().unwrap().name,
        None
    );

    let none = insert_subscriber(
        &pool,
        "none@example.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();
    assert_eq!(
        load_subscriber(&pool, none).await.unwrap().unwrap().name,
        None
    );
}

// Updating timezone/digest time reschedules to the next earliest occurrence WITHOUT losing the
// pending window: last_run_at (the selection lower bound) is untouched, so every event since the
// last delivery still falls in the next digest — the window only reshapes.
#[tokio::test]
async fn update_preferences_snaps_without_losing_window() {
    let (pool, _pg) = setup().await;

    let id = insert_subscriber(
        &pool,
        "t@example.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();

    // A prior delivery establishes a window lower bound that must survive the reschedule.
    let last = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
    sqlx::query("UPDATE subscriber SET last_run_at = $2 WHERE id = $1")
        .bind(id)
        .bind(last)
        .execute(&pool)
        .await
        .unwrap();

    let changed = update_preferences(
        &pool,
        id,
        Recurrence::Daily,
        "America/New_York",
        NaiveTime::from_hms_opt(7, 30, 0).unwrap(),
    )
    .await
    .unwrap();
    assert!(changed);

    let sub = load_subscriber(&pool, id).await.unwrap().unwrap();
    // The window lower bound is preserved — nothing lost, nothing replayed.
    assert_eq!(sub.last_run_at, Some(last));
    assert_eq!(sub.timezone, "America/New_York");
    assert_eq!(sub.digest_time, NaiveTime::from_hms_opt(7, 30, 0).unwrap());
    // Snapped to a future 07:30 New_York slot.
    assert!(sub.next_run_at > Utc::now());
    assert!(sub.next_run_at <= Utc::now() + Duration::days(1));
    assert_eq!(
        local_run_time(&pool, id, "America/New_York").await,
        NaiveTime::from_hms_opt(7, 30, 0).unwrap()
    );
}

// An unknown IANA zone is rejected by the database rather than silently mis-scheduling.
#[tokio::test]
async fn insert_rejects_unknown_timezone() {
    let (pool, _pg) = setup().await;
    let err = insert_subscriber(
        &pool,
        "bad@example.com",
        None,
        Recurrence::Daily,
        "Mars/Phobos",
        nine_am(),
    )
    .await;
    assert!(err.is_err(), "unknown timezone must be rejected");
}

// Weekly recurrence pins a stable weekday at the chosen local time, in the future.
#[tokio::test]
async fn weekly_schedules_on_chosen_weekday() {
    let (pool, _pg) = setup().await;

    // weekly on Tuesday (Postgres DOW 2) at 17:00 UTC.
    let id = insert_subscriber(
        &pool,
        "w@example.com",
        None,
        Recurrence::Weekly { weekday: 2 },
        "UTC",
        NaiveTime::from_hms_opt(17, 0, 0).unwrap(),
    )
    .await
    .unwrap();

    let sub = load_subscriber(&pool, id).await.unwrap().unwrap();
    assert!(sub.next_run_at > Utc::now());
    let dow: i32 = sqlx::query(
        "SELECT extract(dow from (next_run_at AT TIME ZONE 'UTC'))::int AS dow
         FROM subscriber WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap()
    .get("dow");
    assert_eq!(dow, 2, "lands on Tuesday");
    assert_eq!(
        local_run_time(&pool, id, "UTC").await,
        NaiveTime::from_hms_opt(17, 0, 0).unwrap()
    );
}

// After a worker outage spanning several boundaries, one delivery jumps next_run_at straight to the
// next *future* boundary — coalescing, not backfilling a burst of stale digests.
#[tokio::test]
async fn advance_coalesces_missed_boundaries() {
    let (pool, _pg) = setup().await;

    let id = insert_subscriber(
        &pool,
        "c@example.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();
    // Pretend the worker was down: the boundary is days in the past.
    sqlx::query(
        "UPDATE subscriber
         SET next_run_at = now() - interval '3 days', last_run_at = now() - interval '4 days'
         WHERE id = $1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();
    let overdue_boundary = load_subscriber(&pool, id)
        .await
        .unwrap()
        .unwrap()
        .next_run_at;

    advance_after_delivery(&pool, id, overdue_boundary)
        .await
        .unwrap();

    let sub = load_subscriber(&pool, id).await.unwrap().unwrap();
    assert_eq!(sub.last_run_at, Some(overdue_boundary));
    assert!(
        sub.next_run_at > Utc::now(),
        "coalesced to a future boundary — no backlog of past fires"
    );
    assert!(sub.next_run_at <= Utc::now() + Duration::days(1));
    assert_eq!(local_run_time(&pool, id, "UTC").await, nine_am());
}
