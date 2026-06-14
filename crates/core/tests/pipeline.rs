//! Integration coverage for the tick's store contract: PublicBuild grouping, the lookback
//! candidate query, the (now decoupled) build/digest relationship, recurrence scheduling, and the
//! digest delivery lifecycle. Mirrors the orchestration in the `bulletin` binary against a real
//! Postgres.

use bulletin_core::cluster::store::{
    advance_build_watermark, build_bounds, dirty_groups, list_group_events, upsert_cluster,
};
use bulletin_core::digest::select::{select, Candidate, Verdict};
use bulletin_core::digest::store::{create_with_items, mark_delivered, render_items};
use bulletin_core::digest::subscriber::{
    advance_after_delivery, due_subscribers, insert_subscriber, load_subscriber,
    update_preferences, Recurrence,
};
use bulletin_core::ingest::store::{insert_connection, insert_event};
use bulletin_core::link::{
    link,
    store::{candidate_clusters, load_prior_members, persist_assignment},
};
use bulletin_core::{
    begin_scope,
    cluster::{build_private, rollup},
    connect,
    event::EventBuilder,
    grant_runtime_role,
    kind::SourceKind,
    migrate,
    scope::Scope,
    ScopeCtx, RUNTIME_ROLE,
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
    .finalize(None);
    insert_event(pool, &ev).await.unwrap();
}

/// Inserts a private event owned by `subscriber` (a private-repo-shaped item).
async fn insert_private(
    pool: &PgPool,
    subscriber: Uuid,
    stable_id: &str,
    group_key: &str,
    title: &str,
    secs: i64,
) {
    let ev = EventBuilder::new(
        SourceKind::Github,
        stable_id,
        Utc.timestamp_opt(secs, 0).single().unwrap(),
        title,
        group_key,
    )
    .links(vec![format!("https://example.com/{stable_id}")])
    .private(true)
    .finalize(Some(subscriber));
    insert_event(pool, &ev).await.unwrap();
}

/// Runs PublicBuild's core loop inline (the binary's `build::run`, minus the advisory lock).
async fn build_all(pool: &PgPool) {
    let (lo, hi) = build_bounds(pool).await.unwrap();
    let groups = dirty_groups(pool, &Scope::Public, lo, hi).await.unwrap();
    for (source, group_key) in &groups {
        let events = list_group_events(pool, &Scope::Public, *source, group_key)
            .await
            .unwrap();
        if let Some(r) = rollup(&events) {
            upsert_cluster(pool, &Scope::Public, *source, group_key, &r)
                .await
                .unwrap();
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

/// Link a subscriber's candidate clusters into stories, persist the assignment, and return the
/// selected story ids in render order — the store-level mirror of `digest::generate`'s middle.
async fn select_stories(
    pool: &PgPool,
    sub_id: Uuid,
    last_run: Option<chrono::DateTime<Utc>>,
    max_items: usize,
) -> Vec<Uuid> {
    let clusters = candidate_clusters(pool, sub_id, last_run, 30)
        .await
        .unwrap();
    let assignment = link(&clusters, &[], Uuid::now_v7);
    persist_assignment(pool, sub_id, &assignment).await.unwrap();
    let candidates: Vec<Candidate> = assignment
        .stories
        .iter()
        .map(|s| Candidate {
            id: s.id,
            last_event_time: s.last_event_time,
        })
        .collect();
    select(candidates, max_items)
        .into_iter()
        .filter(|d| matches!(d.verdict, Verdict::Selected { .. }))
        .map(|d| d.id)
        .collect()
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
    let shared = list_group_events(&pool, &Scope::Public, SourceKind::Rss, "shared")
        .await
        .unwrap();
    assert_eq!(rollup(&shared).unwrap().title, "Shared latest");

    let candidates = candidate_clusters(&pool, Uuid::nil(), None, 30)
        .await
        .unwrap();
    assert_eq!(candidates.len(), 3);

    // No shared linkable entity (distinct urls; a shared domain doesn't link) → 3 singleton stories;
    // selection caps at 2.
    let assignment = link(&candidates, &[], Uuid::now_v7);
    assert_eq!(assignment.stories.len(), 3);
    let cands: Vec<Candidate> = assignment
        .stories
        .iter()
        .map(|s| Candidate {
            id: s.id,
            last_event_time: s.last_event_time,
        })
        .collect();
    let selected = select(cands, 2)
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
        candidate_clusters(&pool, Uuid::nil(), None, 30)
            .await
            .unwrap()
            .is_empty(),
        "unbuilt event is not a candidate"
    );

    build_all(&pool).await;

    // Once clustered it becomes a candidate.
    assert_eq!(
        candidate_clusters(&pool, Uuid::nil(), None, 30)
            .await
            .unwrap()
            .len(),
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

    let selected = select_stories(&pool, sub.id, sub.last_run_at, sub.max_items as usize).await;
    assert_eq!(selected.len(), 2);

    let digest = create_with_items(&pool, sub_id, window_end, &selected)
        .await
        .unwrap();
    assert!(digest.delivered_at.is_none());

    let items = render_items(&mut pool.acquire().await.unwrap(), digest.id)
        .await
        .unwrap();
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
    assert_eq!(
        render_items(&mut pool.acquire().await.unwrap(), again.id)
            .await
            .unwrap()
            .len(),
        2
    );

    // Watermark advanced one cadence; the just-delivered boundary is now last_run_at, and
    // next_run_at snapped to the next 09:00 UTC slot strictly after it (DST-safe wall-clock grid,
    // not a blind +24h).
    let sub = load_subscriber(&pool, sub_id).await.unwrap().unwrap();
    assert_eq!(sub.last_run_at, Some(window_end));
    assert!(sub.next_run_at > window_end);
    assert!(sub.next_run_at <= window_end + Duration::days(1));
    assert_eq!(local_run_time(&pool, sub_id, "UTC").await, nine_am());
}

// Phase 3 isolation: after the public build and each owner's just-in-time private build, a
// subscriber's candidate set is exactly `public ∪ own-private` — a private event reaches only its
// owner, public stays shared, and a subscriber with no private events sees only public clusters.
#[tokio::test]
async fn private_clusters_are_isolated_to_their_owner() {
    let (pool, _pg) = setup().await;

    let alice = insert_subscriber(
        &pool,
        "alice@x.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();
    let bob = insert_subscriber(
        &pool,
        "bob@x.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();
    let carol = insert_subscriber(
        &pool,
        "carol@x.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();

    insert_public(&pool, "pub", "pub", "Public news", 100).await;
    insert_private(&pool, alice, "a-secret", "a-secret", "Alice secret", 200).await;
    insert_private(&pool, bob, "b-secret", "b-secret", "Bob secret", 300).await;

    build_all(&pool).await; // public clusters only
    assert_eq!(build_private(&pool, alice).await.unwrap(), 1);
    assert_eq!(build_private(&pool, bob).await.unwrap(), 1);
    // carol has no private events → nothing to build.
    assert_eq!(build_private(&pool, carol).await.unwrap(), 0);

    // Alice and Bob each see the public cluster + their own private one (2), never the other's.
    assert_eq!(
        candidate_clusters(&pool, alice, None, 30)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        candidate_clusters(&pool, bob, None, 30)
            .await
            .unwrap()
            .len(),
        2
    );
    // Carol sees only the shared public cluster.
    assert_eq!(
        candidate_clusters(&pool, carol, None, 30)
            .await
            .unwrap()
            .len(),
        1
    );

    // The total cluster count is 1 public + 2 private — a private event never lands in a public
    // cluster (scope is part of the cluster identity).
    assert_eq!(cluster_count(&pool).await, 3);
}

// M3 headline: a private GitHub PR and a public advisory naming the same CVE fuse into ONE story in
// the owner's digest, with a link_reason; the story's id is stable across recompute; and the rendered
// digest carries the fused item with its cross-source connection. The exit criteria, end to end.
#[tokio::test]
async fn cross_source_story_fuses_private_and_public_via_cve() {
    let (pool, _pg) = setup().await;
    let alice = insert_subscriber(
        &pool,
        "alice@x.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();

    // A public advisory and Alice's own private PR, both naming CVE-2026-1234 in their titles
    // (so `finalize` mines the same strong `cve:` entity onto each).
    insert_public(
        &pool,
        "adv",
        "adv",
        "Advisory: CVE-2026-1234 disclosed",
        100,
    )
    .await;
    insert_private(
        &pool,
        alice,
        "pr",
        "pr",
        "Fix for CVE-2026-1234 in api",
        200,
    )
    .await;
    build_all(&pool).await;
    assert_eq!(build_private(&pool, alice).await.unwrap(), 1);

    // Alice's candidate set is her private PR + the public advisory; linking fuses them on the CVE.
    let clusters = candidate_clusters(&pool, alice, None, 30).await.unwrap();
    assert_eq!(clusters.len(), 2);
    let assignment = link(&clusters, &[], Uuid::now_v7);
    assert_eq!(
        assignment.stories.len(),
        1,
        "the shared CVE fuses the public advisory and the private PR into one story"
    );
    let story = &assignment.stories[0];
    assert_eq!(story.clusters.len(), 2);
    assert!(
        story
            .clusters
            .iter()
            .all(|c| c.link_reason.as_deref() == Some("shared cve:CVE-2026-1234")),
        "each member records why it linked"
    );

    // Persist, then recompute with the prior assignment: the story id is forwarded (stable).
    persist_assignment(&pool, alice, &assignment).await.unwrap();
    let prior = load_prior_members(&pool, alice).await.unwrap();
    let again = link(&clusters, &prior, Uuid::now_v7);
    assert_eq!(again.stories.len(), 1);
    assert_eq!(
        again.stories[0].id, story.id,
        "the story id stays stable across recompute"
    );
    assert!(again.merges.is_empty());

    // Freeze + render: one digest item (the fused story) carrying one cross-source connection.
    let sub = load_subscriber(&pool, alice).await.unwrap().unwrap();
    let digest = create_with_items(&pool, alice, sub.next_run_at, &[story.id])
        .await
        .unwrap();
    let items = render_items(&mut pool.acquire().await.unwrap(), digest.id)
        .await
        .unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(
        items[0].connections.len(),
        1,
        "the rendered story shows its fused cross-source member"
    );
}

// PrivateBuild is watermark-bounded like the public build: it processes only events ingested since
// the subscriber's last build, so a re-run with nothing new is a no-op and a quiet private cluster's
// updated_at is not bumped (so it ages out of the candidate floor the way a public cluster does).
#[tokio::test]
async fn private_build_is_watermark_incremental() {
    let (pool, _pg) = setup().await;
    let alice = insert_subscriber(
        &pool,
        "alice@x.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();

    insert_private(&pool, alice, "s1", "g1", "First", 100).await;
    // First build (watermark = epoch) clusters the one group...
    assert_eq!(build_private(&pool, alice).await.unwrap(), 1);
    // ...and a re-run with nothing newly ingested is a no-op (watermark advanced past it).
    assert_eq!(build_private(&pool, alice).await.unwrap(), 0);

    // A new private event re-dirties only its own group; the settled one isn't rebuilt.
    insert_private(&pool, alice, "s2", "g2", "Second", 200).await;
    assert_eq!(build_private(&pool, alice).await.unwrap(), 1);
    assert_eq!(cluster_count(&pool).await, 2);
}

// Phase 4 — the DB physically enforces scope isolation. Under the least-privilege runtime role
// (non-superuser, so FORCE ROW LEVEL SECURITY applies), the two-context policies confine every
// `event`/`cluster` query to its scope: the no-subscriber context sees only public rows, a
// subscriber context sees public ∪ own-private and can never read another tenant's private rows or
// write outside its own scope. This is the RLS backstop behind the query-level predicates and the
// typed `Scope` — proven against a real connection as the runtime role, not the test superuser
// (which bypasses RLS even with FORCE, which is exactly why the runtime role must be neither owner
// nor superuser).
#[tokio::test]
async fn rls_isolates_private_content_under_runtime_role() {
    let (pool, pg) = setup().await;

    // The RLS migration created the runtime role; grant it table access and a password so this test
    // can open a second pool *as that role* over TCP (the deployment does the grant in `migrate`).
    grant_runtime_role(&pool).await.unwrap();
    sqlx::query(&format!(
        "ALTER ROLE {RUNTIME_ROLE} WITH LOGIN PASSWORD 'app'"
    ))
    .execute(&pool)
    .await
    .unwrap();

    let alice = insert_subscriber(
        &pool,
        "alice@x.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();
    let bob = insert_subscriber(
        &pool,
        "bob@x.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();

    // Seed as the superuser (bypasses RLS): one public, one private per owner; build all scopes.
    insert_public(&pool, "pub", "pub", "Public", 100).await;
    insert_private(&pool, alice, "a-secret", "a-secret", "Alice secret", 200).await;
    insert_private(&pool, bob, "b-secret", "b-secret", "Bob secret", 300).await;
    build_all(&pool).await;
    build_private(&pool, alice).await.unwrap();
    build_private(&pool, bob).await.unwrap();

    // A second pool, logged in as the non-superuser runtime role → RLS is in force.
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let runtime_url = format!("postgresql://{RUNTIME_ROLE}:app@127.0.0.1:{port}/postgres");
    let app = connect(&runtime_url).await.unwrap();

    // 1. No-subscriber context: only public content is visible.
    {
        let mut tx = begin_scope(&app, ScopeCtx::NoSubscriber).await.unwrap();
        let events: i64 = sqlx::query("SELECT count(*) AS n FROM event")
            .fetch_one(&mut *tx)
            .await
            .unwrap()
            .get("n");
        let clusters: i64 = sqlx::query("SELECT count(*) AS n FROM cluster")
            .fetch_one(&mut *tx)
            .await
            .unwrap()
            .get("n");
        tx.commit().await.unwrap();
        assert_eq!(
            events, 1,
            "no-subscriber context sees only the public event"
        );
        assert_eq!(
            clusters, 1,
            "no-subscriber context sees only the public cluster"
        );
    }

    // 2. Alice's context: public ∪ her own private, never Bob's.
    {
        let mut tx = begin_scope(&app, ScopeCtx::Subscriber(alice))
            .await
            .unwrap();
        let titles: Vec<String> = sqlx::query("SELECT title FROM event ORDER BY title")
            .try_map(|r: sqlx::postgres::PgRow| Ok(r.get::<String, _>("title")))
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(titles.contains(&"Public".to_string()));
        assert!(titles.contains(&"Alice secret".to_string()));
        assert!(
            !titles.contains(&"Bob secret".to_string()),
            "Alice's context cannot read Bob's private event"
        );
    }

    // 3. Directional invariant: Alice's context cannot write a row scoped to Bob.
    {
        let mut tx = begin_scope(&app, ScopeCtx::Subscriber(alice))
            .await
            .unwrap();
        let bob_ev = EventBuilder::new(
            SourceKind::Github,
            "inject",
            Utc.timestamp_opt(400, 0).single().unwrap(),
            "Injected",
            "g",
        )
        .private(true)
        .finalize(Some(bob));
        let res = insert_event(&mut *tx, &bob_ev).await;
        assert!(
            res.is_err(),
            "writing another tenant's private row must be refused by RLS"
        );
        let _ = tx.rollback().await;
    }

    // 4. The no-subscriber context cannot inject a private row at all (no public→private leak path).
    {
        let mut tx = begin_scope(&app, ScopeCtx::NoSubscriber).await.unwrap();
        let priv_ev = EventBuilder::new(
            SourceKind::Github,
            "sneaky",
            Utc.timestamp_opt(500, 0).single().unwrap(),
            "Sneaky",
            "g",
        )
        .private(true)
        .finalize(Some(alice));
        let res = insert_event(&mut *tx, &priv_ev).await;
        assert!(
            res.is_err(),
            "the no-subscriber context cannot write private rows"
        );
        let _ = tx.rollback().await;
    }
}

// Phase 4 (cont.) — the control-plane / delivery tables are RLS'd too, and fail-closed. Under the
// runtime role: the default (no-subscriber) context can't read `subscriber`/`connection`/`digest`
// at all; a subscriber context sees only its own rows; the admin context is the only cross-tenant
// reach. This is what makes the isolation total rather than partial — there's no unpoliced table on
// the path from a private event to a delivered digest.
#[tokio::test]
async fn rls_isolates_control_plane_under_runtime_role() {
    let (pool, pg) = setup().await;
    grant_runtime_role(&pool).await.unwrap();
    sqlx::query(&format!(
        "ALTER ROLE {RUNTIME_ROLE} WITH LOGIN PASSWORD 'app'"
    ))
    .execute(&pool)
    .await
    .unwrap();

    // Seed (as superuser): two subscribers, one connection owned by alice.
    let alice = insert_subscriber(
        &pool,
        "alice@x.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();
    let bob = insert_subscriber(
        &pool,
        "bob@x.com",
        None,
        Recurrence::Daily,
        "UTC",
        nine_am(),
    )
    .await
    .unwrap();
    insert_connection(
        &pool,
        SourceKind::Github,
        serde_json::json!({ "installation_id": 1, "repos": [] }),
        900,
        Some(alice),
        Some("1"),
    )
    .await
    .unwrap();

    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let runtime_url = format!("postgresql://{RUNTIME_ROLE}:app@127.0.0.1:{port}/postgres");
    let app = connect(&runtime_url).await.unwrap();

    async fn count(app: &PgPool, ctx: ScopeCtx, table: &str) -> i64 {
        let mut tx = begin_scope(app, ctx).await.unwrap();
        let n: i64 = sqlx::query(&format!("SELECT count(*) AS n FROM {table}"))
            .fetch_one(&mut *tx)
            .await
            .unwrap()
            .get("n");
        tx.commit().await.unwrap();
        n
    }

    // 1. Fail-closed: the default no-subscriber context sees none of these tables.
    assert_eq!(count(&app, ScopeCtx::NoSubscriber, "subscriber").await, 0);
    assert_eq!(count(&app, ScopeCtx::NoSubscriber, "connection").await, 0);

    // 2. Admin: the cross-tenant control-plane reach the cron sweeps use.
    assert_eq!(count(&app, ScopeCtx::Admin, "subscriber").await, 2);
    assert_eq!(count(&app, ScopeCtx::Admin, "connection").await, 1);

    // 3. A subscriber context sees only its own rows — Alice sees herself + her connection, not Bob.
    {
        let mut tx = begin_scope(&app, ScopeCtx::Subscriber(alice))
            .await
            .unwrap();
        let ids: Vec<Uuid> = sqlx::query("SELECT id FROM subscriber")
            .try_map(|r: sqlx::postgres::PgRow| Ok(r.get::<Uuid, _>("id")))
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            ids,
            vec![alice],
            "Alice's context sees only her own subscriber row"
        );
    }
    assert_eq!(
        count(&app, ScopeCtx::Subscriber(alice), "connection").await,
        1
    );
    assert_eq!(
        count(&app, ScopeCtx::Subscriber(bob), "connection").await,
        0,
        "Bob owns no connection and can't see Alice's"
    );
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
