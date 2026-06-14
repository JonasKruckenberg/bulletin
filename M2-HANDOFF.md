# M2 Implementation Handoff

**Purpose.** M2 is being built in five reviewable phases, each in its own session. Phase 1 is
merged in this PR. This doc carries the full plan, the locked decisions, the codebase orientation,
and the per-phase implementation detail so a *fresh session with no prior memory* can execute
Phases 2–5 faithfully. Read this top-to-bottom before starting a phase.

**Reads against:** `IMPLEMENTATION-ROADMAP.md` (§M2), `digest-system-design.md` (product: §4 scopes,
§6 data model, §7 ingress/webhooks, §8 aggregation, §9 generation, §12 security), and
`digest-technical-architecture.md` (runtime: §2 topology, §3 ingestion, §3A auth/webhooks, §5.1/5.3/5.4
type modeling, §6 cross-cutting).

---

## 1. What M2 is

> **Goal (roadmap §M2):** add GitHub as a second source exercising **webhooks**, the
> **poll-reconciliation backstop**, and **private scope** (private repos) — and make it **safe to
> point at your own real account**: private data is DB-isolated by RLS and credentials are encrypted
> at rest.

**M2 exit criteria (the demo):** push to a watched public repo → appears in the next digest via
webhook; drop the webhook → still appears via the reconciliation poll (fingerprint collapses the
overlap). A private repo's events appear **only** in the owner's digest *and* are DB-isolated (a
mis-scoped query under the runtime role returns nothing; the scope-invariant property test passes).
The GitHub App key is never stored or logged in plaintext.

**Not in M2** (deferred): OAuth `/connect` flow (M5), managed-KMS backend (M5), SSRF guard (M5, while
only the operator adds feeds), Slack (M6), per-subscriber linking + the `story` table (M3),
relevance/feedback (M4).

---

## 2. Locked cross-cutting decisions (do not relitigate)

| Decision | Choice | Why |
|---|---|---|
| **Delivery** | 5 phases, commit each, **pause for review between phases** | user preference |
| **GitHub client** | **hand-rolled** on `reqwest` + (later) `jsonwebtoken` + `hmac` | tiny API surface (1 token POST, a few conditional GETs, RS256 JWT); avoids the heavy `octocrab` dep, matches hand-rolled RSS |
| **GitHub realism** | **plumbing now, secrets later** | full machinery built + fixture-tested; operator creates the real App / hand-seeds the install + secrets afterward. No live secrets needed during implementation |
| **RLS roles** | **two roles, two connection strings** (owner/migration role owns DDL; app logs in as a separate non-owner role, no `BYPASSRLS`; `FORCE ROW LEVEL SECURITY`; tenant ctx via `SET LOCAL app.subscriber_id` through a `with_scope()` wrapper) | strong privilege boundary at the *credential* level — `SET ROLE` on one elevated connection is defeated by `RESET ROLE`/injection. Matches design §12 prereqs |
| **GitHub event set** | **capture everything, classify in one legible place** | one `event_map` module; known types rich, unknown captured generically; add/reclassify by editing one file (per user) |
| **Scope assignment** | adapter emits a structural `is_private` bool (from `repo.private`); **`finalize` owns the subscriber binding** and maps it to `Scope` | keeps design §12 risk-#1 invariant: an adapter can never name a subscriber or construct a `Scope` |
| **Crate graph** | stays `core` + `bulletin` for all of M2 | design §4 says revisit at end of M2; `core` already isn't I/O-pure (uses `reqwest`). Reconsider a `connectors`/`store`/`support` split as a closing M2 note, don't act mid-milestone |
| **GitHub event scope** | **timeline collaboration set only** (§11.2); **defer all non-timeline signals** — security alerts, CI/CD, org/admin, packages, projects, discussions — to a later milestone. **Keep installation-lifecycle** webhooks (control-plane, not a content signal) since `connection.status` depends on them (roadmap §M2) | user choice; §11 is the reference map for the milestone that adds the rest. `event_map` still captures unknown webhook types generically, so nothing breaks if one arrives |

---

## 3. Codebase orientation (current state, post-Phase-1)

**Layout.** Cargo workspace: `crates/core` (`bulletin-core`, the domain + flows) and
`crates/bulletin` (the `bulletin` binary: clap roles, apalis worker, axum serve, debug CLI).

**Roles** (`crates/bulletin/src/main.rs`): `serve` (axum — currently only `/health`), `worker`
(apalis `Monitor` + cron tick + 3 processing workers), `migrate` (sqlx migrations + apalis storage),
`all` (serve+worker, dev), `debug …` (`crates/bulletin/src/debug.rs`: seed/list connections &
subscribers, run stages inline, status).

**The tick & jobs** (`crates/bulletin/src/worker.rs`): one cron (`"0 * * * * *"`) is the sole
enqueuer; three due-sweeps push `PollConnectionJob` / `PublicBuildJob` / `GenerateDigestJob`.
apalis pins are RC (`=1.0.0-rc.9` / `-rc.8`); `unique-jobs` for idempotent enqueue. The sweep
advances nothing — the processing job advances its own watermark (self-healing).

**Flows** (`crates/core/src`): `ingest` (poll → normalize → append to `event` log),
`cluster` (drain event log into `cluster` rows via the build watermark), `digest`
(lookback select → freeze → render → deliver, advancing the subscriber schedule). `common` holds
shared vocab (`event`, `kind`, `scope`, `fingerprint`, `db`, `status`).

**Connector model (Phase 1).** `ingest::Connection` trait (RPITIT `poll` + pure `to_events ->
Vec<EventBuilder>`). `ingest::ConnDispatch` enum (`Rss`, `Github`) is the hand-written dispatch; a
generic `poll_inner` erases the cursor to JSON inside each arm. GitHub lives in
`ingest/github/{mod,event_map,token}.rs`. `ConnectorCtx { github: Option<GithubCtx> }` is the
app-level seam threaded `main.rs → worker → ingest::poll`.

**Event identity.** `EventBuilder::finalize(scope) -> NewEvent` stamps `scope` + the SHA-256
`Fingerprint` over `(source, stable_id)` (length-framed, content **not** folded in). Connectors
never set scope or fingerprint. `UNIQUE(fingerprint)` + `ON CONFLICT DO NOTHING` = idempotent
ingest.

**Scope.** `scope::Scope { Public, Private(Uuid) }`. The `event` table already has
`scope_kind` + `scope_subscriber_id` (+ CHECK). **`cluster` has no scope columns yet** (public-only)
— Phase 3 adds them. `connection` has **no owning `subscriber_id` yet** — Phase 3 adds it.

**DB / migrations.** `common/db.rs`: `connect` = plain `PgPool::connect`; `migrate` =
`sqlx::migrate!("./migrations")` with `ignore_missing = true`. Migrations are **append-only**
(sqlx checksums them — never edit an applied file; fix forward), additive/expand-contract. Requires
**PostgreSQL 18** (built-in `uuidv7()`). No RLS, no role separation, no `with_scope` yet.

**Test harness.** Integration tests use `testcontainers` + `postgres:18-alpine` and connect as the
**`postgres` superuser** (see `tests/pipeline.rs::setup`). Pure tests (no DB) live in `#[cfg(test)]`
mods + `tests/{rss,github}.rs`. **Docker is required** for the DB-backed suites
(`pipeline`/`poll_rss`/`connection`/`event`) — they may not run in every sandbox; always run
`clippy --all-targets` (compiles them) + the pure suites, and note if Docker was unavailable.

**Dep facts.** `reqwest` is `default-features=false, features=["rustls-tls"]` — Phase 1 uses
`serde_json::from_slice` on response bytes (no `json` feature needed). Already transitive in the
lockfile: `hmac`, `subtle`, `hex`, `ring`, `zeroize`. **Not yet present:** `secrecy`,
`chacha20poly1305`, `jsonwebtoken`, `base64`. `cargo-deny` (advisories+licenses+bans) runs in CI on
every PR — verify new deps' licenses.

**Commands.** `cargo clippy --workspace --all-targets`; `cargo fmt`; pure tests:
`cargo test -p bulletin-core --lib --test rss --test github`; full (Docker): `cargo test --workspace`
or `cargo nextest run`.

---

## 4. Phase status

| Phase | Scope | Status |
|---|---|---|
| **1** | Connector trait family seam, `ContentKind`, GitHub poll, `ConnDispatch` | ✅ merged (commit `6236abd`) |
| **2** | Webhook catcher + `ProcessWebhook` job + HMAC verify + realtime traits | ✅ implemented (branch `claude/m1-phase-2-c1tvgu`) |
| **3** | Private scope load-bearing + per-subscriber private clusters + scope-invariant proptest | ✅ implemented (branch `claude/m2-phase-3-s5ewh9`) |
| **4** | Two-context RLS (two roles, two URLs) | ✅ implemented (branch `claude/m2-phase-4-reso5w`) |
| **5** | Credential-at-rest (interim XChaCha20-Poly1305 envelope) + real GitHub token minting | ✅ implemented (branch `claude/m2-phase-5-milestone-vzqeji`) |

---

## 5. Phase 1 — DONE (context for later phases)

**Landed:** `ContentKind { Message, Announcement, Longform }` (ordered) threaded connector →
`EventBuilder` → `NewEvent`/`Event` → DB (replaced the hardcoded `'longform'`); `ConnDispatch`
enum + generic `poll_inner`; GitHub `Connection` (REST events-feed reconciliation, per-repo
conditional GET + last-seen-id high-water mark); the legible `ingest/github/event_map.rs`;
`TokenProvider` port + `StaticTokenProvider`; `ConnectorCtx`/`GithubCtx` seam.

**Seams deliberately left for later phases — wire these, don't rebuild them:**
- **`ConnectorCtx.github == None`** in `main.rs::connector_ctx()`. Phase 5 sets it to a real
  `GithubCtx { base_url, token_factory }` once the App key is sealed/loaded. Until then GitHub
  connections skip with a logged `BuildError::NotConfigured`.
- **`ingest::poll` finalizes every event `Scope::Public`.** Phase 3 makes this per-event
  (visibility-aware) — see §7.
- **App-level traits `Connector` / `RealtimeConnector` (`verify`/`route`) were intentionally NOT
  added** (would be dead code in Phase 1). Add them in Phase 2 where the catcher uses them.
- **`event_map::stable_id` already uses content-identity** (issue/PR/release/comment ids, push head
  SHA) precisely so a Phase 2 webhook for the same activity dedups against the poll's event. The
  webhook payload's nested objects (`issue.id`, `comment.id`, `release.id`, `after` for push) carry
  the same values — Phase 2 must feed them through the same `stable_id`/`to_builder`.

---

## 6. Phase 2 — Webhook catcher + `ProcessWebhook`

> **Status: implemented** (branch `claude/m1-phase-2-c1tvgu`). What landed, vs. the plan below:
> - **Realtime traits** in `ingest/realtime.rs`: `RealtimeConnection: Connection`
>   (`accept_webhook(event_type, delivery_id, body)` + `hydrate` default-identity), app-level
>   `RealtimeConnector` (`verify`; the credential-free routing peek is the free `realtime::route`
>   fn), `Inbound<I>`, `Verified`, `LifecycleStatus`/`LifecycleChange`, and
>   `RealtimeDispatch { Github(..) }` (**no RSS arm**).
> - **GitHub realtime head**: `ingest/github/webhook.rs` (`GithubWebhook` HMAC-SHA256 verify over raw
>   bytes via `hmac`'s constant-time `verify_slice`; `route` peeks `installation.id`); `event_map`
>   gained `from_webhook` (synthesizes a `GithubEvent` whose `payload` *is* the webhook body, so it
>   reuses the poll's `stable_id`/`group_key`/`to_builder` → dedup-for-free) + `lifecycle_status`;
>   `GithubConnection::realtime_only()` (token-less worker, since accept needs no token) backed by
>   `token::UnavailableToken`.
> - **Catcher** in `crates/bulletin/src/webhook.rs` (`POST /webhooks/github`): verify → `ping`→200 →
>   enqueue `ProcessWebhookJob` (idempotency key `gh-webhook:<delivery>`) → 202. `serve` now takes a
>   `PgPool` + the webhook secret (`--github-webhook-secret` / `BULLETIN_GITHUB_WEBHOOK_SECRET`,
>   fail-closed when absent).
> - **`ProcessWebhook`** flow `ingest::process_webhook` (route → `resolve_connection_by_provider`
>   (IDOR) → accept/normalize → dedup insert / `update_connection_status`); worker handler +
>   `bulletin-process-webhook` registration.
> - **Migration** `…011_webhook_routing.sql`: `connection.provider_account_id` + partial
>   `UNIQUE(source, provider_account_id)` + `creds_ref` (NULL for now; status is free text, so
>   'suspended'/'revoked' need no constraint change).
> - **Seams left for later phases:** event scope is still uniformly `Scope::Public` in
>   `process_webhook` (Phase 3 makes it per-event from the resolved connection's owner — the webhook
>   body already carries `repository.private`); the webhook secret is a plain config value (Phase 5
>   seals it at rest + wires real token minting); webhook-ingested events reuse the `poll_result`
>   ingest counters.
> - **Tests:** pure (`tests/github.rs`) — webhook↔poll fingerprint dedup (issue + push), HMAC verify
>   (valid/tampered/wrong-secret/no-secret/malformed), routing, lifecycle/dispatch. DB-backed
>   (`tests/webhook.rs`, needs Docker) — resolve, ingest, dedup-vs-poll, unrouted drop, revoke.
>   `clippy --all-targets` clean; Docker was unavailable in the build sandbox so the DB suites
>   compiled but did not run.

**Goal:** GitHub webhooks ingest in real time; a dropped webhook is recovered by the Phase 1 poll
(fingerprint collapses the overlap).

**Build:**
1. **Realtime traits** (in `ingest`, design §5.4): `RealtimeConnection: Connection` with
   `accept_webhook(...) -> Result<Inbound<Self::Item>, SourceError>` and `hydrate(item) -> item`
   (default identity); `Inbound<I> { Events(Vec<I>), Lifecycle(LifecycleChange) }`. App-level:
   `RealtimeConnector` with `verify(headers, body) -> Verified` (HMAC over raw bytes, app secret,
   **constant-time**) and `route(body) -> Result<String>` (credential-free peek of `installation.id`).
   `Verified { Authentic, Challenge(Vec<u8>), Invalid }`. Add a **`RealtimeDispatch { Github(...) }`**
   enum — **no RSS arm**, so "webhook routed to a pull-only source" is uncompilable.
2. **Catcher** in `serve` (`crates/bulletin/src/main.rs` — `serve_health` becomes a real router):
   `POST /webhooks/github` → verify `X-Hub-Signature-256` (`sha256=`, app webhook secret, constant
   time) over the **raw body bytes** → on GitHub `ping` reply 200 → else enqueue
   `ProcessWebhook { source, raw_body, delivery_id, event_type }` → return 2xx fast. Unverified →
   401 drop *before* enqueue. The `serve` role now needs a `PgPool` (for the apalis storage handle)
   + the webhook secret + `ConnectorCtx` — thread them in (today `serve_health` takes only an addr).
3. **`ProcessWebhook` job** (`worker.rs` + a `core` flow): `route` (peek `installation.id`) → resolve
   our `connection` by `(source, provider_account_id)` — **IDOR defense: derive subscriber+scope
   from OUR row, never the payload** → `accept_webhook` → `Inbound::Events` → `hydrate` (identity) →
   `to_events` → `finalize` → dedup insert; `Inbound::Lifecycle` → update `connection.status`.
   `unique-jobs` idempotency key = `X-GitHub-Delivery`.
4. **Migration** (additive): `connection.provider_account_id text` + `UNIQUE(source,
   provider_account_id)` (the webhook routing key — installation_id, **not a secret**);
   `connection.creds_ref text NULL`; allow `status = 'revoked'`. Add a store fn
   `resolve_connection_by_provider(source, provider_account_id)`.

**Key wrinkle to decide:** the GitHub **event type is in the `X-GitHub-Event` header, not the body**,
but the design's `accept_webhook(body)` takes body only. Resolution (recommended): carry
`event_type` (+ `delivery_id`) in the `ProcessWebhook` payload and either pass it into a
GitHub-specific `accept_webhook(event_type, body)` or synthesize a `GithubEvent` in the job from
`{type: map(X-GitHub-Event) → "IssuesEvent"/…, repo: repository.full_name, payload: <whole body>,
created_at}` and reuse the existing `event_map::to_builder`. The webhook payload's nested ids line up
with `stable_id`, so dedup-with-poll works for free.

**Exit:** a signed fixture webhook ingests; a bad signature → 401; an item delivered by webhook and
then re-seen by the reconciliation poll produces exactly one event (fingerprint collapse). Tests:
HMAC verify (good/bad/constant-time), `accept_webhook` over recorded GitHub webhook fixtures,
poll↔webhook dedup, lifecycle → status.

---

## 7. Phase 3 — Private scope load-bearing + per-subscriber private clusters

> **Status: implemented** (branch `claude/m2-phase-3-s5ewh9`). What landed, vs. the plan below:
> - **Migration** `…012_private_scope.sql`: `cluster` gains `scope_kind` + `scope_subscriber_id`
>   (+ CHECK); `cluster_identity` replaced with `UNIQUE NULLS NOT DISTINCT (scope_kind,
>   scope_subscriber_id, source, group_key)` (the `NULLS NOT DISTINCT` is load-bearing — without it
>   the public upsert's `ON CONFLICT` never fires); `cluster_scope_recency` index; `connection`
>   gains owning `subscriber_id uuid NULL REFERENCES subscriber(id) ON DELETE CASCADE`.
>   Follow-up `…013_scoped_fingerprint.sql`: `event` dedup key widened to
>   `UNIQUE NULLS NOT DISTINCT (fingerprint, scope_kind, scope_subscriber_id)` — the fingerprint
>   stays pure content identity (poll↔webhook still collapse within a scope), but two owners over the
>   *same* private repo no longer cross-tenant-collide (the global `UNIQUE(fingerprint)` would have
>   dropped the second owner's event). `insert_event` conflicts on the constraint by name.
> - **Visibility-aware finalize**: `EventBuilder` gained `is_private` + `.private(bool)`;
>   `finalize(scope)` became `finalize(owner: Option<Uuid>)` mapping `(is_private, owner)` →
>   `Scope` (`private + owner → Private(owner)`, else `Public`). `ingest::poll` and
>   `process_webhook` finalize per-event with the connection's `subscriber_id` (from OUR row, never
>   the payload). The `(private, None)` case is made **unreachable** rather than fail-open:
>   `…014_connection_owner.sql` adds `CHECK (source = 'rss' OR subscriber_id IS NOT NULL)` and
>   `SourceKind::can_emit_private` gates `connection-add`, so a private-capable source is always
>   owned (FK CASCADE keeps it owned for life).
> - **GitHub visibility**: `repos_to_poll` returns `RepoTarget { name, private }` (discovery reads
>   `/installation/repositories`'s `private`; an allowlist can't, so it reports `private=false`);
>   `GithubEvent` deserializes the feed's per-event `public` flag (default `true`) and `poll` folds
>   the repo-list privacy onto each event (never a downgrade); `from_webhook` sets `public` from
>   `repository.private`; `to_builder` passes `.private(!public)`.
> - **PrivateBuild**: `cluster::build_private(pool, subscriber_id)` — a per-subscriber mirror of the
>   public `build`: a txn holding a per-subscriber advisory lock, the half-open `(built_through,
>   now()]` range from `…015_private_build_watermark.sql` (keyed per subscriber), recompute dirtied
>   groups, upsert, advance the cursor. So it scales with *new* private activity (not lifetime
>   history) and a quiet private cluster ages out of the candidate floor like a public one.
>   `GenerateDigest` (and `dispatch_now`) call it before selecting. The public and private builds
>   share their code: the group-discovery query (`dirty_groups(scope, lo, hi)`), the per-group
>   loader (`list_group_events(scope, …)`), and the rollup→upsert loop (`build_groups`) are all
>   scope-parameterized (via `scope.to_columns()` + `scope_subscriber_id IS NOT DISTINCT FROM`); only
>   the lock/bounds/watermark differ. `upsert_cluster` is scope-aware too. `candidates_in_lookback`
>   takes a `subscriber_id` and filters `scope_kind = 'public' OR scope_subscriber_id = $1` (the
>   isolation boundary) — `explain` is scope-aware too but stays no-writes (doesn't build private).
>   `…016_candidate_lookback_index.sql` replaces the Phase-3 `cluster_scope_recency` index (keyed on
>   last_event_time, which served neither the `updated_at` floor nor the scope OR) with two
>   predicate-aligned indexes the planner bitmap-ORs: a partial `(updated_at) WHERE public` and a
>   `(scope_subscriber_id, updated_at)`.
> - **Scope-invariant proptests** (pure, `cluster::tests`): public build never clusters a private
>   event (scope is part of `ClusterKey`); a subscriber's candidate set (`visible_to`) never holds
>   another subscriber's private cluster. Plus DB-backed tests (Docker): `pipeline.rs`
>   per-owner isolation, `webhook.rs` private-repo → owner scope, `github.rs` visibility→scope.
> - **Debug**: `debug connection-add --owner <subscriber>` binds a connection to its owner.
> - **Seams left for later phases:** isolation is enforced at the *query* layer (the
>   `scope_subscriber_id` predicates) — Phase 4 makes it DB-enforced via RLS so it holds against a
>   logic bug. `cluster_entities` GIN (blocking) is still M3. (The earlier "no private watermark" and
>   "ownerless-private fail-open" seams are now closed — see migrations 014/015.)
> - **Tests:** `clippy --all-targets` clean; pure suites pass; Docker was unavailable in the build
>   sandbox so the DB suites (`pipeline`/`webhook`/`event`/`connection`/`poll_rss`) compiled but did
>   not run.

**Goal:** private-repo events reach only their owner's digest; public stays shared.

**Build:**
1. **Migration** (additive): `cluster` gains `scope_kind` + `scope_subscriber_id`; replace
   `cluster_identity` with `UNIQUE(scope_kind, scope_subscriber_id, source, group_key)`; add a
   scope-aware recency index. `connection` gains owning `subscriber_id uuid NULL` (null = global/
   public source like RSS). (`cluster_entities` GIN for blocking is M3, not now.)
2. **Visibility-aware finalize:** add `is_private: bool` to `EventBuilder` (connector sets via e.g.
   `.private(bool)`); change `finalize` so infra maps `(is_private, connection.subscriber_id)` →
   `Scope` (`private + owner → Private(owner)`, else `Public`). The adapter still only reports a
   bool — it never constructs a `Scope` or names a subscriber (design §12 risk #1). Update
   `ingest::poll` to finalize per-event with the connection's owner instead of the uniform
   `Scope::Public`.
3. **GitHub visibility:** `repos_to_poll` must return per-repo `private` (from
   `/installation/repositories`, which has the `private` field) and tag each event; pass it into
   `event_map::to_builder`. (Phase 2 webhook payloads carry `repository.private` directly.)
4. **private-build inside `GenerateDigest`** (`digest/mod.rs`, design §9.1): build the subscriber's
   private clusters just-in-time, then `candidates = public ∪ own-private`. `PublicBuild` stays
   public-only. Make `cluster::store::candidates_in_lookback` (and the build queries) scope-aware
   (take a subscriber_id).
5. **Scope-invariant property test** (pure, in `core`): for any mixed public/private event set, the
   public build never places a private event into a public cluster, and a subscriber's candidate set
   never contains another subscriber's private cluster. This is a **primary** isolation defense
   alongside the typed `Scope`.

**Exit:** a private-repo fixture event appears only in its owner's digest; public events still
shared; the proptest passes.

---

## 8. Phase 4 — Two-context RLS (two roles, two URLs)

> **Status: implemented** (branch `claude/m2-phase-4-reso5w`). What landed, vs. the plan below:
> - **Migrations** `…019_rls.sql` + `…020_rls_control_plane.sql` (numbered after M3's
>   `017_cluster_entities`/`018_story`; run by the owner role): idempotently creates the non-owner,
>   non-superuser, **no-BYPASSRLS** runtime role `bulletin_app` (a `DO` block that no-ops if a
>   deployment pre-provisioned it — so the owner needn't hold CREATEROLE); `ENABLE`+`FORCE ROW LEVEL
>   SECURITY` on `event` and `cluster` with two-context policies keyed on
>   `nullif(current_setting('app.subscriber_id', true), '')`: SELECT = `public ∪ own-private`; INSERT/
>   UPDATE = a public row only in the no-subscriber context, a private row only as its owner (the
>   directional public→private invariant, DB-enforced). **Table GRANTs are not in the (checksummed)
>   migration** — they're applied by `grant_runtime_role` (re-run every `migrate`) so later schema +
>   the apalis queue schema (created after the domain migrations) are always covered.
> - **`common/db.rs`** gains `ScopeCtx { NoSubscriber, Subscriber(uuid), Admin }` (the `Admin`
>   control-plane variant is the `*` GUC sentinel; added with the control-plane migration), `set_scope` (the
>   `set_config('app.subscriber_id', $1, true)` chokepoint — transaction-local, pool/PgBouncer-safe),
>   `begin_scope` (a tx pre-pinned to a ctx, for the builds + the self-scoping control-plane store
>   fns), and the canonical **`with_scope(pool, ctx, |conn| …)`** wrapper (commit on Ok / rollback on
>   Err). `ScopeCtx::for_scope(&Scope)` maps an event's finalized scope to its write context.
> - **Flows pinned to a context:** PublicBuild → `NoSubscriber`; `build_private`/`generate`/
>   `dispatch_now`/`explain` → `Subscriber(id)` (candidate read, render, cluster-display all run
>   scoped so own-private is visible and no other tenant's is). **Ingest** (`poll` + `process_webhook`)
>   now finalizes all events then `append_scoped` groups them by `ScopeCtx` and commits one txn per
>   context (≤2 per connection: public + the owner) — a public event writes in the no-subscriber
>   context, a private one as its owner, exactly as the write policy demands. `insert_event` and the
>   two `render_items*` fns took an `impl PgExecutor` so they run inside a scoped txn.
> - **Two URLs:** `--database-url` is the runtime role (serve/worker/debug); new
>   `--migration-database-url` / `BULLETIN_MIGRATION_DATABASE_URL` is the owner role (defaults to
>   `--database-url` for single-role dev). `migrate` connects with the owner URL, then runs migrations
>   → `setup_storage` → `grant_runtime_role`.
> - **NixOS:** provisions both roles (owner via `ensureDBOwnership`, `bulletin_app` via `ensureUsers`
>   so the owner needn't hold CREATEROLE), an ident map + a `local` pg_hba line so the one `bulletin`
>   OS user reaches both roles over peer auth; migrate unit uses the owner URL, the service the runtime
>   URL; `database.migrationUrl` option for the external-DB case. README's `Scope` section + deployment
>   notes rewritten.
> - **Verification tests** (`pipeline.rs`, Docker): open a **second pool as `bulletin_app`** (a
>   password set in the test; non-superuser → FORCE RLS bites). `rls_isolates_private_content_…` proves
>   the content tables: no-subscriber sees only public; a subscriber sees `public ∪ own-private`, never
>   another's private; a subscriber can't write another tenant's private row; the no-subscriber context
>   can't write a private row at all. `rls_isolates_control_plane_…` proves the control-plane tables:
>   the no-subscriber context is denied `subscriber`/`connection` (fail-closed, count 0), a subscriber
>   sees only its own rows, and `Admin` is the only cross-tenant reach. Existing DB suites connect as
>   the `postgres` superuser, which **bypasses** RLS even under FORCE — so they keep passing unchanged
>   (and that's *why* the verification tests must use the non-superuser runtime role).
> - **Scope of enforcement — the whole path (the control-plane migration).** An earlier cut policied
>   only `event` + `cluster` and left the control-plane/delivery tables merely granted; that was a
>   partial boundary, and a partial isolation boundary invites reliance on a guarantee that doesn't
>   hold (a `digest`/`digest_item`/`story` *is* "subscriber A's content"). The control-plane migration
>   closes it: FORCE RLS now covers `connection`, `subscriber`, `digest`, `digest_item`,
>   `private_build_watermark`, **and the M3 `story` table** too — the full
>   `event → cluster → story → digest_item → digest → delivery` path plus the build cursor. A **third context,
>   `Admin`** (the `*` sentinel), was added: these control-plane tables are **fail-closed** in the
>   default no-subscriber context (deny), own-only in a subscriber context, and all-rows only in the
>   explicit `Admin` context the cron sweeps / `status` / poll+webhook connection lookups / operator
>   commands opt into. `Admin` deliberately gets **no** extra reach on the *content* tables (still
>   public-only there), so there is no admin backdoor to another tenant's private content — it's
>   readable only in its owner's context. Mechanism: the control-plane store fns self-scope (open
>   their own `begin_scope(Admin|Subscriber(id))` txn), so call sites are unchanged.
> - **Consequences to know:** `status` (run as `Admin`) reports global connection/subscriber/digest
>   counts but **public-only** `event`/`cluster` counts (no admin backdoor to private content);
>   `debug event-list` likewise shows only public events under the runtime role — to inspect a
>   subscriber's private items use `digest-explain`/`digest-run`, which run in that subscriber's
>   context. `build_watermark` (singleton public cursor) and the apalis queue stay un-policied (no
>   per-subscriber data). FK cascade deletes (`subscriber`→`digest`→`digest_item`,
>   `private_build_watermark`) and FK existence checks bypass RLS as Postgres always does for RI.
> - **Seam left for Phase 5:** the webhook signing secret + GitHub App key are still plain config
>   (sealed at rest in Phase 5); `creds_ref` indirection already in place.
> - **Tests:** `clippy --all-targets` clean; `cargo fmt` clean; pure suites pass; **Docker was
>   unavailable in this sandbox**, so the DB suites (incl. the new RLS verification test) compiled but
>   did not run.

**Goal:** the DB physically enforces scope isolation even against a logic bug.

**Build (design §12, tech §6):**
1. **Migration** (run by the **owner/migration role**): create a **non-owner, non-superuser runtime
   role** with **no `BYPASSRLS`**; `FORCE ROW LEVEL SECURITY` on every scoped table (`event`,
   `cluster`, `connection`, `digest`, `digest_item`, and any with subscriber data); policies:
   - **public-build / no-subscriber context** → exposes only `scope_kind='public'`.
   - **subscriber context** (`SET LOCAL app.subscriber_id = $id`) → `scope_kind='public' OR
     scope_subscriber_id = current_setting(...)`, **SELECT-only on public**, writes confined to
     own-private rows.
2. **`with_scope(ctx, …)` wrapper** = the *only* connection path (transaction-scoped `SET LOCAL`,
   pool/PgBouncer-safe). `PublicBuild` runs in the no-subscriber context; `GenerateDigest` (incl.
   private-build) in the subscriber context.
3. **Two URLs:** `migrate` uses the owner connection string; `serve`/`worker` connect as the runtime
   role. Update `main.rs` connect path + config; update the **NixOS module** to provision both
   roles; update the **README** (currently describes a single peer-auth role).
4. **Test harness:** the testcontainers superuser owns DDL; create the runtime role in `setup` and
   open a **second pool as the runtime role**; run flows under it. **Superusers bypass RLS even with
   FORCE** — the runtime role must be non-superuser/non-owner or the test proves nothing. Add a
   verification test: a deliberately mis-scoped query under the runtime role returns nothing.

**Exit:** mis-scoped query under the runtime role returns nothing (RLS verified by integration test).

---

## 9. Phase 5 — Credential-at-rest (interim envelope) + real GitHub tokens

> **Status: implemented** (branch `claude/m2-phase-5-milestone-vzqeji`). What landed, vs. the plan below:
> - **`common/secret.rs`** — the credential primitives. In memory: secrets ride in `secrecy`'s
>   `SecretBox`/`SecretSlice` (redacted `Debug`, zeroize-on-drop, explicit `.expose_secret()`); the
>   `MasterKey` newtype scrubs every stack copy. At rest: **envelope encryption** — `seal(master,
>   plaintext)` mints a fresh random **DEK**, encrypts the plaintext under it, then *wraps* the DEK
>   under the app master key (both legs XChaCha20-Poly1305, fresh 24-byte random nonces). The
>   `SealedSecret` is framed `version ‖ wrap-nonce ‖ wrapped-DEK ‖ payload-nonce ‖ payload`, base64
>   for transport / `creds_ref`. `unseal` unwraps the DEK then opens the payload; the plaintext DEK
>   never escapes the fn. The **DEK-per-secret indirection is the whole point** — swapping the interim
>   master key for a managed-KMS wrap/unwrap (M5) or adding per-connection secrets (Slack, M6) is a
>   *backend* change to the wrap leg, not a re-encryption migration. Pure tests: round-trip (incl.
>   through text + a reloaded key), wrong-key/tamper/short/misversioned rejection, key-length guard.
> - **`ingest/github/app.rs`** — the real two-hop mint. `GithubApp::new(base_url, app_id, pem)` loads
>   the App private key into a `jsonwebtoken` RS256 `EncodingKey`. `installation_tokens(id)` yields a
>   per-installation `GithubAppTokens` (`TokenProvider`) that signs a short-lived app JWT (`iat-60s`,
>   `exp+9m`, `iss=app_id`), exchanges it at `POST /app/installations/{id}/access_tokens`, and
>   **caches** the ~1 h installation token behind a `tokio::Mutex` (re-mints only inside a 60 s expiry
>   skew). Tests (mock REST): one mint per poll then cache reuse, expired-token re-mint, non-PEM
>   rejection — the seam that flips `ConnectorCtx.github` from `None` to a live factory.
> - **Binary wiring (`secrets.rs` + `main.rs`)** — a flattened `SecretConfig` resolves the at-rest
>   secrets once at startup: `connector_ctx()` unseals the App key (needs `--github-app-id` +
>   `--github-app-private-key` + `--master-key`) and builds the `GithubCtx { base_url, token_factory =
>   |id| app.installation_tokens(id) }`; `webhook_secret()` prefers the sealed
>   `BULLETIN_GITHUB_WEBHOOK_SECRET_SEALED` (unsealed) over the plaintext dev fallback and feeds the
>   Phase-2 edge verifier. With no App configured the ctx stays `github = None` (RSS unaffected, GitHub
>   skips with a log) — the Phase-1 seam, now closeable by config alone. A new offline
>   **`bulletin secrets`** command group (`keygen` → a base64 master key; `seal` → seal stdin under the
>   master key) produces the config blobs and needs no database (`--database-url` became optional).
> - **Deps:** `secrecy`, `zeroize`, `chacha20poly1305`, `jsonwebtoken`, `base64` (licenses all
>   MIT/Apache-2.0/ISC — in the `deny.toml` allow set; `ring`/`base64`/`zeroize` were already transitive).
> - **Seams left for M5 (not this milestone):** the managed-KMS backend (swap the master-key wrap leg
>   for `aws-sdk-kms`, key never in-process), the OAuth `/connect` install flow (operators still
>   hand-seed `connection` rows + the sealed App key), and per-connection `creds_ref` population (Slack,
>   M6) — all *backend* changes the envelope indirection already accommodates.
> - **Tests:** `clippy --all-targets` clean; `cargo fmt` clean; pure suites pass (secret round-trips +
>   the App-token mock); `secrets keygen`/`seal` exercised by hand. Docker-backed suites are unchanged
>   by this phase; Docker was unavailable in the sandbox so they compiled but did not run.

**Goal:** secrets encrypted at rest; GitHub token minting becomes real.

**Build (design §6 Secrets, tech §6):**
1. **In memory:** `secrecy` + `zeroize` — redacted `Debug`, zeroized on drop, explicit
   `.expose_secret()`.
2. **At rest:** interim **envelope encryption** — a single app **master key** (sealed file/env,
   e.g. `BULLETIN_MASTER_KEY`, 32 bytes) + **XChaCha20-Poly1305** (`chacha20poly1305` crate) over
   secrets; store only `creds_ref` + a wrapped DEK, plaintext DEK only in a `SecretBox`. Keep the
   `creds_ref` + wrapped-DEK **indirection** so a managed-KMS swap (M5) and per-connection secrets
   (Slack, M6) are later *backend* changes, not migrations.
3. **GitHub secrets become real:** seal the **App private key** + **webhook signing secret** at rest,
   loaded once at startup. Implement `GithubAppTokens` (`jsonwebtoken` RS256 app JWT, iat/exp ≤10m →
   `POST /app/installations/{id}/access_tokens`, cache per connection) and set
   `ConnectorCtx.github = Some(GithubCtx{ base_url: DEFAULT_API_BASE, token_factory })`. Wire the
   webhook secret into the Phase 2 `verify`.
4. **Deps:** add `secrecy`, `zeroize`, `chacha20poly1305`, `jsonwebtoken`, `base64`. Run `cargo-deny`.

**Exit:** App key / webhook secret never stored or logged in plaintext; seal/unseal round-trips;
a real (operator-seeded) GitHub install ingests end-to-end via both webhook and poll.

---

## 10. Per-phase workflow

1. Branch off this PR's merge base (or the branch a given session is assigned).
2. Implement the phase; keep migrations append-only/additive; match the surrounding style (doc
   comment density, naming).
3. `cargo fmt`; `cargo clippy --workspace --all-targets` (must be clean); run pure tests always +
   DB-backed tests if Docker is present (else note it).
4. Commit with a `M2 Phase N: …` subject + a body explaining the *why* and the seams left/closed.
5. Push and **pause for review** before the next phase.

**Open question to settle at end of M2** (roadmap §5): revisit crate-graph names/granularity
(`connectors`/`store`/`support` split) now that real deps exist; group-key/near-dup tuning for
GitHub.

---

## 11. Appendix — GitHub event surface map (repo / org / user / enterprise)

> Catalog is **as of early 2026** — GitHub adds event types regularly; **re-verify against the live
> "Webhook events and payloads" + "REST Activity/Events" docs when configuring the App.** The point
> of this map is to scope deliberately, not to hardcode.

### 11.1 The two intakes have different coverage (the crux)

- **Activity timeline** (`GET /repos/{o}/{r}/events`, `/orgs/{org}/events`, `/users/{u}/events`,
  `/networks/{o}/{r}/events`) — what Phase 1's poll reads. Carries only the **~17 timeline types**
  below. Public events only for public actors; an installation token widens repo visibility.
- **Webhooks** — the full **~70+ type** catalog (header `X-GitHub-Event`). Most types are **not on
  the timeline**, so they arrive *only* by webhook.
- **Resource REST endpoints** — for non-timeline signals (alerts, checks, runs, deployments…),
  each has its own list endpoint. **These are what a reconciliation poll must hit** to keep the
  "poll is the correctness floor; webhooks are freshness" invariant (§7.2/§5.4) for that signal.

**Consequence:** `event_map` already *captures* any webhook type generically (so nothing is dropped
once a webhook arrives), but (a) **rich** classification and (b) **poll reconciliation** for a
non-timeline signal are per-signal work: subscribe the webhook type **and** add a paired REST
fetcher to `GithubConnection::poll`, **and** request the App permission.

### 11.2 Timeline types (poll-visible today via `/events`)

`CommitCommentEvent`, `CreateEvent` (branch/tag), `DeleteEvent`, `ForkEvent`, `GollumEvent` (wiki),
`IssueCommentEvent`, `IssuesEvent`, `MemberEvent`, `PublicEvent` (repo made public),
`PullRequestEvent`, `PullRequestReviewEvent`, `PullRequestReviewCommentEvent`,
`PullRequestReviewThreadEvent`, `PushEvent`, `ReleaseEvent`, `SponsorshipEvent`, `WatchEvent`.
Phase 1 maps Issues/PR/Release/Push/comments richly; the rest fall through to the generic capture.

### 11.3 Webhook catalog by scope (T = also on the timeline → poll-visible without a new endpoint)

**Repo — collaboration & content:** `push`(T) · `pull_request`(T) · `pull_request_review`(T) ·
`pull_request_review_comment`(T) · `pull_request_review_thread`(T) · `issues`(T) · `issue_comment`(T) ·
`sub_issues` · `commit_comment`(T) · `create`(T) · `delete`(T) · `fork`(T) · `gollum`(T) · `release`(T) ·
`discussion` · `discussion_comment` · `label` · `milestone` · `watch`(T) · `star` · `public`(T) ·
`member`(T) · `page_build` · `status`.

**Repo — CI/CD & automation (webhook-only; reconcile via Actions/Checks/Deployments REST):**
`check_run` · `check_suite` · `workflow_run` · `workflow_job` · `workflow_dispatch` ·
`repository_dispatch` · `deployment` · `deployment_status` · `deployment_review` ·
`deployment_protection_rule` · `merge_group` · `registry_package` · `package`.

**Repo — security & policy (webhook-only; reconcile via the alert REST endpoints in §11.4):**
`dependabot_alert` · `code_scanning_alert` · `secret_scanning_alert` ·
`secret_scanning_alert_location` · `secret_scanning_scan` · `security_advisory` ·
`security_and_analysis` · `repository_vulnerability_alert` (deprecated → `dependabot_alert`) ·
`branch_protection_rule` · `branch_protection_configuration` · `repository_ruleset` · `deploy_key`.

**Repo — admin/meta (webhook-only):** `repository` (created/deleted/archived/renamed/transferred/
publicized/privatized) · `repository_import` · `repository_ruleset` · `meta` (hook deleted) ·
`team_add` · `custom_property_values`.

**Project management:** `projects_v2` · `projects_v2_item` · `projects_v2_status_update` (org) ·
`project`/`project_card`/`project_column` (classic, deprecated).

**Org-level:** `organization` (member added/removed/renamed/deleted) · `membership` · `team` ·
`org_block` · `personal_access_token_request` · `custom_property`/`custom_property_values` ·
`repository` (org repos) · `projects_v2*` · `repository_ruleset`.

**App / installation lifecycle (webhook-only — must drive `connection.status`, Phase 2):**
`installation` (created/deleted/suspend/unsuspend/new_permissions_accepted) ·
`installation_repositories` (added/removed) · `installation_target` · `github_app_authorization`.

**Account / marketplace / sponsors / global:** `marketplace_purchase` · `sponsorship`(T) ·
`security_advisory` (global GitHub Advisory DB feed).

**Enterprise-level:** enterprise webhooks receive most repo/org events across all orgs plus
enterprise-scoped security (`dependabot_alert`/`secret_scanning_alert` enterprise-wide), `audit`,
`organization`, `team`, `membership`, `repository`.

### 11.4 REST endpoints for poll reconciliation of non-timeline signals (high value)

| Signal | Repo | Org / Enterprise | App permission (read) |
|---|---|---|---|
| Dependabot alerts | `/repos/{o}/{r}/dependabot/alerts` | `/orgs/{org}/dependabot/alerts`, enterprise | Dependabot alerts |
| Code scanning | `/repos/{o}/{r}/code-scanning/alerts` | `/orgs/{org}/code-scanning/alerts` | Code scanning alerts |
| Secret scanning | `/repos/{o}/{r}/secret-scanning/alerts` | `/orgs/{org}/…`, enterprise | Secret scanning alerts |
| Repo advisories | `/repos/{o}/{r}/security-advisories` | — | Repo advisories |
| Global advisories | `/advisories` (GitHub Advisory DB) | — | none (public) |
| Workflow runs | `/repos/{o}/{r}/actions/runs` | — | Actions |
| Check runs/suites | `/repos/{o}/{r}/commits/{ref}/check-runs` | — | Checks |
| Deployments | `/repos/{o}/{r}/deployments` (+ statuses) | — | Deployments |
| Commit statuses | `/repos/{o}/{r}/commits/{ref}/statuses` | — | Commit statuses / Contents |
| Packages | user/org packages | `/orgs/{org}/packages` | Packages |
| Discussions | GraphQL only (no REST list) | — | Discussions |

### 11.5 M2 tiering — DECIDED: "timeline only"

**Decision (2026-06-13):** M2 ingests **only the timeline collaboration set** (§11.2 — the types the
Phase-1 poll already reads, rich-mapped in `event_map`). **All non-timeline signals are deferred** to
a later milestone: security alerts (Dependabot/code-scanning/secret-scanning/advisories), CI/CD
(`workflow_run`/`check_*`/deployments/status), org/admin/meta, packages, projects_v2, discussions.
So **no new REST reconciliation endpoints** and **no extra App permissions** are added in M2; the
GitHub poll stays the `/events` walk built in Phase 1.

**Phase 2 consequence — webhook subscriptions:** subscribe **only** the timeline-corresponding
content events (`issues`, `issue_comment`, `pull_request`, `pull_request_review`,
`pull_request_review_comment`, `pull_request_review_thread`, `push`, `release`, `commit_comment`,
`create`, `delete`, `fork`, `gollum`, `member`, `public`, `watch`) **plus the installation-lifecycle
events** (`installation`, `installation_repositories`, `installation_target`,
`github_app_authorization`) — the latter are control-plane and drive `connection.status`, not digest
content, so they stay despite "timeline only." Any other webhook type that arrives is still captured
generically by `event_map` (harmless), but we don't subscribe to or reconcile it.

**When the deferred signals land (future milestone):** for each, (1) subscribe its webhook type,
(2) add its REST list endpoint (§11.4) to `GithubConnection::poll` for reconciliation parity, (3)
request the App permission, (4) rich-map it in `event_map`. §11.3/§11.4 are the menu.

**Scope mapping:** private-repo signals → `Private(owner)`; org/account-level meta → owner-private
or treated as administrative; global advisories → `Public`. (`finalize` owns this, Phase 3.)

