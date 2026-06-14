# Digest System — Technical Architecture (Working Snapshot)

**Status:** As-built technical reference for the shipped runtime (M1–M3 + the Thread layer), with
forward-looking notes for M4–M6. Library versions are research snapshots — see below.
**Last updated:** 2026-06-14
**Companion to:** `system-design.md` (product thesis + data model). That doc owns
*what* we build and the data model; **this** doc owns *how* we build it — the Rust
runtime, the process topology, and the cross-cutting concerns (observability, reliability,
testing, reproducibility, security mechanisms).

> **As-built reconciliation (2026-06-14).** The `core` modeling described here is **implemented**:
> `Event` + the fingerprint recipe (§5.2), `Cluster`/`Story` in the per-subscriber linking model
> (§5.3), and the **`Connector`/`Connection` two-layer trait family** (§5.4) all ship in
> `crates/{core,bulletin}`. The runtime topology (§2), ingestion/cursors (§3), webhook auth (§3A),
> two-context RLS (§6, `system-design.md` §12) and at-rest envelope encryption (§6) are all built.
> **Connectors built: RSS + GitHub** — the `ConnDispatch`/`RealtimeDispatch` enums name `Rss`/`Github`
> only; **Slack is deferred to roadmap M6** (the trait family already accommodates it). The Thread
> layer below is no longer "deferred" — its first slice is implemented (see `thread-layer.md` §9). §10
> ("Open threads") is the remaining design/scope work, now read as the **M4–M6 backlog**.

**Thread layer (designed 2026-06-14 — product §8.6–§8.7, §10.4; full design `thread-layer.md`):**
a fourth content-graph phase **`Thread`** (the persistent per-subscriber weave) fed by **tiered
probabilistic identity** (`entity_edge` graph) with **confidence as a rendered signal**. Adds one
write-side job, **`thread_maintenance`** (off the punctual path), and is schema-additive. Built on top
of M3 linking (first slice shipped, behind the `thread-weighting` cargo feature); modeled here as
§5.3, §5.5, §10 notes.

All library versions below are research snapshots as of **2026-06** — treat as "latest known
good," re-verify before locking `Cargo.toml`.

---

## 1. Guiding principles & non-functional priorities

- **Rust-first.** Frontend/rendering is out of scope for this design track.
- **First-class:** performance, reliability, observability, and testing. Metrics, crash
  protection, cyber-safety guards, and tests are not bolt-ons.
- **Nix** provisions dev environments, CI checks, and reproducible builds — set up early.
- **async/tokio** at the I/O edges; **Postgres is the orchestration backbone.**

**The load-bearing reframe:** this is a **Postgres-orchestrated scheduled batch pipeline**, not
a service mesh. The job table(s) are the scheduler, queue, and coordination substrate. Tokio
shines in exactly three places — the always-on webhook catcher (fast-ack), the hydration I/O
fan-out, and outbound delivery. The core tick DAG is DB-heavy batch work parallelized **across
job rows**, not async concurrency per se. CPU-bound clustering/linking/scoring may want
`spawn_blocking`/`rayon` so they don't stall the runtime. **Don't over-async.**

**Rust's biggest payoff here is correctness/security, not speed** — we use the type system to make
the product's #1 risk (scope isolation) *unrepresentable* (see §5), on top of DB-level defenses.

---

## 2. Runtime topology  *(DECIDED)*

### Roles — one binary, multiple roles via `clap`

| `bulletin <cmd>` | runs | scaling |
|---|---|---|
| `serve` | axum: webhook catcher + read API | scale-out (stateless) |
| `worker` | apalis `Monitor`: cron-tick worker + processing workers | scale-out |
| `migrate` | sqlx migrations + apalis storage setup | one-shot |
| `all` | `serve` + `worker` in one process | dev only |

Same workspace, same domain crates, one image; split roles onto separate processes later by
running the binary with a different subcommand — zero code change. This is the modular monolith
made literal.

### The pipeline

```
serve (always-on)   POST /webhooks/:source → verify(raw) → enqueue ProcessWebhook(raw) → 2xx (ms)

worker (apalis Monitor · run_with_signal · 1+ replicas)
 ├─ cron tick (@every ~1m)  handler holds Data<PostgresStorage<…>>, runs 3 cheap due-sweeps:
 │   • connection.next_poll_at ≤ now & active   → push PollConnection  (unique key: connection_id)
 │   • new public events since last public-build → push PublicBuild     (unique key: one-in-flight)
 │   • subscriber.next_run_at ≤ now             → push GenerateDigest  (unique key: subscriber_id+window_end)
 └─ processing workers (PostgresStorage<T> · retry+backoff · concurrency caps):
     PollConnection ─ poll(cursor)+normalize; advance cursor & next_poll_at ─┐
     ProcessWebhook ─ hydrate+normalize ──────────────────────────────────── ┤→ events (UNIQUE(fingerprint) dedup)
     PublicBuild    ─ group public events → public clusters + rollups (no linking; no-subscriber RLS ctx)
     GenerateDigest ─ [subscriber RLS ctx] private-build → pre-select → link (per-sub) → select → render → deliver
                       ▶ email; window_end = scheduled boundary; advance next_run_at to next future boundary (coalesces missed)
```

Key properties:
- **Poll is the reliable floor; webhooks are a layer.** Webhooks are at-most-once, so a
  cursor-driven reconciliation poll (the `Connection` foundation, §5.4) is what *guarantees*
  completeness; the realtime (webhook) layer adds freshness + quota savings on top (it lets the
  reconciliation interval relax — it never carries correctness). Both normalize → dedup into the
  same `event` table via the §5 canonical-event contract. (v1: RSS = poll-only; GitHub/Slack =
  poll-reconcile + realtime webhooks.)
- **Two independent clocks (the read/write split):** *materialization* (ingest + grouping) runs on
  the world's clock (per-`connection` poll cadence, minutes) and is best-effort; *projection*
  (`GenerateDigest`) runs on each `subscriber`'s cadence (daily/weekly, at their local time). They
  share only the durable event log; neither blocks the other. Under load materialization falls
  *behind*, never *wrong* — the digest still fires on time with best-effort-fresh contents.
- **The sweep advances nothing — the processing job does.** The sweep is a stateless "what's
  due?" reader; `next_poll_at`/`next_run_at` are written by the job. → self-healing: a job that
  crashes before advancing its watermark is simply still due next tick.
- **Singleton scheduler not required.** `apalis-cron` is local-clock driven (fires on every
  replica, no catch-up), but the `unique-jobs` feature makes each enqueue idempotent on a key we
  choose, so duplicate ticks across replicas are harmless. Run the cron inside `worker`; split a
  dedicated single `tick` role only as a later scale optimization.
- **`PublicBuild` and `GenerateDigest` are decoupled** (not chained). `PublicBuild` materializes
  shared cluster rollups on its own cadence (private-build is write-side too, but v1 runs it
  just-in-time at the head of `GenerateDigest` — §5.3); `GenerateDigest` reads the *latest
  materialized snapshot* at the subscriber's scheduled instant. A not-yet-clustered event simply
  isn't a candidate that fire and rides the next one — never lost (durable event log; the
  consideration floor reaches back to the last delivery — product §9.4). **Linking is per-subscriber, inside `GenerateDigest`** — a story can fuse
  public clusters with that subscriber's *own* private clusters (product §4), so it can't be a
  global precompute. Public clusters (grouping + rollups) are built once and amortized; only linking
  + scoring are per-subscriber. `PublicBuild` runs in a no-subscriber RLS context, `GenerateDigest`
  in the subscriber's (product §12).

### apalis mapping  *(DECIDED — pinned `=1.0.0-rc.9`)*

- **Version:** `apalis` / `apalis-postgres` `=1.0.0-rc.9`, `apalis-cron` `=1.0.0-rc.8`. RC line,
  expect API churn; pin exactly. (Repo lives at `apalis-dev/apalis`; rc splits Postgres into the
  `apalis-postgres` crate.) Chosen over stable 0.7.x for the rc's `PostgresStorageWithListener`
  (Postgres `LISTEN/NOTIFY` push-based job pickup → lower dispatch latency than polling).
- **What we lean on apalis for:** the recurring clock (`apalis-cron` `CronStream`); the queue +
  retries/backoff/`max_attempts`; failed rows retained = dead-letter view; crash recovery
  (orphaned-job re-enqueue); idempotent enqueue (`unique-jobs`); future-scheduled jobs
  (`Task::builder(job).run_after(Duration)`); one `Monitor` hosting many workers +
  `run_with_signal(...)` graceful drain.
- **What we own:** the three "due rows" SQL sweeps; advancing the `next_*_at` watermarks inside
  the processing jobs (domain logic); the cron singleton/leader concern (solved by `unique-jobs`).
- **Job queue:** the work queue (product doc §6) uses the **apalis-managed schema** — same
  `FOR UPDATE SKIP LOCKED` semantics, library-owned.
- **Deferred job kind:** the Thread layer adds **`thread_maintenance`** (per-subscriber, relaxed
  cadence: enqueued post-`GenerateDigest` + a nightly sweep, `unique-jobs` key = `subscriber_id`,
  coalescing). Write-side, best-effort, **off the punctual path** — it never blocks a fire; projection
  only reads the weights/threads it produced (product §8.6, `thread-layer.md` §5).

### Durable execution / Temporal  *(DECIDED — defer)*

Not now; the trigger to reconsider is **orchestration complexity, not scale** (Postgres+apalis
already scales the per-subscriber fan-out). Blockers: Temporal's Rust SDK is pre-1.0
(`temporalio-sdk` 0.4.0, "not production ready" → you'd author workflows in Go/TS); operational
weight conflicts with the "only Postgres + webhook catcher always-on" thesis; event-history
persists payloads in plaintext (a §12 concern for private content); and it's redundant with our
idempotency design. **Escalation ladder if complexity grows:** plain apalis jobs → apalis
stepped-tasks / `apalis-workflow` → `underway` → Restate → Temporal (last rung, when its Rust SDK
is GA *and* we have genuine long-lived sagas). The generation state machine stays hand-rolled —
queryable domain data beats state hidden in an engine's event history.

---

## 3. Ingestion: pull scheduling & cursors  *(DESIGNED — spec to finalize)*

Two unrelated kinds of per-connection state, two owners:

| | **Scheduling state** — *when* to poll | **Cursor state** — *where we left off* |
|---|---|---|
| Owner | infrastructure (generic) | the **source adapter** (specific) |
| Source-aware? | no | yes — the source's private business |
| Lives in | typed columns on `connection` | one opaque `jsonb` blob on `connection` |
| Used by | the cron **sweep** | the `poll()` call inside the job |
| Examples | `next_poll_at`, `poll_interval`, `status` | RSS `{etag,last_modified}`; GitHub `{since}`; IMAP `{uidvalidity,uidnext}` |

### `connection` scheduling columns (infra-owned, source-agnostic)
`poll_interval`, `next_poll_at` (indexed), `last_polled_at`, `consecutive_failures` (backoff
state), `status` (active|paused|revoked|errored — `revoked` set by lifecycle webhooks, §3A).
Realtime sources use a longer default `poll_interval` (reconciliation cadence — webhooks keep them
fresh between polls); RSS uses a short one (content freshness). Partial index:
`CREATE INDEX due_connections ON connection (next_poll_at) WHERE status = 'active';`

### Cursor (source-owned, opaque)  *(DECIDED)*
An **associated type** on the `Connection` trait — **not** `Serialize` on the `Connection` itself:

```rust
trait Connection {                          // the pull foundation — every connector's worker (full family §5.4)
    /// Opaque, source-private incremental-fetch position. Infra persists as JSON, never reads inside.
    type Cursor: Serialize + DeserializeOwned + Default + Send + Sync;
    type Item;
    async fn poll(&self, cursor: Self::Cursor) -> Result<Batch<Self::Item, Self::Cursor>, SourceError>;
    fn to_events(&self, item: Self::Item) -> Vec<EventContent>;
    // webhook sources add the `RealtimeConnection: Connection` layer — accept_webhook + hydrate (§5.4)
}
struct Batch<I, C> { items: Vec<I>, cursor: C }   // []+unchanged cursor = "nothing new"; failed poll = Err
```

Rationale (rejecting `Connection: Serialize`): a `Connection` is **behavior with live,
non-serializable deps** — the SSRF-guarded HTTP client, a rate limiter, and **decrypted
credentials**. Requiring `Serialize` on it forces `#[serde(skip)]` gymnastics and risks serializing
a secret into the `connection` row (a §12 leak you'd have to prevent *per field, forever*). A
separate minimal `Cursor` type **cannot** hold a secret or a client — illegal states don't compile. It also keeps
`DeserializeOwned` from fighting dependency injection, keeps per-poll `UPDATE`s tiny (config lives
in `connection.config`, not rewritten each poll), and makes `poll` a testable transition
`(deps, cursor) → (items, next_cursor)`. **Mental model: persist the checkpoint, not the worker.**

### `PollConnection` lifecycle + ordering rules  *(DECIDED)*
```
1. load connection; decrypt creds; cursor = deserialize(connection.cursor) || Cursor::default()
2. pull = source.poll(cursor)
3. normalize pull.items → events; INSERT … ON CONFLICT (fingerprint) DO NOTHING   ← persist events FIRST (commit)
4. UPDATE connection SET cursor = pull.next_cursor, last_polled_at = now(),
                         next_poll_at = now()+poll_interval, consecutive_failures = 0
```
- **Events before cursor.** Commit events, *then* advance the cursor. Crash in between → re-poll
  re-fetches, `UNIQUE(fingerprint)` dedups (at-least-once, safe). Never the reverse.
- **Never advance the cursor on a failed poll.**
- **Failure ≠ failure:** a failed *poll attempt* (429/5xx/timeout) is a **domain outcome** — bump
  `consecutive_failures`, set `next_poll_at = now()+backoff(failures)` (exp+jitter, or honor
  `Retry-After`), flip `status='errored'` past a threshold — and the **job returns `Ok`**. A
  failed *job* (DB blip, panic) is **infra** — that's what apalis `max_attempts`/retry and
  orphan-reenqueue are for. Keep apalis per-job retries low (1–2) so real source outages fall
  through to schedule-level backoff instead of re-sweeping every tick.

---

## 3A. Authentication, connection binding & webhook verification  *(DECIDED — research-grounded 2026-06)*

Two of three v1 sources need OAuth/app setup (GitHub App, Slack OAuth v2); RSS needs none.

**No per-connection webhook URLs for our push sources.** GitHub App and Slack app each expose
ONE app-level webhook URL for all installations/workspaces — you cannot mint a per-connection
URL. So: one endpoint per app (`/webhooks/github`, `/webhooks/slack`); route internally by a
verified payload id. (Per-connection `/webhooks/{token}` URLs are possible only for self-created
webhooks — Stripe, classic repo hooks — kept in reserve as defense-in-depth, not used in v1.)

**Identity is bound at connect-time, not webhook-time.** A webhook payload carries only the
*provider's* account id (GitHub `installation.id`, Slack `team_id`) — nothing pointing to our
user. The binding provider-account → subscriber → scope is created during the authenticated
OAuth/install flow (signed `state` ties it to the logged-in subscriber) and stored on
`connection`. Webhooks then only *look up* that binding; they never establish identity.

**Webhook verification = two orthogonal checks, in order:**
1. **Authentic?** HMAC over raw bytes with the **app-level** secret, constant-time.
   - GitHub: `X-Hub-Signature-256` (`sha256=`), app webhook secret; no timestamp → dedupe on `X-GitHub-Delivery`.
   - Slack: `X-Slack-Signature` `v0=`HMAC-SHA256 over `v0:{X-Slack-Request-Timestamp}:{raw_body}`, app Signing Secret; reject timestamp >5 min (replay).
2. **Whose/what scope?** After (1), look up our `connection` by the verified provider id → derive
   subscriber + scope **from our row**; no active match → drop. Never trust a payload-supplied id (IDOR).

**Secrets live in three tiers** (per-connection creds are source-shaped, like the cursor):

| Tier | What | Where |
|---|---|---|
| App-level (per app) | signing secret, OAuth client id/secret, GitHub App id + RSA key, redirect URIs | config + KMS, loaded once |
| Per-connection | source-shaped encrypted creds | `connection.creds_ref` — Slack `xoxb-` (+refresh if rotation); GitHub = just `installation_id` (not a secret) |
| Ephemeral | short-lived access tokens, in-mem, not persisted | GitHub installation token (1h, minted from app JWT RS256 ≤10m); Slack rotated token (12h) |

**Token management:** a source-specific **`TokenProvider`** (`access_token(creds) -> Token`) hides
per-source differences; infra caches + a per-connection lock prevents refresh races. GitHub: mint
installation token on demand (no per-connection refresh token; tokens now `ghs_<APPID>_<JWT>`,
variable length — don't validate length). Slack: long-lived bot token by default, or 12h access +
single-use refresh if rotation enabled; design for **optional/partial scope grants** (2026).

**Lifecycle (control-plane) webhooks** must update `connection.status`: GitHub `installation`
(created/deleted/suspend/unsuspend/new_permissions_accepted), `installation_repositories`; Slack
`app_uninstalled`, `tokens_revoked` (order not guaranteed) → mark revoked, purge tokens, stop
ingestion, prompt re-auth.

**Catcher flow — dumb, authenticity-only, no body parse, no connection resolution** (parse the
content *once*, at process-time, in the job):
```
POST /webhooks/{source}                       (github | slack — one URL per app)
  serve (always-on):
   1. app.verify(headers, raw_body)           ← realtime-app: HMAC over raw bytes (path→app secret) +
        Invalid   → 401 drop                     timestamp/replay. Slack url_verification → 200 echo.
        Authentic → enqueue ProcessWebhook{source, raw_body, delivery_id}   (raw carried inline; offload deferred)
   2. return 2xx fast                          (Slack ≤3s, GitHub ≤10s; delivery_id = X-GitHub-Delivery / sig)

  ProcessWebhook job (worker) — routing + parse + hydrate at process-time:
   3. routing_id = app.route(raw_body)         ← credential-free peek (installation.id / team_id)
   4. resolve connection by (source, routing_id); no active match → drop (log).
        derive subscriber + scope from OUR row — never the payload (IDOR)
   5. build conn from decrypted creds; conn.accept_webhook(raw_body) → Inbound::Events | Lifecycle
   6. Events → conn.hydrate (delayed fetch of latest state) → to_events → finalize(scope, fingerprint)
              → INSERT … ON CONFLICT (fingerprint) DO NOTHING
      Lifecycle → apply connection-state change
```
The catcher's only security job is **authenticity** (HMAC); routing's IDOR defense (derive scope
from our row, never the payload id) moves intact into the job. Authentic-but-unmatched deliveries
(a real install whose `connection` we don't hold) reach the job and drop there — bounded, since
HMAC already proved the sender.

**Model / SDK deltas (apply in the `core`/data pass):**
- `connection.provider_account_id` (installation_id/team_id) — `UNIQUE(source_type, provider_account_id)`, the webhook routing key.
- `connection.creds_ref` nullable/source-shaped; `connection.status` = active|paused|revoked|errored.
- Connector SDK is **two layers** (§5.4): a `Connection` foundation (`poll`/`to_events` — the
  reliable cursor-driven path every connector has) + a `RealtimeConnection: Connection` layer
  (`accept_webhook`/`hydrate`) for webhook sources. The `Connector`'s app-level `verify` (HMAC, no
  parse) is the catcher's only connector call;
  `route`/parse/hydrate run in the job. `Credentials` is consumed at `connect`; `TokenProvider`
  minting + the refresh cache/lock are app-level; app config injected at connector construction.
- New `serve` surface: `GET /connect/{source}` (initiate, signed state) + `/connect/{source}/callback`
  (verify state, exchange code / capture installation_id, create connection, encrypt creds).
- Webhook idempotency: `unique-jobs` key on provider delivery id (`X-GitHub-Delivery` / Slack
  `event_id`) complements `UNIQUE(fingerprint)`.

---

## 4. Crate graph  *(TENTATIVE — names not final; user wants simpler/different)*

**Principle:** the *only* architecturally mandatory boundary is **`core` (pure, no I/O) vs. the
rest**. That boundary is what makes proptest/insta/DST possible. Everything else starts as modules
and is promoted to a crate lazily (compile time / ownership / reuse). Isolate `connectors`
regardless (untrusted external input).

```
                 core         pure domain: types · ID newtypes · Scope · Connector port · scoring/selection
                  ▲           NO tokio/sqlx/apalis            ◀── proptest + insta live here
     ┌────────────┼────────────┐
   store      connectors     support      store=sqlx/migrations/RLS · connectors=source adapters
     ▲            ▲             ▲          support=config(toml)/telemetry/secrets/SSRF http client
     └────────────┴──────┬──────┘
                      runtime              apalis handlers · axum(webhook+api) · DAG orchestration
                         ▲
                      bulletin (bin)       clap · role dispatch · composition root
```
Dependencies point down toward `core`; `core` depends on nothing internal. Leaner day-one option:
fold `support`→`runtime` and give `connectors` a small `http`-only dep. **Open: finalize names &
granularity.**

---

## 5. Type modeling — `core`  *(IMPLEMENTED — `Event`, `Cluster`/`Story`, the connector family, and per-subscriber linking all ship; reason-records-as-types and the config-table thresholds are the M4 remainder, §5.5)*

`core` is the pure domain crate: no tokio/sqlx/apalis at runtime. Deps are runtime-agnostic
(`serde`, `uuid`, `time`, `sha2`, …) plus **feature-gated** `sqlx` + `proptest` (so
`cargo test -p core` stays DB-free).

### 5.1 Foundations  *(DECIDED — research-grounded 2026-06)*

**Typed IDs — own a generic `Id<T>`** (not a UUID-wrapper crate). `Id<Event>` is literally the
value of `event.id` (a `uuid`) and doubles as the FK type (`digest_item.story_id: Id<Story>`).
The phantom `T` is a compile-time tag only — `Id<Cluster>` where `Id<Subscriber>` is expected is a
*compile* error; at runtime / in Postgres it's a plain 16-byte UUID.

- Why own it (vs `newtype-uuid`'s `TypedUuid<T>`, which already solves the phantom-derive traps):
  the **orphan rule** forbids `impl sqlx::Type for` a *foreign* type, and `newtype-uuid` has no
  sqlx support → we'd wrap it anyway. Owning the type lets us write the sqlx `Type/Encode/Decode`
  impls **once, generically, behind a default-off `sqlx` feature**.
- Use **`derive-where`** for `Clone/Copy/Hash/Eq/Ord/Debug` to dodge the `#[derive] ⇒ where T: _`
  trap; `PhantomData<fn() -> T>` for unconditional `Send+Sync` + correct variance.
- **PK is DB-generated** UUIDv7 (`DEFAULT uuidv7()`); never mint via ambient `now_v7()` in `core`.
- Readable TypeID-style prefixes (`subscriber_01h…`) are a **boundary-only presentation** concern
  (a Display/serde adaptor at the API), *not* the storage type — the column stays native `uuid`
  for v7 index locality. Deferred.

```rust
#[derive_where(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct Id<T> { uuid: Uuid, _kind: PhantomData<fn() -> T> }

pub enum Scope       { Public, Private(Id<Subscriber>) }   // illegal scope states uninhabitable
pub enum SourceKind  { Rss, Github, Slack }                // closed set → exhaustive match
pub enum ContentKind { Message, Announcement, Longform }   // ordered: depth → Story/Note
```

**`async fn` dispatch (DECIDED — see §5.4):** as of Rust 1.96
native `async fn` in traits is still **not dyn-compatible**, and associated types that differ per
impl (`Cursor`/`Creds`) are neither `dyn`- nor `enum_dispatch`-able as one type. Researched
path: a native-`async fn` typed `Connection` + a hand-written **type-erased boundary** (cursor/creds →
`serde_json::Value`, à la `erased-serde`) where the runtime dispatches on `source_type`.
**Decided (§5.4):** a hand-written `match` over the closed source set (not `dyn`, not the
`enum_dispatch` crate) — each arm erases `Cursor`/`Creds` to `serde_json::Value` *internally*,
keeping native `async fn` and zero boxing; `dyn`/`Erased<S>` stays the escape hatch only if sources
ever become plugin-loaded.

### 5.2 Canonical `Event`  *(DECIDED)*

**Three-stage hydration (connector → infra → DB).** The connector's `to_events` returns
**`EventContent`** — content only, with *no* `scope`, `fingerprint`, `id`, or `ingest_time`. Infra
`finalize(EventContent, &connection) → NewEvent` stamps `scope` (from the `connection` row) +
computes the `fingerprint` (recipe below), so **no adapter ever touches the scope boundary or the
dedup recipe** — risk #1 made structural (§5.4). `INSERT … RETURNING` then hydrates the full
`Event`; the DB fills `id` (`DEFAULT uuidv7()`) + `ingest_time` (`DEFAULT now()`). (Avoids the
`Option<Id>`-that-lies-after-insert anti-pattern.) **No `cluster_id`** — cluster membership is
*derived* by `(scope, source, group_key)` (§5.3), so there's no per-event pointer to assign.

**`fingerprint` recipe** — the dedup `UNIQUE` key, the property that makes noise-suppression +
crash-safe re-ingest true (proven w/ proptest):
- `SHA-256` (stable across processes/versions — **never** `DefaultHasher`/`ahash`), stored as
  `bytea`, `Fingerprint([u8; 32])`.
- pre-image = `source ‖ stable_source_id`, **domain-separated + length-framed** (so `("ab","c")` ≠
  `("a","bc")`). **No `content_hash`** — v1 deliberately does *not* fold content in, so an edit or a
  re-poll of the same item collapses (`ON CONFLICT DO NOTHING`) instead of spawning a new event that
  would spuriously re-surface a story (product §9.4). Fold a `content_hash` back into the pre-image
  later if edit-as-timeline is wanted — strictly additive.

```rust
pub struct Event {
    // ── data model: what the event *is* ──────────────────────────
    pub source:        SourceKind,          // which connector produced it
    pub scope:         Scope,               // risk #1 — isolation boundary
    pub event_time:    OffsetDateTime,      // when it occurs — timeline + digest window; v1 retrospective (future-valued deferred, §8.5 product)
    pub title:         String,
    pub body:          Option<String>,      // title-only → None; most PII-heavy
    pub links:         Vec<String>,         // backing data — *required* for provenance/timelines

    // ── clustering / scoring: signals aggregation consumes ───────
    pub group_key:     String,              // within-source grouping atom; cluster membership derived by (scope, source, group_key)
    pub entities:      Vec<String>,         // blocking substrate for cross-source linking
    pub content_kind:  ContentKind,         // depth → Story vs Note
    pub severity_hint: Option<i16>,         // priority input (orders; never gates)
    // confidence: DEFERRED (§8.3/§8.5 product) — every v1 source is deterministic; re-add as a field + multiplier

    // ── administrative: system-owned plumbing ────────────────────
    pub id:           Id<Event>,            // PK, DB-generated UUIDv7
    pub fingerprint:  Fingerprint,          // dedup UNIQUE — idempotent ingest
    pub ingest_time:  OffsetDateTime,       // DB DEFAULT now() — audit + future partition key
    pub raw:          Option<Vec<u8>>,      // inline raw payload (TOASTed out-of-line) — replay/audit; object-storage offload deferred
}
```
`entities`/`links` stay freeform `Vec<String>` until the linking pass; `fingerprint`'s `[u8;32]`
may render as hex `text` instead if psql-debuggability beats index size.

### 5.3 `Cluster` & `Story` — the content graph  *(DECIDED — revised for per-subscriber linking, Proposal B)*

Three phases: **`Event`** (deduplicated) → **`Cluster`** (grouped within one source) → **`Story`**
(linked across sources). **Grouping** (events→clusters) is deterministic; **linking** (clusters→story)
is **per-subscriber** (product §4/§8.2). The product doc's self-referential `cluster` (`parent_id`) is
gone; so is a single shared story object. *(Deferred 4th phase — `Thread`: the persistent per-subscriber
weave over many stories across time, plus a `canonical_entities` field on `Cluster`/`Story` from tiered
identity resolution. Types sketched in `thread-layer.md` §8; product §8.6–§8.7. All additive.)*

**Clusters are a recomputed batch artifact (materialization side); stories are the per-subscriber read-model, rebuilt at fire time from a snapshot of the cluster caches (projection side).**
- `PublicBuild` recomputes **public** cluster rollups as public events arrive (shared, amortized; decoupled from generate).
- Inside each `GenerateDigest`, `private-build` recomputes that subscriber's **private** cluster
  rollups; then **linking** computes connected components over the subscriber's candidate clusters
  (public ∪ own private) and writes one `Story` per component (product §8.2).

Consequences for the types:
- **`Cluster` carries no `story_id`.** A public cluster belongs to *many* subscribers' stories, so
  membership can't be a back-pointer on the (shared) cluster — it lives on the story as `clusters`.
- **`Story` is per-subscriber** (`subscriber_id`, always Private-scoped; never a `Scope` enum) and
  holds its members inline as `clusters: Vec<ClusterRef>` (`{cluster_id, link_reason}`). That
  membership *is* the persisted prior assignment the next recompute reads to **forward stable ids**;
  `merged_into` redirects a retro-merged story to its survivor (product §8.2). No sticky-write-once.
- **Membership lookups:** a cluster's events = events sharing its `(scope, source, group_key)`;
  drill-down walks `story.clusters` → each cluster's group_key → events.

**Two-level rollup** (event → cluster → story): every signal is computed once per cluster and
aggregated over a story's clusters. Selection reads **stories only**; story-aggregation reads the
**clusters** named in `clusters`; cluster rollups read only their own group's events — bounded levels,
no raw-event rescans above the bottom. `count`/`max`/`∪` compose additively. (`velocity` and
`confidence` are **deferred** — both near-constant in v1; `velocity` is also time-dependent, so when
it returns it belongs at read-time, not as a cached column — product §2/§8.3.)

```rust
pub struct Cluster {                       // phase 2: a within-source group (public shared / private per-subscriber)
    pub id:          Id<Cluster>,
    pub scope:       Scope,                 // isolation — Public, or pinned to one tenant
    pub source:      SourceKind,            // one source's group
    pub group_key:   String,                // UNIQUE(scope, source, group_key)
    // rollup of this group's events (build-maintained cache); NO story_id — membership lives on Story
    pub event_count:      i32,
    pub max_severity:     Option<i16>,      // max(event.severity_hint) → priority input
    pub content_depth:    ContentKind,      // max(event.content_kind)  → Story-vs-Note depth
    pub entities:         Vec<String>,      // ∪ event.entities → blocking (GIN) + "about X"
    pub first_event_time: OffsetDateTime,
    pub last_event_time:  OffsetDateTime,
}

pub struct ClusterRef { pub cluster_id: Id<Cluster>, pub link_reason: Option<String> }  // serialized in story.clusters

pub struct Story {                          // phase 3: the PER-SUBSCRIBER cross-source unit selection scans
    pub id:            Id<Story>,           // stable across rebuilds (forwarded) → deep-link / feedback target
    pub subscriber_id: Id<Subscriber>,      // owner; always Private-scoped (never spans tenants)
    pub merged_into:   Option<Id<Story>>,   // set when a retro-merge forwards this id to its survivor (§8.2)
    pub clusters:      Vec<ClusterRef>,     // membership (public + own private) + per-member link_reason
    // cross-source rollup (aggregate of the story's clusters; read by selection, never events)
    pub event_count:      i32,              // Σ cluster.event_count
    pub source_diversity: i16,              // distinct cluster.source — the "across sources" value, free
    pub max_severity:     Option<i16>,      // max over clusters
    pub content_depth:    ContentKind,      // max over clusters
    pub entities:         Vec<String>,      // ∪ over clusters
    pub first_event_time: OffsetDateTime,
    pub last_event_time:  OffsetDateTime,   // freshness score + (vs per-subscriber snapshot) recently-surfaced damping (product §9.4)
}
```
`clusters`/`entities`/`link_reason` stay freeform per §5.2; `clusters` persists as `jsonb` (split-trigger
to a normalized `story_cluster` table when the shared public-story cache lands — product §6).

### 5.4 `Connector` / `Connection` trait family  *(DECIDED — 2026-06)*

**Two layers, because poll is correctness and webhooks are a layer on top.** Webhooks are
at-most-once (lossy), so the only thing that *guarantees* completeness is a cursor-driven
reconciliation poll — every connector stands on that. "Realtime" (webhook) intake is an optional
layer that adds freshness + quota savings (it lets the poll interval relax), never the correctness
floor. The names mirror the data model (product §6–7): a **`Connector`** is the cross-tenant adapter
(one per `SourceKind`, built from config TOML + app secrets) and a **`Connection`** is the
per-tenant live worker it spawns (one per `connection` row, built per job from decrypted creds) —
`Connector::connect(creds) → Connection`, like any driver. Four small pieces, not one fat trait:

```rust
// ── per-tenant: the live worker for one `connection` row (holds SSRF-guarded client, limiter, creds, token) ──
pub trait Connection: Send + Sync {             // foundation — EVERY connector's worker
    type Cursor: Serialize + DeserializeOwned + Default + Send + Sync;   // opaque, source-private (§3)
    type Item: Send;                            // one unit; complete after poll, thin after a webhook
    async fn poll(&self, cursor: Self::Cursor) -> Result<Batch<Self::Item, Self::Cursor>, SourceError>;
    fn to_events(&self, item: Self::Item) -> Vec<EventContent>;          // pure normalize; proptest target
}
pub struct Batch<I, C> { pub items: Vec<I>, pub cursor: C }  // []+unchanged cursor = "nothing new" (RSS 304);
                                                            // a failed poll is Err(SourceError), never empty Batch

pub trait RealtimeConnection: Connection {      // layer — webhook sources only
    fn accept_webhook(&self, body: &[u8]) -> Result<Inbound<Self::Item>, SourceError>;  // verified body → units
    async fn hydrate(&self, item: Self::Item) -> Result<Self::Item, SourceError> { Ok(item) } // delayed fetch;
}                                                                          // default identity (complete payloads)
pub enum Inbound<I> { Events(Vec<I>), Lifecycle(LifecycleChange) }

// ── cross-tenant: the adapter, one per SourceKind (built once from config + app secrets; in serve AND worker) ──
pub trait Connector: Send + Sync + 'static {    // foundation — factory for the per-connection worker
    const KIND: SourceKind;
    type Creds: DeserializeOwned + Send + Sync; // source-shaped decrypted per-connection creds
    type Conn: Connection;
    fn connect(&self, creds: Self::Creds) -> Self::Conn;
}
pub trait RealtimeConnector: Connector {        // layer — the catcher's only connector call
    fn verify(&self, headers: &http::HeaderMap, body: &[u8]) -> Verified;   // HMAC over raw bytes; NO parse
    fn route(&self, body: &[u8]) -> Result<String, SourceError>;            // credential-free routing peek
}
pub enum Verified { Authentic, Challenge(Vec<u8>), Invalid }
```
(`http` is a types-only crate — no runtime — so it's allowed in `core` per the §4 boundary. The
persisted `connection` row maps to a **`ConnectionRow`** struct in `store` — the trait is the live
worker, the row is its state.)

**Why `hydrate` is on the realtime layer, not the foundation.** "Delayed fetch" only has meaning
where a **receipt→process split** exists — i.e. webhooks (catcher receives at T0, the job fetches
*latest* state at T1, §3A / product §3.1, which sidesteps stale/out-of-order deliveries). `poll`
has no such split: it fetches complete units synchronously inside its own job, so a pull source
never hydrates. `to_events`/`Item` live once on `Connection`; `RealtimeConnection: Connection`
inherits them, so a both-capable source writes its normalization once and adds the realtime head.

**Pipelines** (both converge on the shared `to_events`, per product §3's two-intake diagram):
- poll: `poll → [complete Item] → to_events → EventContent`
- realtime: `accept_webhook → [thin Item] → hydrate → [complete Item] → to_events → EventContent`

Then infra `finalize(EventContent, &connection) → NewEvent` stamps `scope` + `fingerprint` (§5.2).

**Dispatch = a hand-written enum over the closed source set, NOT `dyn`** (§5.1). The cursor/creds
erase to `serde_json::Value` *inside* each match arm — the whole `poll → hydrate → to_events` chain
runs in the typed arm, and only concrete `core` types (`Vec<EventContent>` + the JSON cursor) cross
out. `Item` is an associated type, so it can never cross the boundary; the chain must complete
inside the arm. Native `async fn` throughout, no boxing, exhaustive (the compiler flags a missing
arm when a 4th source lands):
```rust
enum ConnDispatch     { Rss(RssConn), Github(GithubConn), Slack(SlackConn) }  // poll_step — every connection
enum RealtimeDispatch {              Github(GithubConn), Slack(SlackConn) }   // webhook_step — RSS absent
```
The two enums make capability a **type**: `RealtimeDispatch` cannot hold RSS, so "webhook routed to
a pull-only source" is uncompilable — no runtime `Unsupported`. Built via capability-specific
factories (`connect_pull → ConnDispatch` for `PollConnection`, `connect_push → RealtimeDispatch`
for `ProcessWebhook`); no job needs both, so each decrypts + builds the connection once.

**v1 capability profile** — three connectors, three distinct profiles, shared `to_events` written once:

| Source | `Connection` (poll + to_events) | `RealtimeConnection` (accept_webhook + hydrate) |
|---|---|---|
| RSS | poll = its only intake (conditional GET, cursor = ETag/Last-Modified) | — |
| GitHub | poll = **reconciliation backstop** (REST events since) | webhook; hydrate = identity (full payloads) |
| Slack | poll = **reconciliation** (`conversations.history`) | webhook; hydrate = identity (Events API complete) |

Reconciliation is v1 **by construction** (it's the foundation, not a deferred capability); a dropped
webhook is recovered at the next poll, `UNIQUE(fingerprint)` collapsing the overlap. For a *scheduled*
digest the realtime layer buys **quota + push-only coverage**, not latency — so realtime connections
poll on a relaxed reconciliation cadence (hourly-ish) while RSS polls on content-freshness cadence
(minutes); same mechanism, two default `poll_interval`s.

**Tokens (sketch — finalize next).** `TokenProvider` (`access_token(creds) → Token{secret,
expires_at}`) is a `core` port the `Connector` drives; the per-connection refresh **cache +
lock** is infra (§3A). `connect` hands each `Connection` a token handle. GitHub: app-JWT →
installation token; Slack: bot token, or 12h access + single-use refresh if rotation is enabled.

### 5.5 Still to model (this pass continues)
- **Reason records** as types (link / story / note / drop) — explainability as data. The link reason
  rides per-member in `story.clusters` (§5.3); selection/drop reasons in `digest_item.reasons`.
- **Pure selection function** — `fn(features, now) -> decision` (gate→rank→classify), with fixtures.
  v1 priority time-term is **plain recency decay** over `now − last_event_time`; the signed-gap
  salience curve that also handles future-valued items is deferred with prospective events (§8.5
  product). `now` is injected (§6 Reliability), never ambient.
- **Per-subscriber linking** — connected-components over the candidate-cluster edge graph + the
  id-forwarding/`merged_into` rule (product §8.2). Pure over `(clusters, edges, prior assignment)`.
- **Deferred — Thread layer & tiered identity** (`thread-layer.md` §8): `Thread` +
  `ConfidenceBand { Confirmed, Probable, Uncertain }` + the `entity_edge` graph; the `thread_maintenance`
  job logic (identity resolution → community detection → id-forwarding → state/decay → weight
  projection) is pure over `(entity_edges, prior threads, feedback, now)` and reuses the §8.2
  connected-components/`merged_into` machinery one level up. The render contract
  `{ display_name, canonical_id?, confidence_band, evidence, avatar_ref? }` is the reason-record (above)
  reshaped for the frontend (frontend itself is out of this design track — §1).

---

## 6. Cross-cutting concerns

### Observability  *(DECIDED — KPIs TBD)*  — **two layers**
- **Infra:** `tracing` → OpenTelemetry bridge (OTel Rust SDK is **0.32, sub-1.0**: metrics & logs
  APIs **stable**, traces **beta** — pin versions, expect churn). `tokio-metrics` 0.5 for
  runtime/task health (slow-poll detection). `tower-http` `TraceLayer` / `axum-otel` for HTTP
  spans. Sentry panic hook (`sentry` 0.48) for crash capture. Metrics facade choice **open**:
  `metrics`-rs vs `prometheus-client` (the latter if we want exemplars linking histograms↔traces).
- **Domain/eval — the trace unit is one *job*, not an end-to-end per-event flow** *(DECIDED 2026-06)*.
  The pipeline is **data-coupled, not control-coupled** (the tick is the sole enqueuer; stages are
  joined by watermarks, not calls) and **many-to-many** (one `PublicBuild` feeds every digest that
  window; one poll's events feed many clusters feed many digests). A trace is a *tree*; this DAG has
  shared fan-in nodes, so a single "RSS-item → inbox" trace would lie about ownership. Therefore:
  - **One trace per job execution** (`PollConnection` / `PublicBuild` / `GenerateDigest`); the value
    is the **internal tree** — e.g. digest = `collect_candidates → select → create_with_items →
    render → send_email → mark_delivered`, which separates *our* time from the external SMTP send.
    Root span carries `task_id` (apalis ULID — stable id *and* enqueue clock), `attempt`, `wait_ms`
    (enqueue→pickup) and `elapsed_ms`.
  - **Cross-job causality = span *links* + correlation attributes, not parent-child.** Attributes
    (`connection_id`, `subscriber_id`, `window_end`, `source`) join spans across independent traces;
    links express many-to-many lineage (`GenerateDigest` → the `PublicBuild` it gated on → the polls
    that inserted its events) without a false tree.
  - **The one place real cross-queue propagation fits: webhooks (M2).** An inbound request causes
    *one* `ProcessWebhook` job (1:1) → propagate W3C context (apalis `TracingContext`) from server
    span into job span. The tick→job edge is 1:N and decoupled — do **not** propagate there.
  - **Metrics are the primary signal; traces are secondary.** Daily-ops questions are gauge/
    distributional — queue depth & oldest-pending, build lag, poll p99, delivery success rate, and
    pipeline counters (ingested, dedup-collapsed, clustered, linked, gated-out, delivered). These come
    from counters + the `debug status` aggregates promoted to gauges, **not** from traces. Traces are
    for the per-job breakdown and the rare "this digest took 30 s — autopsy" case; don't build
    elaborate cross-job trace stitching. **Product KPIs** (story precision, false-positive rate,
    §10.3 product doc) derive from the feedback log.
  - **Reason records** *are* decision-level observability — emit as structured span events (the M1
    selection trace — `selection complete` + per-candidate `Verdict` — is the first instance).
  - **M1 state → M5 wiring.** Instrumentation already emits **structured** `tracing` spans/events
    (job spans + timing + selection trace), so M5 is a *wiring* change (add the OTel layer in
    `main.rs`), not re-instrumentation. Field→metric conversion needs one step: either
    `tracing-opentelemetry` `MetricsLayer` field-name conventions
    (`histogram.`/`counter.`/`monotonic_counter.`) or a collector span-metrics processor.
    **KPI definitions deferred.**

### Reliability  *(DECIDED)*
- `panic = "unwind"` + a panic hook into telemetry; the apalis worker loop isolates a panicked
  job (→ retried via attempts → dead-letter; worker survives).
- Graceful shutdown via `Monitor::run_with_signal` (drains in-flight on SIGTERM).
- **Idempotency we *prove*:** the doc's strong restart claims (the **consideration floor** advances
  only after delivery; `window_end` = scheduled boundary so retries collapse on the unique key; each
  stage idempotent) are verified with **DST** (`turmoil`/`madsim`) injecting crashes between stages +
  **proptest** asserting invariants (no event lost from consideration, no double-send, **scope never
  crosses — on the build path especially**, dedup idempotent, linking deterministic + id-stable
  across recompute, missed boundaries coalesce to one). Prereq: an **injectable clock** — no ambient
  "now" in logic (also makes timezone/DST scheduling and recency decay deterministic to test). Every
  read of "now" — lookback bounds, decay, `next_run_at` — flows through it.

### Testing stack  *(DECIDED — "proptest + DST as much as possible")*
`proptest` (1.11) + `insta` (1.47) in `core` over fixtures; `testcontainers` (0.27 + modules
0.15) for real-Postgres integration (queue, RLS); `turmoil` (0.7) / `madsim` (0.2) for
DAG/idempotency simulation; `loom` (0.7) for any lock-free concurrency. `cargo-nextest` as the
runner.

### Config  *(DECIDED)*  — hand-rolled **TOML** (schema TBD).

### Secrets  *(in-memory DECIDED; at-rest TO DESIGN)*
- In memory: `secrecy` (0.10) + `zeroize` (1.8) — redacted `Debug`, zeroized on drop, explicit
  `.expose_secret()`.
- At rest: **envelope encryption** — store only `creds_ref` + a wrapped data key; plaintext DEK
  lives only in a `SecretBox`. v1 if no managed KMS: a single app master key (sealed file/env) +
  XChaCha20-Poly1305 over per-connection creds, with the `creds_ref` indirection preserved so we
  can swap to `aws-sdk-kms` later as a backend change, not a schema change. **Rotation/revocation
  + the exact schema to design.** v1 secrets are modest: GitHub App key, Slack tokens, webhook
  signing secrets (RSS needs none).

### SSRF guard  *(DECIDED)*  — guards the **pull path** (RSS = arbitrary URLs) *and* hydration
Roll a **resolve-then-pin** `reqwest` (0.13) resolver: resolve hostname → reject
RFC1918/loopback/link-local/CGNAT/ULA + `169.254.169.254` → connect to the *pinned* IP (defeats
DNS rebinding). Validate the post-`url`-crate IP directly (decimal/hex/octal encodings bypass
naive checks — per the Vaultwarden CVE). Re-validate every redirect hop; strict timeouts + size
caps. Optional egress proxy/NAT allowlist as a network-layer backstop.

### Supply chain  *(DECIDED)*  — `cargo-deny` (0.19, advisories+licenses+bans) on every PR + a
daily scheduled re-run against an unchanged lockfile.

---

## 7. Build & environment (Nix)  *(DECIDED — scaffolding to build)*

`crane` (0.23.4, workspace-aware, caches the dep tree once and shares it across
clippy/nextest/fmt) + `oxalica/rust-overlay` (pure eval, reads `rust-toolchain.toml`) on
`flake-parts`. `devenv.sh` for declarative Postgres in the dev shell. `cargo-nextest` via
`craneLib.cargoNextest` as a `nix flake check`. CI on GitHub Actions:
`DeterminateSystems/nix-installer-action` + **Cachix** for the binary cache
(magic-nix-cache is deprecated/fragile — avoid). Minimal OCI images via
`dockerTools.streamLayeredImage`, non-root, `cacert` only.

---

## 8. Sources (v1) & external API notes  *(DECIDED)*

**v1 = RSS + GitHub + Slack** (Slack is the private-scope exemplar). **All paid/compliance-gated
sources deferred.**
- **RSS:** poll w/ conditional GET (ETag / Last-Modified); `rss`,`atom` parses (no HTTP — pair with
  the SSRF-guarded `reqwest`). Cursor = validators.
- **GitHub:** use a **GitHub App** (15k req/hr/install, fine-grained perms) — not PATs. Webhooks
  primary + REST/GraphQL backfill.
- **Slack:** **HTTP Events API** for prod (Socket Mode is dev-only). Scope-gated events.
- **Deferred — Gmail:** `gmail.readonly` is a *restricted* scope → annual Google verification +
  **CASA assessment commonly $15k–$75k/yr**. This is why Slack is the v1 private source.
- **Deferred — Twitter/X:** now **pay-per-use** default (~$0.005/read still valid; Basic/Pro
  legacy-only; Enterprise ~$42k/mo).
- **Later — Bluesky:** Jetstream (JSON firehose) for streams; per-user feed via App View polling;
  `atrium-api`. **Later — Mastodon:** user-token required since 4.2.0; refresh-token migration
  coming in 5.0.
- **Later — Calendar (the first forward-looking source):** ICS/webcal poll → CalDAV `sync-token`
  → Google/MS Graph for push + RSVP. Emits prospective events (future `event_time`, high
  `confidence`); recurrence (RRULE) expands **lazily** inside the lookahead horizon against the
  subscriber's wall clock (DST-correct). Connector **and** the prospective-event model are both
  deferred (§8.5 product) — forward-compatible, re-add without schema rework.

---

## 9. Dependency & version pins  *(research snapshot 2026-06 — re-verify before locking)*

| Area | Choice | Version / note |
|---|---|---|
| Jobs | apalis + apalis-postgres / apalis-cron | `=1.0.0-rc.9` / `=1.0.0-rc.8` (pin exact; `unique-jobs` feature) |
| DB | sqlx (raw SQL, compile-time checked) | 0.9 (just released — re-verify); shared `PgPool`; per-txn `SET LOCAL app.subscriber_id` for two-context RLS (product §12) |
| IDs | uuid (v7) + Postgres 18 `uuidv7()` | uuid 1.x `now_v7()` |
| Async util | tokio-util | 0.7.18 (`CancellationToken`/`TaskTracker` if needed beyond Monitor) |
| Telemetry | opentelemetry / _sdk / -otlp; tracing-opentelemetry | 0.32 / 0.31 |
| Runtime metrics | tokio-metrics | 0.5 |
| HTTP | axum + tower-http `TraceLayer`; axum-otel | tower-http 0.6.8 |
| Crash capture | sentry (panic hook) | 0.48 (optional) |
| Secrets | secrecy / zeroize / aws-sdk-kms (later) | 0.10 / 1.8 |
| HTTP client | reqwest (custom DNS resolver) | 0.13 |
| Testing | proptest / insta / testcontainers(+modules) / turmoil / madsim / loom | 1.11 / 1.47 / 0.27(+0.15) / 0.7 / 0.2 / 0.7 |
| Supply chain | cargo-deny (+cargo-audit) | 0.19 |
| Feeds | rss/atom | — |
| Rate limit | governor | — |
| Nix | crane / oxalica rust-overlay / cargo-nextest | 0.23.4 |

---

## 10. Open threads — what's left to design & scope

> **As-built (2026-06-14):** items 1–3 and 5–6 below shipped with M1–M3 (the `core` modeling pass,
> the connection/poll spec, the build + linking SQL, secrets-at-rest, and two-context RLS). The Thread
> layer (item 14) has its first slice implemented (`thread-layer.md` §9). What remains is the
> **M4–M6 backlog**: the config-table thresholds + reason-records-as-types (item 1 remainder, M4), the
> read API + connect flow (item 7, M5), KPIs/eval harness (item 9), full observability wiring (item 11),
> timezone/DST + prospective events (item 12), and data lifecycle/GDPR (item 13). The list is kept below
> as the design backlog; treat the "in progress / next-up" framing as historical.

Proposed sequence (next-up first):

1. **`core` modeling pass** *(shipped; reason-records-as-types is the M4 remainder)* — `TokenProvider` +
   the `connect_pull`/`connect_push` factory shape; the pure selection function. (§5.5, §3A)
2. **Connection/poll spec finalize** *(shipped)* — `connection` DDL deltas, concrete backoff policy values,
   `PollConnection` as a written spec, the three sweep queries + `unique-jobs` keys. (§3)
3. **Build + linking SQL** (product §15 open) — `PublicBuild`: group public events into `cluster`
   rows by `(scope, source, group_key)` + rollups. Per-subscriber linking: blocking-seeded candidate
   selection (`cluster_entities` GIN; affinity ∪ shares-strong-key-with-own-private), edge scoring,
   **connected-components**, and the id-forwarding/`merged_into` rule (§5.3, product §8.2).
4. **Pure selection function** — gate/rank/classify as `fn(features)->decision`; fixtures;
   `relevance_floor`/richness threshold/caps config table; proptest invariants.
5. **Secrets at-rest** — KMS vs interim master key; `creds_ref` + wrapped-DEK schema; rotation.
6. **Two-context RLS mechanism** (product §12) — `public-build` no-subscriber context vs subscriber
   context (`SET LOCAL app.subscriber_id`); `FORCE ROW LEVEL SECURITY`, non-owner runtime role, no
   `BYPASSRLS`, separate migration role, a `with_scope(ctx, …)` wrapper as the only connection path.
   RLS is the backstop; typed `Scope` + a build-path scope-invariant property test are primary.
7. **Read API + connect-flow MVP** — OAuth/app-install endpoints (`/connect/{source}` + callback,
   signed `state` → bind provider-account→subscriber→scope, §3A); drill-down (provenance timeline),
   feedback endpoints, authenticated deep-link digest view; authz/IDOR re-checks; user auth/session.
8. **Crate graph finalize** — names + granularity.
9. **KPIs + eval harness** over the feedback log (story precision, FP rate).
10. **Nix/CI scaffolding** — flake, devShell, checks (can run as a parallel track).
11. **Observability specifics** — span/trace design through the DAG **DECIDED** (per-job traces +
    span links + metrics-primary; §6 Observability). **Still open:** metrics facade (`metrics`-rs vs
    `prometheus-client`, exemplars?) and the counter/gauge taxonomy.
12. **Time/scheduling** *(v1)* — **recurrence**: daily/weekly at a local `at_time` (+ `on_weekday`
    for weekly), computed on the subscriber's wall clock then anchored to UTC (DST-safe); **monthly
    dropped**. One boundary function backs signup, preference change ("snap to next earliest, lose
    nothing"), and advance; advance **coalesces** missed boundaries into one catch-up (product §9.2).
    Selection is a **freshness-scored lookback** over the durable log, not a window partition (product
    §9.4). `chrono` vs `time`; injectable clock (load-bearing — §6 Reliability); `window_end` =
    scheduled boundary (product §9.3). Timezone correctness threaded through storage (UTC
    `timestamptz`), boundary math (subscriber tz), and rendering — never reasoned about in UTC.
    **Deferred — forward-looking** (§8.5 product): future-valued `event_time`, the signed-gap
    **salience curve** + global `lookahead`, **lazy RRULE expansion** (DST-correct), and `confidence`
    as a priority modulator + cached rollup. Re-adds without schema rework.
13. **Data lifecycle/retention** — inline raw-payload horizon (`event.raw`, TOASTed; object-storage
    offload via `raw_ref` deferred), GDPR per-subscriber deletion cascading to `raw` + reasons.
14. **Deferred — Thread layer & tiered identity** (sequences after linking/relevance; `thread-layer.md`):
    the `thread`/`entity_edge` schema + the `thread_maintenance` job (community detection w/ LPA-vs-Louvain
    choice, id-forwarding, dormancy/decay, weight projection); the pure resolution + thread-formation
    functions (proptest determinism + id-stability, like linking); `ConfidenceBand` + the render contract;
    GDPR delete must also cascade to `thread`/`entity_edge`. RLS: `thread_maintenance` runs in the
    subscriber context (no new cross-tenant path).
