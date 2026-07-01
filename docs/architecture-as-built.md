# Bulletin — Technical Architecture (As-Built)

> A component-by-component reference for the Bulletin engine as it exists in the tree today,
> backed by verbatim source excerpts. Where this document and the code disagree, the code wins —
> every excerpt is tagged with its `path:line-range` so you can jump to the source of truth.

**Snapshot**

| | |
|---|---|
| Repository | `jonaskruckenberg/bulletin` |
| Base commit | `e8fa3bf` (2026-06-24) |
| Language / toolchain | Rust (stable; rustfmt, clippy, rust-src) |
| Datastore | PostgreSQL 18 (single database) |
| Crates | `bulletin-core` (engine, ~22.3k LoC) · `bulletin` (binary, ~3.3k LoC) |
| Milestones built | M1 pipeline · M2 GitHub + webhooks + private scope + two-context RLS + envelope-encrypted creds · M3 per-subscriber cross-source linking · first slice of the Thread layer & tiered identity (behind `thread-weighting`) |
| Sources | RSS · GitHub |

This document is organized in two parts. **Part I** is the system-level view: the thesis, the process
topology, the cross-cutting invariants (scope isolation, RLS, watermarks), the data model, and the
end-to-end request lifecycle. **Part II** is the per-component reference, one section per module, each
with the key types, algorithms, SQL, and design decisions quoted from the source.

**Contents**

- Part I — System architecture: [Thesis](#1-thesis-and-shape) · [Topology](#2-process-topology) · [Layout](#3-repository-layout) · [Invariants](#4-cross-cutting-invariants) · [Data model](#5-data-model) · [Lifecycle](#6-end-to-end-lifecycle)
- Part II — Component reference (`bulletin-core`):
  - [`common`](#common--shared-vocabulary) — RLS/scope, event, fingerprint, entity, kind, salience, link-safety, secret, status, watermark
  - [`ingest`](#ingest--connectors--event-log) — connectors (RSS, GitHub), fetch, webhooks, event-log append
  - [`cluster`](#cluster--event-log--clusters) — event log → cluster rollups
  - [`link`](#link--deterministic-cross-source-story-fusion) — cross-source story fusion
  - [`identity`](#identity--tiered-probabilistic-entity-resolution) — entity resolution
  - [`thread`](#thread--the-cross-time-weave) — background thread maintenance + weighting
  - [`enrich`](#enrich--phase-2-llm-entity-mining) — pre-cluster LLM entity mining
  - [`feedback`](#feedback--append-only-correction-log) · [`subscription`](#subscription)
  - [`digest`](#digest--select-render-send) — select → render → send
  - [`summarize`](#summarize--write-side-llm-pre-summarization--on-path-lead) — LLM summaries + faithfulness gate
- Part II — Component reference (binary): [`bulletin`](#binary-crate-bulletin--orchestration) — main, worker, webhook, gRPC API, transport, secrets, metrics, debug

---

# Part I — System architecture

## 1. Thesis and shape

Bulletin is a self-hosted digest engine: it ingests a subscriber's sources, suppresses noise, draws
cross-source connections, and emails a scheduled digest of the few things that matter. Two theses
drive every design decision:

- **Product thesis:** *suppress noise, elevate the few things that matter, earn trust.*
- **Architecture thesis:** *a Postgres-orchestrated scheduled batch pipeline, not a service mesh.*

Concretely, the engine is **three flows over a chain of append-only logs drained by idempotent
cursors**, as the crate's own module doc states:

```rust
// crates/core/src/lib.rs:1-25
//! The bulletin engine: three flows over a chain of append-only logs drained by idempotent
//! cursors.
//!
//! - [`ingest`] — poll connectors, normalize, append to the event log.
//! - [`cluster`] — drain the event log (build-watermark cursor) into `cluster` rows.
//! - [`link`] — per subscriber, fuse candidate clusters into cross-source `story`s (the pure,
//!   deterministic linking core; design §8.2). Runs inside the digest flow.
//! - [`digest`] — link a subscriber's candidate clusters into stories, select by recency, render, send.
//! - [`thread`] — the cross-time weave: a background `thread_maintenance` job that turns the
//!   subscriber's stories into persistent `Thread`s and a projected entity-weight map the digest's
//!   relevance term reads (`docs/thread-layer.md`).
//! - [`identity`] — tiered, probabilistic entity-identity resolution that feeds the thread layer.
//! - [`enrich`] — Phase-2 best-effort LLM entity/topic enrichment: grounded `place:`/`org:`/`person:`/
//!   `topic:` tokens mined per item *before* clustering, so cross-publisher coverage of one happening
//!   fuses into one story/thread. Off the punctual path; a failed/disabled call leaves the item
//!   fully usable with the entities it already has.
//! - [`summarize`] — write-side LLM pre-summarization (Phase A: the content-hashed `cluster.summary`
//!   foundation) and the on-path digest lead (Phase D). A mandatory part of the pipeline (§3.7): a
//!   cluster ships only with a faithful, gate-passed summary and a digest never ships without an LLM
//!   lead; failures are tracked errors with bounded escalating retries and quarantine
//!   (`docs/llm-summarization.md`).
//! - [`feedback`] — the append-only correction log (care/less, must/cannot-link).
```

The pipeline, end to end:

```
                     ┌── poll (correctness floor) ──┐
   RSS / GitHub ─────┤                              ├──► event log ──► cluster ──► (summarize A)
                     └── webhook (freshness) ───────┘        │           │
                                                     (enrich, pre-cluster)│
                                                                          ▼
   per subscriber, at fire time:   candidate clusters ──► link ──► stories ──► select ──► render ──► send
                                                            │                     ▲
   background, off the hot path:   thread_maintenance ──────┴──► entity-weight map┘   (relevance term)
                                   identity ◄── feedback (must/cannot-link, care/less)
```

## 2. Process topology

One binary, `bulletin`, runs in one of several **roles** chosen by subcommand. In production a single
`bulletin all` process runs `serve` + `worker` + `api` together; the roles also split apart for scaling
or ops. The role dispatch and the two-role DB split are in `main.rs` (see the binary-crate section for
the full excerpt). The salient points:

- **`serve`** — the axum HTTP edge: `/health` liveness + `/webhooks/github` (HMAC-verified, enqueue-and-return).
- **`worker`** — the apalis job runtime + a cron tick that enqueues due work; also the Prometheus exporter.
- **`api`** — the tonic gRPC admin plane (control-plane management + operator/debug RPCs), fail-closed on a bearer key.
- **`all`** — all three, joined; the health probe gates on the summarization sidecar being reachable first.
- **`migrate`** — runs as the *owner/migration role*; applies DDL, sets up the apalis queue, re-grants the runtime role.
- **`secrets`** — offline key tooling (keygen / seal); needs no database.
- **`debug`** — a thin gRPC *client* of `api`; opens no DB and holds no SMTP secret.

Two Postgres credentials enforce a privilege boundary at the login level: an **owner/migration role**
that owns the DDL, and a least-privilege **runtime role** (`bulletin_app`: non-owner, `NOBYPASSRLS`)
that `serve`/`worker`/`api` log in as — the prerequisite that makes `FORCE ROW LEVEL SECURITY` bind
(§4).

## 3. Repository layout

```
crates/
  core/                     bulletin-core — the engine (no triggers, no metrics, no transport)
    src/
      common/               shared vocabulary: db/RLS, scope, event, fingerprint, entity, kind,
                            salience, link_safety, secret, status, watermark
      ingest/               connectors + event-log append (rss, github/, fetch, html_text, realtime, store)
      cluster/              event log → cluster rollups
      enrich/               Phase-2 pre-cluster LLM entity mining
      summarize/            write-side LLM summaries + faithfulness gate + on-path lead
      link/                 deterministic cross-source story fusion
      identity/             tiered probabilistic entity resolution
      thread/               background thread_maintenance + fire-time weighting
      digest/               select → render → send
      feedback.rs           append-only correction log
      subscription/         subscriber ↔ connection membership
    migrations/             37 append-only SQL migrations (expand-contract discipline)
    tests/                  integration tests (testcontainers Postgres)
  bulletin/                 the binary: main, worker, webhook, api/, transport, secrets, metric, debug
    proto/                  gRPC contract (compiled by build.rs via protox)
docs/                       design & reference docs (system-design, technical-architecture, thread-layer, …)
ops/                        Grafana dashboard + wipe-db helper
flake.nix / nix/           Nix package, overlay, NixOS module
```

## 4. Cross-cutting invariants

Four invariants recur in every component; understanding them once explains most of the code.

### 4.1 Scope isolation (public → private only)

Every event and cluster carries a `Scope` — `Public` (shared) or `Private(subscriber)` (owner-only).
Information flows public→private only. This is enforced in **three layers, defense in depth**:

1. **Typed `Scope` in the identity.** Scope is part of the cluster key `(scope, source, group_key)`, so
   a public and a private event with the same source+group can never fold into the same cluster.
2. **Query predicates.** Every candidate read is `scope_kind = 'public' OR scope_subscriber_id = $me`.
3. **Two-context row-level security.** The runtime role runs each unit of work through `with_scope(ctx, …)`,
   which sets a transaction-local `app.subscriber_id`. `FORCE ROW LEVEL SECURITY` then makes Postgres
   itself refuse a row a logic bug would otherwise leak.

The content-table policies (migration 0019) — `event`/`cluster`, keyed on the GUC:

```sql
-- crates/core/migrations/20200101000019_rls.sql:41-61
ALTER TABLE event ENABLE ROW LEVEL SECURITY;
ALTER TABLE event FORCE ROW LEVEL SECURITY;

-- Read: public always; own-private only when this subscriber's context is set.
CREATE POLICY event_select ON event FOR SELECT
   USING (
      scope_kind = 'public'
      OR scope_subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );

-- Write (append-only log → INSERT): a public row only in the no-subscriber context; a private row
-- only as its owner. A subscriber context can never inject into the shared public pool, and can
-- never write another tenant's private row — the directional public→private invariant, enforced.
CREATE POLICY event_insert ON event FOR INSERT
   WITH CHECK (
      (scope_kind = 'public'
         AND nullif(current_setting('app.subscriber_id', true), '') IS NULL)
      OR
      (scope_kind = 'private'
         AND scope_subscriber_id::text = nullif(current_setting('app.subscriber_id', true), ''))
   );
```

The control-plane/delivery tables (migration 0020) are **fail-closed**: the no-subscriber context is
denied entirely, a subscriber context sees only its own rows, and a `*` **admin** sentinel is the only
cross-tenant reach — used by the cron sweeps, status, and operator commands, and even then it has no
backdoor to another tenant's private *content* (the content policies above have no `*` branch):

```sql
-- crates/core/migrations/20200101000020_rls_control_plane.sql:34-44
ALTER TABLE connection ENABLE ROW LEVEL SECURITY;
ALTER TABLE connection FORCE ROW LEVEL SECURITY;
CREATE POLICY connection_scope ON connection FOR ALL
   USING (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   )
   WITH CHECK (
      current_setting('app.subscriber_id', true) = '*'
      OR subscriber_id::text = nullif(current_setting('app.subscriber_id', true), '')
   );
```

The three contexts map to the `ScopeCtx` enum in `common::db`, and `with_scope`/`begin_scope` are the
only sanctioned way to open a scoped transaction (see the `common` section).

### 4.2 Append-only logs + monotonic watermarks

Durable truth is the append-only `event` log. Everything downstream — clusters, stories, digests — is a
recomputable projection, advanced by cursors that only ever move forward (`GREATEST(...)`). This makes
every stage idempotent and crash-safe: a poll commits its events *before* advancing its cursor, so a
crash re-fetches the overlap and `UNIQUE(fingerprint)` collapses the duplicates. The public build has a
singleton cursor; private build and thread maintenance have per-subscriber cursors (see `common::watermark`).

### 4.3 The LLM sidecar is off the punctual path

Summarization and enrichment run against a **local** llama-server sidecar (OpenAI-compatible,
grammar-constrained JSON, no egress). Phases A–C (cluster summaries, story synthesis, thread labels) and
enrichment run in background sweeps; only the **Phase-D digest lead** is on the send path, and it is
deadline-bounded with a deferral escape (the job re-runs later rather than shipping a lead-less digest).
The worker refuses to boot if the sidecar is unreachable, so a misconfigured box fails its health probe
instead of quarantining the corpus.

### 4.4 Fail-closed everywhere

No secret ⇒ no feature. An absent webhook secret 401s every delivery; an unset admin key rejects every
RPC; missing GitHub credentials disable GitHub (RSS unaffected) with a clear log. RLS denies rows the
query forgot to filter. The defaults are safe, and degradation is explicit.

## 5. Data model

The schema is defined by 37 append-only migrations (`crates/core/migrations/`), applied with
expand-contract discipline (never edit an applied file — sqlx checksums them; roll forward with a new
file). The load-bearing tables:

| Table | Role |
|---|---|
| `event` | the append-only event log (scope-bearing content) |
| `cluster` | within-source rollups drained from the event log (scope-bearing content) |
| `story` | per-subscriber cross-source recompute (Private-scoped); the frozen selection unit |
| `digest` / `digest_item` | the per-window delivery record and its frozen story list |
| `connection` | a configured source (RSS feed / GitHub installation) + poll schedule + owner |
| `subscriber` | identity, schedule, timezone, PII |
| `subscription` | subscriber ↔ connection membership |
| `thread` / `entity_edge` | the Thread layer + tiered-identity graph |
| `build_watermark` / `private_build_watermark` / `thread_maintenance_watermark` | the cursors |
| `apalis.*` | the job queue (control-plane infra) |

The `event` table is the root of everything; note the content-independent fingerprint and the scope
CHECK constraint that makes a private-without-owner row structurally impossible:

```sql
-- crates/core/migrations/20200101000002_event.sql:1-23
CREATE TABLE event (
    id                  uuid        NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    fingerprint         bytea       NOT NULL,
    source              text        NOT NULL,
    scope_kind          text        NOT NULL,  -- 'public' | 'private'
    scope_subscriber_id uuid            NULL,  -- set iff scope_kind = 'private'
    event_time          timestamptz NOT NULL,
    title               text        NOT NULL,
    body                text            NULL,
    links               text[]      NOT NULL DEFAULT '{}',
    group_key           text        NOT NULL,
    entities            text[]      NOT NULL DEFAULT '{}',
    content_kind        text        NOT NULL,
    severity_hint       smallint        NULL,
    ingest_time         timestamptz NOT NULL DEFAULT now(),
    raw                 bytea           NULL,

    CONSTRAINT event_fingerprint_unique UNIQUE (fingerprint),
    CONSTRAINT event_scope_check CHECK (
        (scope_kind = 'public'  AND scope_subscriber_id IS NULL) OR
        (scope_kind = 'private' AND scope_subscriber_id IS NOT NULL)
    )
);
```

The `story` table captures the M3 model precisely — a per-subscriber *recomputed read-model*, always
Private-scoped, with membership (and per-member link rationale) living on the story so a shared public
cluster can belong to many subscribers' stories, plus the `merged_into` tombstone and `last_delivered_at`
gate that power stable id-forwarding and the asymmetric-merge rule:

```sql
-- crates/core/migrations/20200101000018_story.sql:9-31
CREATE TABLE story (
    id               uuid        NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    subscriber_id    uuid        NOT NULL REFERENCES subscriber(id) ON DELETE CASCADE,
    -- Set when a retro-merge forwards this id to its survivor (the oldest id wins, §8.2); the row
    -- becomes a tombstone (empty `clusters`) that redirects a stale deep-link. NULL = a live story.
    merged_into      uuid            NULL REFERENCES story(id),
    -- Membership + per-member rationale: [{cluster_id, link_reason}] (design §10.2). This *is* the
    -- persisted prior assignment the next recompute reads to forward stable ids.
    clusters         jsonb       NOT NULL DEFAULT '[]',
    -- Cross-source recency span (aggregated over the member clusters); `last_event_time` is the
    -- selection ordering key, mirroring the cluster rollup the digest used pre-M3.
    first_event_time timestamptz NOT NULL,
    last_event_time  timestamptz NOT NULL,
    -- Stamped when a digest carrying this story is delivered. Gates the asymmetric-merge rule: only a
    -- *strong* edge may merge two already-delivered stories, so a weak link can't silently collapse
    -- two stories the subscriber has already seen as distinct (§8.2 single-linkage guard).
    last_delivered_at timestamptz    NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- The recompute loads a subscriber's live stories (the prior assignment) by owner.
CREATE INDEX story_subscriber ON story (subscriber_id) WHERE merged_into IS NULL;
```

## 6. End-to-end lifecycle

Tracing one item from source to inbox ties the components together:

1. **Ingest.** The cron tick finds due connections and enqueues a poll job per connection; GitHub also
   pushes webhooks to the HTTP edge. Each connector normalizes its items to `EventBuilder`s, `finalize()`
   stamps the immutable `Scope` (derived from the *connection row*, never the payload) and the
   content-hash `Fingerprint`, and events are appended under RLS with `ON CONFLICT DO NOTHING` dedup.
2. **Enrich (best-effort, pre-cluster).** For enrichable public events (RSS), a background sweep mines
   grounded `place:`/`org:`/`person:`/`topic:` tokens and unions them onto the event before clustering,
   so cross-publisher coverage of one happening can fuse. A failed call leaves the event fully usable.
3. **Cluster.** The public build (advisory-locked, watermark-bounded to `now() - enrich_grace`) folds
   each dirty `(scope, source, group_key)` group into a `cluster` rollup. Private build is the
   per-subscriber counterpart.
4. **Summarize (Phase A, write-side).** A background sweep produces a content-hashed, faithfulness-gated
   `cluster.summary`; only `confirmed`/`probable` clusters become digest candidates.
5. **Fetch (best-effort).** An SSRF-guarded sweep fetches the real article behind an RSS link, enriches
   `event.full_text`, upgrades depth to Longform, and re-queues the cluster for re-summarization.
6. **Link + select (fire time, per subscriber).** When a subscriber is due, `link` fuses their candidate
   clusters (public ∪ own-private) into stories via graded probabilistic entity matching, the thread
   relevance term is added (if `thread-weighting` is on), and `select` gates → classifies → ranks → caps.
7. **Synthesize + lead + render + send.** Selected stories get inline Phase-C synthesis (deadline-bounded),
   the Phase-D authored lead is composed (retried/deferred to honor the "never ship lead-less" contract),
   the digest is frozen atomically, rendered to escaped/defanged HTML+plaintext, and delivered.
8. **Thread maintenance (background).** Off the hot path, per due subscriber, `thread_maintenance` builds
   the co-occurrence graph over stories, detects communities, id-forwards them onto prior threads, decays
   affinity, runs the state machine, and projects the entity-weight map the next fire's relevance term reads.
9. **Feedback.** Corrections append to a log; `must_link`/`cannot_link` materialize into the identity
   graph in the same transaction, and `care_more`/`care_less`/`done` fold into thread affinity next pass.

---

# Part II — Component reference

Each section below documents one module: purpose, key types, core algorithms, notable SQL, and design
decisions, quoted from the source with `path:line-range` tags.


---

## `common` — shared vocabulary

The `common` module of `bulletin-core` is the crate's shared vocabulary: the small, dependency-light types and functions that every higher layer (ingest, cluster, digest, enrich) agrees on. It owns the database seam and RLS chokepoint, the tenancy `Scope` encoding, the connector-facing event builder and its seal point, the dedup fingerprint, cross-source entity extraction, the source/content taxonomy, the salience scale, link-shaped-text safety, envelope encryption for credentials at rest, the `debug status` snapshot, and the per-subscriber watermark helpers. These are the pieces whose conventions must live in exactly one place so nothing downstream can drift.

### `db.rs`

**Purpose:** the single RLS-aware chokepoint through which every scoped query reaches Postgres, plus connect/migrate and runtime-role privilege plumbing.

Design §12's two-context Row-Level-Security model is anchored here. The app logs in as a least-privilege `RUNTIME_ROLE` (`bulletin_app`) with no `BYPASSRLS`; the scope-bearing content tables (`event`, `cluster`) carry `FORCE ROW LEVEL SECURITY` policies keyed on a transaction-local `app.subscriber_id` GUC. `ScopeCtx` selects which policy applies. The two table families interpret the same context differently: content tables give `Admin` only public reach (no cross-tenant backdoor), while the fail-closed control-plane/delivery tables deny the no-subscriber context entirely and treat `Admin` as the explicit cross-tenant orchestration reach.

`ScopeCtx::guc` renders each context to the GUC string the policies read — empty for no-subscriber, the UUID text for a subscriber, and the `*` sentinel for `Admin` (no real UUID is `*`, so it only ever satisfies the explicit control-plane `= '*'` clause).

```rust
// crates/core/src/common/db.rs:37-50
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScopeCtx {
    /// PublicBuild, public ingest, and public-only content reads. Maps to an empty `app.subscriber_id`
    /// — public content only, and **no** control-plane access (those tables fail closed here).
    NoSubscriber,
    /// One subscriber's context (private-build, generate, digest delivery): public ∪ own-private
    /// content, and own control-plane/delivery rows.
    Subscriber(Uuid),
    /// The control-plane context for trusted, cross-tenant orchestration that names no single
    /// subscriber: the cron tick's due-sweeps, `status`, the poll/webhook connection lookups, and
    /// operator/debug commands. Reaches every control-plane row — but, deliberately, **not** another
    /// tenant's private content (content tables treat it like the no-subscriber context: public only).
    Admin,
}
```

`set_scope` sets the GUC via `set_config(..., is_local => true)` — the function form of `SET LOCAL`, which accepts a bind parameter and is transaction-scoped, so it is pool- and PgBouncer-safe. `begin_scope` opens a transaction already pinned to a context, and `with_scope` wraps it with commit-on-`Ok` / rollback-on-`Err`.

```rust
// crates/core/src/common/db.rs:86-96
/// Sets the transaction-local `app.subscriber_id` GUC the RLS policies read. Uses `set_config(...,
/// is_local => true)` — the function form of `SET LOCAL`, so it accepts a bind parameter and is
/// transaction-scoped (auto-reset at COMMIT/ROLLBACK), which makes it pool- and PgBouncer-safe (no
/// leak onto the next checkout of a pooled connection). Must run inside a transaction to have effect.
pub async fn set_scope(executor: impl PgExecutor<'_>, ctx: ScopeCtx) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config('app.subscriber_id', $1, true)")
        .bind(ctx.guc())
        .execute(executor)
        .await?;
    Ok(())
}
```

```rust
// crates/core/src/common/db.rs:114-132
pub async fn with_scope<T, F>(pool: &PgPool, ctx: ScopeCtx, f: F) -> Result<T>
where
    F: for<'c> FnOnce(&'c mut PgConnection) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>,
{
    let mut tx = begin_scope(pool, ctx)
        .await
        .context("open scoped transaction")?;
    match f(&mut tx).await {
        Ok(value) => {
            tx.commit().await.context("commit scoped transaction")?;
            Ok(value)
        }
        Err(e) => {
            // Best-effort rollback; surface the original error regardless.
            let _ = tx.rollback().await;
            Err(e)
        }
    }
}
```

`grant_runtime_role` is deliberately *not* a checksum-frozen migration: it re-runs on every `migrate` so later tables (and the apalis queue schema, created after the domain migrations) are always covered. It is idempotent and must be run by the owner connection.

```rust
// crates/core/src/common/db.rs:142-152
pub async fn grant_runtime_role(pool: &PgPool) -> Result<(), sqlx::Error> {
    for stmt in [
        format!("GRANT USAGE ON SCHEMA public TO {RUNTIME_ROLE}"),
        format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {RUNTIME_ROLE}"
        ),
        format!("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {RUNTIME_ROLE}"),
    ] {
        sqlx::query(&stmt).execute(pool).await?;
    }
```

### `scope.rs`

**Purpose:** the typed tenancy boundary (`Public` vs `Private(owner)`) and its single on-disk column encoding.

`Scope` is the primary defense in the isolation model — a typed value a connector can never fabricate. Its `(scope_kind, scope_subscriber_id)` column pair is the one encoding shared by every scoped store (the `event` log and the `cluster` cache), so `to_columns`/`from_columns` keep the persistence convention in one place. Reading back, anything but the explicit `private` (which must carry its owner) resolves to `Public`.

```rust
// crates/core/src/common/scope.rs:4-31
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum Scope {
    Public,
    Private(Uuid),
}

impl Scope {
    /// The `(scope_kind, scope_subscriber_id)` column pair this scope persists as — the single
    /// encoding shared by every store that writes a scoped row (the `event` log and the `cluster`
    /// cache), so the on-disk convention lives in one place rather than a hand-written match per store.
    pub fn to_columns(&self) -> (&'static str, Option<Uuid>) {
        match self {
            Scope::Public => ("public", None),
            Scope::Private(subscriber_id) => ("private", Some(*subscriber_id)),
        }
    }

    /// Reconstructs a scope from the `(scope_kind, scope_subscriber_id)` column pair — the inverse of
    /// [`to_columns`], so the on-disk convention is read and written in one place. Anything but the
    /// explicit `private` (which must carry its owner) reads back as `Public`.
    pub fn from_columns(kind: &str, subscriber_id: Option<Uuid>) -> Result<Self, &'static str> {
        match kind {
            "private" => Ok(Scope::Private(
                subscriber_id.ok_or("private scope missing scope_subscriber_id")?,
            )),
            _ => Ok(Scope::Public),
        }
    }
}
```

### `event.rs`

**Purpose:** the connector-side `EventBuilder`, its infra-only seal point `finalize`, and the `Event`/`NewEvent` domain types plus their canonical row mapper.

A connector fills an `EventBuilder` with everything it knows and reports structural visibility only as a bare `is_private` bool — it can name no subscriber and construct no `Scope` (design §12 risk #1). Infra seals the builder via `finalize(owner)`, the one place a subscriber binding is created from a `(is_private, owner)` pair. `finalize` also computes the dedup fingerprint and runs shared entity enrichment (folding `entity::derive` in without disturbing the fingerprint, which excludes entities).

```rust
// crates/core/src/common/event.rs:116-134
    pub fn finalize(self, owner: Option<Uuid>) -> NewEvent {
        let scope = match (self.is_private, owner) {
            (true, Some(subscriber_id)) => Scope::Private(subscriber_id),
            _ => Scope::Public,
        };
        let fingerprint = Fingerprint::compute(self.source.as_str(), &self.stable_id);

        // Enrich the connector's structural entities (`repo:`/`user:`) with the cross-source keys
        // (`cve:`/`url:`/`domain:`) mined from this event's text + links, in one place so every
        // source gets them uniformly — they are the blocking substrate M3 linking runs on (§8.2).
        // Entities are *not* folded into the fingerprint, so enrichment never disturbs dedup.
        let mut entities = self.entities;
        entities.extend(super::entity::derive(
            &self.title,
            self.body.as_deref(),
            &self.links,
        ));
        entities.sort();
        entities.dedup();
```

`Event::best_text` is the single grounding-text accessor every downstream raw-text tier reads (`summary_hash`, `extract_facts`, `source_corpus`), so they uniformly prefer the fetched article over the connector snippet, and a source whose body *is* the content degrades to `body` for free.

```rust
// crates/core/src/common/event.rs:203-208
    pub fn best_text(&self) -> Option<&str> {
        match self.full_text.as_deref() {
            Some(t) if !t.trim().is_empty() => Some(t),
            _ => self.body.as_deref(),
        }
    }
```

### `fingerprint.rs`

**Purpose:** the content-independent dedup key that lets `ON CONFLICT DO NOTHING` collapse a re-polled item.

`Fingerprint` is a SHA-256 of `(source, stable_id)` only — never the mutable content — so re-polling the same item with changed title/body produces the same key. The hash is **length-framed**: each field is prefixed with its byte length as a little-endian `u64`, so no two distinct `(source, stable_id)` pairs can produce the same byte stream by concatenation ambiguity.

```rust
// crates/core/src/common/fingerprint.rs:7-18
impl Fingerprint {
    pub fn compute(source: &str, stable_id: &str) -> Self {
        let mut h = Sha256::new();
        let src = source.as_bytes();
        let id = stable_id.as_bytes();
        h.update((src.len() as u64).to_le_bytes());
        h.update(src);
        h.update((id.len() as u64).to_le_bytes());
        h.update(id);
        Self(h.finalize().into())
    }
}
```

### `entity.rs`

**Purpose:** the blocking substrate for cross-source linking — namespaced `kind:value` tokens, their strong/weak link classification, and the shared derivation of CVE ids and URLs/domains.

The namespace prefix is load-bearing: it prevents cross-namespace collisions and classifies each entity as **strong** (a CVE or exact URL — a near-certain connection that may merge anything) or **weak** (a distinctive named entity that links only when corroborated). `link_strength` encodes design §8.2's asymmetric-merge guard, including the deliberate non-linking of `domain:`/`topic:` and of bot actors.

```rust
// crates/core/src/common/entity.rs:50-72
pub fn link_strength(entity: &str) -> Option<LinkStrength> {
    if entity.starts_with("cve:") || entity.starts_with("url:") {
        Some(LinkStrength::Strong)
    } else if let Some(login) = entity.strip_prefix("user:") {
        // A *bot* actor (`renovate[bot]`, `dependabot[bot]`, `github-actions[bot]`) touches a great many
        // unrelated repos, so a shared bot is noise as a link key — it fused every repo Renovate runs on
        // into one blob. Only a human actor corroborates a connection; a bot is non-linking (it still
        // rides on the cluster for display, it just never forms an edge by itself).
        if is_bot_login(login) {
            None
        } else {
            Some(LinkStrength::Weak)
        }
    } else if entity.starts_with("repo:")
        || entity.starts_with("place:")
        || entity.starts_with("org:")
        || entity.starts_with("person:")
    {
        Some(LinkStrength::Weak)
    } else {
        None
    }
}
```

`derive` is the shared, source-agnostic layer run once at the seal point: it mines URLs (from structured links first, then free text) and CVE ids from the title/body, returning sorted, de-duplicated tokens for a deterministic stored array.

```rust
// crates/core/src/common/entity.rs:86-104
pub fn derive(title: &str, body: Option<&str>, links: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();

    // Structured links first — these are the cleanest URL signal (no parsing ambiguity).
    for link in links {
        push_url(&mut out, link);
    }
    // Then mine the free text: CVE ids and any inline URLs.
    for text in std::iter::once(title).chain(body.iter().copied()) {
        push_cves(&mut out, text);
        for token in text.split_whitespace() {
            push_url(&mut out, token.trim_end_matches(is_url_trailer));
        }
    }

    out.sort();
    out.dedup();
    out
}
```

### `kind.rs`

**Purpose:** the source and content taxonomies, generated by a `text_enum!` macro that binds each variant to its DB text, plus the per-source capability predicates.

`text_enum!` declares an enum and its `as_str`/`TryFrom<&str>` from one variant→literal table so the two string mappings can't drift; it preserves declaration order (making the derived `Ord` meaningful for `ContentKind`).

```rust
// crates/core/src/common/kind.rs:6-34
macro_rules! text_enum {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident { $( $variant:ident => $lit:literal ),+ $(,)? }
        err = $err:literal
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
        $vis enum $name {
            $( $variant ),+
        }

        impl $name {
            pub fn as_str(self) -> &'static str {
                match self { $( $name::$variant => $lit ),+ }
            }
        }

        impl TryFrom<&str> for $name {
            type Error = &'static str;
            fn try_from(s: &str) -> Result<Self, Self::Error> {
                match s {
                    $( $lit => Ok($name::$variant), )+
                    _ => Err($err),
                }
            }
        }
    };
}
```

`SourceKind` and the ordered `ContentKind` are both declared through it; the source capability predicates (`can_emit_private`, `has_fetchable_article`, `benefits_from_enrichment`) are the typed source of truth that later SQL frontiers key on.

```rust
// crates/core/src/common/kind.rs:36-65
text_enum! {
    pub enum SourceKind { Rss => "rss", Github => "github", Slack => "slack" }
    err = "unknown source kind"
}

impl SourceKind {
    /// Whether this source can emit *private* (scope-restricted) events, and therefore requires an
    /// owning subscriber on its connection — without one, `finalize` would have no scope to bind a
    /// private item to. RSS is public-only (a feed URL is global); GitHub sees private repos. Keep in
    /// sync with the `connection_private_source_owned` CHECK in the connection migration.
    pub fn can_emit_private(self) -> bool {
        match self {
            SourceKind::Rss => false,
            SourceKind::Github => true,
            // Slack (M6) has private channels; revisit its owner policy when it lands.
            SourceKind::Slack => true,
        }
    }

    /// Whether this source's events carry a *link to fetchable article content* distinct from the
    /// event body — the gate for the best-effort full-text fetch (`ingest::fetch`, Phase 1). RSS items
    /// link out to an article whose `body` is only a snippet, so fetching the page enriches grounding.
    /// GitHub and Slack events ARE the content (a PR description, a chat message) — their link points
    /// back at the item itself, so there is nothing to fetch and they degrade to `body` unchanged.
    pub fn has_fetchable_article(self) -> bool {
        match self {
            SourceKind::Rss => true,
            SourceKind::Github | SourceKind::Slack => false,
        }
    }
```

```rust
// crates/core/src/common/kind.rs:113-122
text_enum! {
    /// Adapter-declared depth signal: how much material an event carries. **Ordered**
    /// (`Message < Announcement < Longform`) so a cluster's `content_depth` can be `max()` over its
    /// events and feed the later Story-vs-Note classification (design §5.1/§8.3). The connector sets
    /// it because source semantics live there — a GitHub release is an announcement, an RSS item is
    /// longform, a chat/comment is a message; deriving it downstream from body length would be a
    /// gameable heuristic (§7.1).
    pub enum ContentKind { Message => "message", Announcement => "announcement", Longform => "longform" }
    err = "unknown content kind"
}
```

### `salience.rs`

**Purpose:** the `0..=3` importance scale that lifts the digest off a pure-recency sort, always a deterministic function of a classification — never a scalar hallucinated by a model.

Importance rides the existing `severity_hint` → `cluster.max_severity` → digest-weight rollup, so it needs no new ranking plumbing. The scale is a small set of named constants; a structured source maps deterministically (`github_importance`), while a free-text source has the enrichment LLM classify into the closed `IMPACT_VOCAB`, which this module turns into a score.

```rust
// crates/core/src/common/salience.rs:19-33
pub const MAJOR: i16 = 3;
/// A significant development — a major policy change, a serious incident, a notable market/org move.
pub const SIGNIFICANT: i16 = 2;
/// A notable item worth surfacing but not dominating — a policy debate, a routine release, a local
/// incident, an opened issue/PR.
pub const NOTABLE: i16 = 1;
/// Routine churn — chatter, a push, a watch/fork, a market wrap, a minor cultural note. Contributes
/// no priority boost (the un-tuned baseline), so it ranks on recency alone.
pub const ROUTINE: i16 = 0;

/// The closed impact vocabulary the enrichment LLM classifies a free-text item into — ordered
/// least→most significant. A *closed* vocab is what a 3–4B model calibrates reliably (unlike a bare
/// integer), and it is re-validated here exactly like the comprehension pass re-checks `event_type`
/// (defense in depth: an out-of-vocab class degrades to [`ROUTINE`], never an error).
pub const IMPACT_VOCAB: [&str; 4] = ["routine", "notable", "significant", "major"];
```

`impact_score` re-validates the LLM's word against the vocab (an empty, unknown, or garbled value floors to `ROUTINE`, so a flaky classification only forgoes a boost); `github_importance` is the exact structural map for the source enrichment deliberately never runs over.

```rust
// crates/core/src/common/salience.rs:38-58
pub fn impact_score(impact: &str) -> i16 {
    match impact.trim().to_ascii_lowercase().as_str() {
        "major" => MAJOR,
        "significant" => SIGNIFICANT,
        "notable" => NOTABLE,
        _ => ROUTINE,
    }
}

/// Structural importance for a GitHub activity, by its REST event `kind` — the deterministic map for a
/// source whose semantics are exact (no LLM needed, and enrichment is deliberately *not* run over
/// GitHub, see [`crate::common::kind::SourceKind::benefits_from_enrichment`]). A release ships
/// something; an issue/PR is substantive activity; chatter, pushes, and stars are routine churn. Coarse
/// and tunable — the weight that scales it lives in `digest_config`.
pub fn github_importance(kind: &str) -> i16 {
    match kind {
        "ReleaseEvent" => SIGNIFICANT,
        "IssuesEvent" | "PullRequestEvent" => NOTABLE,
        _ => ROUTINE,
    }
}
```

### `link_safety.rs`

**Purpose:** the single source of truth for "does this token look clickable?", guarding against the receiving mail client's own linkifier turning hallucinated or bare-domain text into live links.

Escaping and href allow-listing only govern the renderer's own `<a href>` output; they do nothing about Apple Mail / Gmail linkifying displayed text (including the plaintext part). The only defense that holds is that the text itself never *looks* like a URL or domain. `is_linkable_token` flags the explicit URL forms and, deliberately aggressively, any bare host shape — while sparing versions, ratios, and abbreviations by requiring an alphabetic ≥2-char final label.

```rust
// crates/core/src/common/link_safety.rs:39-76
pub fn is_linkable_token(tok: &str) -> bool {
    if tok.contains("://") {
        return true;
    }
    let has_prefix = |p: &str| {
        tok.len() >= p.len() && tok.as_bytes()[..p.len()].eq_ignore_ascii_case(p.as_bytes())
    };
    if has_prefix("www.") || has_prefix("mailto:") {
        return true;
    }
    looks_like_host(tok)
}

/// The bare-domain test behind [`is_linkable_token`]: take the authority (everything before the first
/// `/`, `?` or `#`), drop any `user@` userinfo and `:port` suffix, then require ≥2 dot-separated
/// labels of letters/digits/hyphen whose last label (the would-be TLD) is alphabetic and ≥2 chars.
fn looks_like_host(tok: &str) -> bool {
    let authority = tok.split(['/', '?', '#']).next().unwrap_or(tok);
    let after_userinfo = authority.rsplit('@').next().unwrap_or(authority);
    let host = after_userinfo.split(':').next().unwrap_or(after_userinfo);

    let mut count = 0usize;
    let mut last = "";
    for label in host.split('.') {
        // Every label must be a non-empty run of letters/digits/hyphen.
        if label.is_empty()
            || !label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return false;
        }
        count += 1;
        last = label;
    }
    // Need at least one dot, and a TLD-looking final label (alphabetic, ≥2 chars).
    count >= 2 && last.len() >= 2 && last.bytes().all(|b| b.is_ascii_alphabetic())
}
```

The detector is used two ways: `first_linkable_token` is the write-side gate (a summary leaking a link is rejected and baseline-replaced), and `defang` is the render-side backstop that borrows clean prose unchanged and otherwise rewrites each clickable token's dots to `[.]` (and the scheme colons) so nothing auto-links in either the HTML or plaintext body.

```rust
// crates/core/src/common/link_safety.rs:115-125
pub fn defang(text: &str) -> Cow<'_, str> {
    let any = text
        .split_whitespace()
        .map(core_token)
        .any(|tok| !tok.is_empty() && is_linkable_token(tok));
    if !any {
        return Cow::Borrowed(text);
    }

    let mut out = String::with_capacity(text.len() + 8);
    let mut rest = text;
```

### `secret.rs`

**Purpose:** credentials at rest via envelope encryption, in-memory redaction, and the one audited constant-time credential comparison.

Secret material is held in `secrecy`'s redacted, zeroize-on-drop boxes; reaching the bytes is an explicit, grep-able `.expose_secret()`. `ct_eq` is the single audited compare every credential check should route through — length-checked (leaking a high-entropy secret's length is harmless; leaking byte positions via early return is not) and accumulating a difference bitmask rather than returning early.

```rust
// crates/core/src/common/secret.rs:41-50
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
```

At rest, a single 32-byte app `MasterKey` wraps every secret with per-secret envelope encryption: each `seal` mints a fresh random DEK, encrypts the plaintext under the DEK, then wraps the DEK under the master key — both legs XChaCha20-Poly1305 with 24-byte random nonces. The stored envelope is `version ‖ wrap-nonce ‖ wrapped-DEK ‖ payload-nonce ‖ payload`. This DEK-per-secret indirection is exactly the shape a managed KMS wants, making an M5 swap a backend change to the wrap leg rather than a re-encryption migration.

```rust
// crates/core/src/common/secret.rs:146-170
pub fn seal(master: &MasterKey, plaintext: &[u8]) -> Result<SealedSecret, SecretError> {
    // Fresh per-secret DEK; copied into a Zeroizing buffer so it's scrubbed when this fn returns.
    let mut dek = Zeroizing::new([0u8; KEY_LEN]);
    dek.copy_from_slice(&XChaCha20Poly1305::generate_key(&mut OsRng));
    let dek_cipher = XChaCha20Poly1305::new_from_slice(&dek[..]).expect("DEK is KEY_LEN bytes");

    let payload_nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let payload = dek_cipher
        .encrypt(&payload_nonce, plaintext)
        .map_err(|_| SecretError::Decrypt)?;

    let wrap_nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let wrapped_dek = master
        .cipher()
        .encrypt(&wrap_nonce, &dek[..])
        .map_err(|_| SecretError::Decrypt)?;

    let mut envelope = Vec::with_capacity(MIN_ENVELOPE_LEN + plaintext.len());
    envelope.push(ENVELOPE_VERSION);
    envelope.extend_from_slice(&wrap_nonce);
    envelope.extend_from_slice(&wrapped_dek);
    envelope.extend_from_slice(&payload_nonce);
    envelope.extend_from_slice(&payload);
    Ok(SealedSecret { envelope })
}
```

`unseal` validates framing before any crypto (length ≥ `MIN_ENVELOPE_LEN`, version byte), slices the fixed-width header, unwraps the DEK under the master key, and decrypts the payload under the DEK — the plaintext DEK never escapes the function, and the coarse `SecretError` avoids offering a decrypt oracle.

```rust
// crates/core/src/common/secret.rs:186-206
    // Slice the fixed-width header: wrap-nonce ‖ wrapped-DEK ‖ payload-nonce ‖ payload.
    let mut off = 1;
    let wrap_nonce = XNonce::from_slice(&env[off..off + NONCE_LEN]);
    off += NONCE_LEN;
    let wrapped_dek = &env[off..off + WRAPPED_DEK_LEN];
    off += WRAPPED_DEK_LEN;
    let payload_nonce = XNonce::from_slice(&env[off..off + NONCE_LEN]);
    off += NONCE_LEN;
    let payload = &env[off..];

    let dek = Zeroizing::new(
        master
            .cipher()
            .decrypt(wrap_nonce, wrapped_dek)
            .map_err(|_| SecretError::Decrypt)?,
    );
    let dek_cipher = XChaCha20Poly1305::new_from_slice(&dek).map_err(|_| SecretError::Decrypt)?;
    let plaintext = dek_cipher
        .decrypt(payload_nonce, payload)
        .map_err(|_| SecretError::Decrypt)?;
    Ok(SecretSlice::from(plaintext))
```

### `status.rs`

**Purpose:** the one-glance pipeline snapshot behind `debug status`, gathered in a single admin control-plane transaction.

`StatusReport` is a bundle of cheap aggregates over the domain tables plus the apalis queue. `gather` runs the whole report in one `ScopeCtx::Admin` transaction — the only context in which the fail-closed control-plane tables are readable. RLS still applies to the content tables even under admin, so `event`/`cluster` aggregates count the public scope only.

```rust
// crates/core/src/common/status.rs:100-113
pub async fn gather(pool: &sqlx::PgPool) -> Result<StatusReport, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let report = StatusReport {
        connections: connection_stats(&mut tx).await?,
        events: event_stats(&mut tx).await?,
        build: build_status(&mut tx).await?,
        clusters: cluster_stats(&mut tx).await?,
        subscribers: subscriber_stats(&mut tx).await?,
        digests: digest_stats(&mut tx).await?,
        queue: queue_stats(&mut tx).await?,
    };
    tx.commit().await?;
    Ok(report)
}
```

The event aggregate is a good example of the "is anything stuck?" framing: it counts the unbuilt (public, ingested past the build watermark) and fetch-pending backlogs in a single FILTERed query, using the typed `fetchable_sources()` list rather than a scattered `source = 'rss'` literal.

```rust
// crates/core/src/common/status.rs:136-155
    let agg = sqlx::query(
        "SELECT count(*) AS total,
                count(*) FILTER (
                    WHERE scope_kind = 'public'
                      AND ingest_time > (SELECT built_through FROM build_watermark)
                ) AS unbuilt,
                count(*) FILTER (
                    WHERE scope_kind = 'public'
                      AND full_text IS NULL
                      AND full_text_attempts < $1
                      AND source = ANY($2)
                      AND array_length(links, 1) >= 1
                ) AS fetch_pending,
                max(ingest_time) AS latest_ingest
         FROM event",
    )
    .bind(crate::ingest::fetch::MAX_FETCH_ATTEMPTS)
    .bind(crate::common::kind::SourceKind::fetchable_sources())
    .fetch_one(&mut *conn)
    .await?;
```

### `watermark.rs`

**Purpose:** the shared per-subscriber `built_through` read/advance convention for the private-build and thread-maintenance cursors.

Each backing table is `(subscriber_id PK, built_through, [ran_at])`; the epoch-default read and monotonic-`GREATEST` advance live here once rather than copied per store. The `table` argument is a trusted `&'static str` (the store's own name, interpolated into SQL) — never caller input. `read_through` defaults a missing row to the epoch so the first pass folds in all prior history.

```rust
// crates/core/src/common/watermark.rs:13-28
pub async fn read_through(
    executor: impl PgExecutor<'_>,
    table: &'static str,
    subscriber_id: Uuid,
) -> Result<DateTime<Utc>, sqlx::Error> {
    let sql = format!(
        "SELECT coalesce(
                  (SELECT built_through FROM {table} WHERE subscriber_id = $1),
                  'epoch'::timestamptz) AS built_through"
    );
    let row = sqlx::query(&sql)
        .bind(subscriber_id)
        .fetch_one(executor)
        .await?;
    Ok(row.get("built_through"))
}
```

`advance` upserts, advancing monotonically via `GREATEST` and optionally stamping `ran_at = now()` — the due-query clock the maintenance cadence reads.

```rust
// crates/core/src/common/watermark.rs:40-55
    let sql = if stamp_ran_at {
        format!(
            "INSERT INTO {table} (subscriber_id, built_through, ran_at)
             VALUES ($1, $2, now())
             ON CONFLICT (subscriber_id) DO UPDATE SET
                built_through = GREATEST({table}.built_through, EXCLUDED.built_through),
                ran_at = now()"
        )
    } else {
        format!(
            "INSERT INTO {table} (subscriber_id, built_through)
             VALUES ($1, $2)
             ON CONFLICT (subscriber_id) DO UPDATE
                SET built_through = GREATEST({table}.built_through, EXCLUDED.built_through)"
        )
    };
```


---

## `ingest` — connectors & event log

The `ingest` module is the producer side of the ingest→clustering seam: it polls each `connection` row's source, normalizes items into `EventBuilder`s, and appends the results to the fingerprint-deduped event log. It is organized around two closed dispatch enums — `ConnDispatch` (pull) and `RealtimeDispatch` (push/webhook) — sitting behind the `Connection` / `RealtimeConnection` traits, plus a scope-aware append path (`append_scoped` → `store::insert_event`), an SSRF-guarded best-effort article fetcher (`fetch`), a shared HTML→text renderer (`html_text`), and the GitHub App connector (`github/`).

### `mod.rs`

**Purpose:** Module root — declares the connector traits, the two source-dispatch enums, the poll/webhook orchestration flow, connection validation, and the scope-grouped append.

The `Connection` trait is the pull-side contract every connector implements. It keeps `Cursor` (an opaque source-private JSON fetch position that infra never reads) and `Item` associated so they never leak past a dispatch arm; `poll` produces a `Batch<Item, Cursor>` and `to_events` is a pure item→builder normalization.

```rust
// crates/core/src/ingest/mod.rs:117-132
/// Per-tenant live worker for one `connection` row. Every connector implements this.
pub trait Connection: Send + Sync {
    /// Opaque, source-private incremental-fetch position. Infra persists as JSON, never reads it.
    type Cursor: serde::Serialize + serde::de::DeserializeOwned + Default + Send + Sync;
    /// One unit of content from the source, complete after a poll.
    type Item: Send;

    fn poll(
        &self,
        cursor: Self::Cursor,
    ) -> impl std::future::Future<Output = Result<Batch<Self::Item, Self::Cursor>, SourceError>> + Send;

    /// Pure normalization: source-specific item → connector-side event builders. Infra calls
    /// `finalize(owner)` on each builder to stamp the scope boundary and fingerprint.
    fn to_events(&self, item: Self::Item) -> Vec<EventBuilder>;
}
```

`ConnDispatch` is hand-written dispatch over the closed source set: each arm holds a concrete `Connection`, the `poll → to_events` chain runs inside the typed arm (via the monomorphized `poll_inner`), and only core types cross out. Adding a fourth source makes the compiler flag the missing arm.

```rust
// crates/core/src/ingest/mod.rs:181-222
pub enum ConnDispatch {
    Rss(rss::RssConnection),
    Github(github::GithubConnection),
}

impl ConnDispatch {
    /// Build the live worker for a connection row from its `config` + the app context.
    pub fn build(row: &store::ConnectionRow, ctx: &ConnectorCtx) -> Result<Self, BuildError> {
        match row.source {
            SourceKind::Rss => {
                let cfg: rss::RssConfig = serde_json::from_value(row.config.clone())
                    .map_err(|e| BuildError::BadConfig(e.to_string()))?;
                Ok(ConnDispatch::Rss(rss::RssConnection::new(cfg.url)))
            }
            SourceKind::Github => {
                let gh = ctx
                    .github
                    .as_ref()
                    .ok_or(BuildError::NotConfigured(SourceKind::Github))?;
                let cfg: github::GithubConfig = serde_json::from_value(row.config.clone())
                    .map_err(|e| BuildError::BadConfig(e.to_string()))?;
                let token = (gh.token_factory)(cfg.installation_id);
                Ok(ConnDispatch::Github(
                    github::GithubConnection::new(&gh.base_url, token).with_repos(cfg.repos),
                ))
            }
            SourceKind::Slack => Err(BuildError::Unsupported(SourceKind::Slack)),
        }
    }

    /// Poll using the persisted cursor JSON and normalize into connector-side builders, returning
    /// the next cursor to persist. The cursor is erased to JSON here; `Item` never escapes the arm.
    pub async fn poll_and_normalize(
        &self,
        cursor: Option<serde_json::Value>,
    ) -> Result<(Vec<EventBuilder>, serde_json::Value), SourceError> {
        match self {
            ConnDispatch::Rss(c) => poll_inner(c, cursor).await,
            ConnDispatch::Github(c) => poll_inner(c, cursor).await,
        }
    }
}
```

`append_scoped` is the write path: events are grouped by their `ScopeCtx`, and each group is committed one transaction per context (at most two per connection — public + the owner) so the per-event scope discipline (a public event in the no-subscriber context, a private event in its owner's, as the DB write policy demands) costs no transaction-per-row.

```rust
// crates/core/src/ingest/mod.rs:264-287
async fn append_scoped(pool: &PgPool, events: Vec<NewEvent>) -> Result<(usize, usize)> {
    let total = events.len();
    let mut groups: HashMap<ScopeCtx, Vec<NewEvent>> = HashMap::new();
    for ev in events {
        groups
            .entry(ScopeCtx::for_scope(&ev.scope))
            .or_default()
            .push(ev);
    }

    let mut inserted = 0usize;
    for (ctx, evs) in groups {
        let mut tx = begin_scope(pool, ctx)
            .await
            .context("open scoped ingest txn")?;
        for ev in &evs {
            if store::insert_event(&mut *tx, ev).await?.is_some() {
                inserted += 1;
            }
        }
        tx.commit().await.context("commit scoped ingest txn")?;
    }
    Ok((inserted, total - inserted))
}
```

The `poll` orchestrator ties it together: load the connection, build the dispatch (non-fatal skip on failure), poll+normalize, then map each builder's `is_private` flag against **this connection's owner** (`finalize(conn_row.subscriber_id)` — the owner comes from our row, never the polled payload, the §12 IDOR boundary). Events commit before the cursor advances (the crash-safety invariant: a re-poll re-fetches, fingerprint dedup collapses the overlap); a failure records backoff instead.

```rust
// crates/core/src/ingest/mod.rs:315-348
    match dispatch.poll_and_normalize(conn_row.cursor.clone()).await {
        Ok((builders, new_cursor)) => {
            // Per-event scope: `finalize` maps the builder's `is_private` flag against THIS
            // connection's owner — a private-repo item becomes `Private(owner)`, public stays
            // shared. The owner comes from our row, never the polled payload (§12 risk #1).
            // `append_scoped` then writes each event in the RLS context its scope requires.
            let events: Vec<NewEvent> = builders
                .into_iter()
                .map(|b| {
                    b.connection(Some(conn_row.id))
                        .finalize(conn_row.subscriber_id)
                })
                .collect();
            let (inserted, deduplicated) = append_scoped(pool, events).await?;
            tracing::info!(
                connection_id = %conn_row.id,
                source = source.as_str(),
                inserted,
                deduplicated,
                "poll complete"
            );
            store::advance_cursor(pool, conn_row.id, new_cursor).await?;
            Ok(PollOutcome::Polled {
                source,
                inserted,
                deduplicated,
            })
        }
        Err(e) => {
            tracing::warn!(%connection_id, error = %e, "poll failed");
            store::record_failure(pool, conn_row.id).await?;
            Ok(PollOutcome::Failed { source })
        }
    }
```

`process_webhook` is the realtime intake counterpart: (1) peek the routing key credential-free via `realtime::route`, (2) resolve OUR connection by `(source, provider_account_id)` — deriving nothing about scope/subscriber from the payload (IDOR defense; an unknown install is dropped as `Unrouted`), (3) normalize via `RealtimeDispatch`, and (4) append (fingerprint dedup collapses any poll overlap) or apply a lifecycle status change. Scope again derives from the *resolved* connection's owner, not the delivery body.

### `fetch.rs`

**Purpose:** Off-hot-path, best-effort full-article text fetch of feed-supplied (attacker-influenced) links, SSRF-guarded on every hop, written back to `event.full_text` with a bounded retry budget.

The core SSRF decision is `is_disallowed_ip`, run over every resolved address (first URL and every redirect hop). It blocks loopback/private/link-local/ULA/CGNAT/multicast/reserved ranges for both v4 and v6, and — crucially — unwraps any v6 form that *embeds* a v4 address (IPv4-mapped/compatible, NAT64, 6to4) and re-checks it as v4, so an internal host can't be reached through a v6 representation.

```rust
// crates/core/src/ingest/fetch.rs:253-300
fn is_disallowed_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_disallowed_v4(v4),
        IpAddr::V6(v6) => {
            // ::ffff:a.b.c.d (mapped) and ::a.b.c.d (compatible) reach the same v4 host.
            if let Some(v4) = v6.to_ipv4() {
                if is_disallowed_v4(v4) {
                    return true;
                }
            }
            // NAT64 (64:ff9b::/96 well-known, and the 64:ff9b:1::/48 local-use prefix) and 6to4
            // (2002::/16) also carry an embedded v4 that `to_ipv4` does not unwrap — extract it from
            // its standard position and re-check, so e.g. the NAT64 form of 127.0.0.1 is blocked too.
            if let Some(v4) = embedded_v4(v6) {
                if is_disallowed_v4(v4) {
                    return true;
                }
            }
            is_disallowed_v6(v6)
        }
    }
}

fn is_disallowed_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    ip.is_unspecified()                          // 0.0.0.0
        || o[0] == 0                             // 0.0.0.0/8 "this network"
        || ip.is_loopback()                      // 127.0.0.0/8
        || ip.is_private()                       // 10/8, 172.16/12, 192.168/16
        || ip.is_link_local()                    // 169.254.0.0/16
        || (o[0] == 100 && (o[1] & 0xC0) == 64)  // 100.64.0.0/10 CGNAT (shared)
        || ip.is_broadcast()                     // 255.255.255.255
        || ip.is_documentation()                 // 192.0.2/24, 198.51.100/24, 203.0.113/24
        || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0.0/24 IETF protocol assignments
        || (o[0] == 198 && (o[1] & 0xFE) == 18)  // 198.18.0.0/15 benchmarking
        || ip.is_multicast()                     // 224.0.0.0/4
        || o[0] >= 240 // 240.0.0.0/4 reserved
}

fn is_disallowed_v6(ip: Ipv6Addr) -> bool {
    let seg = ip.segments();
    ip.is_unspecified()                       // ::
        || ip.is_loopback()                   // ::1
        || (seg[0] & 0xfe00) == 0xfc00        // fc00::/7 unique-local (ULA)
        || (seg[0] & 0xffc0) == 0xfe80        // fe80::/10 link-local
        || ip.is_multicast()                  // ff00::/8
        || (seg[0] == 0x2001 && seg[1] == 0x0db8) // 2001:db8::/32 documentation
}
```

`validate_url` applies that decision *before* the request per hop: it rejects non-http(s) schemes, resolves name hosts by DNS (IP literals directly), and blocks if *any* resolved address is disallowed (so a partial-rebinding host resolving to both public and private is rejected outright). The shared client's `SafeResolver` then re-checks the address actually connected to, closing the DNS-rebinding TOCTOU window for name hosts.

```rust
// crates/core/src/ingest/fetch.rs:346-395
async fn validate_url(url: &Url) -> Result<(), FetchError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(FetchError::BadScheme);
    }
    let port = url.port_or_known_default().ok_or(FetchError::BadScheme)?;
    let host = url.host().ok_or(FetchError::BadScheme)?;

    let ips: Vec<IpAddr> = match host {
        url::Host::Ipv4(ip) => vec![IpAddr::V4(ip)],
        url::Host::Ipv6(ip) => vec![IpAddr::V6(ip)],
        url::Host::Domain(domain) => tokio::net::lookup_host((domain, port))
            .await
            .map_err(|_| FetchError::DnsFailure)?
            .map(|sa| sa.ip())
            .collect(),
    };

    if ips.is_empty() {
        return Err(FetchError::DnsFailure);
    }
    // Conservative: block if *any* resolved address is disallowed, so a host that resolves to a public
    // and a private address (a partial-rebinding attempt) is rejected outright.
    if ips.iter().copied().any(is_disallowed_ip) {
        return Err(FetchError::BlockedAddress);
    }
    Ok(())
}

/// A reqwest DNS resolver that drops every disallowed address ([`is_disallowed_ip`]) before reqwest
/// connects, returning an error when nothing safe remains. Installed on the shared fetch client so the
/// address actually dialed for a *name* host is always one we validated — closing the DNS-rebinding
/// (TOCTOU) gap between the up-front [`validate_url`] check and the connect, with a single reusable
/// client rather than a per-host pinned one. (IP-literal hosts bypass the resolver, so they are
/// guarded only by the up-front check — which is why that check is kept.)
struct SafeResolver;

impl reqwest::dns::Resolve for SafeResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let host = name.as_str().to_owned();
            // Port 0: reqwest applies the URL's port to the returned addrs (documented behavior).
            let resolved = tokio::net::lookup_host((host.as_str(), 0u16)).await?;
            let safe: Vec<SocketAddr> = resolved.filter(|sa| !is_disallowed_ip(sa.ip())).collect();
            if safe.is_empty() {
                return Err(format!("blocked or unresolvable host: {host}").into());
            }
            Ok(Box::new(safe.into_iter()) as reqwest::dns::Addrs)
        })
    }
}
```

Failures are classified permanent vs. transient by `FetchError::is_permanent`: a permanent failure (bad scheme, blocked host, 4xx except 408/429, oversize) spends the whole retry budget at once so a dead link drops out of the queue immediately, while transient ones (DNS blips, 5xx, timeouts, connect errors) keep their per-sweep retries.

```rust
// crates/core/src/ingest/fetch.rs:219-234
    pub fn is_permanent(&self) -> bool {
        match self {
            FetchError::BadScheme
            | FetchError::BlockedAddress
            | FetchError::DisallowedContentType(_)
            | FetchError::TooLarge
            | FetchError::Empty
            | FetchError::TooManyRedirects => true,
            // 4xx won't change on retry, except the explicitly-retryable 408 (Request Timeout) and 429
            // (Too Many Requests); 5xx are transient.
            FetchError::BadStatus(code) => {
                (400..500).contains(code) && *code != 408 && *code != 429
            }
            FetchError::DnsFailure | FetchError::Transport(_) => false,
        }
    }
```

`fetch_article` follows redirects manually (the client's own redirect handling is disabled) so each hop's target is re-validated before it is connected — a first-URL pass is not enough since a `Location` can point at `127.0.0.1`. It enforces a `text/html` content-type allowlist and a size cap (declared and streamed via `read_capped`) before handing the body to `html_text::render_article`. On success `store_full_text` also re-derives depth: once the fetched text clears `LONGFORM_MIN_CHARS` (400) the event's `content_kind` is monotonically raised to `longform`.

### `rss.rs`

**Purpose:** RSS/Atom connector — conditional-GET polling via ETag/Last-Modified, feed parsing (RSS first, Atom fallback), and depth-by-source-semantics normalization.

`poll` implements HTTP conditional GET: it sends the cursor's `If-None-Match`/`If-Modified-Since`, short-circuits on `304 Not Modified` (returning an empty batch with the same cursor), and otherwise parses the body and persists the fresh ETag/Last-Modified as the next cursor.

```rust
// crates/core/src/ingest/rss.rs:136-193
    async fn poll(
        &self,
        cursor: Self::Cursor,
    ) -> Result<Batch<Self::Item, Self::Cursor>, SourceError> {
        tracing::debug!(url = %self.feed_url, etag = ?cursor.etag, "fetching RSS feed");

        let mut req = self.client.get(&self.feed_url);
        if let Some(ref etag) = cursor.etag {
            req = req.header(reqwest::header::IF_NONE_MATCH, etag.as_str());
        }
        if let Some(ref lm) = cursor.last_modified {
            req = req.header(reqwest::header::IF_MODIFIED_SINCE, lm.as_str());
        }

        let resp = req
            .send()
            .await
            .map_err(|e| SourceError::Request(e.to_string()))?;

        if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
            tracing::debug!(url = %self.feed_url, "feed not modified (304), skipping");
            return Ok(Batch {
                items: vec![],
                cursor,
            });
        }

        if !resp.status().is_success() {
            return Err(SourceError::Request(format!("HTTP {}", resp.status())));
        }

        let new_etag = resp
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let new_lm = resp
            .headers()
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let bytes = resp
            .bytes()
            .await
            .map_err(|e| SourceError::Request(e.to_string()))?;
        let items = parse_feed(bytes.as_ref())?;

        tracing::debug!(url = %self.feed_url, count = items.len(), "parsed feed items");

        Ok(Batch {
            items,
            cursor: RssCursor {
                etag: new_etag,
                last_modified: new_lm,
            },
        })
    }
```

`to_events` derives depth from **source semantics, not snippet length**: an RSS item links to a published article, so an item carrying *any* article text is `Longform` (even a short teaser) — the old 400-char gate is gone because it made teaser-only feeds render as all-Notes. Only a genuinely body-less item stays a thin `Announcement`, and even that self-heals once the linked article is fetched. Each item's `id` is its own `group_key`, so each article is its own cluster.

```rust
// crates/core/src/ingest/rss.rs:195-229
    fn to_events(&self, item: Self::Item) -> Vec<EventBuilder> {
        // Use published date when present; fall back to now for feeds that omit dates.
        let event_time = item.published.unwrap_or_else(Utc::now);
        let links: Vec<String> = item.link.into_iter().collect();

        // Depth by **source semantics**, not snippet length (system-design.md §8.3/§300, and
        // `ContentKind`'s own doc): "an RSS item is longform … deriving it downstream from body text
        // would collapse into a gameable length heuristic." An RSS item links out to a published
        // article, so an item that carries any article text *is* a Story-depth `Longform` — even when
        // the *feed* only shipped a short teaser (most do: a 200–300-char snippet under the old
        // 400-char gate is what made nearly every article render as a Note, the all-Notes digest). The
        // teaser still grounds a short tldr now; the best-effort full-text fetch later enriches it
        // ([`crate::ingest::fetch`]) without changing the format. Only a genuinely body-less item — no
        // text to ground a summary at all — stays a thin [`ContentKind::Announcement`] → headline-only
        // Note, and even that self-heals: it has a link, so the fetch raises it to Longform once the
        // article lands.
        let content_kind = match &item.body {
            Some(body) if !body.trim().is_empty() => ContentKind::Longform,
            _ => ContentKind::Announcement,
        };

        let mut builder = EventBuilder::new(
            SourceKind::Rss,
            item.id.clone(),
            event_time,
            item.title,
            item.id, // group_key = stable_id: each article is its own cluster
        )
        .content_kind(content_kind)
        .links(links);
        if let Some(body) = item.body {
            builder = builder.body(body);
        }
        vec![builder]
    }
```

`parse_feed` tries `rss::Channel` first and falls back to `atom_syndication::Feed`, preferring the full-article element (`<content:encoded>` / `<content>`) over the short one (`<description>` / `<summary>`), rendering both through `body_text` (a `html_text::render` wrapper with feed-body caps).

### `html_text.rs`

**Purpose:** Shared HTML-fragment → plain-text rendering for both the RSS body extractor and the full-article fetcher, so the two paths can't drift on markup stripping or bounding.

`render` uses html2text's `plain_no_decorate` and cleans up its two leaked decorations in a single tokenization pass: it splits on the link brackets `[`/`]` as well as whitespace (de-gluing a mashed `coast.[tagesschau.de]The` while keeping the visible link text) and drops standalone heading `##` markers, then caps on a word boundary. Input markup is bounded to `max_html_chars` before parsing so work stays proportional to what is kept, not to page size.

```rust
// crates/core/src/ingest/html_text.rs:31-66
pub(crate) fn render(html: &str, max_html_chars: usize, max_chars: usize) -> Option<String> {
    if html.trim().is_empty() {
        return None;
    }
    // Bound the work before rendering: only allocate a truncated copy when the markup actually exceeds
    // the cap (byte length ≥ char count, so a shorter byte length needs no truncation).
    let bounded: Cow<str> = if html.len() > max_html_chars {
        Cow::Owned(html.chars().take(max_html_chars).collect())
    } else {
        Cow::Borrowed(html)
    };
    let rendered = html2text::config::plain_no_decorate()
        .string_from_read(bounded.as_bytes(), RENDER_WIDTH)
        .ok()?;
    // `plain_no_decorate` still leaks two decorations that pollute the grounding text the model and the
    // entity/number miners read — and that this module's whole premise (links carried structurally in
    // `event.links`, never as prose) means we don't want:
    //   1. It wraps an inline link's visible text in `[...]` and, when the source HTML has no whitespace
    //      around the tag, glues the bracket straight onto the neighbouring words —
    //      `coast.<a>tagesschau.de</a>The` -> `coast.[tagesschau.de]The`. That single mashed token then
    //      both reads as garbage and trips the summarizer's bare-domain faithfulness gate downstream.
    //   2. It prefixes a heading with a markdown `##` marker.
    // Split on the link brackets as well as whitespace — de-gluing the boundary while keeping the link's
    // visible text as plain content — and drop standalone heading markers, in the same tokenization pass
    // that collapses whitespace into clean single-spaced prose (so no extra full-string allocation just to
    // swap the brackets out first). Empty pieces from adjacent delimiters are filtered with the markers.
    let normalized = rendered
        .split(|c: char| c.is_whitespace() || c == '[' || c == ']')
        .filter(|tok| !tok.is_empty() && !tok.bytes().all(|b| b == b'#'))
        .collect::<Vec<_>>()
        .join(" ");
    if normalized.is_empty() {
        return None;
    }
    Some(truncate_on_word_boundary(normalized, max_chars))
}
```

`render_article` is the fetcher's entry point and isolates the main content before rendering — a blind whole-page strip yields only nav/footer chrome. It tries, in falling order of reliability: (1) the schema.org JSON-LD `articleBody`, (2) the `<article>` subtree rendered in isolation, (3) the whole page as last resort. Each candidate is routed back through `render` so all paths share one entity-decode/whitespace-collapse/word-cap normalizer.

```rust
// crates/core/src/ingest/html_text.rs:84-103
pub(crate) fn render_article(
    html: &str,
    max_html_chars: usize,
    max_chars: usize,
) -> Option<String> {
    if let Some(body) = jsonld_article_body(html) {
        // Route the (already-plain) JSON-LD body through `render` too, so it gets the same HTML-entity
        // decode (`&amp;` → `&`), whitespace collapse, and word-boundary cap as every other path — one
        // normalizer, no drift.
        if let Some(text) = render(&body, max_html_chars, max_chars) {
            return Some(text);
        }
    }
    if let Some(article) = first_article_subtree(html) {
        if let Some(text) = render(article, max_html_chars, max_chars) {
            return Some(text);
        }
    }
    render(html, max_html_chars, max_chars)
}
```

The JSON-LD and `<article>` extraction is a byte-safe ASCII-case-insensitive scan (`find_ascii_ci`) over the raw HTML — no DOM parser — which is sound because every offset it slices on (`<script`, `>`, `</script>`, `<article`) is ASCII and so lands on a valid char boundary even amid multibyte UTF-8. `find_article_body` recurses through objects/arrays/`@graph` for the first non-empty `articleBody`.

### `realtime.rs`

**Purpose:** The realtime (webhook) head of the connector model — the push counterpart to the pull `Connection`, turning verified deliveries into the same `EventBuilder`s the poll produces so the two intakes dedup on `UNIQUE(fingerprint)`.

Two traits mirror the pull side. `RealtimeConnector::verify` is the app-level HMAC authentication (constant-time, over the raw bytes with no parse-first). `RealtimeConnection` extends `Connection` with `accept_webhook` (body → source items or a lifecycle change) and `hydrate` (thin-notification → full item, defaulting to identity since GitHub payloads are already complete).

```rust
// crates/core/src/ingest/realtime.rs:74-97
pub trait RealtimeConnector: Send + Sync {
    /// Authenticate the raw body against the app secret in **constant time**, over the bytes exactly
    /// as received (no parse first — a parse-then-verify is a signature-bypass foothold).
    fn verify(&self, headers: &WebhookHeaders, body: &[u8]) -> Verified;
}

/// Per-connection realtime worker — a [`Connection`] that also accepts webhooks (design §5.4).
pub trait RealtimeConnection: Connection {
    /// Normalize a verified delivery into source items (or a lifecycle change). `event_type` and
    /// `delivery_id` come from the headers (the body alone doesn't carry the activity type).
    fn accept_webhook(
        &self,
        event_type: &str,
        delivery_id: &str,
        body: &[u8],
    ) -> Result<Inbound<Self::Item>, SourceError>;

    /// Turn a thin webhook notification into a full item (default: identity). GitHub webhook
    /// payloads are already complete, so it never fetches; a source whose webhook is a bare pointer
    /// overrides this to hydrate via its API.
    fn hydrate(&self, item: Self::Item) -> Self::Item {
        item
    }
}
```

`RealtimeDispatch` is the closed dispatch over *realtime-capable* sources — it has **no RSS arm**, so routing a webhook to a pull-only source fails to compile. Its `build` needs no `ConnectorCtx` (webhook normalization is pure — the App token is only exercised by `poll`), which is why webhooks ingest even while `ctx.github == None`. `accept_and_normalize` runs the `accept_webhook → hydrate → to_events` chain inside the arm, keeping `Item` from escaping.

```rust
// crates/core/src/ingest/realtime.rs:102-142
pub enum RealtimeDispatch {
    Github(github::GithubConnection),
}

impl RealtimeDispatch {
    /// Build the realtime worker for a source. Unlike `ConnDispatch::build`, this needs no
    /// `ConnectorCtx`: webhook normalization is a pure function of the delivery body — the App token
    /// (the part `ctx.github` carries) is only exercised by `poll`. So webhooks ingest even while
    /// "plumbing now, secrets later" leaves `ctx.github == None` (Phase 5 wires the poll creds).
    pub fn build(source: SourceKind) -> Result<Self, BuildError> {
        match source {
            SourceKind::Github => Ok(RealtimeDispatch::Github(
                github::GithubConnection::realtime_only(),
            )),
            other => Err(BuildError::Unsupported(other)),
        }
    }

    /// Accept + normalize one delivery: `accept_webhook → hydrate → to_events`, the realtime mirror
    /// of `poll → to_events`. `Item` never escapes the arm.
    pub fn accept_and_normalize(
        &self,
        event_type: &str,
        delivery_id: &str,
        body: &[u8],
    ) -> Result<Inbound<EventBuilder>, SourceError> {
        match self {
            RealtimeDispatch::Github(c) => match c.accept_webhook(event_type, delivery_id, body)? {
                Inbound::Events(items) => {
                    let builders = items
                        .into_iter()
                        .map(|i| c.hydrate(i))
                        .flat_map(|i| c.to_events(i))
                        .collect();
                    Ok(Inbound::Events(builders))
                }
                Inbound::Lifecycle(change) => Ok(Inbound::Lifecycle(change)),
            },
        }
    }
}
```

The free `route` fn is the credential-free routing peek (a closed match delegating to `github::webhook::route`), used by the worker job which holds no secret and needs none for routing.

### `store.rs`

**Purpose:** The ingest flow's persistence — the RLS-scoped `connection` CRUD/scheduling, and the fingerprint-deduped append to the `event` log.

`insert_event` appends deduplicating on the scope-aware identity: `ON CONFLICT ON CONSTRAINT event_fingerprint_unique DO NOTHING`, returning `Some(event)` if inserted, `None` if it already existed. Because the fingerprint is pure content identity, a poll and a webhook for the same activity within one scope collapse, while two owners seeing the same private activity stay distinct. It also stamps `enriched_at = now()` up front for a source the enrichment sweep will never touch, so it doesn't spin no-op builds or linger in the pending index.

```rust
// crates/core/src/ingest/store.rs:260-300
pub async fn insert_event(
    executor: impl PgExecutor<'_>,
    ev: &NewEvent,
) -> Result<Option<Event>, sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = ev.scope.to_columns();

    sqlx::query(&format!(
        "INSERT INTO event (
            fingerprint, source, scope_kind, scope_subscriber_id,
            event_time, title, body, links, group_key, entities,
            content_kind, severity_hint, raw, connection_id, enriched_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                 CASE WHEN $15 THEN now() END)
        ON CONFLICT ON CONSTRAINT event_fingerprint_unique DO NOTHING
        RETURNING {EVENT_COLUMNS}"
    ))
    .bind(&ev.fingerprint.0[..])
    .bind(ev.source)
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(ev.event_time)
    .bind(&ev.title)
    .bind(ev.body.as_deref())
    .bind(&ev.links)
    .bind(&ev.group_key)
    .bind(&ev.entities)
    .bind(ev.content_kind)
    .bind(ev.severity_hint)
    .bind(ev.raw.as_deref())
    .bind(ev.connection_id)
    // Stamp `enriched_at = now()` up front for a source the enrichment sweep will never touch
    // (`!benefits_from_enrichment` — GitHub/Slack carry structural entities already, §`crate::enrich`):
    // it has no pending enrichment, so it must not match `public_build_due`'s `enriched_at IS NULL`
    // clause (which would spin a no-op build + sweep on every tick through its grace window) nor linger
    // in the `event_enrich_pending` partial index. An enrichable source inserts NULL and the sweep
    // stamps it when it lands.
    .bind(!ev.source.benefits_from_enrichment())
    .try_map(from_row)
    .fetch_optional(executor)
    .await
}
```

`resolve_connection_by_provider` is the IDOR-defense boundary on the webhook path: it resolves the connection (and thus owner/scope) purely by `(source, provider_account_id)`, never from the payload. `record_failure` implements exponential backoff — `poll_interval_secs * 2^consecutive_failures`, capped at 24 h — and flips status to `errored` after 5 consecutive failures; `advance_cursor` conversely resets the counter and reschedules on success.

```rust
// crates/core/src/ingest/store.rs:230-247
pub async fn record_failure(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    sqlx::query(
        "UPDATE connection
         SET consecutive_failures = consecutive_failures + 1,
             next_poll_at = now() + least(
                 poll_interval_secs * power(2, consecutive_failures)::bigint,
                 86400  -- cap at 24 h
             ) * interval '1 second',
             status = CASE WHEN consecutive_failures + 1 >= 5 THEN 'errored' ELSE status END
         WHERE id = $1",
    )
    .bind(id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}
```

Every connection function opens its own admin-scoped transaction via `begin_scope(pool, ScopeCtx::Admin)` (the control-plane RLS context the poll/webhook/tick/debug paths run in) so callers need no scope ceremony. `insert_connection` additionally creates an implicit owner subscription (owning a connection implies subscribing to it).

### `github/`

The GitHub connector: `Connection + RealtimeConnection` where the poll is the correctness floor (a cursor-driven reconciliation over the REST events feed recovering anything a lossy webhook dropped, with `UNIQUE(fingerprint)` collapsing the overlap) and webhooks carry freshness.

#### `github/mod.rs`

**Purpose:** The per-connection worker — repo discovery/allowlist, per-repo conditional-GET polling with a last-seen-id high-water mark, and the `Connection`/`RealtimeConnection` impls.

`poll_repo` fetches one repo's recent activity newer than the cursor's high-water mark: it sends `If-None-Match`, short-circuits on 304, parses the newest-first feed and `take_while`s until the last id already ingested, folds in per-repo privacy (a known-private repo forces all its events private — never a downgrade), and advances the high-water mark to the newest observed id.

```rust
// crates/core/src/ingest/github/mod.rs:162-227
    async fn poll_repo(
        &self,
        token: &str,
        repo: &str,
        private: bool,
        cursor: RepoCursor,
    ) -> Result<(Vec<GithubEvent>, RepoCursor), SourceError> {
        let url = format!("{}/repos/{repo}/events", self.base_url);
        let mut req = self.authed(self.client.get(&url), token);
        if let Some(etag) = &cursor.etag {
            req = req.header(reqwest::header::IF_NONE_MATCH, etag.as_str());
        }
        let resp = req
            .send()
            .await
            .map_err(|e| SourceError::Request(e.to_string()))?;

        if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok((vec![], cursor)); // nothing new for this repo
        }
        if !resp.status().is_success() {
            return Err(SourceError::Request(format!(
                "repo events {repo}: HTTP {}",
                resp.status()
            )));
        }

        let new_etag = resp
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| SourceError::Request(e.to_string()))?;
        let all: Vec<GithubEvent> = serde_json::from_slice(&bytes)
            .map_err(|e| SourceError::Parse(format!("repo events {repo}: {e}")))?;

        // Feed is newest-first; take until we reach the last id we already ingested.
        let mut fresh: Vec<GithubEvent> = match &cursor.last_event_id {
            Some(seen) => all.into_iter().take_while(|e| &e.id != seen).collect(),
            None => all, // first poll: take the page
        };
        // A known-private repo forces every one of its events private (never a downgrade), so the
        // privacy fold lives here next to the parse. Discovery sets `private` from the authoritative
        // per-repo flag; an allowlist can't, so its repos pass `false` and rely on the per-event
        // `public` flag the feed itself carries.
        if private {
            for ev in &mut fresh {
                ev.public = false;
            }
        }
        // Advance the high-water mark to the newest id observed (even if it was a 304-less empty
        // page); keep the prior one when this page had nothing.
        let newest = fresh.first().map(|e| e.id.clone()).or(cursor.last_event_id);

        Ok((
            fresh,
            RepoCursor {
                etag: new_etag,
                last_event_id: newest,
            },
        ))
    }
```

`accept_webhook` routes App lifecycle events (`installation` / `installation_repositories`) to a status change via `event_map::lifecycle_status`, and synthesizes everything else into the same `GithubEvent` shape the poll yields (via `from_webhook`) so the two intakes dedup.

```rust
// crates/core/src/ingest/github/mod.rs:286-308
    fn accept_webhook(
        &self,
        event_type: &str,
        delivery_id: &str,
        body: &[u8],
    ) -> Result<Inbound<Self::Item>, SourceError> {
        let value: serde_json::Value = serde_json::from_slice(body)
            .map_err(|e| SourceError::Parse(format!("webhook body is not JSON: {e}")))?;

        if matches!(event_type, "installation" | "installation_repositories") {
            return Ok(match event_map::lifecycle_status(&value) {
                Some(status) => Inbound::Lifecycle(LifecycleChange { status }),
                // install / new_permissions / repos add-remove: nothing to ingest, status unchanged.
                None => Inbound::Events(vec![]),
            });
        }

        Ok(Inbound::Events(vec![event_map::from_webhook(
            event_type,
            delivery_id,
            value,
        )]))
    }
```

The shared `github_headers` pins the JSON media type + `X-GitHub-Api-Version: 2022-11-28` + UA on every call; `realtime_only()` builds a worker backed by `UnavailableToken` so webhook normalization runs before poll credentials are wired.

#### `github/app.rs`

**Purpose:** The real GitHub App → installation-token exchange — the two-hop auth (sign a short-lived app JWT, exchange for a ~1 h installation token) with an expiry-aware per-installation cache.

The auth constants encode the JWT lifetime rules: GitHub caps the app JWT at 10 min and rejects a future `iat`, so it signs for 9 min and backdates `iat` 60 s for clock skew; the installation token is re-minted 60 s before actual expiry so an in-flight poll never races it.

```rust
// crates/core/src/ingest/github/app.rs:26-31
const EXPIRY_SKEW_SECS: i64 = 60;
/// App-JWT lifetime. GitHub caps it at 10 min and rejects a future `iat` past its own clock, so we
/// sign for 9 min and backdate `iat` 60 s to tolerate skew between us and GitHub.
const APP_JWT_TTL_SECS: i64 = 9 * 60;
const APP_JWT_BACKDATE_SECS: i64 = 60;
```

`app_jwt` mints the RS256-signed app JWT (over the `Arc<EncodingKey>` unsealed once at startup, never logged), and `mint` exchanges it at `POST /app/installations/{id}/access_tokens` for the installation token.

```rust
// crates/core/src/ingest/github/app.rs:77-113
    /// Sign a fresh app JWT (RS256). Short-lived and minted per refresh; never stored.
    fn app_jwt(&self) -> Result<String, SourceError> {
        let now = Utc::now().timestamp();
        let claims = AppClaims {
            iat: now - APP_JWT_BACKDATE_SECS,
            exp: now + APP_JWT_TTL_SECS,
            iss: self.app_id,
        };
        jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &self.encoding_key)
            .map_err(|e| SourceError::Request(format!("signing GitHub App JWT failed: {e}")))
    }
}

/// A per-installation installation-token source with an expiry-aware cache. Implements
/// [`TokenProvider`]; the connector calls [`access_token`](TokenProvider::access_token) every poll
/// and gets a cached token until it nears expiry, when one mint refreshes it under the mutex.
pub struct GithubAppTokens {
    app: GithubApp,
    installation_id: i64,
    cache: Mutex<Option<Token>>,
}

impl GithubAppTokens {
    async fn mint(&self) -> Result<Token, SourceError> {
        let jwt = self.app.app_jwt()?;
        let url = format!(
            "{}/app/installations/{}/access_tokens",
            self.app.base_url, self.installation_id
        );
        let req = super::github_headers(self.app.client.post(&url).bearer_auth(jwt));
        let parsed: TokenResponse = super::fetch_json(req, "installation token exchange").await?;
        Ok(Token {
            secret: parsed.token,
            expires_at: parsed.expires_at,
        })
    }
}
```

The `TokenProvider` impl is the cache: it returns the cached token while it is outside the skew window of expiring, otherwise mints once under the mutex (so a burst of polls makes a single exchange) and stores the result.

```rust
// crates/core/src/ingest/github/app.rs:115-130
impl TokenProvider for GithubAppTokens {
    fn access_token(&self) -> TokenFuture<'_> {
        Box::pin(async move {
            let mut cache = self.cache.lock().await;
            // Reuse the cached token until it's within the skew window of expiring.
            if let Some(token) = cache.as_ref() {
                if token.expires_at > Utc::now() + chrono::Duration::seconds(EXPIRY_SKEW_SECS) {
                    return Ok(token.clone());
                }
            }
            let token = self.mint().await?;
            *cache = Some(token.clone());
            Ok(token)
        })
    }
}
```

#### `github/event_map.rs`

**Purpose:** The single GitHub-activity → canonical-event mapping — everything is captured (unknown types fall through to a generic event, never dropped), with `stable_id` (dedup identity) and `to_builder` (title/group_key/content_kind/links/entities) owning the source semantics.

`stable_id` is the content-identity dedup key folded into the fingerprint. It is derived from content ids present in *both* the REST feed and the webhook payload (issue/PR/release/comment ids, push head SHA) — never the REST event id or webhook delivery id, which differ across the two intakes and would defeat dedup. Push identity is the head SHA (`payload.head` on REST, `payload.after` on webhook, mapped to the same value).

```rust
// crates/core/src/ingest/github/event_map.rs:96-135
pub fn stable_id(ev: &GithubEvent) -> String {
    let repo = &ev.repo.name;
    let act = action(ev).unwrap_or_default();
    match ev.kind.as_str() {
        "IssuesEvent" => match obj_id(ev, "issue") {
            Some(id) => format!("issue:{id}:{act}"),
            None => fallback(ev),
        },
        "IssueCommentEvent" | "CommitCommentEvent" | "PullRequestReviewCommentEvent" => {
            match obj_id(ev, "comment") {
                Some(id) => format!("comment:{id}"),
                None => fallback(ev),
            }
        }
        "PullRequestEvent" => match obj_id(ev, "pull_request") {
            Some(id) => format!("pr:{id}:{act}"),
            None => fallback(ev),
        },
        "PullRequestReviewEvent" => match obj_id(ev, "review") {
            Some(id) => format!("review:{id}"),
            None => fallback(ev),
        },
        "ReleaseEvent" => match obj_id(ev, "release") {
            Some(id) => format!("release:{id}:{act}"),
            None => fallback(ev),
        },
        // Push identity = the head SHA (REST: payload.head; webhook: payload.after — Phase 2 maps
        // the webhook field to the same value), so a re-poll/webhook overlap collapses.
        "PushEvent" => match payload_str(ev, &["head"]).or_else(|| payload_str(ev, &["after"])) {
            Some(sha) => format!("push:{repo}:{sha}"),
            None => fallback(ev),
        },
        _ => fallback(ev),
    }
}
```

`from_webhook` synthesizes a `GithubEvent` from a verified delivery so it flows through the exact `stable_id`/`group_key`/`to_builder` path the poll uses (a webhook body *is* the REST item's `payload` shape). The delivery id is only the generic-fallback identity; `repository.private` sets the `public` flag.

```rust
// crates/core/src/ingest/github/event_map.rs:265-295
pub fn from_webhook(event_type: &str, delivery_id: &str, body: serde_json::Value) -> GithubEvent {
    let kind = rest_type(event_type);
    let repo = body
        .get("repository")
        .and_then(|r| r.get("full_name"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let actor = body
        .get("sender")
        .and_then(|s| s.get("login"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let created_at = webhook_time(&kind, &body).unwrap_or_else(Utc::now);
    // Webhooks carry repo visibility directly (`repository.private`); absent ⇒ treat as public.
    let private = body
        .get("repository")
        .and_then(|r| r.get("private"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    GithubEvent {
        id: delivery_id.to_string(),
        kind,
        repo: RepoRef { name: repo },
        actor: ActorRef { login: actor },
        created_at,
        payload: body,
        public: !private,
    }
}
```

`to_builder` produces the connector-side builder: namespaced structural entities (`repo:`/`user:`), the type's html_url as links, a deterministic structural salience (no LLM — enrichment is deliberately not run over GitHub), and the `stable_id` the fingerprint is computed over. It reports only the structural `private` bool; `finalize` binds it to the connection's owner.

```rust
// crates/core/src/ingest/github/event_map.rs:312-341
pub fn to_builder(ev: GithubEvent) -> EventBuilder {
    // Structural entities, namespaced so they classify as *weak* linking keys and don't collide
    // across kinds (`repo:acme/x` ≠ `user:acme`). `finalize` adds the cross-source `cve:`/`url:`
    // keys mined from the title/links on top (§8.2).
    let mut entities = vec![format!("repo:{}", ev.repo.name)];
    if !ev.actor.login.is_empty() {
        entities.push(format!("user:{}", ev.actor.login));
    }
    let links: Vec<String> = html_url(&ev).into_iter().collect();

    let mut builder = EventBuilder::new(
        SourceKind::Github,
        stable_id(&ev),
        ev.created_at,
        title(&ev),
        group_key(&ev),
    )
    .content_kind(content_kind(&ev.kind))
    .links(links)
    .entities(entities);
    // Structural salience: GitHub's activity kind is exact, so importance is a deterministic map (no
    // LLM — enrichment is deliberately not run over GitHub). A release/issue/PR boosts priority over
    // routine push/star churn; routine (0) leaves the hint unset (`None`), exactly as before.
    let importance = salience::github_importance(&ev.kind);
    if importance > salience::ROUTINE {
        builder = builder.severity_hint(importance);
    }
    // The adapter reports only the structural bool; `finalize` binds it to the connection's owner.
    builder.private(!ev.public)
}
```

`group_key` clusters a thread's activity (issue + comments, PR + reviews) under one key; `rest_type` converts a webhook's snake_case `X-GitHub-Event` to the PascalCase REST `type`; `lifecycle_status` maps `deleted`/`suspend`/`unsuspend` actions to `Revoked`/`Suspended`/`Active`.

#### `github/token.rs`

**Purpose:** The `TokenProvider` port the connector drives, decoupling it from where a bearer token comes from — plus static (fixture) and unavailable (realtime-only) impls.

The trait is dyn-compatible via a boxed future (`TokenFuture`) so the connector holds one `Arc<dyn TokenProvider>` regardless of whether the token came from the real JWT exchange or a test's static provider — no generic leaks through `GithubConnection` into the dispatch enum.

```rust
// crates/core/src/ingest/github/token.rs:27-36
pub type TokenFuture<'a> = Pin<Box<dyn Future<Output = Result<Token, SourceError>> + Send + 'a>>;

/// Mints/returns an installation access token. Infra caches + serializes refreshes per connection
/// (design §3A); a provider impl may itself cache, so `access_token` is cheap to call per poll.
pub trait TokenProvider: Send + Sync {
    fn access_token(&self) -> TokenFuture<'_>;
}
```

`UnavailableToken` backs the realtime-only worker and errors if asked — calling `poll` on it is a bug, turned into a clear runtime error rather than a malformed request; `StaticTokenProvider` yields a fixed far-future token for fixtures.

```rust
// crates/core/src/ingest/github/token.rs:67-78
pub struct UnavailableToken;

impl TokenProvider for UnavailableToken {
    fn access_token(&self) -> TokenFuture<'_> {
        Box::pin(async {
            Err(SourceError::Request(
                "no token provider configured (realtime-only GitHub worker cannot poll)"
                    .to_string(),
            ))
        })
    }
}
```

#### `github/webhook.rs`

**Purpose:** GitHub's app-level webhook head — constant-time HMAC signature verification (fail-closed) and credential-free routing.

`verify` reproduces the `X-Hub-Signature-256` HMAC-SHA256 over the *raw* bytes with the App's webhook secret and compares in constant time via the `hmac` crate's `verify_slice`. It fails closed on every branch: no secret configured, missing signature, malformed header prefix, or bad hex all return `Invalid`.

```rust
// crates/core/src/ingest/github/webhook.rs:33-56
impl RealtimeConnector for GithubWebhook {
    fn verify(&self, headers: &WebhookHeaders, body: &[u8]) -> Verified {
        let Some(secret) = &self.secret else {
            return Verified::Invalid; // no secret configured → reject (fail closed)
        };
        let Some(sig) = headers.signature.as_deref() else {
            return Verified::Invalid; // GitHub always signs; a missing signature is a reject
        };
        // Header form is exactly "sha256=<hex>"; reject anything else.
        let Some(hex_sig) = sig.strip_prefix("sha256=") else {
            return Verified::Invalid;
        };
        let Ok(expected) = hex::decode(hex_sig) else {
            return Verified::Invalid;
        };
        let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
        mac.update(body);
        // `verify_slice` is constant-time and length-checks the tag before comparing.
        match mac.verify_slice(&expected) {
            Ok(()) => Verified::Authentic,
            Err(_) => Verified::Invalid,
        }
    }
}
```

`route` is the credential-free routing peek: it parses just enough JSON to read `installation.id` (the routing key used to resolve OUR connection row), trusting nothing else in the payload for authorization — the IDOR defense.

```rust
// crates/core/src/ingest/github/webhook.rs:61-70
pub fn route(body: &[u8]) -> Result<String, SourceError> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| SourceError::Parse(format!("webhook body is not JSON: {e}")))?;
    value
        .get("installation")
        .and_then(|i| i.get("id"))
        .and_then(|id| id.as_i64())
        .map(|id| id.to_string())
        .ok_or_else(|| SourceError::Parse("webhook missing installation.id".to_string()))
}
```


---

## `cluster` — event log → clusters

The `cluster` module is the consumer of the ingest→clustering seam and the producer of the clustering→digest seam. It drains the append-only event log via a build watermark cursor and folds each *dirtied within-source group* into a single representative `cluster` row. There are two symmetric builds — a global **PublicBuild** and a per-subscriber **PrivateBuild** — that share one per-group folding body and differ only in scope, advisory lock, bounds, and watermark. Scope is part of the cluster identity, so a public and a private event can never fold into the same cluster; this is the primary typed isolation defense (ahead of Phase-4 RLS).

### `cluster/mod.rs`

**Purpose** — the clustering flow orchestration and the two pure kernels: `rollup` (fold a group into a `ClusterRollup`) and the scope-key/visibility invariants (`ClusterKey`, `visible_to`).

The build boundary types are `ClusterKey` (the in-code mirror of the DB uniqueness constraint) and `ClusterRollup` (the recomputed row the build upserts — representative title/link, recency span, the sorted-deduped union of member `entities` that M3 linking blocks on, and the M4 scoring signals `event_count` / `content_depth` / `max_severity`, all folded in one scan). The `rollup` fold picks the latest event by `event_time` as the representative and takes maxes for depth/severity:

```rust
// crates/core/src/cluster/mod.rs:52-84
pub fn rollup(events: &[Event]) -> Option<ClusterRollup> {
    let mut representative = events.first()?;
    let mut first = representative.event_time;
    let mut entities: Vec<String> = Vec::new();
    // Depth is the max content_kind; the conservative floor is the lowest variant (Message), lifted
    // by every event. Severity is the max over the events that carried a hint (None if none did).
    let mut content_depth = ContentKind::Message;
    let mut max_severity: Option<i16> = None;
    for ev in events {
        if ev.event_time >= representative.event_time {
            representative = ev;
        }
        first = first.min(ev.event_time);
        entities.extend(ev.entities.iter().cloned());
        content_depth = content_depth.max(ev.content_kind);
        max_severity = max_severity.max(ev.severity_hint);
    }
    entities.sort();
    entities.dedup();
    Some(ClusterRollup {
        title: representative.title.clone(),
        link: representative.links.first().cloned(),
        first_event_time: first,
        last_event_time: representative.event_time,
        entities,
        event_count: events.len() as i32,
        content_depth,
        max_severity,
        // The representative (latest) event's source — a within-source group shares it; for the
        // single-article public RSS cluster this is exactly the feed the subscriber subscribes to.
        connection_id: representative.connection_id,
    })
}
```

The `PublicBuild` (`build`) runs the whole pass in one transaction holding a transaction-level advisory lock so concurrent builds serialize (the loser returns `Ok(None)`) and a crash rolls back without advancing the watermark. The high watermark is `now() - enrich_grace`, the Phase-2 cluster-eligibility deadline: an event is not clusterable until it ages past the grace window (by which point the best-effort enrichment sweep has run), and `Duration::ZERO ⇒ hwm = now()` recovers pre-Phase-2 behavior.

```rust
// crates/core/src/cluster/mod.rs:136-169
pub async fn build(pool: &PgPool, enrich_grace: Duration) -> Result<Option<BuildStats>> {
    // PublicBuild runs in the no-subscriber RLS context: it can read and write only public rows, so
    // it physically cannot pull a private event into a public cluster (design §12).
    let mut tx = begin_scope(pool, ScopeCtx::NoSubscriber)
        .await
        .context("begin build txn")?;

    if !store::try_build_lock(&mut *tx)
        .await
        .context("acquire build lock")?
    {
        tracing::debug!("public build already in progress; skipping");
        return Ok(None);
    }

    let (built_through, hwm) = store::build_bounds(&mut *tx, enrich_grace)
        .await
        .context("read build bounds")?;
    let groups = store::dirty_groups(&mut *tx, &Scope::Public, built_through, hwm)
        .await
        .context("find dirty groups")?;

    build_groups(&mut tx, &Scope::Public, &groups).await?;

    store::advance_build_watermark(&mut *tx, hwm)
        .await
        .context("advance watermark")?;
    tx.commit().await.context("commit build txn")?;

    Ok(Some(BuildStats {
        dirty_groups: groups.len(),
        built_through: hwm,
    }))
}
```

`ClusterKey` and `visible_to` are the isolation invariants, pinned by the scope-invariant proptests: scope is part of the key, and a private cluster is visible only to its owner.

```rust
// crates/core/src/cluster/mod.rs:91-116
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClusterKey {
    pub scope: Scope,
    pub source: SourceKind,
    pub group_key: String,
}

/// The cluster a finalized event belongs to. Pure; mirrors the build's grouping.
pub fn cluster_key(event: &Event) -> ClusterKey {
    ClusterKey {
        scope: event.scope.clone(),
        source: event.source,
        group_key: event.group_key.clone(),
    }
}

// …

pub fn visible_to(scope: &Scope, subscriber: Uuid) -> bool {
    match scope {
        Scope::Public => true,
        Scope::Private(owner) => *owner == subscriber,
    }
}
```

### `cluster/store.rs`

**Purpose** — the PublicBuild store contract: the build watermark, the dirty-group scan, and the idempotent cluster upsert. The `cluster` rows and `build_watermark` are a rebuildable cache; durable truth is the events.

`build_bounds` reads `(built_through, now() - enrich_grace)` in one shot, keeping the half-open range `(built_through, hwm]` behind `now()` so grace-window events are left for the enrichment sweep. `dirty_groups` then finds the distinct `(source, group_key)` groups *in scope* touched in that range; the `IS NOT DISTINCT FROM` clause matches the scope's nullable subscriber uniformly and is the shared isolation boundary for both builds:

```rust
// crates/core/src/cluster/store.rs:53-73
pub async fn dirty_groups(
    executor: impl PgExecutor<'_>,
    scope: &Scope,
    lo: DateTime<Utc>,
    hi: DateTime<Utc>,
) -> Result<Vec<(SourceKind, String)>, sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = scope.to_columns();
    sqlx::query(
        "SELECT DISTINCT source, group_key
         FROM event
         WHERE scope_kind = $1 AND scope_subscriber_id IS NOT DISTINCT FROM $2
           AND ingest_time > $3 AND ingest_time <= $4",
    )
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(lo)
    .bind(hi)
    .try_map(|row: PgRow| Ok((row.try_get("source")?, row.get::<String, _>("group_key"))))
    .fetch_all(executor)
    .await
}
```

`upsert_cluster` writes the recomputed rollup, keyed on the `cluster_identity` constraint so re-running a build overwrites the cache in place. Scope is part of the identity, so a public and a private group with the same `(source, group_key)` are distinct rows:

```rust
// crates/core/src/cluster/store.rs:79-122
pub async fn upsert_cluster(
    executor: impl PgExecutor<'_>,
    scope: &Scope,
    source: SourceKind,
    group_key: &str,
    r: &ClusterRollup,
) -> Result<Uuid, sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = scope.to_columns();
    let row = sqlx::query(
        "INSERT INTO cluster
            (scope_kind, scope_subscriber_id, source, group_key, title, link,
             first_event_time, last_event_time, entities,
             event_count, content_depth, max_severity, connection_id, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now())
         ON CONFLICT ON CONSTRAINT cluster_identity DO UPDATE SET
            title = EXCLUDED.title,
            link = EXCLUDED.link,
            first_event_time = EXCLUDED.first_event_time,
            last_event_time = EXCLUDED.last_event_time,
            entities = EXCLUDED.entities,
            event_count = EXCLUDED.event_count,
            content_depth = EXCLUDED.content_depth,
            max_severity = EXCLUDED.max_severity,
            connection_id = EXCLUDED.connection_id,
            updated_at = now()
         RETURNING id",
    )
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(source)
    .bind(group_key)
    .bind(&r.title)
    .bind(r.link.as_deref())
    .bind(r.first_event_time)
    .bind(r.last_event_time)
    .bind(&r.entities)
    .bind(r.event_count)
    .bind(r.content_depth)
    .bind(r.max_severity)
    .bind(r.connection_id)
    .fetch_one(executor)
    .await?;
    Ok(row.get("id"))
}
```

The watermark advance is monotonic via `GREATEST`, race-free against concurrent inserts:

```rust
// crates/core/src/cluster/store.rs:160-169
pub async fn advance_build_watermark(
    executor: impl PgExecutor<'_>,
    hwm: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE build_watermark SET built_through = GREATEST(built_through, $1)")
        .bind(hwm)
        .execute(executor)
        .await?;
    Ok(())
}
```

## `link` — deterministic cross-source story fusion

The `link` module is the pure core of per-subscriber linking: it fuses a subscriber's candidate clusters into cross-source **stories** as a deterministic function of `(clusters, prior assignment, id minter)` — no I/O, no ambient clock — so it is exhaustively proptested for determinism, id-stability, and partitioning. It cannot be a global precompute because a story may fuse public clusters with a subscriber's *own* private clusters, so it runs per subscriber inside `GenerateDigest`. The algorithm is textbook entity resolution in four stages: **blocking** (inverted index over entities), **scoring** (graded weighted-Jaccard + temporal, with strong-key promotion), **components** (union-find with the asymmetric-merge guard), and **forwarding** (stable id assignment + retro-merge tombstones).

### `link/mod.rs`

**Purpose** — the whole four-stage algorithm: tunables, `score_edges`/`build_edge` (graded edge weights, weak-edge floors, saturation guard), `components` (asymmetric-guarded union-find), and `forward_ids` (stable id forwarding + `Merge`).

The tunables encode the guardrails against single-linkage chaining — a weak edge needs both a score floor *and* a minimum weighted-Jaccard overlap so temporal proximity can corroborate but not carry an edge, plus a saturation guard that restores IDF's say-so when the shared set is the whole of both sparse clusters:

```rust
// crates/core/src/link/mod.rs:120-148
/// Weight on entity Jaccard in a weak edge's score.
const W_JACCARD: f64 = 0.7;
/// Weight on temporal closeness in a weak edge's score.
const W_TEMPORAL: f64 = 0.3;
/// A weak edge forms only at or above this score — corroboration beyond a single shared weak token.
const WEAK_EDGE_THRESHOLD: f64 = 0.35;
/// A weak edge also requires at least this much **weighted** entity overlap (the weighted Jaccard over
/// the distinctive linkable entities, [`build_edge`]). Temporal proximity **corroborates** an edge; it
/// must not be able to *carry* one. Without this floor two same-window items linked on a single broad
/// shared token — `place:germany`, a shared stock-photo agency — because the temporal term
/// (`W_TEMPORAL`) alone nearly clears the threshold, chaining unrelated coverage into one blob (the
/// reported "wildly inaccurate" grouping). Tuned so a genuine same-story pair (several shared
/// distinctive entities, or one dominant high-weight one) still links, while a lone broad coincidence —
/// already IDF-demoted to a small weight — does not.
const WEAK_MIN_JACCARD: f64 = 0.2;
/// A weighted Jaccard at/above this is **saturated**: the shared entities are (essentially) the whole of
/// *both* clusters' linkable sets, so the weight cancels in the ratio (`w/(w+w−w)=1`) and `wj` reports a
/// perfect match no matter how IDF-demoted the shared token is. This is the one case where IDF can't bite
/// — two entity-sparse items sharing a single broad token (`place:germany` and nothing else linkable).
const SATURATED_WJ: f64 = 0.999;
/// For a **saturated** weak edge ([`SATURATED_WJ`]) the ratio is uninformative, so the shared mass must
/// clear this **absolute** weight instead — restoring IDF's say-so. A single broad, IDF-demoted token
/// (high df ⇒ small weight) falls below it and can't fuse two sparse items, while a rare/specific shared
/// token, or several shared tokens (their weights sum), clears it. Picked so a `place:`/`org:` that recurs
/// across a large fraction of the candidate set is rejected while a distinctive single share passes; like
/// the other thresholds this is conservative and lands in the §15 config table once tuned on real digests.
const MIN_SATURATED_SHARED_WEIGHT: f64 = 1.2;
/// Temporal closeness decays linearly to zero across this many days apart.
const TEMPORAL_WINDOW_DAYS: f64 = 14.0;
```

**Stage 1–2 — blocking + graded scoring.** `score_edges` builds an inverted index over *linkable* entities only (which doubles as the document-frequency table for IDF), computes each entity's graded weight once, then does a single scatter pass: each entity adds its weight to every co-occurring pair and records strong-promotion / best-weak reason, so a pair's shared weight, strong key, and reason all fall out without re-intersecting clusters:

```rust
// crates/core/src/link/mod.rs:266-319
fn score_edges(clusters: &[LinkCluster], entity_weights: &EntityWeights) -> Vec<Edge> {
    let n = clusters.len();
    // Inverted index entity → cluster indices, over *linkable* entities only (a `domain:`/`topic:` is
    // noise as an edge, so it never seeds a candidate pair). It doubles as the **document-frequency**
    // table: `members.len()` is df(entity), the IDF input. BTree keeps iteration stable.
    let mut index: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (i, c) in clusters.iter().enumerate() {
        for e in c.entities.iter().filter(|e| link_strength(e).is_some()) {
            index.entry(e.as_str()).or_default().push(i);
        }
    }

    // The graded weight of each linkable entity, computed once from its df. This is the "grade" — how
    // much *sharing this token* is worth — pre-seeded by the namespace prior and the live corpus IDF,
    // and nudged by any feedback override; no labelled data or denylist required.
    let weight: BTreeMap<&str, f64> = index
        .iter()
        .map(|(&e, members)| (e, entity_weight(e, members.len(), n, entity_weights)))
        .collect();

    // Per-cluster Σ of its linkable entities' weights — the |A|/|B| terms of the weighted Jaccard.
    let mut cluster_weight = vec![0.0f64; n];
    for (&e, members) in &index {
        let w = weight[e];
        for &i in members {
            cluster_weight[i] += w;
        }
    }

    // One scatter pass over the index: each entity adds its weight to every pair of clusters it
    // co-occurs in, and records whether it is a strong key or the pair's best weak reason — so a pair's
    // shared weight, strong-promotion, and reason all fall out without re-intersecting two clusters per
    // pair. The cost is Σ C(df,2) — the blocking work the old pair enumeration already paid.
    let mut acc: BTreeMap<(usize, usize), PairAcc> = BTreeMap::new();
    for (&e, members) in &index {
        let w = weight[e];
        let strong = link_strength(e) == Some(LinkStrength::Strong);
        for (oi, &i) in members.iter().enumerate() {
            for &j in &members[oi + 1..] {
                let p = acc.entry((i.min(j), i.max(j))).or_default();
                p.shared_weight += w;
                if strong {
                    p.strong_key.get_or_insert(e);
                } else if p.best_weak.is_none_or(|(bw, _)| w > bw) {
                    p.best_weak = Some((w, e));
                }
            }
        }
    }

    acc.into_iter()
        .filter_map(|((a, b), p)| build_edge(a, b, &p, clusters, &cluster_weight))
        .collect()
}
```

`build_edge` is where a pair becomes an edge or is rejected: a shared strong key is an unconditional strong edge at score 1.0; otherwise the weighted Jaccard blended with temporal closeness must clear the threshold *and* the overlap floor, then the saturation guard applies the absolute shared-weight floor:

```rust
// crates/core/src/link/mod.rs:326-368
fn build_edge(
    a: usize,
    b: usize,
    p: &PairAcc,
    clusters: &[LinkCluster],
    cluster_weight: &[f64],
) -> Option<Edge> {
    if let Some(key) = p.strong_key {
        return Some(Edge {
            a,
            b,
            strong: true,
            score: 1.0,
            reason: format!("shared {key}"),
        });
    }
    let union = cluster_weight[a] + cluster_weight[b] - p.shared_weight;
    let wj = if union > 0.0 {
        p.shared_weight / union
    } else {
        0.0
    };
    let score = W_JACCARD * wj + W_TEMPORAL * temporal_closeness(&clusters[a], &clusters[b]);
    if score < WEAK_EDGE_THRESHOLD || wj < WEAK_MIN_JACCARD {
        return None;
    }
    // Saturation guard: when `wj` is saturated (the shared entities are the whole of both clusters'
    // linkable sets) the ratio is a no-op and IDF can't demote a broad shared token, so fall back to an
    // absolute floor on the shared weight — a single IDF-demoted token can't fuse two sparse items, a
    // distinctive one (or several shared) still can. Non-saturated pairs are unaffected (the ratio
    // already carries the corroboration signal).
    if wj >= SATURATED_WJ && p.shared_weight < MIN_SATURATED_SHARED_WEIGHT {
        return None;
    }
    let (_, key) = p.best_weak?;
    Some(Edge {
        a,
        b,
        strong: false,
        score,
        reason: format!("shared {key}"),
    })
}
```

The graded weight is `namespace_prior × idf × feedback multiplier`; IDF is smoothed so a corpus-wide token floors at 1 rather than 0 (corroborated coverage of one happening must still link) while a rarer token scores strictly higher:

```rust
// crates/core/src/link/mod.rs:375-404
fn entity_weight(entity: &str, df: usize, n: usize, overrides: &EntityWeights) -> f64 {
    let mult = overrides.get(entity).copied().unwrap_or(1.0);
    namespace_prior(entity) * idf(df, n) * mult
}

// …

fn namespace_prior(entity: &str) -> f64 {
    match crate::identity::namespace(entity).map(|(ns, _)| ns) {
        Some("cve") | Some("url") => 1.0,
        Some("person") => 1.0,
        Some("repo") | Some("user") => 0.9,
        Some("org") => 0.8,
        Some("place") => 0.7,
        _ => 0.0,
    }
}

// …

fn idf(df: usize, n: usize) -> f64 {
    ((n as f64 + 1.0) / (df as f64 + 1.0)).ln() + 1.0
}
```

**Stage 3 — components with the asymmetric guard.** The union-find carries, per component, the set of already-*delivered* prior story ids it contains — a set, not a bool, so the guard can distinguish re-linking clusters of the *same* delivered story (allowed) from merging two *different* delivered stories (forbidden for weak edges). Two passes: strong edges first (they may merge anything, the intended retro-merge), then weak edges strongest-first, each skipped if it would merge two distinct delivered stories:

```rust
// crates/core/src/link/mod.rs:508-582
    fn merges_distinct_delivered(&mut self, a: usize, b: usize) -> bool {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return false;
        }
        let (da, db) = (&self.delivered[ra], &self.delivered[rb]);
        !da.is_empty() && !db.is_empty() && da.is_disjoint(db)
    }
}

// …

fn components(
    clusters: &[LinkCluster],
    edges: &[Edge],
    prior: &[PriorMember],
) -> BTreeMap<usize, Vec<usize>> {
    let prior_story: BTreeMap<Uuid, Uuid> =
        prior.iter().map(|p| (p.cluster_id, p.story_id)).collect();
    let delivered_stories: BTreeSet<Uuid> = prior
        .iter()
        .filter(|p| p.delivered)
        .map(|p| p.story_id)
        .collect();
    // Per cluster: the delivered prior story it belongs to (a singleton set), else empty.
    let delivered: Vec<BTreeSet<Uuid>> = clusters
        .iter()
        .map(|c| {
            prior_story
                .get(&c.id)
                .filter(|s| delivered_stories.contains(s))
                .into_iter()
                .copied()
                .collect()
        })
        .collect();

    let mut uf = UnionFind::new(delivered);

    for e in edges.iter().filter(|e| e.strong) {
        uf.union(e.a, e.b);
    }

    let mut weak: Vec<&Edge> = edges.iter().filter(|e| !e.strong).collect();
    // Strongest weak edge first; deterministic tie-break by endpoints.
    weak.sort_by(|x, y| {
        y.score
            .partial_cmp(&x.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then((x.a, x.b).cmp(&(y.a, y.b)))
    });
    for e in weak {
        if uf.merges_distinct_delivered(e.a, e.b) {
            continue; // would collapse two already-delivered stories — only a strong edge may
        }
        uf.union(e.a, e.b);
    }

    let mut comps: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for i in 0..clusters.len() {
        let r = uf.find(i);
        comps.entry(r).or_default().push(i);
    }
    for members in comps.values_mut() {
        members.sort_unstable();
    }
    comps
}
```

**Stage 4 — stable id forwarding.** Each component maps back onto a prior story id (the *oldest*, since uuidv7 is time-ordered), a prior id is claimed by at most one component, and any prior id present-but-unclaimed becomes a retro-merge loser tombstoned into its survivor:

```rust
// crates/core/src/link/mod.rs:660-685
    // Pass 1: claim a survivor id per component (oldest unclaimed prior id, else a fresh mint).
    let mut claimed: BTreeSet<Uuid> = BTreeSet::new();
    let survivors: Vec<Uuid> = component_prior_ids
        .iter()
        .map(|prior_ids| {
            let survivor = match prior_ids.iter().find(|id| !claimed.contains(id)) {
                Some(&id) => id,
                None => mint(),
            };
            claimed.insert(survivor);
            survivor
        })
        .collect();

    // Pass 2: any prior id present in a component but kept by *no* component was absorbed → a merge.
    let mut merges = Vec::new();
    for (prior_ids, &survivor) in component_prior_ids.iter().zip(&survivors) {
        for &id in prior_ids {
            if id != survivor && !claimed.contains(&id) {
                merges.push(Merge {
                    loser: id,
                    survivor,
                });
            }
        }
    }
```

### `link/store.rs`

**Purpose** — story persistence: read the prior assignment for stable-id forwarding (`load_prior_members`), assemble the subscriber's candidate cluster set (`candidate_clusters`), and upsert the recompute plus retro-merge tombstones (`persist_assignment`). Every read and write is fenced by `subscriber_id`.

`candidate_clusters` is the two-arm blocking query. The **in-floor** arm returns clusters updated since a freshness floor (clamped to at most `MAX_LOOKBACK_DAYS` for a dormant subscriber), subscription-gated on the public side; the **cross-boundary seed** arm returns public clusters that share a strong `cve:`/`url:` key with the subscriber's *active* private clusters *regardless of the floor*, so a fresh private incident still links to an aged-out public advisory — the exact cross-source connection the product exists to surface:

```rust
// crates/core/src/link/store.rs:66-133
    sqlx::query(
        // The candidate floor reaches back to the last delivery (`LEAST(last_run, now − horizon)`,
        // so nothing since the last digest ages out unconsidered), but never past `MAX_LOOKBACK_DAYS`
        // ($5) — the `GREATEST` clamp bounds a dormant subscriber's scan for perf without dropping any
        // context a fresh event could still corroborate with.
        "WITH floor AS (
                  SELECT GREATEST(
                             LEAST($2, now() - make_interval(days => $3)),
                             now() - make_interval(days => $5)
                         ) AS lo),
              in_floor AS (
                  SELECT id, scope_kind, source, entities, first_event_time, last_event_time,
                         event_count, content_depth, max_severity
                  FROM cluster
                  -- The candidate scope, now an explicit per-source filter: a public cluster enters
                  -- only if the subscriber subscribes to the connection that produced it (the source
                  -- they chose for their digest); own-private always. `scope_subscriber_id = $1` stays
                  -- the isolation boundary — never another subscriber's private cluster.
                  --
                  -- `connection_id IS NULL` (an unattributed public cluster — no originating feed on
                  -- record) is treated as global. Real ingest (poll/webhook) always stamps the
                  -- connection, so this only covers off-path/fixture rows; an attributed public source
                  -- is always subscription-gated. Source selection is a product filter, not a privacy
                  -- boundary (that's RLS), so failing open for the degenerate no-origin case is safe.
                  WHERE (
                          (scope_kind = 'public'
                              AND (connection_id IS NULL
                                   OR connection_id IN (
                                       SELECT connection_id FROM subscription
                                       WHERE subscriber_id = $1)))
                          OR scope_subscriber_id = $1
                        )
                    AND updated_at >= (SELECT lo FROM floor)
                    AND (NOT $4 OR summary ->> 'band' IN ('confirmed', 'probable'))
              ),
              -- The strong keys (cve:/url:, mirrors entity::link_strength) the subscriber's *active*
              -- private clusters carry — floored like in_floor, so the seed scales with recent
              -- private activity (a fresh incident), not lifetime history. NULL when they have
              -- none, which makes the seed below a no-op. (Ungated: an unsummarized private incident
              -- can still pull in its related public advisory, which is itself summary-gated below;
              -- the incident itself stays out of the candidate set via the in_floor gate.)
              private_strong AS (
                  SELECT array_agg(DISTINCT e) AS keys
                  FROM cluster c, unnest(c.entities) AS e
                  WHERE c.scope_subscriber_id = $1
                    AND c.updated_at >= (SELECT lo FROM floor)
                    AND (e LIKE 'cve:%' OR e LIKE 'url:%')
              ),
              -- Public clusters sharing a strong key with those — *regardless of the floor*, so an
              -- aged-out advisory still links (a strong CVE/URL edge ignores temporal distance).
              -- Intentionally NOT subscription-gated: this seed is not browsing an unsubscribed
              -- source, it enriches a story the subscriber already gets (via their own private
              -- incident) with the public advisory that incident strong-keys to — the product cross-
              -- source link. It only fires on a cve:/url: match to the subscriber own active private
              -- clusters, so it cannot pull in arbitrary public content.
              cross_boundary AS (
                  SELECT id, scope_kind, source, entities, first_event_time, last_event_time,
                         event_count, content_depth, max_severity
                  FROM cluster
                  WHERE scope_kind = 'public'
                    AND entities && (SELECT keys FROM private_strong)
                    AND (NOT $4 OR summary ->> 'band' IN ('confirmed', 'probable'))
              )
         SELECT * FROM in_floor
         UNION
         SELECT * FROM cross_boundary
         ORDER BY id",
    )
```

`persist_assignment` upserts each surviving story in place (preserving `created_at`/`last_delivered_at`, bumping `updated_at` only when a synthesis-relevant field actually changed) and tombstones each retro-merge loser (`merged_into = survivor`, `clusters = '[]'`) so a stale deep-link redirects:

```rust
// crates/core/src/link/store.rs:246-257
    for merge in &assignment.merges {
        sqlx::query(
            "UPDATE story
             SET merged_into = $2, clusters = '[]'::jsonb, updated_at = now()
             WHERE id = $1 AND subscriber_id = $3",
        )
        .bind(merge.loser)
        .bind(merge.survivor)
        .bind(subscriber_id)
        .execute(&mut *tx)
        .await?;
    }
```

## `identity` — tiered probabilistic entity resolution

The `identity` module resolves the graded, revisable layer on top of M3's exact namespaced tokens. A canonical identity is a connected component over equivalence edges **≥ θ**, carrying a `ConfidenceBand` derived from the *bottleneck of the maximum spanning forest* (so a redundant weak edge cannot downgrade an otherwise-certain merge), with stable id-forwarding so deep-links and feedback targets survive a recompute. `cannot_link` is the dual: a veto materialized in the same `entity_edge` graph as a negative-confidence row, so identity is reconstructible from the graph alone. Everything in `mod.rs` is pure and deterministic; the DB seam is `store.rs`.

### `identity/mod.rs`

**Purpose** — the resolver (`resolve`), the confidence-band vocabulary and edge-source confidences, and the lexical similarity measures (`lexical_similarity` / `dice_bigram`).

`resolve` filters edges to those `≥ θ` and not vetoed, sorts strongest-first (Kruskal max-spanning-forest with a deterministic tiebreak), unions greedily while recording each merge-creating edge as a spanning-forest edge, then takes each component's *bottleneck* (weakest spanning-forest edge) as its band and forwards the prior representative:

```rust
// crates/core/src/identity/mod.rs:233-304
pub fn resolve(
    nodes: &[CanonicalId],
    edges: &[Edge],
    vetoes: &BTreeSet<(CanonicalId, CanonicalId)>,
    theta: f32,
    prior: &PriorReps,
) -> Resolution {
    let mut uf = UnionFind::default();
    for n in nodes {
        uf.touch(n);
    }
    // Eligible edges, strongest first (Kruskal max-spanning-forest), with a deterministic tiebreak.
    let mut sorted: Vec<&Edge> = edges
        .iter()
        .filter(|e| e.confidence >= theta && !vetoes.contains(&pair(&e.a, &e.b)))
        .collect();
    sorted.sort_by(|x, y| {
        y.confidence
            .total_cmp(&x.confidence)
            .then_with(|| x.a.cmp(&y.a))
            .then_with(|| x.b.cmp(&y.b))
    });
    // Union the strongest first; an edge that *creates* a merge is a spanning-forest edge — record it
    // so we can take each component's bottleneck (min spanning-forest-edge confidence).
    let mut forest: Vec<(usize, f32)> = Vec::new(); // (one endpoint's index, confidence)
    for e in &sorted {
        let ia = uf.touch(&e.a);
        uf.touch(&e.b);
        if uf.union(&e.a, &e.b) {
            forest.push((ia, e.confidence));
        }
    }

    // Group members by root; pick the stable representative.
    let mut by_root: BTreeMap<usize, Vec<CanonicalId>> = BTreeMap::new();
    for (id, &idx) in &uf.index {
        by_root
            .entry(uf.find_const(idx))
            .or_default()
            .push(id.clone());
    }
    // Bottleneck (weakest spanning-forest edge) per final root.
    let mut bottleneck: BTreeMap<usize, f32> = BTreeMap::new();
    for (idx, conf) in &forest {
        let root = uf.find_const(*idx);
        bottleneck
            .entry(root)
            .and_modify(|w| *w = w.min(*conf))
            .or_insert(*conf);
    }

    let mut rep = BTreeMap::new();
    let mut band = BTreeMap::new();
    for (root, mut members) in by_root {
        members.sort();
        let representative = members
            .iter()
            .find(|m| prior.contains(*m))
            .cloned()
            .unwrap_or_else(|| members[0].clone());
        let component_band = bottleneck
            .get(&root)
            .map_or(ConfidenceBand::Confirmed, |&w| {
                ConfidenceBand::from_score(w)
            });
        for m in members {
            rep.insert(m, representative.clone());
        }
        band.insert(representative, component_band);
    }
    Resolution { rep, band }
}
```

Lexical similarity is symmetric and bounded in `[0,1]`: token-set Jaccard for multi-word values, character-bigram Sørensen–Dice for single tokens:

```rust
// crates/core/src/identity/mod.rs:309-359
pub fn lexical_similarity(a: &str, b: &str) -> f32 {
    if a == b {
        return 1.0;
    }
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let ta: BTreeSet<&str> = a
        .split([' ', '-', '_', '/'])
        .filter(|s| !s.is_empty())
        .collect();
    let tb: BTreeSet<&str> = b
        .split([' ', '-', '_', '/'])
        .filter(|s| !s.is_empty())
        .collect();
    if ta.len() > 1 || tb.len() > 1 {
        let inter = ta.intersection(&tb).count();
        let union = ta.union(&tb).count();
        return if union == 0 {
            0.0
        } else {
            inter as f32 / union as f32
        };
    }
    dice_bigram(a, b)
}

/// Sørensen–Dice over character bigrams — a forgiving single-token similarity.
fn dice_bigram(a: &str, b: &str) -> f32 {
    let bigrams = |s: &str| -> Vec<[char; 2]> {
        let chars: Vec<char> = s.chars().collect();
        chars.windows(2).map(|w| [w[0], w[1]]).collect()
    };
    let (ba, bb) = (bigrams(a), bigrams(b));
    if ba.is_empty() || bb.is_empty() {
        return 0.0;
    }
    let mut counts: HashMap<[char; 2], i32> = HashMap::new();
    for g in &ba {
        *counts.entry(*g).or_default() += 1;
    }
    let mut shared = 0usize;
    for g in &bb {
        let c = counts.entry(*g).or_default();
        if *c > 0 {
            *c -= 1;
            shared += 1;
        }
    }
    2.0 * shared as f32 / (ba.len() + bb.len()) as f32
}
```

The `ConfidenceBand` cutoffs and `EdgeSource` default confidences are the tuning surface the band derivation reads (feedback/exact = 1.0, normalized = 0.99, embedding = 0.85, lexical carries its own measured score clamped to the lexical band):

```rust
// crates/core/src/identity/mod.rs:40-54
impl ConfidenceBand {
    /// Band cutoffs (a tuning surface, design §10): ≥ 0.99 authoritative/normalized ⇒ `Confirmed`;
    /// ≥ 0.75 ⇒ `Probable`; else `Uncertain`.
    pub const CONFIRMED_FLOOR: f32 = 0.99;
    pub const PROBABLE_FLOOR: f32 = 0.75;

    pub fn from_score(score: f32) -> Self {
        if score >= Self::CONFIRMED_FLOOR {
            ConfidenceBand::Confirmed
        } else if score >= Self::PROBABLE_FLOOR {
            ConfidenceBand::Probable
        } else {
            ConfidenceBand::Uncertain
        }
    }
```

### `identity/store.rs`

**Purpose** — the persistence seam for the `entity_edge` identity graph: load positive edges (`load_edges`) and veto pairs (`load_vetoes`) visible to a subscriber, and upsert `must_link` / veto rows (`upsert`, `upsert_must_link`, `upsert_veto`). The graph holds both directions of the feedback channel as durable rows so identity is a function of the graph, not a replayed log.

The two loaders split the graph on the sign of `confidence` — positive rows are equivalence edges, negative rows are vetoes — both scoped `public ∪ own-private`:

```rust
// crates/core/src/identity/store.rs:17-54
pub async fn load_edges(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
) -> Result<Vec<Edge>, sqlx::Error> {
    sqlx::query(
        "SELECT a, b, confidence, source FROM entity_edge
         WHERE (scope_kind = 'public' OR scope_subscriber_id = $1) AND confidence >= 0",
    )
    .bind(subscriber_id)
    .try_map(|row: PgRow| {
        let source = EdgeSource::parse(&row.get::<String, _>("source"))
            .ok_or_else(|| sqlx::Error::Decode("unknown edge source".into()))?;
        Ok(Edge {
            a: row.get("a"),
            b: row.get("b"),
            confidence: row.get("confidence"),
            source,
        })
    })
    .fetch_all(executor)
    .await
}

/// Load the `cannot_link` veto pairs visible to `subscriber_id` (confidence < 0) — the pairs the
/// resolver must never merge. Order-insensitive `(a, b)` as stored.
pub async fn load_vetoes(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    sqlx::query(
        "SELECT a, b FROM entity_edge
         WHERE (scope_kind = 'public' OR scope_subscriber_id = $1) AND confidence < 0",
    )
    .bind(subscriber_id)
    .try_map(|row: PgRow| Ok((row.get("a"), row.get("b"))))
    .fetch_all(executor)
    .await
}
```

The shared `upsert` keys on `(scope, a, b)` so a positive edge and a veto collide on the same key and the latest assertion wins; `upsert_must_link` writes confidence 1.0 and `upsert_veto` writes -1.0:

```rust
// crates/core/src/identity/store.rs:60-104
async fn upsert(
    executor: impl PgExecutor<'_>,
    scope: &Scope,
    a: &str,
    b: &str,
    confidence: f32,
    source: EdgeSource,
) -> Result<(), sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = scope.to_columns();
    sqlx::query(
        "INSERT INTO entity_edge (scope_kind, scope_subscriber_id, a, b, confidence, source)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (scope_kind, scope_subscriber_id, a, b) DO UPDATE SET
            confidence = EXCLUDED.confidence, source = EXCLUDED.source",
    )
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(a)
    .bind(b)
    .bind(confidence)
    .bind(source.as_str())
    .execute(executor)
    .await?;
    Ok(())
}

/// Assert a positive equivalence edge (a `must_link`): confidence 1.0, source `feedback`.
pub async fn upsert_must_link(
    executor: impl PgExecutor<'_>,
    scope: &Scope,
    edge: &Edge,
) -> Result<(), sqlx::Error> {
    upsert(executor, scope, &edge.a, &edge.b, 1.0, edge.source).await
}

/// Materialize a `cannot_link` veto: a durable negative-confidence row the resolver consults, so the
/// veto survives an `entity_edge` rebuild (identity is a function of the graph, not a replayed log).
pub async fn upsert_veto(
    executor: impl PgExecutor<'_>,
    scope: &Scope,
    a: &str,
    b: &str,
) -> Result<(), sqlx::Error> {
    upsert(executor, scope, a, b, -1.0, EdgeSource::Feedback).await
}
```


---

## `thread` — the cross-time weave

The `thread` module is `bulletin-core`'s persistent memory: durable, recomputable, per-subscriber state that runs the full height of time (an Acme migration over months; an on-call rotation). The module splits the *pure* maintenance algorithms (in `mod.rs`, DB-free and deterministic so they can be proptested in isolation) from the *DB-bound orchestration* (`maintain.rs`) and the *persistence seam* (`store.rs`). The design contract mirrors `public-build`'s "fall behind, never wrong": maintenance is best-effort, due-gated, off the punctual send path, and never blocks a fire. Note that the compile-time `thread-weighting` kill switch that takes the whole consumption path in or out lives in `digest/mod.rs` (outside this section's files); the maintenance job here always runs and always writes the projected `entity_weight` map — the feature only gates whether the fire-time relevance term reads it.

### `mod.rs`

**Purpose:** the pure, deterministic maintenance algorithms plus the lifecycle types — `ThreadOrigin`, `ThreadState`/`Horizons`, the co-occurrence graph, label propagation, id-forwarding, affinity decay, and weight projection.

`ThreadOrigin` distinguishes user-*declared* threads (pinned by policy: never auto-merged or auto-archived) from *emergent* ones detected by community detection. `ThreadState` is the `Active → Dormant → Archived` lifecycle, whose transition is a pure function of the last story time, the horizons, and whether the thread is pinned.

```rust
// crates/core/src/thread/mod.rs:36-60
/// Whether a thread was explicitly declared by the user or emerged from community detection.
/// Declared threads are pinned-by-policy: never auto-merged or auto-archived.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThreadOrigin {
    Declared,
    Emergent,
}

impl ThreadOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            ThreadOrigin::Declared => "declared",
            ThreadOrigin::Emergent => "emergent",
        }
    }

    /// Inverse of [`as_str`]; anything but the explicit `declared` reads back as `Emergent` (the
    /// store column is constrained to the two values).
    pub fn parse(s: &str) -> Self {
        match s {
            "declared" => ThreadOrigin::Declared,
            _ => ThreadOrigin::Emergent,
        }
    }
}
```

The state machine goes `dormant` past the dormancy horizon and `archived` past the archive horizon — unless `pinned`, which caps a declared thread at `Dormant` so it stays in projection; a fresh story reactivates. `Horizons` bundles both cutoffs plus the affinity decay half-life as tuning knobs.

```rust
// crates/core/src/thread/mod.rs:90-133
    /// The state machine (design §5.1 step 6): a thread with no story since `dormancy_horizon` goes
    /// `dormant`; past `archive_horizon` and not pinned it goes `archived`. `pinned` (declared)
    /// threads never auto-archive — they hold `active`/`dormant` but stay in projection. A fresh
    /// story (so `last_story_time` is recent) reactivates a dormant/archived thread to `active`.
    pub fn transition(
        last_story_time: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
        pinned: bool,
        horizons: &Horizons,
    ) -> ThreadState {
        let Some(last) = last_story_time else {
            // No story ever — a freshly declared/seeded thread is active.
            return ThreadState::Active;
        };
        let age = now.signed_duration_since(last);
        if age < horizons.dormancy {
            ThreadState::Active
        } else if pinned || age < horizons.archive {
            ThreadState::Dormant
        } else {
            ThreadState::Archived
        }
    }
}

/// Dormancy / archive horizons + the affinity decay half-life — all tuning knobs (design §10), held
/// as a struct so a config table can supply them later without touching call sites.
#[derive(Debug, Clone, Copy)]
pub struct Horizons {
    pub dormancy: Duration,
    pub archive: Duration,
    /// Half-life of the per-thread affinity decay.
    pub affinity_half_life: Duration,
}

impl Default for Horizons {
    fn default() -> Self {
        Horizons {
            dormancy: Duration::days(21),
            archive: Duration::days(90),
            affinity_half_life: Duration::days(30),
        }
    }
}
```

`co_occurrence` builds a weighted, undirected graph over canonical entities: each item contributes a clique over its distinct entities, every edge weighted by `frequency × recency`, where recency is an exponential half-life decay of the item's age relative to `now`. Edge keys are canonically ordered `(min, max)` and self-edges are never stored, so the graph is symmetric by construction.

```rust
// crates/core/src/thread/mod.rs:177-203
pub fn co_occurrence(
    items: &[CoOccurrenceItem],
    now: DateTime<Utc>,
    half_life: Duration,
) -> CoGraph {
    let hl_secs = half_life.num_seconds().max(1) as f64;
    let mut g = CoGraph::default();
    for item in items {
        // Distinct, sorted entities so a duplicated entity in one item can't double-count, and the
        // clique is built deterministically.
        let ents: Vec<&CanonicalId> = {
            let set: BTreeSet<&CanonicalId> = item.entities.iter().collect();
            set.into_iter().collect()
        };
        let age_secs = now.signed_duration_since(item.at).num_seconds().max(0) as f64;
        let recency = 0.5_f64.powf(age_secs / hl_secs) as f32;
        for e in &ents {
            g.add_node(e);
        }
        for i in 0..ents.len() {
            for j in (i + 1)..ents.len() {
                g.add_edge(ents[i], ents[j], recency);
            }
        }
    }
    g
}
```

`label_propagation` is a deterministic near-linear community detector. Each node starts in its own community; each pass, in fixed ascending node order, a node adopts the highest summed-weight community among its neighbors *plus a small self-weight stabilizer* (so a node whose own community isn't represented among its neighbors doesn't thrash), with ties broken by smallest label. The adjacency list is built once and reused across passes, so the run is `O((V+E)·iters)`.

```rust
// crates/core/src/thread/mod.rs:215-266
pub fn label_propagation(graph: &CoGraph, max_iters: usize) -> BTreeMap<CanonicalId, CanonicalId> {
    let mut label: BTreeMap<CanonicalId, CanonicalId> =
        graph.nodes.iter().map(|n| (n.clone(), n.clone())).collect();

    // Adjacency list, built once: node → [(neighbor, weight)]. Every edge endpoint is also seeded
    // into `label` (an edge may name a node not in `graph.nodes`).
    let mut adj: BTreeMap<CanonicalId, Vec<(CanonicalId, f32)>> = BTreeMap::new();
    for ((a, b), &w) in &graph.edges {
        label.entry(a.clone()).or_insert_with(|| a.clone());
        label.entry(b.clone()).or_insert_with(|| b.clone());
        adj.entry(a.clone()).or_default().push((b.clone(), w));
        adj.entry(b.clone()).or_default().push((a.clone(), w));
    }

    // A self-edge weight that lets a node hold its own label against weakly-tied neighbors. Small
    // relative to a real co-occurrence edge, so it only breaks otherwise-ties.
    const SELF_WEIGHT: f32 = 1e-3;

    let ordered: Vec<CanonicalId> = label.keys().cloned().collect();
    for _ in 0..max_iters {
        let mut changed = false;
        for n in &ordered {
            let Some(neighbors) = adj.get(n) else {
                continue; // isolated node keeps its own label
            };
            // Sum edge weight per candidate label, seeded with the node's own label (the stabilizer).
            let mut score: BTreeMap<CanonicalId, f32> = BTreeMap::new();
            *score.entry(label[n].clone()).or_insert(0.0) += SELF_WEIGHT;
            for (nb, w) in neighbors {
                *score.entry(label[nb].clone()).or_insert(0.0) += w;
            }
            // Pick max weight; tie-break by smallest label (BTreeMap iterates ascending).
            let best = score
                .iter()
                .fold(None::<(&CanonicalId, f32)>, |acc, (lbl, &w)| match acc {
                    Some((_, bw)) if bw >= w => acc,
                    _ => Some((lbl, w)),
                })
                .map(|(lbl, _)| lbl.clone());
            if let Some(best) = best {
                if label[n] != best {
                    label.insert(n.clone(), best);
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    label
}
```

`map_communities_to_threads` gives **stable id-forwarding**: a candidate community is matched to prior threads by entity-spine Jaccard overlap ≥ `match_threshold`; a single match keeps its id, several matches merge with the **oldest id winning** (UUIDv7 is time-ordered, so smallest id is oldest), and the rest are reported as `merged` for `merged_into` forwarding. Pinned threads may match but are never listed among the merged losers.

```rust
// crates/core/src/thread/mod.rs:330-384
pub fn map_communities_to_threads(
    candidates: &[CandidateThread],
    existing: &[ExistingThread],
    match_threshold: f32,
) -> Vec<ThreadMapping> {
    let mut mappings = Vec::with_capacity(candidates.len());
    let mut claimed: BTreeSet<Uuid> = BTreeSet::new();

    for cand in candidates {
        let cand_set: BTreeSet<&CanonicalId> = cand.entities.iter().collect();
        // All existing threads with sufficient overlap, not already claimed by another candidate.
        let mut matches: Vec<&ExistingThread> = existing
            .iter()
            .filter(|e| !claimed.contains(&e.id))
            .filter(|e| jaccard(&cand_set, &e.entities) >= match_threshold)
            .collect();
        // Oldest id first (UUIDv7 time-ordered), so the winner is deterministic and stable.
        matches.sort_by_key(|e| e.id);

        let mapping = match matches.as_slice() {
            [] => ThreadMapping::New {
                entities: cand.entities.clone(),
            },
            [only] => {
                claimed.insert(only.id);
                ThreadMapping::Keep {
                    id: only.id,
                    entities: cand.entities.clone(),
                }
            }
            many => {
                // Oldest non-pinned wins if any are pinned we still keep the oldest overall as winner
                // but never list a pinned thread among the merged losers (it stays independent).
                let winner = many[0].id;
                let merged: Vec<Uuid> = many[1..]
                    .iter()
                    .filter(|e| !e.pinned)
                    .map(|e| e.id)
                    .collect();
                for e in many {
                    if e.id == winner || !e.pinned {
                        claimed.insert(e.id);
                    }
                }
                ThreadMapping::Merge {
                    winner,
                    merged,
                    entities: cand.entities.clone(),
                }
            }
        };
        mappings.push(mapping);
    }
    mappings
}
```

`decay_affinity` decays the prior affinity toward zero by an exponential half-life over the elapsed time, then adds the period's fresh signal, clamped to `[0, max]` — the fix for "cared in Q1, weighted forever." `project_weights` distributes each thread's affinity evenly across its entities (active at full rate, dormant at 0.25, archived excluded), accumulating shared entities, producing the fire-time `entity_weight` map.

```rust
// crates/core/src/thread/mod.rs:403-441
pub fn decay_affinity(
    prior: f32,
    elapsed: Duration,
    half_life: Duration,
    delta: f32,
    max: f32,
) -> f32 {
    let hl = half_life.num_seconds().max(1) as f64;
    let e = elapsed.num_seconds().max(0) as f64;
    let decayed = prior as f64 * 0.5_f64.powf(e / hl);
    ((decayed as f32) + delta).clamp(0.0, max)
}

// … project_weights: distribute active/dormant thread affinity across entities …
pub fn project_weights(
    threads: &[(ThreadState, f32, Vec<CanonicalId>)],
) -> BTreeMap<CanonicalId, f32> {
    let mut weights: BTreeMap<CanonicalId, f32> = BTreeMap::new();
    for (state, affinity, entities) in threads {
        let factor = match state {
            ThreadState::Active => 1.0,
            ThreadState::Dormant => 0.25,
            ThreadState::Archived => continue,
        };
        if entities.is_empty() || *affinity <= 0.0 {
            continue;
        }
        let share = affinity * factor / entities.len() as f32;
        for e in entities {
            *weights.entry(e.clone()).or_insert(0.0) += share;
        }
    }
    weights
}
```

### `maintain.rs`

**Purpose:** `thread_maintenance(subscriber)` — the DB-bound orchestration that resolves identity, builds the co-occurrence graph, detects and id-forwards communities, decays affinity, runs the state machine, and projects the per-entity weight map, all in one subscriber-scoped transaction.

The whole pass runs inside a single `Subscriber`-scoped transaction so every read/write is RLS-fenced and the writes commit atomically; a failure rolls back, leaving the prior thread state intact. A key input step is the **thread-spine filter**: `domain:`/`url:` entities are dropped from both the new sources *and* the prior threads' spines, so threads form around what a life is *about* rather than where it was published, and id-forwarding compares filtered candidates against filtered existing spines (self-healing legacy rows without a data migration).

```rust
// crates/core/src/thread/maintain.rs:82-133
pub async fn maintain(
    pool: &PgPool,
    subscriber_id: Uuid,
    now: DateTime<Utc>,
    cfg: &MaintenanceConfig,
) -> Result<MaintenanceStats> {
    let cfg = *cfg;
    let window_start = now - cfg.window;
    // The whole pass is one Subscriber-scoped transaction: every read/write is RLS-fenced to this
    // subscriber (own threads/edges/feedback/watermark; public ∪ own clusters/stories) and the writes
    // commit atomically. Best-effort — a failure rolls back, leaving the prior thread state intact.
    with_scope(pool, ScopeCtx::Subscriber(subscriber_id), move |conn| {
        Box::pin(async move {
            let since = store::feedback_cursor(&mut *conn, subscriber_id)
                .await
                .context("read maintenance watermark")?;

            // ── inputs ────────────────────────────────────────────────────────────
            let mut sources = store::co_occurrence_sources(&mut *conn, subscriber_id, window_start)
                .await
                .context("load co-occurrence sources")?;
            // Drop `domain:`/`url:` from the thread spine. Every item from one feed shares its
            // `domain:`, so co-occurring on it buckets a "life" by *publisher* — a thread spined on
            // `domain:tagesschau.de` is just "everything from tagesschau". `url:` is per-article and
            // never co-occurs across stories, so it only adds noise. Both stay on the cluster for
            // story *linking* (entity.rs); here, where we build the engaged co-occurrence graph, we
            // want only the distinctive entities a life is actually *about*.
            for s in &mut sources {
                s.entities.retain(is_thread_spine_entity);
            }
            let edges = identity::store::load_edges(&mut *conn, subscriber_id)
                .await
                .context("load identity edges")?;
            let veto_pairs = identity::store::load_vetoes(&mut *conn, subscriber_id)
                .await
                .context("load identity vetoes")?;
            let care = feedback::thread_care_since(&mut *conn, subscriber_id, since)
                .await
                .context("load thread care feedback")?;
            let mut prior = store::load_threads(&mut *conn, subscriber_id)
                .await
                .context("load prior threads")?;
            // Apply the same spine rule to the *prior* side. Threads persisted before this rule landed
            // may still carry `domain:`/`url:` in their stored spine; left unfiltered, id-forwarding
            // would compare a filtered candidate against an unfiltered existing spine — diluting the
            // Jaccard overlap below `match_threshold` and spuriously minting a duplicate thread (or
            // wrongly decaying a legacy publisher-spined one). Filtering here keeps both sides on the
            // same basis, and re-persists the cleaned spine on this pass (self-healing, no data migration).
            for t in &mut prior {
                t.entities.retain(is_thread_spine_entity);
            }
```

The middle of `maintain` resolves identity (durable feedback edges plus freshly-derived lexical edges, honouring `cannot_link` vetoes), remaps sources through component reps, builds the graph, and id-forwards communities onto the prior set. It then folds per-thread care deltas, upserts every mapped thread, and — critically — carries forward any prior thread no mapping claimed through the same `build_upsert` so it re-scores rather than silently decaying:

```rust
// crates/core/src/thread/maintain.rs:220-268
            // Carry-forward: a prior thread no mapping claimed still gets a full re-score through the same
            // `build_upsert` — so if engaged stories still overlap its spine it stays active (its engagement
            // and last_story_time are recomputed), and only a genuinely quiet thread decays toward archived.
            for t in &prior {
                if claimed.contains(&t.id) {
                    continue;
                }
                let care_delta = care_by_thread.get(&t.id).copied().unwrap_or(0.0);
                upserts.push(build_upsert(
                    Some(t.id),
                    Vec::new(),
                    &t.entities,
                    Some(t),
                    &sources,
                    &resolution,
                    care_delta,
                    since,
                    now,
                    &cfg,
                ));
            }

            // ── persist + project weights ───────────────────────────────────────────
            store::save_threads(&mut *conn, subscriber_id, &upserts)
                .await
                .context("save threads")?;
            let projection: Vec<(ThreadState, f32, Vec<CanonicalId>)> = upserts
                .iter()
                .map(|u| (u.state, u.affinity, u.entities.clone()))
                .collect();
            let weights = project_weights(&projection);
            store::save_entity_weights(&mut *conn, subscriber_id, &weights)
                .await
                .context("save entity weights")?;
            store::advance_watermark(&mut *conn, subscriber_id, now)
                .await
                .context("advance maintenance watermark")?;

            Ok(MaintenanceStats {
                sources: sources.len(),
                entities: node_vec.len(),
                communities: candidates.len(),
                threads_written: upserts.len(),
                weighted_entities: weights.len(),
            })
        })
    })
    .await
}
```

`is_thread_spine_entity` is the tiny, load-bearing predicate the spine filter uses on both sides:

```rust
// crates/core/src/thread/maintain.rs:270-276
/// Whether an entity belongs on a thread's co-occurrence spine. `domain:` buckets a life by
/// publisher (every item from a feed shares it) and `url:` is per-article noise that never
/// co-occurs across stories — both are excluded so threads form around what a life is *about*
/// (people, orgs, places, CVEs, repos), not where it was published. Everything else is eligible.
fn is_thread_spine_entity(entity: &CanonicalId) -> bool {
    !entity.starts_with("domain:") && !entity.starts_with("url:")
}
```

`build_upsert` computes one thread's persisted state in a single place (used for matched, new, and carried-forward threads): re-score affinity (decay prior + care + engagement), recompute window metrics, take the **weakest identity confidence band** across the spine, and run the state machine.

```rust
// crates/core/src/thread/maintain.rs:347-370
    let elapsed = (now - since).max(Duration::zero());
    let delta = care_delta + cfg.engagement_weight * new_stories as f32;
    let affinity = decay_affinity(
        prior.map_or(0.0, |p| p.affinity),
        elapsed,
        cfg.horizons.affinity_half_life,
        delta,
        cfg.affinity_max,
    );
    let pinned = prior.is_some_and(|p| p.pinned);
    let origin = prior.map_or(ThreadOrigin::Emergent, |p| p.origin);
    let state = ThreadState::transition(last_story_time, now, pinned, &cfg.horizons);
    let window_days = (cfg.window.num_seconds() as f32 / 86_400.0).max(1.0);
    // Identity confidence: the weakest band among the spine's entities (the band reaches rendering as
    // "possibly part of …"). `Confirmed` for an empty spine or entities unseen this pass.
    let confidence = entities
        .iter()
        .map(|e| resolution.band_of(e))
        .max_by_key(|b| match b {
            ConfidenceBand::Confirmed => 0,
            ConfidenceBand::Probable => 1,
            ConfidenceBand::Uncertain => 2,
        })
        .unwrap_or(ConfidenceBand::Confirmed);
```

### `store.rs`

**Purpose:** the persistence seam — read co-occurrence sources and prior threads, write thread rows (with `merged_into` forwarding) and the projected weight map, advance the watermark, and assign threads to stories at fire time.

`assign_thread` finds the best thread for a freshly-selected story at fire time: among active/dormant threads sharing ≥ `min_overlap` entities, it maximizes `overlap × affinity` (ties → highest affinity, then oldest id). The GIN `entities && $2` predicate narrows to the few candidate threads.

```rust
// crates/core/src/thread/store.rs:295-326
pub async fn assign_thread(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    entities: &[CanonicalId],
    min_overlap: i64,
) -> Result<Option<Uuid>, sqlx::Error> {
    if entities.is_empty() {
        return Ok(None);
    }
    let row = sqlx::query(
        "SELECT id FROM (
            SELECT t.id,
                   (SELECT count(*) FROM unnest(t.entities) AS e
                     WHERE e = ANY($2)) AS overlap,
                   t.affinity
            FROM thread t
            WHERE t.subscriber_id = $1
              AND t.merged_into IS NULL
              AND t.state <> 'archived'
              AND t.entities && $2
         ) cand
         WHERE overlap >= $3
         ORDER BY overlap::real * affinity DESC, affinity DESC, id
         LIMIT 1",
    )
    .bind(subscriber_id)
    .bind(entities)
    .bind(min_overlap)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|row| row.get("id")))
}
```

`save_entity_weights` is the sole writer of the subscriber's projected `entity_weight` map — serialized to the `subscriber.affinity` jsonb column, the fire-time relevance input:

```rust
// crates/core/src/thread/store.rs:210-222
pub async fn save_entity_weights(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    weights: &BTreeMap<CanonicalId, f32>,
) -> Result<(), sqlx::Error> {
    let json = serde_json::to_value(weights).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
    sqlx::query("UPDATE subscriber SET affinity = $2 WHERE id = $1")
        .bind(subscriber_id)
        .bind(json)
        .execute(executor)
        .await?;
    Ok(())
}
```

`due_for_maintenance` is the due-gate: subscribers whose last run is older than `cadence` (or who never ran), enumerated in the `Admin` control-plane context so the tick enqueues only the small due set rather than scanning every subscriber every minute.

```rust
// crates/core/src/thread/store.rs:269-289
pub async fn due_for_maintenance(
    pool: &PgPool,
    cadence: chrono::Duration,
) -> Result<Vec<Uuid>, sqlx::Error> {
    let cadence_secs = cadence.num_seconds().max(1);
    // Enumerates subscribers + their watermarks across owners → the admin (control-plane) context,
    // like `due_subscribers`. Self-scoped so the tick needs no scope ceremony.
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let due = sqlx::query(
        "SELECT s.id
         FROM subscriber s
         LEFT JOIN thread_maintenance_watermark w ON w.subscriber_id = s.id
         WHERE coalesce(w.ran_at, 'epoch'::timestamptz) <= now() - make_interval(secs => $1)",
    )
    .bind(cadence_secs as f64)
    .try_map(|row: PgRow| Ok(row.get("id")))
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(due)
}
```

## `enrich` — Phase-2 LLM entity mining

Phase 2 is a best-effort LLM sweep that runs *before* clustering: for each new item it asks a constrained local LLM for the real-world entities the item is about (`place:`/`org:`/`person:`/`topic:`), validates each against the source text (the grounding gate), and unions the surviving tokens onto the event's `entities` before it becomes cluster-eligible. So three outlets covering the same happening — which today share only a per-publisher `domain:` — come to share grounded tags and fuse into one story on one thread, threaded around what the story is ABOUT. The contract is "never block, fall behind never wrong": a failed/disabled/timed-out call leaves the item fully usable, and the build's grace deadline ages an un-enriched event in regardless.

### `mod.rs`

**Purpose:** the pure core (prompt/schema, the `Enrichment` shape, and the deterministic grounding+slugging gate) plus the DB-bound `sweep_public` orchestration.

The **grounding gate** (`ground_entities`) is non-negotiable — the LLM hallucination surface. Every proposed value must appear as a whole-word run in the normalized title *or* body, checked against title and body as *separate* padded haystacks (so a phrase straddling the title-end/body-start seam can't falsely ground), then slugged to a canonical `kind:value` token. It is pure and deterministic; the result is sorted and de-duplicated.

```rust
// crates/core/src/enrich/mod.rs:97-142
pub fn ground_entities(title: &str, body: Option<&str>, e: &Enrichment) -> Vec<String> {
    // Title and body are kept as *separate* padded haystacks (each normalized + space-padded so a
    // needle " royal navy " matches only on whole-word boundaries). Checking them independently — not
    // a single title‖body concatenation — is deliberate: a value must appear contiguously within one
    // field, so a phrase straddling the title-end/body-start boundary ("…Royal" + "Navy…") can't
    // falsely ground. The grounding gate is non-negotiable, so it must not invent boundary matches.
    let mut haystacks: Vec<String> = vec![format!(" {} ", normalize_text(title))];
    if let Some(b) = body {
        if !b.is_empty() {
            haystacks.push(format!(" {} ", normalize_text(b)));
        }
    }

    let mut out: Vec<String> = Vec::new();
    for (namespace, values) in e.namespaced() {
        for value in values {
            if let Some(token) = grounded_token(namespace, value, &haystacks) {
                out.push(token);
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Validate one proposed `value` against the space-padded normalized `haystacks` (title, body) and, if
/// it appears as a whole-word run in *any one* of them, return its `kind:slug` token. `None` when the
/// value is empty after normalization or appears in none of the haystacks — the hallucination drop.
fn grounded_token(namespace: &str, value: &str, haystacks: &[String]) -> Option<String> {
    let normalized = normalize_text(value);
    let needle = strip_article(&normalized);
    if needle.is_empty() {
        return None;
    }
    // Whole-word containment within a single field: the value's words must appear, in order, on word
    // boundaries somewhere in one haystack.
    let padded = format!(" {needle} ");
    if !haystacks.iter().any(|h| h.contains(&padded)) {
        return None;
    }
    // `namespace` is a lowercase literal and `needle` is already normalized (lowercased, alnum-only),
    // so the token is canonical by construction — no `identity::canonicalize` pass is needed (it would
    // be an identity transform here), and `needle` non-empty ⇒ the slug is non-empty.
    Some(format!("{namespace}:{}", needle.replace(' ', "-")))
}
```

`ENRICH_SYSTEM_PROMPT` is a constant (so it prefix-caches) engineered for a 3–4B model — short, imperative, one job, closed instructions, a worked example. It states the grounding rule *and* the sweep enforces it deterministically afterward (defense in depth), and instructs the model to reason in `analysis` first, tag only story entities (ignoring credits/bylines/the outlet itself), and prefer the most specific entity.

```rust
// crates/core/src/enrich/mod.rs:180-205
pub const ENRICH_SYSTEM_PROMPT: &str = r#"You read one news or work item and tag the real-world entities it is about, then judge how big a deal it is. Think first, then tag.

Fill these fields:
- analysis: 1-2 short sentences naming what the item is about.
- impact: how significant the item is, one of:
    - "major": a breaking, large-scale, or grave development — war or its escalation, mass casualties, a disaster, a critical security advisory, a major release or ruling.
    - "significant": a serious development — a major policy change, a notable incident, an important market or organizational move.
    - "notable": worth knowing but not dominating — a debate, a routine announcement or release, a local incident.
    - "routine": minor or everyday — a market wrap, a soft-news or culture note, chatter, a small update.
  Judge the EVENT itself, not how dramatic the wording is. When unsure, choose the lower level.
- places: geographic places named in the text (countries, regions, cities, bodies of water).
- orgs: organizations, companies, agencies, or teams named in the text.
- people: specific named people in the text.
- topics: a few broad subject tags for what the item is about.

Rules:
- Use ONLY names that literally appear in the text. Never invent, expand, or infer an entity that is not written there. If unsure, leave it out.
- Tag only entities that are part of the STORY. Ignore photo credits, image captions, bylines, and the publishing outlet itself: a photo agency (picture alliance, dpa, Reuters, Getty, imago), a photographer's or reporter's name in a credit line, and the source's own brand are NOT what the item is about. Leave them out.
- Tag the most SPECIFIC entity, not a broad container. Prefer a city, agency, or company over a whole country, and a specific person over a generic group.
- Copy each place/org/person from the text as written (you may drop a leading "the").
- Keep each list short - the few most central. Use an empty list if none apply.
- Output only the JSON the schema asks for. No preamble.

EXAMPLE
text: Royal Navy warships fired warning shots after a standoff in the English Channel on Tuesday, the Ministry of Defence said. (Photo: Jane Doe / picture alliance)
out: {"analysis":"A naval standoff in the English Channel involving the Royal Navy and the Ministry of Defence.","impact":"significant","places":["English Channel"],"orgs":["Royal Navy","Ministry of Defence"],"people":[],"topics":["standoff"]}"#;
```

`enrichment_schema` shapes the answer for llama.cpp's GBNF token-masking — four capped string arrays plus the `analysis` scratchpad and an `impact` enum kept in lockstep with `salience::IMPACT_VOCAB`. It bounds the *shape*, never the *truth*; that is the grounding gate's job.

```rust
// crates/core/src/enrich/mod.rs:230-255
pub fn enrichment_schema() -> serde_json::Value {
    use serde_json::json;
    let value_list = json!({
        "type": "array",
        "maxItems": 8,
        "items": { "type": "string", "maxLength": 60 }
    });
    json!({
        "name": "enrichment",
        "strict": true,
        "schema": {
            "type": "object",
            "properties": {
                "analysis": { "type": "string", "maxLength": 400 },
                // Mirrors `salience::IMPACT_VOCAB` (asserted by `schema_impact_enum_matches_vocab`).
                "impact":   { "type": "string", "enum": ["routine", "notable", "significant", "major"] },
                "places":   value_list,
                "orgs":     value_list,
                "people":   value_list,
                "topics":   value_list
            },
            "required": ["analysis", "impact", "places", "orgs", "people", "topics"],
            "additionalProperties": false
        }
    })
}
```

`sweep_public` walks the pending frontier in one short scoped transaction, then processes each event in its **own short transaction committed right after its model call** — so a mid-sweep failure never rolls back earlier work and no transaction is held open across an LLM round-trip. A disabled sweep is a no-op; a per-event failure is counted and the event left to retry.

```rust
// crates/core/src/enrich/mod.rs:311-333
    let mut stats = EnrichStats::default();
    for ev in &events {
        match client::enrich_event(cfg, &http, ev).await {
            Some((tokens, salience)) => {
                let event_id = ev.id;
                let tokens_for_write = tokens.clone();
                with_scope(pool, ScopeCtx::NoSubscriber, move |conn| {
                    Box::pin(async move {
                        store::apply_enrichment(conn, event_id, &tokens_for_write, salience)
                            .await
                            .context("apply enrichment to event")
                    })
                })
                .await?;
                stats.enriched += 1;
                stats.entities_added += tokens.len();
            }
            None => stats.failed += 1,
        }
    }
    Ok(stats)
}
```

### `client.rs`

**Purpose:** the local-sidecar enrichment call — reuse the summarizer's grammar-constrained chat plumbing to extract entities, then hand the result to the grounding gate.

`enrich_event` is best-effort end-to-end: call the model, ground every proposed value against `Event::best_text` (the Phase-1 `full_text` when present, else the connector snippet — the same accessor the summarizer reads), classify salience to `0..=3`, and return `None` on any model failure. Crucially, a successful call that grounds *nothing* still returns `Some((vec![], score))` so the event is marked enriched and not retried forever.

```rust
// crates/core/src/enrich/client.rs:47-68
pub async fn enrich_event(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    event: &Event,
) -> Option<(Vec<String>, i16)> {
    let text = event.best_text();
    match call_enrichment(cfg, http, &event.title, text).await {
        Ok(extracted) => {
            let tokens = ground_entities(&event.title, text, &extracted);
            let salience = crate::common::salience::impact_score(&extracted.impact);
            Some((tokens, salience))
        }
        Err(e) => {
            tracing::debug!(
                error = %format!("{e:#}"),
                event_id = %event.id,
                "enrichment call failed; leaving event un-enriched for retry"
            );
            None
        }
    }
}
```

### `store.rs`

**Purpose:** the enrichment persistence seam — read the pending public frontier and union grounded tokens back onto an event before it clusters.

`pending_public_events` selects the frontier: public events from an *enrichable* (link-poor) source, not yet enriched and still ahead of the build watermark (so a write still feeds clustering), oldest-first and capped so a backlog drains over several sweeps. Excluding already-built events avoids re-enriching a rolled-up cluster; the `source` filter skips structurally-rich sources (GitHub/Slack) whose events already carry clean entities.

```rust
// crates/core/src/enrich/store.rs:28-47
pub async fn pending_public_events(
    executor: impl PgExecutor<'_>,
    limit: i64,
) -> Result<Vec<Event>, sqlx::Error> {
    sqlx::query(&format!(
        "SELECT {EVENT_COLUMNS}
         FROM event
         WHERE scope_kind = 'public'
           AND enriched_at IS NULL
           AND source = ANY($2)
           AND ingest_time > (SELECT built_through FROM build_watermark)
         ORDER BY ingest_time
         LIMIT $1"
    ))
    .bind(limit)
    .bind(SourceKind::enrichable_sources())
    .try_map(from_row)
    .fetch_all(executor)
    .await
}
```

`apply_enrichment` unions the new tokens onto `entities` (kept sorted + de-duplicated), writes salience as `GREATEST` of any existing `severity_hint` (never lowering a structural hint, and only when positive), and stamps `enriched_at = now()` even when nothing grounded (so a clean pass isn't retried forever). The whole write is idempotent.

```rust
// crates/core/src/enrich/store.rs:56-83
pub async fn apply_enrichment(
    executor: impl PgExecutor<'_>,
    event_id: Uuid,
    new_entities: &[String],
    salience: i16,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE event
         SET entities = ARRAY(
                 SELECT DISTINCT e
                 FROM unnest(entities || $2::text[]) AS e
                 ORDER BY e
             ),
             severity_hint = CASE
                 WHEN $3::smallint > 0
                     THEN GREATEST(COALESCE(severity_hint, 0), $3::smallint)
                 ELSE severity_hint
             END,
             enriched_at = now()
         WHERE id = $1",
    )
    .bind(event_id)
    .bind(new_entities)
    .bind(salience)
    .execute(executor)
    .await?;
    Ok(())
}
```

## `feedback` — append-only correction log

The `feedback` module is the append-only channel through which a user corrects relevance and aggregation. It is per-subscriber and revisable: a correction is logged, and the identity-graph effect of a must/cannot-link is **materialized into `entity_edge` in the same transaction** — so identity stays a function of the graph alone (reconstructible without replaying the log), and the correction takes effect on the next `thread_maintenance` recompute. Two families: entity-level `must_link`/`cannot_link` (act on the identity graph), and thread-level `care_more`/`care_less`/`done` (fold into a thread's affinity delta on the next pass).

### `feedback.rs`

**Purpose:** the whole module — the `Signal`/`TargetType` enums, the transactional `submit`, and the `thread_care_since` reader maintenance folds in.

`TargetType` and `Signal` model the correction. Each signal carries an `affinity_delta` — the nudge a thread-level signal contributes when maintenance folds it in (`done` is a strong care-less at -5.0); entity-link signals contribute none because they act on the graph, not affinity.

```rust
// crates/core/src/feedback.rs:23-75
/// What a feedback signal targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetType {
    Entity,
    Thread,
    Story,
}

impl TargetType {
    pub fn as_str(self) -> &'static str {
        match self {
            TargetType::Entity => "entity",
            TargetType::Thread => "thread",
            TargetType::Story => "story",
        }
    }
}

/// The correction itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    CareMore,
    CareLess,
    /// This thread is resolved — stop surfacing it (a strong care-less).
    Done,
    /// These two entities are the same (confirm an equivalence edge).
    MustLink,
    /// These two entities are *not* the same (veto the edge).
    CannotLink,
}

impl Signal {
    pub fn as_str(self) -> &'static str {
        match self {
            Signal::CareMore => "care_more",
            Signal::CareLess => "care_less",
            Signal::Done => "done",
            Signal::MustLink => "must_link",
            Signal::CannotLink => "cannot_link",
        }
    }

    /// The affinity nudge a thread-level signal contributes when maintenance folds it in. Entity-link
    /// signals contribute none (they act on the identity graph, not affinity).
    pub fn affinity_delta(self) -> f32 {
        match self {
            Signal::CareMore => 1.0,
            Signal::CareLess => -1.0,
            Signal::Done => -5.0,
            Signal::MustLink | Signal::CannotLink => 0.0,
        }
    }
}
```

`submit` opens one `Subscriber`-scoped transaction, canonicalizes both tokens exactly like the resolver, and — for an entity-level link — writes the positive equivalence edge (`must_link`) or the durable veto (`cannot_link`) into the identity store *before* appending the log row, so the log and graph can't disagree across a crash.

```rust
// crates/core/src/feedback.rs:82-130
pub async fn submit(
    pool: &PgPool,
    subscriber_id: Uuid,
    target_type: TargetType,
    target_id: &str,
    signal: Signal,
    other: Option<&str>,
) -> Result<()> {
    // Writes this subscriber's own rows (a private `entity_edge` + the `feedback` log) → its RLS
    // context, atomically.
    let mut tx = begin_scope(pool, ScopeCtx::Subscriber(subscriber_id)).await?;

    if matches!(target_type, TargetType::Entity) {
        let scope = Scope::Private(subscriber_id);
        let a = canonicalize(target_id);
        let b = other.map(canonicalize).filter(|b| !b.is_empty());
        if !a.is_empty() {
            match (signal, b) {
                (Signal::MustLink, Some(b)) => {
                    let edge = Edge::from_source(a, b, EdgeSource::Feedback);
                    identity_store::upsert_must_link(&mut *tx, &scope, &edge).await?;
                }
                (Signal::CannotLink, Some(b)) => {
                    identity_store::upsert_veto(&mut *tx, &scope, &a, &b).await?;
                }
                _ => {}
            }
        }
    }

    let payload = match other {
        Some(o) => serde_json::json!({ "other": o }),
        None => serde_json::json!({}),
    };
    sqlx::query(
        "INSERT INTO feedback (subscriber_id, target_type, target_id, signal, payload)
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(subscriber_id)
    .bind(target_type.as_str())
    .bind(target_id)
    .bind(signal.as_str())
    .bind(payload)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}
```

`thread_care_since` reads thread-level care feedback newer than the maintenance watermark (so each correction folds in exactly once) and maps each signal to its affinity delta; entity-link signals are excluded because they already materialized into the graph at submit time. The caller sums by thread.

```rust
// crates/core/src/feedback.rs:141-167
pub async fn thread_care_since(
    executor: impl PgExecutor<'_>,
    subscriber_id: Uuid,
    since: DateTime<Utc>,
) -> Result<Vec<ThreadCare>, sqlx::Error> {
    sqlx::query(
        "SELECT target_id, signal
         FROM feedback
         WHERE subscriber_id = $1 AND target_type = 'thread'
           AND signal IN ('care_more','care_less','done')
           AND created_at > $2",
    )
    .bind(subscriber_id)
    .bind(since)
    .try_map(|row: PgRow| {
        let thread_id = Uuid::parse_str(&row.get::<String, _>("target_id"))
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;
        let delta = match row.get::<String, _>("signal").as_str() {
            "care_more" => Signal::CareMore.affinity_delta(),
            "care_less" => Signal::CareLess.affinity_delta(),
            _ => Signal::Done.affinity_delta(),
        };
        Ok(ThreadCare { thread_id, delta })
    })
    .fetch_all(executor)
    .await
}
```

## `subscription`

The `subscription` module owns the subscriber ↔ source (`connection`) relation — the explicit join the digest candidate scope filters on, so a public cluster enters a subscriber's digest only when they subscribe to the connection that produced it. Owning a connection implies a subscription; ownerless public sources (RSS) require an explicit subscribe. Both FKs are `ON DELETE CASCADE`, so deleting either side reclaims the row. Operations run in the `Admin` control-plane scope.

### `subscription/mod.rs`

**Purpose:** the whole module — idempotent subscribe/unsubscribe plus a stable listing.

`subscribe` inserts with `ON CONFLICT DO NOTHING` (the composite PK collapses a duplicate), so re-subscribing is a no-op; it returns `true` only when a row was actually created, and only that real change is counted/announced.

```rust
// crates/core/src/subscription/mod.rs:22-44
pub async fn subscribe(
    pool: &PgPool,
    subscriber_id: Uuid,
    connection_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let result = sqlx::query(
        "INSERT INTO subscription (subscriber_id, connection_id) VALUES ($1, $2)
         ON CONFLICT DO NOTHING",
    )
    .bind(subscriber_id)
    .bind(connection_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    let created = result.rows_affected() > 0;
    // Only count/announce a real change; a redundant re-subscribe is a no-op, not an edit.
    if created {
        metrics::counter!("bulletin_subscription_changes_total", "op" => "subscribe").increment(1);
    }
    tracing::info!(%subscriber_id, %connection_id, created, "subscribe");
    Ok(created)
}
```

`unsubscribe` is the symmetric delete — idempotent, returning `true` only when a row was removed; the connection's clusters simply stop entering this subscriber's candidate set on the next digest.

```rust
// crates/core/src/subscription/mod.rs:48-68
pub async fn unsubscribe(
    pool: &PgPool,
    subscriber_id: Uuid,
    connection_id: Uuid,
) -> Result<bool, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Admin).await?;
    let result =
        sqlx::query("DELETE FROM subscription WHERE subscriber_id = $1 AND connection_id = $2")
            .bind(subscriber_id)
            .bind(connection_id)
            .execute(&mut *tx)
            .await?;
    tx.commit().await?;
    let removed = result.rows_affected() > 0;
    if removed {
        metrics::counter!("bulletin_subscription_changes_total", "op" => "unsubscribe")
            .increment(1);
    }
    tracing::info!(%subscriber_id, %connection_id, removed, "unsubscribe");
    Ok(removed)
}
```


---

## `digest` — select, render, send

The `digest` module is the projection (read) side of the pipeline: it takes a freshness-scored lookback over the cluster cache, links candidates into stories, scores/selects them, freezes the selection, renders an email, and delivers — advancing the subscriber's schedule on delivery. It is a pure read of the materialization side's snapshot: there is no build gate; projection reads whatever has been built so far.

### `mod.rs`

**Purpose:** The digest flow spine — `generate` / `dispatch_now` / `explain`, the shared `link_and_select` core, the feature-gated thread-weighting hooks, and the on-path authored-lead retry/deferral.

The public outcome type encodes every terminal state a `generate` run can reach. The critical one is `LeadDeferred`: per the §3.7 contract a populated digest never ships without its LLM lead, so if the lead can't be composed the watermark is left unmoved and the worker errors so apalis retries the same (frozen, idempotent) window.

```rust
// crates/core/src/digest/mod.rs:51-71
#[derive(Debug)]
pub enum DigestOutcome {
    /// Delivered a digest with `items` entries (surfaced via `Debug` in logs / the debug CLI).
    Delivered {
        #[allow(dead_code)]
        items: usize,
    },
    /// Window had nothing to report; sent an "all caught up" note and advanced the watermark.
    Empty,
    /// Already delivered for this window (idempotent re-run).
    AlreadyDelivered,
    /// The boundary moved into the future between enqueue and run — a preference change deferred
    /// this send. Nothing delivered; the next tick fires it at the corrected boundary.
    NotYetDue,
    /// The digest had selected items but its **authored lead couldn't be composed** this run (the sidecar
    /// was down, or every re-seeded draw failed the gate). Per the §3.7 contract a digest with items never
    /// ships without an LLM lead, so nothing was delivered and the watermark did **not** advance: the
    /// worker errors so apalis retries this window. Whether a still-deferred lead warrants an operator
    /// alert is the worker's call (it owns the apalis attempt count and the retry budget).
    LeadDeferred,
}
```

`link_and_select` is the shared core of the scheduled digest, the ad-hoc dispatch, and `explain`, so all three link and rank identically. It reads candidate clusters (`public ∪ own-private`) in the subscriber's RLS context, links them into stories, maps each story to a `Candidate` with its entity spine + last-shown snapshot, pre-assigns threads and applies the thread relevance term (both feature-gated), snapshots the pre-`select` input for replay, then derives the display floor per the caller's `DisplayPolicy` and hands everything to the pure `select`.

```rust
// crates/core/src/digest/mod.rs:180-222
    let mut candidates: Vec<Candidate> = assignment
        .stories
        .iter()
        .map(|s| Candidate::from_story(s, story_entities[&s.id].clone(), shown.get(&s.id).copied()))
        .collect();
    // Assign each candidate its Thread *before* ranking, so the per-thread diversity cap can bound a
    // busy thread during selection (not just label the result after). Best-effort + feature-gated: on
    // error or with the feature off, every `thread_id` stays `None` and the cap is simply inert.
    assign_candidate_threads(pool, sub.id, &story_entities, &mut candidates).await;
    // Add the Thread relevance term before ranking (compiled out when the feature is off; a no-op
    // until thread_maintenance has projected weights) — it folds into the M4 relevance score.
    apply_weighting(pool, sub.id, &mut candidates).await?;
    // M4 scoring + selection (design §8.4): relevance gates, richness classifies Story/Note, priority
    // (relevance + severity, recency-decayed) orders + per-format caps, bounded by the subscriber's
    // overall `max_items`. `now` is read-time so the decay reflects when the digest fires; config is
    // the global `digest_config` row.
    let cfg = load_config(pool).await.context("load scoring config")?;
    // `.max(0)` guards the `i32 → usize` cast: a stray non-positive max_items yields an empty digest
    // (the safe direction), never a sign-wrapped, effectively-unbounded ceiling.
    let max_items = sub.max_items.max(0) as usize;
    // Capture the read-time clock once, and snapshot the candidate set *before* `select` consumes it,
    // so a delivered digest can be re-scored under a trial config later (the eval sweep, §0.1).
    let now = Utc::now();
    let snapshot = ReplaySnapshot {
        now,
        max_items,
        candidates: candidates.clone(),
    };
    // Display floor: the candidate set reaches `horizon_days` back for linking/threading context,
    // but the digest should only *surface* fresh items. Items older than the floor stay candidates
    // (so linking still uses them) but `select` excludes them with `StaleForCadence` — fixing stale
    // items filling slots when fresh content is thin (relevance ranking only decays with age, never
    // gates on it). How far back the floor reaches depends on the path (see [`DisplayPolicy`]):
    //  - scheduled/explain → the subscriber's cadence: reach back to the last delivery (so a delayed
    //    or outage-coalesced run surfaces every fresh event since the previous digest — the literal
    //    "digest window"), floored at one cadence + grace so a recent extra run can't collapse it;
    //  - off-schedule preview → the full lookback, so `--lookback N` shows the last N days as asked.
    let display_floor = match display {
        DisplayPolicy::Cadence => sub.recurrence.display_floor(now, last_run),
        DisplayPolicy::FullLookback => now - Duration::days(horizon_days as i64),
    };
    let decisions = select(candidates, &cfg, max_items, now, display_floor);
    Ok((assignment.stories, decisions, story_entities, snapshot))
```

Thread-weighting is a compile-time kill switch (`#[cfg(feature = "thread-weighting")]`) that takes the whole consumption path off the build; both the relevance-term hook and the candidate thread pre-assignment have inert `#[cfg(not(...))]` twins. `apply_weighting` loads the subscriber's projected entity weights and folds them into `Candidate.relevance`; `assign_candidate_threads` groups candidates by thread before ranking (best-effort — a DB error leaves every `thread_id` `None`, making the diversity cap inert).

```rust
// crates/core/src/digest/mod.rs:267-345
#[cfg(feature = "thread-weighting")]
async fn apply_weighting(
    pool: &PgPool,
    subscriber_id: Uuid,
    candidates: &mut [Candidate],
) -> Result<()> {
    // `subscriber.affinity` lives on the (RLS-fenced) subscriber row, so read it in the subscriber's
    // own context — the no-subscriber context is denied the control-plane tables outright.
    let weights = with_scope(pool, ScopeCtx::Subscriber(subscriber_id), move |conn| {
        Box::pin(async move {
            crate::thread::store::load_entity_weights(&mut *conn, subscriber_id)
                .await
                .context("load entity weights")
        })
    })
    .await?;
    crate::digest::select::apply_thread_weights(candidates, &weights);
    Ok(())
}

#[cfg(not(feature = "thread-weighting"))]
async fn apply_weighting(_: &PgPool, _: Uuid, _: &mut [Candidate]) -> Result<()> {
    Ok(())
}

/// Pre-assign each *candidate* its Thread before ranking, so [`select`]'s per-thread diversity cap has
/// a grouping key (design §8.4). Mirrors [`assign_threads`] (same `assign_thread` overlap query) but
/// runs over the full candidate set and writes onto `Candidate.thread_id` rather than `digest_item`.
/// **Best-effort**: any DB error leaves every `thread_id` `None` (the cap simply does nothing this
/// fire) — thread assignment is render/ranking metadata, never a reason to fail the punctual digest.
#[cfg(feature = "thread-weighting")]
async fn assign_candidate_threads(
    pool: &PgPool,
    subscriber_id: Uuid,
    story_entities: &HashMap<Uuid, Vec<String>>,
    candidates: &mut [Candidate],
) {
    /// Minimum shared entities for a story→thread assignment (mirrors [`assign_threads`]).
    const MIN_OVERLAP: i64 = 1;
    let ids: Vec<Uuid> = candidates.iter().map(|c| c.id).collect();
    let story_entities = story_entities.clone();
    let result = with_scope(pool, ScopeCtx::Subscriber(subscriber_id), move |conn| {
        Box::pin(async move {
            let mut map: HashMap<Uuid, Option<Uuid>> = HashMap::with_capacity(ids.len());
            for id in &ids {
                let entities = story_entities.get(id).cloned().unwrap_or_default();
                let thread_id = crate::thread::store::assign_thread(
                    &mut *conn,
                    subscriber_id,
                    &entities,
                    MIN_OVERLAP,
                )
                .await?;
                map.insert(*id, thread_id);
            }
            Ok(map)
        })
    })
    .await;
    match result {
        Ok(map) => {
            for c in candidates.iter_mut() {
                c.thread_id = map.get(&c.id).copied().flatten();
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "candidate thread pre-assignment failed (non-fatal); per-thread cap inert this fire");
        }
    }
}

#[cfg(not(feature = "thread-weighting"))]
async fn assign_candidate_threads(
    _: &PgPool,
    _: Uuid,
    _: &HashMap<Uuid, Vec<String>>,
    _: &mut [Candidate],
) {
}
```

The on-path lead is the one summarization model call on the punctual path. A gate rejection is deterministic, so `authored_lead` retries it in-process up to `LEAD_SEED_RETRIES` times with an escalating seed (offset by the apalis `job_attempt` so successive job retries also draw fresh leads); a down sidecar or a blown deadline is *not* re-tried here — re-seeding can't revive a dead box — so it returns `None` at once and the whole job retries later.

```rust
// crates/core/src/digest/mod.rs:475-479
/// How many times, within a single `generate` run, the authored lead is re-attempted with an escalated
/// seed past a deterministic gate rejection (§3.7) before the digest defers to a later job attempt. A
/// down sidecar is *not* re-tried in-process (re-seeding can't reach a dead box) — that returns `None`
/// immediately and the whole job retries later, where the box may be back.
const LEAD_SEED_RETRIES: i32 = 3;
```

```rust
// crates/core/src/digest/mod.rs:593-623
    let seed_base = job_attempt
        .saturating_sub(1)
        .saturating_mul(LEAD_SEED_RETRIES as u32) as i32;
    for i in 0..LEAD_SEED_RETRIES {
        let cfg = base.for_attempt(seed_base + i);
        match tokio::time::timeout(
            cfg.lead_deadline,
            summarize::client::authored_lead(&cfg, &http, &dominant, &also, &threads, total_items),
        )
        .await
        {
            Ok(LeadOutcome::Ready(lead)) => return Some(lead),
            // Deterministic voice/grounding miss — re-seed and try again within this run.
            Ok(LeadOutcome::Rejected) => continue,
            // A dead box won't be revived by re-seeding; defer the whole digest to a later job attempt.
            Ok(LeadOutcome::Unavailable) => return None,
            Err(_) => {
                tracing::debug!(
                    deadline_s = cfg.lead_deadline.as_secs(),
                    "digest lead exceeded its deadline; deferring digest"
                );
                return None;
            }
        }
    }
    tracing::debug!(
        retries = LEAD_SEED_RETRIES,
        "digest lead still rejected after re-seeding; deferring digest"
    );
    None
```

In `generate`, the digest+items are created in one transaction (idempotent freeze), inline Phase-C synthesis runs deadline-bounded, and the `let Some(lead) = digest_lead(...) else` arm short-circuits to `LeadDeferred` without sending or advancing the watermark when no lead could be authored.

```rust
// crates/core/src/digest/mod.rs:773-800
    let Some(lead) = digest_lead(pool, digest.id, sub.id, &items, job_attempt).await else {
        tracing::warn!(
            subscriber_id = %sub.id,
            %window_end,
            job_attempt,
            "digest lead unavailable; deferring delivery (no digest ships without an LLM lead)"
        );
        // Nothing delivered, watermark unmoved — the worker errors so apalis retries this window, and
        // decides (from the apalis attempt count + its own budget) when a still-deferred lead warrants
        // an operator alert. Keeping that threshold at the trigger layer keeps core free of retry policy.
        return Ok(DigestOutcome::LeadDeferred);
    };
    let message = render::render(
        mailer.from(),
        &sub.email,
        window_end,
        &sub.timezone,
        &items,
        &greeting,
        Some(lead.as_str()),
        content,
    )?;
    mailer.send(message).await?;
    mark_delivered(pool, digest.id, sub.id, snapshot_at)
        .await
        .context("mark delivered")?;

    Ok(DigestOutcome::Delivered { items: items.len() })
```

### `select.rs`

**Purpose:** The pure heart of scoring & selection — a function over precomputed features (story rollups + thread relevance term + a config row) implementing gate → classify richness → rank by priority → cap.

`ScoringConfig` is the tunable `digest_config` row lifted into a pure value, with `Default` mirroring the migration defaults so fixtures need no DB. The three decay half-lives (recency, salience, thread) are the key dials.

```rust
// crates/core/src/digest/select.rs:108-158
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ScoringConfig {
    /// Inclusion gate: a story is kept iff `relevance ≥ relevance_floor`.
    pub relevance_floor: f32,
    /// Relevance bonus when a story includes the subscriber's own private content.
    pub scope_bonus: f32,
    /// Priority boost per point of a story's `max_severity`.
    pub severity_weight: f32,
    /// Priority halves every this-many days of age (recency decay at read time).
    pub recency_half_life_days: f64,
    /// The **salience** (importance) term ages on its own cadence — slower than recency, faster than
    /// the thread term — so a high-`max_severity` story stays promoted a few days past its freshness.
    pub salience_half_life_days: f64,
    /// The **thread** relevance term ages on a slower cadence than recency — it halves every
    /// this-many days (typically ≫ `recency_half_life_days`), so a story you've invested a thread in
    /// stays promoted for weeks but still eventually fades (design §8.3 + §9.4).
    pub thread_half_life_days: f64,
    /// Max Stories rendered (design §8.4: ~3–5).
    pub story_cap: usize,
    /// Max Notes rendered (~15–25).
    pub note_cap: usize,
    /// Max stories from any one Thread per digest — within-topic diversity (design §8.4). Stories with
    /// no assigned thread are unconstrained; `0` disables the cap.
    pub thread_cap: usize,
    /// Priority multiplier for a story re-surfaced with no new events since it was last shown
    /// (design §9.4 re-surface suppression) — it fades to a "still developing" note and eventually
    /// out. `1.0` disables the penalty.
    pub resurface_penalty: f32,
    /// Max stale "still developing" re-surfaces rendered per digest — the hard cap on recycled-note
    /// padding the priority damping alone can't enforce. Fresh content isn't re-surfaced, so it's
    /// untouched by this; a generous value effectively disables the cap.
    pub resurface_cap: usize,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            relevance_floor: RELEVANCE_FLOOR,
            scope_bonus: SCOPE_BONUS,
            severity_weight: SEVERITY_WEIGHT,
            recency_half_life_days: RECENCY_HALF_LIFE_DAYS,
            salience_half_life_days: SALIENCE_HALF_LIFE_DAYS,
            thread_half_life_days: THREAD_HALF_LIFE_DAYS,
            story_cap: STORY_CAP,
            note_cap: NOTE_CAP,
            thread_cap: THREAD_CAP,
            resurface_penalty: RESURFACE_PENALTY,
            resurface_cap: RESURFACE_CAP,
        }
    }
}
```

Priority is three explicit terms, each on its own half-life so one can't silently mis-split another's decay: base freshness, a salience (severity) boost that lingers, and the thread term that lingers longest.

```rust
// crates/core/src/digest/select.rs:390-398
fn priority(c: &Candidate, cfg: &ScoringConfig, now: DateTime<Utc>) -> f32 {
    let sev = c.max_severity.unwrap_or(0) as f32;
    let recency = recency_decay(now, c.last_event_time, cfg.recency_half_life_days) as f32;
    let salience_decay = recency_decay(now, c.last_event_time, cfg.salience_half_life_days) as f32;
    let thread_decay = recency_decay(now, c.last_event_time, cfg.thread_half_life_days) as f32;
    base_relevance(c, cfg) * recency
        + cfg.severity_weight * sev * salience_decay
        + c.relevance * thread_decay
}
```

`select` runs the whole pipeline. First a per-candidate gate: the cadence freshness floor runs *first* (its drop cause is age, not score — relevance isn't recency-decayed, so without this an old item would sail past the floor), then the relevance floor; survivors are damped for a no-news re-surface (demoted to a "still developing" Note, unless graduating Note→Story) while keeping their *natural* format for the next fire's graduation check.

```rust
// crates/core/src/digest/select.rs:475-547
    // 1. Gate. Dropped candidates get a terminal Decision now; the rest carry forward to ranking.
    let mut gated: Vec<Gated> = Vec::new();
    let mut dropped: Vec<Decision> = Vec::new();
    for c in &candidates {
        // The natural richness classification — what the story *is*. Re-surface may demote what's
        // rendered, but the snapshot must record the natural format so graduation isn't fooled by its
        // own demotion (otherwise a damped Story would "graduate" back next fire, oscillating).
        let (natural_format, natural_phrase) = richness(c);
        let r = relevance(c, cfg);
        // Order: the cadence freshness gate runs *first*. An item too stale to surface has aged out
        // of this digest entirely, so it shouldn't even be relevance-gated — its drop cause is its
        // age, not its score. (Relevance is also not recency-decayed, so without this an old item
        // would sail past the floor and fill a slot when fresh content is thin — the bug this fixes.)
        if c.last_event_time < display_floor {
            dropped.push(Decision {
                id: c.id,
                last_event_time: c.last_event_time,
                relevance: r,
                priority: 0.0,
                natural_format,
                format: natural_format,
                richness: natural_phrase.to_string(),
                verdict: Verdict::Dropped {
                    cause: DropCause::StaleForCadence,
                },
            });
            continue;
        }
        if r < cfg.relevance_floor {
            dropped.push(Decision {
                id: c.id,
                last_event_time: c.last_event_time,
                relevance: r,
                priority: 0.0,
                natural_format,
                format: natural_format,
                richness: natural_phrase.to_string(),
                verdict: Verdict::Dropped {
                    cause: DropCause::BelowFloor,
                },
            });
            continue;
        }
        let mut p = priority(c, cfg, now);
        let mut format = natural_format;
        let mut richness_phrase = natural_phrase;
        let mut resurfaced = false;
        // Re-surface suppression (design §9.4): a story already shown to this subscriber with no new
        // events since — and not graduating Note → Story (natural richness grew) — fades to a compact
        // "still developing" note and is priority-damped, so it sinks and eventually ages out. A
        // genuinely new event (or a graduation) re-surfaces it at full weight.
        if let Some(shown) = c.last_shown {
            let new_events = c.last_event_time > shown.last_event_time;
            let graduated = shown.format == Format::Note && natural_format == Format::Story;
            if !new_events && !graduated {
                format = Format::Note;
                richness_phrase = "still developing";
                p *= cfg.resurface_penalty;
                resurfaced = true;
            }
        }
        gated.push(Gated {
            id: c.id,
            last_event_time: c.last_event_time,
            relevance: r,
            priority: p,
            natural_format,
            format,
            richness: richness_phrase,
            resurfaced,
            thread_id: c.thread_id,
        });
    }
```

Ranking is priority-desc then recency then id (deterministic). The cap pass interleaves Stories and Notes in global priority order and assigns render positions, enforcing four bounds at once: the per-format cap, the small re-surface budget, the per-thread diversity cap, and the subscriber's overall `max_items`. Output is selected (render order) ++ over-cap (rank) ++ dropped (by id), so every candidate is accounted for exactly once.

```rust
// crates/core/src/digest/select.rs:568-604
    for (rank, g) in gated.into_iter().enumerate() {
        let (count, cap) = match g.format {
            Format::Story => (&mut stories, cfg.story_cap),
            Format::Note => (&mut notes, cfg.note_cap),
        };
        // A stale "still developing" re-surface also has to win a slot in the small re-surface budget,
        // on top of its format cap — so a quiet fire can't backfill the digest with recycled notes.
        let resurface_ok = !g.resurfaced || resurfaced < cfg.resurface_cap;
        // Per-thread diversity: a thread already at `thread_cap` can't take another slot this digest,
        // so a busy thread (or an added feed's dominant topic) can't crowd out the rest. Unthreaded
        // stories and `thread_cap == 0` are unconstrained.
        let thread_ok = match g.thread_id {
            Some(t) if cfg.thread_cap > 0 => {
                thread_counts.get(&t).copied().unwrap_or(0) < cfg.thread_cap
            }
            _ => true,
        };
        let verdict = if *count < cap && resurface_ok && thread_ok && position < max_items {
            *count += 1;
            if g.resurfaced {
                resurfaced += 1;
            }
            if let Some(t) = g.thread_id {
                *thread_counts.entry(t).or_insert(0) += 1;
            }
            let pos = position;
            position += 1;
            Verdict::Selected { position: pos }
        } else {
            Verdict::OverCap { rank }
        };
        let decision = g.into_decision(verdict);
        match verdict {
            Verdict::Selected { .. } => selected.push(decision),
            _ => over_cap.push(decision),
        }
    }
```

### `render.rs`

**Purpose:** Render a digest to a `multipart/alternative` email (HTML editorial card + plaintext fallback) and define the `Mailer` delivery seam. Every piece of caller- or feed-supplied text is untrusted, so the module's core is defense against injection: Trojan-Source stripping, HTML escaping, and an `href` scheme allowlist.

`safe_href` is the URL allowlist: feed links are WHATWG-parsed with the `url` crate and their normalized scheme checked against `http`/`https`/`mailto`, so casing/whitespace/control-char tricks can't slip a `javascript:`/`data:` scheme past. Anything else renders as plain text.

```rust
// crates/core/src/digest/render.rs:934-937
fn safe_href(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url.trim()).ok()?;
    matches!(parsed.scheme(), "http" | "https" | "mailto").then(|| parsed.to_string())
}
```

The escaping layer cleans the Trojan-Source class (control/bidi/zero-width) *before* HTML entity-encoding. `escape` is for element text, `escape_attr` for the one double-quoted attribute sink (the `href`), and `escape_prose` additionally `defang`s any URL/bare-domain-shaped token so even a summarizer-gate leak can't reach the client's linkifier as a live link.

```rust
// crates/core/src/digest/render.rs:881-913
fn clean(s: &str) -> Cow<'_, str> {
    if s.chars().any(is_unsafe_char) {
        Cow::Owned(s.chars().filter(|c| !is_unsafe_char(*c)).collect())
    } else {
        Cow::Borrowed(s)
    }
}

// …

fn escape(s: &str) -> String {
    html_escape::encode_text(&clean(s)).into_owned()
}

// …

fn escape_attr(s: &str) -> String {
    html_escape::encode_double_quoted_attribute(&clean(s)).into_owned()
}

// …

fn escape_prose(s: &str) -> String {
    escape(&defang(s))
}
```

The structured TL;DR run-list renders inline: plain text runs escaped, grounded entity `ref` runs turned into namespace-styled inline badges. The model can only reference an entity in the closed grounded set, so a badge can never name a thing that wasn't extracted from ground truth; an unrecognised namespace degrades to plain surface text. `join_run_space` reintroduces a dropped inter-run space so consecutive badges don't glue together.

```rust
// crates/core/src/digest/render.rs:639-677
fn render_summary_runs(runs: &[TldrRun]) -> String {
    let mut out = String::new();
    // The previous run's *visible surface* (not its HTML), so the boundary rule sees real punctuation —
    // a `ref` renders as badge markup whose first/last char is a `<`/`>`, never the surface text.
    let mut prev_surface = String::new();
    for run in runs {
        let surface = run.surface();
        // Re-introduce a single inter-run space the model dropped (the same boundary rule the flat
        // `tldr_text` uses), so consecutive entity badges don't render glued together.
        if crate::summarize::join_run_space(&prev_surface, surface) {
            out.push(' ');
        }
        match run {
            TldrRun::Text { text } => out.push_str(&escape_prose(text)),
            TldrRun::Ref { entity, surface } => out.push_str(&render_entity_badge(entity, surface)),
        }
        prev_surface = surface.to_string();
    }
    out
}

/// One inline entity badge, styled by the `ref` token's namespace (§6.2): a `repo:` dotted-underline
/// tag, a `cve:` severity-tinted pill, a `user:` person chip, anything else plain. Rendering owns the
/// treatment; the model only picks which grounded token to reference and its visible `surface` text.
/// Identity resolution + avatars are a later layer — for now the badge is namespace-styled and the
/// surface text is shown verbatim, degrading gracefully to plain text for an unknown namespace.
fn render_entity_badge(entity: &str, surface: &str) -> String {
    let s = escape_prose(surface);
    match crate::identity::namespace(entity).map(|(ns, _)| ns) {
        Some("repo") => format!(
            r#"<span style="border-bottom:1px dotted {ACCENT};font-weight:600;color:{INK_BODY};">{s}</span>"#
        ),
        Some("cve") => format!(
            r#"<span style="font-family:{SANS};font-size:13px;font-weight:600;background:{BADGE_CVE_BG};color:{BADGE_CVE_INK};padding:1px 7px;border-radius:10px;">{s}</span>"#
        ),
        Some("user") => format!(r#"<span style="font-weight:600;color:{INK};">{s}</span>"#),
        _ => s,
    }
}
```

### `greeting.rs`

**Purpose:** The digest's opening line — a warm salutation keyed to the subscriber's local time-of-day and cadence, phrasing chosen deterministically from a variant table by a per-digest seed (idempotent re-render, rotating across windows). It stands in for the big-picture lead until a real summary is produced. Six time-of-day buckets feed `salutation` (optionally personalized with the subscriber's name); `greeting` splices the salutation + cadence word into one of the `VARIANTS`, and `seed_for` hashes `(subscriber_id, window_end.timestamp())` so the same digest always yields the same line.

```rust
// crates/core/src/digest/greeting.rs:79-97
pub(crate) fn greeting(
    digest_time: NaiveTime,
    recurrence: Recurrence,
    seed: u64,
    name: Option<&str>,
) -> String {
    VARIANTS[(seed % VARIANTS.len() as u64) as usize]
        .replace("{salutation}", &salutation(digest_time, name))
        .replace("{cadence}", cadence_word(recurrence))
}

/// A stable seed from the digest's identity, so the same digest renders the same greeting while
/// consecutive windows rotate. Not persisted or security-sensitive — it only needs to spread.
pub(crate) fn seed_for(subscriber_id: Uuid, window_end: DateTime<Utc>) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    subscriber_id.hash(&mut h);
    window_end.timestamp().hash(&mut h);
    h.finish()
}
```

### `eval.rs`

**Purpose:** Selection-quality evaluation — a pure read over the persisted decision log + the story-feedback log, and the `replay` primitive that re-scores a frozen `ReplaySnapshot` under a trial config (the offline config sweep). It measures the precision family (feedback only exists on shown items), never recall.

`replay` runs the *same* `select` the live path runs over the frozen candidate input, but pins the cadence display floor fully open (`MIN_UTC`): the snapshot doesn't carry the subscriber's recurrence, and the sweep is about the scoring config, not freshness — so every snapshot candidate is re-scored, none gated by age.

```rust
// crates/core/src/digest/eval.rs:35-56
pub fn replay(snapshot: &ReplaySnapshot, cfg: &ScoringConfig) -> Vec<DecisionRecord> {
    select(
        snapshot.candidates.clone(),
        cfg,
        snapshot.max_items,
        snapshot.now,
        DateTime::<Utc>::MIN_UTC,
    )
    .into_iter()
    .map(|d| DecisionRecord {
        story_id: d.id,
        verdict: d.verdict,
        reason: ItemReason {
            relevance: d.relevance,
            format: d.format,
            richness: d.richness,
            priority: d.priority,
            entities: Vec::new(),
        },
    })
    .collect()
}
```

### `subscriber.rs`

**Purpose:** The subscriber row + schedule model — the `Recurrence` type, the cadence display-floor derivation that governs what surfaces, IANA timezone validation, and the RLS-scoped CRUD/schedule-advance queries.

`Recurrence` makes the "weekly ⇔ has a weekday" invariant unrepresentable-when-wrong. `display_window` is one cadence plus a documented grace margin; `display_floor` reaches back to the last delivery (so a delayed/coalesced run surfaces every fresh event since the previous digest) but never less than one `display_window` (so a recent extra run can't collapse the window).

```rust
// crates/core/src/digest/subscriber.rs:63-94
    pub fn display_window(self) -> Duration {
        // Cadence period + grace, kept as named bindings so the grace is documented, not magic.
        // Daily cadence is 1 day; +12h grace covers a run that slips into the next morning.
        let daily_grace = Duration::hours(12);
        // Weekly cadence is 7 days; +1 day grace covers a delayed/coalesced weekly run.
        let weekly_grace = Duration::days(1);
        match self {
            Recurrence::Daily => Duration::days(1) + daily_grace, // 36h
            Recurrence::Weekly { .. } => Duration::days(7) + weekly_grace, // 8d
        }
    }

    // …

    pub fn display_floor(
        self,
        now: DateTime<Utc>,
        last_run: Option<DateTime<Utc>>,
    ) -> DateTime<Utc> {
        let cadence_floor = now - self.display_window();
        last_run.map_or(cadence_floor, |lr| lr.min(cadence_floor))
    }
```

Timezone is validated up front against the IANA database (an unknown zone is a clean error, not a deferred 500) and canonicalized before storage.

```rust
// crates/core/src/digest/subscriber.rs:139-160
    let timezone = if timezone.is_empty() {
        DEFAULT_TIMEZONE
    } else {
        timezone
    };
    let timezone: chrono_tz::Tz = timezone
        .parse()
        .map_err(|_| format!("unknown timezone '{timezone}'"))?;

    let digest_time = if digest_time.is_empty() {
        DEFAULT_DIGEST_TIME
    } else {
        digest_time
    };
    let digest_time = NaiveTime::parse_from_str(digest_time, "%H:%M")
        .map_err(|_| "digest_time must be HH:MM (24-hour)".to_string())?;

    Ok(Schedule {
        recurrence,
        timezone: timezone.name().to_string(),
        digest_time,
    })
```

### `store.rs`

**Purpose:** The digest's persistence contract — the atomic create-with-items freeze, render-item assembly, decision-log / replay-snapshot persistence, and the atomic mark-delivered + schedule-advance.

`create_with_items` idempotently gets-or-creates the `(subscriber, window_end)` digest with its selected stories frozen as `digest_item` rows in one transaction, so the digest is never observed without its selection. The `ON CONFLICT ... DO NOTHING` makes a retry a no-op that returns the existing frozen items untouched; each item also freezes its re-surface snapshot (recency anchor + format).

```rust
// crates/core/src/digest/store.rs:384-440
pub async fn create_with_items(
    pool: &PgPool,
    subscriber_id: Uuid,
    window_end: DateTime<Utc>,
    items: &[FrozenItem],
) -> Result<DigestRow, sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Subscriber(subscriber_id)).await?;

    let created = sqlx::query(
        "INSERT INTO digest (subscriber_id, window_end)
         VALUES ($1, $2)
         ON CONFLICT (subscriber_id, window_end) DO NOTHING
         RETURNING id, subscriber_id, window_end, delivered_at",
    )
    .bind(subscriber_id)
    .bind(window_end)
    .try_map(row_to_digest)
    .fetch_optional(&mut *tx)
    .await?;

    let row = match created {
        Some(row) => {
            for (position, item) in items.iter().enumerate() {
                // Freeze the re-surface snapshot too (design §9.4): the recency anchor + format this
                // story was shown at, so the next fire can damp it if no new events arrive.
                sqlx::query(
                    "INSERT INTO digest_item
                        (digest_id, story_id, position, story_last_event_time, format)
                     VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(row.id)
                .bind(item.story_id)
                .bind(position as i32)
                .bind(item.last_event_time)
                .bind(item.format.as_str())
                .execute(&mut *tx)
                .await?;
            }
            row
        }
        // Already exists — its items are frozen from the original transaction.
        None => {
            sqlx::query(
                "SELECT id, subscriber_id, window_end, delivered_at
             FROM digest WHERE subscriber_id = $1 AND window_end = $2",
            )
            .bind(subscriber_id)
            .bind(window_end)
            .try_map(row_to_digest)
            .fetch_one(&mut *tx)
            .await?
        }
    };

    tx.commit().await?;
    Ok(row)
}
```

`mark_delivered` closes the loop atomically: it stamps `delivered_at` (guarded by `delivered_at IS NULL` so a re-run is a no-op), marks the carried stories delivered, and advances the subscriber's schedule — all in one transaction so the "delivered ⇒ schedule moved" invariant can't tear across a crash.

```rust
// crates/core/src/digest/store.rs:729-747
pub async fn mark_delivered(
    pool: &PgPool,
    digest_id: Uuid,
    subscriber_id: Uuid,
    delivered_through: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let mut tx = begin_scope(pool, ScopeCtx::Subscriber(subscriber_id)).await?;
    sqlx::query("UPDATE digest SET delivered_at = now() WHERE id = $1 AND delivered_at IS NULL")
        .bind(digest_id)
        .execute(&mut *tx)
        .await?;
    // Stamp the carried stories as delivered (gates the asymmetric-merge rule, §8.2) in the same
    // transaction, so "delivered ⇒ story seen" can't tear across a crash.
    crate::link::store::mark_stories_delivered(&mut *tx, digest_id).await?;
    crate::digest::subscriber::advance_after_delivery(&mut *tx, subscriber_id, delivered_through)
        .await?;
    tx.commit().await?;
    Ok(())
}
```

## `summarize` — write-side LLM pre-summarization + on-path lead

The `summarize` module produces the durable, content-hashed cluster summary every higher surface composes from. Its contract is inverted from a best-effort enrichment: a cluster ships in a digest *only* once it carries a faithful model summary (the §3.4 gate's `confirmed`/`probable` band). A summarization that fails is a tracked error with bounded, escalating-seed retries; a cluster whose retries are exhausted is quarantined for operator review and withheld — it never blocks a subscriber's digest, it just slips to a later window. The module splits into a pure, unit-testable core (data model, content signature, grounding facts, prompts/schema, the deterministic gate) and a model edge (`client` + `store`).

### `mod.rs`

**Purpose:** The pure core — the `ClusterSummary` / `Facts` / `Relation` / `Band` data model, `summary_hash`, the deterministic `faithful` gate, and `SummarizationConfig` (including `for_attempt` seed escalation and the `MAX_SUMMARY_ATTEMPTS` quarantine budget).

`Band` is the faithfulness verdict carried to render. Under §3.7 a stored summary is always `Confirmed`/`Probable`; `Uncertain` is only the inert default of a never-summarized unit (a rejected candidate is a tracked failure, never stored as `Uncertain`).

```rust
// crates/core/src/summarize/mod.rs:368-387
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Band {
    Confirmed,
    Probable,
    #[default]
    Uncertain,
}

impl Band {
    /// The lowercase string form (matching the serde rename), for the render debug trace — one source
    /// of the token, mirroring [`ConfidenceBand::as_str`](crate::identity::ConfidenceBand::as_str).
    pub fn as_str(self) -> &'static str {
        match self {
            Band::Confirmed => "confirmed",
            Band::Probable => "probable",
            Band::Uncertain => "uncertain",
        }
    }
}
```

`Relation` is the directional who-did-what-to-whom fact — the backbone a small model is most prone to invert (a `subject`/`object` swap). It is extracted once (subject anchored to the closed entity set), fed to the summarizer as pre-bound facts, and re-checked by the relational gate. `Facts` is the extract-half skeleton; `ClusterSummary` is the stored `cluster.summary` jsonb whose inert `'{}'` default means no pass has run.

```rust
// crates/core/src/summarize/mod.rs:430-449
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Relation {
    /// The actor — constrained to a `facts.entities` id by the extraction schema (and re-validated).
    #[serde(default)]
    pub subject: String,
    /// The action, as a short verb phrase.
    #[serde(default)]
    pub predicate: String,
    /// What the action is done to — a short noun phrase (entity surface or literal).
    #[serde(default)]
    pub object: String,
}

impl Relation {
    /// The one-line `subject -> predicate -> object` form fed to the summarizer prompt and the
    /// relational gate's judge prompt — the single rendering so the two passes read the same shape.
    pub fn line(&self) -> String {
        format!("{} -> {} -> {}", self.subject, self.predicate, self.object)
    }
}
```

```rust
// crates/core/src/summarize/mod.rs:486-503
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ClusterSummary {
    /// Abstractive headline, ≤ ~90 chars (the schema's `maxLength`).
    #[serde(default)]
    pub headline: String,
    /// The structured 2–4 sentence tldr as a run-list of text + grounded entity refs (§6.2).
    #[serde(default)]
    pub tldr: Vec<TldrRun>,
    /// Flat concatenation of the tldr's runs — for the plaintext email + inbox preview (§6.2).
    #[serde(default)]
    pub tldr_text: String,
    /// The grounding facts (the extract half) — reused by every higher tier.
    #[serde(default)]
    pub facts: Facts,
    /// The faithfulness verdict (§3.4).
    #[serde(default)]
    pub band: Band,
}
```

`summary_hash` is the content signature that makes summarization idempotent (recomputed only when content changes, not per fire or per subscriber). It hashes `best_text` (fetched `full_text`, else `body`), so a late article fetch moves the hash and re-grounds the summary for free.

```rust
// crates/core/src/summarize/mod.rs:592-619
pub fn summary_hash(events: &[Event]) -> Vec<u8> {
    const FIELD: u8 = 0x00; // field separator
    const ITEM: u8 = 0x1f; // intra-field list separator (ASCII unit separator)

    let mut order: Vec<&Event> = events.iter().collect();
    order.sort_by(|a, b| a.event_time.cmp(&b.event_time).then(a.id.cmp(&b.id)));

    let mut h = Sha256::new();
    for e in order {
        h.update(e.title.as_bytes());
        h.update([FIELD]);
        if let Some(b) = e.best_text() {
            h.update(b.as_bytes());
        }
        h.update([FIELD]);
        for l in &e.links {
            h.update(l.as_bytes());
            h.update([ITEM]);
        }
        h.update([FIELD]);
        for ent in &e.entities {
            h.update(ent.as_bytes());
            h.update([ITEM]);
        }
        h.update([FIELD]);
    }
    h.finalize().to_vec()
}
```

The deterministic `faithful` gate is the real backstop: the model may drop a fact but never add one. It checks length budgets, closed-enum entity refs, token-equality numeric grounding, the house-voice denylist, and no web addresses in prose.

```rust
// crates/core/src/summarize/mod.rs:1163-1218
pub fn faithful(
    summary: &ClusterSummary,
    facts: &Facts,
    source_text: &str,
) -> Result<(), GateViolation> {
    /// Headline budget (chars) — matches the schema `maxLength` (§3.3).
    const HEADLINE_MAX: usize = 90;
    /// tldr budget (chars) — 2–4 sentences.
    const TLDR_MAX: usize = 480;

    if summary.headline.chars().count() > HEADLINE_MAX
        || summary.tldr_text.chars().count() > TLDR_MAX
    {
        return Err(GateViolation::TooLong);
    }

    // Closed-enum entity refs (a hallucinated mention is structurally impossible, but verify).
    for run in &summary.tldr {
        if let TldrRun::Ref { entity, .. } = run {
            if !facts.entities.iter().any(|e| e == entity) {
                return Err(GateViolation::UngroundedEntity(entity.clone()));
            }
        }
    }

    // Numbers/dates: every numeric token in the output must be grounded — appear as the *same token*
    // in the facts' numbers/dates or the source text. Token-equality, not substring, so an output "40"
    // is never falsely grounded by a source "4000" (see [`first_ungrounded_number`]).
    let mut grounding: String = facts
        .numbers
        .iter()
        .chain(facts.dates.iter())
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    grounding.push(' ');
    grounding.push_str(source_text);
    let output = format!("{} {}", summary.headline, summary.tldr_text);
    if let Some(tok) = first_ungrounded_number(&output, &grounding) {
        return Err(GateViolation::UngroundedNumber(tok));
    }

    // House-voice lint (§3.6 denylist), whole-word + case-insensitive.
    if let Some(w) = banned_word_in(&output) {
        return Err(GateViolation::BannedWord(w));
    }

    // No web addresses in the prose: the model names sources and the interface carries the link, so a
    // leaked URL — or bare domain — is both an artifact and a client-autolink surface the renderer
    // can't keep inert (the client linkifies displayed text on its own). Reject; baseline is true.
    if let Some(u) = url_like_token(&output) {
        return Err(GateViolation::UrlInProse(u));
    }

    Ok(())
}
```

`for_attempt` implements the escalation the §3.7 retry depends on: a gate rejection is deterministic under a fixed seed, so a bare retry only reproduces it — attempt 0 is unchanged (keeping the content-hash cache meaningful for a clean first pass), while a later attempt offsets the seed by a fixed odd stride and nudges the temperature up. `MAX_SUMMARY_ATTEMPTS` is the retry budget before quarantine.

```rust
// crates/core/src/summarize/mod.rs:241-251
    pub fn for_attempt(&self, attempt: i32) -> Self {
        if attempt <= 0 {
            return self.clone();
        }
        let mut c = self.clone();
        c.seed = self
            .seed
            .wrapping_add((attempt as u32).wrapping_mul(0x9E37_79B9));
        c.temperature = (self.temperature + 0.15 * attempt as f32).min(0.9);
        c
    }
```

### `client.rs`

**Purpose:** The local-sidecar model edge — `summarize_cluster` (the extract→summarize→gate flow), the relational-gate ensemble, the on-path `authored_lead`, the boot-time `ensure_reachable`, and the shared `chat_json` plumbing every constrained call routes through. 100% local, so the no-egress invariant holds.

`summarize_cluster` is the flow with no baseline fallback (§3.7): generate the candidate, defang a leaked URL rather than reject for it, run the deterministic gate, then the relational gate — any rejection returns `SummaryFailure::Rejected` (a later sweep retries with an escalated seed); a transport failure returns `SummaryFailure::Unavailable`.

```rust
// crates/core/src/summarize/client.rs:51-94
pub async fn summarize_cluster(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    events: &[Event],
) -> SummaryOutcome {
    let (mut summary, facts, source) = match generate_candidate(cfg, http, events).await {
        Ok(generated) => generated,
        Err(e) => {
            let kind = failure_kind(&e);
            tracing::warn!(
                error = %format!("{e:#}"),
                kind,
                base_url = %cfg.base_url,
                model = %cfg.model,
                "summarization model call failed; leaving cluster unsummarized for retry"
            );
            return SummaryOutcome::Failed(SummaryFailure::Unavailable(kind));
        }
    };

    // Defang a leaked URL/bare-domain into an inert `acme[.]com` rather than letting the gate reject the
    // whole summary for it — with no §3.7 baseline to fall back to, a reject withholds (then quarantines)
    // an otherwise-faithful cluster. The eval path (`eval_cluster`) deliberately does *not* defang, so it
    // still measures the model's raw bare-domain leak rate.
    summary.defang_prose();
    if cfg.faithfulness_gate {
        if let Err(v) = faithful(&summary, &facts, &source) {
            tracing::debug!(violation = ?v, "faithfulness gate rejected summary; will retry with an escalated seed");
            metric::gate_rejection("summarize", &v);
            return SummaryOutcome::Failed(SummaryFailure::Rejected(v));
        }
    }
    // The relational gate (§3.2): the deterministic gate above proves every entity/number is *present*;
    // this proves their *binding* survived — the model didn't reverse a grounded relation's direction
    // (the subject↔object swap the deterministic checks are blind to). Its own toggle, independent of
    // `faithfulness_gate` (a no-op when off or when extraction produced no relations); a returned
    // violation drives the same escalating-seed retry as any other rejection.
    if let Some(v) = relational_gate_violation(cfg, http, &summary, &facts).await {
        tracing::debug!(violation = ?v, "relational gate rejected summary; will retry with an escalated seed");
        metric::gate_rejection("summarize", &v);
        return SummaryOutcome::Failed(SummaryFailure::Rejected(v));
    }
    SummaryOutcome::Faithful(summary)
}
```

The relational gate is a recall-biased **ensemble**: a candidate is rejected only when *every* completed vote agrees it inverts a relation. Because the gate is a model, a single noisy `faithful:false` would (through the retry/quarantine path) silently drop a real story — so the first vote that says `faithful`, or that can't be reached, passes the summary; a rejection needs unanimous, re-seeded agreement.

```rust
// crates/core/src/summarize/client.rs:595-647
async fn relation_gate(
    cfg: &SummarizationConfig,
    http: &reqwest::Client,
    summary: &ClusterSummary,
    facts: &Facts,
) -> Option<GateViolation> {
    let mut completed = 0;
    let mut last_problem = String::new();
    for vote in 0..RELATION_GATE_VOTES {
        // Re-seed each vote (attempt 0 is the base seed) so the opinions are genuinely independent
        // rather than the same deterministic draw repeated.
        let vcfg = cfg.for_attempt(vote);
        let verdict = match chat_json::<RelationVerdict>(
            &vcfg,
            http,
            "relation_gate",
            RELATION_GATE_SYSTEM_PROMPT,
            relation_gate_user_prompt(&facts.relations, &summary.headline, &summary.tldr_text),
            cfg.relations_max_tokens,
            relation_gate_schema(),
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                // Fail-open per vote: a judge we can't reach must not reject a summary the deterministic
                // gate passed. A single unreachable vote forfeits the whole rejection (recall bias).
                tracing::debug!(
                    error = %format!("{e:#}"),
                    kind = failure_kind(&e),
                    vote,
                    "relational gate judge unavailable; passing summary on the deterministic gate alone"
                );
                return None;
            }
        };
        // Any "faithful" opinion keeps the summary — a rejection must be unanimous.
        if verdict.faithful {
            return None;
        }
        completed += 1;
        last_problem = if verdict.problem.trim().is_empty() {
            "summary reverses or contradicts a grounded relation".to_string()
        } else {
            verdict.problem.trim().to_string()
        };
    }
    // Every vote ran and every one said unfaithful.
    if completed > 0 {
        return Some(GateViolation::UnfaithfulRelation(last_problem));
    }
    None
}
```

`chat_json` is the single choke point every constrained call routes through (summarizer, comprehension, relation extraction/gate, synthesis, enrichment): it builds the OpenAI-compatible body with the `response_format: json_schema` grammar, optionally suppresses the reasoning model's `<think>` block, POSTs to the local sidecar, and deserializes `choices[0].message.content` — recording `llm_call` latency and `llm_tokens` at the seam.

```rust
// crates/core/src/summarize/client.rs:759-778
    let mut body = serde_json::json!({
        "model": cfg.model,
        "messages": [
            { "role": "system", "content": system },
            { "role": "user", "content": user }
        ],
        "temperature": cfg.temperature,
        "seed": cfg.seed,
        "max_tokens": max_tokens,
        "response_format": { "type": "json_schema", "json_schema": schema }
    });
    // Turn off the model's native chain-of-thought for these constrained calls. A reasoning model
    // (e.g. Qwen3) left in "thinking" mode spends the small `max_tokens` budget on a `<think>` block
    // and then returns an **empty** `content` (which serde reports as the unhelpful "EOF while parsing
    // a value at line 1 column 0") — or blows the request timeout producing it. The grammar already
    // shapes the answer to JSON, so the reasoning buys nothing here. Honoured by a llama.cpp sidecar
    // run with `--jinja`; harmlessly ignored by servers/models that don't template on it.
    if cfg.disable_thinking {
        body["chat_template_kwargs"] = serde_json::json!({ "enable_thinking": false });
    }
```

`ensure_reachable` is the boot-time gate: summarization is a hard dependency now, so an unreachable sidecar at start is a deployment error surfaced loudly (a failed unit → rollback) rather than a worker that silently quarantines the whole corpus. It probes `{base_url}/models` with capped exponential backoff until a deadline, giving a sidecar still mapping its GGUF time to answer.

```rust
// crates/core/src/summarize/client.rs:443-472
pub async fn ensure_reachable(cfg: &SummarizationConfig, deadline: Duration) -> anyhow::Result<()> {
    // Short per-attempt timeout so one hung connect can't swallow the whole window in a single try.
    let attempt_timeout = Duration::from_secs(5).min(deadline);
    let http = reqwest::Client::builder()
        .timeout(attempt_timeout)
        .build()
        .context("build sidecar readiness http client")?;
    let url = format!("{}/models", cfg.base_url);

    let start = Instant::now();
    let mut backoff = Duration::from_secs(1);
    loop {
        let last_err = match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => format!("sidecar returned HTTP {}", resp.status()),
            Err(e) => format!("{e}"),
        };
        let remaining = deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            anyhow::bail!(
                "summarization sidecar at {} not reachable after {}s: {last_err}",
                cfg.base_url,
                deadline.as_secs(),
            );
        }
        // Never sleep past the deadline, then grow the backoff (capped) for the next try.
        tokio::time::sleep(backoff.min(remaining)).await;
        backoff = (backoff * 2).min(Duration::from_secs(5));
    }
}
```

### `metric.rs`

**Purpose:** The thin `bulletin_llm_*` recorders for the summarization path — one place for the metric-name strings, no-ops until the Prometheus recorder is installed. The load-bearing pair for the §3.7 story: `summary_failed` splits the down-sidecar rate from the hallucination-rejection rate, and `quarantined` marks a unit whose retry budget is spent.

```rust
// crates/core/src/summarize/metric.rs:56-67
pub fn summary_failed(unit: &'static str, kind: &'static str) {
    metrics::counter!("bulletin_llm_summary_failed_total", "unit" => unit, "kind" => kind)
        .increment(1);
}

// …

pub fn quarantined(unit: &'static str) {
    metrics::counter!("bulletin_llm_quarantined_total", "unit" => unit).increment(1);
}
```

### `store.rs`

**Purpose:** The summarization store contract — the work-queue read (with the quarantine/staleness gate), the summary write, and the §3.7 failure/quarantine bookkeeping on the rebuildable `cluster` cache.

`clusters_needing_summary` is the work queue. The cheap SQL gate is "never summarized OR content moved OR model/prompt upgraded" AND "not quarantined OR content moved past the quarantine" — a quarantined cluster is withheld unless its content changed, which earns it a fresh budget (surfaced as `retry_reset` so the sweep zeroes the stale attempt count).

```rust
// crates/core/src/summarize/store.rs:44-80
pub(crate) async fn clusters_needing_summary(
    conn: &mut PgConnection,
    scope: &Scope,
    current_model: &str,
    limit: i64,
) -> Result<Vec<DueCluster>, sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = scope.to_columns();
    sqlx::query(
        "SELECT id, source, group_key, summary_hash, summary_attempts,
                (summary_quarantined_at IS NOT NULL AND updated_at > summary_quarantined_at) AS retry_reset
         FROM cluster
         WHERE scope_kind = $1 AND scope_subscriber_id IS NOT DISTINCT FROM $2
           AND ( summarized_at IS NULL
                 OR updated_at > summarized_at
                 OR summary_model IS DISTINCT FROM $3 )
           AND ( summary_quarantined_at IS NULL
                 OR updated_at > summary_quarantined_at )
         ORDER BY last_event_time DESC
         LIMIT $4",
    )
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(current_model)
    .bind(limit)
    .try_map(|row: PgRow| {
        Ok(DueCluster {
            id: row.get("id"),
            source: row.try_get("source")?,
            group_key: row.get("group_key"),
            summary_hash: row.get("summary_hash"),
            summary_attempts: row.get("summary_attempts"),
            retry_reset: row.get("retry_reset"),
        })
    })
    .fetch_all(conn)
    .await
}
```

`record_summary_failure` (via the shared `record_failure`) sets the consecutive-attempt counter absolutely, stores the coarse error, stamps `summary_failed_at`, and — crucially — does *not* advance `summarized_at`, so the cluster stays in the work queue for the next (escalated-seed) retry. `summary_quarantined_at` is `now()` on quarantine, NULL otherwise (so a unit retrying past a quarantine sheds the stale flag).

```rust
// crates/core/src/summarize/store.rs:140-164
async fn record_failure(
    conn: &mut PgConnection,
    table: &str,
    id: Uuid,
    attempts: i32,
    error: &str,
    quarantine: bool,
) -> Result<(), sqlx::Error> {
    let sql = format!(
        "UPDATE {table}
         SET summary_attempts = $2,
             summary_last_error = $3,
             summary_failed_at = now(),
             summary_quarantined_at = CASE WHEN $4 THEN now() ELSE NULL END
         WHERE id = $1"
    );
    sqlx::query(&sql)
        .bind(id)
        .bind(attempts)
        .bind(error)
        .bind(quarantine)
        .execute(conn)
        .await?;
    Ok(())
}
```

The counterpart `store_summary` writes the gate-passed summary and *clears* the entire §3.7 health record (attempts, last error, quarantine) — a faithful summary is the success that resets the budget.


---

## Binary crate (`bulletin`) — orchestration

The `bulletin` binary is the orchestrator: a single executable whose subcommands select a *role* (a trigger over the shared `bulletin_core` engine). `serve`, `worker`, and `api` are the three long-running triggers; `migrate` provisions schema as the owner role; `secrets` and `debug` are offline/remote tooling that need no DB. Every role authenticates and self-scopes, then defers all durability, dedup, and business logic to `core`.

### `main.rs`

**Purpose:** CLI definition, the two-role DB split, and per-role dispatch — the process entrypoint.

The top-level `Cli` carries two distinct connection strings: a least-privilege runtime role (`DATABASE_URL`, used by `serve`/`worker`/`api` under RLS) and a separate owner/migration role (`BULLETIN_MIGRATION_DATABASE_URL`) that owns the DDL. A runtime credential can therefore never alter the schema or disable RLS. The `Command` enum enumerates every role.

```rust
// crates/bulletin/src/main.rs:78-104
#[derive(Subcommand)]
enum Command {
    Serve,
    Worker,
    Migrate,
    All,
    /// Run the gRPC service API server (admin plane).
    Api,
    /// Read-only faithfulness eval (the `digest-explain` hook, `docs/llm-summarization.md` §3.4/§7):
    /// generate candidate summaries for a sample of historical public clusters, run them through the
    /// faithfulness gate, and report the Vectara-style entity/number accuracy rate — **storing nothing
    /// and touching no digest**. Requires a reachable summarization sidecar (it measures the model).
    SummaryEval {
        /// How many recent public clusters to sample.
        #[arg(long, default_value_t = 100)]
        limit: i64,
    },
    Debug {
        #[command(subcommand)]
        command: debug::DebugCommand,
    },
    /// Offline credential tooling: generate the master key, seal secrets for config. No database.
    Secrets {
        #[command(subcommand)]
        command: secrets::SecretsCommand,
    },
}
```

Dispatch is a single `match`. Offline tooling (`Secrets`) returns before any DB connect. `Migrate` connects as the owner role (falling back to the runtime URL for single-role dev) and runs core migrations, apalis storage setup, and a re-grant of runtime access. The DB-touching triggers connect the runtime pool and unseal their secrets; `Worker` and `All` also gate on the summarization sidecar before doing work.

```rust
// crates/bulletin/src/main.rs:116-166
        Command::Migrate => {
            // Migrate as the owner role: it owns the DDL, creates the runtime role + RLS policies,
            // and (afterwards) grants the runtime role its table access.
            let migration_url = cli
                .migration_database_url
                .as_deref()
                .map(Ok)
                .unwrap_or_else(|| cli.database_url())?;
            let pool = connect_pool(migration_url).await?;
            tracing::info!("running bulletin migrations");
            bulletin_core::migrate(&pool)
                .await
                .context("bulletin migrations failed")?;
            tracing::info!("running apalis storage setup");
            worker::setup_storage(&pool)
                .await
                .context("apalis storage setup failed")?;
            // Re-grant the runtime role its access every migrate, so tables added by this run (and
            // the apalis queue schema, just created above) are always reachable by `bulletin_app`.
            tracing::info!("granting runtime role access");
            bulletin_core::grant_runtime_role(&pool)
                .await
                .context("granting runtime role access failed")?;
            tracing::info!("migrations complete");
        }
        Command::Serve => {
            let pool = connect_pool(cli.database_url()?).await?;
            let webhook_secret = cli.secrets.webhook_secret()?;
            tracing::info!(addr = %cli.http_addr, "starting HTTP server");
            serve(cli.http_addr, pool, webhook_secret).await?;
        }
        Command::Worker => {
            metric::init(cli.metrics_addr)?;
            let pool = connect_pool(cli.database_url()?).await?;
            let connectors = cli.secrets.connector_ctx()?;
            // Fail loud if the summarization sidecar isn't reachable — it's a required dependency (§3.7).
            ensure_sidecar_ready().await?;
            tracing::info!("starting worker");
            worker::start(pool, cli.email.clone(), connectors).await?;
        }
        Command::Api => {
            let pool = connect_pool(cli.database_url()?).await?;
            tracing::info!(addr = %cli.api_addr, "starting gRPC API server");
            api::serve(
                cli.api_addr,
                pool,
                cli.api_admin_key.clone(),
                cli.email.clone(),
            )
            .await?;
        }
```

`All` composes the three triggers in one process via `tokio::try_join!`, cloning the one pool across them. Critically it verifies the sidecar *before* binding `/health`, so a box that can't reach its sidecar never reports healthy and the deploy rolls back instead of quarantining the corpus.

```rust
// crates/bulletin/src/main.rs:167-188
        Command::All => {
            metric::init(cli.metrics_addr)?;
            let pool = connect_pool(cli.database_url()?).await?;
            let webhook_secret = cli.secrets.webhook_secret()?;
            let connectors = cli.secrets.connector_ctx()?;
            // Verify the summarization sidecar *before* binding `/health`. Summarization is required
            // (§3.7), so a box that can't reach its sidecar never reports healthy — the deploy's
            // `ExecStartPost` health probe fails and the rollout rolls back, instead of starting a worker
            // that quarantines the corpus and defers every digest.
            ensure_sidecar_ready().await?;
            tracing::info!(addr = %cli.http_addr, "starting server + worker + api");
            tokio::try_join!(
                serve(cli.http_addr, pool.clone(), webhook_secret),
                worker::start(pool.clone(), cli.email.clone(), connectors),
                api::serve(
                    cli.api_addr,
                    pool,
                    cli.api_admin_key.clone(),
                    cli.email.clone()
                )
            )?;
        }
```

The `ensure_sidecar_ready` gate is the boot-time check: it gives the sidecar a bounded startup window (tunable via `BULLETIN_LLM_STARTUP_TIMEOUT_SECS`, default 60) and hard-fails an absent sidecar with actionable context. The running path handles transient mid-sweep blips separately.

```rust
// crates/bulletin/src/main.rs:222-243
async fn ensure_sidecar_ready() -> Result<()> {
    use std::time::Duration;
    let cfg = bulletin_core::summarize::SummarizationConfig::from_env();
    let timeout_secs = std::env::var("BULLETIN_LLM_STARTUP_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(60);
    tracing::info!(
        base_url = %cfg.base_url,
        timeout_s = timeout_secs,
        "verifying summarization sidecar is reachable"
    );
    bulletin_core::summarize::client::ensure_reachable(&cfg, Duration::from_secs(timeout_secs))
        .await
        .context(
            "summarization sidecar unreachable at startup. Summarization is required (§3.7): bring up \
             the llama-server sidecar and check BULLETIN_LLM_BASE_URL",
        )?;
    tracing::info!("summarization sidecar reachable");
    Ok(())
}
```

### `worker.rs`

**Purpose:** the trigger layer — a cron tick (the sole enqueuer) plus apalis workers that translate each `core` flow's outcome into metrics. All durability/dedup lives in the flows' watermarks.

**Job payloads.** Each job is a small serde struct. Payload-less jobs (`PublicBuildJob`, `FetchArticlesJob`) always process "everything new since the watermark". `ProcessWebhookJob` carries the raw JSON body as `String` (not `Vec<u8>`, which apalis would balloon into an integer array) plus the two header values the body lacks, including the `delivery_id` used as the enqueue idempotency key.

```rust
// crates/bulletin/src/worker.rs:42-92
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollConnectionJob {
    pub connection_id: Uuid,
}

/// PublicBuild carries no payload — it always processes "everything new since the watermark".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublicBuildJob;

/// FetchArticles carries no payload — it always processes "every public event still wanting a
/// full-text fetch" (the work-queue gate). Best-effort, off the punctual path (Phase 1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchArticlesJob;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateDigestJob {
    pub subscriber_id: Uuid,
}

/// The per-job retry budget for `GenerateDigest` (§3.7). A digest with items defers (errors) rather than
/// ship without an LLM lead; apalis re-runs a `Failed` job up to this many attempts before killing it, so
/// this is how long a window waits out a transient sidecar blip before being given up on. Larger than the
/// apalis default (5) so a brief sidecar restart doesn't drop the window. NB: apalis-postgres applies no
/// inter-retry backoff, so the wall-clock window is roughly `DIGEST_MAX_ATTEMPTS x poll-interval`.
const DIGEST_MAX_ATTEMPTS: u32 = 12;

/// The attempt at/after which a still-deferred lead is escalated to an operator alert — set below
/// [`DIGEST_MAX_ATTEMPTS`] so the alert precedes the give-up (the job keeps retrying until killed).
const DIGEST_LEAD_ALERT_ATTEMPT: u32 = 8;

/// thread_maintenance for one subscriber (design `docs/thread-layer.md` §5.1): the write-side,
/// best-effort job that rebuilds the subscriber's identity graph + threads and projects the
/// entity-weight map. Coalesced to a relaxed cadence by an hourly idempotency key, and never on the
/// punctual digest path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadMaintenanceJob {
    pub subscriber_id: Uuid,
}

/// A verified webhook delivery, taken off the HTTP edge for off-request-path processing. Carries the
/// raw body plus the two header values the body itself doesn't hold: the activity `event_type` and
/// the `delivery_id` (also the enqueue idempotency key — GitHub retries on a non-2xx). `body` is the
/// JSON text, not `Vec<u8>`: apalis stores job args as JSON, where a byte vec balloons into an
/// integer array — and a verified GitHub delivery is UTF-8 JSON anyway.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessWebhookJob {
    pub source: SourceKind,
    pub event_type: String,
    pub delivery_id: String,
    pub body: String,
}
```

**A representative handler.** Every handler is `flow → metrics` wrapped in `traced` (a span + wait/elapsed timing derived from the ULID `TaskId`). `poll_connection` shows the shape: call `core`, match its outcome enum, emit counters, and box any error for apalis.

```rust
// crates/bulletin/src/worker.rs:136-157
async fn poll_connection(
    job: PollConnectionJob,
    task_id: TaskId<Ulid>,
    attempt: Attempt,
    pool: Data<PgPool>,
    ctx: Data<ConnectorCtx>,
) -> Result<(), BoxDynError> {
    traced("poll_connection", task_id, attempt, async move {
        match bulletin_core::ingest::poll(&pool, job.connection_id, &ctx).await {
            Ok(PollOutcome::Polled {
                source,
                inserted,
                deduplicated,
            }) => metric::ingest_result(source.as_str(), "poll", inserted, deduplicated),
            Ok(PollOutcome::Failed { source }) => metric::poll_failed(source.as_str()),
            Ok(PollOutcome::Skipped) => {}
            Err(e) => return Err(boxed(e)),
        }
        Ok(())
    })
    .await
}
```

**The retry budget in action.** `generate_digest` folds the apalis attempt index into the lead's seed (so retries draw fresh leads) and enforces the §3.7 contract: a digest with items never ships without an LLM lead. `LeadDeferred` returns an error so apalis re-runs the same job up to `DIGEST_MAX_ATTEMPTS`; once the quiet budget is spent (at `DIGEST_LEAD_ALERT_ATTEMPT`) it alerts loudly before the eventual give-up.

```rust
// crates/bulletin/src/worker.rs:353-382
                match outcome {
                    DigestOutcome::Delivered { items } => {
                        metric::digest_outcome("delivered");
                        metric::digest_items(items);
                    }
                    DigestOutcome::Empty => metric::digest_outcome("empty"),
                    DigestOutcome::AlreadyDelivered => metric::digest_outcome("already_delivered"),
                    DigestOutcome::NotYetDue => metric::digest_outcome("not_yet_due"),
                    // The §3.7 contract: a digest with items never ships without an LLM lead. Nothing was
                    // delivered and the watermark didn't advance — error so apalis re-runs this same job
                    // (the `Failed`-and-retry path, bounded by the job's max_attempts = DIGEST_MAX_ATTEMPTS;
                    // the box may recover, or a re-seeded lead may pass). Once the quiet-recovery budget is
                    // spent (but before the job is killed), alert loudly rather than retry silently. The
                    // retry budget and alert threshold are the trigger layer's policy, kept out of core.
                    DigestOutcome::LeadDeferred => {
                        metric::digest_outcome("lead_deferred");
                        if job_attempt >= DIGEST_LEAD_ALERT_ATTEMPT {
                            metric::digest_lead_unavailable();
                            tracing::error!(
                                subscriber_id = %job.subscriber_id,
                                job_attempt,
                                max_attempts = DIGEST_MAX_ATTEMPTS,
                                "digest still undelivered — its LLM lead has been unavailable across the retry budget; operator attention needed (sidecar?)"
                            );
                        }
                        return Err(boxed(anyhow::anyhow!(
                            "digest lead unavailable (attempt {job_attempt}); deferring delivery until it can be composed"
                        )));
                    }
                }
```

**The cron tick: due-sweeps + idempotency keys.** `run_tick` is the sole enqueuer. It reads three (plus fetch + maintenance) "what's due" conditions and pushes work, advancing no watermarks itself. Dedup mechanisms differ per sweep: `PollConnection` relies on the `next_poll_at` watermark; `PublicBuild` on a grace-aware watermark gate + advisory lock; `FetchArticles` and `GenerateDigest` on apalis idempotency keys. The digest key is once-per-window-ever and the task widens `max_attempts` to the digest retry budget.

```rust
// crates/bulletin/src/worker.rs:552-602
    if bulletin_core::ingest::fetch::events_needing_fetch_exist(&*pool).await? {
        tracing::debug!("tick: enqueuing article fetch");
        let mut storage: PostgresStorage<FetchArticlesJob> = PostgresStorage::new(&pool);
        let bucket = Utc::now().timestamp() / 60;
        let task = TaskBuilder::new(FetchArticlesJob)
            .with_idempotency_key(format!("fetch_articles:{bucket}"))
            .build();
        match storage.push_task(task).await {
            Ok(()) => {}
            Err(e) if is_duplicate_enqueue(&e) => {}
            Err(e) => return Err(e.into()),
        }
    }

    // 3. Subscribers due → GenerateDigest (dedup: apalis idempotency key). No build gate — the
    //    digest reads the latest materialized snapshot; unbuilt events ride the next fire.
    let subs = due_subscribers(&pool).await?;
    if !subs.is_empty() {
        tracing::info!(count = subs.len(), "tick: dispatching due digests");
        let mut storage: PostgresStorage<GenerateDigestJob> = PostgresStorage::new(&pool);
        for s in subs {
            // window_end = next_run_at boundary; once-per-window-ever key.
            let key = format!("digest:{}:{}", s.id, s.next_run_at.timestamp());
            let task = TaskBuilder::new(GenerateDigestJob {
                subscriber_id: s.id,
            })
            .with_idempotency_key(key)
            // Widen the retry budget so a deferred digest (lead unavailable, §3.7) rides out a transient
            // sidecar blip across several re-runs before apalis kills the job, instead of the default 5.
            .max_attempts(DIGEST_MAX_ATTEMPTS)
            .build();
            match storage.push_task(task).await {
                Ok(()) => {}
                Err(e) if is_duplicate_enqueue(&e) => {
                    tracing::debug!(subscriber_id = %s.id, "digest already enqueued for this window");
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    // 4. Thread maintenance (write-side, off the punctual path) — only the subscribers actually due
    //    for a pass (a watermark-gated due query, like the digest sweep), not a full scan every tick.
    //    Compiled out entirely without the `thread-weighting` feature.
    enqueue_due_maintenance(&pool).await?;
```

**Worker registration / Monitor.** `start` wires one local-clock cron (`0 * * * * *`) as the tick backend plus one `PostgresStorage`-backed worker per job type into an apalis `Monitor`. Duplicate ticks across replicas are harmless because every sweep is watermark-gated. Note that the apalis schema is provisioned only by `migrate` (owner role), never here.

```rust
// crates/bulletin/src/worker.rs:633-661
    Monitor::new()
        .register({
            let pool = pool.clone();
            move |_| {
                WorkerBuilder::new("bulletin-tick")
                    .backend(CronStream::new(schedule.clone()))
                    .data(pool.clone())
                    .build(handle_tick)
            }
        })
        .register({
            let pool = pool.clone();
            move |_| {
                WorkerBuilder::new("bulletin-poll-connection")
                    .backend(PostgresStorage::<PollConnectionJob>::new(&pool))
                    .data(pool.clone())
                    .data(connectors.clone())
                    .build(poll_connection)
            }
        })
        .register({
            let pool = pool.clone();
            move |_| {
                WorkerBuilder::new("bulletin-public-build")
                    .backend(PostgresStorage::<PublicBuildJob>::new(&pool))
                    .data(pool.clone())
                    .build(public_build)
            }
        })
```

### `webhook.rs`

**Purpose:** the HTTP edge (`serve` role) — authenticate a raw delivery, enqueue a `ProcessWebhook` job, return fast.

The axum router exposes liveness (`/health`) and the GitHub catcher, with the body limit lifted to GitHub's own 25 MB ceiling so large-but-valid deliveries aren't 413'd after passing signature verification. Without a webhook secret the catcher fails closed and logs the misconfiguration once at startup.

```rust
// crates/bulletin/src/webhook.rs:26-57
const MAX_WEBHOOK_BODY: usize = 25 * 1024 * 1024;

#[derive(Clone)]
struct WebhookState {
    pool: PgPool,
    github: Arc<GithubWebhook>,
}

/// Builds the `serve` router: liveness (`/health`) + the GitHub webhook catcher
/// (`POST /webhooks/github`). Without a webhook secret the catcher fails closed — every delivery is
/// rejected — and we log it once at startup so the misconfiguration is visible.
pub fn router(pool: PgPool, github_webhook_secret: Option<Vec<u8>>) -> Router {
    if github_webhook_secret.is_none() {
        tracing::warn!(
            "no GitHub webhook secret configured; deliveries to /webhooks/github will be rejected"
        );
    }
    let state = WebhookState {
        pool,
        github: Arc::new(GithubWebhook::new(github_webhook_secret)),
    };
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route(
            "/webhooks/github",
            post(github_webhook).layer(DefaultBodyLimit::max(MAX_WEBHOOK_BODY)),
        )
        .with_state(state)
}
```

The `github_webhook` handler verifies the HMAC over the raw bytes *before* any parse (fail-closed on anything but a valid signature), short-circuits GitHub's `ping`/challenge, then enqueues `ProcessWebhookJob` keyed on the delivery id and returns a quick 2xx — a duplicate delivery even maps to `200 OK` rather than an error.

```rust
// crates/bulletin/src/webhook.rs:77-119
    // Verify over the raw bytes BEFORE any parse — fail closed on anything but a valid signature.
    match state.github.verify(&wh, body.as_ref()) {
        Verified::Authentic => {}
        Verified::Challenge(echo) => return (StatusCode::OK, echo).into_response(),
        Verified::Invalid => {
            tracing::warn!("rejected webhook with invalid signature");
            return (StatusCode::UNAUTHORIZED, "invalid signature").into_response();
        }
    }

    let event_type = wh.event_type.unwrap_or_default();
    // `ping` is GitHub's delivery test — authenticated, but there's nothing to ingest.
    if event_type == "ping" {
        return (StatusCode::OK, "pong").into_response();
    }
    let delivery_id = wh.delivery_id.unwrap_or_default();

    let mut storage = PostgresStorage::<ProcessWebhookJob>::new(&state.pool);
    let mut builder = TaskBuilder::new(ProcessWebhookJob {
        source: SourceKind::Github,
        event_type,
        delivery_id: delivery_id.clone(),
        // A verified GitHub delivery is UTF-8 JSON; `_lossy` only guards a non-conforming sender and
        // never drops the delivery (the job re-parses the JSON anyway).
        body: String::from_utf8_lossy(&body).into_owned(),
    });
    // Collapse re-deliveries of the same X-GitHub-Delivery (GitHub retries on a non-2xx). Skip the
    // key if the header was absent — an empty id would alias every keyless delivery onto one job.
    if !delivery_id.is_empty() {
        builder = builder.with_idempotency_key(format!("gh-webhook:{delivery_id}"));
    }
    let task = builder.build();

    match storage.push_task(task).await {
        Ok(()) => (StatusCode::ACCEPTED, "queued").into_response(),
        Err(e) if is_duplicate_enqueue(&e) => {
            (StatusCode::OK, "duplicate delivery").into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to enqueue webhook job");
            (StatusCode::INTERNAL_SERVER_ERROR, "enqueue failed").into_response()
        }
    }
```

### gRPC admin API (`api/`)

**Purpose:** the `api` role — a tonic gRPC server exposing the admin plane over the wire, mirroring the `debug` commands. It adds no business logic: it authenticates a caller and calls the same self-scoping `core` functions the CLI does.

#### `api/mod.rs`

The server backs two services with one `AdminApi` instance: the wire-stable `AdminService` and the `UnstableDebugService`. Auth is enforced by an interceptor over each *whole* service (one chokepoint, no by-omission gap). It also registers gRPC reflection and marks both services serving on the `grpc.health.v1` probe.

```rust
// crates/bulletin/src/api/mod.rs:45-75
    if admin_key.is_none() {
        tracing::warn!(
            "no API admin key configured; all gRPC admin calls will be rejected \
             (set --api-admin-key or BULLETIN_API_ADMIN_KEY)"
        );
    }
    let auth = Arc::new(AuthState::new(admin_key));
    // One `AdminApi` backs both planes. Auth is enforced by an interceptor over each whole service
    // (so the handlers carry no per-RPC auth check — one chokepoint per service, no by-omission gap).
    // The unstable debug plane is its own service (its instability lives in the name) but runs under
    // the same admin bearer.
    let api = admin::AdminApi::new(pool, email);
    let admin = AdminServiceServer::with_interceptor(api.clone(), admin_interceptor(auth.clone()));
    let debug = UnstableDebugServiceServer::with_interceptor(api, admin_interceptor(auth));

    // gRPC server reflection (v1) so tooling can list services/methods over the wire.
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .context("build gRPC reflection service")?;

    // Standard grpc.health.v1 probe alongside `serve`'s HTTP /health. Mark *both* services serving so a
    // per-service health check (`grpc_health_probe -service bulletin.v1.UnstableDebugService`) doesn't
    // read the live debug plane as down.
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<AdminServiceServer<admin::AdminApi>>()
        .await;
    health_reporter
        .set_serving::<UnstableDebugServiceServer<admin::AdminApi>>()
        .await;
```

#### `api/admin.rs`

`AdminApi` is a cheap-to-clone struct (Arc pool + small email config) that implements both service traits. Each handler is pure "convert → call core → convert" with no scope ceremony (the store fns open their own `ScopeCtx::Admin` transaction). `create_connection` is representative of the stable plane: parse args, run shared validation as `InvalidArgument`, call the store, map DB errors to opaque `Internal`, and project the row back.

```rust
// crates/bulletin/src/api/admin.rs:69-98
    async fn create_connection(
        &self,
        req: Request<proto::CreateConnectionRequest>,
    ) -> Result<Response<proto::Connection>, Status> {
        let r = req.into_inner();
        let owner = match r.owner.as_deref() {
            Some(s) => Some(parse_uuid(s, "owner")?),
            None => None,
        };
        // Shared validation (source, owner guard, github routing key) — same path as `debug`.
        let conn =
            ingest::prepare_connection(&r.source, &r.config_json, r.poll_interval_secs, owner)
                .map_err(Status::invalid_argument)?;
        let id = ingest::store::insert_connection(
            &self.pool,
            conn.source,
            conn.config,
            conn.poll_interval_secs,
            conn.owner,
            conn.provider_account_id.as_deref(),
        )
        .await
        .map_err(error::db("insert connection"))?;

        let row = ingest::store::load_connection(&self.pool, id)
            .await
            .map_err(error::db("load connection"))?
            .ok_or_else(|| Status::internal("created connection not found"))?;
        Ok(Response::new(convert::connection(row)))
    }
```

The unstable plane drives the same engine behind the `debug` commands. `run_digest` shows the send RPCs: they build the mailer server-side from the engine's own config, so the CLI never carries the SMTP credential.

```rust
// crates/bulletin/src/api/admin.rs:273-285
    async fn run_digest(
        &self,
        req: Request<proto::RunDigestRequest>,
    ) -> Result<Response<proto::DigestOutcome>, Status> {
        let subscriber = parse_uuid(&req.into_inner().subscriber, "subscriber")?;
        let sender = self.build_sender()?;
        // A manual operator run is a single shot (attempt 0): if the lead defers, the outcome says so and
        // the operator can re-run; there's no apalis retry behind this RPC.
        let outcome = digest::generate(&self.pool, &sender, subscriber, &self.email.content(), 0)
            .await
            .map_err(error::internal("generate digest"))?;
        Ok(Response::new(convert::digest_outcome(outcome)))
    }
```

#### `api/auth.rs`

**Fail-closed admin auth.** `AuthState` holds the trimmed admin bearer; an unset or blank key means the plane is *not configured* and every RPC is rejected. `require_admin` compares the presented token against the configured key with a constant-time `ct_eq` so a wrong key can't be recovered by timing.

```rust
// crates/bulletin/src/api/auth.rs:21-46
impl AuthState {
    pub fn new(admin_key: Option<String>) -> Self {
        // Trim both the configured key (here) and the presented token (in `bearer`) so a stray
        // trailing newline — common when a key is sourced from a file/env — can't lock the plane out.
        // An empty-after-trim key is treated as "not configured" (fail-closed), never a match.
        let admin_key = admin_key
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty());
        Self { admin_key }
    }

    /// Require a valid admin bearer on the request metadata. `Unauthenticated` if absent, malformed, or
    /// non-matching; the comparison is constant-time so a wrong key can't be recovered by timing.
    pub fn require_admin(&self, md: &MetadataMap) -> Result<(), Status> {
        let configured = self
            .admin_key
            .as_deref()
            .ok_or_else(|| Status::unauthenticated("admin API is not configured"))?;
        let presented = bearer(md)?;
        if ct_eq(presented.as_bytes(), configured.as_bytes()) {
            Ok(())
        } else {
            Err(Status::unauthenticated("invalid admin credentials"))
        }
    }
}
```

The interceptor hoists that check above every method of the wrapped service, so a newly-added RPC cannot ship unauthenticated by omission.

```rust
// crates/bulletin/src/api/auth.rs:51-58
pub fn admin_interceptor(
    auth: Arc<AuthState>,
) -> impl Clone + FnMut(Request<()>) -> Result<Request<()>, Status> {
    move |req: Request<()>| {
        auth.require_admin(req.metadata())?;
        Ok(req)
    }
}
```

#### `api/convert.rs`

**Purpose:** the single file where domain ↔ proto shapes drift, all pure projections (no logic). `explain_row` is a representative projection: it flattens core's keyed `Verdict` enum into the wire's parallel-optional fields, resolves the representative source/title (with sentinels for an empty story), and re-encodes the fused connections.

```rust
// crates/bulletin/src/api/convert.rs:193-228
pub fn explain_row(r: ExplainRow) -> proto::ExplainRow {
    let (verdict, position, rank, drop_cause) = match r.verdict {
        Verdict::Selected { position } => ("SELECTED", Some(position as u64), None, None),
        Verdict::OverCap { rank } => ("OVER_CAP", None, Some(rank as u64), None),
        Verdict::Dropped { cause } => ("DROPPED", None, None, Some(format!("{cause:?}"))),
    };
    let (source, title) = match &r.item {
        Some(item) => (item.source.as_str().to_string(), item.title.clone()),
        None => ("?".to_string(), "<empty story>".to_string()),
    };
    let connections = r
        .item
        .iter()
        .flat_map(|i| i.connections.iter())
        .map(|c| proto::ExplainConnection {
            source: c.source.as_str().to_string(),
            title: c.title.clone(),
            link_reason: c.link_reason.clone(),
        })
        .collect();
    proto::ExplainRow {
        verdict: verdict.to_string(),
        position,
        rank,
        drop_cause,
        last_event_time: Some(ts(r.last_event_time)),
        source,
        story_id: r.story_id.to_string(),
        format: r.reason.format.as_str().to_string(),
        richness: r.reason.richness,
        relevance: r.reason.relevance,
        priority: r.reason.priority,
        title,
        connections,
    }
}
```

#### `api/error.rs`

**Opaque internal mapping.** Any DB or engine failure is logged server-side with its detail and returned to the client as a generic `Internal`, so engine internals never leak over the wire. Caller-facing validation is built inline in handlers as `InvalidArgument`/`NotFound` (the latter also being the RLS "no rows" / IDOR backstop).

```rust
// crates/bulletin/src/api/error.rs:10-22
pub fn db(context: &'static str) -> impl FnOnce(sqlx::Error) -> Status {
    internal(context)
}

/// Like [`db`], but for any displayable error — the `core` engine fns the debug plane drives return
/// `anyhow::Error`, not a bare `sqlx::Error`. Same behavior: log the detail server-side, return an
/// opaque `Internal` so engine internals never leak over the wire.
pub fn internal<E: std::fmt::Display>(context: &'static str) -> impl FnOnce(E) -> Status {
    move |e| {
        tracing::error!(error = %e, "api: {context}");
        Status::internal("internal error")
    }
}
```

### `transport.rs`

**Purpose:** email delivery, runtime side — `EmailConfig` (clap-driven) builds a `Sender` that implements `core::digest::Mailer`.

`EmailConfig` defaults to a local file transport so the pipeline runs end-to-end with no external service. It deliberately omits `#[derive(Debug)]` because `smtp_password` holds a live credential, and its `SmtpTls` enum enforces TLS on both modes so a misconfigured server fails closed rather than leaking the token in cleartext.

```rust
// crates/bulletin/src/transport.rs:89-98
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum SmtpTls {
    /// Connect on 587 in plaintext, then upgrade in-band via STARTTLS. The upgrade is *required* —
    /// lettre aborts before authenticating if the server refuses it, so there's no downgrade.
    Starttls,
    /// TLS-wrapped from the first byte on 465.
    Implicit,
}
```

`build_smtp` requires host/username/password, pins TLS via lettre's `starttls_relay`/`relay` builders, and attaches credentials so the token always travels encrypted.

```rust
// crates/bulletin/src/transport.rs:147-172
    fn build_smtp(&self) -> Result<AsyncSmtpTransport<Tokio1Executor>> {
        let host = self
            .smtp_host
            .as_deref()
            .context("--smtp-host / BULLETIN_SMTP_HOST is required for the smtp transport")?;
        let username = self.smtp_username.as_deref().context(
            "--smtp-username / BULLETIN_SMTP_USERNAME is required for the smtp transport",
        )?;
        let password = self.smtp_password.as_deref().context(
            "--smtp-password / BULLETIN_SMTP_PASSWORD is required for the smtp transport",
        )?;

        let mut builder = match self.smtp_tls {
            SmtpTls::Starttls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(host),
            SmtpTls::Implicit => AsyncSmtpTransport::<Tokio1Executor>::relay(host),
        }
        .with_context(|| format!("configuring TLS for smtp host {host}"))?;

        if let Some(port) = self.smtp_port {
            builder = builder.port(port);
        }

        Ok(builder
            .credentials(Credentials::new(username.to_owned(), password.to_owned()))
            .build())
    }
```

The `Mailer` impl is the seam that lets the core digest flow deliver without knowing about clap, the filesystem, or SMTP.

```rust
// crates/bulletin/src/transport.rs:175-193
impl Mailer for Sender {
    fn from(&self) -> &str {
        &self.from
    }

    async fn send(&self, message: Message) -> Result<()> {
        match &self.transport {
            Transport::File(t) => {
                t.send(message)
                    .await
                    .context("file transport write failed")?;
            }
            Transport::Smtp(t) => {
                t.send(message).await.context("smtp send failed")?;
            }
        }
        Ok(())
    }
}
```

### `secrets.rs`

**Purpose:** credential-at-rest wiring — resolve the app's sealed secrets at startup, plus the offline `bulletin secrets` tools that produce them.

`SecretConfig` is flattened into the top-level CLI so every role shares one source of truth. It carries the base64 master key, the GitHub App id (not secret), the sealed App private key, the sealed webhook secret, and a plaintext webhook-secret dev fallback.

```rust
// crates/bulletin/src/secrets.rs:30-62
#[derive(Args, Clone)]
pub struct SecretConfig {
    /// Base64-encoded 32-byte app master key. Unwraps every sealed secret (the GitHub App key, the
    /// sealed webhook secret). Generate with `bulletin secrets keygen`. Without it, sealed GitHub
    /// credentials can't load and GitHub stays disabled (RSS is unaffected).
    #[arg(long, env = "BULLETIN_MASTER_KEY")]
    master_key: Option<String>,

    /// GitHub App id — **not** a secret (design §3A). Required (with the private key + master key)
    /// to mint installation tokens.
    #[arg(long, env = "BULLETIN_GITHUB_APP_ID")]
    github_app_id: Option<i64>,

    /// Sealed GitHub App **private key** envelope: the base64 output of `bulletin secrets seal` over
    /// the App's RSA PEM. Unsealed once at startup with the master key, then held only as an
    /// in-memory signing key.
    #[arg(long, env = "BULLETIN_GITHUB_APP_PRIVATE_KEY")]
    github_app_private_key: Option<String>,

    /// GitHub REST base URL override (GitHub Enterprise / a test mock). Defaults to api.github.com.
    #[arg(long, env = "BULLETIN_GITHUB_API_BASE")]
    github_api_base: Option<String>,

    /// Sealed webhook signing secret envelope (preferred). Unsealed with the master key and fed to
    /// the edge HMAC verifier.
    #[arg(long, env = "BULLETIN_GITHUB_WEBHOOK_SECRET_SEALED")]
    github_webhook_secret_sealed: Option<String>,

    /// Plaintext webhook signing secret — a dev-only fallback used when no sealed secret is set.
    /// Sealing it (above) is preferred in production.
    #[arg(long, env = "BULLETIN_GITHUB_WEBHOOK_SECRET")]
    github_webhook_secret: Option<String>,
}
```

`webhook_secret()` unseals the sealed form when present (erroring if the master key is missing), else falls back to the plaintext dev value, else returns `None` — leaving the edge catcher to fail closed.

```rust
// crates/bulletin/src/secrets.rs:79-91
    pub fn webhook_secret(&self) -> Result<Option<Vec<u8>>> {
        if let Some(sealed) = &self.github_webhook_secret_sealed {
            let master = self.master_key()?.context(
                "BULLETIN_GITHUB_WEBHOOK_SECRET_SEALED is set but BULLETIN_MASTER_KEY is not",
            )?;
            let opened = unseal_text(&master, sealed).context("unsealing the webhook secret")?;
            return Ok(Some(opened.expose_secret().to_vec()));
        }
        Ok(self
            .github_webhook_secret
            .as_ref()
            .map(|s| s.clone().into_bytes()))
    }
```

`connector_ctx()` is the graceful-degradation seam: with the App id + sealed key + master key all present it unseals the PEM, builds a `GithubApp`, and wires a per-installation token factory (real polling on). Anything missing leaves `github = None` — RSS keeps working, GitHub polls skip — and a partial config warns.

```rust
// crates/bulletin/src/secrets.rs:96-129
    pub fn connector_ctx(&self) -> Result<ConnectorCtx> {
        let (Some(app_id), Some(sealed_key)) =
            (self.github_app_id, self.github_app_private_key.as_deref())
        else {
            if self.github_app_id.is_some() || self.github_app_private_key.is_some() {
                tracing::warn!(
                    "GitHub App partially configured (need both --github-app-id and \
                     --github-app-private-key); GitHub polling stays disabled"
                );
            }
            return Ok(ConnectorCtx::default());
        };
        let master = self
            .master_key()?
            .context("a sealed GitHub App private key is set but BULLETIN_MASTER_KEY is not")?;
        let pem =
            unseal_text(&master, sealed_key).context("unsealing the GitHub App private key")?;
        let base_url = self
            .github_api_base
            .clone()
            .unwrap_or_else(|| DEFAULT_API_BASE.to_string());
        let app = GithubApp::new(base_url.clone(), app_id, pem.expose_secret())
            .map_err(|e| anyhow::anyhow!("{e}"))
            .context("loading the GitHub App credentials")?;
        tracing::info!(app_id, base_url = %base_url, "GitHub App credentials loaded; polling enabled");
        let token_factory =
            Arc::new(move |installation_id: i64| app.installation_tokens(installation_id));
        Ok(ConnectorCtx {
            github: Some(GithubCtx {
                base_url,
                token_factory,
            }),
        })
    }
```

The offline `Seal`/`Keygen` tools need no database. `keygen` prints a fresh master key; `seal` reads a secret from stdin, trims one trailing newline, seals it, and scrubs the plaintext buffer before printing the envelope.

```rust
// crates/bulletin/src/secrets.rs:151-177
pub fn run(cfg: &SecretConfig, command: SecretsCommand) -> Result<()> {
    match command {
        SecretsCommand::Keygen => {
            println!("{}", MasterKey::generate().to_base64());
        }
        SecretsCommand::Seal => {
            let master = cfg
                .master_key()?
                .context("`secrets seal` needs BULLETIN_MASTER_KEY (run `secrets keygen` first)")?;
            let mut buf = Vec::new();
            std::io::stdin()
                .read_to_end(&mut buf)
                .context("reading the secret from stdin")?;
            // Trim one trailing newline (and a CR) so a piped `echo` doesn't fold it into the secret.
            if buf.last() == Some(&b'\n') {
                buf.pop();
                if buf.last() == Some(&b'\r') {
                    buf.pop();
                }
            }
            let sealed = seal(&master, &buf).map_err(|e| anyhow::anyhow!("{e}"))?;
            buf.iter_mut().for_each(|b| *b = 0); // scrub the plaintext buffer
            println!("{}", sealed.encode());
        }
    }
    Ok(())
}
```

### `metric.rs`

**Purpose:** every `bulletin_*` metric in one place — thin recorders over the `metrics` facade, plus `init` which installs the Prometheus exporter the `worker`/`all` roles scrape.

A representative recorder: `job_finished` records both a duration histogram and an outcome counter, keyed by job name.

```rust
// crates/bulletin/src/metric.rs:75-80
pub fn job_finished(job: &'static str, ok: bool, elapsed: Duration) {
    metrics::histogram!("bulletin_job_duration_seconds", "job" => job)
        .record(elapsed.as_secs_f64());
    let outcome = if ok { "ok" } else { "err" };
    metrics::counter!("bulletin_jobs_total", "job" => job, "outcome" => outcome).increment(1);
}
```

`publish_gauges` mirrors the same `status::gather` aggregates `debug status` reads into gauges once per tick, keeping the DB read off the scrape path. The queue section is the truest "stuck worker" signal — depth, in-flight, failures, and oldest-pending age per job type.

```rust
// crates/bulletin/src/metric.rs:174-188
    if let Some(queue) = &r.queue {
        for q in queue {
            metrics::gauge!("bulletin_queue_depth", "job_type" => q.job_type.clone())
                .set(q.pending as f64);
            metrics::gauge!("bulletin_queue_running", "job_type" => q.job_type.clone())
                .set(q.running as f64);
            metrics::gauge!("bulletin_queue_failed", "job_type" => q.job_type.clone())
                .set(q.failed as f64);
            metrics::gauge!("bulletin_queue_killed", "job_type" => q.job_type.clone())
                .set(q.killed as f64);
            let oldest = q.oldest_pending_secs.unwrap_or(0);
            metrics::gauge!("bulletin_queue_oldest_pending_seconds", "job_type" => q.job_type.clone())
                .set(oldest as f64);
        }
    }
```

### `debug.rs`

**Purpose:** the `bulletin debug …` inspection commands — a thin gRPC client of the admin plane (even locally). It opens no DB and builds no mailer; the engine behind the API holds those.

`run` dials the API over plaintext h2, parses the admin key into a bearer, and constructs two clients over one channel — `AdminServiceClient` and `UnstableDebugServiceClient` — each fitted with a bearer-attaching interceptor.

```rust
// crates/bulletin/src/debug.rs:140-167
pub async fn run(
    api_addr: SocketAddr,
    admin_key: Option<String>,
    command: DebugCommand,
) -> Result<()> {
    // Dial the API over plaintext h2 (loopback by default; front with TLS off-box). The bearer is
    // attached by an interceptor so every RPC on this client is authenticated uniformly.
    let channel = Endpoint::from_shared(format!("http://{api_addr}"))
        .context("build API endpoint")?
        .connect()
        .await
        .with_context(|| {
            format!("connect to bulletin api at {api_addr} (is `bulletin api` running?)")
        })?;

    let token: Option<MetadataValue<Ascii>> = match admin_key {
        Some(k) => Some(
            format!("Bearer {}", k.trim())
                .parse()
                .context("--api-admin-key is not a valid bearer token")?,
        ),
        None => None,
    };
    // Two clients over the one channel: the wire-stable `AdminService` and the `UnstableDebugService`.
    // Both present the same admin bearer (attached by the interceptor).
    let mut admin = AdminServiceClient::with_interceptor(channel.clone(), bearer(token.clone()));
    let mut dbg = UnstableDebugServiceClient::with_interceptor(channel, bearer(token));
```

The client-side `bearer` interceptor attaches the admin token to every request; with no key configured it attaches nothing and the server fails the call closed.

```rust
// crates/bulletin/src/debug.rs:518-527
fn bearer(
    token: Option<MetadataValue<Ascii>>,
) -> impl FnMut(Request<()>) -> Result<Request<()>, tonic::Status> + Clone {
    move |mut req: Request<()>| {
        if let Some(t) = &token {
            req.metadata_mut().insert("authorization", t.clone());
        }
        Ok(req)
    }
}
```

### `build.rs`

**Purpose:** compile the gRPC contract into Rust stubs at build time.

It uses **protox** (a pure-Rust protobuf compiler) rather than the system `protoc`, so `cargo build` needs no external toolchain. The resulting `FileDescriptorSet` is both handed to `tonic-prost-build` for codegen and written to `OUT_DIR` so the server can register it for gRPC reflection. Both server *and* client are built, because the binary is both the API server and (via `debug`) a client of it.

```rust
// crates/bulletin/build.rs:12-30
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/bulletin/v1/bulletin.proto";
    let fds = protox::compile([proto], ["proto"])?;

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let descriptor_path = out_dir.join("bulletin_descriptor.bin");
    std::fs::write(&descriptor_path, fds.encode_to_vec())?;

    tonic_prost_build::configure()
        .build_server(true)
        // The binary is both the server (`bulletin api`) and a client of it: `bulletin debug` is a
        // thin gRPC client of the admin plane, so it runs against a remote engine without needing the
        // DB credential or the SMTP secret locally.
        .build_client(true)
        .compile_fds(fds)?;

    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
```

### `proto/bulletin/v1/bulletin.proto`

**Purpose:** the v1 wire contract — the stable `AdminService` and the exempt-from-compat `UnstableDebugService`.

The two services are deliberately separate: the debug plane's instability is encoded in its *name*, so it survives code generation (server trait, client, reflection, and wire path) where a comment would vanish.

```proto
// crates/bulletin/proto/bulletin/v1/bulletin.proto:13-36
service AdminService {
  // Connections (mirrors `debug connection-add / connection-list / connection-rm`).
  rpc ListConnections(ListConnectionsRequest) returns (ListConnectionsResponse);
  rpc CreateConnection(CreateConnectionRequest) returns (Connection);
  rpc DeleteConnection(DeleteConnectionRequest) returns (DeleteConnectionResponse);

  // Subscribers (mirrors `debug subscriber-add / subscriber-list / subscriber-rm`).
  rpc ListSubscribers(ListSubscribersRequest) returns (ListSubscribersResponse);
  rpc CreateSubscriber(CreateSubscriberRequest) returns (Subscriber);
  rpc DeleteSubscriber(DeleteSubscriberRequest) returns (DeleteSubscriberResponse);

  // Subscriptions: the sources (connections) that comprise a subscriber's digest (mirrors
  // `debug subscription-add / subscription-list / subscription-rm`).
  rpc Subscribe(SubscribeRequest) returns (SubscribeResponse);
  rpc Unsubscribe(UnsubscribeRequest) returns (UnsubscribeResponse);
  rpc ListSubscriptions(ListSubscriptionsRequest) returns (ListSubscriptionsResponse);

  // Ops / read (mirrors `debug status / event-list / digest-list`). Digest content is metadata only —
  // the private items live behind the subscriber plane.
  rpc GetStatus(GetStatusRequest) returns (StatusReport);
  rpc ListEvents(ListEventsRequest) returns (ListEventsResponse);
  rpc ListDigests(ListDigestsRequest) returns (ListDigestsResponse);

}
```

```proto
// crates/bulletin/proto/bulletin/v1/bulletin.proto:53-62
service UnstableDebugService {
  rpc RunBuild(RunBuildRequest) returns (RunBuildResponse);
  rpc RunDigest(RunDigestRequest) returns (DigestOutcome);
  rpc DispatchDigest(DispatchDigestRequest) returns (DigestOutcome);
  rpc ExplainDigest(ExplainDigestRequest) returns (ExplainDigestResponse);
  rpc EvalDigest(EvalDigestRequest) returns (EvalDigestResponse);
  rpc GetDigestProvenance(GetDigestProvenanceRequest) returns (GetDigestProvenanceResponse);
  rpc GetConfig(GetConfigRequest) returns (ScoringConfig);
  rpc SetConfig(SetConfigRequest) returns (ScoringConfig);
}
```

Two key messages: `Connection` is the core control-plane entity (id, source, status, JSON config, poll cadence), and `StatusReport` aggregates the whole pipeline's health for the `status` dashboard and gauges.

```proto
// crates/bulletin/proto/bulletin/v1/bulletin.proto:65-75
message Connection {
  string id = 1;                                   // uuid
  string source = 2;                               // rss | github | …
  string status = 3;                               // active | paused | errored | …
  string config_json = 4;                          // the connection's JSON config blob
  int64 poll_interval_secs = 5;
  google.protobuf.Timestamp next_poll_at = 6;
  optional google.protobuf.Timestamp last_polled_at = 7;
  int32 consecutive_failures = 8;
  optional string subscriber_id = 9;               // owning subscriber (absent = global/public source)
}
```

```proto
// crates/bulletin/proto/bulletin/v1/bulletin.proto:138-149
message StatusReport {
  ConnectionStats connections = 1;
  EventStats events = 2;
  BuildStatus build = 3;
  ClusterStats clusters = 4;
  SubscriberStats subscribers = 5;
  DigestStats digests = 6;
  // Apalis queue counts by job type. `queue_initialized` is false when the apalis schema isn't set up
  // yet (run `migrate`), in which case `queue` is empty.
  bool queue_initialized = 7;
  repeated QueueStats queue = 8;
}
```
