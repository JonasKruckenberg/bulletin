# Digest System — Architecture & Design

**Status:** Draft
**Last updated:** 14 June 2026
**Scope:** End-to-end design for v1 (purely scheduled digests), plus forward-compatible decisions that let later features slot in without rework.

**Revision (14 June 2026) — the Thread layer & tiered identity.** A designed-for, deferred layer that turns the aggregator from a *stateless topical filter* into a *stateful model of the user*. The content graph gains a fourth phase — **`Thread`**: the persistent, per-subscriber weave that runs the full height of time (the Acme migration over months; the on-call rotation), of which a `Story` is one moment. Feeding it is a **tiered probabilistic identity** layer (authority-minted ids as the exact backbone; graded, revisable `entity_edge`s above), and **confidence becomes a first-class signal** that flows to rendering (real avatar vs question-mark) and back through feedback. It lands *after* per-subscriber linking and relevance (roadmap M3/M4); everything is schema-additive. Full design: **`digest-thread-layer.md`**. Touchpoints below: §1, §2, §6, §8.3–§8.4, §8.6–§8.7, §9.5, §10.3–§10.4, §11, §13.

**Revision (13 June 2026) — read/write split.** The runtime is reframed as two decoupled pipelines — **materialization** (write side; durable, best-effort) and **projection** (read side; per-subscriber, punctual) — communicating only through durable shared state (CQRS / materialized-view). This supersedes the earlier *chained* `public-build → generate` tick (§3.1) and the hard *window-partition* selection (§9.4): selection is now a **freshness-scored lookback** over a durable event log, "no loss" is a **durability** guarantee on that log rather than a property of window partitioning, and missed boundaries **coalesce** into one catch-up digest. Cadence is **daily/weekly** at a local time (monthly dropped). Stories are the **per-subscriber read-model** at the head of the projection side.

---

## 1. Product thesis

The job is to *suppress noise and elevate the few things that matter* — in a way the user **trusts**. Two consequences shape the whole design:

- **Draw connections across sources.** Surface things the user would have missed because the signal was split across GitHub, Slack, email, RSS, etc. with no obvious link between them.
- **Weave events into the threads of a user's life.** Beyond one cross-source *happening*, the persistent things a life is made of — projects, roles, relationships — span *many* happenings over *time*. **`Thread`s** (§8.6) model these as durable, per-subscriber state, so relevance becomes "does this advance a thread you've invested in?" rather than "does this match a keyword?" — a designed-for, deferred layer (`digest-thread-layer.md`).
- **Earn trust through transparency and control.** A filter that hides things is only useful if the user can see *why* a decision was made, drill into the data behind it, and correct it — *including its own uncertainty*: identity/link confidence is rendered (a guaranteed person vs a question mark, §10.4), and the rendered doubt is itself the correction affordance.

Everything in a **Digest** has cleared the same per-user **relevance** bar. Within that set, each item renders as either a **Story** (rich and expandable: many related events, or one substantive item like a followed author's new article) or a **Note** (compact: relevant but atomic — a followed library's release, an album drop). Story vs Note is about *how much material backs the item*, not how much it matters; a Note can outrank a Story in priority. Digests are delivered on a configurable recurrence — **daily or weekly at a chosen local time** (e.g. "weekly, Tuesdays at 17:00"), in the subscriber's timezone.

---

## 2. Scope

**v1 (this doc):**
- Purely scheduled digests. No live/real-time feed.
- Sources: RSS/podcasts, GitHub, plus **Slack** as the one private source — to exercise both visibility scopes and the signal→hydrate path.
- Single delivery channel (email, as notification + authenticated deep-link).
- Deterministic grouping (global for public, per-subscriber for private) + **structured, per-subscriber** cross-source linking (shared entity / shared link / temporal). Embedding-based semantic linking is an additive upgrade, not v1.
- Relevance-led scoring (gate + priority) with hand-tuned floor/thresholds. `confidence` and `velocity` are **deferred** (every v1 source is deterministic and retrospective, so both would be near-constant).
- Modular monolith on a single Postgres (8-table domain model + a library-managed job queue, §6).

**Explicitly deferred:**
- Real-time in-app feed.
- Embedding/ANN semantic linking (designed for; not built).
- Teams / shared group digests (data model is forward-compatible).
- LLM summarization (has security/consent implications — see §12).
- Learned (vs hand-tuned) scoring.
- Twitter/X (cost — see §7 matrix).
- Gmail / email (cost — the `gmail.readonly` restricted scope requires annual Google verification + a CASA security assessment, commonly $15k–$75k/yr; **Slack is the v1 private source instead** — see §7 matrix).
- Forward-looking (prospective) events — calendar, delivery ETAs, later ML-predicted patterns. **Deferred including the model support** (future-valued `event_time`, the signed-gap salience curve, `confidence`); v1 events are all retrospective and the priority time-term is plain recency decay (§8.5). Forward-compatible: re-add without schema rework.
- A **shared public-story cache** — v1 derives every story per-subscriber (§4, §6); memoizing pure-public stories across subscribers is the scale optimization (split-trigger: per-subscriber linking cost / embeddings).
- **The Thread layer & tiered identity** (`digest-thread-layer.md`) — the persistent per-subscriber `Thread` weave (§8.6), the probabilistic `entity_edge` identity graph (§8.7), and confidence-as-rendered-signal (§10.4). **Designed-for, deferred** to *after* linking + relevance (roadmap M3/M4); schema-additive, one new background job (`thread_maintenance`) off the punctual path. v1 entities stay freeform `Vec<String>` matched exactly; v1 relevance is flat per-entity affinity with no thread memory.
- Scale-only persistence (event partitioning, dedup gate, normalized blocking signals, normalized `story_cluster` membership, multi-channel delivery table) — see split-triggers in §6.

---

## 3. Architecture overview

Three logical layers:

- **Ingress** — interfaces with every source (push + pull), normalizes everything into a canonical event.
- **Aggregation** — dedupes, groups, links, scores, and selects events into stories/notes. *(Deferred extension: weaves stories into persistent per-subscriber `Thread`s — §8.6 / `digest-thread-layer.md`.)*
- **Generation** — produces and delivers per-subscriber digests on schedule.

```
 SOURCES
  push (webhooks)            pull (pollers / CDC, cursors)
        │                            │
        └──────────────┬─────────────┘
        ┌──────────────▼──────────────┐
        │        INGRESS LAYER         │
        │  receive · auth · fast-ack   │
        │  normalize → Canonical Event │
        │  dedupe (fingerprint) · DLQ  │
        └──────────────┬──────────────┘
                       │  canonical events (Postgres)
        ┌──────────────▼──────────────┐
        │      AGGREGATION LAYER       │
        │  group → cluster rollups     │   ← global for public; per-subscriber for private
        │  link → gate → rank → classify│   ← per-subscriber (stories)
        └──────────────┬──────────────┘
                       │  clusters + per-subscriber selection
        ┌──────────────▼──────────────┐
        │      GENERATION LAYER        │
        │  per-subscriber schedule →   │
        │  select → render → deliver   │
        └──────────────┬──────────────┘
                       ▼
                    email (deep-link)

  Subscriber/preferences → feeds relevance (aggregation) + schedule (generation)
  Postgres = system of record (events · clusters · digests · feedback · jobs)
```

### 3.0 The read/write split (CQRS / materialized-view)

The functional layers above cross-cut a second, load-bearing decomposition that organizes the runtime:

- **Materialization (write side).** Ingress + the *grouping* half of Aggregation: ingest → dedupe → group events into `cluster` rollups (public shared, private per-subscriber). Runs on the **world's clock** (sources + poll cadence), is **best-effort** under load, and everything it writes is a **recomputable cache over the durable event log**. Its one hard guarantee is **durability** — never lose an event (§5, §9.3).
- **Projection (read side).** The *linking* half of Aggregation + Generation: at each subscriber's scheduled instant, take a **snapshot** of the cluster caches and project it into a digest — link → stories → gate → rank → classify → cap → render → deliver. Runs on the **subscriber's wall clock**, is **punctual**, and is a **pure function of the snapshot**.

The two sides share nothing but durable state: materialization *pushes* into it, projection *pulls a snapshot* on schedule. They never call each other (no job chaining) — which is what lets a digest fire on time regardless of how far materialization has caught up. **Stories live at the head of the read side** (§8.2, §9): a `story` is a per-subscriber **read-model** rebuilt at fire time from the snapshot, with stable forwarded ids. The shared-global / per-subscriber split (§11) is a *third*, orthogonal axis: materialization has both (public clusters shared, private per-subscriber), and the deferred *shared public-story cache* (§2, §6) is exactly the move of promoting the shareable slice of projection back into materialization.

**The diagram, mapped to the split.** The read/write seam cuts *through* the Aggregation layer: `group → cluster rollups` is materialization (write); `link → gate → rank → classify` onward — and all of Generation — is projection (read). The labeled inter-layer arrows (`canonical events`, `clusters`) are the durable seam itself.

**Data flow & the durable seam.** Exactly three durable artifacts carry data across the split, all in Postgres:
1. the **event log** — appended by ingest; the trust boundary (§5, §9.3);
2. the **cluster cache** (`cluster` rollups) — upserted by build, read by projection; the *entire* interface between the two sides;
3. **`digest` + `digest_item`** — written by projection; the delivered record (the per-subscriber `story` read-model is rebuilt alongside it).

Materialization only ever *appends to the log and upserts cluster rollups*. Projection only ever *reads a **consistent snapshot** of clusters* (a single MVCC read taken at the fire instant) *and writes its own `story`/`digest` rows*. Neither side reads the other's in-flight state, and there is no callback or join between them — the cluster cache is the whole contract.

### 3.1 Runtime topology

Only two things run continuously: **Postgres** and a thin **webhook catcher**. Everything else is cron/queue-triggered batch. The catcher verifies the signature, **enqueues a `process_webhook` job** (raw payload inline), and returns 2xx in milliseconds — never processing inline, because webhook senders disable or back off slow endpoints.

The pipeline is **two decoupled pipelines** sharing only durable state — not one chained tick:

```
MATERIALIZATION (write side · world's clock · best-effort · durable)
  ingest        drain process_webhook jobs + run due pollers → canonical events (deduped)   [TRUST BOUNDARY]
  public-build  group public events → public cluster rollups                ← global, shared
  private-build group a subscriber's private events → private cluster rollups ← per-subscriber
        (public-build runs as public events arrive; private-build is write-side too, but v1 runs it
         just-in-time at the head of a fire — tech §2. Neither ever blocks projection.)

PROJECTION (read side · subscriber's wall clock · punctual · pure over a snapshot)
  generate, for each subscriber DUE now (in their RLS context):
    pre-select   blocking-seeded candidate clusters (public ∪ own private), lookback-bounded
    link         connected-components → per-subscriber STORIES (stable ids)
    gate · rank · classify · cap · render · deliver · advance next_run_at
```

The job queue is apalis's Postgres-backed schema, drained with `SELECT … FOR UPDATE SKIP LOCKED` — no Kafka, no stream processor. (Queue/runtime mapping: technical architecture doc §2.)

**`public-build` is shared; `private-build` and everything in projection are per-subscriber.** Build and generate are **decoupled**: a digest reads the *latest materialized snapshot*, not "clusters built in this tick." An event still being ingested or clustered when a digest fires simply isn't a candidate *that* fire and rides the next one — never lost, because the event log is durable and the **consideration floor** always reaches back to the last delivery (§9.4). **Linking is not a global precompute:** it runs per subscriber at generate-time, because a story can fuse public clusters with that subscriber's *own* private clusters (§4). Note the best-effort gap is asymmetric: a subscriber's own *ingested* private events are always current in their digest (private-build runs just-in-time at the fire), so only **shared public** clustering — and any event not yet drained from the ingest queue — is what can lag.

**Two independent clocks — the organizing principle (§3.0).** Materialization runs on the world's clock (per-connection poll cadence, minutes) so clusters are *fresh*; projection runs on each subscriber's cadence (daily/weekly, at their local time). Under load materialization falls *behind*, never *wrong*; the digest still fires on time, with best-effort-fresh contents. Polling frequency is never tied to the digest schedule.

**Key timing decision: hydrate at process-time, not receipt-time.** Source APIs are called for current content when the tick drains the webhook jobs, not when the webhook lands. This sidesteps sparse/out-of-order webhooks (e.g. Notion): you always fetch latest state, so stale or reordered signals don't matter.

---

## 4. Core concept: visibility scopes

Every event carries a mandatory `visibility_scope`, modeled as `scope_kind` + `scope_subscriber_id`:

- **public** — the shared global scope. RSS + public social. Public **grouping** (clusters + rollups) is the shared first phase that amortizes work across all users.
- **private** (`scope_subscriber_id` set) — private to one subscriber. Email, Slack DMs, private Notion/GitHub.

**Grouping runs per scope; linking runs per subscriber.** A private event only ever joins a *cluster* in its own scope. But **stories are built per subscriber**, and a subscriber's linking may fuse public clusters with their *own* private clusters into one story — the cross-source value the product is built on (a private Slack incident thread linked to the public CVE advisory). The rule is directional, not a ban:

> **Information flows public → private only.** A story is always **Private-scoped, owned by one subscriber**; public clusters are **read-only inputs** to it and are never mutated by private data. No private datum ever enters the shared public pool or another subscriber's view.

A subscriber's digest draws from `public clusters ∪ their own private clusters`, linked into stories that live entirely in their own scope. (Pure-public stories are re-derived per subscriber in v1; the deferred **shared public-story cache** memoizes them across subscribers.)

**Teams** slot in as additional scopes shared by their members — no schema rework.

### Subscriber abstraction

A `subscriber` owns connections, preferences, and digests. `kind = user | team`. In v1 every user has exactly one personal subscriber (1:1). A team is later just another subscriber with a membership table. Everything per-user hangs off `subscriber_id`.

---

## 5. Canonical event model

The contract that lets aggregation and generation stay source-agnostic.

| Field | Purpose |
|---|---|
| `id` | internal id (UUIDv7, time-ordered) |
| `fingerprint` | dedup key = `hash(source_type, stable_source_id)`; enforced by `UNIQUE`. Does **not** fold content in, so an edit/re-poll of the same item collapses (no spurious re-surface, §9.4) |
| `source_type` | which connector produced it |
| `scope_kind` / `scope_subscriber_id` | visibility scope (mandatory; `scope_subscriber_id` null iff public) |
| `event_time` / `ingest_time` | when the thing occurs vs when we saw it. v1 events are retrospective (`event_time ≤ ingest_time`); prospective `event_time` deferred (§8.5) |
| `entities[]` | people, orgs, repos, ticket ids, **URLs/domains** |
| `content_kind` | adapter-declared depth signal: `longform` \| `announcement` \| `message` (§8.3) |
| `group_key` | deterministic within-source grouping key (adapter-computed); cluster membership is *derived* by `(scope, source, group_key)` — no stored `cluster_id` |
| `title` / `body` | content |
| `links[]` | backing data — required for provenance/timelines |
| `severity_hint` | optional source-provided importance (priority input) |
| `raw` | raw payload, stored **inline** (Postgres TOAST keeps it out-of-line, so it never bloats hot scans). Object-storage offload (`raw_ref`) deferred (§11). Most PII-heavy field → encrypt + retention (§12) |

---

## 6. Data model

**Principle:** split hot small tables from the cold big table, collapse anything 1:1 or derivable, and defer any table whose only justification is scale. The `event` table is large but *cold* for the per-subscriber fan-out — selection never scans it; it reads small **story** rows (the per-subscriber read-model, rebuilt at fire time) with cached signals. Only grouping/rollup, linking, and drill-down touch `event`/`cluster`.

Eight domain tables in three groups, plus the library-provided work queue:

**Per-subscriber config**
- **`subscriber`** — `kind`, delivery prefs (**recurrence**: `freq` daily|weekly, `at_time` local time-of-day, `timezone`, `on_weekday` for weekly; `max_stories`, `max_notes`, channels, quiet hours), plus `filters` jsonb (sources, mutes, keywords) and `affinity` jsonb (relevance weights, updated by feedback).
- **`connection`** — a linked source: owning `subscriber_id`, `source_type`, `creds_ref` (KMS reference, **not** the secret), `config`, poll `cursor`, `status`. Declares whether its events are public or private.

**Shared content graph** (3 phases: dedup → group → aggregate; append-only, only upward FKs). *(Deferred 4th phase: per-subscriber `Thread`s weave stories across time — adds `thread` + `entity_edge` tables, `canonical_entities` on `cluster`/`story`, and `story.thread_id`; all additive, see `digest-thread-layer.md` §8.)*
- **`event`** — canonical event (§5). `UNIQUE(fingerprint)` for dedup. Unpartitioned in v1.
- **`cluster`** — a within-source group: events sharing `(scope, source, group_key)` (one PR, one Slack thread). **Public clusters are shared** (materialization side; rebuilt as public events arrive, decoupled from generate); **private clusters are per-subscriber**. Carries a build-maintained rollup (`event_count`, `max_severity`, `content_depth`, `entities`). Membership is by `group_key` — no per-event pointer. Carries **no `story_id`** (a public cluster belongs to many subscribers' stories).
- **`story`** — the **per-subscriber** cross-source aggregation a digest references: a set of linked clusters, **owned by one subscriber** (`subscriber_id`). Holds members as `clusters` jsonb (`[{cluster_id, link_reason}]`) plus **cached signals** (`event_count`, `source_diversity`, `content_depth`, `max_severity`, `entities`). Stable `id`; `merged_into` forwards a retro-merged story to its survivor (§8.2).

**Digest / feedback machinery**
- **`digest`** — `subscriber_id`, `window_end` (the scheduled boundary that fired), status machine (`pending→built→rendered→delivered`), `idempotency_key`, `sent_at`. `UNIQUE(subscriber_id, window_end)` makes generation idempotent. No `window_start`: selection is a lookback, not a partition (§9.4).
- **`digest_item`** — selected items: `story_id`, `kind` (story|note), `rank`, `score`, `reasons` jsonb, `story_last_event_time` (snapshot powering the **recently-surfaced** damping — §9.4). PK `(digest_id, story_id)`.
- **`feedback`** — append-only log: `target_type`/`target_id`, `kind` (care_more | care_less | wrong_aggregation), `payload`. Processed async.

**Work queue (library-provided)**
- Apalis's Postgres-backed schema: `kind` (`poll_connection` | `process_webhook` | `public_build` | `generate_digest`), payload, `run_after`, status, attempts. Drained via `FOR UPDATE SKIP LOCKED`. See technical architecture doc §2.

### Deliberate denormalizations (and when to split them back out)

| Combined form | Rather than | Why it's safe | Split out when… |
|---|---|---|---|
| cached signal rollups on `cluster` + `story` | a normalized `cluster_signal` table + inverted `cluster_entity` index | rollups are cheap to recompute at v1 scale; `entities` jsonb doubles as the blocking source | blocking/aggregation gets slow → normalize signals + add an inverted `cluster_entity` index |
| `story.clusters` jsonb (`[{cluster_id, link_reason}]`) | a normalized `story_cluster(story_id, cluster_id, link_reason)` table | a story has few clusters; recompute overwrites one array | the **shared public-story cache** is built, or per-membership attributes / fast cross-subscriber reverse lookups are needed → normalize to `story_cluster` + `GIN(clusters)` |
| one unpartitioned `event` + `UNIQUE(fingerprint)` | partitioned `event` + a separate dedup gate | a separate gate is only needed because a partition key must sit inside the unique constraint | event count nears high tens of millions → range-partition by `ingest_time` (BRIN + prune) and add the dedup gate |
| columns/jsonb on `subscriber` | separate `subscription` / `preference` tables | scalar 1:1 prefs; filters + affinity as jsonb | affinity becomes a learned model → normalize `affinity` |
| columns on `digest` | a separate `delivery` table | v1 is email-only | a second channel is added |
| no membership table | a `subscriber_member` table | teams are a later feature | teams are built |

### The cluster & story tables

```sql
CREATE TABLE story (                                    -- phase 3: the per-subscriber cross-source unit
  id                  uuid PRIMARY KEY DEFAULT uuidv7(),
  subscriber_id       uuid NOT NULL REFERENCES subscriber(id),  -- owner; story is always Private-scoped (§4)
  merged_into         uuid REFERENCES story(id),        -- null normally; set when a retro-merge forwards this id (§8.2)
  clusters            jsonb NOT NULL DEFAULT '[]',       -- membership: [{cluster_id, link_reason}] (public + own private)
  -- cross-source rollup (aggregate of the story's clusters; read by selection):
  event_count         int  NOT NULL DEFAULT 0,         -- Σ cluster.event_count    (breadth)
  source_diversity    int  NOT NULL DEFAULT 0,         -- distinct cluster.source   (breadth)
  content_depth       smallint,                        -- max over clusters         (depth)
  max_severity        smallint,                        -- priority input
  entities            jsonb NOT NULL DEFAULT '[]',     -- rollup; blocking target
  first_event_time    timestamptz,
  last_event_time     timestamptz
);
-- selection hot path: this subscriber's stories by recency
CREATE INDEX story_candidates ON story (subscriber_id, last_event_time DESC);
CREATE INDEX story_merged     ON story (merged_into) WHERE merged_into IS NOT NULL;

CREATE TABLE cluster (                                  -- phase 2: a within-source group
  id                  uuid PRIMARY KEY DEFAULT uuidv7(),
  scope_kind          text NOT NULL CHECK (scope_kind IN ('public','private')),
  scope_subscriber_id uuid,                            -- null iff public; set iff private (one tenant)
  source_type         text NOT NULL,
  group_key           text NOT NULL,                   -- deterministic within-source key
  -- rollup of this group's events (build-maintained cache):
  event_count         int  NOT NULL DEFAULT 0,
  max_severity        smallint,
  content_depth       smallint,                        -- max(content_kind) over events
  entities            jsonb NOT NULL DEFAULT '[]',     -- rollup; blocking source
  first_event_time    timestamptz,
  last_event_time     timestamptz
);
-- one cluster per within-source group (public shared; private per-subscriber):
CREATE UNIQUE INDEX one_cluster_per_group ON cluster
  (scope_kind, scope_subscriber_id, source_type, group_key);
-- blocking + per-subscriber pre-select (shared-entity candidate lookup):
CREATE INDEX cluster_entities   ON cluster USING gin (entities);
CREATE INDEX cluster_candidates ON cluster
  (scope_kind, scope_subscriber_id, last_event_time DESC);
```

Grouping puts events into a `cluster` by `(scope, source, group_key)` — public clusters shared, private clusters per-subscriber. **Linking** runs per subscriber (§8.2): over their candidate clusters (public ∪ own private) it computes connected components and writes one `story` per component, recording each member in `clusters`. Events carry no `cluster_id` (a cluster's events are those sharing its `(scope, source, group_key)`); a story's events are reached via `clusters` → each cluster's group_key → events. **Selection** scans `story` rows only, reading cached signals — never the `event` table. Richness and priority (§8.3) are *derived* from the cached inputs, not stored. Events and clusters are **append-only**; stories are a **per-subscriber recomputed cache** whose stable ids are forwarded across rebuilds (`merged_into`, §8.2).

### Honest costs

Blocking unnests jsonb instead of joining a normalized table (fine at this scale, slower at large scale); affinity-as-jsonb is harder to evolve into a learned model; **pure-public stories are re-derived per subscriber** (cheap while linking is structured SQL; the *shared public-story cache* reclaims the amortization when embeddings/scale arrive); and you'll do one partitioning migration later instead of zero.

---

## 7. Ingress layer

### 7.1 Connector SDK contract

Every `Connector` implements one interface so aggregation never learns a source exists. A **`Connector`** is the cross-tenant adapter (one per source kind) that `connect`s credentials into a per-tenant **`Connection`** (one per `connection` row). The SDK is **two layers** — a pull *foundation* every connection has, and an optional *realtime* layer on top — because **polling is correctness and webhooks are a latency/quota layer**. Webhooks are at-most-once (lossy), so a cursor-driven reconciliation poll is the only thing that *guarantees* completeness; realtime intake just makes things fresher and lets the poll interval relax.

```
Connection (foundation — every connector's worker)
  poll(cursor)        -> (items[], next_cursor)   # the reliable path; conditional GET / since-token
  to_events(item)     -> EventContent[]           # set entities, content_kind, group_key, links

Realtime layer (webhook connectors only)
  verify(hdrs, body)  -> authentic? | challenge   # on the Connector (app-level); HMAC over raw bytes, no parse
  accept_webhook(body)-> items[] | lifecycle      # parse a verified body into units
  hydrate(item)       -> item                      # delayed fetch of latest state; no-op if complete
```

**`to_events` sets content only** — `entities`, `content_kind`, `group_key`, `links`. It does **not** set `scope` or `fingerprint`; those are stamped by the SDK's `finalize` step from the `connection` row (so no adapter can place an event in the wrong scope or botch the dedup key — §12 risk #1).

**Why `hydrate` is realtime-only.** Delayed fetch only has meaning where receipt and processing are split in time — i.e. webhooks. `poll` fetches complete units synchronously, so a pull source never hydrates. v1: RSS is `Connection`-only; GitHub and Slack are `Connection + RealtimeConnection` — their poll is the **reconciliation backstop** for lossy webhooks, and their `hydrate` is identity. Notion and firehose refs are where a non-identity hydrate earns its keep — the **signal → hydrate** pattern.

`to_events` also classifies each event's `content_kind` (`longform` / `announcement` / `message`), because the adapter is where source semantics live — a GitHub release is an announcement, an RSS item is longform, a Slack event is a message. This is the depth signal for Story/Note classification (§8.4); deriving it downstream from body text would collapse into a gameable length heuristic.

### 7.2 Cross-cutting (handled by SDK, not each connector)

- **Idempotency** — `UNIQUE(fingerprint)` + `INSERT … ON CONFLICT DO NOTHING`. Load-bearing because webhook + polling fallback deliver the same item twice; the fingerprint collapses them, making reconciliation polling safe.
- **Cursors** persisted per connection; **rate-limit/backoff** per source; **DLQ** holding raw payload for replay.
- **Scope assignment** — stamped by the SDK's `finalize` from the `connection` row, **never by the adapter** — so no connector can place an event in the wrong scope (§12 risk #1).
- **Reconciliation is the foundation** — every webhook source *also* polls; the realtime layer is the optimization on top. A dropped webhook delivery is recovered at the next reconciliation poll, `UNIQUE(fingerprint)` collapsing the overlap.

### 7.3 Webhook catcher (always-on, dumb)

HMAC-verify over raw bytes (constant-time, app-level secret) → drop unverified → **enqueue a `process_webhook` job with the raw body** → return 2xx fast. The catcher does **no content parse and no connection resolution**: routing, parsing, hydration, and the events/lifecycle fork all run in the job at process-time (§3.1). Authentic-but-unmatched deliveries drop in the job, not at the edge — bounded, since HMAC already proved the sender.

**Secret scope.** GitHub App and Slack app each expose **one app-level webhook URL with one app-level signing secret**. Verification uses the **app-level** secret; per-connection isolation comes from routing on the verified payload id (GitHub `installation.id`, Slack `team_id`) to our `connection` row — never trust a payload-supplied id (IDOR). Per-connection signing secrets apply only to self-created webhooks (Stripe, classic repo hooks). See technical architecture doc §3A.

### 7.4 Source support matrix

| Source | Push? | Pull? | v1 approach |
|---|---|---|---|
| RSS / podcasts | no | yes | poll w/ conditional GET; cursor = last GUID |
| GitHub | yes (webhooks) | yes | webhook primary, REST/GraphQL backfill |
| Notion | yes, **sparse** | yes | webhook = signal → hydrate via API; poll fallback |
| Email (Gmail) | yes (watch→Pub/Sub) | yes | **deferred** — `gmail.readonly` restricted scope → annual Google verification + CASA assessment (commonly $15k–$75k/yr); Slack is the v1 private source |
| Slack | yes (Events API) | yes | Events API (HTTP or socket mode) |
| Bluesky | yes (Jetstream) | yes | Jetstream JSON firehose for streams; per-user feed via App View polling (`atrium-api`) |
| Mastodon | yes (streaming) | yes | user/home stream or REST poll; user token required since 4.2.0 |
| Twitter/X | paid only | paid only | **deferred** — pay-per-use ~$0.005/read (Basic/Pro legacy-only), enterprise ~$42k/mo |

---

## 8. Aggregation layer

### 8.1 Two clustering jobs: grouping vs linking

**Grouping** — within-source, deterministic, exact. "Is this the same thread/PR/page/article?" The `group_key` is computed by the adapter from source-native ids and treated as opaque downstream:

```
GitHub:   (repo_id, pr_number) | (repo_id, release_tag)
Slack:    (channel_id, thread_ts)
Email:    Gmail thread id / RFC822 References
Notion:   page_id
RSS:      article GUID
Bluesky:  thread root URI
```

Equality grouping — indexable, no ML, high precision. These are the **atoms**.

**Linking** — cross-source, cross-group, semantic, **per subscriber**. The headline feature: connecting a (public) CVE advisory + a private GitHub incident PR + a private Slack flurry about the same thing, with no shared natural key. Operates on **clusters, not raw events** (one representative per group → far fewer items) and over the subscriber's candidate set only — public ∪ their own private clusters (§4). The result is a per-subscriber `story` with its member clusters.

### 8.2 Semantic linking mechanism

1. **Representation** — each cluster has a representative text and a structured feature set (rolled-up `entities`, time span, source set).
2. **Blocking** (the O(n²) guard) — only compare clusters sharing a cheap block: a shared named entity, temporal proximity, or — strongest and cheapest — a **shared link target** (two sources referencing the same URL/CVE/PR is a near-certain connection). The per-subscriber candidate set is **blocking-seeded**: public clusters matching the subscriber's affinity *plus* public clusters that share a strong key (URL/CVE/native id) with any of the subscriber's private clusters — otherwise the exact cross-boundary connection you want gets filtered out before linking. In v1 this unnests the rolled-up `entities` jsonb (`GIN(cluster.entities)`); at scale it moves to a normalized `cluster_signal` self-join.
3. **Similarity** on candidate pairs only — weighted: entity Jaccard + temporal closeness + shared-link boost (+ embedding cosine later). Above threshold → an **edge** between the two clusters, tagged with its `link_reason`.
4. **Decision** — **deterministic recompute, not incremental sticky assignment.** Each generate, take the subscriber's candidate clusters and their edges and compute **connected components** (union-find). Each component is a story. This is *order-independent* (no arrival-order bias) and lets late retro-connections form automatically.
   - **Stable ids via forwarding.** Map components → story ids from the *prior* assignment: a component keeps the id its clusters mostly carried; a genuinely new component spawns one; when two already-delivered stories land in one component (a retro-merge), the **oldest id wins** and the loser gets `merged_into = survivor`. Ids stay stable for the user even though membership is recomputed.
   - **Asymmetric thresholds guard against single-linkage blobs.** Only **strong** edges (shared URL/CVE/native id) may *merge two already-delivered stories*; weak entity-overlap can attach a fresh cluster but never collapse established stories. (Threshold tuning: §15.)

**v1 vs later:** structured signals (shared entity / link / temporal) are pure SQL + entity extraction and deliver most of the "connections you missed" value with no model. Embeddings only add the "same meaning, no shared token" tail, and slot in as a `vector` column on the `story` with an HNSW index + one more blocking source — schema-additive.

**Entity extraction (v1):** mostly free from structure (GitHub repos/users, Slack channels, email addresses) + URL/domain extraction + light NER on titles. URLs and native ids carry the load.

### 8.3 Scoring: relevance gates, richness classifies

Three signals (a fourth, `confidence`, is deferred — §2/§8.5). Volume never gates inclusion — folding it into relevance would bury high-relevance/low-volume items like a single album drop.

**Relevance** (per-user) — *do you care?* Subscription match (hard filter), entity affinity (tracked repos/people/keywords), scope bonus (own private content), mutes as hard zeros. Relevance **gates** inclusion (a single `relevance_floor`) and is the primary ranking key. *(Deferred — the Thread term, §8.6: relevance also sums the precomputed weight of the active threads a story advances, so a story whose individual pieces each fall below the floor still clears it by advancing a thread you've invested in — the rescue for the missed-because-split case.)*

**Richness** (global) — *how much material backs the cluster?* `richness = combine(breadth, depth)`:
- **breadth** = f(`event_count`, `source_diversity`)
- **depth** = from `content_depth` (max `content_kind` over the cluster's events)

Richness does **not** gate; it only decides rendering format (§8.4). It is derived from cached inputs, not stored.

**Priority** (per-user, for ordering and caps) — relevance-led, boosted by `max_severity`, and aged by **recency decay** over `now − last_event_time` (applied at read time). v1 is retrospective only; the generalization to a signed-gap salience curve that also handles future-valued items is deferred (§8.5). *(Deferred — two ML-free terms arrive with Threads, §8.6: **corroboration**, where `source_diversity` feeds priority and not only richness/format — independent sources lighting up is importance, not a render hint — and **reactivation/novelty**, a bump when a story lands on a dormant thread or a thread spikes over its baseline rate.)*

**Confidence** (global) — *deferred.* Every v1 source is deterministic, so confidence would be a near-constant. Forward-compatible (§8.5): when estimate/prediction sources land, confidence is adapter-assigned per event, rolled up per cluster and **attenuated by link strength**, and **modulates priority** without gating. Re-adds as a column + a multiplier.

### 8.4 Selection: gate, rank, classify

1. **Gate** — include a story only if `relevance ≥ relevance_floor`. One bar for both tiers.
2. **Rank** — order included clusters by priority.
3. **Classify format** (rendering, not importance):
   - **Story** if richness is high — multi-event OR multi-source OR a substantive single item (`content_kind = longform`).
   - **Note** otherwise — relevant but atomic/thin.
4. **Cap** by priority rank: Stories cap at N (~3–5), Notes at M (~15–25). A Note is never dropped *for being a Note*, only for losing the priority race. *(Deferred — a **thread-diversity cap** (§8.6) bounds stories-per-thread (≈ ≤2) so one busy thread can't monopolize the digest, forcing breadth across the user's life.)*
5. **Order** the digest by priority with format per item — a high-priority Note can sit above a lower-priority Story (**format ≠ importance**).

The selection function should be a **pure function over precomputed features** — testable against fixtures, swappable. The `relevance_floor`, richness threshold, and caps live in a config table in v1.

### 8.5 Forward-looking (prospective) events — deferred

v1 events are all **retrospective** (`event_time ≤ ingest_time`). The model is forward-compatible — when prospective sources land, none of this needs schema rework:

- **Tense is derived, not stored.** An item is prospective iff `event_time > now`, evaluated at read time.
- **One salience curve scores both.** Priority's time term generalizes to `salience(Δ)` over the *signed* gap `Δ = last_event_time − now`: low far in the future, rising as the moment approaches, peaking at `Δ = 0`, decaying once past. A passed item then fades by the same decay — no `resolves_at`, no retire job.
- **Windowing** extends the candidate **upper** bound to `now + lookahead` (a single global knob); v1's candidate set is a freshness-scored **lookback** ending at the fire instant (§9.4).
- **`confidence`** (§8.3) carries a prospective item's certainty (calendar high, ETA medium, ML low). An ML predictor is then just another connector emitting prospective, low-confidence events.
- **Recurrence (RRULE)** expands lazily inside the lookahead horizon against the subscriber's wall clock (DST-correct; technical architecture doc §13).

### 8.6 Threads — the persistent weave  *(DESIGNED — deferred; full design in `digest-thread-layer.md`)*

A **`Thread`** is the fourth phase of the content graph (`Event → Cluster → Story → Thread`): the persistent, per-subscriber weave that runs the *full height of time*, of which a `Story` is one moment. It is what turns the aggregator from a stateless topical filter into a **stateful model of the user** — the projects, roles, and relationships a life is made of, each spanning many stories over weeks and months.

Three load-bearing properties (each preserves an existing invariant):
- **Always Private, one owner** — `subscriber_id`-scoped exactly like `story` (§4); membership/affinity never shared. No new cross-tenant path (§12 risk #1).
- **Cache over the durable log** — a Thread's durable *inputs* are the event + `feedback` logs + user declarations; the rows are a **recomputable cache** (same status as `cluster`/`story`). The CQRS guarantee (§3.0) holds.
- **Formed in the background, read on the hot path** — Threads evolve in a new write-side job, `thread_maintenance` (world's clock, best-effort, per-subscriber, off the punctual path, §11); projection only *reads* their precomputed product.

What it changes (all deferred): **relevance** gains a Thread term (§8.3 — the missed-because-split rescue); **priority** gains corroboration + reactivation/novelty (§8.3); **selection** gains a thread-diversity cap (§8.4); **rendering** groups by Thread with a per-thread delta line (§9.5); **feedback** gains thread-level care-more/less/done (§10.3). `thread_maintenance` forms threads by community detection over the subscriber's canonical-entity co-occurrence graph, with the §8.2 connected-components-and-`merged_into` id-forwarding reused one level up. Performance: nothing asymptotic added to the fire path — see §11.

### 8.7 Tiered identity resolution — the prerequisite  *(DESIGNED — deferred)*

Threads (and linking) are only as good as entity identity, and a **rigid 1:1 canonical map is a dead end** — identity is not committed once. The design is a **tiered resolver producing probabilistic, revisable equivalence edges**, where exactness is the high-confidence floor, not the whole thing:
- **Authority-minted ids** (GitHub node id, Slack `U0123`, email, DOI, CVE, normalized URL) are exact *by construction* — the certain backbone, kept as hard `confidence = 1.0` edges.
- **Surface forms** ("Acme Corp", "@dlewis") get graded `entity_edge`s (normalized / lexical / later embedding). **Soft edges weight, they don't collapse** identity; only 1.0 edges hard-merge; a conflicting authoritative id is a `cannot-link` veto. Canonical identity = connected components over edges ≥ θ (id-forwarded — the §8.2 trick one level down).

It is **revisable** through the §10.3 feedback channel extended to the entity level, and needs **no embeddings in v1** while staying forward-compatible (embedding is "one more edge source with a confidence"). Resolution runs as a sub-pass of `thread_maintenance`; `canonical_entities` is written at build time so the hot path reads resolved ids directly. See §10.4 for how the resulting **confidence** is rendered.

---

## 9. Generation layer

### 9.1 The read path (projection)

See §3.0–§3.1. **Materialization** owns both builds — `public-build` (public clusters; no-subscriber RLS context; shared) and `private-build` (a subscriber's private clusters; that subscriber's RLS context) — each a write-side, recomputable cache over the event log. **Projection** is `generate`: at the subscriber's scheduled instant, in their isolation context (§12), it reads a consistent snapshot of `public ∪ own-private` clusters and runs link → gate → rank → classify → cap → render → deliver. **Linking runs per subscriber** inside `generate` (§8.2) — it can fuse public with own-private clusters, so it can't be precomputed globally.

*Trigger latitude.* `private-build` is write-side, so whether it runs continuously (on private ingest) or **just-in-time at the head of a fire** is a scheduling choice, not an architectural one — v1 does the latter (tech §2), since private clusters are consumed only by their owner's digest. Either way projection's contract is unchanged: read the latest cluster snapshot.

### 9.2 Subscriber scheduling

Each subscriber has a **recurrence** — `freq` (daily | weekly), `at_time` (local time-of-day), `timezone`, and `on_weekday` for weekly — which yields `next_run_at`. The dispatcher sweeps `WHERE next_run_at <= now`, runs generation, and advances `next_run_at` to the **next future boundary** of the recurrence. The boundary is computed on the subscriber's **local wall clock** then anchored to UTC, so it is DST-safe: "weekly Tuesdays 17:00" stays 17:00 local across a DST transition (technical architecture doc §13).

**Catch-up coalesces.** Because advance jumps to the next boundary *strictly after now* (not "previous + one interval"), a worker outage spanning several boundaries produces **one** catch-up digest, not a backfilled burst — the single fire selects the freshest items from a lookback that reaches back to the last delivery (§9.4). One boundary function backs all three callers: signup (`ref = now`), a preference change (`ref = max(now, last_delivered)` — snap to the next earliest slot, never lose the pending window), and advance.

### 9.3 Idempotency & restartability

- The `digest` row is the unit of work, keyed `(subscriber_id, window_end)`, with status machine: `pending → built → rendered → delivered`. **`window_end` is the scheduled boundary that fired (derived from `next_run_at`), not wall-clock `now`** — so a crashed-then-retried run recomputes the *same* key and the `UNIQUE` collapses it. A retried run resumes from its last status; `idempotency_key` prevents double-send.
- Each stage is independently idempotent (ingest dedupes via `UNIQUE(fingerprint)`; build upserts clusters; linking is a deterministic recompute with id-forwarding; selection is a pure view frozen into `digest_item` on first run).
- **`last_delivered` advances only after successful delivery.** It is no longer a hard window edge — it is the **consideration floor** the next digest's lookback must reach back to (§9.4), so a crashed run reprocesses from the same floor rather than dropping events into a gap. No-loss is owed to the **durable, append-only event log** (clusters/stories/digests are recomputable views of it), not to careful window partitioning.

### 9.4 Lookback selection & re-surfacing

Selection is a **freshness-scored lookback**, not a hard window partition (this supersedes the earlier exactly-once window). The candidate set is every cluster (→ story) updated since a lower bound of `min(last_delivered, now − context_horizon)` — so it **always reaches back over the last delivery** (no event since then can be missed, even a backdated or late-arriving one, because the bound is on *ingest/build* recency, not the event's own timestamp) **and** optionally pulls in older context for synthesis. Freshness is a **scoring** term (recency decay over `now − last_event_time`), not a gate; the upper bound is the fire instant (`now + lookahead` once prospective events land — §8.5).

**An item may appear in more than one digest** — wanted, for "ongoing story" continuity and synthesis that references prior context. Repetition is damped, not forbidden, by a **recently-surfaced penalty**: compare a story's current `last_event_time` against the `story_last_event_time` snapshot on its most recent `digest_item` for this subscriber; a story with no new events since it was last shown is **demoted** (faded toward a one-line "still developing" note, eventually out) and **resurfaces** on a genuinely new event. Stable story ids (forwarded across recomputes, §8.2) are what make this lookup well-defined even though membership is recomputed every fire. (Because the fingerprint does not fold content — §5 — a trivial edit or re-poll cannot spuriously re-surface a story.)

**The guarantee, restated.** *Durability:* every event is permanently in the append-only log; digests are recomputable views. *Consideration:* every cluster updated since the last delivery is evaluated in the next digest. An item can score too low and age past the horizon **unsurfaced** — that is "not chosen," never "lost."

**Graduation:** format can change over time. A relevant single Slack ping enters as a Note and **graduates to a Story** as tickets and posts accrete and richness crosses the bar. Re-surface on a Note→Story format change as well as on new events.

### 9.5 Rendering & delivery

The digest renders as a single **priority-ordered list with format per item** (§8.4): Stories as rich expandable cards (headline + timeline + backing links), Notes as compact one-liners. Email is sent as a **notification + authenticated deep-link** to the full digest, not by dumping private content into the inbox. Story lifecycle is *derived*, not stored: a story is active while it has recent events; its rows and links stay referenceable forever (append-only). *(Deferred, §8.6: items group under their `Thread` with a per-thread **delta line** — "Acme migration — staging cutover landed; 2 follow-ups assigned to you" — computed like the §9.4 recently-surfaced damping; identities render with confidence bands per §10.4.)*

---

## 10. Control & trust features

### 10.1 Provenance & timelines

Never collapse membership. The story keeps every constituent event with link + timestamp; the rendered digest is a **projection** that references the story, not a frozen copy. "Show me the data behind this story" walks `story.clusters` → each cluster's `(scope, source, group_key)` → its events, sorted by `event_time`, each with source + link. Works only because `links[]` is on every event and the full trail is retained.

### 10.2 Reason records (explainability as first-class)

Attach a structured rationale to every consequential decision — link, selection, drop:

```
link:       {shared_link: "cve-2026-1234", shared_entity: "acme-corp", cosine: 0.81}
story:      {relevance: 0.9, richness: high (multi-source) -> Story, priority_rank: 1}
note:       {relevance: 0.8, richness: low (announcement)  -> Note,  priority_rank: 3}
drop:       {below relevance_floor} | {muted: source=hackernews}
```

These are **free** — just the signals already computed during the decision, serialized. Stored as `digest_item.reasons` (selection/drop) and per-member `link_reason` inside `story.clusters` (linking). Structured so they render for humans ("Grouped because 3 sources referenced the same CVE; shown because you follow acme-corp") and are machine-inspectable.

### 10.3 Feedback — three signals, two loops

- **"Care a lot" / "Don't care"** → *relevance feedback*. Extract the item's entities/sources, up/down-weight them in the subscriber's `affinity`, feed the relevance term next tick. (The only loop that affects what gets *included*, since relevance is the gate.)
- **"This aggregation is wrong"** → *linking correction*. Modeled as a pairwise constraint (cannot-link / must-link) on the subscriber's edge graph. Because linking is **per-subscriber** (§8.2), the constraint is just an edge dropped/added in *that* subscriber's next recompute — it takes effect on the next digest, with nothing shared to mutate. One person's click only ever changes their own view. *(Deferred, §8.7: the same must-link/cannot-link signal extends one level down to the **entity_edge** graph — the rendered "?" on an uncertain identity is its click target, so confirming/denying "is this the same person?" hardens or breaks an identity edge for that subscriber's future digests.)*
- **"This whole thread matters" / "I'm done with this"** *(deferred, §8.6)* → *thread feedback*. Targets a `Thread` (`target_type='thread'`): adjusts its `affinity` (or archives it on *done*). The loop that was missing — you can up/down-weight a *project*, not only an entity.

All feedback is an **append-only log**, processed async on the next tick, never blocking delivery. It is also the **eval signal**: story precision and false-positive rate are how you measure whether overwhelm is actually decreasing.

### 10.4 Confidence as a rendered signal  *(DESIGNED — deferred; with §8.7)*

The tiered resolver's (§8.7) **confidence is a product surface, not only an internal knob** — the honest face of the trust thesis (§1). It flows to rendering and back through feedback:
- **Frontend sees a band, not a float** — `Confirmed | Probable | Uncertain` maps to treatment (real avatar / avatar + badge / question-mark placeholder); the raw score stays server-side. One vocabulary spans identity *and* edges (a story→thread or cluster→cluster assignment renders "possibly part of *X*" the same way as "possibly Dana").
- **Avatar provenance must equal identity provenance** *(the one footgun)* — a false-confident avatar misleads and can attach the wrong face to private content (§12). Show the guaranteed avatar *only* when the image comes from the authority that minted the exact id (the Slack `U0123` carries its avatar); otherwise the placeholder.
- **The "?" is the correction affordance** — it is the click target for the §10.3 entity-level feedback, so rendered doubt *is* the resolution UI (the flywheel).

Two guardrails: **the weight and the render come from the same number** (a 0.6 identity edge that contributed 0.6× to relevance must also render Uncertain — never a silent inflate); and **budget the doubt** (cap visible "?"s, aggregate the tail — "…and 3 possibly-related items"). The render contract is `{ display_name, canonical_id?, confidence_band, evidence, avatar_ref? (authoritative-only) }` — the §10.2 reason-record reshaped, so it is free.

---

## 11. Performance considerations

Hotspots, by risk:

1. **Per-subscriber generation fan-out** is the real scaling axis (public grouping/rollups are shared/amortized; linking + scoring are per subscriber per cadence). Keep it cheap: cluster rollups are precomputed once per cluster; the per-subscriber candidate set is a **blocking-seeded** index lookup over clusters (`cluster_entities` GIN + `cluster_candidates`), *not* a scan of all public clusters; linking over that bounded set is union-find; only relevance and priority are per-user. Embarrassingly parallel over `SKIP LOCKED` job rows, bucketed by cadence + timezone.
2. **Linking** (per subscriber) — blocking bounds the candidate set to the subscriber's interests + cross-boundary key matches; v1 unnests rolled-up `entities` (`GIN`), scaling to a normalized `cluster_signal` self-join; embed clusters not events; ANN top-k if embeddings arrive. If per-subscriber linking ever dominates, the lever is the **shared public-story cache** (memoize pure-public stories once).
   - **Threads add nothing asymptotic to the fire path** *(deferred, §8.6)*. Thread-aware relevance is the *same* unnest-and-sum already done for affinity (richer weights, identical op); story→thread assignment reuses the `cluster_entities` GIN blocking pattern (bounded); corroboration/novelty are O(1) cached reads. All genuine work — identity resolution, community detection, decay, weight projection — lives in the new **`thread_maintenance`** job: per-subscriber, bounded by the (small) entity graph, best-effort, coalescing, **off the punctual path** (the `public-build` "fall behind, never wrong" contract). Reserve levers: a **shared public co-occurrence baseline** (public entity graph is identical across subscribers) and the `relevance_weight` table split-trigger.
3. **Event growth** — v1 leaves `event` unpartitioned (comfortable into the millions of rows). Deferred lever: range-partition by `ingest_time` + BRIN index + partition pruning. Raw payloads are TOASTed out-of-line, so they don't bloat hot scans; offloading to object storage (`raw_ref`) is a later move.
4. **Hydration I/O** — batch per source API, parallelize per connection, respect backoff.
5. **Webhook catcher** — verify, enqueue a job, return; never block on downstream.

**Principle:** the shared-global / per-user split *is* the performance design. Do the **shareable** work — public grouping + cluster rollups — once and amortize; keep per-user work cheap by pre-filtering on indexed cluster signals. The shared public-story cache extends this boundary when linking cost grows.

---

## 12. Security considerations

1. **Scope isolation (risk #1).** A private event entering the public pool, or becoming visible to another subscriber, is catastrophic. The invariant is **directional** (§4): information flows public → private only. Enforcement runs the **whole pipeline in two RLS contexts, never cross-tenant**:
   - **`public-build`** runs in a *no-subscriber* context whose policy exposes only `scope_kind='public'` rows — it physically cannot read private data.
   - **`generate`** runs in the *subscriber* context: `SET LOCAL app.subscriber_id = $id`; policy = `scope_kind='public' OR scope_subscriber_id = current_setting(...)`, **SELECT-only on public**, writes confined to own-private rows. No code path reads two tenants' private data at once.
   - Prereqs RLS silently depends on: the runtime role is **non-owner, no `BYPASSRLS`**; **`FORCE ROW LEVEL SECURITY`** on every scoped table; a separate migration role owns DDL; `SET LOCAL` is transaction-scoped (pool/PgBouncer-safe), reachable only through a `with_scope(ctx, …)` wrapper. RLS is the backstop; the typed `Scope` (technical architecture doc §5) + a scope-invariant property test on the build path are primary.
2. **Credentials.** Envelope-encrypt at rest (per-connection data keys, KMS) and store only a `creds_ref`, least-privilege scopes (only chosen repos/channels, read-only), never log, support rotation/revocation.
3. **Webhook auth.** HMAC-verify every inbound webhook over raw bytes (constant-time) with the **app-level** signing secret and drop unverified *before* enqueueing. Scope isolation comes from routing on the verified payload id to our `connection` row, never a payload-supplied id (IDOR). See technical architecture doc §3A.
4. **SSRF in fetching.** Fetch through an egress proxy with private-range denylist, pinned DNS, no redirects to private IPs, timeouts + size caps.
5. **PII & retention.** Email/Slack bodies are the most sensitive data; the raw payload most of all (v1 stores it **inline**, TOASTed out-of-line and encrypted). Shortest viable retention, encrypted, access-logged, per-subscriber deletion (GDPR) — deletion must cascade to `event.raw`, derived events, and any `reasons`/`link_reason` that embed private snippets. LLM summarization (if added) is data egress to a model provider → explicit per-source consent.
6. **Stateless workers.** A generation worker processing many subscribers must not retain/leak one's data into another's digest. Per-job context, no shared mutable buffers.
7. **Authz on drill-down / feedback APIs.** Re-check server-side that the cluster/item is in the caller's visible scope; never trust a client-supplied id (IDOR).

**Meta-point:** explainability (§10.2) and isolation (#1) are the same product feature from two angles — both are how the system earns the trust that "peace of mind" depends on.

---

## 13. v1 build vs defer

**Build:**
- Modular monolith on a single Postgres (8-table domain model + a library-provided job queue, §6).
- Always-on webhook catcher + **two decoupled pipelines** — best-effort **materialization** (`public-build` shared, `private-build` per-subscriber) and per-subscriber **scheduled projection** (`generate`), sharing only the durable event log (§3.0, §3.1).
- **Recurrence scheduling** (daily/weekly at a local time, DST-safe) with **coalescing** catch-up; **lookback + freshness-scored** selection over the durable log (no exactly-once partition), with recently-surfaced damping (§9.2, §9.4).
- Connector SDK (incl. per-event `content_kind`) + RSS + GitHub + Slack (the v1 private source; Gmail deferred over CASA cost — §2, §7.4).
- Visibility scopes with the directional public→private rule; subscriber abstraction (1:1 with users).
- Deterministic grouping (public shared / private per-subscriber) + structured **per-subscriber** cross-source linking (connected-components recompute + stable-id forwarding; `cluster` + per-subscriber `story` with `clusters` jsonb).
- Relevance-gated inclusion + richness-classified Story/Note rendering + priority-ordered digest with plain recency decay (hand-tuned floor/thresholds/caps in config).
- Provenance/timelines, reason records, feedback log + relevance loop + per-subscriber linking corrections.
- Email delivery as notification + deep-link; inline raw-payload storage.
- Two-context row-level security + KMS-backed credential storage + SSRF-guarded fetching.

**Defer (designed-for, with split-triggers in §6):**
- **The Thread layer & tiered identity** (`digest-thread-layer.md`) — per-subscriber `Thread`s (§8.6) + the probabilistic `entity_edge` identity graph (§8.7) + confidence-as-rendered-signal (§10.4). Lands after linking + relevance (M3/M4); one new background job (`thread_maintenance`), no fire-path cost (§11).
- `confidence` + forward-looking/prospective events (future-valued `event_time`, signed-gap salience curve) + `velocity`.
- Embedding/ANN semantic linking (`vector` column + HNSW) — also an additive `entity_edge` source for identity (§8.7).
- **Shared public-story cache** — memoize pure-public stories across subscribers (promote the shareable slice of projection into materialization — §3.0).
- Real-time in-app feed.
- Teams / shared scopes.
- Learned scoring (feedback-driven affinity).
- LLM summarization.
- Additional sources (Notion, Bluesky, Mastodon); Twitter/X via 3rd-party only.
- Scale persistence: event partitioning + dedup gate, normalized `cluster_signal`, normalized `story_cluster`, raw object-storage offload, multi-channel `delivery`.

---

## 14. Future Features

### Entropy

Digests risk becoming a filter bubble. We want to surface adjacent or outsize things the user didn't ask for but would value, via a separate "variety" budget that pulls from the global pool of public connections or hand-curated sources.

This system must be:
- **Separate from relevance scoring** — a fixed "variety" budget, not a relevance-weighted boost.
- **Explainable and feedback-aware** — on positive interactions, a variety source may be promoted into the regular affinity set.
- **User-controlled** — exposed as an "Entropy" setting/slider.

### Filtering Out Already Engaged-with Events

Users will have already seen and acted on many incoming events. We should penalize events the user has "dealt with" and promote events they never followed up on.

- unseen → normal treatment
- seen, not acted → boost (grows with age)
- acted → suppress (unless materially changed since the user acted, then re-surface)
- unseen but acted → suppress

Engagement state by source:

| Source | Read state | User interaction |
|---|---|---|
| Gmail | yes (`UNREAD`) | yes (replied in thread) |
| Slack | yes (`last_read`) | yes (posted / reacted) |
| GitHub | yes (notifications read state) | yes (commented / reviewed / closed) |
| Mastodon / Bluesky | notification "seen" markers | yes (replied / liked) |
| Notion | partial | yes (edited / commented) |
| RSS / podcasts | no per-user "seen" exists | not applicable |

Engagement is a *private-scope* feature.

---

## 15. Open questions

- Group-key strategy and near-dup merge thresholds, per source — needs tuning against real data.
- The build-step SQL: grouping events into `cluster` rows by `(scope, source, group_key)` + the rollups; and the per-subscriber linking SQL — blocking-seeded candidate selection, edge scoring, connected-components, id-forwarding.
- **Linking thresholds:** edge-strength cutoffs, and the *asymmetric* bar to merge two already-delivered stories vs attach a fresh cluster (§8.2) — the single-linkage-blob guard.
- Tuning `relevance_floor` and the richness/Story threshold against real digests (avoid all-Notes or all-Stories).
- The `content_kind` taxonomy per source — where the lines sit (is a GitHub release with long notes `longform` or `announcement`?).
- Modular-monolith package boundaries and the deployment unit(s).
- Data lifecycle: inline raw-payload retention horizon vs drill-down needs; GDPR delete cascade.
- Initial weight/threshold values and the eval harness that uses the feedback log.
- Email rendering: how much content in the body vs behind the authenticated link.
- The B→A trigger: at what per-subscriber-linking cost (or when embeddings land) to add the shared public-story cache.
- **Thread layer & tiered identity** (`digest-thread-layer.md` §10): community-detection choice (LPA vs Louvain) + the story→thread overlap `k`; per-edge-source `θ` and the confidence-band thresholds; dormancy/archive horizons + affinity decay; single-primary `thread_id` vs a `story_thread` join; cold-start bootstrap; the visible-doubt budget per digest; where the shared public co-occurrence baseline is amortized.
