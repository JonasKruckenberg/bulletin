# Bulletin — Implementation Roadmap

**Status:** Proposed build plan
**Last updated:** 2026-06-08
**Reads against:** `digest-system-design.md` (product + data model), `digest-technical-architecture.md` (Rust runtime), `digest-data-sources.md` (source backlog).

This is the *order of operations* for building Bulletin. The design docs already separate
v1 from "deferred." This roadmap goes one step further: it is **aggressively scoped** — it
cuts even inside the doc's v1 to find the thinnest end-to-end slice that proves the product
thesis, then re-adds capability only when a milestone forces it. Every milestone ships
something you can run and demo.

---

## 0. The one thing to internalize

The product thesis is *suppress noise, elevate the few things that matter, earn trust*. The
architecture thesis is *a Postgres-orchestrated scheduled batch pipeline, not a service mesh*.
Both point at the same build strategy:

> **Build the spine end-to-end first with one trivial source, then thicken it.**
> Ingest → group → select → render → deliver, on a schedule, idempotently — working — before
> adding a second source, before linking, before relevance, before any hardening.

A digest that emails you yesterday's RSS items, on a cron, without crashing, is worth more as
a foundation than three perfectly-typed connectors with no pipeline to feed.

### Two invariants we never cut, at any milestone

1. **Scope isolation is directional (public → private only).** Even before RLS exists, the
   typed `Scope` enum + the build-path scope-invariant property test (design §12, tech §6) are
   *mandatory* the moment any private data enters the system (M2). No private datum ever reaches
   the public pool or another subscriber.
2. **Ingest is idempotent.** `UNIQUE(fingerprint)` + `ON CONFLICT DO NOTHING`, SHA-256 over a
   domain-separated, length-framed pre-image, content deliberately *not* folded in (tech §5.2).
   This is what makes webhook/poll overlap and crash-retry safe. It lands in M1 and is
   property-tested from day one.

Everything else is negotiable and sequenced below by **value ÷ risk**.

---

## 1. Aggressive cuts (beyond the doc's own defer list)

The design doc's "v1" is forward-compatible and complete, but still large. To reach a working
loop fast, these v1 items are **pushed later in the sequence** (not redesigned — just deferred
to the milestone that needs them, with the re-add trigger noted):

| Doc puts in v1 | Roadmap defers to | Re-add trigger / rationale |
|---|---|---|
| **Slack** (3rd connector) | M6 | RSS + GitHub already exercise *every* architectural axis: poll-only, webhook, public scope, **private scope** (private GitHub repos), and 2 of 3 `content_kind`s. Slack adds `message` + heaviest OAuth for marginal architectural coverage. |
| **OAuth `/connect` flow** (UI install dance) | M5 | Seed `connection` rows by hand (config/SQL) through M4. The flow is real product surface but blocks nothing in the pipeline. |
| **Two-context RLS** | M5 | The doc itself calls RLS "the backstop"; typed `Scope` + property test are "primary." Carry the primary defense from M2; add the DB backstop before real private data / real users. |
| **KMS / envelope encryption** | M4 (interim master key) | RSS needs no creds. Only GitHub (M2) stores a secret, and `installation_id` is *not* a secret. Real secret-at-rest is forced only by Slack tokens / production. |
| **Per-subscriber linking** | M3 | A group-only digest (one cluster = one story) is a valid, shippable intermediate. Linking is the headline feature but the highest-complexity component — earn the pipeline first. |
| **Relevance gate + affinity feedback loop** | M4 | Start with a hard subscription/keyword filter (M1). Learned-ish affinity weighting + the feedback loop come once there's a digest to give feedback *on*. |
| **DST-correct per-subscriber scheduling** | M5 | M1–M4 run a single global UTC daily/weekly tick. Timezone + DST math is threaded in before multi-timezone users. |
| **Nix / Cachix / OCI images** | Parallel track, lands by M5 | Plain Cargo workspace + GitHub Actions is enough to build and test. Add Nix when reproducibility/CI-cache pain is real or before first deploy — not on day one. |
| **OTel / Sentry / tokio-metrics** | M5 | `tracing` to stdout + pipeline counters carry M1–M4. Full observability stack lands with hardening. |
| **turmoil / madsim DST simulation** | M5 | `proptest` + `insta` on the pure `core` (fingerprint, selection, linking determinism) is high-value and cheap — keep from M1. Crash-injection simulation waits until the DAG stops changing shape. |

**What we do *not* cut from v1:** the modular monolith on one Postgres, the `core`-vs-rest crate
boundary, the apalis job queue + cron tick, the canonical `Event` + fingerprint dedup, the
`Connector`/`Connection` two-layer trait family, the injectable clock, and the typed `Scope`.
These are load-bearing for everything after them.

---

## 2. Milestones

Each milestone: **Goal · Build · Defer · Exit criteria.** Exit criteria are demoable.

### M0 — Foundations (skeleton that compiles & tests)

*Goal:* a workspace that builds, a database that migrates, a test harness that runs. No features.

**Build**
- Cargo workspace with the `core`-vs-rest boundary made literal (tech §4). Start lean:
  `core` (pure domain), `runtime` (apalis handlers + axum + orchestration), `connectors`,
  `store` (sqlx + migrations), `bulletin` (bin, `clap` role dispatch). Fold `support` into
  `runtime` for now.
- `core` foundations (tech §5.1): generic `Id<T>` with `derive-where` + `PhantomData<fn() -> T>`,
  feature-gated sqlx impls; `Scope`, `SourceKind`, `ContentKind` enums; `Fingerprint([u8;32])`.
- **Injectable clock** (tech §6 — load-bearing). No ambient `now()` in logic, ever.
- One binary, roles via `clap`: `serve | worker | migrate | all` (tech §2).
- Postgres via `sqlx` (compile-time-checked raw SQL) + migrations; apalis `migrate` storage setup.
- `proptest` + `insta` wired into `core`; `cargo-nextest` runner; `testcontainers` for the one
  real-Postgres integration test (queue round-trip). GitHub Actions: fmt, clippy, nextest,
  `cargo-deny`.

**Defer:** Nix, OTel, every connector, every domain feature.

**Exit:** `bulletin migrate && bulletin all` boots; an empty cron tick fires and logs; CI is green.

---

### M1 — Walking skeleton (RSS → email, end to end) ★ the keystone

*Goal:* one subscriber receives a scheduled email digest of recent **RSS** items. Group = Story.
No linking, no relevance scoring, no auth, no private data.

**Build**
- **Canonical `Event`** + the `fingerprint` recipe, property-tested (tech §5.2). `event` table,
  `UNIQUE(fingerprint)`, `INSERT … ON CONFLICT DO NOTHING`.
- **RSS connector** as a pure `Connection` (poll-only, conditional GET, cursor = ETag/Last-Modified;
  `feed-rs` + SSRF-guarded `reqwest`). `to_events` sets `entities`/`content_kind`/`group_key`/`links`;
  infra `finalize` stamps scope (all `public` here) + fingerprint (tech §5.4).
- **The tick DAG, minimal:** `PollConnection` (events-before-cursor ordering, tech §3) →
  `PublicBuild` (group events into `cluster` by `(scope, source, group_key)` + rollups) →
  `GenerateDigest` (one subscriber: select recent clusters → 1 cluster = 1 story → render → deliver).
- **apalis** cron tick + queue with `unique-jobs` keys (tech §2); the three due-sweeps (even if
  only `PollConnection`/`GenerateDigest` matter yet).
- **`digest` state machine** (`pending→built→rendered→delivered`), `UNIQUE(subscriber_id, window_end)`,
  `window_end` = scheduled boundary, watermark advances only after delivery (design §9.3).
- **Trivial selection:** include everything recent, order by `last_event_time`, cap at N. The
  selection function is **pure over precomputed features** from day one (design §8.4) — just with a
  stub relevance of 1.0 — so M3/M4 swap internals, not the call site.
- **Email delivery:** simplest working sender (SMTP or a transactional API), plaintext or basic HTML.
- One subscriber, seeded by SQL/config. Single global UTC daily cadence.

**Defer:** linking, relevance, feedback, Story/Note distinction (everything renders as a Story),
private scope, webhooks, OAuth, RLS, tz/DST, the authenticated deep-link (email body is fine for now).

**Exit:** add an RSS feed → wait for the tick → receive an email listing recent items, each with a
working link. Re-running the tick sends nothing new (idempotent). Killing the worker mid-run and
restarting reprocesses the same window without dup-sending.

---

### M2 — Multi-source & the scope boundary (GitHub + webhooks)

*Goal:* add GitHub as a second source exercising **webhooks**, the **poll-reconciliation backstop**,
and **private scope** (private repos). The scope-isolation invariant becomes real and is tested.

**Build**
- **GitHub connector** as `Connection + RealtimeConnection` (tech §5.4): poll = REST reconciliation
  backstop; `accept_webhook`; `hydrate` = identity. Connections seeded by hand (`provider_account_id`
  = `installation_id`, `creds_ref` placeholder — installation_id is not a secret).
- **Webhook catcher** in `serve` (tech §3A): HMAC-verify over raw bytes (app-level secret,
  constant-time) → enqueue `ProcessWebhook{source, raw_body, delivery_id}` → 2xx fast. No parse, no
  connection resolution at the edge.
- **`ProcessWebhook` job:** credential-free `route` (peek `installation.id`) → resolve `connection`
  from *our* row (IDOR defense) → `accept_webhook` → `to_events` → `finalize` → dedup insert.
  Lifecycle events update `connection.status`.
- **Connector dispatch** = hand-written `match` over the closed source set, cursor/creds erased to
  `serde_json::Value` inside each arm; `ConnDispatch` / `RealtimeDispatch` enums make "webhook to a
  pull-only source" uncompilable (tech §5.1/§5.4).
- **Scope becomes load-bearing:** private GitHub repos → `scope = Private(subscriber)`.
  `PublicBuild` builds public clusters; private clusters built per-subscriber inside `GenerateDigest`.
  **Add the build-path scope-invariant property test** (no private datum in a public artifact) — this
  is the primary isolation defense until RLS lands in M5 (design §12).
- `content_depth` now spans `announcement` (releases) + `longform` (RSS) + foundations for `message`.

**Defer:** OAuth connect flow (still hand-seeded), KMS, RLS, Slack, linking.

**Exit:** push to a watched public repo → it appears in the next digest via webhook; drop the
webhook and it still appears via the reconciliation poll (fingerprint collapses the overlap). A
private repo's events appear **only** in their owner's digest, and the scope-invariant property test
passes.

---

### M3 — The headline feature: per-subscriber linking

*Goal:* a story fuses clusters **across sources** — the "connections you would have missed." This is
the product's reason to exist; it gets its own milestone because it is the highest-complexity piece.

**Build** (design §8.2, tech §5.3)
- **`story`** table (per-subscriber, `clusters` jsonb of `{cluster_id, link_reason}`, cached rollups,
  `merged_into`). **`Cluster` carries no `story_id`** (a public cluster belongs to many stories).
- **Blocking** (the O(n²) guard): per-subscriber candidate set = public clusters matching affinity ∪
  public clusters sharing a strong key (URL/CVE/native id) with the subscriber's own private clusters.
  v1 unnests `cluster.entities` via `GIN`.
- **Edge scoring** on candidate pairs: entity Jaccard + temporal closeness + shared-link boost.
- **Decision = deterministic recompute:** connected components (union-find) each generate;
  **stable-id forwarding** from the prior assignment; `merged_into` on retro-merge; **asymmetric
  thresholds** (only strong edges merge two already-delivered stories) as the single-linkage guard.
- The linking function is **pure** over `(clusters, edges, prior assignment)` — proptest its
  determinism and id-stability across recompute (tech §6).
- `link_reason` recorded per member (free reason record, design §10.2).

**Defer:** embeddings/ANN (schema-additive later), shared public-story cache (memoize pure-public
stories — the scale lever, re-add when per-subscriber linking cost bites), entity NER beyond
structure + URL extraction.

**Exit:** a private GitHub incident PR and a public RSS/advisory item referencing the same CVE/URL
surface as **one story** in the owner's digest, with a `link_reason`. Re-running generation keeps the
story's id stable; a later strong link retro-merges two stories with the oldest id winning.

---

### M4 — Relevance & trust (the "earn trust" half of the thesis)

*Goal:* the digest is *relevant* and *explainable*, and the user can correct it.

**Build**
- **Scoring (design §8.3–8.4):** relevance **gates** inclusion (single `relevance_floor`: subscription
  match, entity affinity, scope bonus, mutes as hard zeros); **richness classifies** format
  (Story if multi-event/multi-source/`longform`, else Note); **priority** orders + caps (Stories ~3–5,
  Notes ~15–25) with **plain recency decay** at read time. Thresholds/caps in a config table.
- **Story/Note rendering** + priority-ordered list where format ≠ importance (a high-priority Note can
  outrank a Story).
- **Provenance & timelines (design §10.1):** "show the data behind this story" walks `story.clusters`
  → group_key → events. Requires `links[]` on every event (already there).
- **Reason records as types (design §10.2, tech §5.5):** link/story/note/drop rationales, stored in
  `digest_item.reasons`. Free — just serialized signals.
- **Feedback (design §10.3):** append-only log; "care more/less" → relevance affinity update next tick;
  "wrong aggregation" → per-subscriber cannot-link/must-link edge constraint (takes effect in that
  subscriber's next recompute — nothing shared to mutate).
- **Windowing & re-surface suppression (design §9.4):** compare `last_event_time` vs the
  `story_last_event_time` snapshot; re-surface only on new events or Note→Story graduation.
- **Interim secret-at-rest:** `creds_ref` + wrapped-DEK with a single app master key +
  XChaCha20-Poly1305 (tech §6), `creds_ref` indirection preserved so swapping to KMS later is a
  backend change, not a schema change. `secrecy`/`zeroize` in memory.

**Defer:** learned scoring, `confidence`, prospective events, KMS proper, entropy/variety budget,
engagement ("already dealt-with") suppression.

**Exit:** a digest shows a mix of Stories and Notes, priority-ordered, each with a human-readable
reason; "show the data behind this" renders the event timeline; clicking "don't care" demotes that
entity next tick; "wrong aggregation" splits the story for that user only.

---

### M5 — Production hardening

*Goal:* safe to point at real accounts and real users.

**Build**
- **Two-context RLS (design §12, tech §6):** `PublicBuild` in a no-subscriber context (public rows
  only); `GenerateDigest` in subscriber context (`SET LOCAL app.subscriber_id`). Prereqs:
  non-owner runtime role, no `BYPASSRLS`, `FORCE ROW LEVEL SECURITY` on scoped tables, separate
  migration role, a `with_scope(ctx, …)` wrapper as the only connection path. RLS is the backstop
  behind the M2 typed-`Scope` + property test.
- **OAuth / app-install connect flow:** `GET /connect/{source}` (signed `state`) + callback (bind
  provider-account → subscriber → scope, encrypt creds) for GitHub App (tech §3A).
- **KMS-backed envelope encryption** (swap the M4 master key behind `creds_ref`); rotation/revocation.
- **SSRF guard** finalized: resolve-then-pin resolver, RFC1918/loopback/link-local/CGNAT/metadata-IP
  denylist, post-`url`-crate IP validation, redirect re-validation, timeouts + size caps (tech §6).
- **Timezone-aware scheduling + DST** (tech §12): per-subscriber `next_run_at` in their tz, UTC
  `timestamptz` storage, tz boundary math, render in local tz.
- **Authenticated deep-link digest view** + drill-down/feedback APIs with server-side IDOR re-checks
  (design §10, tech §10.7). Email becomes notification + deep-link, not content dump.
- **Observability:** `tracing`→OTel bridge, pipeline counters, reason records as trace events, Sentry
  panic hook, graceful shutdown via `Monitor::run_with_signal` (tech §6).
- **DST/crash simulation:** `turmoil`/`madsim` crash-injection between DAG stages asserting the
  restartability invariants (no gaps, no double-send, scope never crosses) (tech §6).
- **Nix/CI** finalized (crane + rust-overlay + Cachix, minimal OCI image) if not already done in the
  parallel track (tech §7).

**Exit:** a real GitHub App install via the connect flow; private data provably confined by RLS (a
deliberately mis-scoped query returns nothing); SSRF probes blocked; a multi-timezone "daily 8am"
fires at each subscriber's local 8am; crash-injection tests pass.

---

### M6 — Second private source & beyond (deferred backlog)

*Goal:* prove the connector model generalizes; begin the source backlog.

- **Slack** (the doc's original v1 private source): `message` content_kind, Events API, the full OAuth
  v2 dance — now cheap because every mechanism it needs already exists.
- Then, by **value ÷ friction** (data-sources §10): the generic primitives first — **generic inbound
  webhook + JSON normalizer** (a connector factory for the long tail), **WebSub + JSON Feed** on the
  RSS path (poll → push for the open web), **email-alias / inbound-parse** (CASA-free email),
  **ActivityPub** (the whole fediverse via one actor).
- High-value/low-friction connectors: **Calendar** (the first prospective source — also unlocks the
  deferred signed-gap salience curve + `confidence`), **Sentry / Linear / Dependabot / PagerDuty**,
  **Hacker News** + the broad RSS layer, **package registries** filtered against the user's manifests.

**Other deferred-by-design upgrades** (each schema-additive, with split-triggers in design §6):
embedding/ANN linking, the shared public-story cache, teams/shared scopes, learned scoring, LLM
summarization, the entropy/variety budget, engagement ("already dealt-with") suppression, event
partitioning + normalized signal tables, object-storage raw offload, multi-channel delivery.

---

## 3. Sequencing rationale (why this order)

- **M0→M1 builds the spine before the organs.** Most pipeline projects die by perfecting components
  with nothing to feed. RSS is chosen first precisely because it has *no auth, no webhooks, public
  scope only* — the least friction to a working loop.
- **M2 adds the two hardest mechanisms (webhooks, scope) on a working pipeline,** not a blank page —
  and GitHub alone covers public + private + push + pull, so M2 retires most architectural risk
  without Slack.
- **M3 is isolated** because per-subscriber linking (blocking + connected-components + id-forwarding +
  asymmetric merge thresholds) is the single most intricate algorithm in the system; it deserves a
  milestone where the rest of the pipeline is stable underneath it.
- **M4 delivers the "trust" half** only once there are real stories to gate, explain, and correct.
- **M5 is hardening, batched last** because RLS, KMS, OAuth, SSRF, and DST are each load-bearing for
  *production* but block *nothing* in the build loop — and the M2 typed-`Scope` + property test carry
  the critical invariant until then. **The one caveat: do not point M5-deferred items at real third-party
  accounts or real users' private data before M5 lands.** Synthetic/owned test data only through M4.

## 4. Definition of done for "v1"

End of **M5**: a real user connects RSS feeds and a GitHub App; on their schedule (correct in their
timezone) they receive an email notification linking to an authenticated digest of priority-ordered
Stories and Notes; stories fuse public and their own private sources; every item carries a reason and
a drill-down timeline; feedback corrects relevance and aggregation; private data is provably isolated
by both the type system and RLS; secrets are envelope-encrypted; the pipeline is crash-safe and
idempotent under simulation. **Slack and the broader backlog (M6) are post-v1.**

## 5. Tracking the open questions

The design docs' open questions (design §15, tech §10) are not blockers — they are *tuning* work that
attaches to the milestone that surfaces them:

- Group-key strategies & near-dup thresholds per source → **M1 (RSS), M2 (GitHub)**.
- Linking edge-strength cutoffs + the asymmetric merge bar → **M3**.
- `relevance_floor` / richness threshold / caps tuning, `content_kind` taxonomy per source → **M4**.
- Crate-graph names & granularity → revisit end of **M2** once real dependencies exist.
- Data lifecycle / GDPR delete cascade, KPI definitions + eval harness over the feedback log → **M5**.
