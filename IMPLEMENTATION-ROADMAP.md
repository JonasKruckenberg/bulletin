# Bulletin — Implementation Roadmap

**Status:** In progress — M1 completed (2026-06-13); M2 next
**Last updated:** 2026-06-13
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
| **OAuth `/connect` flow** (UI install dance) | M5 | Seed `connection` rows by hand (config/SQL) through M4 — you install your own GitHub App once. The flow is real product surface but blocks nothing in the pipeline. |
| **Managed KMS backend** (aws-sdk-kms) | M5 (optional) | The interim sealed-master-key envelope encryption (in M2) is the actual v1 design and is sufficient for self-hosting. Swapping to a managed KMS is a *backend* change behind `creds_ref`, never a schema change. |
| **Per-subscriber linking** | M3 | A group-only digest (one cluster = one story) is a valid, shippable intermediate. Linking is the headline feature but the highest-complexity component — earn the pipeline first. |
| **Relevance gate + affinity feedback loop** | M4 | Start with a hard subscription/keyword filter (M1). Learned-ish affinity weighting + the feedback loop come once there's a digest to give feedback *on*. |
| **DST-correct per-subscriber scheduling** | M5 | M1–M4 run a single global UTC daily/weekly tick. Timezone + DST math is threaded in before multi-timezone users. |
| **Nix / Cachix / OCI images** | Parallel track, lands by M5 | Plain Cargo workspace + GitHub Actions is enough to build and test. Add Nix when reproducibility/CI-cache pain is real or before first deploy — not on day one. |
| **OTel / Sentry / tokio-metrics** | M5 | `tracing` to stdout + pipeline counters carry M1–M4. Full observability stack lands with hardening. |
| **turmoil / madsim DST simulation** | M5 | `proptest` + `insta` on the pure `core` (fingerprint, selection, linking determinism) is high-value and cheap — keep from M1. Crash-injection simulation waits until the DAG stops changing shape. |

**Pulled *forward* (because we want to dogfood live private data early):** **two-context RLS** and
**credential-at-rest encryption** move from the back of the line into **M2**, the moment private scope
and real credentials first exist. The doc lists both in v1; this roadmap originally batched them into
hardening, but validating usefulness/tuning against your own real GitHub repos means private data must
be DB-isolated and your GitHub App key must be encrypted *as soon as they land*. See M2.

**What we do *not* cut from v1:** the modular monolith on one Postgres, the `core`-vs-rest crate
boundary, the apalis job queue + cron tick, the canonical `Event` + fingerprint dedup, the
`Connector`/`Connection` two-layer trait family, the injectable clock, and the typed `Scope`.
These are load-bearing for everything after them.

---

## 2. Milestones

Each milestone: **Goal · Build · Defer · Exit criteria.** Exit criteria are demoable.

### M1 — Walking skeleton (RSS → email, end to end) ★ the keystone — ✅ Completed (2026-06-13)

*Goal:* one subscriber receives a scheduled email digest of recent **RSS** items. Group = Story.
No linking, no relevance scoring, no auth, no private data.

> **Status:** Completed 2026-06-13. Deployed locally with demo connections and a demo account;
> the end-to-end RSS → email loop and idempotent re-runs were verified against the exit criteria below.

**Build**
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

### M2 — Multi-source, the scope boundary & isolation hardening (GitHub + webhooks) ★ "safe for my own live data"

*Goal:* add GitHub as a second source exercising **webhooks**, the **poll-reconciliation backstop**,
and **private scope** (private repos) — and make it **safe to point at your own real account**: private
data is DB-isolated by RLS and your real credentials are encrypted at rest. This is the milestone where
you can start dogfooding for usefulness/tuning on live private data.

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
  **Build-path scope-invariant property test** (no private datum in a public artifact) — a *primary*
  isolation defense alongside the typed `Scope` (design §12).
- **Two-context RLS — pulled forward (design §12, tech §6):** the DB backstop behind the typed `Scope`,
  here from day one of private data so your own private repos are isolated even against a logic bug.
  `PublicBuild` runs in a no-subscriber context (policy exposes only `scope_kind='public'`);
  `GenerateDigest` runs in the subscriber context (`SET LOCAL app.subscriber_id`, SELECT-only on
  public, writes confined to own-private). Prereqs: **non-owner runtime role, no `BYPASSRLS`,
  `FORCE ROW LEVEL SECURITY`** on every scoped table, a separate migration role owning DDL, and a
  `with_scope(ctx, …)` wrapper as the *only* connection path.
- **Credential-at-rest — pulled forward (interim envelope encryption, tech §6):** `secrecy`/`zeroize`
  in memory (redacted `Debug`, zeroized on drop); the app-level **GitHub App private key + webhook
  signing secret** sealed at rest (sealed file/env via XChaCha20-Poly1305) and loaded once; the
  `creds_ref` + wrapped-DEK indirection scaffolded in the schema so per-connection secrets (Slack, M6)
  and a managed-KMS swap are later *backend* changes, not migrations. (GitHub per-connection creds =
  just `installation_id`, **not** a secret; the secret to protect is the app-level key.)
- `content_depth` now spans `announcement` (releases) + `longform` (RSS) + foundations for `message`.

**Defer:** OAuth connect flow (hand-seed your own single install), managed-KMS backend (the interim
sealed master key is sufficient for self-hosting), Slack, linking. SSRF guard stays in M5 while *you*
are the only one adding feeds — pull it forward the moment anyone but you can add a feed/URL.

**Exit:** push to a watched public repo → it appears in the next digest via webhook; drop the webhook
and it still appears via the reconciliation poll (fingerprint collapses the overlap). Your **private
repo's events appear only in your own digest** *and* are DB-isolated: a deliberately mis-scoped query
run under the runtime role returns nothing (RLS verified), and the scope-invariant property test
passes. Your GitHub App key is never stored or logged in plaintext.

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

**Defer:** learned scoring, `confidence`, prospective events, managed-KMS backend, entropy/variety
budget, engagement ("already dealt-with") suppression. (Credential-at-rest + RLS already landed in M2.)

**Exit:** a digest shows a mix of Stories and Notes, priority-ordered, each with a human-readable
reason; "show the data behind this" renders the event timeline; clicking "don't care" demotes that
entity next tick; "wrong aggregation" splits the story for that user only.

---

### M5 — Production hardening (safe for users beyond yourself)

*Goal:* full product surface + safe for *other* users / multi-tenant operation. (Single-user live
dogfooding was already unlocked at M2 by RLS + credential-at-rest.)

**Build**
- **OAuth / app-install connect flow:** `GET /connect/{source}` (signed `state`) + callback (bind
  provider-account → subscriber → scope, encrypt creds) for GitHub App (tech §3A) — replaces the
  hand-seeded connections so users other than you can onboard.
- **Managed-KMS backend (optional):** swap the M2 interim sealed master key for `aws-sdk-kms` behind
  the existing `creds_ref` indirection — a backend change, not a schema change; plus rotation/revocation.
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

**Exit:** a real GitHub App install via the connect flow (no hand-seeding); SSRF probes blocked; a
multi-timezone "daily 8am" fires at each subscriber's local 8am; crash-injection tests pass. (Private
data is already provably RLS-confined since M2.)

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
  without Slack. **It also absorbs RLS + credential-at-rest, pulled forward** so that the instant
  private data and real creds exist they are isolated and encrypted — the price of dogfooding live
  private data early. This makes M2 the heaviest milestone, deliberately: it is the "safe to use my own
  account" gate.
- **M3 is isolated** because per-subscriber linking (blocking + connected-components + id-forwarding +
  asymmetric merge thresholds) is the single most intricate algorithm in the system; it deserves a
  milestone where the rest of the pipeline is stable underneath it.
- **M4 delivers the "trust" half** only once there are real stories to gate, explain, and correct.
- **M5 batches the *remaining* hardening** (OAuth onboarding, managed-KMS, SSRF, DST, observability,
  crash-sim) — load-bearing for *other users / multi-tenant production* but not for self-dogfooding.
  Because the isolation + secrets work was pulled into M2, **running your own live private data through
  M2–M4 is safe.** The remaining caveat is narrow: do not expose feed/URL-adding or onboard *other*
  users before M5 (SSRF + OAuth + managed-KMS + DST land there).

## 4. Definition of done for "v1"

**Dogfoodable on your own live private data from M2 onward** — RLS-isolated and creds-encrypted — with
the product getting richer through M3 (linking) and M4 (relevance/trust). Full multi-user v1 completes
at M5.

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
