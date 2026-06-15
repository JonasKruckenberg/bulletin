# Bulletin — Service API (gRPC)

**Status:** Design only (no code yet) — 2026-06-14
**Reads against:** `digest-system-design.md` (§3A auth, §10 transparency & control, §12 RLS),
`digest-technical-architecture.md` (§3 roles, §3A connect/verify/IDOR, §10.7 read API),
`digest-web-frontend.md` (§5a *typed read contract* + §2 RLS read path), `README.md` (Data Flow,
ops/seeding), and the engine surface in `crates/core` (the pure, RLS-scoped store fns).

> **Why this doc.** Today the *only* way to drive the service is the `bulletin debug …` CLI (operator
> commands) plus hand-seeded rows. That's fine for one operator on one box; it does not let us build
> tools, dashboards, or frontends. `digest-web-frontend.md` §5a already named the missing piece — "a
> typed read contract … the JSON API that the app makes not-deferrable-forever" — and parked it at
> phase F4. This doc **pulls that contract forward and pins the transport to gRPC**, because the
> immediate goal is *programmatic* access for tools and future frontends, not pixels.

---

## 0. The one thing to internalize

The API is not a new subsystem — it is a **second trigger** over the engine that already exists. `lib.rs`
is explicit: "Each flow exposes a pure entry function over the DB … Nothing here knows about the trigger
(apalis/cron) or metrics — that's the binary's job." The CLI `debug` dispatcher and the cron/worker are
two existing triggers. **The API is a third.** It adds *no* business logic; it authenticates a caller,
resolves them to a `ScopeCtx`, and calls the same `core` functions the CLI already calls — only over the
wire instead of `argv`.

That reframes the whole effort: the hard part is **not** "design endpoints," it is **"map an
authenticated caller onto the `ScopeCtx` the RLS layer already enforces"** (`common/db.rs`). Get that seam
right and tenant isolation is free — the database refuses cross-tenant reads regardless of a handler bug,
exactly as it does for the build and digest paths today (design §12).

---

## 1. Decisions locked (this session)

| Axis | Decision | Consequence |
|---|---|---|
| **Audience** | **Both planes, layered** | An **admin plane** (`ScopeCtx::Admin`, mirrors `debug` — manage connections/subscribers, ops/status) **and** a **subscriber plane** (`ScopeCtx::Subscriber`, sees only its own digests/stories/prefs). Build operator tools now, subscriber frontends next, no auth re-architecture. |
| **Surface** | **Management + read** | CRUD for connections & subscribers + read access to digests/stories/status. **Out of first cut:** pipeline-trigger verbs (`build-run`, `digest-run/dispatch`, `digest-explain`) — those are operator-only and stay on the CLI for now (see §12, deferred). |
| **Transport** | **Plain gRPC (tonic)** | HTTP/2, protobuf contract, generated typed clients. Browser support (gRPC-web / Connect) is explicitly **deferred** — acknowledged limitation, "for now" native/tooling clients only. Streaming is available when we want it (§8) but not built in the first cut. |

---

## 2. Where it sits in the architecture

A **new always-on role**, `api`, peer to `serve` / `worker`. It runs a tonic gRPC server on its own port
and shares the same workspace, same `core` crate, same image (tech §3: "split roles onto separate
processes later"). It does **not** share a port with the `serve` webhook catcher: that endpoint is
public-facing HTTP/1.1 from GitHub, the gRPC API is HTTP/2 and (initially) internal/trusted-tooling —
different audiences, different exposure, keep them on separate listeners.

```
bulletin api      → tonic gRPC on BULLETIN_API_ADDR (default 127.0.0.1:50051)   [NEW]
bulletin serve    → axum: /health + POST /webhooks/github                       [exists]
bulletin worker   → apalis jobs + Prometheus exporter                           [exists]
bulletin all      → serve + worker + api joined (tokio::try_join!)              [extend]
```

- **Crate placement.** The gRPC server is transport, so it is **binary-side**, never in `core` (which
  stays trigger-agnostic). Two options: (a) a module `crates/bulletin/src/api/` with protos compiled by
  the `bulletin` crate's `build.rs`; (b) a dedicated `crates/api` lib crate the binary mounts. Recommend
  **(a) to start** — it is the smallest diff and matches how `webhook.rs` / `worker.rs` already live in
  the binary; split to a crate later if it grows (mirrors the web-frontend doc's "start inside `serve`,
  split later" call, §12.4 there).
- **`main.rs` change.** Add `Command::Api`, a `BULLETIN_API_ADDR` arg (default `127.0.0.1:50051`), and an
  `api::serve(addr, pool, auth_config)` bootstrap; join it into `Command::All`.

---

## 3. The auth model → `ScopeCtx` (the load-bearing seam)

Every RPC resolves its caller to exactly one `ScopeCtx` and runs **all** its DB work through
`with_scope(pool, ctx, …)` / `begin_scope` (`common/db.rs`) — the same chokepoint the build and digest
paths use. A tonic **interceptor** does the resolution once per request and stashes an `AuthIdentity` in
the request extensions; handlers read it.

### Two credential kinds → two planes

| Credential | Presented as | Resolves to | Plane |
|---|---|---|---|
| **Admin key** | `authorization: Bearer <admin-key>` | `ScopeCtx::Admin` | Admin |
| **Subscriber token** | `authorization: Bearer blt_<random>` | `ScopeCtx::Subscriber(id)` | Subscriber |

- **Admin key** — a static high-entropy key from config (`BULLETIN_API_ADMIN_KEY`), sealed at rest with
  the **existing** master-key/`secrecy` machinery the GitHub webhook secret already uses (`secrets.rs`,
  README "GitHub source") — *no new secret backend*. Constant-time compared. (A small set/rotation list
  is a trivial extension.)
- **Subscriber token** — an opaque bearer `blt_<base64url-random>`. Only its **SHA-256 hash** is stored
  (it is high-entropy, so a fast hash is right — this is an API token, not a password). Issued by an
  admin RPC (`AdminService.IssueSubscriberToken`) and, later, self-service. Resolution: hash the
  presented token → look it up in `api_token` → `subscriber_id` → `ScopeCtx::Subscriber(id)`. The lookup
  itself is a cross-tenant read of a control-plane table, so the interceptor performs it under
  `ScopeCtx::Admin` (control-plane reach is exactly Admin's job, design §12); the *handler* then runs
  under `Subscriber(id)`.

### The isolation property worth stating out loud

`ScopeCtx::Admin` **cannot read another tenant's private content** — by design, the content-table RLS
policies treat Admin like the no-subscriber context (public only; `common/db.rs` doc-comment). So:

- **`AdminService` only ever serves control-plane + public data** — connection rows, subscriber roster,
  status, public events, digest *metadata*. If an admin handler tried to read a subscriber's private
  Stories it would get **nothing** — the DB refuses.
- **Private digest content is served *only* by `SubscriberService`, under that subscriber's own token.**
  There is therefore no API path — and no token — that turns the operator plane into a cross-tenant
  content backdoor. The API inherits the build path's isolation posture for free; this is a feature, not
  a limitation to work around.

---

## 4. The contract (`bulletin.v1`)

Two services, one per plane. Proto sketch (illustrative field sets, not final wire layout):

```proto
syntax = "proto3";
package bulletin.v1;
import "google/protobuf/timestamp.proto";

// ── Admin plane → ScopeCtx::Admin (control-plane + public only) ──────────────
service AdminService {
  // Connections (mirrors `debug connection-add/list/rm`)
  rpc ListConnections   (ListConnectionsRequest)   returns (ListConnectionsResponse);
  rpc CreateConnection  (CreateConnectionRequest)  returns (Connection);
  rpc DeleteConnection  (DeleteConnectionRequest)  returns (DeleteConnectionResponse);
  // Subscribers (mirrors `debug subscriber-add/list/rm`)
  rpc ListSubscribers   (ListSubscribersRequest)   returns (ListSubscribersResponse);
  rpc CreateSubscriber  (CreateSubscriberRequest)  returns (Subscriber);
  rpc DeleteSubscriber  (DeleteSubscriberRequest)  returns (DeleteSubscriberResponse);
  rpc IssueSubscriberToken (IssueSubscriberTokenRequest) returns (IssueSubscriberTokenResponse);
  // Ops / read (mirrors `debug status`, `event-list`, `digest-list` — metadata only)
  rpc GetStatus         (GetStatusRequest)         returns (StatusReport);
  rpc ListEvents        (ListEventsRequest)        returns (ListEventsResponse);
  rpc ListDigests       (ListDigestsRequest)       returns (ListDigestsResponse);
}

// ── Subscriber plane → ScopeCtx::Subscriber(id) (own private content) ────────
service SubscriberService {
  rpc GetMe             (GetMeRequest)             returns (Subscriber);
  rpc UpdatePreferences (UpdatePreferencesRequest) returns (Subscriber);   // freq/tz/time/max_items
  rpc ListMyConnections (ListMyConnectionsRequest) returns (ListConnectionsResponse);
  rpc AddMyConnection   (AddMyConnectionRequest)   returns (Connection);    // own RSS / GitHub
  rpc ListMyDigests     (ListMyDigestsRequest)     returns (ListDigestsResponse);
  rpc GetMyDigest       (GetMyDigestRequest)       returns (DigestView);     // items → stories
  rpc GetStory          (GetStoryRequest)          returns (StoryView);      // provenance/timeline
}

// Shared messages: Connection, Subscriber, StatusReport (mirrors core::status),
// DigestView { repeated StoryView stories; repeated NoteView notes; … },
// StoryView { headline; summary; repeated EventRef timeline; … }.  Timestamps are UTC;
// the subscriber's tz travels on Subscriber so clients render local time.
```

Notes:

- **The contract mirrors the domain vocabulary** already in `core` (`StatusReport` and its sub-structs in
  `common/status.rs`, `SubscriberRow`, `ConnectionRow`, the digest/story render types). A `convert.rs`
  module owns the domain↔proto mapping in one place.
- **`StoryView` / `DigestView` are the §5a "typed projection structs" realized in protobuf** — the read
  side is a pure projection (design §3.0), so these carry no logic, only already-materialized rows.
- **List RPCs take a limit/cursor** so paging is in the contract from day one (the CLI's bare `--limit`
  generalizes to a `page_token`).

---

## 5. RPC → core mapping, and the one core refactor

Almost every RPC is a thin wrapper over a function that exists today:

| RPC | Core call (today) | Scope |
|---|---|---|
| `AdminService.ListConnections` | `ingest::store::list_connections` | Admin |
| `AdminService.CreateConnection` | `ingest::store::insert_connection` | Admin |
| `AdminService.DeleteConnection` | `ingest::store::delete_connection` | Admin |
| `AdminService.ListSubscribers` | `digest::subscriber::list_subscribers` | Admin |
| `AdminService.CreateSubscriber` | `digest::subscriber::insert_subscriber` | Admin |
| `AdminService.GetStatus` | `status::gather` | Admin |
| `AdminService.ListEvents` | `ingest::store::list_events` | Admin |
| `AdminService.ListDigests` | `digest::store::list_digests` | Admin |
| `SubscriberService.GetMe` | `digest::subscriber::load_subscriber` | Subscriber |
| `SubscriberService.UpdatePreferences` | `digest::subscriber::update_preferences` | Subscriber |
| `SubscriberService.ListMyDigests` | `digest::store::list_digests` (own, RLS-filtered) | Subscriber |
| `SubscriberService.GetMyDigest` | `digest::store::render_items*` | Subscriber |

**The scoped-core question — already solved (verified during A1).** The original premise of this doc was
that the store fns run on a bare `pool` with no scope GUC and would need an executor-generic refactor.
Reading the code showed that is **not** the case: every control-plane store fn already opens its **own**
`begin_scope` transaction in the correct context — `list_connections` / `insert_connection` /
`insert_subscriber` / `list_subscribers` / `delete_*` and `status::gather` / `list_digests` all pin
`ScopeCtx::Admin`; `load_subscriber` / `update_preferences` pin `ScopeCtx::Subscriber(id)` from their `id`
argument. So the API handlers just call these fns directly with `&pool` and inherit the right scope for
free — no refactor, no `with_scope` ceremony in the handlers. The only bare-pool reader is
`ingest::store::list_events`, which touches the `event` **content** table (public-only under any context),
so it is correct as-is. **A0 was therefore already satisfied by the engine; the first cut is purely the
gRPC glue (A1).**

---

## 6. Error mapping

A single `fn to_status(err: anyhow::Error) -> tonic::Status` keeps handlers tidy and avoids leaking
internals:

- not-found / empty (incl. an IDOR miss that RLS turned into "no rows") → `NotFound`
- bad input (unparseable tz/time, unknown source, malformed config JSON) → `InvalidArgument`
- missing/!valid token → `Unauthenticated`; wrong plane for the RPC → `PermissionDenied`
- everything else → `Internal` with a generic message; the real error is `tracing::error!`-logged
  server-side, never returned.

Domain validation that already produces friendly errors in `debug.rs` (e.g. "a github --config needs an
integer installation_id", private-source-must-be-owned) moves into the handler/convert layer and maps to
`InvalidArgument`.

---

## 7. Token storage (new migration)

A new append-only-ish control-plane table, RLS-protected like the rest:

```sql
-- 20200101000022_api_token.sql  (additive, expand-only)
CREATE TABLE api_token (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  subscriber_id uuid NOT NULL REFERENCES subscriber(id) ON DELETE CASCADE,
  token_hash    bytea NOT NULL UNIQUE,         -- SHA-256 of the opaque bearer
  name          text,                          -- operator label, e.g. "ios app"
  created_at    timestamptz NOT NULL DEFAULT now(),
  last_used_at  timestamptz,
  revoked_at    timestamptz
);
-- RLS: a Subscriber context sees only its own rows; Admin manages all; no-subscriber denied.
-- Same two-context policy shape as `subscriber` / `connection` (migration 20…_rls.sql).
```

The plaintext token is shown **once** at issuance (`IssueSubscriberTokenResponse.token`) and never again.
Resolution compares `token_hash`; `revoked_at IS NULL` gates validity; `last_used_at` is a best-effort
touch. Grants for `bulletin_app` flow through the existing `grant_runtime_role` re-grant on `migrate`.

---

## 8. Streaming (deferred, but the contract leaves room)

The user flagged streaming as "beneficial later, not a must." gRPC server-streaming is the natural fit
when we want it; nothing in the first cut blocks it:

- `rpc WatchStatus(stream) ` — live ops dashboard (push `StatusReport` deltas).
- `rpc WatchDigests(stream)` — "a new digest is ready / connection went errored" nudges (the §5a/F4 live
  channel, now over gRPC instead of SSE — the **dashboard S6** in the web doc is the first consumer).

These ride the same interceptor/`ScopeCtx` machinery; add them when a client needs them, not
speculatively (consistent with the design's "minimal always-on surface" thesis).

---

## 9. Crate / build layout & dependencies

```
crates/bulletin/
  proto/bulletin/v1/bulletin.proto         # the contract
  build.rs                                  # tonic-build compiles proto → OUT_DIR
  src/api/
    mod.rs        # serve(addr, pool, auth) bootstrap; mounts both services + reflection/health
    auth.rs       # the interceptor: metadata → AuthIdentity{ScopeCtx, plane}
    admin.rs      # AdminService impl
    subscriber.rs # SubscriberService impl
    convert.rs    # domain ↔ proto
    error.rs      # to_status
```

New deps: `tonic`, `prost`, `prost-types`; build-dep `tonic-build`; `tonic-reflection` (tooling/grpcurl)
and `tonic-health` (standard `grpc.health.v1`) are cheap, standard add-ons. `sha2` for token hashing
(or reuse what `secrets.rs`/`common::secret` already pulls in).

---

## 10. Versioning & tooling

- Package `bulletin.v1`; breaking changes go to `v2`, never edit `v1` wire layout (same discipline as the
  append-only migrations).
- Ship the **reflection service** so `grpcurl`/IDE tooling can introspect without the `.proto`, and the
  standard **health service** so orchestration/liveness has a gRPC probe alongside `serve`'s `/health`.

---

## 11. Security checklist (deltas a programmatic API introduces)

- **Auth on every RPC** — no anonymous methods; the interceptor rejects missing/invalid bearers with
  `Unauthenticated` *before* the handler. Health/reflection are the only unauthenticated services, and
  only because the listener is internal (§2).
- **Plane enforcement** — a subscriber token on an `AdminService` method → `PermissionDenied`; an admin
  key cannot impersonate a subscriber to read private content (RLS forbids it anyway — §3).
- **RLS is the backstop, not the gate** — every handler runs in `with_scope`; an IDOR (`GetStory` with
  someone else's `story_id`) returns `NotFound` because RLS yields zero rows, never an app-code owner
  compare (tech §3A IDOR rule, same posture as the web read path, web-frontend §2).
- **Tokens at rest** — only SHA-256 hashes stored; plaintext shown once; revocation is a column, not a
  delete (audit trail). Admin key sealed via the existing master-key machinery, never the Nix store.
- **Transport** — TLS terminates at the edge/reverse-proxy in front of `api` (or directly, later); the
  default bind is loopback so nothing is exposed until deliberately fronted.
- **Input validation** — `installation_id`, tz, time, source-kind, config-JSON all validated at the
  convert layer → `InvalidArgument`; the private-source-must-be-owned CHECK still backs it at the DB.
- **SSRF** — unchanged here, but `AddMyConnection` is a place a user-supplied feed URL enters; it inherits
  the M5 SSRF guard requirement (resolve-then-pin, RFC1918 denylist) before non-operator subscribers can
  add sources (web-frontend §11).

---

## 12. Phasing / plan of work

- **A0 — scoped core.** ✅ *Already satisfied by the engine* (see §5): the core store fns self-scope, so no
  refactor was needed. Confirmed while building A1.
- **A1 — the `api` role + admin plane.** ✅ *Implemented.* `proto/bulletin/v1/bulletin.proto` compiled
  protoc-free via `protox` + `tonic-prost-build` (`build.rs`); the admin-bearer auth helper
  (`src/api/auth.rs`, fail-closed, constant-time, unit-tested); `AdminService` over the self-scoping core
  fns (`src/api/admin.rs`); domain↔proto in `convert.rs`; gRPC reflection + `grpc.health.v1`; `Command::Api`
  + `BULLETIN_API_ADDR` / `BULLETIN_API_ADMIN_KEY`, joined into `All`. *Exit:* a `grpcurl` client with the
  admin key can list/create/delete connections & subscribers and read status/events/digests — operator
  parity with `debug` (management + read) over the wire.
- **A2 — subscriber plane.** `api_token` migration, `IssueSubscriberToken`, token resolution in the
  interceptor, `SubscriberService` (`GetMe`, prefs, own digests/stories) under `ScopeCtx::Subscriber`.
  *Exit:* a subscriber token reads **only** its own digest content; a cross-tenant id `NotFound`s.
- **A3 (later) — streaming + operator verbs.** `WatchStatus`/`WatchDigests` (§8) when a client needs
  live; optionally expose the pipeline-trigger verbs (`build-run`, `digest-dispatch`, `digest-explain`)
  as an explicit **operator** sub-surface if/when a tool needs them (kept out of the first cut on
  purpose).
- **A4 (later) — browser reach.** gRPC-web / Connect shim in front of `api` when a web frontend needs to
  call it directly (the acknowledged "for now" gap).

---

## 13. Open decisions (for you)

1. **Crate placement — module in `bulletin` (start) vs `crates/api` (split now)?** Recommend module now,
   split later (smallest diff; matches `webhook.rs`/`worker.rs`).
2. **Self-service subscriber tokens — issue only via admin, or also a `SubscriberService` self-issue?**
   Recommend admin-only for the first cut; add self-issue with the frontend.
3. **Should `debug` be *replaced* by a thin gRPC client over `AdminService`,** or kept as the
   direct-DB operator path? Recommend keep `debug` direct for now (no daemon dependency for ops); revisit
   once `api` is always-on.
4. **Operator verbs (build/digest-run/explain) — confirm they stay CLI-only in the first cut** (this doc
   assumes yes; they're A3 if a tool needs them).
5. **TLS/exposure posture** — loopback + reverse-proxy (assumed) vs direct TLS in `api`. Decide when the
   API leaves the box.
</content>
</invoke>
