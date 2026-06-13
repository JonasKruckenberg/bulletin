# Bulletin — Web Frontend Scope

**Status:** Scoping / design only (no code) — 2026-06-13
**Reads against:** `digest-system-design.md` (§10 transparency & control, §9 scheduling, §3A auth),
`digest-technical-architecture.md` (§3 roles, §3A connect/verify, §5.3 data model, §10.7 read API),
`IMPLEMENTATION-ROADMAP.md` (M2 current, M5 = read API + connect flow + deep-link view).

> **Why this doc, when the design track said "frontend is out of scope" (tech §1).** It still is, as a
> *rendering* track — this doc does not design pixels. It scopes the **web surface as a product and
> backend concern**: which read/write endpoints `serve` must grow, what they read, how auth/IDOR/RLS
> thread through the read path, and a delivery technology recommendation. The architecture already
> reserves the slot — `serve` is "webhook catcher **+ read API**" (tech §3) and M5 already owns
> "authenticated deep-link digest view + drill-down/feedback APIs" (roadmap M5, tech §10.7). This is
> about deciding **what that is** and **whether any of it comes forward of M5**.

---

## 0. The one thing to internalize

The product thesis has two halves: *suppress noise* (the pipeline) and *earn trust through transparency
and control* (design §1: "a filter that hides things is only useful if the user can see **why**, drill
into the data behind it, and **correct** it"). **The pipeline half ships over email. The trust half
cannot.** You cannot dump private content, per-item reasons, drill-down timelines, and feedback controls
into an inbox — design §10/§11.6 are explicit that email is a **notification + authenticated deep-link**,
never a content dump, precisely because the content is private and interactive.

So the web frontend is not a nice-to-have skin. **It is the delivery surface for the entire "earn trust"
half of the product.** Email is the doorbell; the web view is the house.

That reframes the "sooner than later" instinct correctly: the frontend's *floor* (a read-only
authenticated digest view behind the email link) is the thing that makes M3 (linking) and M4 (relevance +
feedback) *demoable as a product* rather than as `psql` queries. Its *ceiling* (connection management, the
OAuth connect flow, preferences) is genuinely M5 and can stay there.

---

## 1. Surfaces (what the frontend is, enumerated)

Five surfaces, in dependency order. Each maps to backend endpoints `serve` must grow and to data that may
or may not exist yet.

| # | Surface | What it shows / does | Backend need | Data ready? |
|---|---|---|---|---|
| **S1** | **Digest view** (the deep-link target) | One digest: the priority-ordered list of Stories (rich cards: headline, timeline, backing links) and Notes (compact one-liners), in the subscriber's tz. The thing the email links to. | `GET /d/{digest_id}` (read), session/token auth | **Partial.** Today every item is a Story-equivalent rendered to email (M1). Story/Note split + priority order is **M4**. |
| **S2** | **Drill-down / provenance** | "Show the data behind this story": walk `story.clusters → group_key → events`, render the event timeline with per-event `links[]`. Plus the per-item **reason record** ("why you're seeing this"). | `GET /s/{story_id}` (timeline), reasons read | **Not yet.** `story` table + `reasons` are **M3/M4**. |
| **S3** | **Feedback / control** | "Care more/less" on an entity; "wrong aggregation" (split/cannot-link); mute. Append-only log that tunes the *next* tick. | `POST` feedback endpoints, CSRF, IDOR re-check | **Not yet.** Feedback log + affinity/edge-constraint consumers are **M4**. |
| **S4** | **Connections & connect flow** | List your connections + status (active/paused/revoked/errored); add one via the OAuth/app-install dance (`/connect/{source}` signed-`state` → callback binds provider-account→subscriber→scope). | `/connect/{source}` + callback (tech §3A), connection list read | **Backend partial.** `connection` rows + status exist (M2, hand-seeded); the *flow* is **M5**. |
| **S5** | **Preferences / account** | Recurrence (daily/weekly, local `at_time`, weekday, tz), name, pause/resume, unsubscribe. | CRUD on `subscriber`/recurrence | **Backend partial.** Subscriber + recurrence land with M5 scheduling; `debug subscriber-rm` exists today. |

**Phasing falls out of the dependency column, not out of taste:** S1 (read-only) is the keystone and can
come forward the moment there's a digest worth viewing; S2 needs M3's `story` table; S3 needs M4's feedback
consumers; S4/S5 are squarely M5 (they're the "other users can onboard" gate the roadmap already draws).

---

## 2. Where it sits in the architecture

Nothing new in the topology — the frontend lives inside the **existing `serve` role** (tech §3: axum,
stateless, scale-out). `serve` today is one `/health` route (`crates/bulletin/src/main.rs`); M2 adds the
webhook catcher (`POST /webhooks/{source}`); this adds the **read/write app endpoints** alongside them.
Same workspace, same domain crates, **one image** (tech §3 "split roles onto separate processes later").

```
serve (always-on, stateless)
  POST /webhooks/{source}   → verify(raw) → enqueue ProcessWebhook → 2xx     [M2, exists/landing]
  GET  /health              → ok                                             [exists]
  ── new: the app surface ──
  GET  /d/{digest_id}       → render digest (S1)                             ┐
  GET  /s/{story_id}        → provenance timeline + reasons (S2)             │ read path —
  POST /feedback/…          → append feedback log (S3)                       │ runs in the
  GET  /connections         → list + status (S4)                            │ SUBSCRIBER
  GET  /connect/{source}    → OAuth/app-install start (S4)                   │ RLS context
  GET  /connect/{source}/cb → callback, bind + encrypt creds (S4)           │
  …prefs/account (S5)                                                        ┘
```

### The load-bearing constraint: the read path runs under RLS, in the subscriber's context

This is the single most important architectural fact for the frontend, and it's *already decided* for the
write side — the frontend just has to honor it on the read side. Per design §12 / tech §6, every scoped
DB access goes through the `with_scope(ctx, …)` wrapper as the **only connection path**, under a non-owner
runtime role with `FORCE ROW LEVEL SECURITY` and no `BYPASSRLS`. `GenerateDigest` already runs in the
subscriber context (`SET LOCAL app.subscriber_id`).

**The read endpoints must do exactly the same.** `GET /d/{digest_id}` resolves the session → subscriber,
opens the connection under *that* subscriber's scope, and only then reads. This gives the frontend a
defense-in-depth property the email path never needed: even a logic bug that fetches another tenant's
`story_id` returns **nothing**, because RLS filters it at the DB. The typed `Scope` + scope-invariant
property test are the primary defense; RLS is the backstop — same posture as the build path (roadmap M2).

**Therefore: never trust a path-supplied id.** `/{digest_id}` and `/{story_id}` are IDOR targets. The
re-check is "load under the session's RLS context and 404 if empty," not "compare `owner_id` in app code"
— the same IDOR-defense shape as the webhook router resolving `installation.id` against *our* row, never a
payload id (tech §3A).

---

## 3. The data the frontend reads (and the gating dependency)

The read endpoints are **pure projections over already-materialized rows** — the frontend computes nothing,
it renders the CQRS read side (design §3.0). What it can render is bounded by what the pipeline has built:

- **`digest` + `digest_item`** (exists, M1) — the selected, ordered item list. `digest_item.reasons`
  (the "why") is **M4**.
- **`Story`** (tech §5.3) — `id` (stable, the deep-link/feedback target), `clusters: Vec<ClusterRef>`
  (membership + per-member `link_reason`), and the cross-source rollup (`source_diversity` = the "across
  sources" value, `content_depth` = Story-vs-Note, severity, entities, time span). **M3.**
- **`Cluster` → events** (M1/M2) — the drill-down walk: `story.clusters → group_key → events`, each event
  carrying `links[]` for the timeline. The events exist now; the `story`-level walk needs M3.
- **`Note`** — the compact format; the Story/Note classification is **M4** (richness classifies format).

**Implication for "sooner than later":** a frontend built **today** can only render M1's flat list (every
item a degenerate Story, no reasons, no real timeline, no Notes). That's a thin but real **S1 skeleton** —
useful to stand up the `serve` app shell, session/auth, RLS-scoped read path, and the email→deep-link
swap. The *valuable* frontend (rich Stories, provenance, feedback) **lights up as M3/M4 land**, because
the frontend is a window onto data those milestones produce. **Build the S1 shell early; let the content
fill in as the pipeline thickens** — exactly the roadmap's "spine first, thicken it" strategy applied to
the read surface.

---

## 4. AuthN / AuthZ (the part email never had to solve)

Email delivery needs no sessions; the moment there's a web view of private content, it does. Two linked
problems:

1. **Getting *into* the view from an email** (the deep-link). The email is a notification; the link must
   authenticate without a password prompt every time. Options:
   - **Signed, scoped, expiring deep-link token** (recommended for S1): the email link carries a token
     bound to `(subscriber_id, digest_id, exp)`, HMAC-signed with an app secret (the same `secrecy`/sealed
     master-key machinery M2 already builds for webhook secrets — no new secret backend). Clicking it
     mints a short session. Single-purpose, revocable by rotating the key, and it means the **email never
     contains a bearer of all your data** — only a link to one digest.
   - **Magic-link sign-in** (recommended for S3+): email is already the trusted channel and the only
     identity we hold, so a magic-link/OTP flow is the natural full-session login — no password store, no
     OAuth-IdP dependency. Reuse the existing transport.
   - Passwords / third-party SSO: **deferred** — they add a credential store or an IdP dependency for no
     v1 value when email already *is* the identity.

2. **Authorizing each object** (IDOR). Covered in §2: load under the session's RLS context, 404 on empty.
   App-level `owner_id` checks are the belt; RLS is the suspenders; never the path id (tech §3A IDOR rule).

**Write-side hygiene:** feedback/connect/prefs are state-changing `POST`s → **CSRF protection** (the read
endpoints are safe/idempotent and need none). The OAuth connect flow's `state` is already a signed
anti-CSRF nonce by design (tech §3A) — same pattern, reused for the app's own forms.

---

## 5. Delivery technology — recommendation

Three coherent options. The repo's stated constraints decide it: **Rust-first** (tech §1), **modular
monolith / one image** (tech §3), **"don't over-async," minimal always-on surface** (tech §1), and a read
side that is **pure server-side projection over Postgres** (§3 above) with **no client-held private data**.

| Option | What | Fit | Cost |
|---|---|---|---|
| **A. Server-rendered Rust + hypermedia** ✅ **recommended** | `askama`/`maud` templates rendered in `serve`, progressive interactivity via **htmx** (drill-down expand, feedback POST → swap). No build step, no JS bundle, no API/JSON contract to version. | **Best.** Stays in-repo, in the one image, in the existing axum role. Private data never leaves the server. Matches "the read side is a projection" — the HTML *is* the projection. Minimal new always-on surface. | Low. Some template work; htmx ceiling is real but well past v1's needs. |
| **B. Rust full-stack (Leptos / Dioxus)** | Rust end-to-end with a reactive client (WASM) + server functions. | Good Rust-alignment, but pulls a WASM toolchain, a client runtime, and a hydration model into a project whose interactivity is "expand a timeline, click a feedback button." Over-tooled for the surface. | Medium-high. Build pipeline, bigger image/asset story, more moving parts against "don't over-engineer." |
| **C. JS SPA (React/Svelte) + JSON API** | Separate frontend app consuming a versioned REST/GraphQL read API. | Worst fit for v1: forces a **stable JSON API contract** and an **auth-token story for a client holding private data** — exactly what the deep-link token model avoids. Two languages, two deploys, CORS, the lot. | High. A second app, a second runtime, an API surface to secure and version. Defer until/unless a genuine rich-client need appears. |

**Recommendation: Option A — *with the read layer kept API-ready* (see §5a).** Server-rendered Rust +
htmx collapses the web frontend into the existing `serve` role with no new language, no build step, no
client-side private data, and no API contract to maintain — and the product's *web* interactivity (expand
provenance, submit feedback, manage connections) is squarely in htmx's sweet spot. This keeps the web
frontend faithful to "the read side is a pure projection over Postgres" — the rendered HTML *is* that
projection.

The one caveat that changes how you *build* A (not whether you pick it) is the planned native app — §5a.

---

## 5a. Forward-looking: the native app + live updates

> **Stated intent:** "we'll need this to probably be an app too, because we want elegant live behind-the-
> scenes updating of user timezone etc." Taken seriously, this is the strongest argument *against* the
> "no API contract" framing above — so address it now, in how F0 is built, not as a later rewrite.

A native (mobile/desktop) app introduces two things the web-only Option A conveniently avoided:

1. **A typed read contract.** A native client cannot consume server-rendered HTML; it needs **JSON
   projections** (digest, story/timeline, reasons, connection status) and **write endpoints** (feedback,
   prefs) it can call. That is exactly the JSON API that Option C would have forced up front and Option A
   defers. The app makes it *not* deferrable forever.

2. **A live push channel.** "Elegant live behind-the-scenes updating" means the client reflects state
   changes without a manual refresh — a new digest is ready, a connection went `errored`, the timezone
   silently changed. That's **SSE or WebSocket** from `serve`, which the always-on `serve` role can host
   but the design's "only Postgres + webhook catcher always-on / don't over-async" thesis (tech §1) means
   you add **deliberately**, when the app needs it — not speculatively.

**The timezone example, concretely** — it's a real feature, not decoration, and the backend already has the
machinery: the app reads the **device timezone**, and when it differs from the stored `subscriber.timezone`
it silently `PATCH`es it. The scheduler's existing **preference-change boundary function** — "`ref =
max(now, last_delivered)`, snap to the next earliest slot, lose nothing" (design §9.2, tech §13) — recomputes
`next_run_at` DST-safely, so "daily 8am" keeps firing at 8am *local* as the user travels, with no missed or
double digest. The "live" part is just the app pushing the tz change behind the scenes + an SSE nudge when
the next-run preview updates. **No new scheduling design — it's an API + push veneer over §9.2.**

**What this means for the build (the actionable part):**

- **Keep the read projections behind a thin serialization-neutral boundary.** Put no business logic in the
  htmx templates: a handler should produce a **typed projection struct** (e.g. `DigestView`, `StoryView`),
  and templates render it to HTML. The day the app arrives, the *same* projection serializes to JSON via a
  second responder — HTML for web, JSON for app — over **one read path, one RLS context, one IDOR re-check**.
  This is the difference between "add a JSON responder" and "rewrite the read layer." It costs ~nothing now
  and is the single most important structural decision in this whole doc for the app future.
- **Don't build the JSON API or the push channel in F0** — building them speculatively violates the
  minimal-always-on-surface thesis. Build only the projection *boundary* now; add JSON + SSE when the app
  is actually being built (a new phase, **F4 — native-app read API + live channel**, after F3/M5).
- **The deep-link token / magic-link auth (§4) generalizes to the app cleanly** — the app does the same
  magic-link/OTP sign-in and holds a session token; no new identity system. The IDOR-under-RLS rule (§2)
  protects the JSON endpoints identically to the HTML ones.

**Revised stance:** still ship **A** for the web (it's right for the web surface and for F0–F3), but build
it **projection-first** so the native app is an *additive* JSON+SSE layer over the same read path — never a
fork. If, when the app lands, the web surface has grown genuinely rich-reactive needs too, that is the
moment to weigh a shared client (B/C) — but you'll be deciding it with a real API contract and real
requirements in hand, not speculatively.

---

## 6. Proposed phasing (mapped onto the existing roadmap)

This does **not** add a milestone; it threads the frontend through the ones already planned, pulling only
the read-only shell forward.

- **F0 — `serve` app shell + S1 read-only skeleton** *(can come forward of M5, alongside M3)*
  Stand up the axum app surface in `serve`: session plumbing, the **deep-link token** mint/verify, the
  **RLS-scoped read connection** (`with_scope` on the read path), and `GET /d/{digest_id}` rendering
  whatever the digest currently holds (M1 flat list at first). Swap the email body from content-dump to
  **notification + deep-link** (design §11.6 — this is the email change that *requires* the view to exist).
  *Exit:* the email links to an authenticated web digest that renders the same items, scoped by RLS; a
  cross-tenant `digest_id` 404s.

- **F1 — S2 provenance, as M3 lands.** `GET /s/{story_id}` walks `clusters → events` into a timeline;
  render per-item `link_reason`. Lights up automatically once the `story` table exists.

- **F2 — S3 feedback, as M4 lands.** Wire the "care more/less" / "wrong aggregation" / mute controls to the
  append-only feedback log (htmx POST + CSRF + IDOR re-check). The "earn trust → control" half goes live.
  Render `digest_item.reasons` ("why you're seeing this") next to each item.

- **F3 — S4/S5 management, at M5.** The OAuth/app-install **connect flow** (`/connect/{source}` + callback),
  connection list/status, and preferences/recurrence editing — the "other users can onboard" surface the
  roadmap already places at M5. Magic-link full sign-in lands here (S1's deep-link token is enough before).

- **F4 — native-app read API + live channel** *(post-M5, when the app is actually built)*
  Add a JSON responder over the *same* projection structs (§5a) — HTML for web, JSON for app, one read path
  — plus an SSE/WebSocket push channel on `serve` for "new digest ready / connection errored / tz updated"
  nudges, and the silent device-tz `PATCH` → §9.2 boundary recompute. **No new scheduling or auth design;**
  it's an additive serialization + push layer, *if* F0 was built projection-first.

**Net:** only **F0** is a pull-forward, and it's a small, high-leverage one — it unblocks demoing M3/M4 as a
product and forces the email→deep-link swap that the design already mandates. F1–F3 are the *UI side* of
milestones already scheduled, lighting up as their data is built. **F4 (the app) is genuinely later** — its
*only* cost imposed on today is the one cheap structural rule in §5a: **build F0 projection-first** so the
app is an additive layer, never a fork.

---

## 7. Security checklist (the deltas a web surface introduces)

- **RLS on the read path** — every read through `with_scope(session.subscriber)`; non-owner role, no
  `BYPASSRLS`, `FORCE RLS`. The view inherits the build path's isolation posture (design §12). *Primary.*
- **IDOR** — path ids (`digest_id`, `story_id`) re-checked by loading under RLS and 404-ing on empty; never
  an app-code `owner_id` compare alone, never trust the path id (tech §3A).
- **Deep-link tokens** — scoped to one `(subscriber, digest, exp)`, HMAC-signed with the sealed app secret
  (reuse M2's `secrecy`/master-key machinery; no new secret backend), short TTL, key-rotation revokes.
- **No private content in email** — the email is the notification; private Stories/Notes/reasons live only
  behind the authenticated view (design §11.6). The F0 email swap is what *enforces* this.
- **CSRF** on all state-changing `POST`s (feedback, connect, prefs); the connect flow's signed `state` is
  the same nonce pattern (tech §3A). Read endpoints are safe and need none.
- **Session security** — short-lived, `HttpOnly` + `Secure` + `SameSite` cookies; logout/rotate;
  rate-limit magic-link/OTP issuance.
- **SSRF** — unchanged by the frontend, but note the connect flow (F3/S4) is where user-supplied
  feed/connection URLs first enter via UI; the M5 SSRF guard (resolve-then-pin, RFC1918 denylist, redirect
  re-validation — tech §6) must gate it before any non-you user can add a source (roadmap M5).
- **Privacy of the link itself** — a deep-link in an email transits the user's mail provider; the token is
  single-digest + expiring precisely so a leaked link is bounded, not a master key.

---

## 8. Open decisions (for you)

1. **Pull F0 (read-only digest view) forward to sit alongside M3, or hold the entire frontend to M5?**
   Recommendation: pull F0 forward — it's small, it unblocks demoing M3/M4 as a product, and it forces the
   email→deep-link swap the design already requires. Everything richer stays where its data is.
2. **Delivery tech: confirm Option A (server-rendered Rust + htmx), built *projection-first* for the future
   app (§5a)?** vs Leptos/Dioxus (B) or a JS SPA (C). Recommendation: A, with the read layer kept
   serialization-neutral so the planned native app's JSON+SSE is additive (F4), not a rewrite. The one
   non-negotiable this implies *today*: handlers produce typed `*View` structs, templates only render them.
3. **Auth for S1: deep-link token only, or magic-link session from day one?** Recommendation: deep-link
   token for F0 (smallest surface that works), magic-link added at F2/F3 when there are write actions and
   repeat visits to justify a full session.
4. **Does the frontend get its own crate** (e.g. `crates/web` with the templates/handlers) **or live inside
   the `bulletin` binary's `serve` module?** Defer to the M2 crate-graph-finalize pass (roadmap §5) — but
   Option A keeps it small enough to start inside `serve` and split later.
5. **Branding/theme ownership** — the email renderer already externalizes brand/masthead/footer
   (`DigestContent`, `render.rs`); the web view should share that config so email and web stay one identity.
