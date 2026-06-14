# Bulletin — Web Frontend Scope

**Status:** Scoping / design only (no code) — 2026-06-13
**Reads against:** `system-design.md` (§10 transparency & control, §9 scheduling, §3A auth),
`technical-architecture.md` (§3 roles, §3A connect/verify, §5.3 data model, §10.7 read API),
`roadmap.md` (M2 current, M5 = read API + connect flow + deep-link view).

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
| **S6** | **The "system working" dashboard** | A consumer-facing glass-box: a live, animated view of the pipeline doing its job for *you* — what it read, what it suppressed, what's next. Trust surface, not an operator console. | per-subscriber count reads (RLS) + global aggregates; **SSE for live (§5a)** | **Deferred to F4.** Counts exist now, but per the decision below we ship **only the live version** on the F4 channel — no static stopgap. **Detailed in §10.** |

**Phasing falls out of the dependency column, not out of taste:** S1 (read-only) is the keystone and can
come forward the moment there's a digest worth viewing; S2 needs M3's `story` table; S3 needs M4's feedback
consumers; S4/S5 are squarely M5 (they're the "other users can onboard" gate the roadmap already draws).

---

## 1a. Stepping back — the full UI inventory

The six surfaces above are the *content*. A real product UI is more than its content screens — it's the
connective tissue, the states, and the off-screen surfaces (emails, empty states, errors). Enumerated so
nothing is discovered late:

### Route / page map

```
/                         home → latest digest (or onboarding if none yet)         [S1]
/d/{digest_id}            a specific digest (the deep-link target)                  [S1]
/archive                  past digests (the back-catalogue; append-only, free)      [S1+]
/s/{story_id}             story provenance / timeline / reasons                     [S2]
/dashboard               the "system working" glass-box                            [S6]
/settings                schedule · content · delivery · account (tabbed)          [S5/§7]
/connections             list + status; add via connect flow                       [S4]
/connect/{source}[/cb]   OAuth/app-install dance                                   [S4]
/signin   /verify        magic-link request + token landing                       [§9]
/signup                  onboarding flow (multi-step)                              [§9]
/unsubscribe             one-click, token-authed (email-compliance)               [§7]
/legal/{privacy,terms}   static                                                    [—]
```

### Cross-cutting concerns (apply to *every* surface, easy to forget)

- **States, not just happy paths.** Each data surface needs **loading / empty / error / expired-link**
  variants. The empty states are load-bearing here because of cold-start (no digest yet, no connections
  yet) — see §9. The expired-deep-link state is its own screen ("this link's expired — sign in to see it").
- **Transactional email is part of the UI.** Beyond the digest itself: **verify/magic-link**, **welcome**,
  **connection-errored alert** ("your GitHub connection needs re-auth"), and the **digest notification**
  itself. The email renderer (`render.rs`, `DigestContent`) already externalizes brand/masthead/footer — all
  transactional mail should share that identity so email and web are one brand.
- **Notifications & the entry seam.** Email is the only channel today; the deep-link is the entry. The
  future app (§5a) adds push. Design the "there's something new" nudge as a channel-agnostic concept now.
- **Responsive / mobile-web first.** The deep-link is opened on a phone, from a mail client, more often than
  not. Mobile-web quality is also the cheapest rehearsal for the native app's layouts (§5a).
- **Accessibility & localization.** a11y from the start (semantic HTML — Option A's server-rendered
  hypermedia is a natural fit). The product is already timezone-aware end-to-end; UI copy/number/date
  formatting should respect locale, not just tz.
- **Session & account lifecycle.** Sign in / sign out / session expiry / "signed in on this device" — plus
  the account-deletion path (GDPR cascade, design §13) and **data export**. These are legal-surface, not
  optional, the moment there are users beyond you.
- **Trust surfaces are the differentiator.** Reasons ("why you're seeing this"), provenance (S2), and the
  dashboard (S6) are not decoration — they *are* the "earn trust" half (§0). Treat them as core, not polish.
- **Frontend observability + perf budget.** No client-held private data (§5); server-render keeps payloads
  small; basic real-user timing so the deep-link open is fast (it's opened from email, on mobile data).

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

**The timezone example, concretely (and DECIDED as app-only)** — continuous, silent tz tracking is the
canonical reason an app is wanted, and it is **explicitly gated on the app** — we will *not* ship a
half-working web version of it (a flaky browser heuristic that moves when your digest fires is worse than
nothing; pre-app, tz is manual with a one-time confirmed guess — §7, §9). The backend machinery already
exists for when the app lands: the app reads the **device timezone**, and when it differs from the stored
`subscriber.timezone` it silently `PATCH`es it. The scheduler's existing **preference-change boundary
function** — "`ref = max(now, last_delivered)`, snap to the next earliest slot, lose nothing" (design §9.2,
tech §13) — recomputes `next_run_at` DST-safely, so "daily 8am" keeps firing at 8am *local* as the user
travels, with no missed or double digest. The "live" part is just the app pushing the tz change behind the
scenes + an SSE nudge when the next-run preview updates. **No new scheduling design — it's an API + push
veneer over §9.2, and it's the reason the app exists rather than just a web view.**

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
  nudges, the **continuous auto-tz** device `PATCH` → §9.2 boundary recompute (app-only, §5a), and the
  **S6 "system working" dashboard** (deferred here on purpose — it ships *only* as the live version, on this
  channel — §10). **No new scheduling or auth design;** it's an additive serialization + push layer, *if*
  F0 was built projection-first.

**Net:** only **F0** is a pull-forward, and it's a small, high-leverage one — it unblocks demoing M3/M4 as a
product and forces the email→deep-link swap that the design already mandates. F1–F3 are the *UI side* of
milestones already scheduled, lighting up as their data is built. **F4 (the app) is genuinely later** — its
*only* cost imposed on today is the one cheap structural rule in §5a: **build F0 projection-first** so the
app is an additive layer, never a fork.

---

## 7. Settings, scoped

Settings is where "control" (the second half of the thesis) lives outside the digest itself. Group it so
each tab maps to a backend owner and a milestone — don't ship it as one flat form.

| Group | Controls | Reads/writes | Default | Lands |
|---|---|---|---|---|
| **Schedule** | freq (daily/weekly), `at_time`, `on_weekday`, **timezone** (manual; one-time browser guess prefills, no continuous auto until app), pause/resume | `subscriber` recurrence → §9.2 boundary recompute | daily, 08:00, manual tz | M5 (sched) |
| **Content & relevance** | muted entities/topics, **boosted** ("care more") list, keyword subscriptions/filters, per-source on/off, digest size ("how much": Stories ~3–5 / Notes ~15–25 caps) | feedback log + config caps table (design §8.4) | sensible caps | M4 |
| **Delivery** | email address (re-verify on change), channel (email now; push later), empty-digest behavior ("send 'all caught up'" vs stay silent), quiet hours (later) | `subscriber`; the empty-digest greeting already exists (`greeting.rs`) | email, send empty | M1→M5 |
| **Connections** | list + status (active/paused/revoked/errored), add/pause/revoke per connection | `connection` rows (M2) + connect flow (M5) — overlaps S4 | — | M5 |
| **Account** | name, timezone, **export my data**, **delete account** (GDPR cascade → `raw` + reasons, design §13), sign-out | `subscriber`; deletion is a real cascade, not a flag | — | M5 |

**Design rules that matter here:**
- **The timezone control is manual until the app (DECIDED).** Pre-app: a user-set tz, prefilled once from a
  confirmed browser guess — no background tracking. Continuous silent auto-update ("digest follows you as
  you travel") is **app-only (§5a, F4)**, because a flaky web heuristic that moves when your digest fires is
  worse than nothing. Whichever sets it, the value flows through the *one* §9.2 boundary function ("snap to
  next slot, lose nothing") — no setting touches scheduling math directly.
- **Every preference change is "effective next digest," never retroactive.** Say so in the UI; it mirrors
  the pipeline's tick model and sets honest expectations (same as feedback, §8).
- **The "boosted/muted" lists in Content are the *persistent view* of the feedback log** (§8) — settings and
  in-context feedback write the same append-only log; settings just renders + lets you undo it.
- **Delete & export are legal surface**, not nice-to-haves, the moment users beyond you exist (design §13).

---

## 8. Feedback, scoped

Feedback is the mechanism behind "earn trust through **control**." Design §10.3 gives two distinct loops;
the UI must keep them distinct because they touch different machinery:

| Gesture (in-context, on an item) | Means | Mechanism (design §10.3) | Effect timing |
|---|---|---|---|
| **Care more / less** (on an entity) | relevance affinity nudge | affinity weight update | next tick |
| **Wrong aggregation** (split / "not the same as") | this story shouldn't fuse these | per-subscriber **cannot-link** edge constraint | that subscriber's next recompute |
| **"These belong together"** (rarer) | force a fuse | per-subscriber **must-link** constraint | next recompute |
| **Mute** (entity/topic/source) | never show this | hard-zero gate | next tick |
| **Dismiss / "dealt with"** | hide this instance | engagement suppression (*deferred* — capture the gesture, act later) | — |

**The non-negotiable UX truths:**
- **Feedback is not instant and must not pretend to be.** It's an **append-only log** that tunes the *next*
  tick — nothing mutates the digest you're looking at, and crucially nothing shared is mutated (constraints
  are per-subscriber). The UI confirms "got it — we'll use this next time," never a fake live re-rank. This
  is honest *and* matches the architecture (CQRS read side is immutable; §3).
- **Feedback is the answer to a reason.** Each item shows "why you're seeing this" (the reason record); the
  feedback control is the *response* to that explanation. Pair them visually — the why and the "that's
  wrong" button live together. This why→correct loop *is* the trust mechanism (design §1/§10).
- **It needs a persistent home + undo.** In-context gestures are fire-and-forget; Settings → Content (§7) is
  where you *see* everything you've muted/boosted and reverse it. Same log, two entry points.
- **Write-side hygiene:** these are state-changing `POST`s → CSRF + IDOR re-check under RLS (§11). The
  feedback target is the **stable `story_id`/entity**, never a path-trusted id.

---

## 9. Signup & onboarding, scoped

Onboarding is the highest-stakes flow — it's where a stranger decides if this is worth it, and it has a
brutal **cold-start problem**: the first digest is empty until sources are connected *and* a tick has run.
Design the flow around that, not around a generic "create account" form.

**The flow (happy path):**
```
1. Enter email          → magic-link sent (email IS the identity; no password — §4)
2. Click link / verify  → session minted; double opt-in satisfied (deliverability + anti-abuse)
3. Name + timezone      → tz picked from a list, prefilled from a ONE-TIME browser guess the user
                          confirms (NOT a continuous auto feature — see below); feeds §9.2 scheduling
4. Schedule             → prefilled "daily, 8:00am your time" — one tap to accept
5. Connect first source → REQUIRED to finish setup (the cold-start fix — DECIDED). Two tiers:
      · Curated public  → pick from a vetted starter list (HN, advisories, a few good feeds) — the
                          zero-friction option that still seeds a non-empty first digest
      · Personal        → RSS paste-a-URL (no auth) / GitHub app-install OAuth (M5) — NUDGED as the
                          more useful choice ("this is where Bulletin earns its keep — your repos,
                          your sources"); curated is the floor, personal is the goal
6. Confirm              → "Your first digest arrives <local time>. Here's what we'll watch."
```

**The decisions/risks baked into this flow:**
- **Email-first, password-never.** Email is already the only identity we hold and the delivery channel, so
  magic-link/OTP is the whole auth story (§4) — no password store, no SSO dependency. Double opt-in is free
  and buys deliverability + abuse resistance.
- **Cold-start — DECIDED: require at least one source before setup completes.** You cannot finish onboarding
  with zero connections, so there is always *something* to digest — that removes the empty-first-digest
  failure mode at its root rather than papering over it. The curated public list is the no-friction floor
  (a single tap seeds a useful digest); the flow **nudges toward personal connections** because that's where
  the product's value actually is. A first digest may still be thin until a tick runs, so we still **set the
  expectation explicitly** ("first digest at 8am"); a `preview-now` off-schedule generation stays *optional*
  (the required-source rule already does the heavy lifting). The existing **"all caught up" greeting**
  (`greeting.rs`) handles the genuinely-quiet case gracefully.
- **Two source tiers by friction (and by value).** *Curated public* (vetted starter list) needs no auth and
  works today / for self-hosters; *personal* — RSS paste-a-URL, then GitHub/Slack via the M5 connect flow —
  is the nudged goal. The UI should let someone reach a working digest on curated/RSS alone before the OAuth
  surfaces exist, while always pointing at personal sources as the upgrade.
- **Timezone — DECIDED: no continuous auto-update until the app exists.** A *one-time* browser guess
  (`Intl.DateTimeFormat().resolvedOptions().timeZone`) prefilling a tz the user **explicitly confirms** at
  signup is fine — it's a sensible default, not a background feature. What we will **not** ship pre-app is
  *continuous, silent* tz tracking, because doing it well needs a real client (the app) that can detect
  travel reliably; a half-working web heuristic that silently changes when your digest fires is worse than
  nothing. So: manual tz with a one-time confirmed guess now; **continuous auto-tz is an app-only feature
  (F4)** — see §5a, §7.
- **SSRF gate on the RSS/personal-source URL field (§11).** Signup (and the connect flow) is the *first
  place a non-you user supplies a URL* — the M5 SSRF guard (resolve-then-pin, RFC1918 denylist) must gate it
  before public signup opens (roadmap M5). Until then, onboarding is invite/self-host only.

---

## 10. The "system working" dashboard (S6)

Your instinct here is good and it's **on-thesis, not a toy**: the product's whole pitch is "we read the
firehose so you don't have to, and we'll *show you* we're doing it." A live, animated glass-box that makes
the suppression *visible* is the most direct possible expression of "earn trust through transparency"
(design §1). It's the difference between "trust the black box" and "watch the box work."

**Crucial framing: this is NOT the operator dashboard.** A Prometheus/Grafana operator dashboard already
exists (`metric.rs`, the shipped Grafana board, commit `bbedca4`) — that's for *you*, in ops, with global
counters. S6 is **consumer-facing, per-subscriber, and emotional**: it answers "is this thing working *for
me*, and is it earning its keep?" Different audience, different data source, different tone.

**What it shows (grounded in data that exists / is coming):**

| Element | The number | Source | Why it lands |
|---|---|---|---|
| **Hero: the suppression ratio** | "We read **1,240** things this week and surfaced **6**." | events-in-scope count vs `digest_item` count | This *is* the product thesis as a single stat. The money shot. (Meaningful once M4 gating exists.) |
| **Pipeline stages, animated** | inbox → events → clusters → stories/notes → digest, with live counts flowing through | per-stage row counts (per-subscriber, RLS-scoped) | Makes the DAG legible — you *see* funnelling/fusing happen. |
| **Next digest** | live countdown to `next_run_at`, in local tz | scheduler (§9.2) | Anticipation + reassurance it's scheduled. |
| **Source freshness** | per-connection "last checked 4m ago", status dots | `connection.status` + cursor/poll time (M2) | Proves it's *live*, surfaces stale/errored connections. |
| **Recent activity ticker** | "fused a GitHub PR + an advisory into one story (same CVE)" | reason records (§8/design §10.2) | Shows the *headline feature* (linking) actually happening. |

**Data sourcing — the important constraint.** Per-subscriber numbers (your events, your clusters, your
suppression ratio) are **read under RLS in the subscriber context** (§2), exactly like the digest view —
this dashboard is just *another projection* over the same scoped rows, computed nowhere but the read path.
Any genuinely global/aggregate stat ("Bulletin watched 3.2M public events this month") is a separate,
non-scoped public aggregate and must be explicitly that — never per-subscriber data leaking into a global
view. **It does not read Prometheus** (that's operator-global and not tenant-isolated).

**Live + animated — how, and when.** The "live" part is the **SSE/WebSocket channel from §5a (F4)** — the
dashboard is in fact the *first natural consumer* of that channel, and a fine reason to build a minimal
version of it. The animation is client-side (counts ticking up, items flowing stage-to-stage); with Option
A this is htmx + a little CSS/JS or a small island — it does **not** justify a SPA framework on its own.

**Phasing — DECIDED: defer the whole thing until live (no static stopgap).** A static snapshot was on the
table, but the call is to **ship S6 only as the live, animated version, riding the F4 SSE channel** the
native app needs anyway. Rationale: the dashboard's entire reason to exist is the *liveness* — a static
counts page is a weaker, separate build that we'd then throw away; better to wait and do it once, properly,
on shared infrastructure. So **S6 lands with/after F4**, not alongside M4. (The underlying counts exist far
earlier, so nothing blocks pulling it in the moment F4's channel is up — it's a scheduling choice, not a
data dependency.)

**Risks to name out loud:** (1) **scope creep** — a "delightful animated dashboard" is a bottomless time
sink; cap it at the table above and resist turning it into analytics. (2) **honesty** — the suppression
ratio must be *real* (in-scope events vs surfaced), never an inflated vanity number, or it corrodes the very
trust it's meant to build. (3) **perf/privacy** — it's per-subscriber data; same RLS + no-client-private-
data rules as every other read (§11). (4) **it's delight, not core** — sequence it behind the digest view,
provenance, and feedback; it amplifies trust but doesn't *deliver* the digest.

---

## 11. Security checklist (the deltas a web surface introduces)

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

## 12. Open decisions (for you)

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
6. **The "system working" dashboard (§10) — ✅ DECIDED: defer until live.** No static stopgap; S6 ships
   *only* as the live, animated version on the F4 SSE channel. It's a scheduling choice, not a data
   dependency (the counts exist earlier), so it can be pulled in the moment F4's channel is up.
7. **Cold-start (§9) — ✅ DECIDED: require ≥1 source to finish setup.** Onboarding can't complete with zero
   connections; a curated public starter list is the no-friction floor, but the flow nudges toward personal
   connections (where the value is). `preview-now` stays optional — the required source already prevents the
   empty first digest.
8. **Timezone — ✅ DECIDED: manual until the app.** No continuous auto-tz on web (a flaky heuristic that
   moves your digest time is worse than nothing); a one-time browser guess prefills a tz the user confirms.
   **Continuous auto-tz is an app-only feature (F4, §5a).** *Still open:* confirm the five-tab Settings IA
   (Schedule · Content · Delivery · Connections · Account).
