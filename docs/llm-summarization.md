# Digest System — LLM Summarization

**Status:** Design doc (2026-06-15). Not yet implemented. Promotes the roadmap-deferred *LLM
summarization* item (design §9.5, roadmap M6 backlog) into a concrete build plan, now that the email
template has summary slots to fill (`digest/render.rs` — the three lorem-ipsum placeholders).
**Companion to:** `local-ml-options.md` (the serving stack + model picks — this doc is the *how it
plugs into Bulletin* to that doc's *what to run*), `thread-layer.md` (§5.1 `thread_maintenance`, where
the per-subscriber half lives), `system-design.md` (§8.4 richness/format, §9.5 rendering, §10.2 reason
records, §12 trust).
**What it owns:** the four summary surfaces the digest wants, the **durability-tiered, content-hashed,
hierarchical pre-summarization** that produces them off the punctual path, the **fields** that make
that cheap and faithful, and the **constrained invocation** that keeps a 3–4B local model honest.

> **The governing constraint, restated.** Per `local-ml-options.md` §0 (ground-truth-first) and
> `thread-layer.md` §3.1 (fall behind, never wrong): **every model call is write-side, best-effort,
> off the punctual path, behind a flag, and degrades to a deterministic baseline.** Nothing here ever
> runs on a digest fire. A missing or rejected summary costs a slightly plainer email — never a late
> digest, never a wrong one, never egress.

---

## 1. The surfaces — what actually needs summarizing

The email template (`digest/render.rs`, `DigestContent`) has **three lorem-ipsum stand-ins** plus one
implicit one the thread layer wants. Each maps to a different unit of the content graph, and — this is
the whole design — **each unit has a different durability and sharing profile**, which dictates where
its summary is computed and how often.

| Email slot (`render.rs`) | Content-graph unit | Durability / sharing | Where it's produced |
|---|---|---|---|
| per-item `item_summary` (the TL;DR under a Story headline) | **Cluster** (representative) → **Story** (cross-source) | cluster: durable, *public ones shared across all subscribers*; story: per-fire recompute, id-forwarded, per-subscriber | `public_build` / `private_build` (cluster); `thread_maintenance` (story synthesis) |
| per-item `item_category` → the **context eyebrow** | **Thread** label + delta (not a topic category — §1.1/§6.1) | durable, per-subscriber | `thread_maintenance` |
| digest-level `summary` (the "big picture" lead) | **Digest** (this fire's selected set) | per-fire, per-subscriber, *cannot* exist before selection | fire-time **deterministic compose**; optional async editor's note (Phase D) |
| in-summary **entity badges** (person/repo/CVE) | **Entity** refs in the tldr (§6.2) | resolved per-subscriber via the identity layer | model emits refs; render resolves |

The key consequence: **the only unit that is both stable and shared is the cluster.** The story is
recomputed every fire; the digest can't be summarized until its items are picked; the thread is
per-subscriber. So the cluster summary is the *foundation* — it is computed once per content change,
public ones once for everybody — and every higher surface is **composed from summaries below it**,
never re-derived from raw events. That is the incremental pre-summarization answer (§4), and it is what
makes the whole thing affordable on a bandwidth-bound M2 (`local-ml-options.md` §2).

### 1.1 The per-item kicker is the Thread, not a topic category

The reference template's per-item slot is a **kicker** (a small label above the summary, e.g.
"Geopolitics/Diplomacy"). A *generic topic category* ("Incident", "Release") is weakly meaningful and
needs a whole closed-taxonomy apparatus to stay stable. The far stronger fill is the **label of the
Thread the story belongs to** — "Acme migration", "On-call rotation" — the persistent thread of the
user's life this happening advances (`thread-layer.md` §1). It is more specific, already per-subscriber
and personal, and it is *already produced* by the thread layer (`thread.label`), so it costs no new
model call beyond the thread label we were summarizing anyway (§2.3). The redesigned row (§6.1) renders
it as the **context eyebrow** — thread + a terse delta — and **drops the topic category entirely**.

**Consolidate with the existing chip.** The renderer *already* names the thread once, as a chip above
the headline (`render.rs::render_thread_chip` → "▸ possibly Acme migration"). So this is a
**consolidation**, not an addition: the Thread label becomes the context eyebrow (carrying the
confidence qualifier and the §5.2 delta with it), the standalone chip folds into it, and an un-threaded
story simply renders **no eyebrow** — graceful omit, exactly as a story with no thread chip does today.

---

## 2. Q1 — Fields to add (cluster / story / thread / digest)

All additive, `jsonb`-now-normalize-on-trigger (design §6 convention), defaulting to "no summary" so
the layer is **inert until a pass has run** — same shadow-safety as the thread layer's
`subscriber.affinity '{}'` default.

The shape of every tier is the same triplet: **a `jsonb` payload + a content signature + provenance**
(model/prompt version + timestamp). The signature is the efficiency lever: it lets a pass *skip* a unit
whose inputs are unchanged, so a unit is summarized **once per content change, not once per fire or per
subscriber**.

### 2.1 Cluster — the foundation (durable, public ones shared)

```sql
ALTER TABLE cluster ADD COLUMN summary        jsonb NOT NULL DEFAULT '{}';
ALTER TABLE cluster ADD COLUMN summary_hash   bytea;        -- signature of the events that fed the summary
ALTER TABLE cluster ADD COLUMN summary_model  text;         -- "<model>@<prompt-version>" → invalidate on upgrade
ALTER TABLE cluster ADD COLUMN summarized_at  timestamptz;  -- staleness + the "due" sweep

-- The work queue for the summarizer: clusters whose content changed since (or never) summarized.
CREATE INDEX cluster_needs_summary ON cluster (last_event_time)
  WHERE summarized_at IS NULL;
```

`cluster.summary` (jsonb), the **extract-then-summarize product** for one cluster:

```jsonc
{
  "headline":  "Auth service returns 500s after the token-rotation deploy",  // abstractive, ≤ ~90 chars
  "tldr":      [ /* structured run-list with grounded entity refs — see §6.2 */ ], // 1–2 sentences
  "tldr_text": "A bad config in the 14:02 rollout broke token validation; ~12% of logins failed for 40m until rollback.", // flat concat, for plaintext + preview
  "facts": {                          // the "extract" half — Phase-2 comprehension output, reused as grounding
    "entities":   ["repo:acme/auth", "cve:CVE-2026-1234", "user:dlewis"],
    "event_type": "incident",
    "state":      "resolved",         // detected → resolved (local-ml-options §6 comprehension)
    "numbers":    ["12%", "40m"],
    "dates":      ["2026-06-14T14:02Z"]
  },
  "band":      "confirmed"            // faithfulness verdict (§3.4): confirmed | probable | uncertain
}
```

- `summary_hash` = hash of the cluster's contributing event content (`title‖body‖links‖entities`, in
  `event_time` order). **Recompute only when it changes** — the cheap staleness gate. (A cluster is a
  durable rollup over its events; its summary is a cache over that same content, recomputable like the
  cluster itself.)
- `summary_model` carries the model + prompt version so a model/prompt upgrade invalidates the whole
  corpus by a `WHERE summary_model <> $current` sweep, without a data migration.
- `facts` is **not summarization output** — it is the comprehension/extraction pass (GLiNER + tiny LLM,
  `local-ml-options.md` Phase 2) that *grounds* the summarization pass (§3.2/§4). Storing it on the
  cluster means the extract step runs once and feeds every higher tier.

### 2.2 Story — the cross-source synthesis (per-subscriber, cached by member-signature)

The story is a per-fire recompute (migration 018), so it cannot host an *authored-at-fire-time*
summary without an LLM call on the hot path (forbidden). Instead it hosts a **cache** keyed by the
signature of its members, written by a best-effort pass and *read* at fire time:

```sql
ALTER TABLE story ADD COLUMN summary       jsonb NOT NULL DEFAULT '{}';  -- the fused, cross-source item summary (headline + tldr)
ALTER TABLE story ADD COLUMN summary_sig   bytea;        -- hash of (sorted member cluster.summary_hash[] ‖ thread_id)
ALTER TABLE story ADD COLUMN summary_model text;
ALTER TABLE story ADD COLUMN summarized_at timestamptz;
```

- `summary_sig` is the **member signature**: the sorted set of member-cluster `summary_hash`es plus the
  assigned `thread_id`. Because stories are id-forwarded and stable across fires (§8.2), a story's sig
  is stable until its membership actually changes — so the synthesis is **reused across fires for free**
  and regenerated only when a source is added/dropped or a member's content moves.
- The synthesis answers *"what is this happening, across the sources that lit up"* ("A CVE advisory, an
  incident PR, and a Slack flurry are the same outage"), which a single cluster summary can't — but it
  is built from the **member cluster summaries**, never by re-reading their raw events (§4).
- **Graceful cold-start (the important bit):** a brand-new story has no cached synthesis. Fire-time does
  **not** wait — it falls back to the representative cluster's `summary.tldr`/`headline` (always
  precomputed, build-side). The fallback is itself a grounded, good one-liner, so the email is never
  empty; the cross-source rewrite is a *quality upgrade* that lands on the next fire after a best-effort
  pass has synthesized it. This is why Phase A (cluster summaries) already fills the email slot.

### 2.3 Thread — label + the "what changed" delta (per-subscriber)

`thread.label` already exists (auto-filled from top entities in `thread_maintenance`). Add the authored
label source + the delta:

```sql
ALTER TABLE thread ADD COLUMN summary       jsonb NOT NULL DEFAULT '{}';  -- "state of this thread" + the readable auto-label
ALTER TABLE thread ADD COLUMN delta         text;         -- the §5.2 delta line ("staging cutover landed; 2 follow-ups…")
ALTER TABLE thread ADD COLUMN delta_through timestamptz;  -- watermark the delta covers: the thread's last *delivered* appearance
ALTER TABLE thread ADD COLUMN summary_model text;
ALTER TABLE thread ADD COLUMN summarized_at timestamptz;
```

- The LLM **upgrades** the auto-label "repo:acme/auth +3" to a readable "Acme auth migration"; the
  resolver's identity `confidence` band (already on `thread`) carries straight to the chip ("possibly
  …"), unchanged.
- `delta` is computed exactly as `thread-layer.md` §5.2 / system-design §9.4 prescribe: the stories on
  this thread with **new events since `delta_through`** (the thread's last delivered `digest_item`). The
  delta's *inputs* are those new stories' summaries; the LLM compresses them into one line. `delta_through`
  is the memoization watermark — no new events since it ⇒ delta is current, skip.

### 2.4 Digest — the big-picture lead

```sql
ALTER TABLE digest ADD COLUMN lead text;   -- the rendered "big picture"; null ⇒ greeting-only (today's behavior)
```

Persisted (nullable) for explainability parity with `digest.decisions` (migration 021) and so the lead
is reproducible in the debug trace. **Composed at fire time, deterministically, from the selected
items' precomputed summaries/deltas (§3.1) — no model call on the punctual path.** Null ⇒ the lead
falls back to the greeting alone (exactly today's lead).

### 2.5 Config + kill switch (mirror `MaintenanceConfig` / `digest_config`)

A `SummarizationConfig` (model name, sidecar `base_url`, per-task `max_tokens`/temperature, the
entity-badge budget, the faithfulness-gate toggle) held like `thread::maintain::MaintenanceConfig` — a struct
now, a `summarization_config` row when per-deployment tuning bites. Guard the whole feature behind a
**`llm-summarization` cargo feature** (compile-time kill switch, mirroring `thread-weighting`) **and** a
runtime flag, so it compiles out entirely and the deterministic baseline ships by default.

---

## 3. Q2 — Constrained invocation (how to keep a 3–4B model honest)

Per `local-ml-options.md` §4–§7: a **`llama-server` (llama.cpp) sidecar** over its OpenAI-compatible
API, driven from Rust via **`async-openai`** with `base_url` pointed at the local box. A small
**Apache** instruct model — **Qwen3.5-4B** (default) or **Granite-4.0-H-Micro 3B** on a RAM-bound box
(§7). Five constraints stack to make a sub-4B model "good enough" for short, grounded text:

### 3.1 Nothing on the punctual path

The hard rule. Cluster summaries run in `public_build`/`private_build`; story synthesis and thread
label/delta run in `thread_maintenance`; the digest lead is **deterministic string assembly** over
precomputed per-item `headline`/`tldr` + thread `delta` (no LLM). The optional *authored* big-picture
(Phase D) is a deadline-bounded best-effort call with the deterministic lead as its fallback — it can
miss its deadline and the digest still sends on time. "Fall behind, never wrong" (thread-layer §3.1).

### 3.2 Extract-then-summarize (grounding, not free generation)

The non-negotiable faithfulness guardrail (`local-ml-options.md` §7). We never hand the model raw text
and ask it to "summarize." We hand it the **pre-extracted facts** (`cluster.summary.facts` — entities,
event-type, state, numbers, dates from the Phase-2 comprehension pass) **plus** the source snippets, and
ask it to *rewrite the given facts* into a headline/tldr. The model's job is compression and phrasing,
not recall — which is exactly where a 3–4B model is reliable and where it hallucinates entities/numbers
if left to free-associate (the Phi-4-mini 23.5% trap, §7).

For the **comprehension/extraction** step itself (producing `facts`), follow CRANE/scratchpad
(`local-ml-options.md` §6): **don't** hard-constrain the reasoning (10–30% "grammar tax") — let it reason
free, then constrain only the final JSON. For the **summarization** step the output is already short
JSON, so direct grammar-constrained decoding is fine.

### 3.3 Grammar-constrained, schema-typed output

`response_format: json_schema` → llama.cpp's GBNF token-masking **guarantees structurally valid JSON**
(`local-ml-options.md` §4), so the Rust side never parses-and-prays. The schema does real work beyond
validity:

- `headline`/`tldr`/`delta` carry **`maxLength`** → length control (and the single-line eyebrow, §6.1)
  is enforced by the grammar, not hoped for.
- the `tldr` is a **run-list** (§6.2) whose entity `ref`s are a **closed `enum` of the input
  `facts.entities`** — so the model can *reference* a grounded entity for an inline badge but can never
  name one that wasn't extracted from ground truth. Structural entity-faithfulness, not a post-check.
- Low **temperature (≤ 0.2)** + a **fixed seed** → reproducible output, so a content-unchanged cluster
  re-summarizes identically (idempotency; the cache is meaningful).
- Short **`max_tokens`** per task (headline ~24, tldr ~80, delta ~60) — short outputs both cut latency
  (§7: headline ~1 s, a 150-tok delta ~7 s if Vulkan engages) and measurably reduce hallucination.

### 3.4 The faithfulness gate — ML never grounds alone

The doctrine (`local-ml-options.md` §0): a model output is *one signal*, verified against ground truth,
degraded to deterministic on failure. After generation, a **cheap deterministic check**: every entity,
number, and date in the output must appear in the input `facts` (the model may *drop* facts, never
*add* one). On a violation:

- **reject** the candidate and **fall back to the deterministic baseline** — the extractive cluster
  `title` for the headline, a templated one-liner for the tldr (e.g. "{n} updates across {sources}") —
  and band it `uncertain`. The digest **never ships an unverified hallucination**; the worst case is a
  plainer, true line.
- the `band` (confirmed/probable/uncertain) rides to render as the §10.4 confidence surface — a thread
  whose *label* failed the gate renders "possibly …", same vocabulary as identity doubt. Rendered
  uncertainty, not hidden.

This is also the **`digest-explain` eval hook**: run the gate read-only over historical clusters,
measure the Vectara-style entity/number-accuracy rate (`local-ml-options.md` §7) before any summary
touches a delivered digest.

### 3.5 No egress, scope-clean (the §12 property the deferral was waiting on)

The roadmap deferred summarization because cloud summarization is *data egress to a model provider →
per-source consent* (system-design §12 #5). **A 100%-local sidecar removes that requirement entirely**
(`local-ml-options.md` §0): no private content leaves the box, so the consent gate is replaced by the
no-egress invariant. Scope discipline is preserved end-to-end:

- **Public** cluster summaries are generated in `public_build`'s **no-subscriber** context and shared
  (the big multiplier saving, §5).
- **Private** cluster/story/thread summaries are generated in the **subscriber's RLS context**, on rows
  that are already RLS-forced. Calls are **per-unit and stateless** — one cluster/story per request — so
  the model never sees two tenants' content in one context (system-design §12 #1/#6: stateless workers,
  no shared buffers).

---

## 4. Q3 — What we feed, and yes: incremental pre-summarization

**Yes — hierarchical, memoized pre-summarization, aligned to the durability tiers.** Each tier
summarizes the **summaries of the tier below**, never the raw firehose, and each unit is summarized
**once per content change** (gated by its `*_hash`/`*_sig`), not once per fire or per subscriber.

```
RAW EVENTS  ──extract+summarize──►  CLUSTER.summary   (build-side, content-hashed, public ones shared)
                                         │  facts + tldr (entity refs) + headline
CLUSTER summaries  ──synthesize──►  STORY.summary     (thread_maintenance, cached by member-sig)
                                         │  the cross-source "what's happening"
STORY summaries    ──delta──────►   THREAD.delta      (thread_maintenance, watermark = last delivered)
                                         │  "what changed since you last looked"
selected items'    ──compose────►   DIGEST.lead       (FIRE-TIME, DETERMINISTIC — no model)
summaries/deltas
```

**What each call actually receives (kept short on purpose):**

- **Cluster:** the member events' `title` + `body` (budgeted/truncated — a small model degrades on long
  inputs, the §7 cliff), **plus** the extracted `facts`. The only tier that touches raw text.
- **Story:** the member clusters' `headline`/`tldr`/`facts` (a handful of short summaries) + the thread
  label — **not** their raw events again. A story over 4 sources is ~4 short strings in, not four walls
  of text.
- **Thread:** the **new** stories' summaries since `delta_through` — bounded by what changed, not the
  thread's whole history.
- **Digest lead:** the selected items' precomputed `headline`/`tldr` + thread `delta`s, assembled by a
  template. Inherently per-fire, so it must read-only and stay deterministic.

**Why incremental, not one-shot-per-digest — the two reasons it's mandatory here:**

1. **Cost (the multiplier).** A public cluster (a CVE advisory, an HN thread) appears in *many*
   subscribers' digests. Summarizing per fire/per subscriber multiplies one model call by
   subscribers × fires. Content-hashing + build-side public generation collapses that to **one call per
   content change, shared by everyone** — the same amortization the shared public-build already gives
   grouping/rollups (system-design §11). On a ~13 tok/s box (`local-ml-options.md` §1), this is the
   difference between viable and not.
2. **Quality (short inputs win).** Sub-4B faithfulness collapses on long context ("lost in the middle",
   the §7 cliff). The hierarchy *is the mechanism* that keeps every individual call short and grounded:
   the model never sees more than a few pre-distilled, fact-tagged strings, so each call sits in the
   regime where a 3–4B model is faithful. Extract-then-summarize (§3.2) is the same idea one level down.

---

## 5. Runtime placement — no new job, two existing ones do the work

```
MATERIALIZATION (write side · best-effort · off the punctual path)
  public_build        + cluster.summary for PUBLIC clusters   (no-subscriber ctx, shared, content-hashed)
  private_build       + cluster.summary for PRIVATE clusters  (subscriber ctx)
  thread_maintenance  + thread.label/summary/delta            (subscriber ctx)
                      + story.summary synthesis (best-effort; it already walks the subscriber's stories)

PROJECTION (read side · fire-time · pure over the snapshot · NO model call)
  generate:
    … select stories (unchanged) …
    + read story.summary (or fall back to representative cluster.summary)  → headline + summary (zones 2–3)
    + read thread.label + thread.delta                                     → context eyebrow (zone 1)
    + compose provenance from member sources, digest.lead from the above   → provenance (zone 4) + lead
```

No new apalis job kind is required — the cluster work hangs off the builds (which already recompute a
cluster when its events change, the natural `summary_hash`-invalidation point), and the per-subscriber
work hangs off `thread_maintenance` (already due-gated, best-effort, subscriber-scoped — the exact
contract summarization needs). A summarizer that falls behind just means a slightly staler `tldr`, never
a late or wrong digest.

---

## 6. Render — the item row, redesigned around the summary

The current `render_story_row` (`digest/render.rs`) stacks seven labeled zones — a loud `▸ possibly
…` chip, headline, a category kicker, a summary, a full "Related" list, a `Why · … · relevance 0.94`
caption, and an amber debug block. It names the thread twice and leans on *machine* signals (a relevance
float, format tags) to assert trust. The LLM summary lets us invert that: **trust is carried by grounded
specifics + provenance, and the scaffolding comes out.** Mockup: `docs/mockups/item-redesign.html`.

### 6.1 Four quiet zones

```
Acme auth migration · staging cutover landed         1 · CONTEXT eyebrow  (one line, a few words)
Auth outage traced to the token-rotation deploy      2 · HEADLINE         (editor-grade, abstractive)
A bad config in the 14:02 rollout broke validation   3 · SUMMARY          (one grounded sentence)
in acme/auth; ~12% of logins failed for 40m until
Dana rolled it back.
Across GitHub · a CVE advisory · Slack — 2h ago      4 · PROVENANCE       (corroboration, made calm)
```

1. **Context eyebrow** — the assigned **thread label** (§1.1) + a **terse delta flag**, on **one line**.
   The delta is a few words ("staging cutover landed", "reactivated", "3 follow-ups") — *a flag, not a
   clause*; the detail lives in the summary below. Enforced two ways: a hard `maxLength` on `thread.delta`
   in the schema (§3.3) **and** `white-space:nowrap; overflow:hidden; text-overflow:ellipsis` at render,
   so it can never wrap. Identity doubt shows as a quiet italic "possibly" before the label (Probable;
   omit the eyebrow for Uncertain — *budget the doubt*, §10.4). **Un-threaded ⇒ no eyebrow at all** (the
   standalone `render_thread_chip` is folded into this zone — one thread reference per item, not two).
2. **Headline** ← `cluster.summary.headline` (the representative). Editor-grade and normalized across
   sources; falls back to the raw cluster `title` if the faithfulness gate (§3.4) rejects it.
3. **Summary** ← `story.summary.tldr` ‖ representative `cluster.summary.tldr` — **one** grounded
   sentence, set in upright body serif (a confident statement, not a muted italic aside). Its specifics
   (`14:02`, `12%`, `40m`) are the trust workhorse. Entity mentions render as inline badges (§6.2).
4. **Provenance** ← composed deterministically from the story's member sources: **`Across <source> ·
   <source> · <source> — <when>`** for a multi-source story (surfacing the M3 cross-source value as the
   confidence line), or just **`<source> — <when>`** for a single source. This replaces *both* the
   verbose "Related" list and the `Why · relevance` caption.

The digest-level `DigestContent.summary` → `digest.lead` (§2.4); null ⇒ greeting alone.

**What moves off the email:** the full per-source breakdown (each fused cluster + its `link_reason`) and
the machine "why" (relevance/priority/`digest.decisions`) move to the **authenticated deep-link / explain
view** — email stays editorial, the audit trail stays inspectable (system-design §9.5 "notification +
deep-link, not content-dumping"; §10.2 reason records). The amber **debug block is deleted**.

Every zone has a precomputed-or-omit fallback, so **the renderer is correct at every phase** — ship
Phase A and the summary fills from cluster summaries with no eyebrow; later phases light up the eyebrow,
delta, and cross-source synthesis with no render rework.

### 6.2 Structured entity references → inline badges

The model may **refer to entities inside the summary** — but in a structured, *grounded* way that lets
rendering emit an inline badge (a person + avatar, a repo tag, a CVE pill) instead of plain text. The
correctness comes from constraining the model to a **closed set**, never free-naming.

- **The model references, it does not name.** The summarization schema (§3.3) emits the tldr as an
  ordered run-list, each run either plain `text` or an **entity `ref`** whose token is grammar-constrained
  to an `enum` of the input `facts.entities` (the deterministically-extracted set, §2.1/§3.2). So the
  model can only point at an entity that was extracted from ground truth — a hallucinated mention is
  *structurally impossible*, not merely caught after the fact. It picks *which* token to reference and
  the visible surface text; it asserts no identity.

  ```jsonc
  "tldr": [
    { "text": "A bad config broke token validation in " },
    { "ref": "repo:acme/auth", "surface": "acme/auth" },     // ref ∈ enum(facts.entities)
    { "text": "; ~12% of logins failed until " },
    { "ref": "user:U0123", "surface": "Dana" },
    { "text": " rolled it back." }
  ]
  // a flat "tldr_text" is stored alongside (concat of text+surface) for the plaintext email + inbox preview.
  ```

- **Rendering owns identity, avatar, and confidence — not the model.** Each `ref` resolves through the
  identity layer to the existing render contract `{ display_name, canonical_id?, confidence_band,
  avatar_ref? }` (thread-layer §4 / §10.4). Treatment is by entity *type* × *confidence*:
  - `user:` → a person chip; the **authoritative avatar only** (the §4 footgun: avatar provenance must
    equal identity provenance — a Slack `U0123` *carries* its avatar; otherwise initials/placeholder).
    Uncertain → name + a quiet "?", which **is the §10.4 correction affordance** (tap → must-link /
    cannot-link feedback → the edge hardens for next recompute).
  - `repo:` → a faint dotted-underline tag linking to the repo; `cve:` → a severity-tinted pill linking
    to the advisory; `url:`/`domain:` → a domain chip.
- **Safe degeneration.** An unresolved or dropped `ref` renders as **plain `surface` text** — never a
  broken badge; clients that strip the badge styling still get the inline display text (the badge is
  progressive enhancement, the plaintext part already uses `tldr_text`). **Budget the badges** (§10.4):
  cap the badged mentions per summary to the salient few, the rest stay plain, so the sentence reads as
  prose, not a wall of chips. Private-scope person badges resolve only in the subscriber's RLS context
  (§12 — no cross-tenant avatar).

This is the §3.4 faithfulness gate, extended one level: it already forbids unsupported numbers/dates;
the closed-`enum` ref makes the same guarantee for *entity references*, structurally.

---

## 7. Phased plan — additive, independently shippable, eval-gated

Each phase is behind the `llm-summarization` flag and evaluated **read-only via `digest-explain`**
(faithfulness rate over historical units) before it touches a delivered digest — same gate the thread
layer uses.

- **Phase A — Cluster summaries (the foundation).** `cluster.summary` (headline/tldr/facts) in the
  builds, content-hashed; the extract-then-summarize pass + the faithfulness gate. Fills `item_summary`
  from the representative cluster; the lead becomes a **deterministic** compose. *Biggest single win —
  retires the per-item summary and the big-picture lorem.*
- **Phase B — Thread label + delta (the context eyebrow).** Readable `thread.label`, which becomes the
  per-item **context eyebrow** (§1.1/§6.1, retiring the `item_category` lorem and folding in the existing
  chip); the §5.2 `delta` flag in `thread_maintenance`, watermarked by last-delivered, on the same line.
- **Phase C — Story cross-source synthesis.** `story.summary` cached by member-sig in
  `thread_maintenance`; upgrades `item_summary` from the representative-cluster fallback to a fused
  multi-source rewrite for recurring stories.
- **Phase D — Authored big-picture lead (optional).** Replace the deterministic lead with a
  deadline-bounded best-effort "editor's note" over the selected items' summaries, deterministic lead as
  the fallback. Only worth it if the templated lead proves too flat.

---

## 8. Invariants preserved

- **Punctuality (§3.1).** No model call on the fire path; the lead is deterministic; every summary is a
  precomputed read or a graceful omit. A slow box ⇒ staler summaries, never a late digest.
- **No-egress / scope (§12 #1/#5/#6).** 100% local; public summaries shared from the no-subscriber
  context, private ones in the subscriber's RLS context; per-unit stateless calls — no cross-tenant
  content in one context. The consent requirement that deferred this is dissolved by no-egress.
- **Determinism / recomputability.** Temperature-0 + seed + content-hash gating ⇒ idempotent;
  `summary`/`facts`/`delta` are recomputable caches over the durable event/feedback logs (status of
  `cluster`/`story`/`thread`), reconstructible from truth — lose them, rebuild them.
- **Trust / explainability (§10.2/§10.4).** The faithfulness gate degrades to a true deterministic line
  on any unsupported entity/number; uncertainty renders as a band, not hidden; `digest.lead` joins
  `digest.decisions` in the debug trace.
- **Graceful degradation.** Disable the cargo feature and the digest is exactly today's: greeting lead,
  no eyebrow/summary, no delta, raw titles — the deterministic baseline, intact.

---

## 9. Open questions (tuning surface)

- **Entity-badge budget** (§6.2) — how many mentions to badge per summary before it reads as chips not
  prose; whether `repo:`/`cve:` tags count against the same budget as person chips; the exact treatment
  of a Probable (vs Confirmed/Uncertain) identity inline.
- **Un-threaded items** — confirmed: no eyebrow (the topic category is dropped, §1.1). Revisit only if
  un-threaded items feel context-starved in practice.
- **Faithfulness gate strictness** — exact-token vs normalized entity/number matching; the
  reject-rate vs coverage trade-off; whether to band-and-ship `probable` rather than reject outright.
- **Input budgets** — token caps for cluster `body` truncation and the story member-summary fan-in,
  against the §7 long-context faithfulness cliff.
- **Story synthesis placement** — fold into `thread_maintenance` (reuses its story walk) vs a dedicated
  best-effort `summarize` step enqueued post-generate; cold-start reliance on the cluster fallback.
- **Big-picture lead** — is the deterministic template good enough (Phase D never needed), and if not,
  the deadline budget for the authored version.
- **Model choice on the real box** — Qwen3.5-4B vs Granite-4.0-H-Micro 3B under the actual RAM/Vulkan
  profile (`local-ml-options.md` §7, §9 — re-verify before committing).
