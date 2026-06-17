# LLM Summarization — Phase A Foundation: Implementation Handoff

**Status:** Implemented 2026-06-15. The **foundation** of `llm-summarization.md` Phase A — the schema,
the write-side summarization pipeline, the local-sidecar client, and the llama.cpp deployment — is
built, behind the `llm-summarization` cargo feature as the **sole, compile-time** kill switch (no
runtime flag), **off by default**. The render consumption (filling the email's summary slots) and the
per-subscriber/private wiring are the *next phase*; this doc hands them off.
**Reads against:** `llm-summarization.md` (the design), `local-ml-options.md` (the serving stack),
`thread-layer.md` §3.1 (the "fall behind, never wrong" contract this inherits).

> **The one-line summary of what shipped:** a cluster can now be summarized into a content-hashed,
> grammar-constrained, faithfulness-gated `cluster.summary`, generated off the punctual path by a
> best-effort sweep that calls a 100%-local llama.cpp sidecar and **degrades to a deterministic
> baseline** on any miss. Nothing reads it into a digest *yet* — that is the next step, and it is small.

---

## 1. What was built (and where)

### 1.1 Schema — `crates/core/migrations/20200101000027_cluster_summary.sql`

The cluster tier of `llm-summarization.md` §2.1, exactly as designed:

- `cluster.summary jsonb NOT NULL DEFAULT '{}'` — the extract-then-summarize product.
- `cluster.summary_hash bytea` — the content signature (the staleness gate).
- `cluster.summary_model text` — `<model>@<prompt-version>` provenance (upgrade ⇒ `WHERE`-sweep
  invalidation, no data migration).
- `cluster.summarized_at timestamptz` — staleness + the due sweep.
- `CREATE INDEX cluster_needs_summary … WHERE summarized_at IS NULL` — the work queue's cheap "never
  summarized" scan.

**Additive and inert:** `'{}'` deserializes to the empty [`ClusterSummary`] (`is_empty()` true), so a
deployment that never runs a pass is byte-for-byte the deterministic digest. *Only the cluster tier was
migrated* — the story/thread/digest columns (§2.2–2.4) are deliberately deferred to their phases (B/C/D)
to avoid dead schema. See §4.

### 1.2 The `summarize` module — `crates/core/src/summarize/`

Split into a **pure core (always compiled, unit-tested)** and a **gated edge (feature
`llm-summarization`)**:

| File | Gated? | Contents |
|---|---|---|
| `mod.rs` | core always; sweep gated | `SummarizationConfig` (+ `from_env`), the data model (`ClusterSummary` / `TldrRun` / `Facts` / `Certainty` / `Band`), `summary_hash`, `extract_facts`, `faithful` (the gate) + `GateViolation`, `baseline`, `response_schema`, `SYSTEM_PROMPT` / `user_prompt`, `source_corpus`, and the `sweep_public` / `sweep_private` entries. |
| `client.rs` | gated | `summarize_cluster` — the reqwest POST to the sidecar's `/chat/completions`, parse → gate → fallback. |
| `store.rs` | gated | `clusters_needing_summary` (the work-queue read), `store_summary`, `touch_summarized`. |

Registered in `lib.rs` as `pub mod summarize;`.

**Faithfulness gate (§3.4) — the real backstop, fully deterministic and tested:**
- entity `ref`s must be in the closed `facts.entities` set (also grammar-enforced — re-checked here);
- every numeric/date token in the output must appear in `facts` *or* verbatim in the source text
  (substring match — see the caveat in §3);
- a house-voice lint rejects the §3.6 banned hype/second-person words and `!`;
- length budgets on headline/tldr.
On any violation → reject → [`baseline`] banded `Uncertain`. **The digest never ships an unverified
hallucination.**

### 1.3 The sidecar client — `client.rs`

OpenAI-compatible `POST {base_url}/chat/completions` with `response_format: { type: json_schema, … }`
so llama.cpp's GBNF token-masking guarantees valid JSON. Low temperature + fixed seed (idempotency).
`summarize_cluster` **never returns an error** — transport failure, non-2xx, malformed JSON, or a gate
rejection all degrade to the deterministic baseline.

### 1.4 Worker integration — `crates/bulletin/src/worker.rs`

`summarize_public(&pool)` is called **after a successful `public_build`** (the natural
`summary_hash`-invalidation point, §5), best-effort: it reads `SummarizationConfig::from_env()` for the
sidecar address/model and never propagates an error. The binary's forwarded `llm-summarization` feature
is the only switch — without it `summarize_public` is an empty no-op and no summarization code exists.

### 1.5 Deployment — `flake.nix` + `nix/module.nix`

- The flake exposes two builds: `bulletin` (plain, no summarization) and `bulletin-llm` (the
  `--features llm-summarization` build; no new deps — rides the existing `reqwest`). The kill switch is
  the **build choice**, not a runtime flag: the off build has no summarization code.
- `services.bulletin.llm`: `enable` (selects the `bulletin-llm` package + sets the `BASE_URL`/`MODEL`/
  `PROMPT_VERSION` *config* env — not an enable flag), `baseUrl`, `model`, `promptVersion`,
  `serveLocally` (provisions `services.llama-cpp` on the port parsed from `baseUrl`), `package`,
  `modelPath`.
- The worker is `wants`/`after` (not `requires`) the sidecar — summarization is best-effort *once
  running*, so a down/slow sidecar mid-sweep never blocks the worker or a digest (clusters degrade to
  baseline and retry).
- **Startup is the exception — a fail-loud gate.** A `llm-summarization` build verifies the sidecar is
  reachable (`{BASE_URL}/models`) before the worker starts / before `all` binds `/health`
  (`ensure_sidecar_ready` → `summarize::client::ensure_reachable`). An unreachable sidecar at boot is a
  deploy/config error (wrong `BULLETIN_LLM_BASE_URL`, sidecar absent, model failed to load), not a
  transient blip, so it is a **hard error**: the unit never reports healthy and the deploy rolls back,
  rather than silently shipping deterministic baselines forever. The wait window (the sidecar may still
  be loading its GGUF) is bounded by `BULLETIN_LLM_STARTUP_TIMEOUT_SECS` (default `60`); the systemd
  `ExecStartPost` health probe is widened to match when `llm.enable`.

---

## 2. Key decisions & deviations from the design doc

1. **`reqwest`, not `async-openai`.** The design (§3, `local-ml-options.md` §5) names `async-openai`.
   We implemented the client directly over the already-present `reqwest`: the request is a single JSON
   POST, this adds **no new dependency** (keeps the build closure flat and offline-friendly), and gives
   exact control of the `response_format`/grammar payload. Swap to `async-openai` later only if richer
   client features (streaming, tool-calls) are wanted.
2. **`facts` is a deterministic skeleton, not the Phase-2 comprehension output.** The design grounds the
   summarizer on `facts` from a GLiNER + tiny-LLM comprehension pass (`local-ml-options.md` §6, Phase 2)
   — **which is not built.** `extract_facts` currently derives `facts.entities` from the cluster's
   already-extracted entities and mines `numbers`/`dates` with a light scan; `event_type` / `state` /
   per-fact `certainty` stay at neutral defaults (so the summarizer degrades to "state asserted facts
   plainly" — the safe direction). **This is the single biggest follow-up** (see §4).
3. **Only the public sweep is wired into the worker.** Private cluster summaries (§5, subscriber RLS
   context) have a ready entry — `summarize::sweep_private(pool, subscriber_id, cfg)` — but are **not
   yet called**. Deliberately *not* hung off `generate()` (the punctual path); they want a dedicated
   best-effort step (see §4).
4. **Cluster tier schema only (at Phase A).** Story/thread/digest columns were deferred to their phases
   — since landed: thread (migration `028`, Phase B), story (`029`, Phase C), digest (`030`, Phase D).
5. **~~The render side is untouched.~~** **Done (§4.1, 2026-06-15):** the digest now reads
   `cluster.summary` into the email. `ClusterCard`/`cluster_cards` `SELECT summary` and deserialize it;
   `RenderItem` carries `headline` (the representative cluster's `summary.headline`, degrading to the raw
   `title`) and `summary` (the grounded `tldr_text`, omitted when no summary has run). `render.rs` fills
   the §6.1 zone-2 headline and zone-3 summary line from these, and composes the big-picture **lead
   deterministically** from the selected items' headlines (§2.4 — no model call). The per-item summary +
   big-picture lorem are retired. With the feature off, `summary` is
   the inert `'{}'` default, so the headline/lead degrade to the cluster titles — a real deterministic
   lead, no lorem.
   **Update (2026-06-16): the full §6 four-zone redesign landed** — the item row is now (1) the
   **context eyebrow** (`render_eyebrow`: thread label + delta on one nowrap line; "possibly" for a
   Probable thread, omitted for Uncertain; the standalone chip + `item_category` lorem are retired),
   (2) the **headline**, (3) the **grounded summary** rendered from the structured run-list with inline
   entity **badges** (`render_summary_runs`/`render_entity_badge`: repo dotted-tag / CVE pill / person
   chip, degrading to plain `surface`), and (4) the deterministic **provenance** line
   (`render_provenance`: `Across <source> · <source> — <when>` / `<source> — <when>`, replacing the
   "Related" list + the "Why" caption on the email). **The amber debug block is KEPT — not deleted**
   (a deliberate deviation from §6.1's "delete the debug block") — and **enriched** for Phases B/C: it
   now carries the summary provenance (story-synthesis vs cluster vs raw title + faithfulness band),
   the thread label/identity/**delta**, and the fused connections + `link_reason`s the email shed — so
   the full selection + summarization trace stays inspectable.

---

## 3. Caveats / known rough edges

- **Numeric gate is token-equality** (both output and grounding go through `tokenize_numeric`, so they
  agree on boundaries and on unit-suffix stripping `"40m"`→`"40"`). An output `"40"` is *not* grounded
  by a source `"4000"`. The remaining looseness is the unit-suffix stripping itself (the
  `llm-summarization.md` §9 "exact vs normalized" open question) — tighten once the Phase-2 comprehension
  pass supplies real `facts.numbers`.
- **Sidecar-down does not stick.** A model/transport error returns `None` from `summarize_cluster`, so
  the cluster is left unsummarized (`summarized_at` not advanced) and a later sweep retries once the
  sidecar recovers. A *gate rejection* still persists the deterministic baseline (stable, content-
  derived). Cost: a persistently-down sidecar re-attempts every due cluster each sweep — bounded by
  `max_per_sweep` and off the punctual path.
- **`services.llama-cpp` option names** (`model`/`host`/`port`/`package`/`extraFlags`) are assumed
  against current nixpkgs — **verify against the pinned nixpkgs** before deploying
  (`local-ml-options.md` §9 flags this surface as fast-moving). The module was not `nix`-evaluated in CI
  (no nix in the build env). The module passes `extraFlags = [ "--jinja" ]` so the sidecar honours the
  worker's `chat_template_kwargs` and `--reasoning-budget 0` (the thinking switches, below).
- **Reasoning models / empty completions.** A reasoning model (Qwen3 et al.) left in "thinking" mode
  spends the short `max_tokens` budget on a `<think>` block; llama.cpp's default
  `--reasoning-format auto`/`deepseek` then routes the thoughts to `message.reasoning_content` and
  leaves `content` **empty** — which serde reports as `EOF while parsing a value at line 1 column 0` —
  or the thinking blows the request timeout outright. Mitigated in layers, grounded in the
  [llama.cpp server README](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md):
  - **Server (the reliable switch):** the module runs the sidecar with `--jinja --reasoning-budget 0`
    ("immediate end" of thinking, enforced for every request regardless of the model template —
    `enable_thinking: false` alone is template-dependent and not always honoured, llama.cpp#13189).
  - **Request:** every call sends `chat_template_kwargs: {enable_thinking: false}` (config
    `disable_thinking`, default on; honoured on the `--jinja` path) as a second layer for templates
    that respect it.
  - **Client (defense in depth):** `client::strip_reasoning` drops any leading `<think>…</think>` still
    inlined into `content` (the `--reasoning-format none` shape), and an empty completion now bails with
    a clear, classifiable message (`failure_kind = "response"`) carrying `finish_reason` and naming the
    `reasoning_content`-only case, instead of a bare serde EOF. The cluster then degrades to baseline /
    retries, as for any other sidecar failure.
- **Operational knobs are env-tunable** (no recompile): `BULLETIN_LLM_REQUEST_TIMEOUT_SECS` (raise on
  slow hardware — default `120`), `BULLETIN_LLM_COMPREHEND` (`false` drops the extra per-cluster
  comprehension call to halve sidecar load when timeouts bite), `BULLETIN_LLM_DISABLE_THINKING`
  (clear for a model that genuinely needs thinking), and `BULLETIN_LLM_STARTUP_TIMEOUT_SECS` (how long
  the startup reachability gate waits for the sidecar before failing — default `60`). See
  `SummarizationConfig::from_env` / `ensure_sidecar_ready`.
- **The model path is never exercised in CI** (no sidecar). All *pure* logic is unit-tested (11 tests
  in `summarize::tests`); the network round-trip needs a live `llama-server`. To smoke-test locally:
  run a llama.cpp server, build with `--features llm-summarization`, set `BULLETIN_LLM_BASE_URL`,
  ingest, build (cluster pass).
- **No metrics yet** for the sweep (the worker logs `summarized`/`skipped`). Add a counter when wiring
  consumption.

---

## 4. Next phase — concrete TODO (ordered)

1. ~~**Finish Phase A render consumption.**~~ **Done (2026-06-15) — see §2 deviation 5.** Threaded
   `cluster.summary` into the digest: `cluster_cards` selects + deserializes it, `RenderItem` carries
   `headline`/`summary`, `render.rs` fills the headline + grounded summary line and composes the lead
   deterministically from the selected headlines; the per-item summary + big-picture lorem are retired.
   **Still open:** the `tldr` run-list → inline entity **badges** (§6.2, needs identity resolution at
   render), and the full §6 four-zone redesign (context eyebrow, deterministic provenance line replacing
   the "Related"/"Why" captions, deleting the amber debug block) — the flat `tldr_text` ships first.
2. ~~**Wire the comprehension pass into `extract_facts`**~~ **Done (2026-06-16).** A tiny
   grammar-constrained comprehension call now runs **before** the summarizer
   (`summarize::client::comprehend_cluster`), classifying `event_type` / `state` / `certainty` and
   folding them onto the deterministic `Facts` skeleton via `apply_comprehension` (closed-vocab
   re-validated against `EVENT_TYPES` / `STATES`, defense-in-depth). Reasoning is free (an `analysis`
   scratchpad — CRANE, avoid the "grammar tax"), only the classification is enum-constrained. The
   summarizer's hedge rule (§3.6) is now a mechanical branch on `facts.certainty`, not an inference.
   It is itself best-effort: off (`comprehend = false`) or unavailable ⇒ neutral defaults (asserted,
   plain), the safe direction. **Deviation from the design's GLiNER + tiny-LLM split:** the entity-span
   half is already served by M3's namespaced entity tokens (`facts.entities` from ground truth), so
   only the *reasoning* half (type/state/stance) is an LLM call — no separate span model deployed.
   `prompt_version` bumped to `2` (code + nix module) so the corpus re-summarizes with the richer facts.
3. ~~**Wire the private sweep.**~~ **Done (2026-06-16).** `summarize_private` (the private mirror of
   `summarize_public`) is folded into the `thread_maintenance` worker job — the per-subscriber,
   due-gated, best-effort pass that already walks the subscriber's content — calling
   `summarize::sweep_private` in the owner's RLS context. It runs regardless of the maintenance outcome
   and never fails the job; kept off `generate()`. (It therefore rides `thread-weighting`'s cadence;
   the realistic `llm-summarization` build keeps `thread-weighting` on by default, so both run.)
4. ~~**Phase B — thread label + delta eyebrow**~~ **Done (2026-06-16).** Migration `028` adds the
   thread tier (`summary`/`delta`/`delta_through`/`summary_model`/`summarized_at`). `thread_maintenance`
   now writes a **deterministic auto-label** (`summarize::auto_label`, top entities → "acme/auth +3")
   onto `thread.label` every pass — so the context eyebrow lights up even with the feature off — and a
   gated **label/delta sweep** (`summarize::sweep_thread_labels`) upgrades the label to a readable
   prose name (stored on `thread.summary`, the auto-label stays the baseline beneath) and composes the
   §5.2 delta flag from the new stories since `delta_through` (deterministic count `summarize::auto_delta`
   as the fallback). The label uses the lighter `clean_label` voice/length gate (a name, not a grounded
   claim); the delta uses `clean_delta` (≤6 words, no end punctuation).
5. ~~**Phase C — story synthesis**~~ **Done (2026-06-16).** Migration `029` adds the story tier
   (`summary`/`summary_sig`/`summary_model`/`summarized_at`). `summarize::sweep_stories` (folded into
   the per-subscriber pass, after the cluster sweep so member summaries exist) fuses the member cluster
   summaries into one cross-source headline + tldr (`synthesize_story` → `synthesize_facts` union +
   `STORY_SYSTEM_PROMPT`, the same faithfulness gate), **cached by the member signature**
   (`story_summary_sig` over the sorted member `summary_hash`es). Fire-time prefers `story.summary` and
   degrades to the representative cluster (cold-start). **Deviation:** the signature is keyed on member
   content alone — `thread_id` is *not* folded in (decoupling Phase C from fire-time thread-assignment);
   a story moving threads doesn't itself force a re-synthesis. Singleton/all-unsummarized stories are
   skipped (render already shows the representative cluster identically).
6. ~~**Phase D — authored big-picture lead**~~ **Done (2026-06-17).** Migration `030` adds the digest
   tier (`digest.lead text`, nullable). `digest::generate` now composes the lead through `digest_lead`
   → `authored_lead`: the deterministic Phase-A lead (`render::compose_lead`, now `pub(crate)`) is the
   fallback beneath a deadline-bounded best-effort editor's note (`summarize::client::authored_lead` over
   the selected headlines + the threads they advance, `LEAD_SYSTEM_PROMPT` + `lead_schema`, gated by
   `clean_lead` — voice/length/URL + numeric grounding against the headlines). It is the **one model call
   on the punctual path**, so it is wrapped in `tokio::time::timeout(cfg.lead_deadline, …)`
   (`SummarizationConfig::lead_deadline`, env `BULLETIN_LLM_LEAD_DEADLINE_SECS`, default 20s): on a
   transport miss, gate rejection, *or* the deadline it ships the deterministic lead and sends on time.
   The chosen lead is persisted onto `digest.lead` best-effort (`store::store_lead`) for the debug trace,
   parity with `digest.decisions`. **Feature-off / manual `dispatch_now` preview is unchanged** — render
   composes the deterministic lead at fire time (`render` now takes `lead: Option<&str>`; `None` ⇒
   deterministic), the column stays NULL, no model call. **Deviation from §2.4:** the lead is composed
   from the selected items' **headlines** (always present, gate-passed) + thread **labels**, not the full
   tldr/delta payloads — short, grounded inputs that keep the on-path call fast and the §3.4 gate simple
   (no entity-ref check — the lead is plain prose, it *names* threads rather than badging entities).
7. **Eval hook (`digest-explain`).** Run the faithfulness gate read-only over historical clusters to
   measure the Vectara-style entity/number accuracy rate before any summary touches a delivered digest
   (§3.4, §7).

---

## 5. How to verify what's here

```sh
# Pure logic (no sidecar, no DB): the gate, hash, facts, schema, baseline, serde.
cargo test -p bulletin-core --lib summarize

# Both feature configs compile clean (CI parity):
cargo clippy -p bulletin-core -- -D warnings
cargo clippy -p bulletin-core --features llm-summarization -- -D warnings
cargo clippy -p bulletin --features llm-summarization -- -D warnings
```

Default build (feature off) is unchanged behavior; `migrate` adds four nullable/defaulted columns +
one partial index. The deterministic digest is intact at every step (`llm-summarization.md` §8).
