# LLM Summarization — Phase A Foundation: Implementation Handoff

**Status:** Implemented 2026-06-15. The **foundation** of `llm-summarization.md` Phase A — the schema,
the write-side summarization pipeline, the local-sidecar client, and the llama.cpp deployment — is
built, behind the `llm-summarization` cargo feature and a runtime flag, **off by default**. The render
consumption (filling the email's summary slots) and the per-subscriber/private wiring are the *next
phase*; this doc hands them off.
**Reads against:** `llm-summarization.md` (the design), `local-ml-options.md` (the serving stack),
`thread-layer.md` §3.1 (the "fall behind, never wrong" contract this inherits).

> **The one-line summary of what shipped:** a cluster can now be summarized into a content-hashed,
> grammar-constrained, faithfulness-gated `cluster.summary`, generated off the punctual path by a
> best-effort sweep that calls a 100%-local llama.cpp sidecar and **degrades to a deterministic
> baseline** on any miss. Nothing reads it into a digest *yet* — that is the next step, and it is small.

---

## 1. What was built (and where)

### 1.1 Schema — `crates/core/migrations/20200101000025_cluster_summary.sql`

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
`summary_hash`-invalidation point, §5), best-effort: it reads `SummarizationConfig::from_env()`, no-ops
unless `BULLETIN_LLM_ENABLED` is set, and never propagates an error. Compiled out without the binary's
forwarded `llm-summarization` feature.

### 1.5 Deployment — `flake.nix` + `nix/module.nix`

- The `bulletin` package is now built **with** `--features llm-summarization` (no new deps; rides the
  existing `reqwest`), so the feature can be toggled by config without a rebuild.
- `services.bulletin.llm`: `enable` (sets `BULLETIN_LLM_ENABLED=1` + the `BASE_URL`/`MODEL`/
  `PROMPT_VERSION` env), `baseUrl`, `model`, `promptVersion`, `serveLocally` (provisions
  `services.llama-cpp` on the port parsed from `baseUrl`), `package`, `modelPath`.
- The worker is `wants`/`after` (not `requires`) the sidecar — summarization is best-effort, so a
  down/slow sidecar never blocks the worker or a digest.

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
4. **Cluster tier schema only.** Story/thread/digest columns are deferred to their phases.
5. **The render side is untouched.** `cluster.summary` is produced and stored but nothing reads it into
   the email yet — the §6 row redesign is the next phase.

---

## 3. Caveats / known rough edges

- **Numeric gate is substring-based.** Because the miner strips unit suffixes (`"40m"` → `"40"`), the
  gate matches numeric tokens by *substring* against a grounded+source haystack. This is intentionally
  lenient on tiny tokens (the `local-ml-options`/`llm-summarization.md` §9 "exact vs normalized" open
  question). Tighten once the comprehension pass supplies real `facts.numbers`.
- **`services.llama-cpp` option names** (`model`/`host`/`port`/`package`) are assumed against current
  nixpkgs — **verify against the pinned nixpkgs** before deploying (`local-ml-options.md` §9 flags this
  surface as fast-moving). The module was not `nix`-evaluated in CI (no nix in the build env).
- **The model path is never exercised in CI** (no sidecar). All *pure* logic is unit-tested (11 tests
  in `summarize::tests`); the network round-trip needs a live `llama-server`. To smoke-test locally:
  run a llama.cpp server, set `BULLETIN_LLM_ENABLED=1` + `BULLETIN_LLM_BASE_URL`, ingest, build.
- **No metrics yet** for the sweep (the worker logs `summarized`/`skipped`). Add a counter when wiring
  consumption.

---

## 4. Next phase — concrete TODO (ordered)

1. **Finish Phase A render consumption (small, high-value).** Thread `cluster.summary` into the digest:
   - extend `digest::store::ClusterCard` + `cluster_cards` to `SELECT summary`, deserialize to
     `ClusterSummary`;
   - carry `headline` / `tldr_text` onto `RenderItem` (fall back to the cluster `title` when
     `is_empty()` — the gate's `Uncertain` baseline is already a good line);
   - in `render.rs`, fill `item_summary` from the representative cluster's `tldr_text` and prefer
     `summary.headline` for the headline; compose `digest.lead` **deterministically** from the selected
     items' headlines (§2.4/§3.1 — no model call). Retire the `item_summary` + big-picture lorem.
   - The `tldr` run-list → inline entity **badges** (§6.2) needs identity resolution at render; ship the
     flat `tldr_text` first, badges as a follow-up.
2. **Wire the comprehension pass into `extract_facts`** (Phase 2, `local-ml-options.md` §6): GLiNER
   spans + a tiny constrained LLM for `event_type` / `state` / per-fact `certainty`. This is what makes
   the hedge rule (§3.6) and the numeric gate actually sharp. Until then the summarizer is honest but
   blunt.
3. **Wire the private sweep.** Add a best-effort `summarize_private` step (a new apalis job, or fold
   into `thread_maintenance` which already walks the subscriber's stories) calling
   `summarize::sweep_private`. Keep it off `generate()`.
4. **Phase B — thread label + delta eyebrow** (§2.3, §6.1): the migration's thread columns +
   `thread_maintenance` producing the readable label and the watermarked delta.
5. **Phase C — story synthesis** (§2.2): the story columns + member-signature-cached cross-source
   rewrite in `thread_maintenance`.
6. **Eval hook (`digest-explain`).** Run the faithfulness gate read-only over historical clusters to
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
