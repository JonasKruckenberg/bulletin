# Digest System — Local ML Options (mid-2026 research)

**Status:** Research snapshot (14 June 2026). Verify fast-moving items before committing (§9).
**Companion to:** `digest-thread-layer.md` (the ML described here runs inside `thread_maintenance`/build —
write-side, best-effort, **off the punctual path**, §3.1 of `digest-system-design.md`).
**Question answered:** what locally-hostable ML (mid-2026) can power Bulletin's comprehension /
embedding / summarization, on a **Mac Mini M2 running NixOS**, as a **local sidecar** called from Rust,
with **no data egress** (the §12 trust property)?

> **Why local-only:** Bulletin ingests private Slack/GitHub/email content. LLM summarization is data
> egress with consent/security implications (`digest-system-design.md` §12). Running ML locally keeps
> the no-egress invariant intact — and an **all-Apache model stack** exists for every task, so there is
> no licensing compromise either.

---

## 1. Verdict (TL;DR)

1. **The box is the binding constraint, and worse on Linux than hoped.** A base M2 is
   **memory-bandwidth-bound (~100 GB/s)**; its **unified RAM (8/16/24 GB) is fixed for life** — pick
   model sizes from RAM first. Under **Asahi Linux** you get only the slow GPU path: llama.cpp offloads
   to Asahi's conformant "Honeykrisp" Vulkan at **~50 tok/s prompt, ~13.5 tok/s generation for 7B-Q4**,
   **~3–6× slower than macOS Metal on the same silicon**; CPU-only is ~5–10 tok/s for 1–3B.
2. **That's fine for Bulletin** — every ML task runs in `thread_maintenance`/build (best-effort
   materialization), **never blocking a digest fire**. We need throughput-over-minutes on small models,
   not interactive latency. "Fall behind, never wrong" (§3.1) already covers it.
3. **Strategic fork to decide (§3):** if real Linux isn't a hard requirement, **macOS + nix-darwin on
   the same Mac Mini unlocks Metal + MLX (20–87% faster) + CoreML/ANE.** Asahi's only inference-relevant
   win is being genuine ARM64 Linux.
4. **Recommended sidecar:** **`llama-server` (llama.cpp)** as the workhorse + **HF TEI** for
   embeddings/reranking. **`mistral.rs`** is the Rust-native alternative. **Avoid** TGI (archived),
   vLLM (CPU second-class), SGLang (no ARM-CPU), LocalAI (stale/broken in nixpkgs).
5. **All-Apache model stack:** Qwen3-Embedding-0.6B · bge-reranker-v2-m3 · GLiNER2-class · Qwen3.5-4B
   (or Granite-4.0-H-Micro). Driven from Rust via `async-openai`.

## 2. Hardware reality (Asahi / M2)

| Fact | Detail | Source |
|---|---|---|
| Bandwidth-bound | base M2 ~100 GB/s; generation is the bottleneck | sitepoint/popularai 2026 |
| RAM tiers (Q4_K_M ≈0.6 GB/B) | 8 GB → 3B comfortably / 7B tight · 16 GB → 8–14B · 24 GB → 14–20B | sitepoint 2026 |
| Asahi GPU works but slow | Honeykrisp Vulkan **1.4 conformant**; llama.cpp runs on it: 7B-Q4 ≈ **50 pp / 13.5 tg** tok/s | Asahi 2024; llama.cpp #10879 |
| Asahi vs Metal | M2 Max: **580/61 (Metal) vs 92/22 (Asahi Vulkan)** tok/s pp/tg → ~6.3×/2.8× | llama.cpp #10982 |
| CPU-only | ~5–10 tok/s for 1–3B Q4 — fallback, not the plan | sitepoint 2026 |
| Setup friction | Vulkan-in-container "no usable GPU found"; manual ICD + shader-cache env; lead Asahi GPU dev left Aug 2025 | llama.cpp #11944 |
| NixOS path | `tpwrules/nixos-apple-silicon` flake (`linux-asahi` + `mesa-asahi-edge`); M2 Mac Mini supported, bleeding-edge, lags Fedora Asahi Remix | nixos-apple-silicon 2026 |

## 3. Strategic fork — Asahi-Linux vs macOS + nix-darwin

| | **Asahi Linux + NixOS** | **macOS + nix-darwin** |
|---|---|---|
| GPU inference | Vulkan only, **~3–6× slower** | **Metal + MLX (20–87% faster than llama.cpp)** + CoreML/ANE |
| Ops | native systemd/journald/Docker; matches the NixOS module | nix-darwin (not full NixOS) |
| Maturity | bleeding edge, manual GPU plumbing | mature, turnkey GPU |
| Verdict | keep only if real-Linux is non-negotiable | **recommended for raw speed** |

**Recommendation:** unless Linux-native deployment is non-negotiable, run the box as **macOS +
nix-darwin** for ~3–6× faster inference + MLX. CPU-path note: on Asahi, **llamafile's tinyBLAS ARM
kernels** (30–500% CPU speedups) are worth benchmarking against `llama-server` for raw throughput.

## 4. Serving stack (sidecar over HTTP, called from Rust)

- **`llama-server` (llama.cpp) — the pick.** GBNF grammar **guarantees valid JSON** (JSON-Schema→grammar
  token-masking; `response_format`/`json_schema`), `--jinja` tool-calling, and serves
  **`/embedding` + `/v1/embeddings` + `/reranking` in one process**, with `-np` parallel slots +
  continuous batching. First-class NixOS `services.llama-cpp` (incl. `llama-cpp-vulkan`; set
  `MESA_SHADER_CACHE_DIR`). [llama.cpp server/grammar READMEs; NixOS wiki]
- **HF TEI — embeddings/rerank engine.** Official **`cpu-arm64` image**, native `/embed` + `/rerank`,
  itself Rust/Candle. Run as an OCI unit (not in nixpkgs). [TEI README v1.9]
- **`mistral.rs` (v0.8.x) — Rust-native alternative.** Runs **in-process** (the `mistralrs` crate) *and*
  as an OpenAI/Anthropic-compatible server; llguidance constraints (json-schema/regex/CFG); serves
  embeddings. Caveats: nixpkgs trails (~0.5.0), **no Vulkan** (CPU-f32 on Asahi), **no rerank endpoint**.
- **`llama-swap`** to hot-swap the 3–4 small models on one box.
- **Avoid:** **TGI** (archived/maintenance-mode 2026-03-21); **vLLM** (aarch64-Linux CPU wheels exist
  since v0.11.2, but CPU is second-class — ~2-thread reports, nightly "not for production"); **SGLang**
  (Intel-Xeon-AMX CPU only; Apple support is MLX/macOS); **LocalAI** (nixpkgs `local-ai` stale 2.28.0 &
  marked broken, no service module; reranker flaky early 2026).

## 5. Rust integration

- Sidecar over OpenAI-compatible API: **`async-openai` v0.41.0** (configurable `base_url`,
  `ResponseFormat::JsonSchema`, embeddings) or **`ollama-rs` v0.3.4** (`FormatType::StructuredJson`).
- In-process embeddings (no service): **`fastembed`** (ONNX, quantized BGE), **`candle` v0.10.2**, or
  **`ort`** (ONNX Runtime, ARM64).

## 6. Per-task model picks (all CPU/ARM-friendly; licenses flagged)

| Task | Pick (Apache/MIT unless noted) | Size | Notes |
|---|---|---|---|
| **Embeddings** (linking "same-meaning-no-shared-token" + identity edges, §8.2/§8.7) | **Qwen3-Embedding-0.6B** | 0.6B | Top small-MTEB (~64.3), **Matryoshka 32–1024 dims**, 32K ctx, official GGUF+ONNX. Truncate to 256–512d to halve pgvector HNSW RAM. |
| ″ alternates | Arctic-Embed-L-v2.0 (568M, 8192 ctx); Nomic-Embed-v2-MoE (~305M active, lightest); *EmbeddingGemma 308M — sub-200MB QAT but **Gemma 3 terms**, not OSI* | | **Avoid Jina v3 (CC-BY-NC).** |
| **Reranking** (relevance/priority, §8.3) | **bge-reranker-v2-m3** | 568M | TEI-servable. Alts: `mxbai-rerank-base-v2` (0.5B), `Qwen3-Reranker-0.6B`. Jina reranker CC-BY-NC — avoid. |
| **Extraction / NER** (entity spans) | **GLiNER2-class encoder** | ~encoder | F1 ≈ parity with GPT-4o on zero-shot NER, **130–208ms CPU, flat in label count** — 10×+ faster than an LLM emitting JSON. [arXiv 2507.18546] |
| **Comprehension** (event-type, detected→resolved state) | **Qwen3.5-2B/4B** + constrained *final* output | 2–4B | Hybrid (DiCoRe/CascadeNER): encoder for spans, LLM only for reasoning. **Don't hard-constrain the reasoning step** (10–30% "grammar tax"); scratchpad → constrain final JSON (CRANE). [arXiv 2408.02442, 2502.09061, 2604.06066] |
| **Summarization** (Story/Thread-delta headlines, §9.5) | **Qwen3.5-4B-Instruct** (Mar 2026, **Apache-2.0**) | 4B | See §7 — pick for *grounded faithfulness*, not size. |

## 7. Summarization — pick for faithfulness, mind the sub-4B cliff

Vectara grounded-summarization hallucination leaderboard (May 2026): **Phi-4 (14B) 3.7% · Qwen3-8B
4.8% · Granite-4.0-H-Small 5.2% · Qwen3-4B 5.7% · Gemma-3-4B 6.4%** — but **Phi-4-*mini* (3.8B) 23.5%**
(~6× worse than its 14B sibling). Faithfulness (entity/date/number accuracy) collapses below ~3–4B.

**Ranking (all Apache-2.0, CPU-runnable, off-hot-path):**
1. **Qwen3.5-4B** — best all-round (instruction-following, length control, ~2.5 GB Q4, 5.7%).
2. **Granite-4.0-H-Micro 3B** — best **memory profile** on a RAM-bound box (Mamba-2 hybrid, >70% RAM
   reduction at long context), top IFEval.
3. **SmolLM3-3B** — fully-open/reproducible fallback.

License notes: **Phi-4-mini is a trap** (MIT but 23.5% hallucination — don't use for deltas). **Gemma 4
(~Apr 2026) moved to Apache-2.0** (Gemma-4-E4B now a permissive option to test); **Gemma 3** and **Llama
3.x/4** remain restrictive. On 8 GB, prefer a **3B** over a 2B for faithful deltas.

**Non-negotiable grounding guardrails** (what makes 3–4B "good enough"): **extract-then-summarize** (feed
the pre-extracted facts from the GLiNER+LLM comprehension step), **low temperature, short outputs,
source-span grounding, plan-guided generation**; optional DPO-tuning against hallucination-injected
samples later. [arXiv 2504.09071, 2601.04212, 2507.22744]

**Latency:** ~22 tok/s (if Vulkan engages) → headline ~1 s, 150-tok delta ~7 s; pure-CPU ~8–10 tok/s →
delta ~15–19 s. Slow but irrelevant off the hot path.

**Bottom line:** a **4B Apache model with grounding guardrails is genuinely good enough** for Bulletin's
short headlines + deltas; a bigger box is needed only for sub-3% hallucination or long free-form
abstraction.

## 8. How this maps onto Bulletin

- **Embeddings** → semantic edge source for §8.2 linking + the `entity_edge` identity graph (§8.7) — the
  "same meaning, no shared token" bridge, now local instead of a deferred cloud dep.
- **Reranker** → relevance/priority over candidate stories (§8.3).
- **GLiNER + small LLM** → the comprehension pass (event type + detected→resolved) feeding Story state +
  Thread deltas.
- **Summarizer** → Story/Thread headlines + "what changed since last time" (§9.5).
- **All of it in `thread_maintenance`/build** (write-side, best-effort, off the punctual path, §11) — a
  slow M2 never delays a digest; it just makes materialization slightly less fresh. **100% local ⇒ §12
  no-egress holds**, with an all-Apache stack.

## 9. Verify before committing (fast-moving / contested)

- Asahi-vs-Metal gap (likely to narrow) [llama.cpp #10982]; M2 **`FEAT_BF16`** absence (test) [vLLM
  cpu.arm]; Qwen3.5 small-variant exact names/dates (HF cards 403 during research); llama.cpp Vulkan
  large-buffer (~4 GB) limit on current build [#13024]; exact `services.llama-cpp` Vulkan shader-cache
  unit settings; llamafile v0.10.x ARM throughput vs llama-server.

## 10. Key sources

Asahi Linux blog & feature-support; llama.cpp #10879/#10982/#11944 + server/grammar READMEs;
`tpwrules/nixos-apple-silicon`; HF TEI & TGI repos; vLLM CPU/ARM docs; SGLang CPU docs; LocalAI repo +
nixpkgs `local-ai`; QwenLM/Qwen3 & Qwen3.5 READMEs; Vectara hallucination-leaderboard (May 2026); IBM
Granite 4.0; blog.google/gemma-4; HuggingFaceTB/SmolLM3-3B; arXiv 2507.18546 (GLiNER2), 2408.02442,
2502.09061, 2604.06066, 2504.09071; crates.io (async-openai, ollama-rs, candle, mistralrs, fastembed);
model cards for Qwen3-Embedding-0.6B, Arctic-Embed-L-v2.0, bge-reranker-v2-m3, mxbai-rerank-v2.

*All API/model details are a 2026-06 research snapshot — re-verify versions, licenses, and Asahi
acceleration status against current sources before committing any model or runtime to the build.*
