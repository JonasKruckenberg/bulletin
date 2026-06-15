# Operating the digest: evaluating & tuning selection

Operator notes for using the **eval harness** to tune what the digest selects — the deterministic
backbone (M4 scoring), before any ML. It's the Phase-0 keystone from
[`local-ml-options.md`](local-ml-options.md) §0.1: *nothing below is tunable without it*, and it's how
any later ML signal must prove its lift. Grounds: `system-design.md` §8.3–§8.4 (scoring) and §10.3
(feedback / eval).

All commands here are **read-only except `config-set`** and safe to re-run.

## The three tools

| Command | Answers | Reads |
|---|---|---|
| `bulletin debug eval <sub> [--limit N]` | "How has selection *been* doing?" — a retrospective scorecard | The **frozen** `digest.decisions` logs (scored at each past fire under the config live *then*) |
| `bulletin debug eval <sub> --config trial.json` | "What would config X do vs current, over real history?" — an **A/B sweep** | Replays the frozen candidate **snapshots** under both configs |
| `bulletin debug digest-explain <sub>` | "What would the *next* digest do under the current config?" | A fresh `link → select` over the current window |
| `bulletin debug config` / `config-set` | Read / change the tunable knobs (`digest_config`) | The singleton config row |

**The mental model that matters:** plain `eval` numbers don't move when you change config — they score
history already decided. You see a config change's effect three ways: instantly forward via
`digest-explain`, as a head-to-head over history via `eval --config`, and (after new digests fire) in
plain `eval`.

## The tuning loop

```sh
# 1. Diagnose — read the structural block (and precision/nDCG once feedback exists).
bulletin debug eval <sub>

# 2. Capture the current config, edit a trial copy.
bulletin debug config > trial.json
$EDITOR trial.json                      # e.g. story_cap 5→3, recency_half_life_days 3→2

# 3. A/B it over real history — current vs trial, side by side, same inputs.
bulletin debug eval <sub> --config trial.json

# 4. If the trial wins, apply it (only the flags you pass change).
bulletin debug config-set --story-cap 3 --recency-half-life-days 2.0

# 5. (optional) Preview the very next digest under the new live config.
bulletin debug digest-explain <sub>
```

Run as the service user, which presets `DATABASE_URL`: `sudo -u bulletin bulletin debug …`.

## The knobs (`digest_config`) and what they do

| Field | Default | Effect / direction |
|---|---|---|
| `recency_half_life_days` | 3 | **Main freshness dial.** Lower → fresher-biased, smaller digests; higher → backlog persists |
| `story_cap` / `note_cap` | 5 / 20 | Per-format volume ceilings — the most direct overwhelm control |
| `scope_bonus` | 0.5 | How much your own private content outranks public |
| `corroboration_weight` | 0.5 | Priority boost when independent **sources corroborate** one story (2 src → ×1.25, 3 → ×1.33, saturating). `0` disables |
| `relevance_floor` | 0.0 | Inclusion gate. **Mostly a post-feedback knob** (see caveats) |
| `resurface_penalty` | 0.25 | How hard a no-news re-surface is damped (lower = more aggressive fade) |
| `thread_half_life_days` | 21 | How long an invested thread lingers — **only with the `thread-weighting` build** |
| `severity_weight` | 0.1 | **Inert today** — no v1 source emits severity |

## Reading the metrics → what to change

Structural block (always populated):

- **`mean items/digest` too high** → lower `story_cap`/`note_cap`, or shorten `recency_half_life_days`.
- **Frequent `empty digests`** → lengthen `recency_half_life_days` or raise caps; if *most* are empty,
  you're content-starved, not mis-tuned.
- **High `cap-limited` + many `over-cap` drops** → you're discarding a lot, so ranking quality is
  load-bearing: check the priority order in `digest-explain` before just raising caps.
- **`story_share` near 0% or 100%** (all-Notes / all-Stories) → this is a **code change, not a knob**:
  the Story/Note line lives in `richness()` in `select.rs`, not `digest_config`. The metric tells you
  *whether* you need to touch it (`system-design.md` §15 open question).
- **Unexpected `below-floor` drops** → a thread/feedback term went negative; confirm via the `rel=`
  values in `digest-explain`.

Feedback block (populated once a feedback surface exists):

- **Low `precision`** (high `care_less`) → raise `relevance_floor`, and inspect which entities/sources
  draw `care_less` via `digest-explain`.
- **Low `nDCG`** (ranking order wrong) → tune the priority dials: `recency_half_life_days`,
  `scope_bonus`, `thread_half_life_days`.

## The config sweep (`eval --config`)

`eval --config trial.json` replays a subscriber's recent digests under **both** the current config and
the trial config, over the *identical* frozen candidate snapshots, and prints both metric sets — a true
A/B that isolates the config change. The trial file is just the JSON `debug config` prints (copy →
edit). Only digests fired **after the snapshot column landed** (migration `…025`) are replayable; older
ones are silently skipped. For a zero-prod-risk sweep, do it on a restored snapshot (README Tier 0):
`pg_dump -Fc` prod → restore locally → loop `eval --config`.

## Caveats — read before you tune

1. **No feedback surface exists yet** (the `feedback` engine is built but unwired — `feedback::submit`
   has no caller). So **today the harness populates only the structural block**; `precision`/`nDCG` show
   "pending" until the feedback API ships (M5). Structural metrics are still enough to tune volume,
   freshness, and the Story/Note balance.
2. **Precision ≠ recall.** Feedback only exists on *shown* items, so the harness can tell you you're
   showing junk, never that you *missed* something. Don't chase precision upward without watching
   `empty digests` / `over-cap` — you can quietly starve the digest. Recall waits on consented
   audit-digests / the entropy budget (`system-design.md` §14).
3. **`relevance_floor` is near-inert pre-feedback.** Base relevance is ~1.0 for every candidate, so a
   floor in (0,1) does nothing and a floor >1 drops everything without a private/thread signal. Leave it
   at 0 until feedback/thread weights give relevance real spread.
4. **Build flag matters.** Without the `thread-weighting` feature, relevance is pure recency —
   `thread_half_life_days` and most of `relevance_floor` are inert.
5. **Per-subscriber + small samples are noisy.** Run across several subscribers and a reasonable
   `--limit`; one quiet week isn't a signal. Eval is per-subscriber by design (the decision logs +
   feedback are the subscriber's own, read in *their* RLS context — never a cross-tenant admin read).
6. **`severity_weight` does nothing yet** (no source emits severity).
