# M3 Implementation Handoff

**Purpose.** M3 — *per-subscriber linking*, the product's headline feature — is implemented on branch
`claude/m3-milestone-work-bw39id`. This doc captures what landed, the design decisions, and the seams
left for M4, so a fresh session can continue faithfully. Read alongside `IMPLEMENTATION-ROADMAP.md`
(§M3), `digest-system-design.md` (§8.2 linking, §10.2 reason records), and
`digest-technical-architecture.md` (§5.3 Cluster/Story, §6 determinism proptests).

---

## 1. What M3 is

> **Goal (roadmap §M3):** a story fuses clusters **across sources** — "the connections you would have
> missed." A private GitHub incident PR and a public RSS/advisory referencing the same CVE/URL surface
> as **one story** in the owner's digest, with a `link_reason`. Re-running generation keeps the story
> id stable; a later strong link retro-merges two stories with the oldest id winning.

**Exit criteria (the demo).** A private GitHub PR + a public advisory naming the same CVE → one story
with a `link_reason`; story ids stable across recompute; a later strong link retro-merges two
already-delivered stories (oldest id wins). The DB-backed
`pipeline::cross_source_story_fuses_private_and_public_via_cve` test encodes the first two; the pure
`link::tests::retro_merge_keeps_oldest_id_and_tombstones_loser` encodes the third.

**Not in M3** (deferred — see §5): embeddings/ANN linking, the shared public-story cache, entity NER
beyond structure + URL/CVE extraction, relevance/richness scoring (Story-vs-Note, priority, caps),
feedback (care-more/less, wrong-aggregation must-link/cannot-link), thread layer.

---

## 2. Locked design decisions (do not relitigate)

| Decision | Choice | Why |
|---|---|---|
| **Linking is pure** | `link::link(clusters, prior, mint) -> Assignment` — no I/O, no ambient clock; the id minter is injected | exhaustive determinism + id-stability proptests (design §6); the pipeline passes `Uuid::now_v7`, tests pass a counter |
| **Story is the digest unit** | selection/freeze/render shifted from cluster → story; a lone cluster is a singleton story (renders like pre-M3) | the exit criteria require rendering a *fused* story; a singleton keeps M1/M2 behavior intact |
| **Entities are namespaced** | `cve:` / `url:` / `domain:` / `repo:` / `user:` | the prefix both prevents cross-kind collisions and classifies link strength in one place (`entity::link_strength`) |
| **Three link tiers** | **strong** (`cve:`/`url:`) merge anything · **weak** (`repo:`/`user:`) corroborate-only · **non-linking** (`domain:`) | a shared domain is noise as an edge (every feed item shares one) — it stays for M4 affinity but never forms a link |
| **Asymmetric merge guard** | only **strong** edges may merge two *already-delivered* stories; a weak edge attaches a fresh cluster but never collapses two established stories | the textbook single-linkage *chaining* failure of transitive closure (confirmed in the M3 research pass) |
| **Stable-id forwarding** | survivor = **oldest** prior story id (uuidv7 `min`) a component carries; a prior id is claimed by **at most one** component (split-safe); absorbed ids → `merged_into` tombstone | "ids stay stable across recompute; oldest wins on retro-merge" (design §8.2) without id collisions |
| **Entity enrichment lives in `finalize`** | `EventBuilder::finalize` unions the connector's structural entities with `entity::derive` (cve/url/domain from title+body+links) | one uniform seal point; entities are *not* in the fingerprint, so enrichment never disturbs dedup |
| **Thresholds are consts** | edge weights/threshold/temporal window are named consts in `link/mod.rs` | design §15 moves them to a config table in M4; conservative defaults for now |

---

## 3. What landed (codebase orientation)

**New: entity vocabulary** (`crates/core/src/common/entity.rs`). `derive(title, body, links)` mines
`cve:`/`url:`/`domain:` (hand-rolled, no `regex` dep); `link_strength(entity) -> Option<LinkStrength>`
classifies strong/weak/non-linking. Called from `event::EventBuilder::finalize`; GitHub's `to_builder`
supplies namespaced `repo:`/`user:` structural entities.

**New: the linking core** (`crates/core/src/link/mod.rs`). The pure `link()` in four stages:
1. **Blocking** — inverted index over *linkable* entities → only candidate pairs that share a key.
2. **Scoring** — `W_JACCARD * jaccard + W_TEMPORAL * temporal_closeness`, promoted to a strong edge at
   score 1.0 on a shared strong key; weak edges must clear `WEAK_EDGE_THRESHOLD`.
3. **Components** — union-find; **strong pass first** (may merge anything), then **weak pass**
   (strongest-first, skipping a union that would collapse two already-established components).
4. **Forwarding** — claim the oldest prior id per component (uniquely); absorbed prior ids → `Merge`.
`member_reasons` attaches each cluster's strongest incident edge as its `link_reason`.

**New: story persistence** (`crates/core/src/link/store.rs`). `candidate_clusters` (the linking input,
same `public ∪ own-private` + freshness-floor predicate the digest used pre-M3), `load_prior_members`
(prior assignment, with `delivered`), `persist_assignment` (upsert survivors + tombstone merges),
`story_members` (resolve a frozen story → clusters, one merged_into hop), `mark_stories_delivered`.

**Changed: cluster rollup** (`cluster/mod.rs`, `cluster/store.rs`). `ClusterRollup` now folds the
union of event `entities` and the `first_event_time`; `upsert_cluster` writes them.

**Changed: digest path** (`digest/mod.rs`, `digest/store.rs`, `digest/select.rs`, `digest/render.rs`).
`link_and_select` (link → optionally persist → rank stories); `generate` persists + freezes story ids;
`dispatch_now`/`explain` link in-memory (persist nothing). `Candidate`/`Decision` carry a generic `id`
(now a story id). `RenderItem` gained `connections` (the fused cross-source members + their
`link_reason`); the HTML/plaintext renderers show a "Connected across sources" block. `explain` is
story-based and reuses the exact render assembly.

**Migrations.** `…017_cluster_entities.sql` (cluster `entities text[]` + GIN `cluster_entities` +
`first_event_time`); `…018_story.sql` (`story` table; `digest_item` dropped + recreated keyed by
`story_id` — it is a rebuildable projection artifact, so swap-outright rather than expand-contract).

**Tests.** Pure: `entity` (extraction/classification), `link` (8 unit + 3 proptests: determinism,
id-stability, partition). DB-backed (Docker): `pipeline::cross_source_story_fuses_…`,
`build_groups_events_into_clusters` and the isolation tests updated to the story flow.

---

## 4. The pipeline, after M3

```
GenerateDigest(subscriber):
  build_private               # subscriber's private clusters, just-in-time (unchanged)
  candidate_clusters          # public ∪ own-private, within the freshness floor  → LinkCluster[]
  load_prior_members          # the prior story assignment (for id forwarding)
  link(clusters, prior)       # PURE: blocking → scoring → components → forwarding → Assignment
  persist_assignment          # upsert survivor stories + tombstone retro-merge losers
  select(stories by recency)  # cap at max_items (unchanged pure select)
  create_with_items           # freeze the selected STORY ids as digest_item rows
  render_items                # digest_item → story.clusters → cluster cards → RenderItem(+connections)
  deliver → mark_delivered    # also stamps story.last_delivered_at (gates the asymmetric guard)
```

`PublicBuild` is unchanged (public clusters, shared/amortized). Linking is per-subscriber inside
`GenerateDigest` because a story can fuse public clusters with the subscriber's *own* private ones.

---

## 5. Seams left for M4 (and beyond)

- **Story rollups for scoring.** `story` carries membership + recency span only. M4's
  richness/priority needs `event_count`, `source_diversity`, `content_depth`, `max_severity` — add
  them to the cluster rollup first (cluster currently caches none of these), then aggregate onto the
  story. `source_diversity` (distinct member sources) is free and is the literal "across sources" signal.
- **Reason records as types** (design §10.2). `link_reason` is a free-text string today
  (`"shared cve:CVE-2026-1234"`). M4 makes link/story/note/drop rationales structured + stored in
  `digest_item.reasons`.
- **Feedback → must-link / cannot-link** (design §10.3). "Wrong aggregation" becomes per-subscriber
  edge constraints fed into the pure `link()` — the function already takes the full edge set, so this
  is an additive input, not a redesign.
- **Threshold/weight tuning → config table** (design §15). The consts in `link/mod.rs`
  (`W_JACCARD`, `W_TEMPORAL`, `WEAK_EDGE_THRESHOLD`, `TEMPORAL_WINDOW_DAYS`) move to a config table.
- **RLS (M2 Phase 4).** Story reads/writes are fenced by `subscriber_id` at the query layer, exactly
  like clusters; when RLS lands, `story` joins the FORCE-RLS set and the `with_scope` wrapper.
- **Blocking (design §8.2/§11).** `candidate_clusters` already GIN-seeds the **cross-boundary** half
  of the blocking set — public clusters sharing a strong key (`cve:`/`url:`) with the subscriber's own
  private clusters, beyond the freshness floor (so a fresh private incident links to an aged public
  advisory). The other half — public clusters matching the subscriber's **affinity** — lands with
  relevance in **M4**; until then the in-floor arm carries the public set. Further levers: a normalized
  `cluster_signal` self-join, the shared public-story cache, and embeddings (`vector` + ANN) — all
  schema-additive, none needed at single-operator dogfood scale.
- **`digest-explain` already shows stories + per-member `link_reason`**; a dedicated linking
  inspector could come later but `digest-explain` covers it.

---

## 6. Commands

`cargo fmt --all`; `cargo clippy --workspace --all-targets` (clean); pure tests:
`cargo test -p bulletin-core --lib --test rss --test github`; full (Docker):
`cargo test --workspace` / `cargo nextest run`.
