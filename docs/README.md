# Bulletin — Documentation

Design and reference docs for Bulletin. The top-level [`../README.md`](../README.md) is the operator's
guide (concepts + deployment); this directory is the design record.

**A note on status.** These docs describe the *target* design; the build is staged by milestones. As of
2026-06-14, **M1–M3 plus a first slice of the Thread layer are implemented; M4–M6 are pending** (Slack
included). Each doc carries an "as-built reconciliation" note where its prose describes more than is yet
built. The single source of truth for what's shipped is [`roadmap.md`](roadmap.md).

## Reading order

1. **[`system-design.md`](system-design.md)** — start here. The product thesis, visibility scopes, the
   canonical event model, the data model, and the aggregation → generation pipeline. Owns *what* we
   build and *why*.
2. **[`technical-architecture.md`](technical-architecture.md)** — the companion: *how* it's built. The
   Rust runtime, the Postgres-orchestrated batch topology, the connector trait family, and the
   cross-cutting concerns (observability, reliability, testing, secrets, SSRF).
3. **[`roadmap.md`](roadmap.md)** — the build sequence. Milestones M1–M6, aggressively scoped, with
   exit criteria and current status.

## Deep dives & references

| Doc | What it owns |
|---|---|
| [`thread-layer.md`](thread-layer.md) | The persistent per-subscriber **Thread** weave + **tiered probabilistic identity** + confidence-as-rendered-signal. Designed-for; first slice implemented (§9). |
| [`data-sources.md`](data-sources.md) | The candidate-source **backlog** — everything beyond the v1 set, scored on the same connector axes. Research snapshot. |
| [`local-ml-options.md`](local-ml-options.md) | Locally-hostable ML (mid-2026) for the deferred ML layer that runs inside `thread_maintenance` — no data egress. Research snapshot. |
| [`web-frontend.md`](web-frontend.md) | The web surface as a backend/product concern — the read API, auth/IDOR/RLS on the read path, delivery-tech recommendation. Scoping (lands around M5). |
| [`github-events.md`](github-events.md) | Reference map of GitHub's webhook/event surface and what M2 ingests. |

## Conventions

- Docs cross-reference each other by filename + section (`system-design.md` §8.2). They are co-located,
  so bare-filename links resolve within this directory.
- "Research snapshot" docs (`data-sources.md`, `local-ml-options.md`) capture mid-2026 external API /
  model facts — **re-verify before building** against them; vendors move fast.
