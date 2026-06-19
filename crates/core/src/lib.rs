//! The bulletin engine: three flows over a chain of append-only logs drained by idempotent
//! cursors.
//!
//! - [`ingest`] — poll connectors, normalize, append to the event log.
//! - [`cluster`] — drain the event log (build-watermark cursor) into `cluster` rows.
//! - [`link`] — per subscriber, fuse candidate clusters into cross-source `story`s (the pure,
//!   deterministic linking core; design §8.2). Runs inside the digest flow.
//! - [`digest`] — link a subscriber's candidate clusters into stories, select by recency, render, send.
//! - [`thread`] — the cross-time weave: a background `thread_maintenance` job that turns the
//!   subscriber's stories into persistent `Thread`s and a projected entity-weight map the digest's
//!   relevance term reads (`docs/thread-layer.md`).
//! - [`identity`] — tiered, probabilistic entity-identity resolution that feeds the thread layer.
//! - [`summarize`] — write-side LLM pre-summarization (Phase A: the content-hashed `cluster.summary`
//!   foundation) and the on-path digest lead (Phase D). A mandatory part of the pipeline (§3.7): a
//!   cluster ships only with a faithful, gate-passed summary and a digest never ships without an LLM
//!   lead; failures are tracked errors with bounded escalating retries and quarantine
//!   (`docs/llm-summarization.md`).
//! - [`feedback`] — the append-only correction log (care/less, must/cannot-link).
//!
//! Each flow exposes a pure entry function over the DB; [`common`] holds the shared vocabulary.
//! Nothing here knows about the trigger (apalis/cron) or metrics — that's the binary's job.

pub mod cluster;
pub mod common;
pub mod digest;
pub mod feedback;
pub mod identity;
pub mod ingest;
pub mod link;
pub mod summarize;
pub mod thread;

// Ergonomic re-exports of the shared vocabulary.
pub use common::db::{
    begin_scope, connect, grant_runtime_role, migrate, with_scope, ScopeCtx, RUNTIME_ROLE,
};
pub use common::{event, fingerprint, kind, scope, secret, status};
