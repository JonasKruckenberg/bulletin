//! The bulletin engine: three flows over a chain of append-only logs drained by idempotent
//! cursors.
//!
//! - [`ingest`] — poll connectors, normalize, append to the event log.
//! - [`cluster`] — drain the event log (build-watermark cursor) into `cluster` rows.
//! - [`link`] — per subscriber, fuse candidate clusters into cross-source `story`s (the pure,
//!   deterministic linking core; design §8.2). Runs inside the digest flow.
//! - [`digest`] — link a subscriber's candidate clusters into stories, select by recency, render, send.
//!
//! Each flow exposes a pure entry function over the DB; [`common`] holds the shared vocabulary.
//! Nothing here knows about the trigger (apalis/cron) or metrics — that's the binary's job.

pub mod cluster;
pub mod common;
pub mod digest;
pub mod ingest;
pub mod link;

// Ergonomic re-exports of the shared vocabulary.
pub use common::db::{connect, migrate};
pub use common::{event, fingerprint, kind, scope, status};
