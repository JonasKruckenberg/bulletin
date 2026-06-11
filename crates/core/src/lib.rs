//! The bulletin engine: three flows over a chain of append-only logs drained by idempotent
//! cursors.
//!
//! - [`ingest`] — poll connectors, normalize, append to the event log.
//! - [`cluster`] — drain the event log (build-watermark cursor) into `cluster` rows.
//! - [`digest`] — drain a subscriber's new clusters (subscriber watermark cursor), render, send.
//!
//! Each flow exposes a pure entry function over the DB; [`common`] holds the shared vocabulary.
//! Nothing here knows about the trigger (apalis/cron) or metrics — that's the binary's job.

pub mod cluster;
pub mod common;
pub mod digest;
pub mod ingest;

// Ergonomic re-exports of the shared vocabulary.
pub use common::db::{connect, migrate};
pub use common::{event, fingerprint, kind, scope, status};
