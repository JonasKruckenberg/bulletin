//! Cross-cutting vocabulary shared by every flow: the event record, its identity primitives,
//! the source/scope enums, the DB handle, and the read-only status dashboard. Flows depend on
//! `common`; `common` depends on no flow.

pub mod db;
pub mod entity;
pub mod event;
pub mod fingerprint;
pub mod kind;
pub mod scope;
pub mod secret;
pub mod status;
pub mod watermark;
