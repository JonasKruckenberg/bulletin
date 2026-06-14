//! Error mapping for the gRPC API. Internal failures are logged server-side with their detail and
//! returned to the client as a generic `Internal` status, so engine internals never leak over the wire.
//! Caller-facing validation errors are constructed inline in the handlers as `InvalidArgument` /
//! `NotFound` (which is also what an RLS-filtered "no rows" miss maps to — the IDOR backstop).

use tonic::Status;

/// A `map_err` adapter for a database (or other internal) failure: log `context` + the error, return
/// an opaque `Internal`. Usage: `something(...).await.map_err(error::db("list connections"))?`.
pub fn db(context: &'static str) -> impl FnOnce(sqlx::Error) -> Status {
    move |e| {
        tracing::error!(error = %e, "api: {context}");
        Status::internal("internal error")
    }
}
