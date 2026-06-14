//! Request authentication for the gRPC API. The first cut has one credential — the **admin bearer**
//! — enforced by an interceptor over the whole `AdminService` (so a newly-added RPC can't ship
//! unauthenticated by forgetting a per-handler check). (Subscriber tokens, an async DB lookup, land
//! with the subscriber plane in A2; the `AuthState` seam generalizes to that.)
//!
//! The core store fns already self-scope to `ScopeCtx::Admin`, so auth's job is purely *authentication*
//! — prove the caller may use the admin plane — not scope injection.

use std::sync::Arc;

use bulletin_core::secret::ct_eq;
use tonic::{metadata::MetadataMap, Request, Status};

pub struct AuthState {
    /// The configured admin bearer (trimmed). `None`/empty = admin plane not configured → every admin
    /// RPC is rejected (fail-closed), mirroring how the webhook catcher rejects deliveries without a
    /// secret.
    admin_key: Option<String>,
}

impl AuthState {
    pub fn new(admin_key: Option<String>) -> Self {
        // Trim both the configured key (here) and the presented token (in `bearer`) so a stray
        // trailing newline — common when a key is sourced from a file/env — can't lock the plane out.
        // An empty-after-trim key is treated as "not configured" (fail-closed), never a match.
        let admin_key = admin_key
            .map(|k| k.trim().to_string())
            .filter(|k| !k.is_empty());
        Self { admin_key }
    }

    /// Require a valid admin bearer on the request metadata. `Unauthenticated` if absent, malformed, or
    /// non-matching; the comparison is constant-time so a wrong key can't be recovered by timing.
    pub fn require_admin(&self, md: &MetadataMap) -> Result<(), Status> {
        let configured = self
            .admin_key
            .as_deref()
            .ok_or_else(|| Status::unauthenticated("admin API is not configured"))?;
        let presented = bearer(md)?;
        if ct_eq(presented.as_bytes(), configured.as_bytes()) {
            Ok(())
        } else {
            Err(Status::unauthenticated("invalid admin credentials"))
        }
    }
}

/// A gRPC interceptor enforcing the admin bearer on every method of the service it wraps. Hoisting auth
/// here — rather than a check at the top of each handler — means a newly-added RPC can't ship
/// unauthenticated by omission, and gives the subscriber plane (A2) a single identity-resolution seam.
pub fn admin_interceptor(
    auth: Arc<AuthState>,
) -> impl Clone + FnMut(Request<()>) -> Result<Request<()>, Status> {
    move |req: Request<()>| {
        auth.require_admin(req.metadata())?;
        Ok(req)
    }
}

/// Extracts the `authorization: Bearer <token>` value from request metadata (scheme case-insensitive,
/// surrounding whitespace trimmed).
fn bearer(md: &MetadataMap) -> Result<String, Status> {
    let raw = md
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("missing authorization metadata"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("malformed authorization metadata"))?;
    raw.strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))
        .map(|t| t.trim().to_string())
        .ok_or_else(|| Status::unauthenticated("authorization must be a Bearer token"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::{metadata::MetadataMap, Code};

    fn md_with_auth(value: &str) -> MetadataMap {
        let mut md = MetadataMap::new();
        md.insert("authorization", value.parse().unwrap());
        md
    }

    #[test]
    fn bearer_parses_case_insensitive_scheme_and_trims() {
        assert_eq!(bearer(&md_with_auth("Bearer tok")).unwrap(), "tok");
        assert_eq!(bearer(&md_with_auth("bearer tok ")).unwrap(), "tok");
    }

    #[test]
    fn bearer_rejects_missing_or_wrong_scheme() {
        assert_eq!(
            bearer(&MetadataMap::new()).unwrap_err().code(),
            Code::Unauthenticated
        );
        assert_eq!(
            bearer(&md_with_auth("Basic tok")).unwrap_err().code(),
            Code::Unauthenticated
        );
    }

    #[test]
    fn require_admin_fails_closed_when_unconfigured_or_blank() {
        for key in [None, Some(String::new()), Some("   ".to_string())] {
            let auth = AuthState::new(key);
            assert_eq!(
                auth.require_admin(&md_with_auth("Bearer anything"))
                    .unwrap_err()
                    .code(),
                Code::Unauthenticated
            );
        }
    }

    #[test]
    fn require_admin_accepts_match_rejects_mismatch_and_missing() {
        let auth = AuthState::new(Some("s3cret".to_string()));
        assert!(auth.require_admin(&md_with_auth("Bearer s3cret")).is_ok());
        assert_eq!(
            auth.require_admin(&md_with_auth("Bearer nope"))
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
        assert_eq!(
            auth.require_admin(&MetadataMap::new()).unwrap_err().code(),
            Code::Unauthenticated
        );
    }

    #[test]
    fn key_and_token_are_trimmed_consistently() {
        // A key with surrounding whitespace still authenticates a (trimmed) presented token.
        let auth = AuthState::new(Some(" s3cret \n".to_string()));
        assert!(auth.require_admin(&md_with_auth("Bearer s3cret")).is_ok());
    }
}
