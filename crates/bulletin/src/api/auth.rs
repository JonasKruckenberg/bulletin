//! Request authentication for the gRPC API. The first cut has one credential — the **admin bearer**
//! — checked at the top of every admin-plane RPC. (Subscriber tokens, an async DB lookup, land with the
//! subscriber plane in A2; the helper shape here generalizes to that.)
//!
//! The core store fns already self-scope to `ScopeCtx::Admin`, so auth's job is purely *authentication*
//! — prove the caller may use the admin plane — not scope injection.

use tonic::{metadata::MetadataMap, Status};

pub struct AuthState {
    /// The configured admin bearer. `None` = admin plane not configured → every admin RPC is rejected
    /// (fail-closed), mirroring how the webhook catcher rejects deliveries without a secret.
    admin_key: Option<String>,
}

impl AuthState {
    pub fn new(admin_key: Option<String>) -> Self {
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
        if constant_time_eq(presented.as_bytes(), configured.as_bytes()) {
            Ok(())
        } else {
            Err(Status::unauthenticated("invalid admin credentials"))
        }
    }
}

/// Extracts the `authorization: Bearer <token>` value from request metadata.
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

/// Length-checked constant-time byte comparison. Leaking the *length* of a high-entropy key is
/// harmless; leaking byte positions via early-exit is not.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
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
    fn require_admin_fails_closed_when_unconfigured() {
        let auth = AuthState::new(None);
        let err = auth.require_admin(&md_with_auth("Bearer anything")).unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
    }

    #[test]
    fn require_admin_accepts_match_rejects_mismatch_and_missing() {
        let auth = AuthState::new(Some("s3cret".to_string()));
        assert!(auth.require_admin(&md_with_auth("Bearer s3cret")).is_ok());
        assert_eq!(
            auth.require_admin(&md_with_auth("Bearer nope")).unwrap_err().code(),
            Code::Unauthenticated
        );
        assert_eq!(
            auth.require_admin(&MetadataMap::new()).unwrap_err().code(),
            Code::Unauthenticated
        );
    }

    #[test]
    fn constant_time_eq_basics() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab")); // length mismatch
    }
}
