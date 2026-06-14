//! Credential-at-rest: in-memory redaction + the interim envelope-encryption seal (design §6,
//! tech §6 "Secrets").
//!
//! **In memory.** Secret material is held in [`secrecy`]'s `SecretBox`/`SecretSlice`: a redacted
//! `Debug` (so a key never lands in a log line) and zeroize-on-drop (so it doesn't linger in freed
//! memory). Reaching the bytes is an explicit `.expose_secret()` — a grep-able audit point.
//!
//! **At rest.** A single app **master key** (32 bytes, sealed file/env) wraps every secret with
//! **envelope encryption**: each seal mints a fresh random **data-encryption key (DEK)**, encrypts
//! the plaintext under the DEK, then encrypts (*wraps*) the DEK under the master key — both legs
//! XChaCha20-Poly1305 (a 24-byte random nonce, so nonce reuse isn't a concern even across many
//! seals). The stored [`SealedSecret`] is `version ‖ wrap-nonce ‖ wrapped-DEK ‖ payload-nonce ‖
//! payload`; the plaintext DEK exists only transiently inside [`unseal`].
//!
//! Why the indirection (DEK-per-secret rather than master-over-plaintext): it is exactly the shape a
//! managed KMS wants — swapping the interim master key for a KMS "wrap/unwrap DEK" call (M5) or
//! adding per-connection secrets (Slack, M6) becomes a *backend* change to the wrap leg, not a
//! re-encryption migration. The `creds_ref` column already carries this envelope blob.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use secrecy::{ExposeSecret, SecretBox, SecretSlice};
use zeroize::Zeroizing;

/// XChaCha20-Poly1305 parameters (the crate enforces these, but we frame the envelope by hand).
const KEY_LEN: usize = 32; // master key / DEK
const NONCE_LEN: usize = 24; // XNonce (extended nonce → safe with random generation)
const TAG_LEN: usize = 16; // Poly1305 tag
const WRAPPED_DEK_LEN: usize = KEY_LEN + TAG_LEN; // DEK ciphertext + tag
const ENVELOPE_VERSION: u8 = 1;
/// Smallest a well-formed envelope can be: version + both nonces + wrapped DEK + an empty payload's
/// bare tag. A shorter blob is structurally invalid (rejected before any crypto).
const MIN_ENVELOPE_LEN: usize = 1 + NONCE_LEN + WRAPPED_DEK_LEN + NONCE_LEN + TAG_LEN;

/// Constant-time byte-slice equality, for comparing a presented credential (an API bearer token, a
/// key) against the expected one. Length-checked — leaking the *length* of a high-entropy secret is
/// harmless, leaking byte positions via an early return is not. The one audited compare every
/// credential check should route through, rather than hand-rolling its own.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// What went wrong handling a secret. Deliberately coarse: a caller can't act on *why* a decrypt
/// failed (and a precise reason would be an oracle), only that the material is unusable.
#[derive(Debug)]
pub enum SecretError {
    /// The base64/text wrapper didn't decode, or the key wasn't 32 bytes.
    Encoding(String),
    /// The envelope was truncated/misframed (wrong length or version).
    Format(String),
    /// AEAD open failed — wrong master key, or tampered/corrupt ciphertext.
    Decrypt,
}

impl std::fmt::Display for SecretError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecretError::Encoding(m) => write!(f, "secret encoding error: {m}"),
            SecretError::Format(m) => write!(f, "malformed sealed secret: {m}"),
            SecretError::Decrypt => write!(f, "secret decryption failed (wrong key or tampered)"),
        }
    }
}

impl std::error::Error for SecretError {}

/// The app master key — the one root secret the whole envelope hangs off. Held in a redacted,
/// zeroize-on-drop box; loaded once at startup from a sealed file/env (`BULLETIN_MASTER_KEY`) and
/// never logged. A managed KMS (M5) replaces this with a remote unwrap and the key never enters the
/// process at all.
pub struct MasterKey(SecretBox<[u8; KEY_LEN]>);

impl MasterKey {
    /// Mints a fresh random master key (the `secrets keygen` operator command). The caller prints
    /// [`to_base64`](Self::to_base64) once and stores it in the deployment's secret store.
    pub fn generate() -> MasterKey {
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let mut bytes = [0u8; KEY_LEN];
        bytes.copy_from_slice(&key);
        let mk = MasterKey(SecretBox::new(Box::new(bytes)));
        bytes.iter_mut().for_each(|b| *b = 0); // scrub the stack copy
        mk
    }

    /// Loads a master key from its base64 text form (`BULLETIN_MASTER_KEY`). Rejects anything that
    /// isn't exactly 32 bytes — a short key is a silent downgrade we never want to boot with.
    pub fn from_base64(s: &str) -> Result<MasterKey, SecretError> {
        let raw = Zeroizing::new(
            B64.decode(s.trim())
                .map_err(|e| SecretError::Encoding(e.to_string()))?,
        );
        let bytes: [u8; KEY_LEN] = raw
            .as_slice()
            .try_into()
            .map_err(|_| SecretError::Encoding(format!("master key must be {KEY_LEN} bytes")))?;
        Ok(MasterKey(SecretBox::new(Box::new(bytes))))
    }

    /// The base64 text form, for the `keygen` command's one-time output. (Exposes the key by
    /// design — that's the whole point of printing it for the operator to seal.)
    pub fn to_base64(&self) -> String {
        B64.encode(self.0.expose_secret())
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new_from_slice(self.0.expose_secret().as_slice())
            .expect("master key is exactly KEY_LEN bytes")
    }
}

/// An encrypted secret at rest — the opaque envelope stored in `connection.creds_ref` (or app
/// config). Carries no plaintext and no usable structure beyond its framing; the only way back to
/// the secret is [`unseal`] with the right master key.
pub struct SealedSecret {
    envelope: Vec<u8>,
}

impl SealedSecret {
    /// The transportable text form: base64 of the raw envelope. This is what an operator pastes
    /// into config / what lands in `creds_ref`.
    pub fn encode(&self) -> String {
        B64.encode(&self.envelope)
    }

    /// Parse the text form back into an envelope (structure validated lazily at [`unseal`]).
    pub fn decode(s: &str) -> Result<SealedSecret, SecretError> {
        let envelope = B64
            .decode(s.trim())
            .map_err(|e| SecretError::Encoding(e.to_string()))?;
        Ok(SealedSecret { envelope })
    }
}

/// Seal `plaintext` under `master`: mint a DEK, encrypt the plaintext under it, wrap the DEK under
/// the master key, and frame the two together. Each call uses fresh random nonces and a fresh DEK,
/// so sealing the same secret twice yields different envelopes (no equality oracle).
pub fn seal(master: &MasterKey, plaintext: &[u8]) -> Result<SealedSecret, SecretError> {
    // Fresh per-secret DEK; copied into a Zeroizing buffer so it's scrubbed when this fn returns.
    let mut dek = Zeroizing::new([0u8; KEY_LEN]);
    dek.copy_from_slice(&XChaCha20Poly1305::generate_key(&mut OsRng));
    let dek_cipher = XChaCha20Poly1305::new_from_slice(&dek[..]).expect("DEK is KEY_LEN bytes");

    let payload_nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let payload = dek_cipher
        .encrypt(&payload_nonce, plaintext)
        .map_err(|_| SecretError::Decrypt)?;

    let wrap_nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let wrapped_dek = master
        .cipher()
        .encrypt(&wrap_nonce, &dek[..])
        .map_err(|_| SecretError::Decrypt)?;

    let mut envelope = Vec::with_capacity(MIN_ENVELOPE_LEN + plaintext.len());
    envelope.push(ENVELOPE_VERSION);
    envelope.extend_from_slice(&wrap_nonce);
    envelope.extend_from_slice(&wrapped_dek);
    envelope.extend_from_slice(&payload_nonce);
    envelope.extend_from_slice(&payload);
    Ok(SealedSecret { envelope })
}

/// Unseal an envelope: unwrap the DEK under the master key, then decrypt the payload under the DEK.
/// Returns the plaintext in a zeroize-on-drop `SecretSlice`; the DEK itself never escapes this fn.
pub fn unseal(master: &MasterKey, sealed: &SealedSecret) -> Result<SecretSlice<u8>, SecretError> {
    let env = &sealed.envelope;
    if env.len() < MIN_ENVELOPE_LEN {
        return Err(SecretError::Format("envelope too short".into()));
    }
    if env[0] != ENVELOPE_VERSION {
        return Err(SecretError::Format(format!(
            "unsupported envelope version {}",
            env[0]
        )));
    }

    // Slice the fixed-width header: wrap-nonce ‖ wrapped-DEK ‖ payload-nonce ‖ payload.
    let mut off = 1;
    let wrap_nonce = XNonce::from_slice(&env[off..off + NONCE_LEN]);
    off += NONCE_LEN;
    let wrapped_dek = &env[off..off + WRAPPED_DEK_LEN];
    off += WRAPPED_DEK_LEN;
    let payload_nonce = XNonce::from_slice(&env[off..off + NONCE_LEN]);
    off += NONCE_LEN;
    let payload = &env[off..];

    let dek = Zeroizing::new(
        master
            .cipher()
            .decrypt(wrap_nonce, wrapped_dek)
            .map_err(|_| SecretError::Decrypt)?,
    );
    let dek_cipher = XChaCha20Poly1305::new_from_slice(&dek).map_err(|_| SecretError::Decrypt)?;
    let plaintext = dek_cipher
        .decrypt(payload_nonce, payload)
        .map_err(|_| SecretError::Decrypt)?;
    Ok(SecretSlice::from(plaintext))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_unseal_round_trips() {
        let master = MasterKey::generate();
        let secret = b"-----BEGIN RSA PRIVATE KEY-----\nMIIE...\n-----END-----";
        let sealed = seal(&master, secret).expect("seal");
        let opened = unseal(&master, &sealed).expect("unseal");
        assert_eq!(opened.expose_secret(), secret);
    }

    #[test]
    fn round_trips_through_text_encoding() {
        let master = MasterKey::generate();
        let sealed = seal(&master, b"webhook-signing-secret").expect("seal");
        let text = sealed.encode();
        let decoded = SealedSecret::decode(&text).expect("decode");
        let opened = unseal(&master, &decoded).expect("unseal");
        assert_eq!(opened.expose_secret(), b"webhook-signing-secret");
    }

    #[test]
    fn master_key_survives_base64_round_trip() {
        let master = MasterKey::generate();
        let reloaded = MasterKey::from_base64(&master.to_base64()).expect("reload");
        let sealed = seal(&master, b"x").expect("seal");
        // A key that round-tripped through text must still open what the original sealed.
        assert_eq!(
            unseal(&reloaded, &sealed).expect("unseal").expose_secret(),
            b"x"
        );
    }

    #[test]
    fn wrong_master_key_cannot_unseal() {
        let sealed = seal(&MasterKey::generate(), b"top secret").expect("seal");
        match unseal(&MasterKey::generate(), &sealed) {
            Err(SecretError::Decrypt) => {}
            other => panic!("expected Decrypt error, got {other:?}"),
        }
    }

    #[test]
    fn tampered_envelope_is_rejected() {
        let master = MasterKey::generate();
        let mut sealed = seal(&master, b"top secret").expect("seal");
        // Flip a byte in the payload ciphertext (last byte) — the Poly1305 tag must catch it.
        let last = sealed.envelope.len() - 1;
        sealed.envelope[last] ^= 0xff;
        assert!(matches!(
            unseal(&master, &sealed),
            Err(SecretError::Decrypt)
        ));
    }

    #[test]
    fn short_or_misversioned_envelope_is_format_error() {
        let master = MasterKey::generate();
        assert!(matches!(
            unseal(
                &master,
                &SealedSecret {
                    envelope: vec![1, 2, 3]
                }
            ),
            Err(SecretError::Format(_))
        ));
        let mut sealed = seal(&master, b"x").expect("seal");
        sealed.envelope[0] = 9; // bogus version
        assert!(matches!(
            unseal(&master, &sealed),
            Err(SecretError::Format(_))
        ));
    }

    #[test]
    fn rejects_wrong_length_master_key() {
        assert!(matches!(
            MasterKey::from_base64(&B64.encode([0u8; 16])),
            Err(SecretError::Encoding(_))
        ));
    }
}
