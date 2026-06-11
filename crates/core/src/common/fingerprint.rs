use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct Fingerprint(pub [u8; 32]);

impl Fingerprint {
    pub fn compute(source: &str, stable_id: &str) -> Self {
        let mut h = Sha256::new();
        let src = source.as_bytes();
        let id = stable_id.as_bytes();
        h.update((src.len() as u64).to_le_bytes());
        h.update(src);
        h.update((id.len() as u64).to_le_bytes());
        h.update(id);
        Self(h.finalize().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn length_framed(a in "[a-z]{1,8}", b in "[a-z]{1,8}", id in "[a-z]{1,8}") {
            prop_assume!(a != b);
            prop_assert_ne!(
                Fingerprint::compute(&a, &id),
                Fingerprint::compute(&b, &id),
            );
        }
    }
}
