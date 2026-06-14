use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum Scope {
    Public,
    Private(Uuid),
}

impl Scope {
    /// The `(scope_kind, scope_subscriber_id)` column pair this scope persists as — the single
    /// encoding shared by every store that writes a scoped row (the `event` log and the `cluster`
    /// cache), so the on-disk convention lives in one place rather than a hand-written match per store.
    pub fn to_columns(&self) -> (&'static str, Option<Uuid>) {
        match self {
            Scope::Public => ("public", None),
            Scope::Private(subscriber_id) => ("private", Some(*subscriber_id)),
        }
    }
}
