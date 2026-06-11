use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Scope {
    Public,
    Private(Uuid),
}
