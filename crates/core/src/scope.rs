use crate::id::Id;
use serde::{Deserialize, Serialize};

pub struct Subscriber;

#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Scope {
    Public,
    Private(Id<Subscriber>),
}
