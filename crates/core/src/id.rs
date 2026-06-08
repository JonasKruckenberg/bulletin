use serde::{Deserialize, Serialize};
use std::{
    fmt,
    hash::{Hash, Hasher},
    marker::PhantomData,
};
use uuid::Uuid;

pub struct Id<T> {
    uuid: Uuid,
    _kind: PhantomData<fn() -> T>,
}

impl<T> Id<T> {
    pub fn new(uuid: Uuid) -> Self {
        Self {
            uuid,
            _kind: PhantomData,
        }
    }

    pub fn as_uuid(&self) -> Uuid {
        self.uuid
    }
}

impl<T> Clone for Id<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Id<T> {}
impl<T> PartialEq for Id<T> {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
    }
}
impl<T> Eq for Id<T> {}
impl<T> Hash for Id<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.uuid.hash(state)
    }
}
impl<T> PartialOrd for Id<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for Id<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.uuid.cmp(&other.uuid)
    }
}
impl<T> fmt::Debug for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Id({})", self.uuid)
    }
}

impl<T> Serialize for Id<T> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.uuid.serialize(s)
    }
}

impl<'de, T> Deserialize<'de> for Id<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Uuid::deserialize(d).map(Self::new)
    }
}

#[cfg(feature = "sqlx")]
mod sqlx_impls {
    use super::*;
    use sqlx::{
        encode::IsNull,
        postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef},
        Decode, Encode, Postgres, Type,
    };

    impl<T> Type<Postgres> for Id<T> {
        fn type_info() -> PgTypeInfo {
            <Uuid as Type<Postgres>>::type_info()
        }
    }

    impl<T> Encode<'_, Postgres> for Id<T> {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<IsNull, sqlx::error::BoxDynError> {
            Encode::<Postgres>::encode_by_ref(&self.uuid, buf)
        }
    }

    impl<'r, T> Decode<'r, Postgres> for Id<T> {
        fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
            Uuid::decode(value).map(Self::new)
        }
    }
}
