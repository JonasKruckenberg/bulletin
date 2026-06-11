use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum SourceKind {
    Rss,
    Github,
    Slack,
}

impl SourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            SourceKind::Rss => "rss",
            SourceKind::Github => "github",
            SourceKind::Slack => "slack",
        }
    }
}

impl TryFrom<&str> for SourceKind {
    type Error = &'static str;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "rss" => Ok(SourceKind::Rss),
            "github" => Ok(SourceKind::Github),
            "slack" => Ok(SourceKind::Slack),
            _ => Err("unknown source kind"),
        }
    }
}

/// `SourceKind` round-trips as its `as_str()` text in Postgres, so `.bind(source)` and
/// `row.try_get::<SourceKind, _>("source")` work directly — no per-query decode boilerplate.
mod sqlx_impls {
    use super::SourceKind;
    use sqlx::{
        encode::IsNull,
        error::BoxDynError,
        postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef},
        Decode, Encode, Postgres, Type,
    };

    impl Type<Postgres> for SourceKind {
        fn type_info() -> PgTypeInfo {
            <str as Type<Postgres>>::type_info()
        }
        fn compatible(ty: &PgTypeInfo) -> bool {
            <str as Type<Postgres>>::compatible(ty)
        }
    }

    impl Encode<'_, Postgres> for SourceKind {
        fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
            <&str as Encode<Postgres>>::encode_by_ref(&self.as_str(), buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for SourceKind {
        fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
            let s = <&str as Decode<Postgres>>::decode(value)?;
            SourceKind::try_from(s).map_err(|_| format!("unknown source kind: {s}").into())
        }
    }
}
