use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
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

    /// Whether this source can emit *private* (scope-restricted) events, and therefore requires an
    /// owning subscriber on its connection — without one, `finalize` would have no scope to bind a
    /// private item to. RSS is public-only (a feed URL is global); GitHub sees private repos. Keep in
    /// sync with the `connection_private_source_owned` CHECK in the connection migration.
    pub fn can_emit_private(self) -> bool {
        match self {
            SourceKind::Rss => false,
            SourceKind::Github => true,
            // Slack (M6) has private channels; revisit its owner policy when it lands.
            SourceKind::Slack => true,
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

/// Adapter-declared depth signal: how much material an event carries. **Ordered**
/// (`Message < Announcement < Longform`) so a cluster's `content_depth` can be `max()` over its
/// events and feed the later Story-vs-Note classification (design §5.1/§8.3). The connector sets it
/// because source semantics live there — a GitHub release is an announcement, an RSS item is
/// longform, a chat/comment is a message; deriving it downstream from body length would be a
/// gameable heuristic (§7.1).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize, Deserialize)]
pub enum ContentKind {
    Message,
    Announcement,
    Longform,
}

impl ContentKind {
    pub fn as_str(self) -> &'static str {
        match self {
            ContentKind::Message => "message",
            ContentKind::Announcement => "announcement",
            ContentKind::Longform => "longform",
        }
    }
}

impl TryFrom<&str> for ContentKind {
    type Error = &'static str;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            "message" => Ok(ContentKind::Message),
            "announcement" => Ok(ContentKind::Announcement),
            "longform" => Ok(ContentKind::Longform),
            _ => Err("unknown content kind"),
        }
    }
}

/// `SourceKind`/`ContentKind` round-trip as their `as_str()` text in Postgres, so `.bind(kind)` and
/// `row.try_get::<_, _>("col")` work directly — no per-query decode boilerplate. The macro stamps
/// the same text-backed `Type`/`Encode`/`Decode` triple for each enum.
mod sqlx_impls {
    use super::{ContentKind, SourceKind};
    use sqlx::{
        encode::IsNull,
        error::BoxDynError,
        postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef},
        Decode, Encode, Postgres, Type,
    };

    macro_rules! text_enum_sqlx {
        ($ty:ty, $what:literal) => {
            impl Type<Postgres> for $ty {
                fn type_info() -> PgTypeInfo {
                    <str as Type<Postgres>>::type_info()
                }
                fn compatible(ty: &PgTypeInfo) -> bool {
                    <str as Type<Postgres>>::compatible(ty)
                }
            }

            impl Encode<'_, Postgres> for $ty {
                fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
                    <&str as Encode<Postgres>>::encode_by_ref(&self.as_str(), buf)
                }
            }

            impl<'r> Decode<'r, Postgres> for $ty {
                fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
                    let s = <&str as Decode<Postgres>>::decode(value)?;
                    <$ty>::try_from(s).map_err(|_| format!("unknown {}: {s}", $what).into())
                }
            }
        };
    }

    text_enum_sqlx!(SourceKind, "source kind");
    text_enum_sqlx!(ContentKind, "content kind");
}
