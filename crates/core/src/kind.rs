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

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
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
