use serde::{Deserialize, Serialize};

// #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
// pub enum SourceKind {
//     Rss,
//     Github,
//     Slack,
// }

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub enum ContentKind {
    Message,
    Announcement,
    Longform,
}
