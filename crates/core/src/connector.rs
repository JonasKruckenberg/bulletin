use crate::event::EventBuilder;

pub struct Batch<I, C> {
    pub items: Vec<I>,
    pub cursor: C,
}

#[derive(Debug)]
pub enum SourceError {
    Request(String),
    Parse(String),
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceError::Request(msg) => write!(f, "request error: {msg}"),
            SourceError::Parse(msg) => write!(f, "parse error: {msg}"),
        }
    }
}

impl std::error::Error for SourceError {}

/// Per-tenant live worker for one `connection` row. Every connector implements this; webhook
/// sources additionally implement `RealtimeConnection`.
pub trait Connection: Send + Sync {
    /// Opaque, source-private incremental-fetch position. Infra persists as JSON, never reads it.
    type Cursor: serde::Serialize + serde::de::DeserializeOwned + Default + Send + Sync;
    /// One unit of content from the source, complete after a poll.
    type Item: Send;

    fn poll(
        &self,
        cursor: Self::Cursor,
    ) -> impl std::future::Future<Output = Result<Batch<Self::Item, Self::Cursor>, SourceError>> + Send;

    /// Pure normalization: source-specific item → connector-side event builders. Infra calls
    /// `finalize(scope)` on each builder to stamp the scope boundary and fingerprint.
    fn to_events(&self, item: Self::Item) -> Vec<EventBuilder>;
}
