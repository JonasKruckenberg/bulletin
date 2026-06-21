use crate::common::{
    event::EventBuilder,
    kind::{ContentKind, SourceKind},
};
use crate::ingest::{html_text, Batch, Connection, SourceError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io::BufReader;

/// Per-connection config persisted in `connection.config`: just the feed URL.
#[derive(Debug, Clone, Deserialize)]
pub struct RssConfig {
    pub url: String,
}

/// Opaque cursor for RSS/Atom feeds: HTTP conditional-GET validators.
#[derive(Serialize, Deserialize, Default)]
pub struct RssCursor {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

pub struct RssItem {
    pub id: String,
    pub title: String,
    /// Plain text rendered from the item's HTML body (`<content:encoded>`/`<description>` for RSS,
    /// `<content>`/`<summary>` for Atom). `None` when the feed carries no body — the summarizer then
    /// falls back to title-only grounding, as it did before bodies were extracted.
    pub body: Option<String>,
    pub link: Option<String>,
    pub published: Option<DateTime<Utc>>,
}

/// Max stored body length (chars). A feed `<content>` can be a whole article, but the summarizer only
/// needs a few paragraphs of grounding (and `source_corpus` re-budgets across the cluster on top), so
/// cap here to keep DB rows and the model's input bounded.
const MAX_BODY_CHARS: usize = 2000;

/// The shared depth threshold (chars) — defined once in [`crate::ingest`] so the ingest gate here and the
/// article-fetch re-derivation can't drift. Applied here to the feed snippet at ingest: an item whose
/// snippet clears it is [`ContentKind::Longform`] (enough to ground a Story tldr), else a thin
/// [`ContentKind::Announcement`] → a headline-only Note. A late article fetch re-applies it to the fetched
/// `full_text` and *raises* the depth (`ingest::fetch`), so a real article behind a teaser snippet still
/// earns its Story depth.
use crate::ingest::LONGFORM_MIN_CHARS;

/// Max HTML handed to the renderer (see [`html_text::render`]): a full-article `<content>` is parsed
/// only up to the leading slice that comfortably yields the kept text, so ingest work stays
/// proportional to what we store ([`MAX_BODY_CHARS`]), not to feed size.
const MAX_BODY_HTML_CHARS: usize = 16_000;

/// Render a feed item's HTML body fragment to the plain text we store and summarize. `None` for
/// empty/blank input (or markup that renders to nothing), so a content-less item keeps `body = None`.
/// Thin wrapper over the shared [`html_text::render`] with the feed-body caps.
fn body_text(html: &str) -> Option<String> {
    html_text::render(html, MAX_BODY_HTML_CHARS, MAX_BODY_CHARS)
}

pub struct RssConnection {
    pub feed_url: String,
    client: reqwest::Client,
}

impl RssConnection {
    pub fn new(feed_url: impl Into<String>) -> Self {
        Self {
            feed_url: feed_url.into(),
            client: reqwest::Client::new(),
        }
    }
}

pub fn parse_feed(bytes: &[u8]) -> Result<Vec<RssItem>, SourceError> {
    // Try RSS first; fall back to Atom.
    if let Ok(channel) = rss::Channel::read_from(BufReader::new(bytes)) {
        return Ok(channel
            .into_items()
            .into_iter()
            .map(|item| {
                let id = item
                    .guid()
                    .map(|g| g.value().to_owned())
                    .or_else(|| item.link().map(str::to_owned))
                    .or_else(|| item.title().map(str::to_owned))
                    .unwrap_or_default();
                let title = item.title().unwrap_or(&id).to_owned();
                // Prefer `<content:encoded>` (the full article) over the short `<description>`.
                let body = item
                    .content()
                    .or_else(|| item.description())
                    .and_then(body_text);
                let link = item.link().map(str::to_owned);
                let published = item
                    .pub_date()
                    .and_then(|s| DateTime::parse_from_rfc2822(s).ok())
                    .map(|dt| dt.with_timezone(&Utc));
                RssItem {
                    id,
                    title,
                    body,
                    link,
                    published,
                }
            })
            .collect());
    }

    let feed = atom_syndication::Feed::read_from(BufReader::new(bytes))
        .map_err(|e| SourceError::Parse(format!("not valid RSS or Atom: {e}")))?;

    Ok(feed
        .entries()
        .iter()
        .map(|entry| {
            let id = entry.id().to_owned();
            let title = entry.title().value.clone();
            // Prefer `<content>` (the full entry) over the short `<summary>`.
            let body = entry
                .content()
                .and_then(|c| c.value())
                .or_else(|| entry.summary().map(|s| s.as_str()))
                .and_then(body_text);
            let link = entry.links().first().map(|l| l.href().to_owned());
            let published = entry
                .published()
                .or_else(|| Some(entry.updated()))
                .copied()
                .map(|dt| dt.with_timezone(&Utc));
            RssItem {
                id,
                title,
                body,
                link,
                published,
            }
        })
        .collect())
}

impl Connection for RssConnection {
    type Cursor = RssCursor;
    type Item = RssItem;

    async fn poll(
        &self,
        cursor: Self::Cursor,
    ) -> Result<Batch<Self::Item, Self::Cursor>, SourceError> {
        tracing::debug!(url = %self.feed_url, etag = ?cursor.etag, "fetching RSS feed");

        let mut req = self.client.get(&self.feed_url);
        if let Some(ref etag) = cursor.etag {
            req = req.header(reqwest::header::IF_NONE_MATCH, etag.as_str());
        }
        if let Some(ref lm) = cursor.last_modified {
            req = req.header(reqwest::header::IF_MODIFIED_SINCE, lm.as_str());
        }

        let resp = req
            .send()
            .await
            .map_err(|e| SourceError::Request(e.to_string()))?;

        if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
            tracing::debug!(url = %self.feed_url, "feed not modified (304), skipping");
            return Ok(Batch {
                items: vec![],
                cursor,
            });
        }

        if !resp.status().is_success() {
            return Err(SourceError::Request(format!("HTTP {}", resp.status())));
        }

        let new_etag = resp
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let new_lm = resp
            .headers()
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let bytes = resp
            .bytes()
            .await
            .map_err(|e| SourceError::Request(e.to_string()))?;
        let items = parse_feed(bytes.as_ref())?;

        tracing::debug!(url = %self.feed_url, count = items.len(), "parsed feed items");

        Ok(Batch {
            items,
            cursor: RssCursor {
                etag: new_etag,
                last_modified: new_lm,
            },
        })
    }

    fn to_events(&self, item: Self::Item) -> Vec<EventBuilder> {
        // Use published date when present; fall back to now for feeds that omit dates.
        let event_time = item.published.unwrap_or_else(Utc::now);
        let links: Vec<String> = item.link.into_iter().collect();

        // Depth gate: only a body with real substance (>= LONGFORM_MIN_CHARS) earns Longform — and with
        // it a Story-depth multi-sentence tldr. A thin or body-less item is an Announcement, so the
        // summarizer renders it as a headline-only Note rather than padding it into a vague Story.
        let content_kind = match &item.body {
            Some(body) if body.chars().count() >= LONGFORM_MIN_CHARS => ContentKind::Longform,
            _ => ContentKind::Announcement,
        };

        let mut builder = EventBuilder::new(
            SourceKind::Rss,
            item.id.clone(),
            event_time,
            item.title,
            item.id, // group_key = stable_id: each article is its own cluster
        )
        .content_kind(content_kind)
        .links(links);
        if let Some(body) = item.body {
            builder = builder.body(body);
        }
        vec![builder]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::Connection;

    /// Build a minimal `RssItem` with the given body and run it through `to_events`, returning the
    /// resulting event's `content_kind` (the only field these depth-gate tests inspect).
    fn content_kind_for(body: Option<&str>) -> ContentKind {
        let conn = RssConnection::new("http://example.invalid/feed");
        let item = RssItem {
            id: "id-1".to_string(),
            title: "A title".to_string(),
            body: body.map(str::to_owned),
            link: Some("http://example.invalid/article".to_string()),
            published: None,
        };
        let event = conn
            .to_events(item)
            .pop()
            .expect("one event")
            .finalize(None);
        event.content_kind
    }

    #[test]
    fn bodyless_item_is_announcement() {
        assert_eq!(content_kind_for(None), ContentKind::Announcement);
    }

    #[test]
    fn short_body_is_announcement() {
        // A body shorter than LONGFORM_MIN_CHARS is too thin to ground a Story tldr → Note depth.
        let short = "x".repeat(LONGFORM_MIN_CHARS - 1);
        assert_eq!(content_kind_for(Some(&short)), ContentKind::Announcement);
    }

    #[test]
    fn long_body_is_longform() {
        // A body at/above the threshold carries enough material for a Story-depth summary.
        let long = "x".repeat(LONGFORM_MIN_CHARS);
        assert_eq!(content_kind_for(Some(&long)), ContentKind::Longform);
    }
}
