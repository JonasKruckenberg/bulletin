use crate::common::{event::EventBuilder, kind::SourceKind};
use crate::ingest::{Batch, Connection, SourceError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
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

/// Max HTML handed to the renderer. `RENDER_WIDTH` only *wraps* lines — it does not bound how much
/// html2text parses and renders — so without this a full-article `<content>` would be parsed and
/// rendered in full only for ~all of it to be discarded by [`MAX_BODY_CHARS`]. A few KB of markup
/// comfortably yields the kept text, so render at most the leading slice (html2text tolerates a
/// truncated fragment) to keep ingest work proportional to what we store, not to feed size.
const MAX_BODY_HTML_CHARS: usize = 16_000;

/// Wrap width handed to html2text. Immaterial to the result — the rendered line breaks are collapsed
/// straight back out below — but it must clear the renderer's internal minimum; a roomy value avoids
/// needless mid-text wraps.
const RENDER_WIDTH: usize = 200;

/// Render a feed item's HTML body fragment to the plain text we store and summarize: strip the markup
/// with `html2text`, normalize whitespace, and cap the length. `None` for empty/blank input (or markup
/// that renders to nothing), so a content-less item keeps `body = None`.
///
/// Uses `plain_no_decorate` deliberately: it drops link/emphasis markup entirely rather than emitting
/// `[text][1]` footnotes, so the rendered prose never re-introduces the URLs the summarizer's
/// faithfulness gate would only have to strip back out (the link is carried structurally in `links`).
fn body_text(html: &str) -> Option<String> {
    if html.trim().is_empty() {
        return None;
    }
    // Bound the work before rendering: only allocate a truncated copy when the markup actually exceeds
    // the cap (byte length ≥ char count, so a shorter byte length needs no truncation).
    let bounded: Cow<str> = if html.len() > MAX_BODY_HTML_CHARS {
        Cow::Owned(html.chars().take(MAX_BODY_HTML_CHARS).collect())
    } else {
        Cow::Borrowed(html)
    };
    let rendered = html2text::config::plain_no_decorate()
        .string_from_read(bounded.as_bytes(), RENDER_WIDTH)
        .ok()?;
    // Collapse all runs of whitespace (incl. the wrapper's line breaks) to single spaces — the body is
    // grounding for the model and the entity/number miners, not displayed, so flat prose is cleanest.
    let normalized = rendered.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty() {
        return None;
    }
    Some(truncate_on_word_boundary(normalized, MAX_BODY_CHARS))
}

/// Cap `s` to `max_chars`, cutting on a whitespace boundary so the stored body never ends in a split
/// token — a half-truncated number would otherwise be mined as a (bogus) grounded quantity. Falls back
/// to a hard char cut only when the truncated prefix holds no space at all (one pathological long
/// token). `s` is already whitespace-normalized to single spaces, so the split is on `' '`.
fn truncate_on_word_boundary(s: String, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s;
    }
    let truncated: String = s.chars().take(max_chars).collect();
    match truncated.rsplit_once(' ') {
        Some((head, _)) if !head.is_empty() => head.to_string(),
        _ => truncated,
    }
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

        let mut builder = EventBuilder::new(
            SourceKind::Rss,
            item.id.clone(),
            event_time,
            item.title,
            item.id, // group_key = stable_id: each article is its own cluster
        )
        .links(links);
        if let Some(body) = item.body {
            builder = builder.body(body);
        }
        vec![builder]
    }
}
