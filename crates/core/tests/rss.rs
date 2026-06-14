use bulletin_core::ingest::rss::{parse_feed, RssConnection, RssItem};
use bulletin_core::ingest::Connection;
use bulletin_core::{kind::SourceKind, scope::Scope};
use chrono::{TimeZone, Utc};

const RSS_XML: &str = r#"<?xml version="1.0"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <link>https://example.com</link>
    <description>A test feed</description>
    <item>
      <guid>https://example.com/post-1</guid>
      <title>Hello World</title>
      <link>https://example.com/post-1</link>
      <pubDate>Mon, 09 Jun 2025 10:00:00 +0000</pubDate>
    </item>
    <item>
      <guid>https://example.com/post-2</guid>
      <title>Second Post</title>
      <link>https://example.com/post-2</link>
      <pubDate>Mon, 09 Jun 2025 11:00:00 +0000</pubDate>
    </item>
  </channel>
</rss>"#;

const ATOM_XML: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Test Atom Feed</title>
  <id>https://example.com/</id>
  <entry>
    <id>urn:example:atom-1</id>
    <title>Atom Entry</title>
    <link href="https://example.com/atom-1"/>
    <updated>2025-06-09T10:00:00Z</updated>
  </entry>
</feed>"#;

fn item(id: &str, title: &str, link: Option<&str>) -> RssItem {
    RssItem {
        id: id.into(),
        title: title.into(),
        link: link.map(String::from),
        published: Some(Utc.with_ymd_and_hms(2025, 6, 9, 10, 0, 0).unwrap()),
    }
}

#[test]
fn parse_rss_returns_items() {
    let items = parse_feed(RSS_XML.as_bytes()).unwrap();

    assert_eq!(items.len(), 2);
    assert_eq!(items[0].id, "https://example.com/post-1");
    assert_eq!(items[0].title, "Hello World");
    assert_eq!(items[0].link, Some("https://example.com/post-1".into()));
    assert_eq!(items[1].id, "https://example.com/post-2");
    assert_eq!(items[1].title, "Second Post");
}

#[test]
fn parse_atom_returns_items() {
    let items = parse_feed(ATOM_XML.as_bytes()).unwrap();

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, "urn:example:atom-1");
    assert_eq!(items[0].title, "Atom Entry");
    assert_eq!(items[0].link, Some("https://example.com/atom-1".into()));
}

#[test]
fn parse_atom_falls_back_to_updated_for_published() {
    let items = parse_feed(ATOM_XML.as_bytes()).unwrap();
    let published = items[0].published.expect("should have a timestamp");
    assert_eq!(
        published.timestamp(),
        Utc.with_ymd_and_hms(2025, 6, 9, 10, 0, 0)
            .unwrap()
            .timestamp()
    );
}

#[test]
fn to_events_maps_fields_correctly() {
    let conn = RssConnection::new("unused");
    let builders = conn.to_events(item(
        "https://example.com/post-1",
        "Hello World",
        Some("https://example.com/post-1"),
    ));

    assert_eq!(builders.len(), 1);
    let ev = builders.into_iter().next().unwrap().finalize(None);

    assert_eq!(ev.source, SourceKind::Rss);
    assert_eq!(ev.title, "Hello World");
    assert_eq!(ev.body, None);
    assert_eq!(ev.links, vec!["https://example.com/post-1"]);
    assert_eq!(ev.group_key, "https://example.com/post-1");
    assert_eq!(ev.entities, Vec::<String>::new());
    assert_eq!(ev.scope, Scope::Public);
}

#[test]
fn to_events_omits_link_when_absent() {
    let conn = RssConnection::new("unused");
    let ev = conn
        .to_events(item("id-no-link", "No Link", None))
        .into_iter()
        .next()
        .unwrap()
        .finalize(None);
    assert!(ev.links.is_empty());
}

#[test]
fn same_item_produces_same_fingerprint() {
    let conn = RssConnection::new("unused");

    let fp1 = conn
        .to_events(item(
            "https://example.com/post-1",
            "Hello World",
            Some("https://example.com/post-1"),
        ))
        .into_iter()
        .next()
        .unwrap()
        .finalize(None)
        .fingerprint;
    let fp2 = conn
        .to_events(item(
            "https://example.com/post-1",
            "Hello World",
            Some("https://example.com/post-1"),
        ))
        .into_iter()
        .next()
        .unwrap()
        .finalize(None)
        .fingerprint;

    assert_eq!(
        fp1, fp2,
        "fingerprint must be stable across identical items"
    );
}
