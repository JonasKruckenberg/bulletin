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
      <description><![CDATA[<p>A <strong>bad config</strong> broke <a href="https://example.com/x">logins</a> for 12% of users.</p>]]></description>
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
    <content type="html"><![CDATA[<p>Release <b>v2.0</b> shipped.</p>]]></content>
    <updated>2025-06-09T10:00:00Z</updated>
  </entry>
</feed>"#;

fn item(id: &str, title: &str, link: Option<&str>) -> RssItem {
    RssItem {
        id: id.into(),
        title: title.into(),
        body: None,
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

    // The HTML `<description>` is rendered to plain text: tags stripped, the number kept, and the
    // link's URL dropped (we keep it in `links`, not pasted into prose for the summarizer to gate out).
    let body = items[0].body.as_deref().expect("post-1 has a description");
    assert!(body.contains("bad config broke"), "body was: {body:?}");
    assert!(body.contains("12% of users"), "body was: {body:?}");
    assert!(!body.contains('<'), "tags must be stripped: {body:?}");
    assert!(
        !body.contains("example.com"),
        "link URL must not leak into prose: {body:?}"
    );
    // No `<description>`/`<content>` on the second item ⇒ no body.
    assert_eq!(items[1].body, None);
}

#[test]
fn parse_rss_caps_body_on_word_boundary() {
    // A body well over the stored cap, of repeated whole words, so a correct truncation must land on a
    // word boundary (never a split token like a half-number the miner would treat as grounded).
    let long = "alpha ".repeat(500); // ~3000 chars of rendered text
    let xml = format!(
        r#"<?xml version="1.0"?>
<rss version="2.0"><channel><title>t</title><link>https://e.com</link><description>d</description>
<item><guid>g1</guid><title>T</title><link>https://e.com/1</link>
<description><![CDATA[<p>{long}</p>]]></description></item>
</channel></rss>"#
    );
    let items = parse_feed(xml.as_bytes()).unwrap();
    let body = items[0].body.as_deref().expect("item has a description body");

    assert!(
        body.chars().count() <= 2000,
        "body must be capped, was {} chars",
        body.chars().count()
    );
    // Cut on a whitespace boundary: the final token is a complete "alpha", with no trailing space.
    assert!(body.ends_with("alpha"), "body must end on a whole word: {body:?}");
    assert!(!body.ends_with(' '), "no dangling trailing space: {body:?}");
}

#[test]
fn parse_atom_returns_items() {
    let items = parse_feed(ATOM_XML.as_bytes()).unwrap();

    assert_eq!(items.len(), 1);
    assert_eq!(items[0].id, "urn:example:atom-1");
    assert_eq!(items[0].title, "Atom Entry");
    assert_eq!(items[0].link, Some("https://example.com/atom-1".into()));

    // Atom `<content type="html">` is rendered to plain text the same way as RSS `<description>`.
    let body = items[0].body.as_deref().expect("atom entry has content");
    assert!(body.contains("Release"), "body was: {body:?}");
    assert!(body.contains("v2.0"), "body was: {body:?}");
    assert!(!body.contains('<'), "tags must be stripped: {body:?}");
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
    // RSS supplies no structural entities; `finalize` derives `url:`/`domain:` from the link.
    assert_eq!(
        ev.entities,
        vec![
            "domain:example.com".to_string(),
            "url:https://example.com/post-1".to_string(),
        ]
    );
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
