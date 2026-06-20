//! Phase 1: best-effort full-article text fetch.
//!
//! Link-based sources (RSS) give the summarizer only a short `<description>` snippet; the real
//! article behind the link grounds far better. This module resolves an event's link, fetches the
//! page, extracts the readable text ([`super::html_text`]), and stores it as `event.full_text` — a
//! distinct column, so the original snippet in `body` is never lost and a fetch that fails or never
//! runs leaves the event fully summarizable from what it already carried (`Event::best_text` prefers
//! `full_text`, else `body`).
//!
//! **Off the hot path, by contract.** Fetching is a due-gated background sweep ([`sweep_article_fetch`]),
//! run from its own apalis job exactly like the summarization sweep — never inline in the
//! poll/cluster/digest/send path. "Fall behind, never wrong": a slow or unreachable site delays only
//! the *enrichment*, never a punctual digest, and a per-event failure is recorded (with a bounded
//! retry budget) and simply skipped.
//!
//! **Security — this is outbound fetch of attacker-influenced URLs (feed-supplied links).** Every
//! request is SSRF-guarded ([`fetch_article`]): the scheme must be http(s); the host is DNS-resolved
//! and rejected if it lands on any loopback/private/link-local/ULA/CGNAT/multicast/reserved range
//! (v4 + v6, including IPv4-mapped v6); the validated IPs are pinned into the request's resolver to
//! blunt DNS-rebinding; and **every redirect hop is re-validated**, not just the first URL. A
//! response-size cap, a request timeout, and a `text/html` content-type allowlist bound what one
//! fetch can pull. Per-domain politeness (a per-sweep cap + a min-interval) keeps us a good citizen.
//!
//! Deferred: `robots.txt` is **not** consulted yet. The per-domain rate limit and the conservative
//! defaults are the interim politeness; honoring `robots.txt` (and `Crawl-delay`) is a follow-up.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};

use sqlx::{postgres::PgRow, PgConnection, Row};
use url::Url;
use uuid::Uuid;

use crate::common::kind::SourceKind;
use crate::common::scope::Scope;

/// Consecutive fetch attempts after which an event drops out of the work queue (a permanently
/// unfetchable link — 404, blocked host, persistent timeout — must not burn the sweep every pass).
/// A content change does not reset this; the link is fixed for the life of the event.
pub const MAX_FETCH_ATTEMPTS: i16 = 3;

/// The `bulletin_fetch_*` metrics for the article-fetch path — thin recorders so the metric-name
/// strings live in one place, mirroring [`summarize::metric`](crate::summarize). The Prometheus
/// recorder is installed by the `bulletin` binary; until then (unit tests, non-`worker` roles) these
/// are no-ops, so recording is always safe to call unconditionally.
mod metric {
    use std::time::Duration;

    /// One completed fetch attempt: wall-time into the latency histogram keyed by `outcome` — `ok` or
    /// one of the bounded [`FetchError::describe`](super::FetchError) buckets (`blocked_address`,
    /// `bad_status`, `too_large`, `transport`, …), plus `no_link` for an event with no usable URL. The
    /// histogram's `_count{outcome}` doubles as the per-outcome attempt total (so no separate counter is
    /// needed), and `outcome="blocked_address"` is the directest read on SSRF rejections in the wild.
    pub fn attempt(outcome: &'static str, elapsed: Duration) {
        metrics::histogram!("bulletin_fetch_duration_seconds", "outcome" => outcome)
            .record(elapsed.as_secs_f64());
    }

    /// An event deferred to a later sweep for per-domain politeness (the per-sweep cap) — not an error,
    /// counted apart from the attempt outcomes so a busy origin's deferral rate is visible.
    pub fn skipped() {
        metrics::counter!("bulletin_fetch_skipped_total").increment(1);
    }
}

// ── Config ───────────────────────────────────────────────────────────────────────────────────────

/// Tuning surface for the article fetch, mirroring [`SummarizationConfig`](crate::summarize::SummarizationConfig):
/// pure config (timeouts, caps, politeness), never a guard. Conservative defaults; the operational
/// knobs are overridable via the `BULLETIN_FETCH_*` environment without a recompile.
#[derive(Debug, Clone)]
pub struct FetchConfig {
    /// Per-request HTTP timeout (applies to each redirect hop). Generous — off the punctual path; a
    /// timeout just leaves the event at its snippet for this pass.
    pub request_timeout: Duration,
    /// Hard cap on the response body downloaded (bytes), enforced against both the `Content-Length`
    /// header and the actual streamed length (the header can lie).
    pub max_bytes: usize,
    /// Max redirect hops followed. Each hop is independently SSRF-validated before it is connected to.
    pub max_redirects: usize,
    /// Max events fetched per sweep — bounds one best-effort pass so a backlog drains over several.
    pub max_per_sweep: i64,
    /// Max fetches to any single domain in one sweep (politeness — don't hammer one origin).
    pub max_per_domain_per_sweep: usize,
    /// Minimum wall-clock gap between two fetches to the *same* domain in a sweep (politeness).
    pub per_domain_min_interval: Duration,
    /// `User-Agent` sent on every request — identifies the fetcher to the origin.
    pub user_agent: String,
    /// Max stored article text (chars). Richer than the RSS snippet cap, but still bounded so DB rows
    /// and the model's input stay small (`source_corpus` re-budgets across the cluster on top).
    pub full_text_max_chars: usize,
    /// Max HTML actually parsed/rendered (chars) — keeps render work proportional to what we store,
    /// not to page size (see [`super::html_text::render`]).
    pub max_html_chars: usize,
}

impl Default for FetchConfig {
    fn default() -> Self {
        FetchConfig {
            request_timeout: Duration::from_secs(10),
            max_bytes: 2 * 1024 * 1024, // 2 MiB
            max_redirects: 5,
            max_per_sweep: 100,
            max_per_domain_per_sweep: 5,
            per_domain_min_interval: Duration::from_secs(1),
            user_agent: "bulletin/0.1 (+article-fetch)".to_string(),
            full_text_max_chars: 8000,
            max_html_chars: 200_000,
        }
    }
}

impl FetchConfig {
    /// Build a config from the `BULLETIN_FETCH_*` environment — the operational knobs an operator may
    /// need to tune the fetch edge (slow sites, stricter caps) without a recompile. Everything else
    /// stays at the conservative defaults.
    pub fn from_env() -> Self {
        let mut cfg = FetchConfig::default();
        if let Ok(v) = std::env::var("BULLETIN_FETCH_TIMEOUT_SECS") {
            if let Ok(secs) = v.trim().parse::<u64>() {
                if secs > 0 {
                    cfg.request_timeout = Duration::from_secs(secs);
                }
            }
        }
        if let Ok(v) = std::env::var("BULLETIN_FETCH_MAX_BYTES") {
            if let Ok(n) = v.trim().parse::<usize>() {
                if n > 0 {
                    cfg.max_bytes = n;
                }
            }
        }
        if let Ok(v) = std::env::var("BULLETIN_FETCH_MAX_PER_SWEEP") {
            if let Ok(n) = v.trim().parse::<i64>() {
                if n > 0 {
                    cfg.max_per_sweep = n;
                }
            }
        }
        if let Ok(v) = std::env::var("BULLETIN_FETCH_USER_AGENT") {
            if !v.trim().is_empty() {
                cfg.user_agent = v.trim().to_string();
            }
        }
        cfg
    }
}

// ── SSRF guard (pure, unit-tested) ─────────────────────────────────────────────────────────────────

/// Why a fetch was rejected or failed — the coarse outcome surfaced in logs and (as a string) on the
/// event's failure record. Kept deliberately small; the `Blocked*` variants are the SSRF rejections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FetchError {
    /// The URL (or a redirect target) was not an absolute `http(s)` URL with a host.
    BadScheme,
    /// The host resolved to no address, or DNS failed.
    DnsFailure,
    /// The host resolved to a blocked range (loopback/private/link-local/ULA/CGNAT/multicast/reserved)
    /// — the SSRF rejection, applied to every redirect hop.
    BlockedAddress,
    /// More than [`FetchConfig::max_redirects`] hops.
    TooManyRedirects,
    /// A non-success final status.
    BadStatus(u16),
    /// The response `Content-Type` was not in the allowlist (`text/html`).
    DisallowedContentType(String),
    /// The body exceeded [`FetchConfig::max_bytes`] (by header or by streamed length).
    TooLarge,
    /// The page rendered to no usable text.
    Empty,
    /// A transport/protocol error (connect, TLS, read).
    Transport(String),
}

impl FetchError {
    /// A coarse, stable string for the event's failure record / metrics (no per-host detail).
    pub fn describe(&self) -> &'static str {
        match self {
            FetchError::BadScheme => "bad_scheme",
            FetchError::DnsFailure => "dns_failure",
            FetchError::BlockedAddress => "blocked_address",
            FetchError::TooManyRedirects => "too_many_redirects",
            FetchError::BadStatus(_) => "bad_status",
            FetchError::DisallowedContentType(_) => "disallowed_content_type",
            FetchError::TooLarge => "too_large",
            FetchError::Empty => "empty",
            FetchError::Transport(_) => "transport",
        }
    }
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::BadStatus(c) => write!(f, "bad_status({c})"),
            FetchError::DisallowedContentType(ct) => write!(f, "disallowed_content_type({ct})"),
            FetchError::Transport(e) => write!(f, "transport({e})"),
            other => f.write_str(other.describe()),
        }
    }
}

/// Is this IP one a public-internet fetch must never connect to? Blocks the SSRF surface: loopback,
/// private, link-local, CGNAT/shared, multicast/broadcast, documentation, and reserved ranges, for
/// both v4 and v6 (IPv4-mapped/compatible v6 is unwrapped and re-checked as v4). The single decision
/// every resolved address — first URL and every redirect hop — is run through.
fn is_disallowed_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_disallowed_v4(v4),
        IpAddr::V6(v6) => {
            // An IPv4 embedded in a v6 address (::ffff:a.b.c.d mapped, or ::a.b.c.d compatible) reaches
            // the same v4 host — unwrap and apply the v4 rules so e.g. ::ffff:127.0.0.1 is blocked.
            if let Some(v4) = v6.to_ipv4() {
                if is_disallowed_v4(v4) {
                    return true;
                }
            }
            is_disallowed_v6(v6)
        }
    }
}

fn is_disallowed_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    ip.is_unspecified()                          // 0.0.0.0
        || o[0] == 0                             // 0.0.0.0/8 "this network"
        || ip.is_loopback()                      // 127.0.0.0/8
        || ip.is_private()                       // 10/8, 172.16/12, 192.168/16
        || ip.is_link_local()                    // 169.254.0.0/16
        || (o[0] == 100 && (o[1] & 0xC0) == 64)  // 100.64.0.0/10 CGNAT (shared)
        || ip.is_broadcast()                     // 255.255.255.255
        || ip.is_documentation()                 // 192.0.2/24, 198.51.100/24, 203.0.113/24
        || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0.0/24 IETF protocol assignments
        || (o[0] == 198 && (o[1] & 0xFE) == 18)  // 198.18.0.0/15 benchmarking
        || ip.is_multicast()                     // 224.0.0.0/4
        || o[0] >= 240 // 240.0.0.0/4 reserved
}

fn is_disallowed_v6(ip: Ipv6Addr) -> bool {
    let seg = ip.segments();
    ip.is_unspecified()                       // ::
        || ip.is_loopback()                   // ::1
        || (seg[0] & 0xfe00) == 0xfc00        // fc00::/7 unique-local (ULA)
        || (seg[0] & 0xffc0) == 0xfe80        // fe80::/10 link-local
        || ip.is_multicast()                  // ff00::/8
        || (seg[0] == 0x2001 && seg[1] == 0x0db8) // 2001:db8::/32 documentation
}

/// The Content-Type allowlist for an article fetch: HTML only. The header's parameters (`; charset=…`)
/// are dropped and the type compared case-insensitively. A page served as anything else (PDF, image,
/// JSON, octet-stream) is rejected rather than fed to the HTML renderer.
fn is_allowed_content_type(ct: &str) -> bool {
    let essence = ct
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    essence == "text/html" || essence == "application/xhtml+xml"
}

/// Resolve a parsed URL to the set of IP addresses it would connect to, rejecting non-http(s) and
/// applying the SSRF range checks. For an IP-literal host the address is validated directly (no DNS);
/// for a domain it is looked up. Returns `(ips, port, pin_domain)` — `pin_domain` is `Some(domain)`
/// only for a name host, so the caller can pin the validated IPs into the request resolver.
async fn resolve_and_validate(url: &Url) -> Result<(Vec<IpAddr>, u16, Option<String>), FetchError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(FetchError::BadScheme);
    }
    let port = url.port_or_known_default().ok_or(FetchError::BadScheme)?;
    let host = url.host().ok_or(FetchError::BadScheme)?;

    let (ips, pin): (Vec<IpAddr>, Option<String>) = match host {
        url::Host::Ipv4(ip) => (vec![IpAddr::V4(ip)], None),
        url::Host::Ipv6(ip) => (vec![IpAddr::V6(ip)], None),
        url::Host::Domain(domain) => {
            let domain = domain.to_string();
            let resolved: Vec<IpAddr> = tokio::net::lookup_host((domain.as_str(), port))
                .await
                .map_err(|_| FetchError::DnsFailure)?
                .map(|sa| sa.ip())
                .collect();
            (resolved, Some(domain))
        }
    };

    if ips.is_empty() {
        return Err(FetchError::DnsFailure);
    }
    // Conservative: block if *any* resolved address is disallowed, so a host that resolves to a public
    // and a private address (a partial-rebinding attempt) is rejected outright.
    if ips.iter().copied().any(is_disallowed_ip) {
        return Err(FetchError::BlockedAddress);
    }
    Ok((ips, port, pin))
}

// ── The fetch (async, network) ─────────────────────────────────────────────────────────────────────

/// Fetch and extract the readable article text at `raw_url`, SSRF-guarding every hop. Returns the
/// extracted plain text on success, or a [`FetchError`] describing the rejection/failure. Pure of the
/// DB; the sweep persists the result.
///
/// Redirects are followed manually (reqwest's own redirect handling is disabled) so each hop's target
/// is re-resolved and re-validated *before* it is connected — the first URL passing the guard is not
/// enough, since a `Location` can point at `127.0.0.1`. The validated IPs are pinned into a per-hop
/// client's resolver so the address reqwest connects to is the one we checked (DNS-rebinding guard).
pub async fn fetch_article(cfg: &FetchConfig, raw_url: &str) -> Result<String, FetchError> {
    let mut url = Url::parse(raw_url).map_err(|_| FetchError::BadScheme)?;

    for _hop in 0..=cfg.max_redirects {
        let (ips, port, pin) = resolve_and_validate(&url).await?;

        let mut builder = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(cfg.user_agent.clone());
        // Pin the validated addresses for a name host so reqwest connects to exactly what we checked,
        // not a freshly (and possibly rebound) re-resolution. IP-literal hosts need no pin.
        if let Some(domain) = &pin {
            let addrs: Vec<SocketAddr> = ips.iter().map(|ip| SocketAddr::new(*ip, port)).collect();
            builder = builder.resolve_to_addrs(domain, &addrs);
        }
        let client = builder
            .build()
            .map_err(|e| FetchError::Transport(e.to_string()))?;

        let resp = client
            .get(url.as_str())
            .send()
            .await
            .map_err(|e| FetchError::Transport(e.to_string()))?;

        let status = resp.status();
        if status.is_redirection() {
            let location = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or(FetchError::Transport("redirect without location".into()))?;
            // Resolve a possibly-relative Location against the current URL; the next loop iteration
            // re-validates the resolved target before connecting.
            url = url
                .join(location)
                .map_err(|_| FetchError::Transport("bad redirect location".into()))?;
            continue;
        }

        if !status.is_success() {
            return Err(FetchError::BadStatus(status.as_u16()));
        }

        // Content-type allowlist (HTML only) before reading the body.
        let ct = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        if !is_allowed_content_type(&ct) {
            return Err(FetchError::DisallowedContentType(ct));
        }

        // Size cap: reject early on a too-large declared length, then enforce against the real stream.
        if let Some(len) = resp.content_length() {
            if len > cfg.max_bytes as u64 {
                return Err(FetchError::TooLarge);
            }
        }
        let html = read_capped(resp, cfg.max_bytes).await?;

        return super::html_text::render(&html, cfg.max_html_chars, cfg.full_text_max_chars)
            .ok_or(FetchError::Empty);
    }

    Err(FetchError::TooManyRedirects)
}

/// Stream a response body into a string, aborting with [`FetchError::TooLarge`] the moment the running
/// byte count would exceed `max_bytes` — so a lying/absent `Content-Length` can't make us buffer an
/// unbounded body. Decoded lossily (article HTML is text; a stray invalid byte shouldn't fail it).
async fn read_capped(mut resp: reqwest::Response, max_bytes: usize) -> Result<String, FetchError> {
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| FetchError::Transport(e.to_string()))?
    {
        if buf.len() + chunk.len() > max_bytes {
            return Err(FetchError::TooLarge);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

// ── Store contract (the fetch work queue + write-back) ──────────────────────────────────────────────

/// True iff any public, fetchable-source event still wants a full-text fetch — the tick's gate for
/// enqueuing a [`FetchArticlesJob`](crate::ingest::fetch). Mirrors `unbuilt_public_events_exist`: a
/// cheap `EXISTS` over the partial work-queue index, so no fetch job is enqueued when there's nothing
/// to do. Runs on the pool directly (the tick's context), reading only public rows.
pub async fn events_needing_fetch_exist(
    executor: impl sqlx::PgExecutor<'_>,
) -> Result<bool, sqlx::Error> {
    let row = sqlx::query(
        "SELECT EXISTS (
            SELECT 1 FROM event
            WHERE scope_kind = 'public'
              AND full_text IS NULL
              AND full_text_attempts < $1
              AND source = 'rss'
              AND array_length(links, 1) >= 1
         ) AS pending",
    )
    .bind(MAX_FETCH_ATTEMPTS)
    .fetch_one(executor)
    .await?;
    Ok(row.get("pending"))
}

/// One event the fetch sweep should try: its identity (to re-find its cluster for the re-summarize
/// nudge) and its links (the fetch candidate).
struct FetchTarget {
    id: Uuid,
    source: SourceKind,
    group_key: String,
    links: Vec<String>,
}

/// The fetch work queue in `scope`: fetchable-source events with a link, no `full_text` yet, and retry
/// budget left. Newest-first, bounded by `limit` so one pass drains a slice. (Only RSS is fetchable
/// today — see [`SourceKind::has_fetchable_article`]; the `source = 'rss'` filter is the SQL mirror.)
async fn due_events_for_fetch(
    conn: &mut PgConnection,
    scope: &Scope,
    limit: i64,
) -> Result<Vec<FetchTarget>, sqlx::Error> {
    let (scope_kind, scope_subscriber_id) = scope.to_columns();
    sqlx::query(
        "SELECT id, source, group_key, links
         FROM event
         WHERE scope_kind = $1 AND scope_subscriber_id IS NOT DISTINCT FROM $2
           AND full_text IS NULL
           AND full_text_attempts < $3
           AND source = 'rss'
           AND array_length(links, 1) >= 1
         ORDER BY ingest_time DESC
         LIMIT $4",
    )
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(MAX_FETCH_ATTEMPTS)
    .bind(limit)
    .try_map(|row: PgRow| {
        Ok(FetchTarget {
            id: row.get("id"),
            source: row.try_get("source")?,
            group_key: row.get("group_key"),
            links: row.get("links"),
        })
    })
    .fetch_all(conn)
    .await
}

/// Persist a successful fetch: set `full_text` (+ provenance), bump the attempt counter, and nudge the
/// event's cluster (if already built) back into the summarization work queue by bumping its
/// `updated_at` — so a fetch that lands *after* the cluster was first summarized off the snippet
/// re-summarizes off the richer text (the `summary_hash` staleness gate then confirms the content
/// actually moved). A not-yet-built cluster needs no nudge: it'll summarize with `full_text` already
/// present. Runs in the cluster's scope context (public sweep → no-subscriber).
async fn store_full_text(
    conn: &mut PgConnection,
    id: Uuid,
    source: SourceKind,
    group_key: &str,
    scope: &Scope,
    text: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE event
         SET full_text = $2, full_text_fetched_at = now(), full_text_attempts = full_text_attempts + 1
         WHERE id = $1",
    )
    .bind(id)
    .bind(text)
    .execute(&mut *conn)
    .await?;

    let (scope_kind, scope_subscriber_id) = scope.to_columns();
    sqlx::query(
        "UPDATE cluster SET updated_at = now()
         WHERE scope_kind = $1 AND scope_subscriber_id IS NOT DISTINCT FROM $2
           AND source = $3 AND group_key = $4",
    )
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(source)
    .bind(group_key)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Record a failed fetch attempt: bump the counter (leaving `full_text` NULL) so the event is retried
/// next sweep until the budget ([`MAX_FETCH_ATTEMPTS`]) is spent, after which it drops out of the work
/// queue and stays at its snippet — a fetch failure is never fatal to summarization.
async fn record_fetch_failure(conn: &mut PgConnection, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE event SET full_text_attempts = full_text_attempts + 1 WHERE id = $1")
        .bind(id)
        .execute(conn)
        .await?;
    Ok(())
}

// ── The sweep ───────────────────────────────────────────────────────────────────────────────────────

/// What one fetch sweep did, for the trigger layer's logging (mirrors `SummarizeStats`).
#[derive(Debug, Default, Clone, Copy)]
pub struct FetchStats {
    /// Events whose `full_text` was stored this pass.
    pub fetched: usize,
    /// Events whose fetch failed this pass (rejected, timed out, etc.) — retried next sweep until the
    /// budget is spent.
    pub failed: usize,
    /// Events deferred to a later sweep for politeness (per-domain cap reached) — not an error.
    pub skipped: usize,
}

/// Run a best-effort full-text fetch sweep over **public** fetchable-source events, in the
/// no-subscriber RLS context (RSS is public-only, so there is no private counterpart). For each due
/// event it picks the first valid http(s) link, applies per-domain politeness, fetches with the SSRF
/// guard, and stores the text (nudging the cluster to re-summarize) or records the failure. The sweep
/// never errors as a whole — a per-event failure is tracked and skipped, exactly like the
/// summarization sweep's per-cluster failures (§3.7 spirit).
pub async fn sweep_article_fetch(
    pool: &sqlx::PgPool,
    cfg: &FetchConfig,
) -> anyhow::Result<FetchStats> {
    use crate::common::db::{with_scope, ScopeCtx};
    use anyhow::Context;

    let scope = Scope::Public;
    let ctx = ScopeCtx::for_scope(&scope);

    let due = with_scope(pool, ctx, {
        let scope = scope.clone();
        let limit = cfg.max_per_sweep;
        move |conn| {
            Box::pin(async move {
                due_events_for_fetch(conn, &scope, limit)
                    .await
                    .context("load events needing fetch")
            })
        }
    })
    .await?;

    tracing::debug!(due = due.len(), "article fetch sweep starting");

    let mut stats = FetchStats::default();
    let mut domain_counts: HashMap<String, usize> = HashMap::new();
    let mut domain_last: HashMap<String, Instant> = HashMap::new();

    for target in due {
        // The work-queue SQL filters to `source = 'rss'`; this ties that string filter back to the
        // typed contract ([`SourceKind::has_fetchable_article`]) so the two can't silently diverge.
        debug_assert!(
            target.source.has_fetchable_article(),
            "fetch work queue returned a non-fetchable source"
        );
        // The first parseable http(s) link is the article candidate.
        let Some((link, domain)) = first_fetchable_link(&target.links) else {
            // No usable link — burn an attempt so a malformed-link event eventually drops out.
            metric::attempt("no_link", Duration::ZERO);
            let id = target.id;
            let _ = with_scope(pool, ctx, move |conn| {
                Box::pin(async move {
                    record_fetch_failure(conn, id)
                        .await
                        .context("record fetch failure (no link)")
                })
            })
            .await;
            stats.failed += 1;
            continue;
        };

        // Per-domain politeness: a per-sweep cap, then a min-interval since this domain's last fetch.
        let count = domain_counts.entry(domain.clone()).or_insert(0);
        if *count >= cfg.max_per_domain_per_sweep {
            metric::skipped();
            stats.skipped += 1;
            continue;
        }
        *count += 1;
        if let Some(last) = domain_last.get(&domain) {
            let elapsed = last.elapsed();
            if elapsed < cfg.per_domain_min_interval {
                tokio::time::sleep(cfg.per_domain_min_interval - elapsed).await;
            }
        }
        domain_last.insert(domain.clone(), Instant::now());

        let started = Instant::now();
        let outcome = fetch_article(cfg, &link).await;
        let elapsed = started.elapsed();
        match outcome {
            Ok(text) => {
                metric::attempt("ok", elapsed);
                tracing::debug!(
                    event_id = %target.id,
                    %domain,
                    chars = text.chars().count(),
                    elapsed_ms = elapsed.as_millis() as u64,
                    "article fetched"
                );
                let scope = scope.clone();
                let id = target.id;
                let source = target.source;
                let group_key = target.group_key.clone();
                with_scope(pool, ctx, move |conn| {
                    Box::pin(async move {
                        store_full_text(conn, id, source, &group_key, &scope, &text)
                            .await
                            .context("store full text")
                    })
                })
                .await?;
                stats.fetched += 1;
            }
            Err(e) => {
                metric::attempt(e.describe(), elapsed);
                tracing::debug!(
                    event_id = %target.id,
                    %domain,
                    error = %e,
                    elapsed_ms = elapsed.as_millis() as u64,
                    "article fetch failed (event stays at snippet)"
                );
                let id = target.id;
                with_scope(pool, ctx, move |conn| {
                    Box::pin(async move {
                        record_fetch_failure(conn, id)
                            .await
                            .context("record fetch failure")
                    })
                })
                .await?;
                stats.failed += 1;
            }
        }
    }

    Ok(stats)
}

/// Pick the first link that parses as an absolute http(s) URL with a host, returning
/// `(url, domain)`. The domain (host, lowercased, leading `www.` dropped) keys the per-domain
/// politeness map. `None` when no link is fetchable.
fn first_fetchable_link(links: &[String]) -> Option<(String, String)> {
    for link in links {
        let Ok(url) = Url::parse(link) else { continue };
        if !matches!(url.scheme(), "http" | "https") {
            continue;
        }
        let Some(host) = url.host_str() else { continue };
        let lower = host.to_ascii_lowercase();
        let domain = lower.strip_prefix("www.").unwrap_or(&lower).to_string();
        return Some((link.clone(), domain));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_loopback_and_unspecified() {
        assert!(is_disallowed_ip("127.0.0.1".parse().unwrap()));
        assert!(is_disallowed_ip("127.255.255.254".parse().unwrap()));
        assert!(is_disallowed_ip("0.0.0.0".parse().unwrap()));
        assert!(is_disallowed_ip("::1".parse().unwrap()));
        assert!(is_disallowed_ip("::".parse().unwrap()));
    }

    #[test]
    fn blocks_private_v4_ranges() {
        for ip in [
            "10.0.0.1",
            "10.255.255.255",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.0.1",
            "192.168.255.255",
        ] {
            assert!(is_disallowed_ip(ip.parse().unwrap()), "should block {ip}");
        }
        // Just outside the 172.16/12 block is public.
        assert!(!is_disallowed_ip("172.15.255.255".parse().unwrap()));
        assert!(!is_disallowed_ip("172.32.0.1".parse().unwrap()));
    }

    #[test]
    fn blocks_link_local_cgnat_and_reserved() {
        assert!(is_disallowed_ip("169.254.0.1".parse().unwrap())); // link-local
        assert!(is_disallowed_ip("100.64.0.1".parse().unwrap())); // CGNAT
        assert!(is_disallowed_ip("100.127.255.255".parse().unwrap())); // CGNAT edge
        assert!(is_disallowed_ip("255.255.255.255".parse().unwrap())); // broadcast
        assert!(is_disallowed_ip("240.0.0.1".parse().unwrap())); // reserved
        assert!(is_disallowed_ip("224.0.0.1".parse().unwrap())); // multicast
                                                                 // 100.128/x is outside CGNAT → public.
        assert!(!is_disallowed_ip("100.128.0.1".parse().unwrap()));
    }

    #[test]
    fn blocks_v6_ula_link_local_and_mapped() {
        assert!(is_disallowed_ip("fc00::1".parse().unwrap())); // ULA
        assert!(is_disallowed_ip("fd12:3456::1".parse().unwrap())); // ULA
        assert!(is_disallowed_ip("fe80::1".parse().unwrap())); // link-local
        assert!(is_disallowed_ip("ff02::1".parse().unwrap())); // multicast
        assert!(is_disallowed_ip("2001:db8::1".parse().unwrap())); // documentation
                                                                   // IPv4-mapped loopback/private must be unwrapped and blocked.
        assert!(is_disallowed_ip("::ffff:127.0.0.1".parse().unwrap()));
        assert!(is_disallowed_ip("::ffff:10.0.0.1".parse().unwrap()));
    }

    #[test]
    fn allows_public_addresses() {
        for ip in ["8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:2800:220:1::1"] {
            assert!(!is_disallowed_ip(ip.parse().unwrap()), "should allow {ip}");
        }
    }

    #[test]
    fn content_type_allowlist_is_html_only() {
        assert!(is_allowed_content_type("text/html"));
        assert!(is_allowed_content_type("text/html; charset=utf-8"));
        assert!(is_allowed_content_type("TEXT/HTML"));
        assert!(is_allowed_content_type("application/xhtml+xml"));
        for bad in [
            "application/pdf",
            "image/png",
            "application/json",
            "application/octet-stream",
            "text/plain",
            "",
        ] {
            assert!(!is_allowed_content_type(bad), "should reject {bad}");
        }
    }

    #[tokio::test]
    async fn rejects_non_http_scheme() {
        for bad in ["ftp://example.com/x", "file:///etc/passwd", "gopher://x"] {
            assert_eq!(
                fetch_article(&FetchConfig::default(), bad).await,
                Err(FetchError::BadScheme),
                "should reject {bad}"
            );
        }
    }

    #[tokio::test]
    async fn rejects_url_resolving_to_loopback() {
        // An explicit loopback/private IP host is validated without DNS and blocked.
        let cfg = FetchConfig::default();
        assert_eq!(
            fetch_article(&cfg, "http://127.0.0.1/secret").await,
            Err(FetchError::BlockedAddress)
        );
        assert_eq!(
            fetch_article(&cfg, "http://169.254.169.254/latest/meta-data").await,
            Err(FetchError::BlockedAddress)
        );
        assert_eq!(
            fetch_article(&cfg, "http://[::1]:80/x").await,
            Err(FetchError::BlockedAddress)
        );
        assert_eq!(
            fetch_article(&cfg, "http://10.0.0.5/internal").await,
            Err(FetchError::BlockedAddress)
        );
    }

    #[test]
    fn resolve_and_validate_blocks_private_literal_and_allows_public() {
        // Drive the resolver synchronously (IP-literal hosts take no DNS) to assert the validation arm
        // that `fetch_article` relies on for every redirect hop.
        let rt = tokio::runtime::Runtime::new().unwrap();
        let blocked = rt.block_on(resolve_and_validate(
            &Url::parse("http://192.168.1.1/admin").unwrap(),
        ));
        assert_eq!(blocked, Err(FetchError::BlockedAddress));

        let ok = rt
            .block_on(resolve_and_validate(
                &Url::parse("http://8.8.8.8/").unwrap(),
            ))
            .expect("public literal validates");
        assert_eq!(ok.0, vec!["8.8.8.8".parse::<IpAddr>().unwrap()]);
        assert_eq!(ok.1, 80);
        assert_eq!(ok.2, None);
    }

    #[test]
    fn first_fetchable_link_picks_http_and_derives_domain() {
        assert_eq!(
            first_fetchable_link(&["https://www.Example.com/a".to_string()]),
            Some((
                "https://www.Example.com/a".to_string(),
                "example.com".to_string()
            ))
        );
        // Skips a non-http scheme, takes the next http link.
        assert_eq!(
            first_fetchable_link(&[
                "mailto:x@y.com".to_string(),
                "http://news.site/p".to_string(),
            ]),
            Some(("http://news.site/p".to_string(), "news.site".to_string()))
        );
        assert_eq!(first_fetchable_link(&["not a url".to_string()]), None);
        assert_eq!(first_fetchable_link(&[]), None);
    }
}
