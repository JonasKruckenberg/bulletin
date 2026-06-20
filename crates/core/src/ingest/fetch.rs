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
//! poll/cluster/digest/send path, and serialized by an advisory lock so duplicate/overlapping jobs
//! no-op (mirroring PublicBuild). "Fall behind, never wrong": a slow or unreachable site delays only
//! the *enrichment*, never a punctual digest, and a per-event failure is recorded (with a bounded
//! retry budget) and simply skipped.
//!
//! **Security — this is outbound fetch of attacker-influenced URLs (feed-supplied links).** Every
//! request is SSRF-guarded ([`fetch_article`]): the scheme must be http(s); each hop's host is
//! validated up front (IP literals directly, names by DNS) and rejected if it lands on any
//! loopback/private/link-local/ULA/CGNAT/multicast/reserved range (v4 + v6, including IPv4-mapped and
//! NAT64/6to4-embedded v4); **every redirect hop is re-validated**, not just the first URL; and the
//! shared client's own [`SafeResolver`] re-checks the address actually connected to, closing the
//! DNS-rebinding (TOCTOU) window for name hosts. A response-size cap, a request timeout, and a
//! `text/html` content-type allowlist bound what one fetch can pull. Per-domain politeness (a
//! per-sweep cap) plus bounded fetch concurrency keep us a good citizen.
//!
//! Deferred: `robots.txt` is **not** consulted yet. The per-domain cap and conservative defaults are
//! the interim politeness; honoring `robots.txt` (and `Crawl-delay`) is a follow-up.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::{postgres::PgRow, PgConnection, Row};
use url::Url;
use uuid::Uuid;

use crate::common::kind::SourceKind;
use crate::common::scope::Scope;

/// Consecutive fetch attempts after which an event drops out of the work queue. A *transient* failure
/// (DNS blip, 5xx, timeout) bumps the counter by one and is retried; a *permanent* one (bad scheme,
/// blocked host, 4xx, oversize) spends the whole budget at once ([`FetchError::is_permanent`]), so a
/// dead link doesn't re-burn the sweep. A content change does not reset this; the link is fixed for
/// the life of the event.
pub const MAX_FETCH_ATTEMPTS: i16 = 3;

/// A 64-bit key for the fetch-sweep session advisory lock — distinct from the build lock. Serializes
/// fetch sweeps across replicas/piled-up jobs so a duplicate no-ops instead of double-fetching.
const FETCH_LOCK_KEY: i64 = 0x6275_6c6c_6574_6e02; // "bulletn\x02"

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
    /// Max concurrent in-flight fetches in a sweep. Bounds outbound load and keeps one slow/timing-out
    /// origin from serializing the whole pass, while staying polite (with the per-domain cap, at most
    /// that many hit one origin at once).
    pub max_concurrent_fetches: usize,
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
            max_concurrent_fetches: 4,
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
        if let Ok(v) = std::env::var("BULLETIN_FETCH_MAX_CONCURRENT") {
            if let Ok(n) = v.trim().parse::<usize>() {
                if n > 0 {
                    cfg.max_concurrent_fetches = n;
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
    /// A transport/protocol error (connect, TLS, read) — also the surfaced outcome when the shared
    /// client's [`SafeResolver`] refuses every resolved address for a name host (defense-in-depth
    /// rebinding guard, in addition to the up-front [`BlockedAddress`](Self::BlockedAddress) check).
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

    /// Whether retrying this failure on a later sweep is pointless — the link/content won't change.
    /// Permanent failures spend the whole retry budget at once (the dead/disallowed link drops out of
    /// the work queue immediately rather than re-failing identically for `MAX_FETCH_ATTEMPTS` sweeps);
    /// transient ones (DNS blips, 5xx, timeouts, connection errors) keep their per-sweep retries so a
    /// momentarily-flaky-but-live article is not abandoned on the first stumble.
    pub fn is_permanent(&self) -> bool {
        match self {
            FetchError::BadScheme
            | FetchError::BlockedAddress
            | FetchError::DisallowedContentType(_)
            | FetchError::TooLarge
            | FetchError::Empty
            | FetchError::TooManyRedirects => true,
            // 4xx won't change on retry, except the explicitly-retryable 408 (Request Timeout) and 429
            // (Too Many Requests); 5xx are transient.
            FetchError::BadStatus(code) => {
                (400..500).contains(code) && *code != 408 && *code != 429
            }
            FetchError::DnsFailure | FetchError::Transport(_) => false,
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
/// both v4 and v6. Any v6 form that *embeds* a v4 address (IPv4-mapped/compatible, NAT64, 6to4) is
/// unwrapped and re-checked as v4, so an internal v4 host can't be reached through a v6 representation.
/// The single decision every resolved address — first URL and every redirect hop — is run through.
fn is_disallowed_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_disallowed_v4(v4),
        IpAddr::V6(v6) => {
            // ::ffff:a.b.c.d (mapped) and ::a.b.c.d (compatible) reach the same v4 host.
            if let Some(v4) = v6.to_ipv4() {
                if is_disallowed_v4(v4) {
                    return true;
                }
            }
            // NAT64 (64:ff9b::/96 well-known, and the 64:ff9b:1::/48 local-use prefix) and 6to4
            // (2002::/16) also carry an embedded v4 that `to_ipv4` does not unwrap — extract it from
            // its standard position and re-check, so e.g. the NAT64 form of 127.0.0.1 is blocked too.
            if let Some(v4) = embedded_v4(v6) {
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

/// Extract the IPv4 address embedded in a transition-mechanism v6 address that `Ipv6Addr::to_ipv4`
/// does not unwrap: NAT64 (`64:ff9b::/96` and `64:ff9b:1::/48`) carries it in the low 32 bits, 6to4
/// (`2002::/16`) in segments 1–2. `None` for any other address. Used by [`is_disallowed_ip`] to
/// re-check the embedded host against the v4 rules, closing a v6-representation SSRF bypass.
fn embedded_v4(ip: Ipv6Addr) -> Option<Ipv4Addr> {
    let s = ip.segments();
    let from = |hi: u16, lo: u16| {
        Ipv4Addr::new(
            (hi >> 8) as u8,
            (hi & 0xff) as u8,
            (lo >> 8) as u8,
            (lo & 0xff) as u8,
        )
    };
    if s[0] == 0x0064 && s[1] == 0xff9b {
        // NAT64: embedded v4 is the final 32 bits (well-known /96; the /48 local-use prefix places it
        // there too for the common case we care about — an internal-v4 target).
        Some(from(s[6], s[7]))
    } else if s[0] == 0x2002 {
        // 6to4: embedded v4 is segments 1–2.
        Some(from(s[1], s[2]))
    } else {
        None
    }
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

/// Validate that a parsed URL's host is safe to connect to: reject a non-http(s) scheme, and reject
/// the host if any address it resolves to is in a blocked range. IP-literal hosts are checked directly
/// (no DNS); name hosts are looked up. Returns a clean [`FetchError`] (and so a precise metric/log)
/// per hop, *before* the request — the shared client's [`SafeResolver`] then re-validates the actual
/// connect address as defense-in-depth against DNS rebinding.
async fn validate_url(url: &Url) -> Result<(), FetchError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(FetchError::BadScheme);
    }
    let port = url.port_or_known_default().ok_or(FetchError::BadScheme)?;
    let host = url.host().ok_or(FetchError::BadScheme)?;

    let ips: Vec<IpAddr> = match host {
        url::Host::Ipv4(ip) => vec![IpAddr::V4(ip)],
        url::Host::Ipv6(ip) => vec![IpAddr::V6(ip)],
        url::Host::Domain(domain) => tokio::net::lookup_host((domain, port))
            .await
            .map_err(|_| FetchError::DnsFailure)?
            .map(|sa| sa.ip())
            .collect(),
    };

    if ips.is_empty() {
        return Err(FetchError::DnsFailure);
    }
    // Conservative: block if *any* resolved address is disallowed, so a host that resolves to a public
    // and a private address (a partial-rebinding attempt) is rejected outright.
    if ips.iter().copied().any(is_disallowed_ip) {
        return Err(FetchError::BlockedAddress);
    }
    Ok(())
}

/// A reqwest DNS resolver that drops every disallowed address ([`is_disallowed_ip`]) before reqwest
/// connects, returning an error when nothing safe remains. Installed on the shared fetch client so the
/// address actually dialed for a *name* host is always one we validated — closing the DNS-rebinding
/// (TOCTOU) gap between the up-front [`validate_url`] check and the connect, with a single reusable
/// client rather than a per-host pinned one. (IP-literal hosts bypass the resolver, so they are
/// guarded only by the up-front check — which is why that check is kept.)
struct SafeResolver;

impl reqwest::dns::Resolve for SafeResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        Box::pin(async move {
            let host = name.as_str().to_owned();
            // Port 0: reqwest applies the URL's port to the returned addrs (documented behavior).
            let resolved = tokio::net::lookup_host((host.as_str(), 0u16)).await?;
            let safe: Vec<SocketAddr> = resolved.filter(|sa| !is_disallowed_ip(sa.ip())).collect();
            if safe.is_empty() {
                return Err(format!("blocked or unresolvable host: {host}").into());
            }
            Ok(Box::new(safe.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Build the one HTTP client a sweep reuses across every event and redirect hop (connection pool / TLS
/// cache amortized), with redirects disabled (we follow them manually to re-validate each hop) and the
/// [`SafeResolver`] enforcing SSRF at connect time. Mirrors `summarize::build_summarize_http` — client
/// construction in one place.
fn build_fetch_http(cfg: &FetchConfig) -> Result<reqwest::Client, FetchError> {
    reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(cfg.user_agent.clone())
        .dns_resolver(Arc::new(SafeResolver))
        .build()
        .map_err(|e| FetchError::Transport(e.to_string()))
}

// ── The fetch (async, network) ─────────────────────────────────────────────────────────────────────

/// Fetch and extract the readable article text at `raw_url` over the shared `client`, SSRF-guarding
/// every hop. Returns the extracted plain text on success, or a [`FetchError`] describing the
/// rejection/failure. Pure of the DB; the sweep persists the result.
///
/// Redirects are followed manually (the client's own redirect handling is disabled) so each hop's
/// target is re-validated *before* it is connected — the first URL passing the guard is not enough,
/// since a `Location` can point at `127.0.0.1`. The client's [`SafeResolver`] additionally re-checks
/// the connect address for name hosts (DNS-rebinding guard).
pub async fn fetch_article(
    client: &reqwest::Client,
    cfg: &FetchConfig,
    raw_url: &str,
) -> Result<String, FetchError> {
    let mut url = Url::parse(raw_url).map_err(|_| FetchError::BadScheme)?;

    for _hop in 0..=cfg.max_redirects {
        validate_url(&url).await?;

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
/// unbounded body. Decodes with `from_utf8` and only falls back to a lossy copy on invalid input, so
/// the common valid-UTF-8 path takes the buffer by value without a second full allocation.
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
    Ok(String::from_utf8(buf)
        .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned()))
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
              AND source = ANY($2)
              AND array_length(links, 1) >= 1
         ) AS pending",
    )
    .bind(MAX_FETCH_ATTEMPTS)
    .bind(SourceKind::fetchable_sources())
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
/// budget left. Newest-first, bounded by `limit` so one pass drains a slice. The fetchable-source set
/// is driven by [`SourceKind::fetchable_sources`], the single typed source of truth.
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
           AND source = ANY($4)
           AND array_length(links, 1) >= 1
         ORDER BY ingest_time DESC
         LIMIT $5",
    )
    .bind(scope_kind)
    .bind(scope_subscriber_id)
    .bind(MAX_FETCH_ATTEMPTS)
    .bind(SourceKind::fetchable_sources())
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
/// event's cluster (if already built) back into the summarization work queue via
/// [`cluster::store::touch_group`](crate::cluster::store::touch_group) — so a fetch that lands *after*
/// the cluster was first summarized off the snippet re-summarizes off the richer text (the
/// `summary_hash` staleness gate then confirms the content actually moved). A not-yet-built cluster
/// needs no nudge: it'll summarize with `full_text` already present. Runs in the cluster's scope
/// context (public sweep → no-subscriber).
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

    crate::cluster::store::touch_group(&mut *conn, scope, source, group_key).await
}

/// Record a failed fetch attempt (leaving `full_text` NULL). A `permanent` failure spends the whole
/// budget at once so the dead/disallowed link drops out of the work queue immediately; a transient one
/// bumps the counter by one and is retried next sweep until [`MAX_FETCH_ATTEMPTS`] is spent. Either
/// way a fetch failure is never fatal to summarization — the event stays at its snippet.
async fn record_fetch_failure(
    conn: &mut PgConnection,
    id: Uuid,
    permanent: bool,
) -> Result<(), sqlx::Error> {
    if permanent {
        sqlx::query("UPDATE event SET full_text_attempts = $2 WHERE id = $1")
            .bind(id)
            .bind(MAX_FETCH_ATTEMPTS)
            .execute(conn)
            .await?;
    } else {
        sqlx::query("UPDATE event SET full_text_attempts = full_text_attempts + 1 WHERE id = $1")
            .bind(id)
            .execute(conn)
            .await?;
    }
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
/// no-subscriber RLS context (RSS is public-only, so there is no private counterpart).
///
/// Serialized by a session advisory lock: if another sweep already holds it (a piled-up duplicate
/// job, or a concurrent replica), this one no-ops with empty stats rather than double-fetching and
/// double-spending retry budgets — the same protection PublicBuild gets from its build lock. The lock
/// is released on the normal and error paths; a hard panic in the body would leak it until the worker
/// process exits (only the *enrichment* path is affected, never digests).
pub async fn sweep_article_fetch(
    pool: &sqlx::PgPool,
    cfg: &FetchConfig,
) -> anyhow::Result<FetchStats> {
    use anyhow::Context;

    let mut lock_conn = pool
        .acquire()
        .await
        .context("acquire fetch-lock connection")?;
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(FETCH_LOCK_KEY)
        .fetch_one(lock_conn.as_mut())
        .await
        .context("acquire fetch sweep lock")?;
    if !locked {
        tracing::debug!("article fetch sweep already in progress; skipping");
        return Ok(FetchStats::default());
    }

    let result = run_sweep(pool, cfg).await;

    // Release the session lock (best-effort: a failure to unlock is logged, not surfaced over the
    // sweep result, and the lock would in any case clear when this connection's session ends).
    if let Err(e) = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(FETCH_LOCK_KEY)
        .execute(lock_conn.as_mut())
        .await
    {
        tracing::warn!(error = %e, "failed to release article fetch sweep lock");
    }
    result
}

/// The locked sweep body: load the work queue, apply the per-domain cap (a cheap sequential pre-pass),
/// then fetch the survivors with bounded concurrency, writing each result back as it lands. Never
/// errors as a whole — a per-event fetch or write-back failure is tracked and skipped, exactly like
/// the summarization sweep's per-cluster failures (§3.7 spirit).
async fn run_sweep(pool: &sqlx::PgPool, cfg: &FetchConfig) -> anyhow::Result<FetchStats> {
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

    // Sequential pre-pass: pick each event's candidate link and apply the per-domain cap (cheap, no
    // network), so the concurrent stage only fetches the survivors. An event with no usable link is a
    // permanent failure (a malformed link won't fix itself) and is dropped now.
    let mut domain_counts: HashMap<String, usize> = HashMap::new();
    let mut to_fetch: Vec<(Uuid, SourceKind, String, String, String)> = Vec::new();
    for target in due {
        // The work-queue SQL filters to fetchable sources; this ties that filter back to the typed
        // contract ([`SourceKind::has_fetchable_article`]) so the two can't silently diverge.
        debug_assert!(
            target.source.has_fetchable_article(),
            "fetch work queue returned a non-fetchable source"
        );
        let Some((link, domain)) = first_fetchable_link(&target.links) else {
            metric::attempt("no_link", Duration::ZERO);
            let id = target.id;
            let _ = with_scope(pool, ctx, move |conn| {
                Box::pin(async move {
                    record_fetch_failure(conn, id, true)
                        .await
                        .context("record fetch failure (no link)")
                })
            })
            .await;
            stats.failed += 1;
            continue;
        };

        let count = domain_counts.entry(domain.clone()).or_insert(0);
        if *count >= cfg.max_per_domain_per_sweep {
            metric::skipped();
            stats.skipped += 1;
            continue;
        }
        *count += 1;
        to_fetch.push((target.id, target.source, target.group_key, link, domain));
    }

    // Concurrent fetch stage, bounded by a semaphore so a slow/timing-out origin can't serialize the
    // whole pass (and at most `max_concurrent_fetches` requests are in flight). Each task fetches and
    // writes back its own result; a write-back failure is logged, never aborting the sweep.
    let http =
        build_fetch_http(cfg).map_err(|e| anyhow::anyhow!("build fetch http client: {e}"))?;
    let sem = Arc::new(tokio::sync::Semaphore::new(
        cfg.max_concurrent_fetches.max(1),
    ));
    let mut tasks: tokio::task::JoinSet<bool> = tokio::task::JoinSet::new();

    for (id, source, group_key, link, domain) in to_fetch {
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .expect("fetch semaphore is never closed");
        let http = http.clone();
        let cfg = cfg.clone();
        let pool = pool.clone();
        let scope = scope.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let started = Instant::now();
            let outcome = fetch_article(&http, &cfg, &link).await;
            let elapsed = started.elapsed();
            match outcome {
                Ok(text) => {
                    metric::attempt("ok", elapsed);
                    tracing::debug!(
                        event_id = %id, %domain, chars = text.chars().count(),
                        elapsed_ms = elapsed.as_millis() as u64, "article fetched"
                    );
                    let store = with_scope(&pool, ctx, move |conn| {
                        Box::pin(async move {
                            store_full_text(conn, id, source, &group_key, &scope, &text)
                                .await
                                .context("store full text")
                        })
                    })
                    .await;
                    match store {
                        Ok(()) => true,
                        Err(e) => {
                            tracing::warn!(event_id = %id, error = %format!("{e:#}"), "store full text failed");
                            false
                        }
                    }
                }
                Err(e) => {
                    metric::attempt(e.describe(), elapsed);
                    tracing::debug!(
                        event_id = %id, %domain, error = %e,
                        elapsed_ms = elapsed.as_millis() as u64,
                        "article fetch failed (event stays at snippet)"
                    );
                    let permanent = e.is_permanent();
                    let rec = with_scope(&pool, ctx, move |conn| {
                        Box::pin(async move {
                            record_fetch_failure(conn, id, permanent)
                                .await
                                .context("record fetch failure")
                        })
                    })
                    .await;
                    if let Err(e) = rec {
                        tracing::warn!(event_id = %id, error = %format!("{e:#}"), "record fetch failure failed");
                    }
                    false
                }
            }
        });
    }

    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(true) => stats.fetched += 1,
            Ok(false) => stats.failed += 1,
            Err(e) => {
                tracing::warn!(error = %e, "article fetch task panicked");
                stats.failed += 1;
            }
        }
    }

    Ok(stats)
}

/// Pick the first link that parses as an absolute http(s) URL with a host, returning
/// `(url, domain)`. The domain (host, lowercased, leading `www.` dropped) keys the per-domain
/// politeness cap. `None` when no link is fetchable.
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
    fn blocks_embedded_v4_in_nat64_and_6to4() {
        // NAT64 (64:ff9b::/96) of internal v4 targets — the embedded host must be unwrapped + blocked.
        assert!(is_disallowed_ip("64:ff9b::7f00:1".parse().unwrap())); // 127.0.0.1
        assert!(is_disallowed_ip("64:ff9b::a00:1".parse().unwrap())); // 10.0.0.1
        assert!(is_disallowed_ip("64:ff9b::a9fe:a9fe".parse().unwrap())); // 169.254.169.254
                                                                          // 6to4 (2002::/16) of a private v4.
        assert!(is_disallowed_ip("2002:c0a8:0101::1".parse().unwrap())); // 192.168.1.1
                                                                         // NAT64/6to4 of a *public* v4 stays allowed (no false positive).
        assert!(!is_disallowed_ip("64:ff9b::808:808".parse().unwrap())); // 8.8.8.8
        assert!(!is_disallowed_ip("2002:0808:0808::1".parse().unwrap())); // 8.8.8.8
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

    #[test]
    fn permanent_vs_transient_classification() {
        // Permanent — spend the budget at once (won't change on retry).
        for e in [
            FetchError::BadScheme,
            FetchError::BlockedAddress,
            FetchError::DisallowedContentType("application/pdf".into()),
            FetchError::TooLarge,
            FetchError::Empty,
            FetchError::TooManyRedirects,
            FetchError::BadStatus(404),
            FetchError::BadStatus(403),
        ] {
            assert!(e.is_permanent(), "should be permanent: {e}");
        }
        // Transient — keep retrying.
        for e in [
            FetchError::DnsFailure,
            FetchError::Transport("connect".into()),
            FetchError::BadStatus(503),
            FetchError::BadStatus(500),
            FetchError::BadStatus(408),
            FetchError::BadStatus(429),
        ] {
            assert!(!e.is_permanent(), "should be transient: {e}");
        }
    }

    #[tokio::test]
    async fn rejects_non_http_scheme() {
        let cfg = FetchConfig::default();
        let client = build_fetch_http(&cfg).unwrap();
        for bad in ["ftp://example.com/x", "file:///etc/passwd", "gopher://x"] {
            assert_eq!(
                fetch_article(&client, &cfg, bad).await,
                Err(FetchError::BadScheme),
                "should reject {bad}"
            );
        }
    }

    #[tokio::test]
    async fn rejects_url_resolving_to_loopback() {
        // An explicit loopback/private IP host is validated without DNS and blocked before any connect.
        let cfg = FetchConfig::default();
        let client = build_fetch_http(&cfg).unwrap();
        for bad in [
            "http://127.0.0.1/secret",
            "http://169.254.169.254/latest/meta-data",
            "http://[::1]:80/x",
            "http://10.0.0.5/internal",
        ] {
            assert_eq!(
                fetch_article(&client, &cfg, bad).await,
                Err(FetchError::BlockedAddress),
                "should block {bad}"
            );
        }
    }

    #[test]
    fn validate_url_blocks_private_literal_and_allows_public() {
        // IP-literal hosts take no DNS, so this drives the per-hop validation arm synchronously.
        let rt = tokio::runtime::Runtime::new().unwrap();
        assert_eq!(
            rt.block_on(validate_url(
                &Url::parse("http://192.168.1.1/admin").unwrap()
            )),
            Err(FetchError::BlockedAddress)
        );
        assert_eq!(
            rt.block_on(validate_url(&Url::parse("ftp://8.8.8.8/").unwrap())),
            Err(FetchError::BadScheme)
        );
        assert_eq!(
            rt.block_on(validate_url(&Url::parse("http://8.8.8.8/").unwrap())),
            Ok(())
        );
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
