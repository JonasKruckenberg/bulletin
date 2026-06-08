# Digest System — Candidate Data Sources (Backlog)

**Status:** Research backlog — not committed scope.
**Last updated:** 2026-06-08
**Companion to:** `digest-system-design.md` (product + data model) and
`digest-technical-architecture.md` (Rust runtime). Those docs own v1 scope
(**RSS + GitHub + Slack**, with Notion / Gmail / Bluesky / Mastodon / Twitter-X
already triaged). **This** doc is the long list of *everything else we could add
later*, evaluated against the same source model so each one drops into the
existing `Source` trait without rework.

> **API specifics below are a 2026-06 research snapshot — re-verify before building any
> connector.** Pricing, scopes, and rate limits move constantly (several sources
> changed materially in early 2026).

---

## 1. How to read this — the evaluation axes

Every candidate is scored on the axes the design already cares about, so adding a
source is a matter of filling in the connector contract (§5/§7 of the design doc),
not inventing new machinery:

| Axis | Why it matters | Where it lives in the design |
|---|---|---|
| **Push / Pull** | Real-time webhook/stream vs poll-with-cursor. Both normalize into the same `event` table. | §7.1 `Source` trait (`parse_webhook` vs `poll`) |
| **Auth & cost/compliance gate** | The real filter. Things like Gmail's CASA ($15–75k/yr) or X's pay-per-read defer a source regardless of its value. | §7.4, tech-doc §8 |
| **Scope** | public (globally clustered, amortized) vs private (per-subscriber, isolated). | §4 visibility scopes |
| **content_kind** | `longform` / `announcement` / `message` — the depth signal that drives Story vs Note. | §7.1, §8.3 |
| **group_key** | The within-source grouping atom (PR#, thread id, incident id). Deterministic, adapter-computed. | §8.1 grouping |
| **Linking entities** | What it emits for cross-source linking — URLs, CVEs, repos, customer ids. URLs/native ids carry the load. | §8.2 |
| **Engagement state** | seen / acted support, for the §14 "filter out already-dealt-with" feature. | §14 |

Three recurring **architectural patterns** show up across families and are worth
naming up front, because they recur in the tables:

- **Signal → hydrate** (Notion's pattern): sparse webhook just says "X changed";
  the tick fetches current state via API. Applies to most PM/docs tools
  (Airtable, ClickUp, Drive, Asana, Dropbox).
- **Webhook → poll reconciliation**: webhooks are lossy (at-most-once), so even
  push sources get a periodic poll. Already in the design (§7.2).
- **Public RSS/feed as the cheap path**: for many "content" sources (YouTube,
  podcasts, Medium, Substack, package registries) a no-auth feed delivers 80% of
  the value with none of the API friction.

---

## 2. Headline findings (read this first)

1. **The biggest wins are generic ingestion patterns, not connectors.** A single
   **generic inbound webhook + JSON normalizer** turns Zapier/Make/IFTTT/n8n/
   Pipedream into a connector factory for *thousands* of apps — the source app's
   OAuth and maintenance burden lives on *their* platform, not ours. Build this
   before almost any bespoke connector. (§9)

2. **WebSub upgrades the RSS pipeline we already have from poll to push** for a
   large slice of the open web (WordPress, Medium, Blogger, Mastodon, many news
   sites) at near-zero marginal cost — we already parse those feeds. Pair with
   **JSON Feed** (same fetch path, easier than XML). (§9)

3. **One ActivityPub consumer actor unlocks the whole fediverse** (Mastodon,
   Lemmy, PeerTube, WriteFreely, Pixelfed) through one protocol instead of five
   connectors. Generalizes the already-planned Mastodon work. (§9)

4. **The email-alias / inbound-parse pattern is a CASA-free path to email value.**
   Give each user a unique address on our domain (Postmark/Mailgun/SendGrid
   inbound parse); they forward newsletters + notification emails. No
   `gmail.readonly`, no CASA, no holding the user's mailbox creds — and we get
   `List-Id`/`List-Unsubscribe` for clustering and a free SpamAssassin score. This
   likely captures most of the newsletter/notification value Gmail was deferred
   for. **Microsoft Graph mail** is the other big email unlock: real webhooks +
   delta sync and, crucially, *no CASA-equivalent* (publisher verification is
   optional). (§4, §6)

5. **Ops/monitoring sources are a natural fit twice over.** Their
   acknowledge→resolve lifecycle maps directly onto the §14 "seen vs acted"
   engagement feature, and their severity (Sev0–4, P1–P5, SLO burn) is a strong
   cross-source **priority** signal (§8.3). Mostly push-native and mostly private.

6. **A handful of high-value sources are effectively blocked by policy** and should
   stay deferred no matter how much we want them: **LinkedIn** (member-feed read is
   a closed permission), **Google Scholar** (no API; ToS/robots forbid scraping),
   **Twitter/X** (per-read billing), and **Instagram/Facebook** (Business
   Verification + per-permission App Review, only worthwhile for accounts you own).

---

## 3. Developer & code platforms

| Source | Push | Pull | Auth/cost gate | Scope | content_kind | group_key | Linking entities | Engagement | Notes |
|---|---|---|---|---|---|---|---|---|---|
| **GitLab** | webhooks (MR/issue/pipeline/deploy/comment) | REST+GraphQL; events API; keyset | OAuth2 (2h+refresh); coarse scopes (prefer `read_api`); self-host = per-instance app | both | announcement / message | MR/issue `iid`, pipeline id | repos, users, MRs, commits, CVEs, URLs | partial (todos/notifications API) | GitHub peer; webhook auto-disables after 4 fails |
| **Bitbucket Cloud** | repo/workspace webhooks | REST v2 (cursor) | OAuth2 (Atlassian); free | both | message / announcement | PR id, commit hash | repos, users, PRs, commits | weak | Lower priority; smaller share |
| **Gitea / Forgejo** | repo/org/system webhooks (GH-compatible) | GH-compatible REST | PAT/OAuth2; **no vendor gate** (self-host) | both (usually private) | message / announcement | PR/issue index, release tag | repos, users, issues, commits | partial (notifications API) | Great for self-hosters; low auth friction |
| **Linear** | webhooks (issues/comments/projects) | **GraphQL only**; cursor | OAuth2/PAT; admin scope for webhooks; free | private | message / announcement | issue ident (`ENG-123`) | issues, users, projects, GitHub PR links | **strong** (state/assignee/subscribers) | Top-tier eng signal; clean cross-link to GitHub |
| **Jira Cloud** | webhooks (Connect/REST); 5/app/tenant | REST + JQL `updated>=` | OAuth2/Connect/Forge; **points-based limits from Mar 2026** | both | message / announcement | issue key (`PROJ-42`) | issues, users, projects, sprints | **strong** (status/assignee/watchers) | High value where teams live in Jira; heavier auth |
| **Sentry** | Integration-Platform webhooks (issue/metric/comment) | REST (cursor) | **internal integration = org-only token, no review** (easy) | both | announcement / message | issue fingerprint, release | projects, releases, **suspect commits/PRs**, users | **strong** (resolved/ignored/assigned, seen-by) | Excellent S/N; internal integration is the fast path |
| **npm** | `npm hook` on publish | registry REST; **CouchDB `_changes`** since-seq | token for hooks; read = none | public | announcement | package+version | package, version, maintainers, repo | — | Filter `_changes` against the user's manifest |
| **PyPI** | — | RSS (new/updates/per-project) + JSON | none | public | announcement | project+version | project, version, repo | — | Per-project RSS = clean dep-watch |
| **crates.io** | — | RSS + REST | none | public | announcement | crate+version | crate, owners, repo | — | Simplest path for Rust stacks |
| **Go modules** | — | `index.golang.org/index?since=` JSONL | none | public | announcement | module path+version | module, version, repo | — | Native since-cursor; high volume → filter |
| **RubyGems** | per-gem or global `*` web hooks | REST | key for hooks | public | announcement | gem+version | gem, owners, repo | — | Global `*` hook = firehose; filter client-side |
| **NuGet** | — | **V3 catalog** (append-only, `commitTimeStamp` cursor) | none | public | announcement | package+version | package, authors, repo | — | Best-designed registry delta API |
| **Maven Central** | — | REST search; per-artifact XML | none | public | announcement | `group:artifact`+version | coordinates, repo | — | No first-party feed → poll search; coarse |
| **Docker Hub / GHCR** | repo push webhooks (own repos); GH `registry_package` | OCI/REST tags | token / `read:packages` | both | announcement | repo:tag, package+version | image/package, digest, owner | — | No webhook for *upstream* images → poll tags |
| **Dependabot** | `dependabot_alert` webhook | REST alerts | GitHub App/PAT; some org features need Adv. Security | private | announcement | alert # / GHSA | repo, package, **CVE/GHSA**, fix PR | **yes** (open/fixed/dismissed + assignee) | High-value security signal |
| **Renovate** | — (emits PRs + Dependency Dashboard issue) | via host PR/issue API | inherits host; OSS free | both | announcement | PR # / dashboard issue | repo, package, version, PR | via host PR state | Model as `renovate[bot]` PR author |
| **CircleCI / GitHub Actions / Buildkite** | webhooks (workflow/job/build result) | REST/GraphQL (cursor) | token / GitHub App; mostly free tiers | both | announcement | pipeline/run/build id | repo, commit, branch, actor | partial (conclusion) | Filter to failures / first-success only |
| **Vercel / Netlify** | deploy webhooks (created/ready/error) | REST (deployments) | OAuth/integration token; free tiers | both | announcement | deployment id | project, deploy, commit, URL | partial (deploy state) | Ties deploy→commit for cross-linking |
| **Statuspage family** (Atlassian Statuspage, GitHub/npm status, vendor pages) | webhook on incident/component change | public status JSON + **RSS/Atom** | public read, no auth | **public** | announcement | incident id | provider, components, URL, window | lifecycle in payload | Universal across thousands of SaaS; subscribe by RSS or webhook |

**Top picks:** **Sentry** (frictionless internal-integration auth, richest engagement
state, suspect-commit linking), **Linear** (clean GraphQL deltas, stable ids, native
GitHub cross-links), **Dependabot** (CVE/GHSA + real alert state), the **package-registry
layer** (all free, all with proper since-cursors — valuable filtered against the user's
own dependency manifests), and **Statuspage feeds** (trivially cheap public RSS/webhook,
high "is my tooling down" relevance).

---

## 4. Email & newsletters

> v1 deferred Gmail over the CASA assessment. The play here is the *alternatives* that
> sidestep CASA entirely.

| Source | Push | Pull | Auth/cost gate | Scope | content_kind | group_key | Linking entities | Engagement | Notes |
|---|---|---|---|---|---|---|---|---|---|
| **Microsoft Graph mail (Outlook/M365)** | Graph change notifications (`isRead` filterable); sub ~3-day TTL, renew | REST + **`message: delta`** | OAuth `Mail.Read`; **no CASA** — publisher verification optional; personal Outlook.com works | private | message | `conversationId` (native) | addresses+domains, `List-Id`, body URLs, `webLink` | `isRead`, flag, replied (inferable) | **Strongest first-party pick** — push + delta, no security-assessment gate |
| **Generic IMAP** | partial (`IDLE` held connection) | `UIDVALIDITY`/`UIDNEXT` + **CONDSTORE/QRESYNC** (`MODSEQ`) deltas | app password / provider OAuth; **no central gate** but you custody creds | private | message | RFC822 `References` (thread yourself) | addresses+domains, `List-Id`, URLs | `\Seen`/`\Answered`/`\Flagged` | Universal fallback (reaches iCloud, Proton-via-bridge); op cost = held connections + cred custody |
| **JMAP (Fastmail)** | spec supports push but **Fastmail doesn't enable it for 3rd parties** → pull-only today | best-in-class: `state` token + `Email/changes` deltas | bearer token / OAuth; no CASA | private | message | `threadId` (native) | addresses+domains, `List-Id`, URLs | `$seen`/`$answered` | Cleanest sync model but vendor-locked; poll the efficient `/changes` |
| **ProtonMail (Bridge)** | — (local IMAP only) | local IMAP | **no cloud API**; needs paid plan + local Bridge daemon per user | private | message | References | addresses, headers, URLs | IMAP flags | Architecturally unsuitable for a hosted service |
| **Apple iCloud Mail** | — (`IDLE`) | IMAP (UID/CONDSTORE) | app-specific password (needs 2FA) | private | message | References | addresses, headers, URLs | IMAP flags | Long-tail provider via the generic-IMAP path |
| **Substack / Buttondown / Ghost / beehiiv** (as content) | owner-only webhooks (beehiiv `post.sent`, Ghost `post.published`) | **public RSS per publication** | none (RSS); API is owner-facing | **public** | **longform** | feed / post GUID | publication domain, post URL, body links | — | RSS is the lingua franca for *following* others' newsletters |
| **Email-alias / inbound-parse** (Mailgun/Postmark/SendGrid) | **true push** — provider POSTs each forwarded message as JSON (full MIME) | — | provider account only; **no OAuth, no CASA**; you never touch user's mailbox. Postmark cheap, Mailgun ~$2/1k | private (per-user alias) | message / longform | `Message-ID`+`References`; or per-alias routing key | From+domain, **`List-Id`/`List-Unsubscribe`**, body URLs | derive yourself; Postmark passes spam score | **The smart CASA-free path** — see headline #4 |

**Top picks:** **Microsoft Graph mail** for whole-inbox users (no CASA gate);
**email-alias/inbound-parse** as the *default* path for newsletters + notification emails
(no compliance burden, true push, free spam scoring, `List-*` headers for clustering and
one-click unsubscribe); **generic IMAP** as the universal fallback. RSS for any followed
newsletter. Proton/iCloud have no usable cloud API.

---

## 5. Chat & communication

> Key distinction: can we read the *real person's* messages, or only bot-/channel-scoped
> content? This separates personal-digest value from business-only sources.

| Source | Push | Pull | Auth/cost gate | Scope | group_key | Linking entities | Engagement | Notes |
|---|---|---|---|---|---|---|---|---|
| **Slack** (already v1) | Events API / Socket Mode | `conversations.history/.replies` (cursor) | user token; **non-Marketplace apps tightened Mar 2026**; history cut to 15/call | both (user token sees the real person) | channel+`thread_ts` | channels, users, files, URLs, reactions | read: `last_read`; acted: reactions, replies | Best-in-class personal signal |
| **Matrix** | `/sync` long-poll (`since` cursor) | `/sync` + `/messages` | access token; **open/self-hostable, no vendor gate** | personal (all joined rooms) | room + `m.thread` | room, mxid, URL, media | **native `m.receipt` receipts** + reactions | Cleanest open protocol; E2EE decryption is the gotcha |
| **Zulip** | event queue + `/events` long-poll | `GET /messages` anchor | API key (user key = personal view); self-hostable | both | stream + **topic** | stream, topic, user, URL | **per-msg `read` flag** + reactions | Topic = free thread `group_key`; explicit read state |
| **MS Teams (Graph)** | change notifications (**data inline**) | `/getAllMessages` (`@odata.nextLink`) | Azure AD; **export APIs unmetered since Aug 2025**; delegated token = real chats | both | chat/channel id | team, channel, chat, user, URL | no clean read marker; reactions/replies | Much cheaper post-2025; notification delays reported |
| **Telegram (MTProto)** | update stream | `messages.getHistory` (offset) | **user-account auth** (api_id+phone); flood-wait/ban risk | personal (all chats/DMs) | peer + topic id | chat, user, channel, URL, media | `readHistory` marker + reactions | Only way to ingest a real Telegram identity (Bot API can't) |
| **Telegram (Bot API)** | `setWebhook` XOR `getUpdates` | `getUpdates` offset; no backfill | BotFather token, free | bot only | chat + `message_thread_id` | chat, user, URL | none for bots | Can't read your personal DMs |
| **Discord** | Gateway WS (intents) | REST messages (id paging) | bot token + **MESSAGE_CONTENT** intent; user-token = ToS ban | bot only (channels it's in) | channel + thread | guild, channel, user, URL | reactions, replies | Effectively bot-scoped; good for owned community servers |
| **Google Chat** | Workspace Events → Pub/Sub | `spaces.messages.list` | OAuth user (real spaces) or bot; `readonly` needs admin approval | both | space + thread | space, user, URL | reactions, replies | User-auth reads real account; Pub/Sub setup friction |
| **Rocket.Chat / Mattermost** | WS subscriptions / events | REST history (cursor) | PAT (real user); self-host = no gate | both | room/channel + thread/`root_id` | room, user, URL, file | unread markers + reactions | Full personal read via PAT (only member channels for MM) |
| **Twist** | OAuth webhooks | REST threads/comments | OAuth (Doist), free | private | channel + thread | workspace, channel, thread, user | inbox/unread + comments | Async/low-noise — fits digest ethos; small ecosystem |
| **IRC** | live TCP stream (real-time only) | none (unless IRCv3 chathistory/bouncer) | nick/SASL, free | joined channels | channel / nick | channel, nick, URL | none native | Needs a persistent bouncer (ZNC/soju) to be a durable source |
| **Signal** | poll via `signal-cli` (JSON-RPC) | signal-cli REST | **no official API**; self-host signal-cli as linked device; fragile/ToS-adjacent | personal | source#/group | contact, group, media | receipts, reactions | Best-effort only |
| **WhatsApp Cloud API** | webhook (msgs/status/read) | Graph REST (no history backfill) | Meta Business verification; **business-only**; per-template billing | **business only** | wa_id + phone# | contact, URL, media | read receipts, replies | Not your personal WhatsApp |
| **SMS/Voice (Twilio)** | inbound webhook | REST messages (paging) | account SID; **pay-per-use** (~$0.0083/SMS US) | numbers you own | conversation id / from↔to | phone#, URL, media | delivery/read status | Great for SMS/2FA/alert ingestion; metered |

**Top picks:** **Matrix** and **Zulip** (open, self-hostable, *native read-state* — the
only chat sources that hand us the §14 seen-marker for free), **MS Teams** (now unmetered,
delegated token reads real chats), and **Telegram via MTProto** (the only route to a real
Telegram identity). Deprioritize bot-/business-only sources for a *personal* digest
(Discord, WhatsApp, Signal). All chat is `content_kind: message`.

---

## 6. Productivity, PM, docs & calendar

> Most of these follow Notion's **sparse-webhook → hydrate** model, or are poll-only with
> a delta cursor. Calendar is the standout new *digest section* ("what's coming up").

| Source | Push | Pull | Auth/cost gate | Scope | content_kind | group_key | Linking entities | Engagement | Notes |
|---|---|---|---|---|---|---|---|---|---|
| **Asana** | webhooks (sparse refs) | **Events API sync tokens** (4h TTL) | OAuth2; free API | private | announcement / message | task / project id | assignees, projects, due, URLs | stories (activity); no seen | Signal→hydrate; token expiry forces frequent polls |
| **Trello** | per-model webhooks | actions feed `?since=` | OAuth **1.0a** (friction) / key+token | private (+ public boards) | announcement / message | board+card | members, labels, lists, URLs | comment/update actions | Easy webhooks; OAuth 1.0a is the pain |
| **ClickUp** | webhooks (minimal payload) | REST poll (no delta) | OAuth2/token; 100→10k rpm by plan | private | announcement / message | task / list id | assignees, tags, custom fields | comment/update events | Sparse → signal-then-hydrate |
| **Monday.com** | webhooks | **GraphQL** (`updated_at` filter, complexity budget) | OAuth2; paid | private | announcement / message | board+item | assignees, boards, status | item "updates" | Query only changed fields cheaply |
| **Airtable** | webhooks **ping-only** | **list-payloads cursor delta** (7-day retention) | PAT/OAuth2; 5 rps/base | private | longform / announcement | base/table/record | linked records, collaborators | change diffs | Cleanest mirror of the Notion pattern; mind 7-day webhook expiry |
| **Confluence** | webhooks (page/comment) | REST + **CQL `lastModified>=`** | OAuth2 3LO | private (+ public spaces) | **longform** / message | page / space | authors, labels, parent page | versions, comments | Best for docs longform; CQL = frictionless updated-since |
| **Coda** | audit-events only (admin) | REST poll (async mutations) | token/OAuth | private | longform | doc / table+row | people, URLs, linked rows | limited | Effectively poll-only for content |
| **Basecamp** | per-project webhooks | REST `events.json` | OAuth2; paid | private | message / announcement / longform | bucket+recording | assignees, parent recording | recording status | Rich `kind` taxonomy aids classification |
| **Todoist** | webhooks | **Unified API `sync_token`** delta | OAuth2 coarse scopes; free | private | announcement / message | task / project | project, labels, due | completed/updated in delta | Excellent delta sync; clean personal signal |
| **Shortcut / Height** | webhooks | REST/search (poll by updated) | Shortcut = token-only (good for personal); Height OAuth | private | announcement / message | story / task id | owners, epics, external links | state-change events | Shortcut's token auth is a plus for a personal tool |
| **Google Drive / Docs / Sheets** | `changes.watch` push (sparse, ~3min, channels expire) | **changes feed `startPageToken`** delta | OAuth2; **broad scopes → Google CASA** | private | announcement / longform | file / doc id | owners, parents, URLs, comments | modified-by; comment resolved | Changes feed → hydrate doc; CASA on sensitive scopes |
| **MS 365 / SharePoint / OneDrive** | subscriptions (basic/rich/**lifecycle**) | **`driveItem: delta`** | OAuth2; Azure AD app | private (+ org sites) | announcement / longform | drive/list item | authors, sites, sharing | modified-by | Best-engineered file stack; subs expire → renew |
| **Dropbox / Box** | webhook ping (sparse) + longpoll | **`list_folder/continue`** / `stream_position` cursor | OAuth2 scoped; Box = enterprise auth | private | announcement | file path/id | sharing, parent folder | event stream | Cursor+longpoll is cheap delta |
| **Google Calendar** | `events.watch` push (channels expire) | **`syncToken`** delta (+ `updatedMin`) | OAuth2; CASA on broad scopes | private (+ shared) | announcement | event / calendar id | attendees, organizer, Meet links, location | RSVP/responseStatus | **High digest value** — upcoming-events section |
| **MS Graph Calendar** | subscriptions (rich/lifecycle) | **`event: delta`** | OAuth2; Azure AD | private (+ org) | announcement | event / calendar id | attendees, Teams links, location | responseStatus, isCancelled | Same value as Google; org-tenant auth gate |
| **CalDAV** | — (poll) | **`sync-collection` REPORT** (`sync-token`) or CTag | basic / app-password / OAuth; often free | private (+ shared) | announcement | event UID | attendees, organizer, location | per-event ETag | Near-universal (iCloud, Fastmail, Nextcloud) |
| **ICS / webcal** | — | poll `.ics`, diff yourself | **none — just a URL** | mostly private-by-URL (+ public) | announcement | event UID | attendees, location, DESCRIPTION links | none | **Lowest-friction universal calendar path**; coarse freshness |
| **Calendly** | webhooks (invitee created/canceled) | REST v2 | PAT/OAuth2.1; **webhooks need paid plan** | private | announcement | scheduled_event uri | invitee, event type, links | created/canceled | "New meeting booked/canceled" signal |

**Top picks:** **Google + MS Graph Calendar** (clean `syncToken`/`delta` + push; an
upcoming-events block is the single highest-value new section), **MS Graph for files/docs**
(best delta + lifecycle notifications), **Todoist** (true `sync_token` + simple scopes),
**Airtable** (cleanest Notion-pattern mirror), **Confluence** (CQL updated-since for
longform docs). **Universal calendar path:** subscribe to each user's **ICS export URL**
(zero auth, near-universal) for coarse polling; upgrade to **CalDAV** `sync-token` where
supported; reserve provider-native APIs (Google/MS) for real-time push + RSVP state.

---

## 7. Social & content

> Bluesky / Mastodon already planned; Twitter/X already deferred (per-read billing).

| Source | Push | Pull | Auth/cost gate | Scope | content_kind | group_key | Linking entities | Engagement | Notes |
|---|---|---|---|---|---|---|---|---|---|
| **Hacker News** | Firebase `/v0/updates` + SSE | Firebase REST + Algolia search | **none** | public | announcement / message | story id | author, URL, story↔comment tree | score, comment count | Best friction; dual API (now + history). Top pick |
| **Lobsters** | — | `*.json` views + RSS | **none** | public | announcement / message | story short-id | author, URL, tags | score, comments | Tiny high-signal community; trivial; clean tags |
| **Reddit** | — | REST listings (`after`/`before`) | OAuth2; **commercial use = contract** (Standard ~$12k/yr, Enterprise $50k+) | public (+ private subs) | announcement / message | subreddit+post | author, subreddit, URLs | score, saved/voted | Great for keyword/subreddit watching but commercial-gated + pull-heavy |
| **Product Hunt** | — | GraphQL (cursor) | OAuth2; complexity limits | public | announcement | post (launch) | maker, product URL, topics | votes, comments | Naturally batched by day → daily "what launched" |
| **YouTube** | WebSub on channel RSS (near real-time) | Data API v3 + **uploads RSS** | OAuth/key; 10k units/day quota | public (+ private OAuth) | **longform** | video id | channel, URLs, tags, playlists | views/likes; watch state (OAuth) | **RSS/WebSub per channel = zero quota**; API for enrichment |
| **Twitch** | **EventSub** (`stream.online` etc.) | Helix REST | app token, free; host a webhook | public | announcement / message | channel/stream id | streamer, game, URL | live status | Clean "creators I follow went live" push |
| **Podcasts (Apple/iTunes → RSS)** | WebSub on feed | iTunes lookup → poll **podcast RSS** | **none** | public | **longform** | episode `<guid>` | show, author, episode URL | none | Resolve show via iTunes, ingest open RSS (covers Spotify shows too) |
| **Spotify (shows)** | — | REST `/shows/{id}/episodes` | OAuth2; **Feb 2026 cuts** (removed New Releases, batch) | public catalog (+ library) | longform | show id | show, publisher, episode URL | saved/played (scopes) | Prefer podcast RSS; music new-releases now awkward |
| **Medium** | — | per-author/pub/topic **RSS** | **none** (write API deprecated) | public | **longform** | post GUID | author, publication, tags | none | RSS-only; fine for following authors |
| **arXiv** | RSS per category | Query API (Atom) + OAI-PMH | **none**; **1 req/3s** | public | **longform** | arXiv id+version | authors, categories, DOI | none | Top academic pick; respect throttle |
| **Semantic Scholar / OpenAlex / Crossref** | — | Graph/REST + bulk | free (get S2 key) | public | longform | paperId / DOI | authors, citations, fields | citation counts | Free structured scholarly metadata; Google Scholar replacements |
| **GDELT / Google News RSS / NewsAPI** | — | REST / RSS | GDELT+GNews-RSS free; NewsAPI commercial-paid | public | announcement | article/event id | source, themes, entities, URL | none | GDELT = free news firehose w/ entity extraction; GNews RSS = zero-friction topical |
| **Nostr** | relay WS `REQ` subscriptions (push+replay) | same | **none** (pick relays) | public | message | event id / `e`-tag root | pubkeys, `t` hashtags, `e`/`p` tags | likes/boosts | Genuinely open, low-friction; relay fragmentation |
| **Lemmy** | — (ActivityPub federation) | REST (JWT per-instance) | per-instance, near-zero cost | public | announcement / message | post id | author, community, URLs | score | Reddit alternative; reachable via ActivityPub (§9) |
| **Farcaster** | **Neynar webhooks** (filtered casts) | REST feeds | Neynar paid (removes hub ops) or self-host hub | public | message | cast hash / parent | fid/username, channels, embeds | reactions | Neynar removes hub-ops friction |
| **Threads (Meta)** | webhooks (mentions/replies) | REST public read | Meta App Review; per-profile caps | public (+ own) | message / announcement | thread/post id | author, topic tags, URLs, mentions | own-post metrics | Real surface now, but App Review friction |
| **LinkedIn** | — | very limited | **blocked** — member-feed read is a closed permission; useful endpoints need Partner status | own-profile only | — | — | author, org pages (w/ MDP) | none | **Effectively blocked**; defer |
| **Instagram / Facebook** | page/IG webhooks | Graph REST | **Business Verification + per-permission App Review** | own/managed assets | message / announcement | media id | author, hashtags, mentions | likes/comments | Only worthwhile for accounts you own |
| **Twitter / X** | streaming (Enterprise only) | REST | **deferred** — ~$0.005/read, Enterprise ~$42k/mo | public/own | message | conversation id | author, URLs, hashtags | likes/replies | Cost-prohibitive (as planned) |

**Top picks (high value, low friction):** **Hacker News**, the broad **RSS layer**
(YouTube channels via WebSub, podcasts via iTunes→RSS, Medium, arXiv, Lobsters, Google
News topics — all auth-free), **Bluesky Jetstream** + **Mastodon streaming** (already
planned), **Twitch EventSub** (clean live-alert push), and **arXiv + Semantic Scholar /
OpenAlex** for academic. **Blocked/deferred by policy:** LinkedIn, Google Scholar,
Twitter/X, Instagram/Facebook.

---

## 8. Ops, monitoring, observability & cloud

> Overwhelmingly **push-native**, mostly **private**, mostly `content_kind: announcement`.
> Two free wins: ack→resolve = §14 engagement; severity = §8.3 priority.

| Source | Push | Pull | Auth/cost gate | Scope | group_key | Linking entities | Engagement | Notes |
|---|---|---|---|---|---|---|---|---|
| **PagerDuty** | V3 webhook subs (HMAC) | REST incidents/log (`since`) | token; webhooks need paid plan | private | incident id (`dedup_key`) | service, escalation policy, assignee, urgency | **ack / resolve / reassign (first-class)** | Cleanest lifecycle; priority = strong sort. Top pick |
| **Sentry** | Integration-Platform webhooks | REST issues/releases | internal integration token; free | private | issue fingerprint, release | project, env, release, culprit, level | resolved/ignored/assigned | Group fingerprint = excellent dedup. Top pick |
| **Datadog** | webhooks on monitor/alert transitions | Events + Incidents API | API+App keys; Incidents = paid | private | monitor id; incident `public_id` | host, service, tags, severity | incident state, ack | Richest service/host/tag linking |
| **Grafana / Prometheus Alertmanager** | contact-point / `webhook_config` (grouped, `send_resolved`) | Alertmanager-compatible read | token / self-host free | private | label-set `groupKey` | labels (service, instance, severity), `generatorURL` | firing/resolved; silence ≈ ack | Label grouping pre-reduces noise |
| **Opsgenie** | outbound webhook | Alert API | API key | private | alert id / alias | service, responder, P1–P5, tags | ack / close / assign | **EOL April 2027** — don't build net-new |
| **Better Stack / UptimeRobot / Pingdom** | webhooks (Pingdom paywalls low tier) | REST monitors/incidents | token; generous free tiers | private (+ status pages) | incident/monitor id | monitor, service, URL, status | up/down; BS full ack/resolve | Better Stack = polished incident model on free tier |
| **Statuspage / status.io / StatusGator** | webhook + component subs | status API + **public RSS/Atom** | public subscribe, no auth | **public** | incident / component id | service, impact, URL | incident lifecycle | **Best lens on third-party deps**; StatusGator = one integration covers hundreds of pages |
| **AWS** | **EventBridge** hub (CloudWatch Alarms, Health, CloudTrail) → SNS/API Destinations | DescribeAlarmHistory, Health, CloudTrail | IAM/SigV4; Health API needs Business/Ent support | private | alarm name; Health `eventArn` | service, region, account, ARN, severity | alarm state; Health open/closed | EventBridge→SNS HTTPS gives clean push; SigV4 heavier |
| **GCP** | Cloud Monitoring → **Pub/Sub (push *and* pull)** | Monitoring API (incidents) | service account / OAuth | private | policy / incident id | resource, project, severity | ack / close / snooze | **Pub/Sub pull** = no public endpoint needed (nice for personal ingester) |
| **Azure** | Monitor action groups → webhook / Event Grid | Alerts Mgmt REST | Entra ID / key | private | alert / event id | resource, RG, Sev0–4, signal | **New / Acknowledged / Closed (first-class)** | Explicit Acknowledged state = cleanest seen/acted mapping |
| **Cloudflare** | Notifications → webhook (HMAC) | Alerting API | API token | private (+ some public) | notification/policy id | zone, account, ASN, event type | none native | Edge/network + origin-health signals |
| **New Relic / Honeycomb** | Workflows / Triggers → webhook | NerdGraph / Triggers API | user API key; free tiers | private | issue id / trigger/SLO id | entity, policy, condition, priority / SLO | created/ack/closed; burn-alert state | NR issue correlation = dedup; SLO burn = high-signal priority |
| **Argo CD / Kubernetes events** | argocd-notifications webhook / exporter required | Argo API / `/events` watch | in-cluster / SA token | private | app+revision / object uid+reason | app, cluster, namespace, status | sync/health state | K8s raw events very noisy → only valuable post-filter |

**Top picks:** **PagerDuty** + **Sentry** (cleanest lifecycle + dedup, both map straight
onto §14), **Statuspage family / StatusGator** (uniquely *public*, no-auth, best
third-party-dependency lens), **Datadog** (richest linking), and **Better Stack** or
**GCP via Pub/Sub** as the easiest self-serve ingestion. Across all of these the
ack→resolve lifecycle is a natural §14 engagement source and severity/priority is a strong
§8.3 ranking signal.

---

## 9. Generic ingestion & read-later (the high-leverage meta-patterns)

> These give the most coverage-per-unit-effort in the whole backlog — build the top three
> *before* most bespoke connectors.

| Source / Pattern | Push | Pull | Auth/cost gate | Scope | content_kind | group_key | Engagement | Notes |
|---|---|---|---|---|---|---|---|---|
| **Generic inbound webhook** (unique POST URL) | ✅ native | — | self-issued secret; free | either | anything | caller `id` else payload hash | none | **The universal escape hatch.** Thin envelope (`{title,body,url,author,ts,source,tags[]}`) + per-integration JSONPath/jq mapping. Highest-leverage primitive in the system |
| **Zapier / Make / IFTTT / n8n / Pipedream** | ✅ they POST us | n8n/Pipedream can poll | their tier (n8n/Pipedream self-host = free); *they* hold the source OAuth | mostly private | anything | per-Zap mapping | usually lost in transit | Fan thousands of apps into one webhook; connector cost lives on their platform. Long-tail connector factory |
| **WebSub / PubSubHubbub** | ✅ hub POSTs on publish | fallback poll | free | public | underlying feed | feed item `id`/`guid` | none | Turns our existing RSS/Atom polling into **real-time push** for WordPress, Medium, Blogger, Mastodon, many news sites — near-zero marginal work |
| **JSON Feed 1.1** | — | ✅ poll | none | public | article/note | item `id` (stable) | none | Cleaner than XML; same fetch path as RSS. Treat as first-class equal |
| **ActivityPub** (generic fediverse) | ✅ inbox delivery | outbox pull | run an actor (HTTP sigs); free | public (+ followers-only) | note/article/video | activity/object URI | likes/boosts as activities | **One consumer actor follows any fediverse account** (Mastodon, Lemmy, PeerTube, WriteFreely, Pixelfed). Generalizes the Mastodon work |
| **Email-to-ingest alias** | ✅ inbound SMTP | — | free; alias is the secret | private | article/newsletter/notification | `Message-ID` + `References` | derive yourself | Near-universal fallback (anything that can email feeds us). Detail in §4 |
| **Webmention / IndieWeb** | ✅ sender POSTs | verify by fetch | free (or webmention.io) | public | reply/like/mention | source URL | none | Inbound signal when anyone links your URLs; only useful if user publishes. Niche |
| **Scraping-as-a-service** (Firecrawl, Apify, changedetection.io) | ✅ change webhooks | ✅ crawl | paid credits or self-host | public | HTML→Markdown | URL + content-hash | none | **Last resort** for no-API sites. **ToS/legal caveat** — gate behind per-site allowlist, respect robots |
| **Readwise / Reader** | ✅ native webhooks | ✅ REST | token (subscription); 240 rpm | private | highlights, saved docs | document/highlight id | **`reading_progress`, location (new/later/archive)** | Richest read-later signal; itself aggregates RSS/newsletters/social → a meta-source |
| **Wallabag** | via your glue | ✅ REST | OAuth2; **self-host, free** | private | articles + annotations | entry id | **`is_read`, archived, starred** | Privacy-respecting Pocket replacement; you own the data |
| **Instapaper / Raindrop.io** | via Zapier/wrappers | ✅ REST | OAuth (Raindrop has official **MCP server**) | private (+ public collections) | saved articles, bookmarks | item id | Instapaper: read/unread/progress | Reliable read-later atoms; lower velocity |
| **Hypothesis** (annotations) | — | ✅ REST + search | API token; free | public + private | web annotations | annotation id | annotation *is* the engagement | Surfaces what you marked up, keyed to the URL |
| **Browser history** (local) | — | ✅ read local SQLite | local file access; free | private (very) | visited URLs | URL + visit ts | dwell/visit count = implicit | **Strongest private-intent signal** (what you *read*, not saved). Needs a local agent; keep on-device |
| **Podcast 2.0 namespace** | WebSub on feed | ✅ poll RSS | none | public | episodes | episode `<guid>` | — | Extends RSS we already ingest; **`<podcast:transcript>` turns audio into digestible text** |
| **MCP servers** | — (pull-oriented today) | ✅ via client | per-server auth | mostly private | server resources | per-server id | per-server | **Forward-looking:** >10k public servers in 2026; promising as a *single client abstraction* over many tools, but no standard push/subscribe yet → complements, doesn't replace, the webhook |
| **Pocket** | — | ❌ | — | — | — | — | — | **DEAD** — API removed Nov 2025, service ended. Do not build on it |

**Strategic note.** Three patterns dominate and should precede most bespoke connectors:
**(1) generic inbound webhook + JSON normalizer** (the keystone — Zapier/Make/IFTTT/n8n/
Pipedream then act as a free connector factory for the entire long tail); **(2) WebSub on
the existing RSS pipeline** (open web from poll → push for near-zero work, with JSON Feed
on the same path); **(3) one ActivityPub consumer actor** (the whole fediverse via one
protocol). For read-later, standardize on **Readwise Reader** (API-first, native webhooks,
richest read-state, itself a meta-aggregator) with **Wallabag** as the self-hosted
fallback. Reserve bespoke connectors for high-value, high-velocity *private* sources
unreachable any other way (the GitHub/Slack class already in scope).

---

## 10. Proposed prioritization (a backlog, not a commitment)

Ranked by **value ÷ friction**, mapped to where they slot in:

**Tier 0 — generic primitives (build first; each unlocks many sources):**
- Generic inbound webhook + JSON normalizer envelope
- WebSub + JSON Feed on the existing RSS path
- ActivityPub consumer actor (generalizes Mastodon)
- Email-alias / inbound-parse (CASA-free email value)

**Tier 1 — high value, low friction, clean fit:**
- **Calendar** via ICS/CalDAV (new "upcoming" digest section) → Google/MS Graph Calendar for push+RSVP
- **Sentry**, **Linear**, **Dependabot**, **PagerDuty** (dev/ops; rich engagement + priority)
- **Hacker News** + the broad RSS content layer (YouTube/WebSub, podcasts via iTunes→RSS, Medium, arXiv, Lobsters)
- **Package registries** (npm `_changes`, NuGet catalog, Go index, PyPI/crates RSS) filtered against the user's manifests
- **Statuspage family / StatusGator** (public, no-auth, third-party-dependency lens)
- **Microsoft Graph mail** (whole-inbox, no CASA)

**Tier 2 — valuable, moderate friction or narrower audience:**
- **Matrix / Zulip** (chat with native read-state), **MS Teams** (now unmetered)
- **GitLab / Jira / Confluence** (where the team lives there)
- **Stripe / Lemon Squeezy / Shopify** (founder/solopreneur revenue events — see §11)
- **Twitch EventSub**, **Bluesky/Mastodon** (already planned), **Reddit** (personal/non-commercial)
- **Datadog / Grafana / cloud alerting**, **Readwise Reader**
- PM/docs (Todoist, Airtable, Asana, Notion-class) via signal→hydrate

**Tier 3 — niche, heavy, or audience-specific:**
- **Telegram MTProto** (real identity, but ban risk), **generic IMAP** (cred custody)
- **Salesforce / HubSpot / Zendesk / Intercom** (sales/support teams)
- **Plaid** (cashflow; heaviest compliance, but 2026 Trial plan helps), **QuickBooks/Xero**
- **Scraping-as-a-service** (ToS-gated), **browser history** (needs a local agent), **MCP** (track)

**Deferred / blocked by policy (don't invest until the gate moves):**
- **Twitter/X** (per-read billing), **LinkedIn** (closed member-feed permission),
  **Google Scholar** (no API; scraping forbidden), **Instagram/Facebook** (only for owned accounts),
  **Gmail** (CASA — and largely obviated by the email-alias pattern), **Opsgenie** (EOL 2027),
  **ProtonMail/iCloud** (no usable cloud API).

---

## 11. Audience note — who each family serves

The catalog spans several user archetypes; we likely won't want all of them at once,
and the source mix should follow the target user:

- **Developers / eng teams:** GitHub (v1), GitLab, Linear/Jira, Sentry, Dependabot, CI/CD,
  package registries, Statuspage, PagerDuty.
- **Indie founders / solopreneurs:** Stripe, Lemon Squeezy/Paddle, Shopify, GitHub Sponsors/
  Patreon/Open Collective, Plaid, Crisp/Help Scout.
- **Sales / RevOps teams:** Salesforce, HubSpot, Pipedrive.
- **Support teams:** Zendesk, Intercom, Front, Help Scout, Freshdesk.
- **Knowledge workers / everyone:** Calendar, email (alias/Graph), newsletters/RSS, chat,
  read-later, social/content.

---

## 12. Design implications worth folding back into the main docs

A few things this research surfaced that affect the *core* design, not just the source list:

3. **The §14 engagement feature has natural suppliers.** Ack→resolve (ops), `isRead`
   (email/Graph), read receipts (Matrix/Zulip), alert state — all map onto seen/acted. The
   table in §14 of the design doc could be extended with these.
4. **`content_kind` taxonomy gets exercised harder.** Newsletters and YouTube/podcasts are
   clearly `longform`; ops alerts and registry publishes are `announcement`; chat is
   `message`. The open question (§15) about where GitHub-release-with-long-notes sits
   recurs across many sources — a shared rule would help.
5. **The generic-webhook envelope is itself a mini canonical-event contract.** Designing it
   well (§9) is essentially extending the `Event` model to untrusted external callers —
   same scope-assignment and SSRF/auth concerns as any connector (§12 of the design doc).

---

*All API details are a 2026-06 research snapshot. Re-verify push/pull mechanics, auth
scopes, rate limits, and pricing against current vendor docs before committing any connector
to the build.*
