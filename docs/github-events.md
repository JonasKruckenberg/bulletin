# GitHub Event Surface Map

**Status:** Reference (salvaged from the M2 build notes).
**Companion to:** `technical-architecture.md` §3A (auth/webhooks), `system-design.md` §7 (ingress),
`data-sources.md` (the broader source backlog).

This is the reference map of GitHub's event surface, used to scope ingestion *deliberately*. M2
ships **only the timeline collaboration set** (§2 below); everything else is a menu for later
milestones. `event_map` (`crates/core/src/ingest/github/event_map.rs`) captures any unknown webhook
type generically, so nothing breaks if an unscoped type arrives — but **rich classification** and
**poll reconciliation** of a non-timeline signal are per-signal work.

> Catalog is **as of early 2026** — GitHub adds event types regularly; **re-verify against the live
> "Webhook events and payloads" + "REST Activity/Events" docs when configuring the App.**

## 1. The two intakes have different coverage (the crux)

- **Activity timeline** (`GET /repos/{o}/{r}/events`, `/orgs/{org}/events`, `/users/{u}/events`,
  `/networks/{o}/{r}/events`) — what the poll backstop reads. Carries only the **~17 timeline types**
  below. Public events only for public actors; an installation token widens repo visibility.
- **Webhooks** — the full **~70+ type** catalog (header `X-GitHub-Event`). Most types are **not on
  the timeline**, so they arrive *only* by webhook.
- **Resource REST endpoints** — for non-timeline signals (alerts, checks, runs, deployments…),
  each has its own list endpoint. **These are what a reconciliation poll must hit** to keep the
  "poll is the correctness floor; webhooks are freshness" invariant (technical-architecture.md §5.4)
  for that signal.

**Consequence:** adding a non-timeline signal is per-signal work — subscribe the webhook type **and**
add a paired REST fetcher to `GithubConnection::poll`, **and** request the App permission.

## 2. Timeline types (poll-visible today via `/events`)

`CommitCommentEvent`, `CreateEvent` (branch/tag), `DeleteEvent`, `ForkEvent`, `GollumEvent` (wiki),
`IssueCommentEvent`, `IssuesEvent`, `MemberEvent`, `PublicEvent` (repo made public),
`PullRequestEvent`, `PullRequestReviewEvent`, `PullRequestReviewCommentEvent`,
`PullRequestReviewThreadEvent`, `PushEvent`, `ReleaseEvent`, `SponsorshipEvent`, `WatchEvent`.
M2 maps Issues/PR/Release/Push/comments richly; the rest fall through to the generic capture.

## 3. Webhook catalog by scope (T = also on the timeline → poll-visible without a new endpoint)

**Repo — collaboration & content:** `push`(T) · `pull_request`(T) · `pull_request_review`(T) ·
`pull_request_review_comment`(T) · `pull_request_review_thread`(T) · `issues`(T) · `issue_comment`(T) ·
`sub_issues` · `commit_comment`(T) · `create`(T) · `delete`(T) · `fork`(T) · `gollum`(T) · `release`(T) ·
`discussion` · `discussion_comment` · `label` · `milestone` · `watch`(T) · `star` · `public`(T) ·
`member`(T) · `page_build` · `status`.

**Repo — CI/CD & automation (webhook-only; reconcile via Actions/Checks/Deployments REST):**
`check_run` · `check_suite` · `workflow_run` · `workflow_job` · `workflow_dispatch` ·
`repository_dispatch` · `deployment` · `deployment_status` · `deployment_review` ·
`deployment_protection_rule` · `merge_group` · `registry_package` · `package`.

**Repo — security & policy (webhook-only; reconcile via the alert REST endpoints in §4):**
`dependabot_alert` · `code_scanning_alert` · `secret_scanning_alert` ·
`secret_scanning_alert_location` · `secret_scanning_scan` · `security_advisory` ·
`security_and_analysis` · `repository_vulnerability_alert` (deprecated → `dependabot_alert`) ·
`branch_protection_rule` · `branch_protection_configuration` · `repository_ruleset` · `deploy_key`.

**Repo — admin/meta (webhook-only):** `repository` (created/deleted/archived/renamed/transferred/
publicized/privatized) · `repository_import` · `repository_ruleset` · `meta` (hook deleted) ·
`team_add` · `custom_property_values`.

**Project management:** `projects_v2` · `projects_v2_item` · `projects_v2_status_update` (org) ·
`project`/`project_card`/`project_column` (classic, deprecated).

**Org-level:** `organization` (member added/removed/renamed/deleted) · `membership` · `team` ·
`org_block` · `personal_access_token_request` · `custom_property`/`custom_property_values` ·
`repository` (org repos) · `projects_v2*` · `repository_ruleset`.

**App / installation lifecycle (webhook-only — must drive `connection.status`):**
`installation` (created/deleted/suspend/unsuspend/new_permissions_accepted) ·
`installation_repositories` (added/removed) · `installation_target` · `github_app_authorization`.

**Account / marketplace / sponsors / global:** `marketplace_purchase` · `sponsorship`(T) ·
`security_advisory` (global GitHub Advisory DB feed).

**Enterprise-level:** enterprise webhooks receive most repo/org events across all orgs plus
enterprise-scoped security (`dependabot_alert`/`secret_scanning_alert` enterprise-wide), `audit`,
`organization`, `team`, `membership`, `repository`.

## 4. REST endpoints for poll reconciliation of non-timeline signals (high value)

| Signal | Repo | Org / Enterprise | App permission (read) |
|---|---|---|---|
| Dependabot alerts | `/repos/{o}/{r}/dependabot/alerts` | `/orgs/{org}/dependabot/alerts`, enterprise | Dependabot alerts |
| Code scanning | `/repos/{o}/{r}/code-scanning/alerts` | `/orgs/{org}/code-scanning/alerts` | Code scanning alerts |
| Secret scanning | `/repos/{o}/{r}/secret-scanning/alerts` | `/orgs/{org}/…`, enterprise | Secret scanning alerts |
| Repo advisories | `/repos/{o}/{r}/security-advisories` | — | Repo advisories |
| Global advisories | `/advisories` (GitHub Advisory DB) | — | none (public) |
| Workflow runs | `/repos/{o}/{r}/actions/runs` | — | Actions |
| Check runs/suites | `/repos/{o}/{r}/commits/{ref}/check-runs` | — | Checks |
| Deployments | `/repos/{o}/{r}/deployments` (+ statuses) | — | Deployments |
| Commit statuses | `/repos/{o}/{r}/commits/{ref}/statuses` | — | Commit statuses / Contents |
| Packages | user/org packages | `/orgs/{org}/packages` | Packages |
| Discussions | GraphQL only (no REST list) | — | Discussions |

## 5. The shipped tiering — "timeline only"

**Decision (M2):** ingest **only the timeline collaboration set** (§2 — the types the poll already
reads, rich-mapped in `event_map`). **All non-timeline signals are deferred**: security alerts
(Dependabot/code-scanning/secret-scanning/advisories), CI/CD (`workflow_run`/`check_*`/deployments/
status), org/admin/meta, packages, projects_v2, discussions. So **no extra REST reconciliation
endpoints** and **no extra App permissions** beyond the `/events` walk.

**Webhook subscriptions:** the timeline-corresponding content events (`issues`, `issue_comment`,
`pull_request`, `pull_request_review`, `pull_request_review_comment`, `pull_request_review_thread`,
`push`, `release`, `commit_comment`, `create`, `delete`, `fork`, `gollum`, `member`, `public`,
`watch`) **plus the installation-lifecycle events** (`installation`, `installation_repositories`,
`installation_target`, `github_app_authorization`) — the latter are control-plane and drive
`connection.status`, not digest content, so they stay despite "timeline only." Any other type that
arrives is captured generically by `event_map` (harmless) but not subscribed or reconciled.

**When the deferred signals land (future milestone):** for each, (1) subscribe its webhook type,
(2) add its REST list endpoint (§4) to `GithubConnection::poll` for reconciliation parity, (3)
request the App permission, (4) rich-map it in `event_map`. §3/§4 are the menu.

**Scope mapping:** private-repo signals → `Private(owner)`; org/account-level meta → owner-private
or treated as administrative; global advisories → `Public`. (`finalize` owns this.)
