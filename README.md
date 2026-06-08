## Data Flow

### `Scope`

TDB

### `Inbox`

WebHook endpoints and CRON scrape jobs feed into this queue. 
The queue is periodically emptied by an async ingest job that normalizes and deduplicates
the raw events into _canicalized_ `Event`s.

### `Event`

A common event format that represents an event in time.

## `Cluster`

Events related to the same underlying topic are aggregated into a cluster. Cluster processing happens in 2 phases:
1. Per-scope: `group` -> `link` -> `signals` events are grouped, cross-cluster links are resolved and each cluster is given a general `salience`(how important is this to begin with) score
2. Per-user: `gate` -> `ranke` -> `classify` -> `inhibit` clusters are scored and sorted by their relevance for the particular user. This step takes takes user feedback and preferences into account. The `N` most relevant clusters are getting promoted to `Story` or `Note`.

## `Story`

Stories are rich and substantive. They represent complex proceedings that may span many sources (e.g. a security incident reponse with slack messages, github issues, PRs, commits, emails, etc.) or one big source (e.g. a published video or blog post). 

Stories are rendered with a headline, short summary, and timeline of its constituent events.

## `Note`

Note are small but highly relevant. They represent events that do not warrant a headline, summary and timeline but are still important to flag. Examples: A band published a new album, a library published new release, an online order is shipped.

Notes are rendered in a compact format with one or two sentences max.
