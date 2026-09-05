# X Bookmark Contract

## Current Surfaces

Thoth currently captures bookmark activity through three places:

- `userscript/thoth_capture.user.js` emits live bookmark payloads from the browser.
- `thoth_api.py` accepts those payloads at `/api/bookmark`, persists them to the durable queue, and processes them.
- `thoth.py` still has a legacy bookmark download path that replays cached GraphQL data from local files.

The contract for the X API upgrade is to keep the userscript as the low-latency live path, add an authenticated X API sync path for mobile/backfill coverage, and keep the payload shape consistent across both.

## Canonical Payload

Bookmark producers must provide:

- `tweet_id`
- `source`
- `timestamp`

Optional fields:

- `tweet_data`
- `graphql_response`
- `graphql_cache_file`
- `force`

## Rules

- `tweet_id` must be numeric.
- `source` must be explicit and non-empty.
- `timestamp` must be present before the payload is persisted.
- If `graphql_response` is present, it must be cached separately and the durable payload must keep only the filename reference.
- The durable queue stores canonical bookmark metadata, not raw GraphQL blobs.

## Upgrade Contract

The future X API sync path must emit the same canonical payload shape as the userscript path. It can differ in source tags, but it must not introduce a parallel storage format or a separate queue contract.

## Recovering incomplete API imports

`POST /api/x-api/bookmarks/sync` supports an explicit historical scan:

```json
{
  "max_results": 50,
  "max_pages": 20,
  "resume_from_checkpoint": false,
  "full_scan": true
}
```

Ordinary incremental runs stop on a checkpoint-known ID. Full scans skip that
ID and continue through subsequent items/pages, without re-emitting known IDs.
`resume_from_checkpoint: false` starts from the newest page; `true` continues a
saved pagination cursor. Inspect `backfill.checkpoint.pagination_token` to see
whether the page budget stopped a scan before the end.

Historical scans preserve existing bookmark and ingestion jobs, including review
states, rather than implicitly retrying them. The bounded checkpoint retains the
newest-page boundary during long scans so subsequent incremental runs can stop
at recent known bookmarks. The Settings result distinguishes newly fetched
payloads from jobs queued for background processing; neither means the resulting
notes have all finished publication.

A checkpoint records fetched/delivered IDs, **not successful vault publication**.
Failed queue entries need an explicit retry after their cause is fixed. IDs in
an old checkpoint but absent from durable queues require reconciliation against
the original captures; do not delete the entire checkpoint or reprocess already
completed notes blindly.

On purrsephone, 2026-09-05, a 100-item request returned 99 posts without a cursor,
while 50-item requests returned multiple pages with continuation cursors. The
deployment now uses 50 as a measured workaround, not an undocumented API limit.
X documents a per-page range of 1–100 and `meta.next_token`/`pagination_token` for
continuation: [Get Bookmarks](https://docs.x.com/x-api/users/get-bookmarks).
