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
