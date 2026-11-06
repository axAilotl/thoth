# Cissa-managed Thoth runtime

`python docker/managed.py /managed/settings.json` serves only a Unix socket.
It never imports the standalone API or starts collector/archivist schedules.
Cissa owns admission, leases, source connections and credentials; Hermes cannot
call this service. Deploy one process/container, account root and SQLite store
per account/archive. Do not reuse the home Thoth database, personal vault or
credentials. The managed API exposes no filesystem paths, model credentials,
arbitrary connector entrypoints, shell commands or administrative endpoints.

Build the existing Dockerfile on CI with `THOTH_BUILD_REVISION` equal to the
committed source revision. Runtime settings must match the baked `BUILD_REVISION`;
an unversioned image is rejected. Deploy `compose.managed.yaml` with a digest-pinned
image and explicit CPU, RAM, PID, tmpfs, logging and stop-grace limits. All policy
and storage budgets are in the supplied settings/config files. `config.json`
is loaded explicitly without merging local configuration or environment secrets.
The example settings are templates, not an activated deployment.

The socket directory is shared ONLY with the Cissa-managed mTLS proxy, with its
group restricted to those two services. There are no published TCP ports. The
proxy authenticates the Cissa dispatcher service identity, rejects Hermes, and
**strips and replaces** `X-Cissa-Managed-Peer`. The ASGI app rejects TCP peers,
missing/duplicate/mismatched headers. The header alone is not authentication;
the protected socket plus verified proxy identity are required. Cissa separately
checks account/integration grants before sending any request. Access logs are off.

## Request and response contract

Every POST requires an exact JSON envelope (extra fields rejected), with the
unchanged `run` DTO in [managed-collection.md](managed-collection.md), and a
`lease_expires_at` UTC offset timestamp in the protected outer envelope. The
expiry is NOT part of the frozen run DTO or idempotency fingerprint. It must be
future and within `max_lease_seconds`. Account, archive and connector version
must match this runtime. Cissa must use the CURRENT lease; cancelled/revoked
authority must never be renewed or sent. Runtime deadlines bound execution to
the lesser of remaining lease lifetime and `execution_timeout_seconds`.

| Endpoint | Additional envelope fields | Successful JSON body |
| --- | --- | --- |
| `POST /v1/collections/execute` | `credentials`: `{}` or `{"token":"…"}` | Inner collection result, including `managed_run`, `status`, `coverage`, `artifacts`, windows and resume token; not the AgentSurfaceService history wrapper |
| `POST /v1/collections/receipt` | none | Same durable inner result for this exact run/input fingerprint and current lease generation |
| `POST /v1/collections/artifacts` | `artifact_ids`: unique list of IDs in that receipt | JSON list of ExternalDocument objects |
| `GET /health` | no body; same trusted peer required | `{"status":"ok","connector_version":"…"}` |

Artifact documents have `external_id`, `source_uri`, `title`, `body_text`,
`content_sha256` (UTF-8 body hash), `source_updated_at`, and `metadata` containing
`capture_content_hash` plus `run_id`. The artifact receipt hashes raw captured
content; the body hash covers the derived text Cissa imports. Reads verify the
durable receipt, input fingerprint, identity, lease, ID membership and current
raw hash. There is no arbitrary-path read endpoint. Missing or oversized evidence
fails explicitly; text is never silently truncated. The Cissa importer verifies
the body hashes and exact artifact set before committing its source revisions.

Responses other than 200 are `{"error":{"code":"safe_code"}}`, never provider
errors, input values, tokens or paths:

| Status | Meaning / examples |
| --- | --- |
| 403 | untrusted peer, wrong account, expired lease, unmanaged connector, artifact outside receipt, receipt lease/input mismatch |
| 404 | unknown route or missing persisted artifact |
| 409 | runtime busy, receipt unavailable/running, changed artifact revision, connector version mismatch |
| 413 | request bytes or artifact text budget exceeded |
| 422 | malformed JSON/envelope/run, invalid credentials/selection/artifact list |
| 502 | worker failed, invalid response or mismatched identity |
| 504 | worker deadline exceeded; process group terminated |
| 507 | configured account storage admission threshold exceeded |
| 500 | sanitized unexpected runtime error |

One collection executes at a time. The existing durable managed ledger also
locks the idempotency key across processes. Completed replays avoid source I/O,
refresh the receipt lease generation and retain captures/cursors across restart.
A lost response or timeout is an **uncertain outcome**, not permission to start
another job. Cissa queries `/receipt` using the original authority and reconciles
with its durable job; a deliberate retry uses the same immutable inputs/key and
a newer server-issued lease. Changed inputs with the same key are rejected.

Secrets exist only in the protected request and fixed worker stdin, not argv,
environment, stored config, ledger or artifact metadata. The worker receives an
empty credential environment and runs from the pinned source directory. Deadline
termination includes spawned YouTube/Play children. Source calls remain bounded
by configured collector pages/items/response sizes and request deadlines.

Storage checks run before/after collection and reject symlink escapes. They are
admission checks, **not a filesystem quota**; production must place the account
directory on a quota-limited volume for a hard disk ceiling. SQLite/raw evidence
persist together. The managed runtime currently uses SQLite capture metadata;
ambient PostgreSQL event-store configuration is rejected. Browser rendering and
wiki model/file-write transports require separately scoped Cissa capabilities;
no browser or model provider credentials are silently inherited. The wiki
compiler proposal/receipt interface is described in [managed-wiki.md](managed-wiki.md).
