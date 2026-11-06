# Managed wiki proposals

Cissa supplies `ManagedWikiContext` with account/archive/job/lease identity,
author, granted topic IDs, UTC source cutoff, account root, and byte budgets
(`max_source_bytes`, `max_total_source_bytes`). `source_manifest` maps normalized
Thoth source paths to Cissa SourceRefs: account/archive, stable `source_id`,
`revision`, `content_hash`, local snapshot `file_sha256`, `observed_at`, and URI.
Only granted, unchanged snapshots at/before the cutoff enter retrieval. Scoped
inventory does not prune other source records. It filters before file parsing or
model embedding. Sources are verified again after generation.

`expected_pages` maps each selected output path to `{revision, content_hash}`;
both are null for a new page. Human revisions that do not match the expected
hash produce a blocked result. Imported `06ChatGPT` sources are never output
targets. Cissa's writer remains responsible for selected-vault path policy,
managed sections, area INDEX rules, current source authorization, and atomic
revision comparison when applying content.

Construct a `ManagedModelGateway(identity=context.identity(), tasks=routes,
invoke=authenticated_cissa_callback)`. Every route specifies a model and
`max_input_characters`; generation routes also specify `max_tokens`. No provider
token, dotenv lookup, Pi invocation or provider SDK is used by this gateway.
The callback enforces Cissa's actual token/cost budget and current job authority.
The configured excerpt length and default standalone generation settings live
under `llm.prompts.archivist` in settings; explicit model settings take priority.

Call the existing `run_archivist_topics` with explicit config, project root,
layout, database, `managed_context` and `llm_interface=gateway`. Results may be
`proposed`, `blocked`, `failed`, `skipped` or `dry_run`. Topic failures do not hide
successful sibling proposals. Each proposal contains identity, `proposal_id`,
relative path, author, expected revision/hash, proposed content/hash, cutoff,
source manifest and existing compiler provenance metadata.

Proposal generation creates no wiki scaffold, page, index or log and does not
mark the topic successfully compiled. Cissa applies the proposal, then calls
`acknowledge_wiki_proposal(context=..., layout=..., db=..., proposal_id=...,
receipt=...)`. The protected receipt includes the same identity, proposal ID,
`status: applied`, new `revision` and exact `content_hash`. Only a matching
receipt with still-authorized sources advances the existing dirty-check state.
Conflict/rejection leaves the proposal retryable and the success baseline
untouched. Thoth never uses this acknowledgement to write vault files.

## Protected runtime transport

Enable `managed_wiki.enabled` in the explicit managed config and configure its
private `callback_socket`, timeout/response-byte limit, maximum source/topic
counts and page-byte limit. The socket is served by a reverse proxy with Thoth's
distinct mTLS service identity; no model token enters the runtime. These routes
use the same protected UDS peer admission as managed collection.

`POST /v1/wiki/propose` takes an exact JSON envelope:

* `context`: all `ManagedWikiContext` fields above **except `account_root`**.
  The account root is bound by runtime configuration, never supplied by callers.
* `source_documents`: map of normalized relative vault paths to UTF-8 Markdown
  strings, exactly matching `context.source_manifest`. Each string must match
  its `file_sha256`, within per-source and total byte budgets. A source may carry
  a separate original `content_hash` if its Markdown snapshot is a derivative.
* `page_snapshots`: map of output paths to existing whole-page UTF-8 text or
  `null`, exactly matching `context.expected_pages`. Hashes must match; a new
  page requires both expected revision and hash to be null.
* `topics`: the existing topic registry object (`version`, `defaults`, `topics`),
  with exactly the granted topic IDs and output paths. Existing root selection,
  retrieval and citation behavior is reused. Configure retrieval limits in these
  topic definitions; do not expand model authority to discover arbitrary roots.
* `model_routes`: task routes for `ManagedModelGateway`, including allowed model,
  input-character and output-token budgets; optional embedding route is explicit.
* `lease_expires_at`: protected UTC expiry, outside the context.

Thoth stages only those verified snapshots beneath an isolated job directory
inside its account storage. Page snapshots are input copies, never the user's
vault. It invokes the existing compiler without a writer or scheduler. The
response is the existing `serialize_archivist_run` shape: `status`, `results`
with topic statuses/proposals, and summary counts. A durable exact-request replay
within the same lease returns the previous result without another model call.
Changed inputs under the same job/lease are rejected. Older lease generations
cannot overwrite newer jobs. Collection and wiki execution share serial admission.

The fixed callback is `POST /v1/integrations/thoth/model` via the configured UDS.
Generation request:

```json
{"identity":{"account_id":"…","archive_id":"…","job_id":"…","lease_generation":1},
 "lease_expires_at":"UTC ISO timestamp","task":"archivist","model":"approved-model",
 "prompt":"…","system_prompt":"…","max_tokens":16384,"temperature":0.2}
```

Embedding replaces the prompt/output fields with `texts:["…"]` and uses the
explicit embedding task/model. Response is `{content:string,error?:string}` or
`{vectors:[[number]],error?:string}`. There is no caller-controlled endpoint or
credential. Cissa must validate the peer against its durable compilation
invocation, parent job, current source/lease authority, route and remaining
budget; the identity in the body alone is insufficient. Callback refusals and
oversized responses are sanitized failures. Lease checks run before callbacks
and before accepting the result; the compiler has an overall configured deadline.

`POST /v1/wiki/ack` takes `{context, proposal_id, receipt, lease_expires_at}`.
Context must match the staged job and lease exactly. The Cissa writer receipt
contains `operation_id`, `job_id`, `status`, `path`, `revision`, `content_hash`,
`published_content_hash`, `current_revision`, `sync_status`, and `source_refs`.
`content_hash` is the exact proposal text hash, while `revision` and
`published_content_hash` describe the whole published page (which can include a
managed-section/provenance wrapper). The transport validates job/path and binds
account/archive/lease/proposal ID before the existing acknowledgement function
checks content hash and authorized source snapshots. `sync_status:pending` may
still acknowledge a durable local publication; it does not claim cloud sync.
Response is `{status:"acknowledged",proposal_id,revision}` or `unapplied` for a
conflict/refusal. No acknowledgement writes vault files.

Standard managed runtime errors apply, plus 503 for a disabled/misconfigured
wiki transport, 413 for snapshot/model response budgets, 409 for hash/input
conflicts and 403 for scope/lease/receipt mismatches. Topic failures remain in
the successful HTTP response as explicit failed/blocked topic outcomes.
