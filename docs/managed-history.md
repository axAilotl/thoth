# Selected company history

The managed `ai_history` connector uses the existing collection ledger, queue
and artifact-read endpoints. Cissa owns uploaded export bytes, source selection,
project permission and revocation. Thoth never scans a directory or reads a
personal chat database. The old committed AI export reader was a scaffold; this
implementation supports JSON ChatGPT conversations, Claude conversations and an
explicit normalized format. Cursor database imports are not claimed.

`run.source_config.sources` is an exact selected list:

```json
{"provider":"chatgpt","project_id":"company","source_id":"session-id",
 "revision":"source-revision","operation":"upsert","content_sha256":"sha256-of-export-utf8"}
```

Provider is `chatgpt`, `claude` or `normalized`. Project and source IDs are
nonempty and cannot contain colons. `source_id` is the actual conversation ID
inside the export. For a deletion use `operation:delete` and omit content hash.
Resource IDs are `ai_history:<project_id>:<source_id>`; coverage uses those same
IDs. Source revisions are separate from the stable artifact/external ID
`history:<provider>:<project_id>:<source_id>`. Reusing one revision for changed
content is rejected. Updates upsert the same queue row, not another knowledge
identity. Explicit tombstones replace the current row; absence in a partial
export never implies deletion.

Only `POST /v1/collections/execute` additionally accepts `history_exports`:

```json
[{"resource_id":"ai_history:company:session-id","content":"JSON export text"}]
```

This separate protected field is accepted only for `ai_history`. It must match
the selected upserts exactly; deletes supply no bytes. The byte digest in the
run binds the upload. Export bytes never enter the run DTO, options, ledger,
logs or returned receipt. The parser reads only the selected session and, for
ChatGPT, walks `current_node` parents to retain the current branch. Unselected
sessions and abandoned branches are never persisted as raw evidence. Selected
roles, message IDs, text, times and model references are preserved. Missing
times remain missing. Unsupported multimodal content fails with coverage loss
instead of silently dropping evidence.

Normalized input is `{"id":"session-id","project_id":"company","title":"…",
"messages":[{"id":"m1","role":"user","text":"…","created_at":"UTC ISO time"}]}`.
An explicitly present mismatched project ID is rejected. Provider exports that
do not contain project identity rely on Cissa's selected source/project grant;
the parser cannot infer ownership from the words of a conversation.

Limits live in `collection.max_history_export_bytes`, `max_history_messages`,
`max_history_message_characters`, `max_items`, the runtime HTTP body budget and
artifact text budget. Exceeding a limit never produces a false complete receipt.

Artifact-read documents contain role/time-labeled body text and metadata:
`historical_evidence:true`, `authority:"none"`, `provider`, `project_id`,
`source_id`, `revision`, `operation`, plus capture hash/run ID. A delete document
has an empty body and `operation:delete`. **Cissa must translate those IDs to
`SyncBatch.deleted_external_ids`, never index the tombstone as a new note.**
These records cannot create jobs; their quoted user directives and assistant
suggestions are historical evidence. Raw prior captures follow account retention;
the managed runtime exposes only current receipt hashes and has no autonomous
history processing loop. Cissa revokes downstream search/graph/wiki source refs
when it applies a tombstone.
