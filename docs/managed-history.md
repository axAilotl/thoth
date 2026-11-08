# Selected company history

The managed `ai_history` connector uses the existing collection ledger, queue
and artifact-read endpoints. Cissa owns uploaded export bytes, source selection,
project permission and revocation. Thoth never scans a directory or reads a
personal chat database. The old committed AI export reader was a scaffold; this
implementation supports JSON ChatGPT conversations, Claude conversations, Grok
(`prod-grok-backend.json`) and an explicit normalized format. Cursor database
imports are not claimed.

`run.source_config.sources` is an exact selected list:

```json
{"provider":"chatgpt","project_id":"company","source_id":"session-id",
 "revision":"source-revision","operation":"upsert","content_sha256":"sha256-of-export-utf8"}
```

Provider is `chatgpt`, `claude`, `grok` or `normalized`. Project and source IDs are
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
times remain missing.

Only conversation text is kept. Tool calls, tool results (including ChatGPT
`tool` role messages), reasoning/thinking, images, attachments and unknown
non-text parts are dropped and counted; a message with no text left is skipped
(`empty_message`). The counts travel as `stripped_parts` in the document
metadata. A conversation with no text at all is rejected as
`history_no_text_content`; one odd part never rejects the conversation.

Grok exports are `{"conversations":[{"conversation":{id,title,create_time,
modify_time,leaf_response_id,…},"responses":[{"response":{_id,parent_response_id,
sender,message,create_time,model,…}}]}],…}`. `source_id` is `conversation.id`.
The current branch is followed from `leaf_response_id` through
`parent_response_id` (the newest response when there is no usable leaf). In
real exports most human messages name a parent outside the export; there the
walk follows the reverse of the parent's `children` link, and only when both
leave the export the latest earlier response (time, then human first, then id).
Validated on a real export: 688/688 conversations, 5,479 of 5,504 responses kept
(23 empty, 2 abandoned).
`sender` (`human`/`assistant`, any case) is the role; `create_time` is Mongo
extended JSON (`{"$date":{"$numberLong":ms}}` or `{"$date":"ISO"}`).
`thinking_trace`, `steps`, `web_search_results`, `file_attachments`,
`card_attachments_json`, `generated_image_urls` and `xpost_ids` are stripped.
Grok's own workspace/project fields are not a Cissa project; the selected
source grant is. Source URIs are `https://grok.com/chat/<id>`. A whole
`prod-grok-backend.json` (about 12 MB for several hundred chats) exceeds the
default `max_history_export_bytes` and request budgets; an uploader sends a
trimmed export holding the selected conversations.

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

## Owner X archive (`x_archive`)

Same protected upload and digest binding as history; `history_exports` is
accepted for `ai_history` and `x_archive` only. Resources are
`x_archive:<archive_id>:<file>` where `file` is a selected data-file stem:
`tweets`, `tweets-part1`, `like`, `like-part1`, `note-tweet` (the uploader lists
parts from `data/manifest.js` `dataTypes.<type>.files`; a part too large for the
request budget may be re-sliced as `<stem>.<n>` with the same
`window.YTD.<name>.partN` global). Sources are
`{"archive_id","file","revision","operation":"upsert","content_sha256"}`; only
upsert exists, deletion is disconnecting the source. Direct messages and every
other archive section are refused (`x_archive_section_not_ingested`).

Each post becomes `x:tweet:<id>`, each like `x:like:<tweetId>` (likes carry
only id, expanded status URL and optional text), each long-form note
`x:note:<noteTweetId>`. Notes carry no tweet link; a note matches the post
created in the same second (longest shared prefix breaks ties) when that post is
in the same run, and replaces it. Upload `note-tweet` with the tweets parts.
Bodies expand `t.co` links. Metadata `x` holds kind, ids, author, time, urls and
`follow_on` candidates (`arxiv`, `youtube`, `github`, `x_post`, unresolved
`short_link`); nothing is fetched. Owner decision 2026-11-07: archive documents
are search-only (`x.search_only: true`): indexed, never enriched or used for
memory/task extraction. No per-item raw copy is written and the queue row keeps
one copy of the text plus metadata. A malformed record is
skipped and makes the file `partial` (`x_archive_item_rejected`).
`collection.max_x_archive_file_bytes` bounds one file and
`collection.max_x_archive_documents` bounds documents per run; the rest is
`limit_exhausted` with an `{"offset": n}` cursor.

## Managed X bookmarks (`x_api`)

Resource `x_api:bookmarks:<username>`. Cissa owns the OAuth token and is its
only refresher; it sends a fresh access token as `credentials.token`. Managed
runs never read, write or refresh `x_api_tokens.json`. The run checks that
`/2/users/me` matches the selected username (`x_api_account_mismatch`), pages
bookmarks within `collection.max_pages` and keeps the incremental checkpoint in
the account root. Each bookmark is `x:bookmark:<id>` with the same `x`
metadata as archive posts (not search-only).
