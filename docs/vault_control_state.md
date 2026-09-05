# Reading vault and THOTH control state

Implemented under Beads epic `thoth-2j4q`.

The Obsidian vault is the source of record for document bytes and human-authored
text. One active THOTH runtime owns processing and relationship state. Obsidian
sync is not a database replication protocol.

## Storage contract

- `knowledge_vault/`: useful source documents, captures and transcripts.
- `knowledge_vault/inbox/`: optional unstructured input, reconciled by the inbox connector.
- `wiki/pages/`: synthesized human-readable articles with citations and explicit feedback.
- `paths.system_dir` and `database.path`, outside Obsidian: source records,
  extraction/abstract metadata, canonical associations, queues, search indexes,
  generated revisions, feedback state, publication metadata, logs and archives.

`wiki.publish_source_pages` defaults to `false`. Successful ingestion persists
source records without generating one Markdown wiki page per document. The
optional `true` setting explicitly requests source-note publication; those
source notes are still excluded from the topic-oriented wiki index and guarded
against overwriting user edits. It is not needed for corpus search or topic
compilation. The existing source/queue/canonical tables remain authoritative for
their respective responsibilities; `source_records` preserves the processed
artifact plus its derived metadata independently of a Markdown rendering.

Generated topic frontmatter contains only identity, kind, title and update time.
Full publication metadata and evidence manifests live in the database. Human
citations continue to link to actual source files. Maintenance logs are under
`paths.system_dir/wiki/log.md`, not appended into Obsidian on every job.

See [wiki publication and feedback](wiki_publication_feedback.md) for edit
protection, adoption, original feedback text and its lifecycle. New registered
connectors use the existing persistent scheduler; reconciliation works after
downtime and does not depend on receiving every filesystem notification.

## Existing generated source-page migration

Back up the database, configuration and vault first; stop application writers.
Check other synced clients for pending edits before applying the approved plan.

```sh
python -m core.vault_maintenance plan --obsidian-root /path/to/obsidian
python -m core.vault_maintenance apply --plan /outside/obsidian/plan.json --archive-root /outside/obsidian/archive
python -m core.vault_maintenance compact-topics --obsidian-root /path/to/obsidian --plan /outside/obsidian/topic-hashes.json --archive-root /outside/obsidian/archive
python -m core.vault_maintenance export
```

Save the plan output to an operator-controlled file outside Obsidian. Planning
is read-only. Applying rechecks identities, document hashes, source existence,
and references from other Markdown notes. Referenced records and files changed
since planning remain in place. Case-only source renames are reconciled only
when a unique name and the recorded checksum agree. Missing sources are not
silently discarded.

Eligible records are copied byte-for-byte to a content-addressed archive outside
Obsidian, verified, and retained with their complete metadata/document text in
the database before the generated file is removed. Originals are never moved by
this migration. Its removals can be undone from the archive and recorded original
paths. Old pages have no trustworthy edit baseline: the approved migration
preserves **all** their contents, including unknown fields and possible manual
annotations, rather than claiming it can infer authorship retroactively.

Existing topic pages require explicit, hash-checked baseline adoption; a file
with an existing baseline cannot be re-adopted to bypass an edit conflict.
During the deployment migration, full topic documents are archived before
metadata is condensed, and prose/citation links must be verified unchanged.
The `compact-topics` plan is a JSON object mapping Obsidian-relative page paths
to their approved SHA-256 hashes. Compaction preserves pending feedback; it does
not claim that a metadata cleanup researched or fulfilled a request.

## Backups and export

Back up the SQLite database consistently (online backup API or with writers
stopped) along with the THOTH control directory and your separate document
backups. Do not place a live SQLite file inside Obsidian sync. Per-source JSON
export uses the versioned `thoth.source-records/v1` envelope and retains complete
archived records. It is an inspectable control-state export, **not** a claim of
CCF capsule import/export compatibility. Existing CCF serializers retain their
own contracts; portable control state does not require Markdown sidecars.
