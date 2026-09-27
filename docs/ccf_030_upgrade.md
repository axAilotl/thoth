# CCF 0.3.0 upgrade

Thoth uses [CCF 0.3.0](../spec/ccf/0.3.0/CCF-0.3.0-SPEC.md) as metadata over
native sources. It replaces the retired 0.1.2/0.2.0 signed archive protocol.
The old protocol remains in Git history; the runtime has no compatibility path.

CCF records, links, blobs, conflicts, and peer acknowledgement counters live in
a separate SQLite database under the local system directory. Notes, PDFs,
recordings, native queue state, and wiki pages stay in their existing locations.
No signing keys, archive journals, policy catalogs, or Postgres server are
required for CCF. Thoth's existing content security and review gates still apply.

## Configuration

```json
{
  "database": {
    "ccf": {
      "enabled": true,
      "path": "ccf/metadata.sqlite3",
      "device": "homelab"
    }
  }
}
```

The path resolves under `paths.system_dir`. Remove the retired
`database.ccf_archive` object from operator config. Give each installation a
stable device name. The device name describes where new objects originated;
changing it does not rewrite existing object sources or IDs.

Captures, native review decisions, and wiki writes publish metadata through
`core.ccf_metadata` and `ccf.metadata`. Metadata failures are reported without
undoing native processing. Re-running backfill repairs missed metadata. Model
inferences remain `inferred`; only actual native confirmation can accept them.
Connector lanes are Thoth application metadata, not a closed CCF type registry.

## Back up and populate

1. Build and test the image before stopping the live service.
2. Stop the single live Thoth container. Copy the runtime config and system
   directory to a new private backup directory, preserving original files.
   Use SQLite's backup API for a verified standalone copy of `meta.db`.
3. Test the new image against copies of the runtime with automation disabled
   and source mounts read-only. Enable `database.ccf` only in the copied config.
4. Run the commands below on the copy. Inspect every reported backfill error;
   missing files do not invalidate metadata for the rest of the inventory.
5. Enable CCF in live operator config, start the validated image, then backfill
   and verify health, counts, export validation, and idempotent replay.

```bash
python thoth.py ccf status
python thoth.py ccf backfill
python thoth.py ccf export /private/path/ccf-export
python spec/ccf/0.3.0/tools/validate.py export /private/path/ccf-export
```

Backfill opens the native database read-only, enumerates every queue row and
indexed file, and imports available entity, semantic candidate, and wiki
metadata. It never runs an LLM, requeues work, accepts a review, or writes to
source files. It generates UUIDv7 IDs once and reuses them on later runs.

For a separate rehearsal database use
`python thoth.py ccf --store /private/path/rehearsal.sqlite3 backfill`.
Do not use an empty native database as evidence that the installation has no
data: check the configured path, including older doubled system directories.

## Portable exchange

`ccf export PATH` creates a new directory or `.zip` with `manifest.json`,
`records.jsonl`, `links.jsonl`, and `blobs.jsonl`. Exports contain metadata by
default. `--include-files` explicitly includes hash-verified local bytes from
configured content roots, or retained copies from earlier imports. Changed,
missing, or inaccessible files are reported; metadata and other files continue.

`ccf import PATH` accepts the same directory or ZIP layout. It preserves every
ID, source, unknown field, and location, applies the deterministic merge rule,
and keeps conflicts. Imported locations are never fetched or written to.
Included files are stored privately by content hash beside the metadata store.
Malformed objects are skipped with the exact line and reason. Hash mismatches
are retained and reported as warnings. An invalid manifest cannot identify a
valid export and is rejected before import.

`ccf changes PEER --limit 1000` returns unacknowledged objects plus a local
cursor. Only call `ccf ack PEER CURSOR` after that peer actually acknowledges
receipt. Reading or exporting changes does not acknowledge them. Selection
uses local change counters, never timestamps, so clock rollback cannot hide
updates. These are local commands; no unauthenticated network sync is exposed.

Commands print JSON. Exit code 1 means reported item errors or a failed
operation; valid neighboring items may already have completed. Exit code 0
may include warnings. Keep exports private, like the native content they describe.

## Rollback

Stop the new image before rollback. Restore the saved operator configuration
and runtime snapshot, then start the recorded previous image using the same
Compose files and mounts. Keep the new CCF store and reports separately for
diagnosis; do not point old code at changed runtime databases. Native source
files are not rewritten by this upgrade.

## Validation

```bash
python spec/ccf/0.3.0/tools/validate.py vectors
PYTHONPATH=. pytest tests/test_ccf_030_*.py
PYTHONPATH=. pytest tests/
python -m compileall -q ccf core collectors processors thoth.py thoth_api.py tests
```

Tests cover the published hash/merge vectors, revisions and conflicts,
acknowledgement isolation, partial imports, hostile file paths, source
preservation, real capture/review/wiki entrypoints, and idempotent backfill.
