# Web Clipper Ingest Operator Notes

Use the Web Clipper pipeline when notes and attachments live under the explicit
allowlist configured in `sources.web_clipper`.

## Required Layout

- `paths.vault_dir` is the synced vault root.
- `paths.raw_dir` holds source captures.
- `paths.library_dir` holds staged attachments and generated library files.
- `paths.wiki_dir` holds the compiled wiki layer.
- `paths.system_dir` holds local-only state such as the database, auth files,
  logs, and temp staging.
- `sources.web_clipper.note_dirs` and `sources.web_clipper.attachment_dirs`
  must be explicit directories inside `paths.raw_dir`.

## Operator Flow

1. Run `python3 thoth.py web-clipper` to scan the allowlisted directories.
2. Run `python3 thoth.py ingest-queue` to drain pending `web_clipper` entries
   through the shared runtime.
3. Keep `python3 thoth_api.py` running if you want startup background workers to
   continue draining the queue automatically.

## Failure Rules

- Missing or misconfigured source directories fail closed.
- Notes without YAML frontmatter fail closed.
- Attachments are staged into `paths.library_dir` through `.thoth_system/tmp`
  before being published atomically.
- Source files remain intact; the collector does not mutate raw captures.
- Queue writes and staging failures are not ignored.

## Validation

- `tests/test_web_clipper_collector.py` covers note discovery, staging, queue
  handoff, and failure cases.
- `tests/test_ingestion_runtime.py` covers the shared runtime routing for
  `web_clipper` artifacts.
