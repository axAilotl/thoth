# File inbox

The opt-in `inbox` connector accepts a dump folder without publishing per-source
Markdown sidecars. Its manifest is discovered by the normal connector registry;
the existing scheduler and `connectors run inbox` execution surface run it.
There is no separate always-running filesystem daemon.

Example deployment settings (merge into existing configuration):

```json
{
  "sources": {
    "inbox": {
      "enabled": true,
      "directory": "/data/vault/inbox",
      "stable_seconds": 60,
      "consume": true,
      "max_files_per_run": 25,
      "max_source_bytes": 52428800,
      "max_text_chars": 500000,
      "pdf_max_pages": 40,
      "schedule": {
        "enabled": true,
        "interval_seconds": 60,
        "run_on_startup": true
      }
    }
  }
}
```

Create the configured inbox first. Both `enabled` and `consume` default to
`false`; a missing directory is a configuration error, not an alternate path.
Control state and archives require `paths.system_dir` outside the vault.
Add `documents` to your existing `sources.corpus_index.include_roots`; keep
`clippings` and `pdfs` there too. Do not replace other configured include roots.
Use the normal corpus-index schedule for keyword indexing and, if explicitly
enabled, embedding backfill. Intake extraction itself uses no language model.
The standard `sources.inbox.budgets` connector byte/file budgets also apply.

## Intake contract

Each file must have matching device/inode/size/mtime/ctime observations in two
scans separated by at least `stable_seconds`, and its mtime must be old enough.
Bytes are read from a bounded snapshot and checked again after extraction.
Observation and receipt records live under `inbox:file:*` in the metadata
database's existing `automation_state` table, not in Obsidian.

| Input | Managed destination | Local extracted evidence |
|---|---|---|
| `.md`, `.markdown` | `documents/` | Original UTF-8 text |
| `.txt` | `documents/` | Original UTF-8 text |
| `.pdf` | `pdfs/` | Poppler text, explicitly capped to configured page count |
| `.docx` | `documents/` | Main document body, not headers/comments/embedded objects |

Routing is by file format, not a guess that every PDF is a research paper.
Generic Markdown goes to `documents`, not `clippings`: Web Clipper's existing
strict parser expects clip-specific YAML frontmatter that ordinary notes lack.
Original bytes are unchanged. Names include a SHA-256 digest; identical content
reuses its queued identity without resetting processing/review state. Different
content never overwrites an existing destination. A filename's first captured
extension/destination wins when byte-identical inputs have different names.

Artifacts use the existing `markdown` capture lane with `source_type: inbox`.
Their source path, checksum, extracted text, extraction coverage and explicitly
headed abstract are retained in the queue/source metadata for CCF, retrieval and
topic compilation. DOCX body text is also available through the corpus index
without creating a companion Markdown file. An abstract is extracted when an
explicit heading and terminating section are found; it is not an AI summary.

Unsupported formats, malformed/empty documents, oversized extraction, and unsafe
paths stay in the inbox and appear in the existing ingestion Review interface.
Security findings go through the normal capture policy. No review-held,
rejected, blocked or failed queue entry is consumed. Changing the input creates
a fresh observation; merely rerunning a scan does not erase its review decision.

## Recoverable consumption

With `consume: false`, the inbox original stays in place and subsequent scans
reuse its receipt. With `consume: true`, THOTH first verifies all of these:

1. A complete, non-overwritten destination exists inside the managed vault.
2. The shared queue has persisted the artifact and its security metadata.
3. An exact byte copy exists under `<system_dir>/inbox/archive/<sha256>.<ext>`.
4. The source is still the revision that was captured.

Only then does THOTH claim and remove that unchanged inbox directory entry.
A replacement arriving at the original name is not removed. A crash during the
claim can leave a visibly named `*.thoth-preserved-*` file in the inbox; it remains
available and is surfaced for review on a later scan. The database receipt keeps
the original, managed destination and archive paths. To restore an input, copy
its archived bytes to a chosen inbox name; do not overwrite newer user content.
Keep the database and system archive in your normal THOTH backups.

Intake is serialized with a host file lock. It never watches its output folders
as its inbox. Existing Web Clipper and corpus reconciliation still handle files
manually added to managed roots; their schedules and source settings are not
rewritten by this connector. Polling catches changes missed while the server was
offline. For unusually slow/sparse file copying, increase `stable_seconds`.
