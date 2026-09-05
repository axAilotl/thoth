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
  must be explicit directories inside `paths.vault_dir` (for example `Clippings`,
  `clippings`, `pdfs`, or `papers`). The collector does not scan the whole vault.

## Local PDF intake and optional summaries

Set `sources.web_clipper.queue_pdfs` to `true` to enqueue allowlisted PDFs into
the same artifact runtime as notes. Previously indexed PDFs with no queue row
are discovered too. Leave `sources.web_clipper.summarize` at its default `false`
for deterministic extraction with **no per-document LLM calls**.
Both opt-ins require JSON booleans: strings such as `"false"`, numbers, and null
are rejected before collection or model use.

PDF processing uses Poppler to extract text, preserves the original PDF, and
copies an explicitly headed Abstract section when a bounded section can be
identified. Missing abstracts are reported as `not_found`; image-only PDFs are
routed to review for OCR instead of producing imaginary summaries. Extracted
text and provenance live in the artifact's `custom_metadata.document_text`,
`document_extraction`, and `document_abstract`. The generated wiki links the
original PDF and labels source abstracts as extracted, not AI-generated.

Processing limits under `sources.web_clipper`:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `max_source_bytes` | 52428800 | Reject larger source files before reading them |
| `pdf_max_pages` | 40 | Extract only the first N PDF pages |
| `pdf_text_max_chars` | 500000 | Retained extracted text; independent of LLM input |
| `summary_max_chars` | 24000 | Optional LLM request character limit |

PDF extraction always records bounded coverage; it does not claim the entire
paper was read. Truncation is recorded, and the original remains accessible.

For deliberately requested AI summaries, enable `summarize` and configure the
existing `llm` summary task route. The runtime stores a separate
`custom_metadata.document_summary` with source SHA-256, provider/model, prompt
version, time, input limits, and generated Markdown. The full generated summary
appears in the wiki, with a partial-input warning when applicable. Source notes,
their raw capture/body, and PDFs are never rewritten. Model failures are queue
failures, not successful "summary unavailable" strings. Retries reuse a stored
derivative for the same checksum and limits.

## Operator Flow

1. Run `python3 thoth.py web-clipper` to scan the allowlisted directories.
2. Run `python3 thoth.py ingest-queue` to drain pending `web_clipper` entries
   through the shared runtime.
3. Keep `python3 thoth_api.py` running if you want startup background workers to
   continue draining the queue automatically.

The connector execution surface accepts `options.limit` for a bounded catch-up
batch. `WebClipperCollector.collect(limit=N)` selects only work that needs queueing
before applying connector budgets; unchanged files cannot starve later work.
Malformed or oversized sources are retained and reported in `last_scan_errors`.
Other valid selected files can queue before the scan reports aggregate failure.

## Failure Rules

- Missing or misconfigured source directories fail closed.
- Notes without YAML frontmatter fail closed.
- Attachments already at their managed vault path stay there; configured alternate
  managed destinations use the existing atomic staged-asset publisher.
- Source files remain intact; the collector does not mutate raw captures.
- Queue writes and staging failures are not ignored.
- File metadata is acknowledged only after successful queue handoff, allowing
  retry after queue failure. Changed processed or ordinary terminal-failed
  sources return to pending; pending and in-flight sources are not overwritten
  or acknowledged as indexed. Review decisions remain in effect.
- All path components must be non-symlinks; source path/extension allowlists and
  checksums are checked before and after extraction, and before publishing or
  reusing successful derivatives. Extracted PDF text is scanned
  for prompt-security findings before any optional LLM call.
- Newly created Web Clipper wiki slugs include a source-identity suffix, so
  unrelated documents with identical titles cannot overwrite one another.
  Existing canonical pages keep their recorded paths; no old pages are deleted.

## Validation

- `tests/test_web_clipper_collector.py` covers note discovery, staging, queue
  handoff, and failure cases.
- `tests/test_ingestion_runtime.py` covers the shared runtime routing for
  `web_clipper` artifacts.
- `tests/test_document_enrichment.py` covers deterministic abstract/full-text
  retention, opt-in summaries, provenance, source preservation, and security.
