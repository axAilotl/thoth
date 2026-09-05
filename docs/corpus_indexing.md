# Corpus indexing and evidence search

The `corpus_index` connector independently maintains the existing SQLite source
inventory and FTS index. It reads only explicitly configured roots relative to
THOTH's vault, raw, or library scopes. It does not rewrite originals, generate
summaries, or require a topic definition. Enable only after checking the root
allowlist and the configured embedding provider's data-processing policy.
Supported inventory formats are Markdown, plain text, and text-extractable PDF;
this does not claim Office-document or image OCR coverage.

```json
{
  "sources": {
    "corpus_index": {
      "enabled": true,
      "include_roots": ["papers", "clippings", "Clippings", "pdfs"],
      "exclude_roots": [],
      "embeddings_enabled": true,
      "max_new_embeddings_per_run": 128,
      "schedule": {
        "enabled": true,
        "interval_seconds": 900,
        "run_on_startup": false
      }
    }
  }
}
```

Use the existing connector plan/run API or `run_connector` agent tool with
`connector_name: "corpus_index"`. A run override may set
`max_new_embeddings_per_run`. Generic connector file/byte/token budgets apply;
an allowlist larger than the defaults requires an explicit
`connectors.budgets.per_connector.corpus_index` budget. The whole file allowlist
is preflighted before inventory writes. Keyword inventory reuses unchanged
files; embedding work is separately bounded and resumes at uncached documents.

The embedding budget counts actual text inputs, not documents. All extracted
text is split into at most 6,000-character chunks. Normalized chunk vectors are
averaged into one document-level vector, preserving the existing provenance and
retention key. This is **document search, not passage-vector search**. The method
version invalidates the older prefix-only cache. Completed documents are cached
immediately; an interrupted partial document is retried. A document needing more
chunks than the entire run budget remains reported as oversized and pending.

Run results expose indexed/reused/empty-text counts and embedding
eligible/blocked/empty/covered/pending/oversized counts, actual chunk inputs, and
the representation method. Latest successful results are also stored under
`corpus_index:last_result` in automation state. PDFs with failed or empty text
extraction are reported by path and retried; image-only PDFs need OCR separately.
Metadata-only discovery is not reported as full-content coverage.

`GET /api/query/corpus?query=memory&mode=keyword&limit=10` searches the existing
index without a model call. `mode=semantic` or `mode=hybrid` embeds only the query
and uses cached document vectors; neither mode silently backfills documents.
Hybrid combines keyword and semantic rankings. Results include original paths,
source hashes, provenance, excerpts, retrieval sources, and coverage counts.
Security/privacy gates apply before returning or embedding source content.
Query eligibility and embedding coverage counts describe the actual retrieval
candidates, not the entire backlog; `eligible_count_scope` and coverage `scope`
make that distinction explicit. Use `corpus_index:last_result` for full-scan
coverage. Semantic queries inspect only valid cached vectors; keyword queries
inspect ranked FTS matches, keeping uncached backlog processing off the read path.
Excerpts are document previews, not a promise of the best matching passage.

This endpoint provides evidence for agents and topic synthesis; it does not
claim to generate an answer. The existing Archivist topic retriever shares the
same full-content embedding cache. Summaries remain a separate optional intake
operation, and paper abstracts can be reused without generation.
