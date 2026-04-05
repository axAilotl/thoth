# Wiki Loop Notes

The compiled wiki lives under `vault/wiki` and is driven by the shared runtime.

Operator coverage is exercised by:
- `PYTHONPATH=. pytest -q tests/test_wiki_updater.py tests/test_wiki_query.py tests/test_wiki_lint.py tests/test_wiki_regressions.py`
- `PYTHONPATH=. pytest -q tests`

The regression fixtures under `tests/fixtures/wiki/` model the current rules:
- compiled pages keep their original `created_at` when refreshed
- curated query pages are persisted as `thoth_type: wiki_query`
- lint rejects invalid timestamps but accepts generated query pages
