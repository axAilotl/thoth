---
name: thoth
description: >
  Thoth operator guide for the live CLI, settings UI, wiki tooling, and archivist registry
  flow. Use when the user asks to run collectors, inspect stats, manage pipeline work, or
  work with Thoth's local control surfaces.
allowed-tools: "Read,Bash(python*,uvicorn*,pytest*,black*,flake8*,mypy*)"
version: "1.0.0"
---

# Thoth Operator Guide

This file is the agent-facing runbook. User-facing operational context belongs in `README.md`.
If something matters and is missing there, add it there instead of creating another broad doc.

## Ground Rules

- Prefer `.venv/bin/python thoth.py ...` or `uv run python thoth.py ...` unless the venv is already active.
- Do not assume a command exists. Check `.venv/bin/python thoth.py --help` and `.venv/bin/python thoth.py <command> --help` before documenting or invoking unusual paths.
- Prefer resume-safe and cache-aware commands unless the user explicitly wants a force reprocess.
- Avoid loading large raw cache files into context unless the task truly requires it.
- Treat `config.json` plus `control.json` as the live runtime config surface. `config.example.json` is only a template for seeding new installs.
- Archivist prompt text lives in tracked files under `prompts/`, not in Python. Adjust those files when the synthesis contract needs to change.
- Fresh arXiv ingestion should trust arXiv entry metadata for the canonical filename. Only use local PDF title extraction when repairing legacy or non-canonical files that arrived through another route.

## First Commands

Show the current CLI surface:

```bash
.venv/bin/python thoth.py --help
.venv/bin/python thoth.py <command> --help
```

Check current state:

```bash
.venv/bin/python thoth.py stats
.venv/bin/python thoth.py stats --verbose
.venv/bin/python thoth.py db stats
```

## Core CLI Workflows

Single-pass processing:

```bash
.venv/bin/python thoth.py pipeline --use-cache --batch-size 10
.venv/bin/python thoth.py pipeline --use-cache --rerun-llm
```

Bookmark backfill:

```bash
.venv/bin/python thoth.py x-api-sync --max-pages 3 --max-results 100
```

Archivist compilation:

```bash
.venv/bin/python thoth.py archivist
.venv/bin/python thoth.py archivist --topics companion-ai-research --force
.venv/bin/python thoth.py archivist --dry-run
```

Discovery and source sync:

```bash
.venv/bin/python thoth.py arxiv --discover --topics "agent systems,multimodal reasoning"
.venv/bin/python thoth.py social --sync --github-user <user> --hf-user <user>
.venv/bin/python thoth.py web-clipper
.venv/bin/python thoth.py ingest-queue --limit 25
```

Media and transcript follow-up:

```bash
.venv/bin/python thoth.py youtube --limit 100
.venv/bin/python thoth.py update-videos
.venv/bin/python thoth.py twitter-transcripts --limit 20 --verbose
```

Wiki operations:

```bash
.venv/bin/python thoth.py wiki-query "companion ai"
.venv/bin/python thoth.py wiki-query "companion ai" --write-back --title "Companion AI Notes"
.venv/bin/python thoth.py wiki-lint --stale-after-days 30
```

Maintenance:

```bash
.venv/bin/python thoth.py delete <tweet_id> --dry-run
.venv/bin/python thoth.py migrate-filenames --dry-run
.venv/bin/python thoth.py migrate-frontmatter --dry-run
.venv/bin/python thoth.py db vacuum
```

## API and Settings

Start the API:

```bash
.venv/bin/python thoth_api.py
```

The local API binds to `127.0.0.1:8090` by default unless `THOTH_API_PORT` or `PORT` overrides it.

Open `http://127.0.0.1:8090/settings` for:
- provider credentials and task routing
- model aliases per provider
- X API auth and manual sync
- monitored-account X webhooks with `llm.tasks.x_monitor`, `THOTH_X_MONITOR_WEBHOOK_SECRET`, and `bookmark.write`
- Web Clipper source configuration
- path layout inspection for active shared roots
- archivist topic registry editing, immediate runs, and background automation

## Archivist

Archivist now ships as a real compiler plus the existing control plane.

What exists:
- `archivist_topics.example.yaml` as the tracked template
- local `archivist_topics.yaml` bootstrapped from the template
- tracked staged prompt files at `prompts/archivist_*.md`
- source gates by root scope
- source-type, tag, and term filters plus modular retrieval policy
- incremental corpus inventory, full-text retrieval, semantic retrieval, and hybrid ranking
- SQLite-backed corpus metadata plus lazy cached document embeddings in `archivist_corpus_embeddings`
- staged source-type briefing followed by final topic synthesis
- durable topic/source usage tracking so automated runs skip unchanged never-used sources
- cadence and dirty-check state
- manual force flags
- dedicated `archivist` and `embedding` task routing in settings
- the `Archivist` tab in `/settings`
- the `thoth.py archivist` CLI command
- the `thoth.py archivist --benchmark` retrieval benchmark path
- immediate API/UI archivist execution
- scheduled archivist automation via `automation.archivist`
- PDF text extraction in corpus indexing, including `pdfs/` roots and paper-grade weighting for whitepapers/manual PDFs

How to work with it now:

1. Edit `archivist_topics.yaml` in the `Archivist` tab or directly on disk.
2. Adjust the staged prompt files in `prompts/archivist_*.md` if source-type briefing, repository framing, or final synthesis needs to change.
3. Set the `archivist` provider/model route in settings, and set `embedding` too if any topic uses semantic or hybrid retrieval.
4. Run `.venv/bin/python thoth.py archivist` to compile due topics.
5. Use `.venv/bin/python thoth.py archivist --topics <id> --force` for intentional reruns.
6. Use `.venv/bin/python thoth.py archivist --benchmark --topics <id>` to inspect retrieval quality without writing wiki pages.
7. Use `Run Due Topics` or a topic card `Run Now` / `Force Run` action in `/settings` for immediate execution.
8. Use `automation.archivist` in settings when you want the API service to compile due topics on a fixed interval.
9. Use `retrieval.source_type_limits` and `carryover_limit_per_type` in the topic registry when one source type is crowding out the others.
10. Include `papers` and `pdfs` in topic roots when manual PDFs or whitepapers should enter the same research pool as downloaded papers.

Semantic retrieval details that matter operationally:
- Full-text indexing covers the corpus inventory in SQLite.
- Document embeddings are generated lazily for the filtered candidate set, not precomputed for the whole vault.
- Cached embeddings are reused until the document content, embedding provider, or embedding model changes.
- The query embedding is generated each run, and `max_new_embeddings_per_run` limits how many missing document embeddings get filled in one pass.

## X Monitor Webhooks

- Configure `sources.x_api.monitoring.accounts` with usernames or numeric ids for accounts that should be watched outside the normal bookmark flow.
- Webhook callers must send `X-Thoth-Webhook-Secret`, backed by `THOTH_X_MONITOR_WEBHOOK_SECRET`.
- `llm.tasks.x_monitor` is the classifier route for deciding whether a monitored post is useful enough to auto-bookmark and queue.
- When `auto_bookmark` is enabled, the connected X account must have `bookmark.write` in `sources.x_api.scopes`.

## Current Data Boundaries

- Raw and processed artifacts belong in the vault.
- The compiled wiki belongs outside the vault.
- `.thoth_system` holds local-only operational state.
- `repos/` holds raw README source files.
- `stars/` holds generated repo summary notes.

## Validation

```bash
PYTHONPATH=. .venv/bin/pytest tests/
python3 -m compileall -q core collectors processors thoth.py thoth_api.py tests
black .
flake8 core processors thoth.py
mypy core processors
```
