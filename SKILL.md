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
- Treat `config.example.json` as tracked defaults and `config.json`, `control.json`, and `archivist_topics.yaml` as local operator files.

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

Open `/settings` for:
- provider credentials and task routing
- model aliases per provider
- X API auth and manual sync
- Web Clipper source configuration
- path layout inspection
- archivist topic registry editing and force controls

## Archivist

Archivist currently ships as a control plane plus topic/state plumbing.

What exists:
- `archivist_topics.example.yaml` as the tracked template
- local `archivist_topics.yaml` bootstrapped from the template
- source gates by root scope
- source-type, tag, and term filters
- cadence and dirty-check state
- manual force flags
- dedicated `archivist` task routing in settings
- the `Archivist` tab in `/settings`

How to work with it now:

1. Run `.venv/bin/python thoth_api.py`.
2. Open `/settings`.
3. Edit `archivist_topics.yaml` in the `Archivist` tab or directly on disk.
4. Set the `archivist` provider/model route in settings.
5. Use `Queue Force` if the next run should ignore normal cadence.

Important reality check:
- There is not yet a standalone `thoth.py archivist ...` runtime command.
- Do not tell users to run a non-existent archivist CLI.
- The topic registry and force/state controls are real; the dedicated compiler/runtime command is still under implementation.

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
