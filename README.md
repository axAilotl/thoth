<p align="center">
  <img src="static/thoth.png" alt="Chibi Thoth" width="220">
</p>

# Thoth

Thoth is the evolution of the earlier `xmarks` system. `xmarks` handled high-volume raw capture and bookmark ingestion; Thoth keeps that ingestion layer, adds stronger storage boundaries, and builds a compiled knowledge layer on top. The wiki side is explicitly inspired by Andrej Karpathy's persistent LLM wiki idea, but adapted to a larger historical corpus and a more automated ingest stack.

Thoth separates:
- pipeline source and generated material in the vault
- compiled wiki output outside the vault
- local operational state in `.thoth_system`

That split is the core contract. Raw sources stay raw. Generated artifacts stay traceable. Local metadata, caches, auth, and temp state do not get synced with the vault.

## Operating Model

Thoth currently has three layers:

1. Ingestion
Raw capture, normalization, translation, downloads, transcripts, summaries, and safe artifact publishing.

2. Archivist
Topic-scoped compilation over selected source folders, types, tags, and terms.

3. Analyst / Agent
Higher-cost interactive work over the compiled layer for synthesis, comparison, and refinement.

## Quick Start

### Setup

```bash
git clone https://github.com/axAilotl/thoth.git
cd thoth

curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

If you do not want to activate the venv globally, use `.venv/bin/python thoth.py ...` or `uv run python thoth.py ...`.

### Configure

```bash
cp config.example.json config.json
cp .env.example .env
```

Important config layers:
- `config.example.json` is the tracked default config.
- `config.json` is your local untracked config override.
- `control.json` is local untracked operator state written by the settings UI.
- `archivist_topics.example.yaml` is the tracked archivist template.
- `archivist_topics.yaml` is the live local registry bootstrapped from the template and kept untracked.
- `prompts/archivist_system.md` and `prompts/archivist_user.md` are tracked prompt files for the archivist compiler and can be edited without changing Python.

Important paths:
- `paths.vault_dir` is the synced vault root for raw and processed artifacts.
- `paths.wiki_dir` is the compiled wiki root and resolves outside the vault by default.
- `paths.system_dir` is local-only state for auth, databases, caches, logs, and temp files.

### Start the API

```bash
.venv/bin/python thoth_api.py
```

Then open `/settings` for the operator control plane.

## Settings UI

The settings UI exposes:
- provider credentials and task routing
- model aliases per provider
- a dedicated embedding route for semantic archivist retrieval
- X API auth and manual sync controls
- Web Clipper source directory settings
- path layout for active shared roots and registries
- archivist registry editing, corpus diagnostics, due-topic runs, force runs, and background automation

## CLI Overview

Use `python thoth.py --help` for the full command list and `python thoth.py <command> --help` for the exact flags on a subcommand.

| Command | Purpose | Typical flags |
| --- | --- | --- |
| `process` | Process tweet/bookmark material into markdown. | `--limit`, `--use-cache`, `--no-resume` |
| `pipeline` | Run the single-pass enrichment pipeline. | `--use-cache`, `--batch-size`, `--rerun-llm` |
| `x-api-sync` | Backfill bookmarks from the X API and process them. | `--max-pages`, `--max-results`, `--no-resume` |
| `arxiv` | Discover research papers from ArXiv. | `--discover`, `--source`, `--topics`, `--categories`, `--limit` |
| `social` | Sync GitHub stars and Hugging Face likes. | `--sync`, `--github-user`, `--hf-user`, `--limit` |
| `github-stars` | Pull GitHub stars directly. | `--limit`, `--no-resume` |
| `huggingface-likes` | Pull Hugging Face likes directly. | `--limit`, `--no-resume`, `--no-models` |
| `web-clipper` | Index configured vault source directories for imported markdown and attachments. | none |
| `connectors` | Inspect built-in and plugin connector manifests. | `list`, `--json` |
| `archivist` | Compile archivist topic pages or benchmark retrieval for selected topics. | `--topics`, `--force`, `--dry-run`, `--benchmark`, `--limit` |
| `research` | Inspect research graph reports. | `missing-papers`, `--queue`, `--json` |
| `youtube` | Post-process existing tweets for YouTube metadata and transcripts. | `--limit`, `--no-resume`, `--no-transcripts` |
| `update-videos` | Refresh existing tweet/thread outputs with video data. | none |
| `twitter-transcripts` | Run local Whisper over Twitter video media. | `--limit`, `--no-resume`, `--verbose` |
| `wiki-query` | Search the compiled wiki and optionally write back a curated page. | `--limit`, `--write-back`, `--selected-slugs`, `--title` |
| `wiki-lint` | Check wiki health. | `--stale-after-days` |
| `ingest-queue` | Drain the generalized ingestion queue. | `--limit` |
| `digest` | Generate Obsidian-facing discovery notes. | `weekly`, `inbox`, `dashboard`, `all`, `--notify` |
| `stats` | Show current artifact and queue stats. | `--verbose` |
| `db` | Database maintenance commands. | `stats`, `vacuum`, `export` |
| `delete` | Delete a tweet and its artifacts. | `--dry-run` |
| `migrate-filenames` | Normalize legacy filenames. | `--dry-run`, `--analyze` |
| `migrate-frontmatter` | Upgrade legacy frontmatter. | `--dry-run` |

## Common Commands

Check state:

```bash
.venv/bin/python thoth.py stats
.venv/bin/python thoth.py stats --verbose
.venv/bin/python thoth.py db stats
```

Process cached data:

```bash
.venv/bin/python thoth.py pipeline --use-cache --batch-size 10
.venv/bin/python thoth.py pipeline --use-cache --rerun-llm
```

Backfill X bookmarks:

```bash
.venv/bin/python thoth.py x-api-sync --max-pages 3 --max-results 100
```

Run discovery:

```bash
.venv/bin/python thoth.py arxiv --discover --topics "agent systems,multimodal reasoning"
.venv/bin/python thoth.py social --sync --github-user <user> --hf-user <user>
.venv/bin/python thoth.py web-clipper
```

Work the wiki:

```bash
.venv/bin/python thoth.py wiki-query "companion ai"
.venv/bin/python thoth.py wiki-query "companion ai" --write-back --title "Companion AI Notes"
.venv/bin/python thoth.py wiki-lint --stale-after-days 30
```

## Archivist

Archivist is topic-scoped compilation, not free-roaming summarization.

What exists now:
- topic registry loading and validation
- hard source gates by folder scope
- incremental corpus inventory with reuse-safe change detection
- source-type, tag, and term filters plus modular retrieval policy
- full-text retrieval, semantic retrieval, and hybrid ranking
- cadence and dirty-check state
- manual force flags
- prompt files outside the codebase at `prompts/archivist_system.md` and `prompts/archivist_user.md`
- a standalone `archivist` CLI command that compiles selected topics
- settings UI support for editing the registry, viewing corpus stats, running due topics, force-running a topic, and viewing parsed topics/state
- API routes for direct archivist execution
- background archivist automation driven by `automation.archivist`
- task routing support for dedicated `archivist` and `embedding` model routes
- optional Pi CLI execution for Archivist synthesis, locked to non-interactive no-tool mode

Current archivist workflow:

1. Edit the live local `archivist_topics.yaml`, either in `/settings` or on disk.
2. Adjust the archivist prompt files in `prompts/` if you want to change synthesis style or sectioning.
3. Configure the `archivist` task route. The tracked default prefers the existing local `pi` setup with `zai-coding-cn/glm-5.2`, then falls back to a Pi/OpenRouter profile, then direct OpenRouter.
4. Configure `embedding` too if any topic uses semantic or hybrid retrieval.
5. Run `.venv/bin/python thoth.py archivist` for due topics, or `.venv/bin/python thoth.py archivist --topics companion-ai-research --force` for an intentional rerun.
6. Use `.venv/bin/python thoth.py archivist --benchmark --topics companion-ai-research` when you want retrieval diagnostics without writing wiki pages.
7. In `/settings`, use `Run Due Topics` for an immediate due-topic pass, or `Force Run` on a topic card when you want that topic to ignore cadence right now.
8. Use `automation.archivist` in settings when you want background topic compilation a couple times a day with the same task route.

The Pi-backed provider is generation-only inside Thoth. It invokes `pi --print --no-tools --no-session --no-context-files`, so the Archivist gets Pi's model/profile behavior without granting it file editing or shell tools during wiki compilation. The primary `pi` provider uses the installed Pi auth/config as-is. The `pi_openrouter` fallback can install `@earendil-works/pi-coding-agent` if `pi` is missing and bridges the existing `OPEN_ROUTER_API_KEY` setting into Pi's expected `OPENROUTER_API_KEY` environment variable.

## Storage Layout

Repo-level tracked files:

```text
thoth.py
thoth_api.py
config.example.json
archivist_topics.example.yaml
README.md
docs/
core/
collectors/
processors/
static/
tests/
```

Logical runtime layout:

```text
vault/
  tweets/
  threads/
  papers/
  repos/
  stars/
  translations/
  imports/
  notes/
  media/
  videos/
  images/
  transcripts/

wiki/

.thoth_system/
```

Key rules:
- tweet and thread artifacts stay in the vault
- repo `README` source files live in `repos/`
- generated repo summaries live in `stars/`
- selected imported-note source directories live directly under the vault
- the compiled wiki stays outside the vault
- databases, caches, auth, temp files, and logs stay in `.thoth_system/`

## Development

### Local Postgres pgvector

SQLite remains the default local metadata store. For development or integration tests that need Postgres with pgvector, start the local Docker stack:

```bash
docker compose -f compose.dev.yml up -d postgres
```

The stack uses local-only demo credentials, creates both `thoth` and `thoth_test`, and enables the `vector` extension in each database. Copy `.env.example` to `.env` or export the DSNs directly:

```bash
export THOTH_POSTGRES_DSN=postgresql://thoth:thoth_dev@localhost:5432/thoth
export THOTH_TEST_POSTGRES_DSN=postgresql://thoth:thoth_dev@localhost:5432/thoth_test
```

If port 5432 is already in use, set `THOTH_POSTGRES_PORT` before starting compose and update both DSNs to match the chosen host port:

```bash
THOTH_POSTGRES_PORT=55432 docker compose -f compose.dev.yml up -d postgres
```

Stop the stack without deleting data:

```bash
docker compose -f compose.dev.yml down
```

Remove the local database volume when you want a clean dev database:

```bash
docker compose -f compose.dev.yml down -v
```

Validation:

```bash
PYTHONPATH=. .venv/bin/pytest tests/
python3 -m compileall -q core collectors processors thoth.py thoth_api.py tests
```

## License

MIT. See [LICENSE](LICENSE).
