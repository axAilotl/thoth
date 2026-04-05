<p align="center">
  <img src="static/thoth.png" alt="Chibi Thoth" width="220">
</p>

# Thoth

Thoth is the evolution of the earlier `xmarks` system. `xmarks` handled high-volume raw capture and bookmark ingestion; Thoth keeps that ingestion layer, adds stronger storage boundaries, and builds a compiled knowledge layer on top. The wiki side is explicitly inspired by Andrej Karpathy's persistent LLM wiki idea, but adapted to a larger historical corpus and a more automated ingest stack.

Thoth separates:
- raw and processed source material in the vault
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

Important paths:
- `paths.vault_dir` is the synced vault root for raw and processed artifacts.
- `paths.raw_dir` is the raw source area inside the vault.
- `paths.library_dir` is the processed artifact area inside the vault.
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
- X API auth and manual sync controls
- Web Clipper source directory settings
- path layout for vault, raw, library, wiki, and system state
- archivist registry editing and per-topic force controls

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
| `web-clipper` | Index configured Obsidian Web Clipper source directories. | none |
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
- source-type, tag, and term filtering
- cadence and dirty-check state
- manual force flags
- settings UI support for editing the registry and viewing parsed topics/state
- task routing support for a dedicated `archivist` model route

Current archivist workflow:

1. Start `thoth_api.py`.
2. Open `/settings` and go to the `Archivist` tab.
3. Edit the live local `archivist_topics.yaml`, either in the built-in editor or on disk.
4. Configure the `archivist` task route to the provider/model alias you want.
5. Use `Queue Force` to mark a topic for the next archivist runtime pass.

Current limitation:
- There is not yet a standalone `thoth.py archivist ...` compile command.

The control plane is shipped. The dedicated archivist page compiler/runtime is still being implemented.

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
  raw/
  library/
    translations/
  tweets/
  threads/
  papers/
  repos/
  stars/
  media/
  videos/
  images/
  transcripts/

wiki/

.thoth_system/
```

Key rules:
- raw tweets and threads stay in the vault
- repo `README` source files live in `repos/`
- generated repo summaries live in `stars/`
- the compiled wiki stays outside the vault
- databases, caches, auth, temp files, and logs stay in `.thoth_system/`

## Development

Validation:

```bash
PYTHONPATH=. .venv/bin/pytest tests/
python3 -m compileall -q core collectors processors thoth.py thoth_api.py tests
```

## License

MIT. See [LICENSE](LICENSE).
