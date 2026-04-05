<p align="center">
  <img src="static/thoth.png" alt="Chibi Thoth" width="220">
</p>

# Thoth 1.0 – Universal Knowledge Processor

Thoth is the evolution of the earlier `xmarks` system: `xmarks` handled high-volume bookmark capture and raw ingestion, while Thoth takes the next step and turns that material into structured knowledge. The current stack separates raw/source material from generated library outputs and a compiled wiki layer, with durable local metadata and caches kept outside the synced vault.

It is also explicitly influenced by Andrej Karpathy's persistent LLM wiki idea: instead of treating every question as a one-off retrieval problem, Thoth maintains a compiled knowledge layer that can be updated over time from accumulated source material.

Built on a polymorphic `KnowledgeArtifact` architecture, Thoth automates discovery via userscript capture, X API backfill, ArXiv and social APIs, Obsidian Web Clipper source directories, linked media downloads, LLM-powered enrichment, and incremental compilation into higher-level knowledge artifacts.

> *"The god of wisdom, writing, and knowledge. The scribe who records all things."*

## Lineage

- `xmarks`: focused on high-scale raw bookmark capture and first-pass processing.
- `thoth`: closes the loop on universal ingestion, storage boundaries, translation, compilation, and long-lived knowledge maintenance.
- `karpathy/llm-wiki`: inspired the compiled wiki layer and the idea that the system should maintain an evolving knowledge artifact, not just raw notes.

## Core Flow

1. Ingest source material from live and scheduled collectors.
2. Normalize, enrich, download, translate, and cache artifacts safely.
3. Publish raw and generated outputs into the vault.
4. Compile selected material into a separate wiki layer for deeper analysis.
5. Hand the compiled layer to interactive agents for refinement, comparison, and retrieval.

---

## Quick Start

### 1. Setup Environment (Recommended: `uv`)
```bash
# Clone the repo
git clone https://github.com/axAilotl/thoth.git
cd thoth

# Install uv and create venv
curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt
```

### 2. Configure
```bash
cp config.example.json config.json    # local-only override; do not commit
cp .env.example .env                  # add API keys (Anthropic, GitHub, etc.)
```

Important defaults:
- `config.example.json` is the tracked default config for the repo.
- `config.json` is your local untracked override layer.
- `control.json` is local untracked operator state written by the settings UI.
- `archivist_topics.example.yaml` is the tracked template; the live local `archivist_topics.yaml` is bootstrapped from it and stays untracked.
- `paths.vault_dir` is the synced Obsidian vault for source and generated content.
- `paths.system_dir` is local-only state for auth, databases, caches, logs, and temp files.
- `paths.wiki_dir` is resolved outside the vault by default so the compiled wiki is distinct from raw source storage.

### 3. Start Collectors
```bash
# Discover research papers based on your keywords
python thoth.py arxiv --discover

# Scan arXiv category feeds directly
python thoth.py arxiv --discover --source rss --categories "cs.AI,cs.LG"

# Backfill bookmarks from the X API
python thoth.py x-api-sync --max-pages 3 --max-results 100

# Sync GitHub stars and HuggingFace likes
python thoth.py social --sync --github-user youruser --hf-user youruser

# Index Obsidian Web Clipper source directories
python thoth.py web-clipper

# Start the real-time Twitter capture API
python thoth_api.py
```

---

## CLI Overview (`thoth.py`)

| Command | Purpose | Most useful flags |
| --- | --- | --- |
| `arxiv` | **NEW**: Discover research papers via ArXiv API or category feeds. | `--discover`, `--source`, `--topics`, `--categories`, `--limit` |
| `social` | **NEW**: Sync GitHub stars and HuggingFace likes. | `--sync`, `--github-user`, `--hf-user` |
| `web-clipper` | Index the explicit Obsidian Web Clipper source directories under `paths.raw_dir`, stage attachments, and publish English companions for non-English notes. | none |
| `x-api-sync` | Backfill bookmarks from the X API and process them immediately. | `--max-results`, `--max-pages`, `--no-resume` |
| `pipeline` | Single-pass ingestion → enrichment → markdown. | `--use-cache`, `--batch-size`, `--rerun-llm` |
| `digest` | Generate discovery views (Inbox, Weekly, Dashboard). | `weekly`, `inbox`, `dashboard`, `all` |
| `stats` | Snapshot of all artifacts and database state. | `--verbose` |
| `db` | Database maintenance and stats. | `db stats`, `db vacuum` |
| `youtube` | Process YouTube videos for transcripts/metadata. | `--limit`, `--transcripts` |
| `twitter-transcripts` | Transcribe Twitter videos using local Whisper. | `--limit`, `--verbose` |

---

## Research Discovery
Thoth is designed for deep research. Configure your keywords in your local `config.json` override or pass them via CLI to cut through the firehose:

```bash
python thoth.py arxiv --discover --topics "agent systems,multimodal reasoning"
```

---

## Project Structure
```
thoth/
├── thoth.py                  # Main CLI entry point
├── thoth_api.py              # FastAPI capture service
├── core/
│   ├── artifacts/             # KnowledgeArtifact models (Tweet, Paper, Repo, etc.)
│   ├── router.py              # Capability-based processor routing
│   ├── metadata_db.py         # SQLite source of truth (.thoth/meta.db)
│   └── pipeline_registry.py   # Modular stage registry
├── collectors/                # Inbound data collectors
│   ├── arxiv_collector.py     # ArXiv discovery pipeline
│   ├── social_collector.py    # GitHub/HuggingFace sync
│   ├── web_clipper_collector.py # Web Clipper source indexing
│   └── personal/              # Scaffolding for Takeout, Health, AI Exports
├── core/path_layout.py        # Canonical vault + system storage layout
├── processors/                # LLM, Media, and Document enrichment
├── knowledge_vault/
│   ├── raw/                   # Immutable source captures
│   ├── library/               # Processed artifacts for Obsidian use
│   │   └── translations/      # English companions for non-English source docs
│   ├── tweets/                # Tweet markdown
│   ├── threads/               # Thread markdown
│   ├── repos/                 # Raw GitHub / Hugging Face READMEs
│   └── stars/                 # Generated repo summary notes
├── wiki/                      # Compiled LLM-maintained knowledge layer
└── .thoth_system/             # Database, auth, caches, logs, and temp state
```

## Storage Layout

Thoth now treats the vault and system state as separate concerns:

- `paths.vault_dir` is the synced vault root.
- `paths.raw_dir` is for immutable source captures.
- `paths.library_dir` is for processed artifacts and generated markdown.
- `paths.library_dir/translations` holds English companion outputs for non-English source docs.
- `paths.wiki_dir` is for the compiled Karpathy-style wiki layer and resolves outside the vault by default.
- `paths.system_dir` is for local-only operational state such as databases, logs, auth tokens, and temporary files.
- `sources.web_clipper.note_dirs` and `sources.web_clipper.attachment_dirs` define the exact directories under `paths.raw_dir` that the Web Clipper collector will watch.
- See [docs/web_clipper_ingest.md](docs/web_clipper_ingest.md) for the operator runbook.

## Wiki Contract

The compiled wiki is a curated layer, not raw source storage.

- `wiki/index.md` is the navigation entry point.
- `wiki/log.md` is the chronological maintenance log.
- `wiki/pages/{slug}.md` holds compiled pages, where slugs are lower-kebab-case and max 80 characters.
- Wiki page frontmatter uses `thoth_type`, `title`, `slug`, `kind`, `aliases`, `source_paths`, `related_slugs`, `language`, `translated_from`, `created_at`, and `updated_at`.
- Supported page kinds are `topic`, `entity`, and `concept`.
- Source documents stay in `raw/` or `library/`; wiki pages should link back to them instead of copying them into the compiled layer.

## Wiki Scaffolding

The runtime seeds the wiki structure on startup:

- `thoth_api.py` and `thoth.py` both ensure `wiki/index.md`, `wiki/log.md`, and `wiki/pages/` exist before processing starts.
- `core/wiki_scaffold.py` owns the seed content and append-only log primitive.
- The scaffold lives in `paths.wiki_dir`, while operational state remains under `.thoth_system/`.

## Operating Model

Thoth now spans three layers of work:

1. Ingestion
Raw capture, normalization, downloads, media/document extraction, and durable caching.

2. Archivist
Topic-scoped compilation from selected source folders and tags into first-pass wiki/topic pages.

3. Analyst / Agent
Interactive refinement over the compiled layer, where higher-cost models can be used for deeper comparisons, synthesis, and conversation.

---

## Digest & Discovery (Obsidian + Dataview)
Thoth generates Dataview-compatible frontmatter for all artifacts. Use the `digest` command to build your research hub.

```bash
python thoth.py digest all --notify
```

**Query Example:**
```dataview
TABLE author, title, importance
FROM "papers"
WHERE relevance_score > 0.8
SORT created DESC
```

---

## Testing & Dev Tooling
```bash
# Use the uv-managed environment
PYTHONPATH=. .venv/bin/pytest tests/
```

---

## License
MIT — see [LICENSE](./LICENSE).
