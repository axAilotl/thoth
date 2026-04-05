---
name: thoth
description: >
  Universal knowledge pipeline for processing Twitter bookmarks, ArXiv papers, GitHub stars,
  HuggingFace likes, and personal data into an Obsidian-ready knowledge vault. Use when the
  user asks to process bookmarks, run the pipeline, discover papers, check stats, generate
  digests, or work with any thoth data ingestion or enrichment task.
allowed-tools: "Read,Bash(python*,uvicorn*,pytest*,black*,flake8*,mypy*)"
version: "1.0.0"
---

# Thoth — Universal Knowledge Processor

Polymorphic knowledge pipeline built on a `KnowledgeArtifact` architecture. Transforms
Twitter bookmarks, research papers, GitHub stars, and personal data into a structured,
queryable Obsidian vault at `knowledge_vault/`.

## Critical Safety Rules

- **NEVER read** `twitter-bookmarks-merged.json`, `bookmarks_processed.json`, or any file
  in `graphql_cache/` — these are large JSON files that will flood context
- Always prefer `--dry-run` before destructive pipeline operations
- Always prefer `--use-cache` over live downloads when testing

---

## Common Workflows

### Check status
```bash
python thoth.py stats
python thoth.py stats --verbose
python thoth.py db stats
```

### Process cached data (fast, no rate limits)
```bash
python thoth.py pipeline --use-cache --batch-size 10
python thoth.py pipeline --use-cache --dry-run      # preview first
python thoth.py pipeline --use-cache --rerun-llm    # force LLM re-run
```

### Discover and process research papers
```bash
python thoth.py arxiv --discover
python thoth.py arxiv --discover --topics "agentic ai,ai security" --limit 20
```

### Sync GitHub stars and HuggingFace likes
```bash
python thoth.py social --sync --github-user <user> --hf-user <user>
python thoth.py github-stars --limit 50
python thoth.py huggingface-likes --limit 50
```

### Download new Twitter/X GraphQL data (slow — 45 req/15 min rate limit)
```bash
python thoth.py download --resume --limit 50
```

### Full pipeline (download + process)
```bash
python thoth.py full --resume
```

### YouTube and video transcripts
```bash
python thoth.py youtube --limit 100
python thoth.py update-videos
python thoth.py twitter-transcripts --limit 20 --verbose
```

### Generate Obsidian digests
```bash
python thoth.py digest all
python thoth.py digest weekly
python thoth.py digest inbox
python thoth.py digest dashboard
python thoth.py digest all --notify    # with notification
```

### Delete artifacts
```bash
python thoth.py delete <tweet_id> --dry-run   # always dry-run first
python thoth.py delete <tweet_id>
```

### Database maintenance
```bash
python thoth.py db stats
python thoth.py db vacuum
```

### Start the real-time capture API
```bash
uvicorn thoth_api:app --reload --port 19100   # use high port
```

### Development
```bash
PYTHONPATH=. .venv/bin/pytest tests/
black .
flake8 core processors thoth.py
mypy core processors
```

---

## Architecture at a Glance

| Layer | What |
|---|---|
| `thoth.py` | CLI entry point (argparse) |
| `thoth_api.py` | FastAPI server for real-time browser capture |
| `core/artifacts/` | KnowledgeArtifact base + Tweet, Paper, Repo subclasses |
| `core/router.py` | CapabilityRouter — dispatches artifacts to processors |
| `core/metadata_db.py` | SQLite at `.thoth/meta.db` — queue, file index, LLM cache |
| `collectors/` | ArXiv discovery, GitHub/HF sync, browser extension receiver |
| `processors/` | Media, URL, LLM, transcription, document processors |
| `knowledge_vault/` | Output — Obsidian-ready markdown (git-ignored) |

### Pipeline stages (in order)
`url_expansion → media_download → documents → transcripts → llm_processing → markdown`

### Output directories
```
knowledge_vault/
├── tweets/        ← individual tweet .md files
├── threads/       ← multi-tweet threads
├── transcripts/   ← YouTube/Twitter video transcripts
├── repos/         ← GitHub/HF READMEs
├── stars/         ← GitHub stars summaries
├── papers/        ← ArXiv papers
├── pdfs/          ← general PDFs
└── media/         ← images and videos
```

---

## Configuration

- `config.json` — pipeline stages, LLM providers, output paths
- `.env` — API keys: `OPENAI_API_KEY`, `ANTHROPIC_API`, `OPEN_ROUTER_API_KEY`,
  `YOUTUBE_API_KEY`, `DEEPGRAM_API_KEY`, `GITHUB_API`, `HF_USER`
- Config access pattern: `config.get('pipeline.stages.media_download')`

### LLM task routing (in `config.json`)
```json
{
  "llm": {
    "tasks": {
      "tags":       {"provider": "anthropic", "enabled": true},
      "summary":    {"provider": "anthropic", "enabled": true},
      "alt_text":   {"provider": "openrouter", "model": "vision", "enabled": true},
      "transcript": {"provider": "openrouter", "model": "transcript", "enabled": true}
    }
  }
}
```

---

## Key Patterns

- **Resume by default** — most commands skip already-processed items; pass `--no-resume`
  to force reprocessing
- **Dry-run** — `--dry-run` on pipeline/delete shows what would happen without changes
- **Async processing** — concurrency configured at `processing.llm_async` in `config.json`
- **Thread detection** — uses Twitter's `tweetDisplayType: "SelfThread"` (not time gaps)
- **GraphQL cache** — controlled by `pipeline.keep_graphql_cache`; kept by default

---

## Dataview Queries (Obsidian)

All artifact frontmatter is Dataview-compatible. Example:
```dataview
TABLE author, title, importance
FROM "papers"
WHERE relevance_score > 0.8
SORT created DESC
```
