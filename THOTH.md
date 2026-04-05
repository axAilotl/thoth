# THOTH - Universal Knowledge Processor

> *"The god of wisdom, writing, and knowledge. The scribe who records all things."*

Thoth transforms scattered data into actionable knowledge. Built on Thoth' proven pipeline architecture, it extends from a Twitter bookmark processor into an **extensible, multi-source knowledge system** powered by agentic AI.

---

## Vision

### From Thoth to Thoth

| Thoth | Thoth |
|--------|-------|
| Twitter bookmark pipeline | Universal knowledge processor |
| Single entity (Tweet) | Polymorphic artifacts |
| SQLite queue (bookmark-centric) | Ingestion queue (source-agnostic) |
| Fixed pipeline stages | Capability-based routing |
| Manual discovery | Agent-driven discovery (Hermes) |
| Obsidian markdown output | Multi-output (Obsidian, Telegram, Discord, APIs) |

### Core Philosophy

1. **Everything is a KnowledgeArtifact** - Tweets, papers, financial records, health data, code diffs - all flow through the same pipeline
2. **Agents discover, humans decide** - Hermes finds papers, but you control what gets processed
3. **Knowledge compounds** - Every query, every reflection, every insight enriches the base
4. **Your data, your rules** - Local-first, privacy-preserving, extensible

---

## Architecture

### Layer 1: KnowledgeArtifact Abstraction

```python
@dataclass
class KnowledgeArtifact:
    """Base class for all ingestible knowledge entities."""
    id: str                           # Unique identifier
    source_type: str                  # 'twitter', 'arxiv', 'github', 'hermes', 'financial', etc.
    raw_content: str                  # Original content (text, JSON, HTML)
    created_at: datetime              # When artifact was created (source time)
    ingested_at: datetime             # When artifact entered Thoth
    processing_status: str            # 'pending', 'processing', 'processed', 'failed'
    
    # Capability flags - what can this artifact provide?
    capabilities: Tuple[str, ...] = field(default_factory=tuple)
    # Examples: ('media', 'urls', 'transcription', 'llm_summary', 'embedding')
    
    # Metadata
    tags: List[str] = field(default_factory=list)
    importance_score: Optional[float] = None
    custom_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Output tracking
    output_paths: Dict[str, str] = field(default_factory=dict)  # {'markdown': 'path/to/file.md'}
```

### Layer 2: Artifact Subclasses

```python
@dataclass
class TweetArtifact(KnowledgeArtifact):
    """Twitter/X bookmark with rich GraphQL data."""
    source_type: str = 'twitter'
    capabilities: Tuple[str, ...] = ('media', 'urls', 'transcription', 'llm_summary', 'embedding')
    
    # Twitter-specific fields
    screen_name: str = ''
    full_text: str = ''
    media_items: List[MediaItem] = field(default_factory=list)
    url_mappings: List[URLMapping] = field(default_factory=list)
    engagement: Dict[str, int] = field(default_factory=dict)
    thread_id: Optional[str] = None
    
@dataclass
class PaperArtifact(KnowledgeArtifact):
    """Research paper discovered by Hermes or manual import."""
    source_type: str = 'arxiv'  # or 'semantic_scholar', 'openreview'
    capabilities: Tuple[str, ...] = ('pdf_download', 'llm_summary', 'embedding', 'citation_graph')
    
    # Paper-specific fields
    title: str = ''
    authors: List[str] = field(default_factory=list)
    abstract: str = ''
    doi: Optional[str] = None
    arxiv_id: Optional[str] = None
    pdf_url: Optional[str] = None
    citations_count: Optional[int] = None
    relevance_score: Optional[float] = None  # Hermes-calculated relevance

@dataclass
class RepositoryArtifact(KnowledgeArtifact):
    """GitHub/HuggingFace repository."""
    source_type: str = 'github'  # or 'huggingface'
    capabilities: Tuple[str, ...] = ('readme_download', 'llm_summary', 'code_index', 'embedding')
    
    repo_name: str = ''
    description: str = ''
    stars: int = 0
    language: Optional[str] = None
    topics: List[str] = field(default_factory=list)

@dataclass
class FinancialArtifact(KnowledgeArtifact):
    """Financial record from APIs, exports, or manual entry."""
    source_type: str = 'financial'
    capabilities: Tuple[str, ...] = ('embedding', 'analysis', 'visualization')
    
    transaction_type: str = ''  # 'expense', 'income', 'transfer'
    amount: float = 0.0
    currency: str = 'USD'
    category: str = ''
    merchant: Optional[str] = None
    account: Optional[str] = None

@dataclass
class HealthArtifact(KnowledgeArtifact):
    """Health/fitness data from Apple Health, Google Fit, etc."""
    source_type: str = 'health'
    capabilities: Tuple[str, ...] = ('embedding', 'analysis', 'visualization', 'trend_detection')
    
    metric_type: str = ''  # 'steps', 'heart_rate', 'sleep', 'workout'
    value: float = 0.0
    unit: str = ''
    duration_minutes: Optional[float] = None

@dataclass
class ConversationArtifact(KnowledgeArtifact):
    """AI conversation export (Claude, ChatGPT, etc.)."""
    source_type: str = 'claude_conversation'  # or 'chatgpt', 'opencode_session'
    capabilities: Tuple[str, ...] = ('embedding', 'llm_summary', 'code_extraction')
    
    messages: List[Dict[str, str]] = field(default_factory=list)
    model: Optional[str] = None
    tokens_used: Optional[int] = None

@dataclass
class BrowserBookmarkArtifact(KnowledgeArtifact):
    """Browser bookmark for content extraction."""
    source_type: str = 'browser_bookmark'
    capabilities: Tuple[str, ...] = ('content_extraction', 'llm_summary', 'embedding')
    
    url: str = ''
    title: str = ''
    favicon_url: Optional[str] = None
    browser: str = 'chrome'  # 'chrome', 'firefox', 'safari', 'obsidian_clipper'
```

### Layer 3: Generalized Ingestion Queue

Replace `bookmark_queue` with `ingestion_queue`:

```sql
CREATE TABLE ingestion_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_id TEXT NOT NULL UNIQUE,
    artifact_type TEXT NOT NULL,  -- 'tweet', 'paper', 'financial', etc.
    source TEXT NOT NULL,         -- 'hermes', 'browser_extension', 'api', 'manual'
    priority INTEGER DEFAULT 0,   -- Higher = process first
    status TEXT NOT NULL DEFAULT 'pending',
    payload_json TEXT NOT NULL,   -- Serialized artifact data
    capabilities_json TEXT,       -- What this artifact can provide
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    next_attempt_at TEXT,
    created_at TEXT NOT NULL,
    processed_at TEXT
);

CREATE INDEX idx_ingestion_status ON ingestion_queue(status, next_attempt_at);
CREATE INDEX idx_ingestion_type ON ingestion_queue(artifact_type);
CREATE INDEX idx_ingestion_priority ON ingestion_queue(priority DESC);
```

### Layer 4: Capability-Based Pipeline Routing

```python
class CapabilityRouter:
    """Routes artifacts to appropriate processors based on capabilities."""
    
    CAPABILITY_PROCESSORS = {
        'media': MediaProcessor,
        'urls': URLProcessor,
        'transcription': TranscriptionProcessor,
        'llm_summary': LLMProcessor,
        'embedding': EmbeddingProcessor,
        'pdf_download': PDFProcessor,
        'content_extraction': ContentExtractor,
        'code_extraction': CodeExtractor,
        'analysis': AnalysisProcessor,
        'visualization': VisualizationProcessor,
    }
    
    def route(self, artifact: KnowledgeArtifact) -> List[Processor]:
        """Return ordered list of processors for this artifact."""
        processors = []
        for capability in artifact.capabilities:
            if capability in self.CAPABILITY_PROCESSORS:
                processors.append(self.CAPABILITY_PROCESSORS[capability])
        return processors
```

---

## Data Sources

### Primary Sources (Phase 1)

| Source | Type | Collection Method |
|--------|------|-------------------|
| **Twitter/X Bookmarks** | TweetArtifact | Browser userscript + GraphQL |
| **Hermes Agent** | PaperArtifact | Automated discovery pipeline |
| **GitHub Stars** | RepositoryArtifact | GitHub API |
| **HuggingFace Likes** | RepositoryArtifact | HF API |
| **Browser Bookmarks** | BrowserBookmarkArtifact | Browser extension / export file |

### Secondary Sources (Phase 2)

| Source | Type | Collection Method |
|--------|------|-------------------|
| **Google Takeout** | Multiple | Manual export + parser |
| - YouTube History | TweetArtifact-like | Parse watch-history.json |
| - Gmail | ConversationArtifact-like | Parse mbox |
| - Maps Timeline | LocationArtifact | Parse location-history.json |
| **Claude Exports** | ConversationArtifact | Parse exported conversations |
| **ChatGPT Exports** | ConversationArtifact | Parse conversation.json |
| **OpenCode Sessions** | ConversationArtifact | Session database |
| **Apple Health Export** | HealthArtifact | Parse export.xml |
| **Financial APIs** | FinancialArtifact | Plaid, manual CSV, etc. |

### Tertiary Sources (Phase 3)

| Source | Type | Collection Method |
|--------|------|-------------------|
| **Obsidian Clipper** | BrowserBookmarkArtifact | Obsidian plugin integration |
| **Obsidian Vault Sync** | KnowledgeArtifact | Watch folder for changes |
| **Calendar Events** | CalendarArtifact | Google Calendar API |
| **Code Diffs** | CodeDiffArtifact | Git hooks |
| **Documents Folder** | DocumentArtifact | Watch folder (PDFs, docs) |

---

## Hermes Agent Integration

### What is Hermes?

Hermes Agent (https://github.com/nousresearch/hermes-agent) is an AI-powered research paper discovery system. It:
- Monitors ArXiv, Semantic Scholar, OpenReview for new papers
- Uses ML to rank papers by relevance to your interests
- Extracts metadata (authors, citations, abstracts)
- Can be configured with topic filters

### Integration Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      HERMES DISCOVERY                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │   ArXiv     │    │  Semantic   │    │ OpenReview  │     │
│  │   Monitor   │    │  Scholar    │    │  Monitor    │     │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘     │
│         │                  │                   │            │
│         └──────────────────┼───────────────────┘            │
│                            ▼                                │
│                  ┌─────────────────┐                        │
│                  │  Relevance      │                        │
│                  │  Classifier     │  ◄── Your interest     │
│                  │  (ML Model)     │      profile           │
│                  └────────┬────────┘                        │
│                           │                                 │
│                           ▼                                 │
│                  ┌─────────────────┐                        │
│                  │  Paper Queue    │                        │
│                  │  (scored)       │                        │
│                  └────────┬────────┘                        │
│                           │                                 │
└───────────────────────────┼─────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                       THOTH PIPELINE                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  PaperArtifact ──► PDF Download ──► LLM Summary ──► Embed  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
                  knowledge_vault/papers/
```

### Configuration

```json
{
  "hermes": {
    "enabled": true,
    "schedule": "0 */6 * * *",  // Every 6 hours
    "sources": ["arxiv", "semantic_scholar"],
    "topics": [
      "large language models",
      "retrieval augmented generation", 
      "prompt engineering",
      "agent systems",
      "code generation"
    ],
    "relevance_threshold": 0.7,
    "max_papers_per_run": 50,
    "auto_process": false,  // Manual review required
    "notification": {
      "telegram": true,
      "discord": true
    }
  }
}
```

---

## Notification & Distribution System

### Output Channels

```python
@dataclass
class NotificationChannel:
    """Base class for notification channels."""
    name: str
    enabled: bool = True
    
    async def send(self, artifact: KnowledgeArtifact, message: str) -> bool:
        raise NotImplementedError

class TelegramChannel(NotificationChannel):
    """Telegram notifications via bot."""
    bot_token: str
    chat_id: str
    
class DiscordChannel(NotificationChannel):
    """Discord notifications via webhook."""
    webhook_url: str

class FileDistributor(NotificationChannel):
    """Copy files to specified locations."""
    mappings: Dict[str, Path]  # artifact_type -> destination_path
    
class ObsidianSynchronizer(NotificationChannel):
    """Sync to Obsidian vault with backlinks."""
    vault_path: Path
    create_backlinks: bool = True
```

### Notification Events

| Event | Trigger | Channels |
|-------|---------|----------|
| `paper_discovered` | Hermes finds high-relevance paper | Telegram, Discord |
| `pipeline_complete` | Batch processing finished | Telegram |
| `insight_available` | LLM generates actionable insight | All |
| `query_answered` | Complex query answered from knowledge base | Origin + Telegram |
| `daily_digest` | Scheduled summary | Telegram, Email |
| `weekly_digest` | Weekly insights | All |

---

## Discovery & Query System

### Semantic Search

```python
class KnowledgeIndex:
    """Semantic search over all artifacts."""
    
    def __init__(self):
        self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
        self.index = faiss.IndexFlatIP(384)
        self.artifact_map: Dict[int, KnowledgeArtifact] = {}
    
    async def search(
        self, 
        query: str, 
        artifact_types: Optional[List[str]] = None,
        date_range: Optional[Tuple[datetime, datetime]] = None,
        limit: int = 10
    ) -> List[Tuple[KnowledgeArtifact, float]]:
        """Semantic search with filters."""
        pass
    
    async def add_artifact(self, artifact: KnowledgeArtifact):
        """Add artifact to index."""
        embedding = await self._embed(artifact)
        idx = self.index.ntotal
        self.index.add(embedding)
        self.artifact_map[idx] = artifact
```

### Query Types

1. **Factual**: "How much did I spend on trading cards in 2022?"
2. **Analytical**: "What are my most common coding mistakes?"
3. **Temporal**: "How many km did I walk in the last 3 months?"
4. **Cross-source**: "What papers mention the same topics as this GitHub repo?"
5. **Reflective**: "What have I learned about RAG this year?"

### Output Formats

- **Markdown** (Obsidian)
- **Slides** (Marp)
- **Visualizations** (matplotlib, Plotly)
- **Interactive Dashboards** (Streamlit)
- **Structured Data** (JSON, CSV)

---

## Directory Structure

```
thoth/
├── thoth.py                    # CLI entry point
├── thoth_api.py                # FastAPI server
├── THOTH.md                    # This file
├── CLAUDE.md                   # AI assistant instructions
│
├── core/
│   ├── artifacts/              # Artifact data models
│   │   ├── __init__.py
│   │   ├── base.py             # KnowledgeArtifact
│   │   ├── tweet.py
│   │   ├── paper.py
│   │   ├── repository.py
│   │   ├── financial.py
│   │   ├── health.py
│   │   ├── conversation.py
│   │   └── bookmark.py
│   │
│   ├── config.py               # Extended configuration
│   ├── router.py               # CapabilityRouter
│   ├── ingestion_queue.py      # Generalized queue
│   ├── metadata_db.py          # Extended SQLite
│   ├── knowledge_index.py      # Semantic search
│   └── pipeline_registry.py    # Extended registry
│
├── collectors/                 # Data collection agents
│   ├── hermes_collector.py     # Hermes agent integration
│   ├── browser_collector.py    # Browser extension
│   ├── google_takeout.py       # Google data parser
│   ├── claude_export.py        # Claude conversation parser
│   ├── chatgpt_export.py       # ChatGPT export parser
│   ├── apple_health.py         # Health data parser
│   └── financial_apis.py       # Financial data connectors
│
├── processors/                 # Pipeline processors (from Thoth)
│   ├── pipeline_processor.py   # Updated for artifacts
│   ├── media_processor.py
│   ├── document_factory.py
│   ├── llm_processor.py
│   ├── embedding_processor.py  # NEW: Vector embeddings
│   └── ...
│
├── outputs/                    # Output channels
│   ├── obsidian_writer.py
│   ├── telegram_notifier.py
│   ├── discord_notifier.py
│   ├── file_distributor.py
│   └── marp_generator.py       # Slide decks
│
├── query/                      # Query & discovery
│   ├── query_engine.py
│   ├── semantic_search.py
│   ├── insight_generator.py
│   └── visualizer.py
│
├── knowledge_vault/            # Output (git-ignored)
│   ├── tweets/
│   ├── threads/
│   ├── papers/
│   ├── repos/
│   ├── conversations/
│   ├── bookmarks/
│   ├── financial/
│   ├── health/
│   ├── transcripts/
│   └── _digests/
│
├── userscript/
│   └── thoth_capture.user.js   # Updated browser extension
│
└── .thoth/
    ├── meta.db                 # Extended SQLite
    ├── embeddings.faiss        # Vector index
    └── config.json
```

---

## Implementation Phases

### Phase 1: Core Refactoring (Week 1-2) [COMPLETED]

**Goal**: Generalize architecture without breaking existing Thoth functionality

1. Create `KnowledgeArtifact` base class (Done)
2. Refactor `Tweet` to extend `KnowledgeArtifact` (Done: TweetArtifact created)
3. Update `PipelineProcessor` to handle generic artifacts (In progress)
4. Implement `CapabilityRouter` (Done)
5. Extend database schema (backward-compatible) (Done: ingestion_queue added)
6. Add `EmbeddingProcessor` (Pending)
7. Implement `KnowledgeIndex` (Pending)

**Validation**: All existing Thoth tests pass

### Phase 2: New Sources (Week 3-4)

**Goal**: Add Hermes + Browser Bookmarks + Conversation exports

1. Implement `HermesCollector`
2. Create `PaperArtifact` processor pipeline
3. Add `BrowserBookmarkArtifact` + content extraction
4. Build Claude/ChatGPT export parsers
5. Implement notification system (Telegram, Discord)

**Validation**: Papers flow through pipeline, notifications work

### Phase 3: Personal Data (Week 5-6)

**Goal**: Add personal data sources (Google Takeout, Health, Financial)

1. Build Google Takeout parser
2. Add `HealthArtifact` + visualization
3. Add `FinancialArtifact` + analysis
4. Implement cross-source queries
5. Add daily/weekly digests

**Validation**: Personal insights queryable

### Phase 4: Agentic Features (Week 7-8)

**Goal**: Agent-driven discovery and proactive insights

1. Implement proactive insight generation
2. Add trend detection
3. Build recommendation engine
4. Create Obsidian backlink automation
5. Add file distribution rules

**Validation**: System proactively surfaces relevant knowledge

---

## Configuration Schema

```json
{
  "vault_dir": "knowledge_vault",
  "database": {
    "enabled": true,
    "path": ".thoth/meta.db",
    "wal_mode": true
  },
  
  "sources": {
    "twitter": {
      "enabled": true,
      "userscript": true,
      "graphql_cache": true
    },
    "hermes": {
      "enabled": true,
      "schedule": "0 */6 * * *",
      "topics": ["llm", "rag", "agents"],
      "relevance_threshold": 0.7,
      "max_papers_per_run": 50,
      "auto_process": false
    },
    "browser_bookmarks": {
      "enabled": true,
      "watch_file": null,
      "extension": true
    },
    "google_takeout": {
      "enabled": false,
      "import_path": null
    },
    "claude_export": {
      "enabled": false,
      "import_path": null
    },
    "apple_health": {
      "enabled": false,
      "import_path": null
    },
    "financial": {
      "enabled": false,
      "providers": []
    }
  },
  
  "pipeline": {
    "stages": {
      "url_expansion": true,
      "media_download": true,
      "documents": {
        "arxiv_papers": true,
        "general_pdfs": true,
        "github_readmes": true,
        "huggingface_readmes": true
      },
      "transcripts": {
        "youtube_videos": true,
        "twitter_videos": true
      },
      "llm_processing": {
        "tweet_tags": true,
        "tweet_summaries": true,
        "alt_text": true,
        "paper_summaries": true,
        "readme_summaries": true
      },
      "embeddings": {
        "enabled": true,
        "model": "all-MiniLM-L6-v2"
      }
    }
  },
  
  "notifications": {
    "telegram": {
      "enabled": false,
      "bot_token": null,
      "chat_id": null,
      "events": ["paper_discovered", "daily_digest"]
    },
    "discord": {
      "enabled": false,
      "webhook_url": null,
      "events": ["paper_discovered", "weekly_digest"]
    }
  },
  
  "query": {
    "embedding_model": "all-MiniLM-L6-v2",
    "index_path": ".thoth/embeddings.faiss",
    "default_limit": 10
  },
  
  "output": {
    "obsidian": {
      "enabled": true,
      "backlinks": true,
      "dataview": true
    },
    "file_distribution": {
      "enabled": false,
      "rules": []
    }
  },
  
  "llm": {
    "default_provider": "anthropic",
    "tasks": {
      "tags": {"provider": "anthropic", "enabled": true},
      "summary": {"provider": "anthropic", "enabled": true},
      "alt_text": {"provider": "openrouter", "model": "vision", "enabled": true},
      "paper_summary": {"provider": "anthropic", "enabled": true},
      "insight": {"provider": "anthropic", "enabled": true}
    }
  }
}
```

---

## CLI Commands (Extended)

```bash
# Core pipeline
python thoth.py pipeline --use-cache --batch-size 10
python thoth.py pipeline --source hermes --dry-run
python thoth.py pipeline --source all --resume

# Hermes paper discovery
python thoth.py hermes discover --topics "llm,agents,rag"
python thoth.py hermes review                 # Review discovered papers
python thoth.py hermes process --limit 10     # Process approved papers

# Personal data import
python thoth.py import google-takeout --path ./takeout.zip
python thoth.py import claude-export --path ./conversations/
python thoth.py import apple-health --path ./export.zip
python thoth.py import financial --provider plaid --days 30

# Query & discovery
python thoth.py query "What papers discuss retrieval augmentation?"
python thoth.py search --semantic "agent architectures" --type paper
python thoth.py stats --by-source
python thoth.py digest daily --notify
python thoth.py digest weekly --format marp

# Notifications
python thoth.py notify test --channel telegram
python thoth.py notify send --message "Pipeline complete" --channels telegram,discord

# Maintenance
python thoth.py index rebuild                   # Rebuild embedding index
python thoth.py db vacuum
python thoth.py stats
```

---

## API Endpoints (Extended)

```
# Artifact ingestion
POST /api/artifacts                 # Submit any artifact type
GET  /api/artifacts/{id}            # Get artifact by ID
GET  /api/artifacts?type=paper      # List by type

# Hermes
POST /api/hermes/discover           # Trigger discovery
GET  /api/hermes/pending            # Pending papers
POST /api/hermes/approve/{id}       # Approve paper for processing

# Query
POST /api/query                     # Natural language query
POST /api/search                    # Semantic search
GET  /api/insights                  # Generated insights

# Notifications
GET  /api/notifications/channels
POST /api/notifications/test
POST /api/notifications/send

# Sources
GET  /api/sources                   # List configured sources
POST /api/sources/{source}/sync     # Trigger source sync

# Digests
POST /api/digest/daily
POST /api/digest/weekly
```

---

## Inspired By

- **Karpathy's LLM Knowledge Bases**: Raw data → Compiled wiki → Q&A → Enhancement loop
- **@omarsar0's Paper Curation**: Daily automated discovery with tuned relevance filtering
- **Personal Data Liberation**: All your data in one queryable system

## Wiki Contract

The compiled wiki lives under `vault/wiki` and is separate from raw source material.

- `index.md` is the navigation root.
- `log.md` is the append-only maintenance log.
- `pages/{slug}.md` stores compiled wiki pages.
- Slugs are normalized to lower-kebab-case and are capped at 80 characters.
- Compiled page frontmatter carries `thoth_type`, `title`, `slug`, `kind`, `aliases`, `source_paths`, `related_slugs`, `language`, `translated_from`, `created_at`, and `updated_at`.
- Supported kinds are `topic`, `entity`, and `concept`.
- Raw documents stay in `raw/` or `library/`; the wiki layer should link back to them, not duplicate them.

## Wiki Scaffolding

The runtime seeds the wiki scaffold before processing starts.

- `thoth.py` and `thoth_api.py` both ensure `wiki/index.md`, `wiki/log.md`, and `wiki/pages/` exist at startup.
- `core/wiki_scaffold.py` owns the seed content and the append-only maintenance log primitive.
- The scaffold stays inside the synced vault; `.thoth_system/` remains reserved for local operational state.

---

## Next Steps

1. Clone thoth to `/mnt/samesung/ai/thoth`
2. Create artifact base classes
3. Implement Hermes integration
4. Add semantic search
5. Build notification system

*"The measure of intelligence is the ability to change." - Einstein*
