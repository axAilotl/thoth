"""
SQLite Metadata Database - Persistent metadata for efficient re-runs and browser/API usage
Stores tweets, downloads, LLM cache, files index, and more for fast lookups
"""

import sqlite3
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional
from datetime import datetime, timedelta
from contextlib import contextmanager
from dataclasses import dataclass

from .config import config
from .path_layout import build_path_layout
from .prompt_security import (
    THOTH_REDACTION_METADATA_KEY,
    THOTH_SECURITY_AUDIT_KEY,
    THOTH_SECURITY_FINDINGS_KEY,
    THOTH_SECURITY_POLICY_KEY,
    THOTH_SECURITY_SCANNED_LENGTH_KEY,
    PROMPT_SECURITY_POLICY_ALLOWED,
    PROMPT_SECURITY_POLICY_BLOCKED,
    PROMPT_SECURITY_POLICY_NEEDS_REVIEW,
    PROMPT_SECURITY_POLICY_OVERRIDE_APPROVED,
    is_strict_prompt_security_source,
    merge_prompt_security_metadata,
    merge_prompt_security_policy_metadata,
    prompt_security_metadata_for_text,
    prompt_security_policy_for_metadata,
    prompt_security_requires_review,
)

logger = logging.getLogger(__name__)

FILES_INDEX_ALLOWED_TYPES = (
    'media',
    'pdf',
    'readme',
    'tweet',
    'thread',
    'transcript',
    'video',
    'thumbnail',
    'note',
    'attachment',
    'translation',
)
FILES_INDEX_TYPE_CHECK = "', '".join(FILES_INDEX_ALLOWED_TYPES)
INGESTION_QUEUE_ALLOWED_STATUSES = (
    'pending',
    'processing',
    'processed',
    'failed',
    'needs_review',
    'blocked',
)
INGESTION_QUEUE_STATUS_CHECK = "', '".join(INGESTION_QUEUE_ALLOWED_STATUSES)
INGESTION_QUARANTINE_STATUSES = frozenset(
    {
        PROMPT_SECURITY_POLICY_NEEDS_REVIEW,
        PROMPT_SECURITY_POLICY_BLOCKED,
    }
)


def _payload_security_metadata(payload: dict[str, Any]) -> dict[str, Any]:
    normalized_metadata = payload.get("normalized_metadata")
    if not isinstance(normalized_metadata, dict):
        return {}
    return {
        key: normalized_metadata[key]
        for key in (
            THOTH_SECURITY_FINDINGS_KEY,
            THOTH_SECURITY_POLICY_KEY,
            THOTH_SECURITY_AUDIT_KEY,
            THOTH_REDACTION_METADATA_KEY,
        )
        if normalized_metadata.get(key)
    }


def _payload_has_prompt_security_scan(payload: dict[str, Any]) -> bool:
    normalized_metadata = payload.get("normalized_metadata")
    return bool(
        isinstance(normalized_metadata, dict)
        and (
            normalized_metadata.get(THOTH_SECURITY_FINDINGS_KEY)
            or normalized_metadata.get(THOTH_SECURITY_SCANNED_LENGTH_KEY) is not None
        )
    )


def _ingestion_payload_with_security_metadata(entry: "IngestionQueueEntry") -> str:
    """Attach reviewable prompt-security metadata inside persisted queue JSON."""
    try:
        payload = json.loads(entry.payload_json)
    except Exception:
        logger.warning(
            "Skipping prompt-security metadata for invalid ingestion payload %s",
            entry.artifact_id,
        )
        return entry.payload_json
    if not isinstance(payload, dict):
        return entry.payload_json
    if _payload_has_prompt_security_scan(payload):
        return entry.payload_json

    content = payload.get("raw_content")
    if not isinstance(content, str):
        content = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    source_label = f"{entry.artifact_type}:{entry.source}:{entry.artifact_id}"
    normalized_metadata = payload.get("normalized_metadata")
    strict_scope = is_strict_prompt_security_source(
        source_type=entry.source,
        source_label=source_label,
        source_path=_payload_source_path(payload),
        metadata=normalized_metadata if isinstance(normalized_metadata, Mapping) else None,
    )
    security_metadata = prompt_security_metadata_for_text(
        content,
        source_label=source_label,
        scope="strict" if strict_scope else "context",
    )
    if not security_metadata:
        return entry.payload_json

    if not isinstance(normalized_metadata, dict):
        normalized_metadata = {}
    payload["normalized_metadata"] = merge_prompt_security_metadata(
        normalized_metadata,
        security_metadata,
    )
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _json_payload(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_source_path(payload: Mapping[str, Any]) -> str | None:
    custom_metadata = payload.get("custom_metadata")
    normalized_metadata = payload.get("normalized_metadata")
    candidates: list[Any] = [
        payload.get("raw_payload_path"),
        payload.get("source_path"),
        payload.get("source_relative_path"),
        payload.get("source_file"),
    ]
    if isinstance(custom_metadata, Mapping):
        candidates.extend(
            custom_metadata.get(key)
            for key in (
                "raw_payload_path",
                "source_path",
                "source_relative_path",
                "skill_output_path",
            )
        )
    if isinstance(normalized_metadata, Mapping):
        candidates.extend(
            normalized_metadata.get(key)
            for key in (
                "raw_payload_path",
                "source_path",
                "source_relative_path",
                "skill_output_path",
            )
        )
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            return text
    return None


def _security_policy_audit_entry(
    *,
    action: str,
    status: str,
    reason: str,
    actor: str | None = None,
    at: str | None = None,
    previous_status: str | None = None,
) -> dict[str, str]:
    entry = {
        "action": action,
        "status": status,
        "reason": reason,
        "at": at or datetime.now().isoformat(),
    }
    if actor:
        entry["actor"] = actor
    if previous_status:
        entry["previous_status"] = previous_status
    return entry


def _finding_fingerprints(metadata: Mapping[str, Any] | None) -> tuple[str, ...]:
    if not isinstance(metadata, Mapping):
        return ()
    findings = metadata.get(THOTH_SECURITY_FINDINGS_KEY)
    if not isinstance(findings, list):
        return ()
    values = []
    for finding in findings:
        if not isinstance(finding, Mapping):
            continue
        fingerprint = (
            finding.get("fingerprint")
            or ":".join(
                str(finding.get(key) or "")
                for key in ("scanner", "source_label", "scope", "pattern_id")
            )
        )
        if fingerprint:
            values.append(str(fingerprint))
    return tuple(sorted(set(values)))


def _ingestion_payload_with_security_policy(
    entry: "IngestionQueueEntry",
    payload_json: str,
    *,
    existing_payload_json: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Attach quarantine policy metadata based on persisted scanner findings."""
    payload = _json_payload(payload_json)
    if not payload:
        return payload_json, {
            "status": PROMPT_SECURITY_POLICY_ALLOWED,
            "reason": "invalid_or_empty_payload",
        }

    normalized_metadata = payload.get("normalized_metadata")
    if not isinstance(normalized_metadata, dict):
        normalized_metadata = {}

    existing_payload = _json_payload(existing_payload_json)
    existing_metadata = existing_payload.get("normalized_metadata")
    existing_policy = (
        existing_metadata.get(THOTH_SECURITY_POLICY_KEY)
        if isinstance(existing_metadata, Mapping)
        else None
    )
    if (
        isinstance(existing_policy, Mapping)
        and existing_policy.get("status") == PROMPT_SECURITY_POLICY_OVERRIDE_APPROVED
        and _finding_fingerprints(normalized_metadata)
        == _finding_fingerprints(
            existing_metadata if isinstance(existing_metadata, Mapping) else None
        )
    ):
        normalized_metadata = merge_prompt_security_policy_metadata(
            normalized_metadata,
            existing_policy,
        )
        existing_audit = (
            existing_metadata.get(THOTH_SECURITY_AUDIT_KEY)
            if isinstance(existing_metadata, Mapping)
            else None
        )
        if isinstance(existing_audit, list) and existing_audit:
            normalized_metadata[THOTH_SECURITY_AUDIT_KEY] = [
                dict(item) for item in existing_audit if isinstance(item, Mapping)
            ]
        payload["normalized_metadata"] = normalized_metadata
        return json.dumps(payload, ensure_ascii=False, sort_keys=True), dict(existing_policy)

    source_label = f"{entry.artifact_type}:{entry.source}:{entry.artifact_id}"
    policy = prompt_security_policy_for_metadata(
        normalized_metadata,
        source_type=entry.source,
        source_label=source_label,
        source_path=_payload_source_path(payload),
    )
    audit_entry = None
    if policy["status"] in INGESTION_QUARANTINE_STATUSES:
        audit_entry = _security_policy_audit_entry(
            action="quarantined",
            status=str(policy["status"]),
            reason=str(policy["reason"]),
        )

    if normalized_metadata.get(THOTH_SECURITY_FINDINGS_KEY) or audit_entry:
        normalized_metadata = merge_prompt_security_policy_metadata(
            normalized_metadata,
            policy,
            audit_entry=audit_entry,
        )
        payload["normalized_metadata"] = normalized_metadata

    return json.dumps(payload, ensure_ascii=False, sort_keys=True), policy


def _queue_status_for_security_policy(
    requested_status: str,
    policy: Mapping[str, Any],
) -> str:
    status = str(policy.get("status") or PROMPT_SECURITY_POLICY_ALLOWED)
    if status == PROMPT_SECURITY_POLICY_BLOCKED:
        return "blocked"
    if status == PROMPT_SECURITY_POLICY_NEEDS_REVIEW:
        return "needs_review"
    return requested_status


def _security_policy_last_error(policy: Mapping[str, Any]) -> str | None:
    status = str(policy.get("status") or "")
    if status == PROMPT_SECURITY_POLICY_BLOCKED:
        return f"security policy blocked: {policy.get('reason')}"
    if status == PROMPT_SECURITY_POLICY_NEEDS_REVIEW:
        return f"security review required: {policy.get('reason')}"
    return None


@dataclass
class TweetMetadata:
    """Tweet metadata for database storage"""
    tweet_id: str
    screen_name: str
    created_at: str
    is_thread_tweet: bool = False
    thread_id: Optional[str] = None
    file_path: Optional[str] = None
    last_processed_at: Optional[str] = None
    content_hash: Optional[str] = None


@dataclass
class DownloadMetadata:
    """Download metadata for database storage"""
    url: str
    status: str  # success|404|error|pending
    target_path: Optional[str] = None
    size_bytes: Optional[int] = None
    updated_at: Optional[str] = None
    error_msg: Optional[str] = None


@dataclass
class FileMetadata:
    """File metadata for database storage"""
    path: str
    file_type: str  # media|pdf|readme|tweet|thread|transcript|video|thumbnail
    size_bytes: int
    hash: Optional[str] = None
    updated_at: Optional[str] = None
    source_id: Optional[str] = None


@dataclass
class BookmarkQueueEntry:
    """Bookmark queue entry for durable background processing"""
    tweet_id: str
    source: Optional[str] = None
    captured_at: Optional[str] = None
    status: str = 'pending'
    attempts: int = 0
    last_error: Optional[str] = None
    last_attempt_at: Optional[str] = None
    processed_at: Optional[str] = None
    payload_json: Optional[str] = None
    next_attempt_at: Optional[str] = None
    processed_with_graphql: bool = False


@dataclass
class IngestionQueueEntry:
    """Generalized ingestion queue entry for all artifact types"""
    artifact_id: str
    artifact_type: str  # 'tweet', 'paper', 'repository', etc.
    source: str         # 'twitter', 'hermes', 'manual', etc.
    payload_json: str
    priority: int = 0
    status: str = 'pending'
    attempts: int = 0
    last_error: Optional[str] = None
    next_attempt_at: Optional[str] = None
    created_at: Optional[str] = None
    processed_at: Optional[str] = None
    capabilities_json: Optional[str] = None


@dataclass(frozen=True)
class ResearchPaperRecord:
    """Normalized paper metadata stored for research graph operations."""

    paper_id: str
    title: str = ""
    authors: tuple[str, ...] = ()
    abstract: str = ""
    doi: Optional[str] = None
    arxiv_id: Optional[str] = None
    pdf_url: Optional[str] = None
    venue: Optional[str] = None
    published_at: Optional[str] = None
    source_provider: Optional[str] = None
    collected: bool = False
    raw_payload: Dict[str, Any] | None = None
    updated_at: Optional[str] = None


@dataclass(frozen=True)
class ResearchPaperEdge:
    """Typed paper-to-paper relationship evidence."""

    source_paper_id: str
    target_paper_id: str
    edge_type: str
    source_evidence: str = ""
    discovery_source: str = ""
    discovered_at: Optional[str] = None
    metadata: Dict[str, Any] | None = None


class MetadataDB:
    """SQLite metadata database with WAL mode and connection pooling"""
    
    def __init__(self, db_path: str = None):
        if db_path:
            self.db_path = Path(db_path)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            path_layout = build_path_layout(config)
            self.db_path = path_layout.database_path
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._initialized = False
        self._setup_database()
    
    def _setup_database(self):
        """Initialize database with schema and optimizations"""
        try:
            with self._get_connection() as conn:
                # Enable WAL mode for better concurrency (configurable)
                if config.get('database.wal_mode', True):
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA cache_size=10000")
                conn.execute("PRAGMA temp_store=memory")
                
                # Create tables
                self._create_tables(conn)
                
                self._initialized = True
                logger.info(f"Metadata database initialized: {self.db_path}")
                
        except Exception as e:
            logger.error(f"Failed to setup metadata database: {e}")
            raise
    
    @contextmanager
    def _get_connection(self):
        """Get database connection with proper error handling"""
        conn = None
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            yield conn
            conn.commit()
        except Exception:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def _create_tables(self, conn: sqlite3.Connection):
        """Create all database tables"""
        
        # Tweets table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tweets (
                tweet_id TEXT PRIMARY KEY,
                screen_name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                is_thread_tweet BOOLEAN DEFAULT FALSE,
                thread_id TEXT,
                file_path TEXT,
                last_processed_at TEXT,
                content_hash TEXT,
                FOREIGN KEY (thread_id) REFERENCES tweets (tweet_id)
            )
        """)
        
        # URL mappings table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS url_mappings (
                short_url TEXT PRIMARY KEY,
                expanded_url TEXT NOT NULL,
                first_seen_tweet_id TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                FOREIGN KEY (first_seen_tweet_id) REFERENCES tweets (tweet_id)
            )
        """)
        
        # Downloads table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS downloads (
                url TEXT PRIMARY KEY,
                status TEXT NOT NULL CHECK (status IN ('success', '404', 'error', 'pending')),
                target_path TEXT,
                size_bytes INTEGER,
                updated_at TEXT NOT NULL,
                error_msg TEXT
            )
        """)
        
        # LLM cache table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS llm_cache (
                cache_key TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                model_provider TEXT
            )
        """)
        
        # Files index table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS files_index (
                path TEXT PRIMARY KEY,
                type TEXT NOT NULL CHECK (type IN ('{FILES_INDEX_TYPE_CHECK}')),
                size_bytes INTEGER NOT NULL,
                hash TEXT,
                updated_at TEXT NOT NULL,
                source_id TEXT
            )
        """)
        self._ensure_files_index_types(conn)
        
        # GraphQL cache index table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS graphql_cache_index (
                tweet_id TEXT PRIMARY KEY,
                cache_paths_json TEXT NOT NULL,
                first_cached_at TEXT NOT NULL,
                last_cached_at TEXT NOT NULL,
                FOREIGN KEY (tweet_id) REFERENCES tweets (tweet_id)
            )
        """)

        # Bookmark queue table for durable background processing
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bookmark_queue (
                tweet_id TEXT PRIMARY KEY,
                source TEXT,
                captured_at TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'processed', 'failed')),
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                last_attempt_at TEXT,
                processed_at TEXT,
                payload_json TEXT,
                next_attempt_at TEXT,
                processed_with_graphql BOOLEAN DEFAULT 0
            )
        """)

        # Generalized ingestion queue for all artifact types
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                artifact_id TEXT NOT NULL UNIQUE,
                artifact_type TEXT NOT NULL,
                source TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'processed', 'failed', 'needs_review', 'blocked')),
                payload_json TEXT NOT NULL,
                capabilities_json TEXT,
                attempts INTEGER DEFAULT 0,
                last_error TEXT,
                next_attempt_at TEXT,
                created_at TEXT NOT NULL,
                processed_at TEXT
            )
        """)
        self._ensure_ingestion_queue_statuses(conn)

        # Ensure new columns exist when upgrading from earlier schema
        try:
            conn.execute("ALTER TABLE bookmark_queue ADD COLUMN processed_with_graphql BOOLEAN DEFAULT 0")
        except Exception:
            pass

        # Transcript chunk cache for long-running LLM operations
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transcript_chunk_cache (
                context_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                result_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                model_provider TEXT,
                PRIMARY KEY (context_id, chunk_index)
            )
        """)

        # Durable automation state for schedulers and probe backoff.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS automation_state (
                state_key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)

        # Create indexes for performance
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_tweets_screen_name ON tweets (screen_name)",
            "CREATE INDEX IF NOT EXISTS idx_tweets_thread_id ON tweets (thread_id)",
            "CREATE INDEX IF NOT EXISTS idx_tweets_processed_at ON tweets (last_processed_at)",
            "CREATE INDEX IF NOT EXISTS idx_downloads_status ON downloads (status)",
            "CREATE INDEX IF NOT EXISTS idx_downloads_updated_at ON downloads (updated_at)",
            "CREATE INDEX IF NOT EXISTS idx_llm_cache_type ON llm_cache (task_type)",
            "CREATE INDEX IF NOT EXISTS idx_llm_cache_hash ON llm_cache (content_hash)",
            "CREATE INDEX IF NOT EXISTS idx_files_type ON files_index (type)",
            "CREATE INDEX IF NOT EXISTS idx_files_source ON files_index (source_id)",
            "CREATE INDEX IF NOT EXISTS idx_bookmark_queue_status ON bookmark_queue (status)",
            "CREATE INDEX IF NOT EXISTS idx_bookmark_queue_next_attempt ON bookmark_queue (next_attempt_at)",
            "CREATE INDEX IF NOT EXISTS idx_ingestion_status ON ingestion_queue (status, next_attempt_at)",
            "CREATE INDEX IF NOT EXISTS idx_ingestion_type ON ingestion_queue (artifact_type)",
            "CREATE INDEX IF NOT EXISTS idx_ingestion_priority ON ingestion_queue (priority DESC)",
            "CREATE INDEX IF NOT EXISTS idx_transcript_chunk_context ON transcript_chunk_cache (context_id)",
            "CREATE INDEX IF NOT EXISTS idx_automation_state_updated_at ON automation_state (updated_at)"
        ]

        for index_sql in indexes:
            conn.execute(index_sql)

    def ensure_research_graph_tables(self) -> None:
        """Initialize research paper graph tables on demand."""
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS research_papers (
                    paper_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL DEFAULT '',
                    authors_json TEXT NOT NULL DEFAULT '[]',
                    abstract TEXT NOT NULL DEFAULT '',
                    doi TEXT,
                    arxiv_id TEXT,
                    pdf_url TEXT,
                    venue TEXT,
                    published_at TEXT,
                    source_provider TEXT,
                    collected BOOLEAN NOT NULL DEFAULT 0,
                    raw_payload_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS research_paper_edges (
                    source_paper_id TEXT NOT NULL,
                    target_paper_id TEXT NOT NULL,
                    edge_type TEXT NOT NULL CHECK (
                        edge_type IN ('references', 'cited_by', 'co_referenced')
                    ),
                    source_evidence TEXT NOT NULL DEFAULT '',
                    discovery_source TEXT NOT NULL DEFAULT '',
                    discovered_at TEXT NOT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (source_paper_id, target_paper_id, edge_type),
                    FOREIGN KEY (source_paper_id) REFERENCES research_papers (paper_id),
                    FOREIGN KEY (target_paper_id) REFERENCES research_papers (paper_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_research_papers_collected ON research_papers (collected)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_research_papers_doi ON research_papers (doi)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_research_papers_arxiv ON research_papers (arxiv_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_research_edges_target ON research_paper_edges (target_paper_id, edge_type)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_research_edges_source ON research_paper_edges (source_paper_id, edge_type)"
            )

    def upsert_research_paper(self, record: ResearchPaperRecord) -> None:
        """Insert or update normalized paper metadata."""
        self.ensure_research_graph_tables()
        now_iso = record.updated_at or datetime.now().isoformat()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO research_papers (
                    paper_id,
                    title,
                    authors_json,
                    abstract,
                    doi,
                    arxiv_id,
                    pdf_url,
                    venue,
                    published_at,
                    source_provider,
                    collected,
                    raw_payload_json,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(paper_id) DO UPDATE SET
                    title=CASE WHEN excluded.title != '' THEN excluded.title ELSE research_papers.title END,
                    authors_json=CASE WHEN excluded.authors_json != '[]' THEN excluded.authors_json ELSE research_papers.authors_json END,
                    abstract=CASE WHEN excluded.abstract != '' THEN excluded.abstract ELSE research_papers.abstract END,
                    doi=COALESCE(excluded.doi, research_papers.doi),
                    arxiv_id=COALESCE(excluded.arxiv_id, research_papers.arxiv_id),
                    pdf_url=COALESCE(excluded.pdf_url, research_papers.pdf_url),
                    venue=COALESCE(excluded.venue, research_papers.venue),
                    published_at=COALESCE(excluded.published_at, research_papers.published_at),
                    source_provider=COALESCE(excluded.source_provider, research_papers.source_provider),
                    collected=CASE WHEN excluded.collected THEN 1 ELSE research_papers.collected END,
                    raw_payload_json=CASE WHEN excluded.raw_payload_json != '{}' THEN excluded.raw_payload_json ELSE research_papers.raw_payload_json END,
                    updated_at=excluded.updated_at
                """,
                (
                    record.paper_id,
                    record.title or "",
                    json.dumps(list(record.authors), ensure_ascii=False),
                    record.abstract or "",
                    record.doi,
                    record.arxiv_id,
                    record.pdf_url,
                    record.venue,
                    record.published_at,
                    record.source_provider,
                    1 if record.collected else 0,
                    json.dumps(record.raw_payload or {}, ensure_ascii=False, sort_keys=True),
                    now_iso,
                ),
            )

    def get_research_paper(self, paper_id: str) -> Optional[ResearchPaperRecord]:
        """Fetch a normalized research paper record by ID."""
        self.ensure_research_graph_tables()
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM research_papers WHERE paper_id = ?",
                (paper_id,),
            ).fetchone()
            if not row:
                return None
            return self._research_paper_from_row(row)

    def upsert_research_paper_edge(self, edge: ResearchPaperEdge) -> bool:
        """Insert or update a typed paper graph edge and return true if new."""
        self.ensure_research_graph_tables()
        discovered_at = edge.discovered_at or datetime.now().isoformat()
        metadata = edge.metadata or {}
        with self._get_connection() as conn:
            before = conn.execute(
                """
                SELECT 1 FROM research_paper_edges
                WHERE source_paper_id = ? AND target_paper_id = ? AND edge_type = ?
                """,
                (edge.source_paper_id, edge.target_paper_id, edge.edge_type),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO research_paper_edges (
                    source_paper_id,
                    target_paper_id,
                    edge_type,
                    source_evidence,
                    discovery_source,
                    discovered_at,
                    metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_paper_id, target_paper_id, edge_type) DO UPDATE SET
                    source_evidence=excluded.source_evidence,
                    discovery_source=excluded.discovery_source,
                    discovered_at=excluded.discovered_at,
                    metadata_json=excluded.metadata_json
                """,
                (
                    edge.source_paper_id,
                    edge.target_paper_id,
                    edge.edge_type,
                    edge.source_evidence or "",
                    edge.discovery_source or "",
                    discovered_at,
                    json.dumps(metadata, ensure_ascii=False, sort_keys=True),
                ),
            )
            return before is None

    def list_research_paper_edges(
        self,
        *,
        source_paper_id: Optional[str] = None,
        target_paper_id: Optional[str] = None,
        edge_type: Optional[str] = None,
    ) -> list[ResearchPaperEdge]:
        """List research paper graph edges with optional filters."""
        self.ensure_research_graph_tables()
        where: list[str] = []
        params: list[Any] = []
        if source_paper_id:
            where.append("source_paper_id = ?")
            params.append(source_paper_id)
        if target_paper_id:
            where.append("target_paper_id = ?")
            params.append(target_paper_id)
        if edge_type:
            where.append("edge_type = ?")
            params.append(edge_type)
        sql = "SELECT * FROM research_paper_edges"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY discovered_at DESC, source_paper_id, target_paper_id"
        with self._get_connection() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
            return [self._research_edge_from_row(row) for row in rows]

    def list_missing_research_paper_candidates(
        self,
        *,
        min_references: int = 2,
        limit: int = 50,
    ) -> list[Dict[str, Any]]:
        """Rank referenced papers that have not been collected locally."""
        self.ensure_research_graph_tables()
        with self._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT
                    target.paper_id AS paper_id,
                    target.title AS title,
                    target.doi AS doi,
                    target.arxiv_id AS arxiv_id,
                    target.pdf_url AS pdf_url,
                    target.venue AS venue,
                    target.published_at AS published_at,
                    COUNT(DISTINCT edge.source_paper_id) AS referenced_by_count,
                    GROUP_CONCAT(DISTINCT edge.source_paper_id) AS source_ids,
                    MAX(edge.discovered_at) AS last_discovered_at
                FROM research_paper_edges AS edge
                JOIN research_papers AS source
                  ON source.paper_id = edge.source_paper_id AND source.collected = 1
                LEFT JOIN research_papers AS target
                  ON target.paper_id = edge.target_paper_id
                WHERE edge.edge_type = 'references'
                  AND COALESCE(target.collected, 0) = 0
                GROUP BY target.paper_id
                HAVING referenced_by_count >= ?
                ORDER BY referenced_by_count DESC, last_discovered_at DESC, target.paper_id ASC
                LIMIT ?
                """,
                (max(1, int(min_references)), max(1, int(limit))),
            ).fetchall()
            candidates: list[Dict[str, Any]] = []
            for row in rows:
                identifiers = {
                    "doi": row["doi"],
                    "arxiv_id": row["arxiv_id"],
                    "pdf_url": row["pdf_url"],
                }
                has_identifier = any(value for value in identifiers.values())
                source_ids = [
                    item
                    for item in str(row["source_ids"] or "").split(",")
                    if item
                ]
                candidates.append(
                    {
                        "paper_id": row["paper_id"],
                        "title": row["title"] or row["paper_id"],
                        "doi": row["doi"],
                        "arxiv_id": row["arxiv_id"],
                        "pdf_url": row["pdf_url"],
                        "venue": row["venue"],
                        "published_at": row["published_at"],
                        "referenced_by_count": int(row["referenced_by_count"] or 0),
                        "referenced_by": source_ids,
                        "last_discovered_at": row["last_discovered_at"],
                        "status": "high_confidence" if has_identifier else "ambiguous",
                        "queueable": bool(has_identifier),
                    }
                )
            return candidates

    def get_research_paper_context(self, paper_id: str) -> Dict[str, Any]:
        """Return local graph context for a paper wiki page."""
        self.ensure_research_graph_tables()
        with self._get_connection() as conn:
            referenced_by_rows = conn.execute(
                """
                SELECT source.paper_id, source.title, edge.source_evidence, edge.discovered_at
                FROM research_paper_edges AS edge
                JOIN research_papers AS source
                  ON source.paper_id = edge.source_paper_id
                WHERE edge.edge_type = 'references'
                  AND edge.target_paper_id = ?
                  AND source.collected = 1
                ORDER BY edge.discovered_at DESC, source.paper_id
                """,
                (paper_id,),
            ).fetchall()
            reference_rows = conn.execute(
                """
                SELECT target.paper_id, target.title, target.collected, edge.source_evidence, edge.discovered_at
                FROM research_paper_edges AS edge
                LEFT JOIN research_papers AS target
                  ON target.paper_id = edge.target_paper_id
                WHERE edge.edge_type = 'references'
                  AND edge.source_paper_id = ?
                ORDER BY target.collected DESC, target.title, target.paper_id
                """,
                (paper_id,),
            ).fetchall()
            co_rows = conn.execute(
                """
                SELECT target.paper_id, target.title, edge.source_evidence
                FROM research_paper_edges AS edge
                LEFT JOIN research_papers AS target
                  ON target.paper_id = edge.target_paper_id
                WHERE edge.edge_type = 'co_referenced'
                  AND edge.source_paper_id = ?
                ORDER BY target.title, target.paper_id
                """,
                (paper_id,),
            ).fetchall()
            return {
                "referenced_by": [
                    {
                        "paper_id": row["paper_id"],
                        "title": row["title"] or row["paper_id"],
                        "source_evidence": row["source_evidence"],
                        "discovered_at": row["discovered_at"],
                    }
                    for row in referenced_by_rows
                ],
                "references": [
                    {
                        "paper_id": row["paper_id"],
                        "title": row["title"] or row["paper_id"],
                        "collected": bool(row["collected"]),
                        "source_evidence": row["source_evidence"],
                        "discovered_at": row["discovered_at"],
                    }
                    for row in reference_rows
                ],
                "co_referenced": [
                    {
                        "paper_id": row["paper_id"],
                        "title": row["title"] or row["paper_id"],
                        "source_evidence": row["source_evidence"],
                    }
                    for row in co_rows
                ],
            }

    def _research_paper_from_row(self, row: sqlite3.Row) -> ResearchPaperRecord:
        return ResearchPaperRecord(
            paper_id=row["paper_id"],
            title=row["title"] or "",
            authors=tuple(json.loads(row["authors_json"] or "[]")),
            abstract=row["abstract"] or "",
            doi=row["doi"],
            arxiv_id=row["arxiv_id"],
            pdf_url=row["pdf_url"],
            venue=row["venue"],
            published_at=row["published_at"],
            source_provider=row["source_provider"],
            collected=bool(row["collected"]),
            raw_payload=json.loads(row["raw_payload_json"] or "{}"),
            updated_at=row["updated_at"],
        )

    def _research_edge_from_row(self, row: sqlite3.Row) -> ResearchPaperEdge:
        return ResearchPaperEdge(
            source_paper_id=row["source_paper_id"],
            target_paper_id=row["target_paper_id"],
            edge_type=row["edge_type"],
            source_evidence=row["source_evidence"],
            discovery_source=row["discovery_source"],
            discovered_at=row["discovered_at"],
            metadata=json.loads(row["metadata_json"] or "{}"),
        )

    def _ensure_files_index_types(self, conn: sqlite3.Connection):
        """Expand files_index type constraints when newer file types are introduced."""
        row = conn.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'files_index'
            """
        ).fetchone()
        current_sql = (row["sql"] or "") if row else ""
        if all(f"'{allowed_type}'" in current_sql for allowed_type in FILES_INDEX_ALLOWED_TYPES):
            return

        conn.execute("DROP TABLE IF EXISTS files_index_new")
        conn.execute(f"""
            CREATE TABLE files_index_new (
                path TEXT PRIMARY KEY,
                type TEXT NOT NULL CHECK (type IN ('{FILES_INDEX_TYPE_CHECK}')),
                size_bytes INTEGER NOT NULL,
                hash TEXT,
                updated_at TEXT NOT NULL,
                source_id TEXT
            )
        """)
        conn.execute("""
            INSERT INTO files_index_new (path, type, size_bytes, hash, updated_at, source_id)
            SELECT path, type, size_bytes, hash, updated_at, source_id
            FROM files_index
        """)
        conn.execute("DROP TABLE files_index")
        conn.execute("ALTER TABLE files_index_new RENAME TO files_index")

    def _ensure_ingestion_queue_statuses(self, conn: sqlite3.Connection):
        """Expand ingestion queue status constraints for quarantine states."""
        row = conn.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'table' AND name = 'ingestion_queue'
            """
        ).fetchone()
        current_sql = (row["sql"] or "") if row else ""
        if all(
            f"'{allowed_status}'" in current_sql
            for allowed_status in INGESTION_QUEUE_ALLOWED_STATUSES
        ):
            return

        conn.execute("DROP TABLE IF EXISTS ingestion_queue_new")
        conn.execute(f"""
            CREATE TABLE ingestion_queue_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                artifact_id TEXT NOT NULL UNIQUE,
                artifact_type TEXT NOT NULL,
                source TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('{INGESTION_QUEUE_STATUS_CHECK}')),
                payload_json TEXT NOT NULL,
                capabilities_json TEXT,
                attempts INTEGER DEFAULT 0,
                last_error TEXT,
                next_attempt_at TEXT,
                created_at TEXT NOT NULL,
                processed_at TEXT
            )
        """)
        conn.execute("""
            INSERT INTO ingestion_queue_new (
                id,
                artifact_id,
                artifact_type,
                source,
                priority,
                status,
                payload_json,
                capabilities_json,
                attempts,
                last_error,
                next_attempt_at,
                created_at,
                processed_at
            )
            SELECT
                id,
                artifact_id,
                artifact_type,
                source,
                priority,
                status,
                payload_json,
                capabilities_json,
                attempts,
                last_error,
                next_attempt_at,
                created_at,
                processed_at
            FROM ingestion_queue
        """)
        conn.execute("DROP TABLE ingestion_queue")
        conn.execute("ALTER TABLE ingestion_queue_new RENAME TO ingestion_queue")
    
    # GraphQL cache index operations
    def upsert_graphql_cache_entry(self, tweet_id: str, cache_path: str) -> bool:
        """Insert or update GraphQL cache index for a tweet, tracking all cache paths and timestamps"""
        try:
            with self._get_connection() as conn:
                row = conn.execute(
                    "SELECT cache_paths_json, first_cached_at FROM graphql_cache_index WHERE tweet_id = ?",
                    (tweet_id,)
                ).fetchone()
                paths: list = []
                first_cached_at: str
                now_iso = datetime.now().isoformat()
                if row:
                    try:
                        paths = json.loads(row["cache_paths_json"]) or []
                    except Exception:
                        paths = []
                    first_cached_at = row["first_cached_at"] or now_iso
                else:
                    first_cached_at = now_iso
                # Ensure unique paths
                if cache_path not in paths:
                    paths.append(cache_path)
                conn.execute(
                    """
                    INSERT INTO graphql_cache_index (tweet_id, cache_paths_json, first_cached_at, last_cached_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(tweet_id) DO UPDATE SET
                        cache_paths_json = excluded.cache_paths_json,
                        last_cached_at = excluded.last_cached_at
                    """,
                    (tweet_id, json.dumps(paths), first_cached_at, now_iso)
                )
                return True
        except Exception as e:
            logger.debug(f"Failed to upsert graphql cache index for {tweet_id}: {e}")
            return False

    def get_graphql_cache_paths(self, tweet_id: str) -> List[str]:
        """Get list of cached GraphQL paths for a tweet"""
        try:
            with self._get_connection() as conn:
                row = conn.execute(
                    "SELECT cache_paths_json FROM graphql_cache_index WHERE tweet_id = ?",
                    (tweet_id,)
                ).fetchone()
                if not row:
                    return []
                try:
                    return json.loads(row["cache_paths_json"]) or []
                except Exception:
                    return []
        except Exception as e:
            logger.debug(f"Failed to read graphql cache index for {tweet_id}: {e}")
            return []

    def replace_graphql_cache_path(self, tweet_id: str, old_path: str, new_path: str) -> bool:
        """Replace a stored GraphQL cache path with a new value."""
        try:
            with self._get_connection() as conn:
                row = conn.execute(
                    "SELECT cache_paths_json, first_cached_at FROM graphql_cache_index WHERE tweet_id = ?",
                    (tweet_id,)
                ).fetchone()
                if not row:
                    return False

                try:
                    paths = json.loads(row["cache_paths_json"]) or []
                except Exception:
                    paths = []

                changed = False
                normalized_old = str(old_path)
                normalized_new = str(new_path)
                updated_paths = []
                for path in paths:
                    if str(path) == normalized_old:
                        if normalized_new not in updated_paths:
                            updated_paths.append(normalized_new)
                            changed = True
                    elif str(path) not in updated_paths:
                        updated_paths.append(str(path))

                if normalized_new not in updated_paths:
                    updated_paths.append(normalized_new)
                    changed = True

                if not changed:
                    return True

                conn.execute(
                    """
                    UPDATE graphql_cache_index
                    SET cache_paths_json = ?, last_cached_at = ?
                    WHERE tweet_id = ?
                    """,
                    (json.dumps(updated_paths), datetime.now().isoformat(), tweet_id)
                )
                return True
        except Exception as exc:
            logger.debug(f"Failed to replace graphql cache path for {tweet_id}: {exc}")
            return False

    def get_automation_state(self, state_key: str) -> Optional[Dict[str, Any]]:
        """Return durable automation state for a specific key."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT value_json FROM automation_state WHERE state_key = ?",
                (state_key,),
            ).fetchone()
            if not row:
                return None

            payload = json.loads(row["value_json"])
            if not isinstance(payload, dict):
                raise ValueError(
                    f"automation_state payload for {state_key} must be a JSON object"
                )
            return payload

    def upsert_automation_state(self, state_key: str, payload: Dict[str, Any]) -> None:
        """Insert or update durable automation state."""
        if not isinstance(payload, dict):
            raise ValueError("automation_state payload must be a dictionary")

        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO automation_state (state_key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(state_key) DO UPDATE SET
                    value_json=excluded.value_json,
                    updated_at=excluded.updated_at
                """,
                (
                    state_key,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    datetime.now().isoformat(),
                ),
            )

    def delete_automation_state(self, state_key: str) -> bool:
        """Delete durable automation state for a specific key."""
        with self._get_connection() as conn:
            result = conn.execute(
                "DELETE FROM automation_state WHERE state_key = ?",
                (state_key,),
            )
            return result.rowcount > 0

    def ensure_archivist_corpus_tables(self) -> None:
        """Initialize archivist retrieval tables on demand."""
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS archivist_corpus_documents (
                    candidate_key TEXT PRIMARY KEY,
                    path TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    scope_relative_path TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    file_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    tags_json TEXT NOT NULL,
                    content_text TEXT NOT NULL,
                    search_corpus TEXT NOT NULL,
                    source_hash TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    source_id TEXT,
                    indexed_at TEXT NOT NULL
                )
                """
            )
            try:
                conn.execute(
                    """
                    CREATE VIRTUAL TABLE IF NOT EXISTS archivist_corpus_fts
                    USING fts5(
                        candidate_key UNINDEXED,
                        title,
                        tags,
                        path,
                        source_type,
                        content,
                        tokenize = 'unicode61'
                    )
                    """
                )
            except sqlite3.OperationalError as exc:
                raise RuntimeError(
                    "SQLite FTS5 support is required for archivist full-text retrieval"
                ) from exc

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS archivist_corpus_embeddings (
                    candidate_key TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    model TEXT NOT NULL,
                    source_hash TEXT NOT NULL,
                    vector_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (candidate_key, provider, model)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_archivist_docs_scope_path ON archivist_corpus_documents (scope, scope_relative_path)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_archivist_docs_source_type ON archivist_corpus_documents (source_type)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_archivist_embeddings_model ON archivist_corpus_embeddings (provider, model)"
            )

    def upsert_archivist_corpus_document(self, document) -> None:
        """Insert or update a parsed archivist corpus document and refresh FTS rows."""
        self.ensure_archivist_corpus_tables()
        search_corpus = document.search_corpus()
        indexed_at = datetime.now().isoformat()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO archivist_corpus_documents (
                    candidate_key,
                    path,
                    scope,
                    scope_relative_path,
                    source_type,
                    file_type,
                    title,
                    tags_json,
                    content_text,
                    search_corpus,
                    source_hash,
                    size_bytes,
                    updated_at,
                    source_id,
                    indexed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(candidate_key) DO UPDATE SET
                    path=excluded.path,
                    scope=excluded.scope,
                    scope_relative_path=excluded.scope_relative_path,
                    source_type=excluded.source_type,
                    file_type=excluded.file_type,
                    title=excluded.title,
                    tags_json=excluded.tags_json,
                    content_text=excluded.content_text,
                    search_corpus=excluded.search_corpus,
                    source_hash=excluded.source_hash,
                    size_bytes=excluded.size_bytes,
                    updated_at=excluded.updated_at,
                    source_id=excluded.source_id,
                    indexed_at=excluded.indexed_at
                """,
                (
                    document.candidate_key,
                    str(document.path),
                    document.scope,
                    document.scope_relative_path,
                    document.source_type,
                    document.file_type,
                    document.title,
                    json.dumps(list(document.tags), ensure_ascii=False),
                    document.content_text,
                    search_corpus,
                    document.source_hash,
                    document.size_bytes,
                    document.updated_at,
                    document.source_id,
                    indexed_at,
                ),
            )
            conn.execute(
                "DELETE FROM archivist_corpus_fts WHERE candidate_key = ?",
                (document.candidate_key,),
            )
            conn.execute(
                """
                INSERT INTO archivist_corpus_fts (
                    candidate_key,
                    title,
                    tags,
                    path,
                    source_type,
                    content
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    document.candidate_key,
                    document.title,
                    " ".join(document.tags),
                    document.scope_relative_path,
                    document.source_type,
                    document.content_text,
                ),
            )

    def get_archivist_corpus_document(self, candidate_key: str):
        """Fetch a parsed archivist corpus document by key."""
        from .archivist_retrieval.models import ArchivistCorpusDocument

        self.ensure_archivist_corpus_tables()
        with self._get_connection() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM archivist_corpus_documents
                WHERE candidate_key = ?
                """,
                (candidate_key,),
            ).fetchone()
            if not row:
                return None
            return ArchivistCorpusDocument(
                candidate_key=row["candidate_key"],
                path=Path(row["path"]),
                scope=row["scope"],
                scope_relative_path=row["scope_relative_path"],
                source_type=row["source_type"],
                file_type=row["file_type"],
                title=row["title"],
                tags=tuple(json.loads(row["tags_json"] or "[]")),
                content_text=row["content_text"] or "",
                source_hash=row["source_hash"],
                size_bytes=row["size_bytes"],
                updated_at=row["updated_at"],
                source_id=row["source_id"],
            )

    def list_archivist_corpus_documents(
        self,
        *,
        root_filters: tuple[tuple[str, str], ...] = (),
        source_types: tuple[str, ...] = (),
        candidate_keys: tuple[str, ...] = (),
    ):
        """List indexed archivist corpus documents with optional scope and key filters."""
        from .archivist_retrieval.models import ArchivistCorpusDocument

        self.ensure_archivist_corpus_tables()
        where_clauses: list[str] = []
        params: list[Any] = []

        if root_filters:
            root_clauses: list[str] = []
            for scope, relative_prefix in root_filters:
                if relative_prefix:
                    root_clauses.append(
                        "(scope = ? AND (scope_relative_path = ? OR scope_relative_path LIKE ?))"
                    )
                    params.extend([scope, relative_prefix, f"{relative_prefix}/%"])
                else:
                    root_clauses.append("(scope = ?)")
                    params.append(scope)
            where_clauses.append("(" + " OR ".join(root_clauses) + ")")

        if source_types:
            placeholders = ",".join("?" for _ in source_types)
            where_clauses.append(f"source_type IN ({placeholders})")
            params.extend(source_types)

        if candidate_keys:
            placeholders = ",".join("?" for _ in candidate_keys)
            where_clauses.append(f"candidate_key IN ({placeholders})")
            params.extend(candidate_keys)

        sql = "SELECT * FROM archivist_corpus_documents"
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY updated_at DESC, candidate_key DESC"

        with self._get_connection() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
            return [
                ArchivistCorpusDocument(
                    candidate_key=row["candidate_key"],
                    path=Path(row["path"]),
                    scope=row["scope"],
                    scope_relative_path=row["scope_relative_path"],
                    source_type=row["source_type"],
                    file_type=row["file_type"],
                    title=row["title"],
                    tags=tuple(json.loads(row["tags_json"] or "[]")),
                    content_text=row["content_text"] or "",
                    source_hash=row["source_hash"],
                    size_bytes=row["size_bytes"],
                    updated_at=row["updated_at"],
                    source_id=row["source_id"],
                )
                for row in rows
            ]

    def prune_archivist_corpus_documents(
        self,
        *,
        scope: str,
        relative_prefix: str,
        keep_candidate_keys: tuple[str, ...],
    ) -> int:
        """Delete indexed docs that disappeared from a scanned scope prefix."""
        self.ensure_archivist_corpus_tables()
        where_parts = ["scope = ?"]
        params: list[Any] = [scope]
        if relative_prefix:
            where_parts.append("(scope_relative_path = ? OR scope_relative_path LIKE ?)")
            params.extend([relative_prefix, f"{relative_prefix}/%"])
        if keep_candidate_keys:
            placeholders = ",".join("?" for _ in keep_candidate_keys)
            where_parts.append(f"candidate_key NOT IN ({placeholders})")
            params.extend(keep_candidate_keys)
        where_sql = " AND ".join(where_parts)

        with self._get_connection() as conn:
            rows = conn.execute(
                f"SELECT candidate_key FROM archivist_corpus_documents WHERE {where_sql}",
                tuple(params),
            ).fetchall()
            if not rows:
                return 0
            keys = tuple(row["candidate_key"] for row in rows)
            placeholders = ",".join("?" for _ in keys)
            conn.execute(
                f"DELETE FROM archivist_corpus_fts WHERE candidate_key IN ({placeholders})",
                keys,
            )
            conn.execute(
                f"DELETE FROM archivist_corpus_embeddings WHERE candidate_key IN ({placeholders})",
                keys,
            )
            deleted = conn.execute(
                f"DELETE FROM archivist_corpus_documents WHERE candidate_key IN ({placeholders})",
                keys,
            )
            return deleted.rowcount or 0

    def search_archivist_corpus_full_text(
        self,
        *,
        query: str,
        root_filters: tuple[tuple[str, str], ...] = (),
        source_types: tuple[str, ...] = (),
        limit: int = 100,
    ) -> list[tuple[Any, float]]:
        """Search the corpus FTS index and return documents with BM25-style scores."""
        from .archivist_retrieval.models import ArchivistCorpusDocument

        self.ensure_archivist_corpus_tables()
        cleaned_query = str(query or "").strip()
        if not cleaned_query:
            return []

        where_clauses = ["archivist_corpus_fts MATCH ?"]
        params: list[Any] = [cleaned_query]
        if root_filters:
            root_clauses: list[str] = []
            for scope, relative_prefix in root_filters:
                if relative_prefix:
                    root_clauses.append(
                        "(d.scope = ? AND (d.scope_relative_path = ? OR d.scope_relative_path LIKE ?))"
                    )
                    params.extend([scope, relative_prefix, f"{relative_prefix}/%"])
                else:
                    root_clauses.append("(d.scope = ?)")
                    params.append(scope)
            where_clauses.append("(" + " OR ".join(root_clauses) + ")")
        if source_types:
            placeholders = ",".join("?" for _ in source_types)
            where_clauses.append(f"d.source_type IN ({placeholders})")
            params.extend(source_types)

        sql = f"""
            SELECT
                d.*,
                bm25(archivist_corpus_fts, 5.0, 2.0, 1.0, 1.0, 0.75) AS rank_score
            FROM archivist_corpus_fts
            JOIN archivist_corpus_documents AS d
              ON d.candidate_key = archivist_corpus_fts.candidate_key
            WHERE {" AND ".join(where_clauses)}
            ORDER BY rank_score ASC
            LIMIT ?
        """
        params.append(int(limit))

        with self._get_connection() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
            results: list[tuple[Any, float]] = []
            for row in rows:
                document = ArchivistCorpusDocument(
                    candidate_key=row["candidate_key"],
                    path=Path(row["path"]),
                    scope=row["scope"],
                    scope_relative_path=row["scope_relative_path"],
                    source_type=row["source_type"],
                    file_type=row["file_type"],
                    title=row["title"],
                    tags=tuple(json.loads(row["tags_json"] or "[]")),
                    content_text=row["content_text"] or "",
                    source_hash=row["source_hash"],
                    size_bytes=row["size_bytes"],
                    updated_at=row["updated_at"],
                    source_id=row["source_id"],
                )
                rank_score = float(row["rank_score"]) if row["rank_score"] is not None else 0.0
                results.append((document, rank_score))
            return results

    def upsert_archivist_corpus_embedding(
        self,
        *,
        candidate_key: str,
        provider: str,
        model: str,
        source_hash: str,
        vector: list[float],
    ) -> None:
        """Insert or update a semantic embedding for a corpus document."""
        self.ensure_archivist_corpus_tables()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO archivist_corpus_embeddings (
                    candidate_key,
                    provider,
                    model,
                    source_hash,
                    vector_json,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(candidate_key, provider, model) DO UPDATE SET
                    source_hash=excluded.source_hash,
                    vector_json=excluded.vector_json,
                    updated_at=excluded.updated_at
                """,
                (
                    candidate_key,
                    provider,
                    model,
                    source_hash,
                    json.dumps(vector, ensure_ascii=False),
                    datetime.now().isoformat(),
                ),
            )

    def get_archivist_corpus_embeddings(
        self,
        *,
        candidate_keys: tuple[str, ...],
        provider: str,
        model: str,
    ) -> dict[str, dict[str, Any]]:
        """Fetch stored embeddings for a provider/model pair."""
        self.ensure_archivist_corpus_tables()
        if not candidate_keys:
            return {}
        placeholders = ",".join("?" for _ in candidate_keys)
        params: list[Any] = [provider, model, *candidate_keys]
        with self._get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT candidate_key, source_hash, vector_json, updated_at
                FROM archivist_corpus_embeddings
                WHERE provider = ? AND model = ? AND candidate_key IN ({placeholders})
                """,
                tuple(params),
            ).fetchall()
            return {
                row["candidate_key"]: {
                    "source_hash": row["source_hash"],
                    "vector": json.loads(row["vector_json"]),
                    "updated_at": row["updated_at"],
                }
                for row in rows
            }

    def get_archivist_corpus_stats(self) -> Dict[str, Any]:
        """Return corpus inventory counts for diagnostics."""
        self.ensure_archivist_corpus_tables()
        with self._get_connection() as conn:
            docs_row = conn.execute(
                "SELECT COUNT(*) AS count, MAX(indexed_at) AS last_indexed_at FROM archivist_corpus_documents"
            ).fetchone()
            embeddings_row = conn.execute(
                "SELECT COUNT(*) AS count, MAX(updated_at) AS last_updated_at FROM archivist_corpus_embeddings"
            ).fetchone()
            type_rows = conn.execute(
                "SELECT source_type, COUNT(*) AS count FROM archivist_corpus_documents GROUP BY source_type ORDER BY source_type"
            ).fetchall()
            return {
                "document_count": int(docs_row["count"] or 0),
                "last_indexed_at": docs_row["last_indexed_at"],
                "embedding_count": int(embeddings_row["count"] or 0),
                "last_embedding_at": embeddings_row["last_updated_at"],
                "by_source_type": {
                    row["source_type"]: int(row["count"] or 0) for row in type_rows
                },
            }
    
    # Tweet operations
    def upsert_tweet(self, tweet_meta: TweetMetadata) -> bool:
        """Insert or update tweet metadata"""
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO tweets 
                    (tweet_id, screen_name, created_at, is_thread_tweet, thread_id, file_path, last_processed_at, content_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    tweet_meta.tweet_id,
                    tweet_meta.screen_name,
                    tweet_meta.created_at,
                    tweet_meta.is_thread_tweet,
                    tweet_meta.thread_id,
                    tweet_meta.file_path,
                    tweet_meta.last_processed_at,
                    tweet_meta.content_hash
                ))
                return True
        except Exception as e:
            logger.error(f"Failed to upsert tweet {tweet_meta.tweet_id}: {e}")
            return False
    
    def get_tweet(self, tweet_id: str) -> Optional[TweetMetadata]:
        """Get tweet metadata by ID"""
        try:
            with self._get_connection() as conn:
                row = conn.execute(
                    "SELECT * FROM tweets WHERE tweet_id = ?", (tweet_id,)
                ).fetchone()
                
                if row:
                    return TweetMetadata(
                        tweet_id=row['tweet_id'],
                        screen_name=row['screen_name'],
                        created_at=row['created_at'],
                        is_thread_tweet=bool(row['is_thread_tweet']),
                        thread_id=row['thread_id'],
                        file_path=row['file_path'],
                        last_processed_at=row['last_processed_at'],
                        content_hash=row['content_hash']
                    )
                return None
        except Exception as e:
            logger.error(f"Failed to get tweet {tweet_id}: {e}")
            return None
    
    def get_tweets_by_thread(self, thread_id: str) -> List[TweetMetadata]:
        """Get all tweets in a thread"""
        try:
            with self._get_connection() as conn:
                rows = conn.execute(
                    "SELECT * FROM tweets WHERE thread_id = ? ORDER BY created_at",
                    (thread_id,)
                ).fetchall()
                
                return [TweetMetadata(
                    tweet_id=row['tweet_id'],
                    screen_name=row['screen_name'],
                    created_at=row['created_at'],
                    is_thread_tweet=bool(row['is_thread_tweet']),
                    thread_id=row['thread_id'],
                    file_path=row['file_path'],
                    last_processed_at=row['last_processed_at'],
                    content_hash=row['content_hash']
                ) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get tweets for thread {thread_id}: {e}")
            return []
    
    # Download operations
    def upsert_download(self, download_meta: DownloadMetadata) -> bool:
        """Insert or update download metadata"""
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO downloads 
                    (url, status, target_path, size_bytes, updated_at, error_msg)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    download_meta.url,
                    download_meta.status,
                    download_meta.target_path,
                    download_meta.size_bytes,
                    download_meta.updated_at or datetime.now().isoformat(),
                    download_meta.error_msg
                ))
                return True
        except Exception as e:
            logger.error(f"Failed to upsert download {download_meta.url}: {e}")
            return False

    def get_download_status(self, url: str) -> Optional[DownloadMetadata]:
        """Get download status for URL"""
        try:
            with self._get_connection() as conn:
                row = conn.execute(
                    "SELECT * FROM downloads WHERE url = ?", (url,)
                ).fetchone()

                if row:
                    return DownloadMetadata(
                        url=row['url'],
                        status=row['status'],
                        target_path=row['target_path'],
                        size_bytes=row['size_bytes'],
                        updated_at=row['updated_at'],
                        error_msg=row['error_msg']
                    )
                return None
        except Exception as e:
            logger.error(f"Failed to get download status for {url}: {e}")
            return None

    def rename_download_target(self, old_path: str, new_path: str) -> bool:
        """Update download target paths after file renames."""
        try:
            with self._get_connection() as conn:
                result = conn.execute(
                    "UPDATE downloads SET target_path = ? WHERE target_path = ?",
                    (new_path, old_path)
                )
                return result.rowcount > 0
        except Exception as exc:
            logger.debug(f"Failed to rename download target {old_path} -> {new_path}: {exc}")
            return False

    # Bookmark queue operations
    def upsert_bookmark_entry(self, entry: BookmarkQueueEntry) -> bool:
        """Insert or reset a bookmark queue entry"""
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO bookmark_queue (
                    tweet_id, source, captured_at, status, attempts, last_error,
                    last_attempt_at, processed_at, payload_json, next_attempt_at,
                    processed_with_graphql
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tweet_id) DO UPDATE SET
                    source=excluded.source,
                    captured_at=excluded.captured_at,
                    status='pending',
                    attempts=0,
                    last_error=NULL,
                    last_attempt_at=NULL,
                    processed_at=NULL,
                    payload_json=excluded.payload_json,
                    next_attempt_at=excluded.next_attempt_at,
                    processed_with_graphql=0
                """,
                (
                    entry.tweet_id,
                    entry.source,
                    entry.captured_at or datetime.now().isoformat(),
                    entry.status,
                    entry.attempts,
                    entry.last_error,
                    entry.last_attempt_at,
                    entry.processed_at,
                    entry.payload_json,
                    entry.next_attempt_at or entry.captured_at or datetime.now().isoformat(),
                    1 if entry.processed_with_graphql else 0
                )
            )
            return True

    def mark_bookmark_processing(self, tweet_id: str) -> Optional[BookmarkQueueEntry]:
        """Mark a bookmark as being processed and increment attempts."""
        now_iso = datetime.now().isoformat()
        try:
            with self._get_connection() as conn:
                conn.execute(
                    """
                    UPDATE bookmark_queue
                    SET status='processing',
                        attempts=attempts + 1,
                        last_attempt_at=?,
                        next_attempt_at=NULL
                    WHERE tweet_id = ? AND status IN ('pending', 'processing', 'failed')
                    """,
                    (now_iso, tweet_id)
                )
            return self.get_bookmark_entry(tweet_id)
        except Exception as e:
            logger.error(f"Failed to mark bookmark {tweet_id} processing: {e}")
            return None

    def mark_bookmark_processed(self, tweet_id: str, with_graphql: bool) -> bool:
        """Mark a bookmark as processed."""
        try:
            with self._get_connection() as conn:
                conn.execute(
                    """
                    UPDATE bookmark_queue
                    SET status='processed', processed_at=?, last_error=NULL, next_attempt_at=NULL
                    WHERE tweet_id = ?
                    """,
                    (datetime.now().isoformat(), tweet_id)
                )
                conn.execute(
                    "UPDATE bookmark_queue SET processed_with_graphql=? WHERE tweet_id = ?",
                    (1 if with_graphql else 0, tweet_id)
                )
                return True
        except Exception as e:
            logger.error(f"Failed to mark bookmark {tweet_id} processed: {e}")
            return False

    def mark_bookmark_failed(self, tweet_id: str, error: str, max_attempts: int = 5) -> Optional[BookmarkQueueEntry]:
        """Mark a bookmark processing attempt as failed and schedule retry."""
        try:
            entry = self.get_bookmark_entry(tweet_id)
            if not entry:
                return None

            attempts = entry.attempts
            # Determine backoff in seconds (exponential with cap)
            delay_seconds = min(300, 2 ** max(0, attempts))
            status = 'failed'
            next_attempt_at = None

            if attempts < max_attempts:
                status = 'pending'
                next_attempt_time = datetime.now() + timedelta(seconds=delay_seconds)
                next_attempt_at = next_attempt_time.isoformat()

            with self._get_connection() as conn:
                conn.execute(
                    """
                    UPDATE bookmark_queue
                    SET status=?,
                        last_error=?,
                        next_attempt_at=?,
                        processed_at=NULL,
                        processed_with_graphql=0
                    WHERE tweet_id = ?
                    """,
                    (status, error, next_attempt_at, tweet_id)
                )

            entry = self.get_bookmark_entry(tweet_id)
            return entry
        except Exception as e:
            logger.error(f"Failed to mark bookmark {tweet_id} failed: {e}")
            return None

    def get_bookmark_entry(self, tweet_id: str) -> Optional[BookmarkQueueEntry]:
        """Fetch single bookmark queue entry."""
        try:
            with self._get_connection() as conn:
                row = conn.execute(
                    "SELECT * FROM bookmark_queue WHERE tweet_id = ?",
                    (tweet_id,)
                ).fetchone()
                if not row:
                    return None
                return BookmarkQueueEntry(
                    tweet_id=row['tweet_id'],
                    source=row['source'],
                    captured_at=row['captured_at'],
                    status=row['status'],
                    attempts=row['attempts'],
                    last_error=row['last_error'],
                    last_attempt_at=row['last_attempt_at'],
                    processed_at=row['processed_at'],
                    payload_json=row['payload_json'],
                    next_attempt_at=row['next_attempt_at'],
                    processed_with_graphql=bool(row['processed_with_graphql'])
                )
        except Exception as e:
            logger.error(f"Failed to get bookmark entry {tweet_id}: {e}")
            return None

    def get_pending_bookmarks(self, limit: Optional[int] = None) -> List[BookmarkQueueEntry]:
        """Return bookmarks ready for processing (status pending and due)."""
        try:
            with self._get_connection() as conn:
                query = (
                    "SELECT * FROM bookmark_queue "
                    "WHERE status='pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?) "
                    "ORDER BY captured_at"
                )
                params: List[Any] = [datetime.now().isoformat()]
                if limit:
                    query += " LIMIT ?"
                    params.append(limit)
                rows = conn.execute(query, tuple(params)).fetchall()
                return [
                    BookmarkQueueEntry(
                        tweet_id=row['tweet_id'],
                        source=row['source'],
                        captured_at=row['captured_at'],
                        status=row['status'],
                        attempts=row['attempts'],
                        last_error=row['last_error'],
                        last_attempt_at=row['last_attempt_at'],
                        processed_at=row['processed_at'],
                        payload_json=row['payload_json'],
                        next_attempt_at=row['next_attempt_at'],
                        processed_with_graphql=bool(row['processed_with_graphql'])
                    )
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"Failed to list pending bookmarks: {e}")
            return []

    def get_unprocessed_bookmarks(self, limit: Optional[int] = None) -> List[BookmarkQueueEntry]:
        """Return bookmarks that are not processed (pending or failed)."""
        try:
            with self._get_connection() as conn:
                query = (
                    "SELECT * FROM bookmark_queue WHERE status IN ('pending', 'processing', 'failed') "
                    "ORDER BY captured_at"
                )
                params: List[Any] = []
                if limit:
                    query += " LIMIT ?"
                    params.append(limit)
                rows = conn.execute(query, tuple(params)).fetchall()
                return [
                    BookmarkQueueEntry(
                        tweet_id=row['tweet_id'],
                        source=row['source'],
                        captured_at=row['captured_at'],
                        status=row['status'],
                        attempts=row['attempts'],
                        last_error=row['last_error'],
                        last_attempt_at=row['last_attempt_at'],
                        processed_at=row['processed_at'],
                        payload_json=row['payload_json'],
                        next_attempt_at=row['next_attempt_at'],
                        processed_with_graphql=bool(row['processed_with_graphql'])
                    )
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"Failed to list unprocessed bookmarks: {e}")
            return []

    def get_bookmark_statuses(self, tweet_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Return status metadata for provided tweet IDs."""
        if not tweet_ids:
            return {}
        placeholders = ','.join('?' for _ in tweet_ids)
        try:
            with self._get_connection() as conn:
                rows = conn.execute(
                    f"SELECT * FROM bookmark_queue WHERE tweet_id IN ({placeholders})",
                    tuple(tweet_ids)
                ).fetchall()
                status_map = {}
                for row in rows:
                    status_map[row['tweet_id']] = {
                        'status': row['status'],
                        'captured_at': row['captured_at'],
                        'processed_at': row['processed_at'],
                        'attempts': row['attempts'],
                        'last_error': row['last_error'],
                        'next_attempt_at': row['next_attempt_at'],
                        'processed_with_graphql': bool(row['processed_with_graphql'])
                    }
                return status_map
        except Exception as e:
            logger.error(f"Failed to fetch bookmark statuses: {e}")
            return {}

    def get_bookmark_queue_counts(self) -> Dict[str, int]:
        """Return counts of bookmarks by status."""
        try:
            with self._get_connection() as conn:
                rows = conn.execute(
                    "SELECT status, COUNT(*) as count FROM bookmark_queue GROUP BY status"
                ).fetchall()
                counts = {row['status']: row['count'] for row in rows}
                counts.setdefault('pending', 0)
                counts.setdefault('processing', 0)
                counts.setdefault('processed', 0)
                counts.setdefault('failed', 0)
                return counts
        except Exception as e:
            logger.error(f"Failed to get bookmark queue counts: {e}")
            return {'pending': 0, 'processing': 0, 'processed': 0, 'failed': 0}
    
    def delete_bookmark_entry(self, tweet_id: str) -> bool:
        """Delete a bookmark from the queue."""
        try:
            with self._get_connection() as conn:
                conn.execute("DELETE FROM bookmark_queue WHERE tweet_id = ?", (tweet_id,))
                return True
        except Exception as e:
            logger.error(f"Failed to delete bookmark entry {tweet_id}: {e}")
            return False

    # Ingestion queue operations
    def upsert_ingestion_entry(self, entry: IngestionQueueEntry) -> bool:
        """Insert or update an ingestion queue entry."""
        try:
            existing = self.get_ingestion_entry(entry.artifact_id)
            payload_json = _ingestion_payload_with_security_metadata(entry)
            payload_json, security_policy = _ingestion_payload_with_security_policy(
                entry,
                payload_json,
                existing_payload_json=existing.payload_json if existing else None,
            )
            status = _queue_status_for_security_policy(entry.status, security_policy)
            last_error = (
                _security_policy_last_error(security_policy)
                if status in {"needs_review", "blocked"}
                else entry.last_error
            )
            with self._get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO ingestion_queue (
                        artifact_id, artifact_type, source, priority, status,
                        payload_json, capabilities_json, attempts, last_error,
                        next_attempt_at, created_at, processed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(artifact_id) DO UPDATE SET
                        artifact_type=excluded.artifact_type,
                        source=excluded.source,
                        priority=excluded.priority,
                        status=excluded.status,
                        payload_json=excluded.payload_json,
                        capabilities_json=excluded.capabilities_json,
                        attempts=excluded.attempts,
                        last_error=excluded.last_error,
                        next_attempt_at=excluded.next_attempt_at,
                        created_at=excluded.created_at,
                        processed_at=excluded.processed_at
                    """,
                    (
                        entry.artifact_id,
                        entry.artifact_type,
                        entry.source,
                        entry.priority,
                        status,
                        payload_json,
                        entry.capabilities_json,
                        entry.attempts,
                        last_error,
                        entry.next_attempt_at or entry.created_at or datetime.now().isoformat(),
                        entry.created_at or datetime.now().isoformat(),
                        entry.processed_at
                    )
                )
                return True
        except Exception as e:
            logger.error(f"Failed to upsert ingestion entry {entry.artifact_id}: {e}")
            return False

    def mark_ingestion_processing(self, artifact_id: str) -> Optional[IngestionQueueEntry]:
        """Mark an artifact as being processed and increment attempts."""
        try:
            with self._get_connection() as conn:
                conn.execute(
                    """
                    UPDATE ingestion_queue
                    SET status='processing',
                        attempts=attempts + 1,
                        next_attempt_at=NULL
                    WHERE artifact_id = ? AND status IN ('pending', 'processing', 'failed')
                    """,
                    (artifact_id,)
                )
            return self.get_ingestion_entry(artifact_id)
        except Exception as e:
            logger.error(f"Failed to mark ingestion {artifact_id} processing: {e}")
            return None

    def mark_ingestion_processed(self, artifact_id: str) -> bool:
        """Mark an artifact as processed."""
        try:
            with self._get_connection() as conn:
                result = conn.execute(
                    """
                    UPDATE ingestion_queue
                    SET status='processed', processed_at=?, last_error=NULL, next_attempt_at=NULL
                    WHERE artifact_id = ? AND status NOT IN ('needs_review', 'blocked')
                    """,
                    (datetime.now().isoformat(), artifact_id)
                )
                return (result.rowcount or 0) > 0
        except Exception as e:
            logger.error(f"Failed to mark ingestion {artifact_id} processed: {e}")
            return False

    def mark_ingestion_failed(self, artifact_id: str, error: str, max_attempts: int = 5) -> Optional[IngestionQueueEntry]:
        """Mark an ingestion attempt as failed and schedule retry."""
        try:
            entry = self.get_ingestion_entry(artifact_id)
            if not entry:
                return None
            if entry.status in {"needs_review", "blocked"}:
                return entry

            attempts = entry.attempts
            delay_seconds = min(3600, 300 * (2 ** max(0, attempts - 1)))
            status = 'failed'
            next_attempt_at = None

            if attempts < max_attempts:
                status = 'pending'
                next_attempt_time = datetime.now() + timedelta(seconds=delay_seconds)
                next_attempt_at = next_attempt_time.isoformat()

            with self._get_connection() as conn:
                conn.execute(
                    """
                    UPDATE ingestion_queue
                    SET status=?,
                        last_error=?,
                        next_attempt_at=?,
                        processed_at=NULL
                    WHERE artifact_id = ?
                    """,
                    (status, error, next_attempt_at, artifact_id)
                )

            return self.get_ingestion_entry(artifact_id)
        except Exception as e:
            logger.error(f"Failed to mark ingestion {artifact_id} failed: {e}")
            return None

    def approve_ingestion_security_override(
        self,
        artifact_id: str,
        *,
        actor: str,
        reason: str,
    ) -> Optional[IngestionQueueEntry]:
        """Move a quarantined ingestion back to pending with audited operator approval."""
        clean_actor = str(actor or "").strip()
        clean_reason = str(reason or "").strip()
        if not clean_actor:
            raise ValueError("Security override requires actor")
        if not clean_reason:
            raise ValueError("Security override requires reason")

        entry = self.get_ingestion_entry(artifact_id)
        if not entry:
            return None

        payload = _json_payload(entry.payload_json)
        normalized_metadata = payload.get("normalized_metadata")
        if not isinstance(normalized_metadata, dict):
            normalized_metadata = {}
        current_policy = prompt_security_policy_for_metadata(
            normalized_metadata,
            source_type=entry.source,
            source_label=f"{entry.artifact_type}:{entry.source}:{entry.artifact_id}",
            source_path=_payload_source_path(payload),
        )
        previous_status = str(current_policy.get("status") or entry.status)
        if previous_status not in {
            PROMPT_SECURITY_POLICY_NEEDS_REVIEW,
            PROMPT_SECURITY_POLICY_BLOCKED,
            "needs_review",
            "blocked",
        }:
            return entry

        now_iso = datetime.now().isoformat()
        approved_policy = {
            **current_policy,
            "status": PROMPT_SECURITY_POLICY_OVERRIDE_APPROVED,
            "reason": "operator_override",
            "override_actor": clean_actor,
            "override_reason": clean_reason,
            "override_at": now_iso,
        }
        normalized_metadata = merge_prompt_security_policy_metadata(
            normalized_metadata,
            approved_policy,
            audit_entry=_security_policy_audit_entry(
                action="override_approved",
                status=PROMPT_SECURITY_POLICY_OVERRIDE_APPROVED,
                reason=clean_reason,
                actor=clean_actor,
                at=now_iso,
                previous_status=previous_status,
            ),
        )
        payload["normalized_metadata"] = normalized_metadata
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)

        try:
            with self._get_connection() as conn:
                conn.execute(
                    """
                    UPDATE ingestion_queue
                    SET status='pending',
                        payload_json=?,
                        last_error=NULL,
                        next_attempt_at=?,
                        processed_at=NULL
                    WHERE artifact_id = ?
                    """,
                    (payload_json, now_iso, artifact_id),
                )
            return self.get_ingestion_entry(artifact_id)
        except Exception as e:
            logger.error(f"Failed to approve ingestion security override {artifact_id}: {e}")
            return None

    def ingestion_entry_requires_security_review(self, artifact_id: str) -> bool:
        """Return True when an ingestion entry must be excluded by default."""
        entry = self.get_ingestion_entry(artifact_id)
        if not entry:
            return False
        if entry.status in {"needs_review", "blocked"}:
            return True
        payload = _json_payload(entry.payload_json)
        normalized_metadata = payload.get("normalized_metadata")
        return bool(
            isinstance(normalized_metadata, Mapping)
            and prompt_security_requires_review(normalized_metadata)
        )

    def get_ingestion_entry(self, artifact_id: str) -> Optional[IngestionQueueEntry]:
        """Fetch single ingestion queue entry."""
        try:
            with self._get_connection() as conn:
                row = conn.execute(
                    "SELECT * FROM ingestion_queue WHERE artifact_id = ?",
                    (artifact_id,)
                ).fetchone()
                if not row:
                    return None
                return IngestionQueueEntry(
                    artifact_id=row['artifact_id'],
                    artifact_type=row['artifact_type'],
                    source=row['source'],
                    payload_json=row['payload_json'],
                    priority=row['priority'],
                    status=row['status'],
                    attempts=row['attempts'],
                    last_error=row['last_error'],
                    next_attempt_at=row['next_attempt_at'],
                    created_at=row['created_at'],
                    processed_at=row['processed_at'],
                    capabilities_json=row['capabilities_json']
                )
        except Exception as e:
            logger.error(f"Failed to get ingestion entry {artifact_id}: {e}")
            return None

    def get_pending_ingestions(self, limit: Optional[int] = None) -> List[IngestionQueueEntry]:
        """Return ingestions ready for processing (status pending and due)."""
        try:
            with self._get_connection() as conn:
                query = (
                    "SELECT * FROM ingestion_queue "
                    "WHERE status='pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?) "
                    "ORDER BY priority DESC, created_at ASC"
                )
                params: List[Any] = [datetime.now().isoformat()]
                if limit:
                    query += " LIMIT ?"
                    params.append(limit)
                rows = conn.execute(query, tuple(params)).fetchall()
                return [
                    IngestionQueueEntry(
                        artifact_id=row['artifact_id'],
                        artifact_type=row['artifact_type'],
                        source=row['source'],
                        payload_json=row['payload_json'],
                        priority=row['priority'],
                        status=row['status'],
                        attempts=row['attempts'],
                        last_error=row['last_error'],
                        next_attempt_at=row['next_attempt_at'],
                        created_at=row['created_at'],
                        processed_at=row['processed_at'],
                        capabilities_json=row['capabilities_json']
                    )
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"Failed to list pending ingestions: {e}")
            return []

    def list_ingestion_entries(
        self,
        *,
        artifact_type: Optional[str] = None,
        status: Optional[str] = None,
        source: Optional[str] = None,
        limit: int = 50,
    ) -> List[IngestionQueueEntry]:
        """List ingestion queue entries for artifact lookup surfaces."""
        where: list[str] = []
        params: list[Any] = []
        if artifact_type:
            where.append("artifact_type = ?")
            params.append(artifact_type)
        if status:
            where.append("status = ?")
            params.append(status)
        if source:
            where.append("source = ?")
            params.append(source)

        query = "SELECT * FROM ingestion_queue"
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY created_at DESC, priority DESC LIMIT ?"
        params.append(max(1, int(limit)))

        try:
            with self._get_connection() as conn:
                rows = conn.execute(query, tuple(params)).fetchall()
                return [
                    IngestionQueueEntry(
                        artifact_id=row['artifact_id'],
                        artifact_type=row['artifact_type'],
                        source=row['source'],
                        payload_json=row['payload_json'],
                        priority=row['priority'],
                        status=row['status'],
                        attempts=row['attempts'],
                        last_error=row['last_error'],
                        next_attempt_at=row['next_attempt_at'],
                        created_at=row['created_at'],
                        processed_at=row['processed_at'],
                        capabilities_json=row['capabilities_json']
                    )
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"Failed to list ingestion entries: {e}")
            return []
    
    def delete_tweet(self, tweet_id: str) -> bool:
        """Delete tweet metadata."""
        try:
            with self._get_connection() as conn:
                conn.execute("DELETE FROM tweets WHERE tweet_id = ?", (tweet_id,))
                return True
        except Exception as e:
            logger.error(f"Failed to delete tweet {tweet_id}: {e}")
            return False
    
    def delete_downloads_for_context(self, context: str) -> bool:
        """Delete all download entries for a given context."""
        try:
            with self._get_connection() as conn:
                # Check if the context column exists
                cursor = conn.execute("PRAGMA table_info(downloads)")
                columns = [row[1] for row in cursor.fetchall()]
                
                if 'context' in columns:
                    conn.execute("DELETE FROM downloads WHERE context LIKE ?", (f"%{context}%",))
                elif 'url' in columns:
                    # Fallback: delete by URL pattern if context column doesn't exist
                    conn.execute("DELETE FROM downloads WHERE url LIKE ?", (f"%{context}%",))
                
                return True
        except Exception as e:
            logger.debug(f"Could not delete downloads for context {context}: {e}")
            return False
    
    def delete_llm_cache_for_context(self, tweet_id: str) -> bool:
        """Delete LLM cache entries related to a tweet."""
        try:
            with self._get_connection() as conn:
                # Check if the context column exists
                cursor = conn.execute("PRAGMA table_info(llm_cache)")
                columns = [row[1] for row in cursor.fetchall()]
                
                if 'context' in columns:
                    conn.execute("DELETE FROM llm_cache WHERE context LIKE ?", (f"%{tweet_id}%",))
                elif 'cache_key' in columns:
                    # Fallback: delete by cache_key pattern if context column doesn't exist
                    conn.execute("DELETE FROM llm_cache WHERE cache_key LIKE ?", (f"%{tweet_id}%",))
                
                return True
        except Exception as e:
            logger.debug(f"Could not delete LLM cache for tweet {tweet_id}: {e}")
            return False
    
    # File index operations
    def upsert_file(self, file_meta: FileMetadata) -> bool:
        """Insert or update file metadata"""
        try:
            with self._get_connection() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO files_index 
                    (path, type, size_bytes, hash, updated_at, source_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    file_meta.path,
                    file_meta.file_type,
                    file_meta.size_bytes,
                    file_meta.hash,
                    file_meta.updated_at or datetime.now().isoformat(),
                    file_meta.source_id
                ))
                return True
        except Exception as e:
            logger.error(f"Failed to upsert file {file_meta.path}: {e}")
            return False

    def get_file_entry(self, path: str) -> Optional[FileMetadata]:
        """Fetch a file index entry by exact path."""
        try:
            with self._get_connection() as conn:
                row = conn.execute(
                    "SELECT * FROM files_index WHERE path = ?",
                    (path,),
                ).fetchone()
                if not row:
                    return None
                return FileMetadata(
                    path=row["path"],
                    file_type=row["type"],
                    size_bytes=row["size_bytes"],
                    hash=row["hash"],
                    updated_at=row["updated_at"],
                    source_id=row["source_id"],
                )
        except Exception as exc:
            logger.error(f"Failed to fetch file entry {path}: {exc}")
            return None

    def rename_file_entry(
        self,
        old_path: str,
        new_path: str,
        file_type: Optional[str] = None,
        size_bytes: Optional[int] = None,
        source_id: Optional[str] = None,
        file_hash: Optional[str] = None
    ) -> bool:
        """Rename an existing file entry while optionally refreshing metadata."""
        try:
            with self._get_connection() as conn:
                existing = conn.execute(
                    "SELECT * FROM files_index WHERE path = ?",
                    (old_path,)
                ).fetchone()

                if not existing:
                    return False

                updated_type = file_type or existing['type']
                updated_size = size_bytes if size_bytes is not None else existing['size_bytes']
                updated_source = source_id if source_id is not None else existing['source_id']
                updated_hash = file_hash if file_hash is not None else existing['hash']

                conn.execute(
                    """
                    UPDATE files_index
                    SET path = ?, type = ?, size_bytes = ?, hash = ?, source_id = ?, updated_at = ?
                    WHERE path = ?
                    """,
                    (
                        new_path,
                        updated_type,
                        updated_size,
                        updated_hash,
                        updated_source,
                        datetime.now().isoformat(),
                        old_path
                    )
                )
                return True
        except Exception as exc:
            logger.debug(f"Failed to rename file entry {old_path} -> {new_path}: {exc}")
            return False
    
    def get_file_stats(self) -> Dict[str, Any]:
        """Get file statistics by type"""
        try:
            with self._get_connection() as conn:
                # Count by type
                type_counts = {}
                rows = conn.execute(
                    "SELECT type, COUNT(*) as count, SUM(size_bytes) as total_size FROM files_index GROUP BY type"
                ).fetchall()
                
                for row in rows:
                    type_counts[row['type']] = {
                        'count': row['count'],
                        'total_size_bytes': row['total_size'] or 0
                    }
                
                # Total stats
                total_row = conn.execute(
                    "SELECT COUNT(*) as total_files, SUM(size_bytes) as total_size FROM files_index"
                ).fetchone()
                
                return {
                    'by_type': type_counts,
                    'total_files': total_row['total_files'],
                    'total_size_bytes': total_row['total_size'] or 0,
                    'total_size_mb': round((total_row['total_size'] or 0) / (1024 * 1024), 2)
                }
        except Exception as e:
            logger.error(f"Failed to get file stats: {e}")
            return {}

    def get_download_summary(self) -> Dict[str, Any]:
        """Aggregate download statistics by status."""
        try:
            with self._get_connection() as conn:
                rows = conn.execute(
                    "SELECT status, COUNT(*) AS count, SUM(COALESCE(size_bytes, 0)) AS total_bytes FROM downloads GROUP BY status"
                ).fetchall()
                summary = {
                    'by_status': {},
                    'total_entries': 0,
                    'total_bytes': 0
                }
                for row in rows:
                    status = row['status'] or 'unknown'
                    count = row['count'] or 0
                    total_bytes = row['total_bytes'] or 0
                    summary['by_status'][status] = {
                        'count': count,
                        'total_bytes': total_bytes,
                        'total_mb': round(total_bytes / (1024 * 1024), 2)
                    }
                    summary['total_entries'] += count
                    summary['total_bytes'] += total_bytes

                summary['total_mb'] = round(summary['total_bytes'] / (1024 * 1024), 2)
                return summary
        except Exception as exc:
            logger.error(f"Failed to summarize downloads: {exc}")
            return {}

    def get_llm_cache_stats(self) -> Dict[str, Any]:
        """Aggregate LLM cache statistics by task and provider."""
        try:
            with self._get_connection() as conn:
                by_task = {}
                rows = conn.execute(
                    "SELECT task_type, COUNT(*) AS count FROM llm_cache GROUP BY task_type"
                ).fetchall()
                for row in rows:
                    task = row['task_type'] or 'unknown'
                    by_task[task] = row['count'] or 0

                by_provider = {}
                provider_rows = conn.execute(
                    "SELECT COALESCE(model_provider, 'unknown') AS provider, COUNT(*) AS count FROM llm_cache GROUP BY provider"
                ).fetchall()
                for row in provider_rows:
                    by_provider[row['provider']] = row['count'] or 0

                total_entries = sum(by_task.values())

                recent_rows = conn.execute(
                    "SELECT cache_key, task_type, model_provider, created_at FROM llm_cache ORDER BY created_at DESC LIMIT 5"
                ).fetchall()
                recent = []
                for row in recent_rows:
                    recent.append({
                        'cache_key': row['cache_key'],
                        'task_type': row['task_type'],
                        'model_provider': row['model_provider'],
                        'created_at': row['created_at']
                    })

                return {
                    'total_entries': total_entries,
                    'by_task': by_task,
                    'by_provider': by_provider,
                    'recent_entries': recent
                }
        except Exception as exc:
            logger.error(f"Failed to summarize llm cache: {exc}")
            return {}

    def get_transcript_chunk_stats(self) -> Dict[str, Any]:
        """Summarize transcript chunk cache health."""
        try:
            with self._get_connection() as conn:
                rows = conn.execute(
                    "SELECT context_id, chunk_index, content_hash, result_json, updated_at "
                    "FROM transcript_chunk_cache"
                ).fetchall()

                contexts: Dict[str, Dict[str, Any]] = {}
                for row in rows:
                    context_id = row['context_id']
                    chunk_index = row['chunk_index']
                    updated_at = row['updated_at']
                    context = contexts.setdefault(context_id, {
                        'entries': 0,
                        'chunks_total': 0,
                        'chunks_processed': 0,
                        'failed_chunks': set(),
                        'fallback': False,
                        'last_updated': updated_at
                    })
                    context['entries'] += 1
                    context['last_updated'] = max(context['last_updated'], updated_at)
                    context['chunks_total'] = max(context['chunks_total'], chunk_index or 0)

                    data = {}
                    try:
                        data = json.loads(row['result_json'] or '{}')
                    except Exception:
                        data = {}

                    if isinstance(data, dict) and data.get('status') == 'failed':
                        context['failed_chunks'].add(chunk_index)
                        context['fallback'] = True
                        continue

                    if isinstance(data, dict):
                        meta = data.get('chunk_metadata') or {}
                        if meta:
                            context['chunks_total'] = max(context['chunks_total'], meta.get('chunks_total', 0) or context['chunks_total'])
                            context['chunks_processed'] = max(context['chunks_processed'], meta.get('chunks_processed', 0) or 0)
                            failed = meta.get('chunks_failed', 0) or 0
                            if failed:
                                failed_chunks = meta.get('failed_chunks') or []
                                for idx in failed_chunks:
                                    context['failed_chunks'].add(idx)
                                if not failed_chunks and chunk_index is not None:
                                    context['failed_chunks'].add(chunk_index)
                            if meta.get('fallback_used'):
                                context['fallback'] = True

                total_contexts = len(contexts)
                total_chunks = len(rows)
                contexts_with_failures = sum(1 for ctx in contexts.values() if ctx['failed_chunks'])
                contexts_with_fallback = sum(1 for ctx in contexts.values() if ctx['fallback'])
                total_failed_chunks = sum(len(ctx['failed_chunks']) for ctx in contexts.values())

                context_details = []
                for context_id, ctx in contexts.items():
                    if ctx['failed_chunks'] or ctx['fallback']:
                        context_details.append({
                            'context_id': context_id,
                            'chunks_total': ctx['chunks_total'],
                            'chunks_processed': ctx['chunks_processed'],
                            'failed_count': len(ctx['failed_chunks']),
                            'fallback': ctx['fallback'],
                            'failed_chunks': sorted(ctx['failed_chunks']),
                            'last_updated': ctx['last_updated']
                        })

                context_details.sort(key=lambda item: item['last_updated'], reverse=True)

                return {
                    'total_contexts': total_contexts,
                    'total_chunks': total_chunks,
                    'total_failed_chunks': total_failed_chunks,
                    'contexts_with_failures': contexts_with_failures,
                    'contexts_with_fallback': contexts_with_fallback,
                    'context_details': context_details
                }
        except Exception as exc:
            logger.error(f"Failed to summarize transcript chunks: {exc}")
            return {}
    
    # Database maintenance
    def vacuum(self) -> bool:
        """Vacuum database to reclaim space"""
        try:
            with self._get_connection() as conn:
                conn.execute("VACUUM")
                logger.info("Database vacuumed successfully")
                return True
        except Exception as e:
            logger.error(f"Failed to vacuum database: {e}")
            return False
    
    def get_db_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            with self._get_connection() as conn:
                # Table counts
                table_stats = {}
                tables = [
                    "tweets",
                    "url_mappings",
                    "downloads",
                    "llm_cache",
                    "files_index",
                    "graphql_cache_index",
                    "bookmark_queue",
                    "ingestion_queue",
                    "transcript_chunk_cache",
                ]

                for table in tables:
                    count = conn.execute(f"SELECT COUNT(*) as count FROM {table}").fetchone()['count']
                    table_stats[table] = count
                
                # Database size
                db_size = self.db_path.stat().st_size if self.db_path.exists() else 0
                
                return {
                    'db_path': str(self.db_path),
                    'db_size_bytes': db_size,
                    'db_size_mb': round(db_size / (1024 * 1024), 2),
                    'table_counts': table_stats,
                    'total_records': sum(table_stats.values())
                }
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {}

    # URL mappings operations
    def upsert_url_mapping(self, short_url: str, expanded_url: str, first_seen_tweet_id: str) -> bool:
        """Insert or update a URL mapping record"""
        try:
            with self._get_connection() as conn:
                from datetime import datetime
                conn.execute(
                    """
                    INSERT INTO url_mappings (short_url, expanded_url, first_seen_tweet_id, last_seen_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(short_url) DO UPDATE SET
                        expanded_url=excluded.expanded_url,
                        last_seen_at=excluded.last_seen_at
                    """,
                    (short_url, expanded_url, first_seen_tweet_id, datetime.now().isoformat())
                )
                return True
        except Exception as e:
            logger.debug(f"Failed to upsert url mapping {short_url}: {e}")
            return False

    # LLM cache operations
    def upsert_llm_cache(self, cache_key: str, task_type: str, content_hash: str, result_json: str, model_provider: str = None) -> bool:
        """Insert or update an LLM cache entry"""
        try:
            with self._get_connection() as conn:
                from datetime import datetime
                conn.execute(
                    """
                    INSERT INTO llm_cache (cache_key, task_type, content_hash, result_json, created_at, model_provider)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(cache_key) DO UPDATE SET
                        result_json=excluded.result_json,
                        created_at=excluded.created_at,
                        model_provider=excluded.model_provider
                    """,
                    (cache_key, task_type, content_hash, result_json, datetime.now().isoformat(), model_provider)
                )
                return True
        except Exception as e:
            logger.debug(f"Failed to upsert llm cache {cache_key}: {e}")
            return False

    def get_transcript_chunk(self, context_id: str, chunk_index: int) -> Optional[Dict[str, Any]]:
        """Fetch a cached transcript chunk result if available."""
        try:
            with self._get_connection() as conn:
                row = conn.execute(
                    "SELECT content_hash, result_json, model_provider, updated_at FROM transcript_chunk_cache WHERE context_id = ? AND chunk_index = ?",
                    (context_id, chunk_index)
                ).fetchone()
                if not row:
                    return None
                return {
                    'content_hash': row['content_hash'],
                    'result_json': row['result_json'],
                    'model_provider': row['model_provider'],
                    'updated_at': row['updated_at'],
                }
        except Exception as exc:
            logger.debug(f"Failed to read transcript chunk cache for {context_id}:{chunk_index}: {exc}")
            return None

    def upsert_transcript_chunk(
        self,
        context_id: str,
        chunk_index: int,
        content_hash: str,
        result_json: str,
        model_provider: Optional[str]
    ) -> bool:
        """Persist a transcript chunk result."""
        try:
            with self._get_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO transcript_chunk_cache (context_id, chunk_index, content_hash, result_json, updated_at, model_provider)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(context_id, chunk_index) DO UPDATE SET
                        content_hash = excluded.content_hash,
                        result_json = excluded.result_json,
                        updated_at = excluded.updated_at,
                        model_provider = excluded.model_provider
                    """,
                    (context_id, chunk_index, content_hash, result_json, datetime.now().isoformat(), model_provider)
                )
                return True
        except Exception as exc:
            logger.debug(f"Failed to upsert transcript chunk cache for {context_id}:{chunk_index}: {exc}")
            return False

    def clear_transcript_chunks(self, context_id: str) -> bool:
        """Remove cached transcript chunks for a context once processing succeeds."""
        try:
            with self._get_connection() as conn:
                conn.execute(
                    "DELETE FROM transcript_chunk_cache WHERE context_id = ?",
                    (context_id,)
                )
                return True
        except Exception as exc:
            logger.debug(f"Failed to clear transcript chunk cache for {context_id}: {exc}")
            return False


# Global metadata database instance
metadata_db = MetadataDB()


def get_metadata_db() -> MetadataDB:
    """Get the global metadata database instance"""
    return metadata_db
