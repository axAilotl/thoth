"""
SQLite Metadata Database - Persistent metadata for efficient re-runs and browser/API usage
Stores tweets, downloads, LLM cache, files index, and more for fast lookups
"""

import sqlite3
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from contextlib import contextmanager
from dataclasses import dataclass

from .config import config
from .path_layout import build_path_layout

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
                status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'processed', 'failed')),
                payload_json TEXT NOT NULL,
                capabilities_json TEXT,
                attempts INTEGER DEFAULT 0,
                last_error TEXT,
                next_attempt_at TEXT,
                created_at TEXT NOT NULL,
                processed_at TEXT
            )
        """)

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
                        entry.status,
                        entry.payload_json,
                        entry.capabilities_json,
                        entry.attempts,
                        entry.last_error,
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
                conn.execute(
                    """
                    UPDATE ingestion_queue
                    SET status='processed', processed_at=?, last_error=NULL, next_attempt_at=NULL
                    WHERE artifact_id = ?
                    """,
                    (datetime.now().isoformat(), artifact_id)
                )
                return True
        except Exception as e:
            logger.error(f"Failed to mark ingestion {artifact_id} processed: {e}")
            return False

    def mark_ingestion_failed(self, artifact_id: str, error: str, max_attempts: int = 5) -> Optional[IngestionQueueEntry]:
        """Mark an ingestion attempt as failed and schedule retry."""
        try:
            entry = self.get_ingestion_entry(artifact_id)
            if not entry:
                return None

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
