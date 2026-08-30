"""Supervised local read-only Keeper profile for Cissa queries.

This module provides a read-only query surface over the Thoth archivist corpus.
It opens the SQLite metadata database in URI read-only mode (``mode=ro``) and
never constructs a ``MetadataDB``, runs migrations, starts ingestion, loads
providers or connectors, accesses the network, or exposes mutating tools.

Production entrypoint::

    python thoth_keeper.py --db <path> --roots <roots>

Example::

    python thoth_keeper.py \\
        --db ./.thoth_system/meta.db \\
        --roots vault/transcripts,raw/cissa
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")
_MAX_QUERY_RESULTS = 100
_MAX_PASSAGE_CHARS = 8_000
_MAX_QUERY_TIMEOUT_MS = 60_000


class KeeperProfileError(RuntimeError):
    """Raised when a keeper request cannot be fulfilled safely."""


@dataclass(frozen=True)
class KeeperRoot:
    """An explicitly allowed archivist root."""

    scope: str
    relative_prefix: str = ""


@dataclass(frozen=True)
class KeeperPassage:
    """A bounded passage from the corpus with full provenance."""

    candidate_key: str
    artifact_id: str | None
    event_id: str | None
    source_id: str | None
    source_key: str
    source_type: str
    scope: str
    scope_relative_path: str
    path: str
    title: str
    tags: tuple[str, ...]
    snippet: str
    selector: str
    trust_score: float
    trust_reason: str
    security_status: str
    privacy_class: str
    retention_class: str
    provenance: dict[str, Any]
    score: float


@dataclass(frozen=True)
class KeeperQueryResult:
    """Result of a keeper corpus query."""

    query: str
    status: str  # ok, empty_query, stale_index, unavailable_storage, error
    total: int
    passages: tuple[KeeperPassage, ...]
    readiness: str


@dataclass(frozen=True)
class KeeperReadiness:
    """Readiness state of the keeper storage surface."""

    status: str  # ready, stale_index, unavailable_storage
    document_count: int
    last_indexed_at: str | None
    reason: str | None = None


@dataclass(frozen=True)
class _CorpusDocument:
    candidate_key: str
    path: Path
    scope: str
    scope_relative_path: str
    source_type: str
    title: str
    tags: tuple[str, ...]
    content_text: str
    source_hash: str
    source_id: str | None
    source_key: str
    source_trust_score: float
    source_trust_reason: str
    source_security_status: str
    artifact_id: str | None
    event_id: str | None
    privacy_class: str
    retention_class: str


class KeeperProfileConfig:
    """Validated configuration for a supervised keeper profile."""

    def __init__(
        self,
        db_path: str,
        allowed_roots: list[str],
        *,
        query_timeout_ms: int = 10_000,
        max_passage_chars: int = 2_000,
        stale_index_seconds: int = 7 * 24 * 3_600,
    ):
        self.db_path = Path(db_path)
        self.allowed_roots = _parse_roots(allowed_roots)
        self.query_timeout_ms = _bounded_positive_int(
            query_timeout_ms,
            name="query_timeout_ms",
            maximum=_MAX_QUERY_TIMEOUT_MS,
        )
        self.max_passage_chars = _bounded_positive_int(
            max_passage_chars,
            name="max_passage_chars",
            maximum=_MAX_PASSAGE_CHARS,
        )
        self.stale_index_seconds = _bounded_positive_int(
            stale_index_seconds,
            name="stale_index_seconds",
        )


class KeeperProfile:
    """Supervised read-only keeper profile over the archivist corpus."""

    def __init__(self, profile_config: KeeperProfileConfig):
        self.config = profile_config
        self._db_uri = self._build_uri(profile_config.db_path)

    def _build_uri(self, db_path: Path) -> str:
        try:
            absolute = db_path.resolve().as_posix()
        except Exception as exc:
            raise KeeperProfileError(f"Invalid database path: {db_path}: {exc}") from exc
        return f"file:{absolute}?mode=ro"

    def _open_connection(
        self,
        cancel_event: threading.Event | None = None,
    ) -> sqlite3.Connection:
        """Open a read-only connection with progress-based timeout/cancellation."""
        if not self.config.db_path.exists():
            raise KeeperProfileError(f"Storage unavailable: {self.config.db_path}")
        try:
            conn = sqlite3.connect(self._db_uri, uri=True, timeout=5.0)
        except sqlite3.OperationalError as exc:
            raise KeeperProfileError(f"Cannot open storage read-only: {exc}") from exc
        except Exception as exc:
            raise KeeperProfileError(f"Cannot open storage: {exc}") from exc

        conn.row_factory = sqlite3.Row
        cancelled = cancel_event or threading.Event()
        if cancelled.is_set():
            conn.close()
            raise KeeperProfileError("Query cancelled")
        deadline = time.monotonic() + (self.config.query_timeout_ms / 1_000.0)

        def _progress_handler() -> int:
            if cancelled.is_set():
                return 1
            if time.monotonic() > deadline:
                return 1
            return 0

        conn.set_progress_handler(_progress_handler, 50)
        return conn

    def _root_filter(self, *, table_alias: str = "d") -> tuple[str, list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        for root in self.config.allowed_roots:
            if root.relative_prefix:
                clauses.append(
                    f"({table_alias}.scope = ? AND "
                    f"({table_alias}.scope_relative_path = ? OR "
                    f"{table_alias}.scope_relative_path LIKE ?))"
                )
                params.extend(
                    [root.scope, root.relative_prefix, f"{root.relative_prefix}/%"]
                )
            else:
                clauses.append(f"({table_alias}.scope = ?)")
                params.append(root.scope)
        return f"({' OR '.join(clauses)})", params

    def readiness(self) -> KeeperReadiness:
        """Check readiness without mutating state."""
        if not self.config.db_path.exists():
            return KeeperReadiness(
                status="unavailable_storage",
                document_count=0,
                last_indexed_at=None,
                reason=f"database file not found: {self.config.db_path}",
            )

        conn: sqlite3.Connection | None = None
        try:
            conn = self._open_connection()
        except KeeperProfileError as exc:
            return KeeperReadiness(
                status="unavailable_storage",
                document_count=0,
                last_indexed_at=None,
                reason=str(exc),
            )

        try:
            with conn:
                tables = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (?, ?)",
                    ("archivist_corpus_documents", "archivist_corpus_fts"),
                ).fetchall()
                table_names = {row["name"] for row in tables}
                if "archivist_corpus_documents" not in table_names:
                    return KeeperReadiness(
                        status="unavailable_storage",
                        document_count=0,
                        last_indexed_at=None,
                        reason="archivist_corpus_documents table missing",
                    )
                if "archivist_corpus_fts" not in table_names:
                    return KeeperReadiness(
                        status="unavailable_storage",
                        document_count=0,
                        last_indexed_at=None,
                        reason="archivist_corpus_fts table missing",
                    )

                root_filter, root_params = self._root_filter(
                    table_alias="archivist_corpus_documents"
                )
                row = conn.execute(
                    "SELECT COUNT(*) AS count, MAX(indexed_at) AS last_indexed_at "
                    "FROM archivist_corpus_documents WHERE " + root_filter,
                    tuple(root_params),
                ).fetchone()
                count = int(row["count"] or 0)
                last_indexed_at = row["last_indexed_at"]

                if count == 0:
                    return KeeperReadiness(
                        status="stale_index",
                        document_count=0,
                        last_indexed_at=last_indexed_at,
                        reason="corpus contains no documents",
                    )

                age_seconds = None
                if last_indexed_at:
                    try:
                        last = datetime.fromisoformat(last_indexed_at.replace("Z", "+00:00"))
                        if last.tzinfo is None:
                            last = last.replace(tzinfo=timezone.utc)
                        age_seconds = (datetime.now(timezone.utc) - last).total_seconds()
                    except ValueError:
                        return KeeperReadiness(
                            status="stale_index",
                            document_count=count,
                            last_indexed_at=last_indexed_at,
                            reason="index timestamp is invalid",
                        )

                if age_seconds is not None and age_seconds > self.config.stale_index_seconds:
                    return KeeperReadiness(
                        status="stale_index",
                        document_count=count,
                        last_indexed_at=last_indexed_at,
                        reason=f"index last updated {int(age_seconds)}s ago",
                    )

                return KeeperReadiness(
                    status="ready",
                    document_count=count,
                    last_indexed_at=last_indexed_at,
                )
        except Exception as exc:
            return KeeperReadiness(
                status="unavailable_storage",
                document_count=0,
                last_indexed_at=None,
                reason=f"readiness query failed: {exc}",
            )
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def query(
        self,
        query_text: str,
        *,
        limit: int = 10,
        cancel_event: threading.Event | None = None,
    ) -> KeeperQueryResult:
        """Query the corpus within allowed roots and return bounded passages."""
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise KeeperProfileError("limit must be an integer")
        if limit < 1 or limit > _MAX_QUERY_RESULTS:
            raise KeeperProfileError(
                f"limit must be between 1 and {_MAX_QUERY_RESULTS}"
            )
        if cancel_event is not None and cancel_event.is_set():
            raise KeeperProfileError("Query cancelled")
        readiness = self.readiness()
        if readiness.status == "unavailable_storage":
            return KeeperQueryResult(
                query=query_text,
                status="unavailable_storage",
                total=0,
                passages=(),
                readiness=readiness.status,
            )
        if readiness.status == "stale_index":
            return KeeperQueryResult(
                query=query_text,
                status="stale_index",
                total=0,
                passages=(),
                readiness=readiness.status,
            )

        expression = _build_match_expression(query_text)
        if not expression:
            return KeeperQueryResult(
                query=query_text,
                status="empty_query",
                total=0,
                passages=(),
                readiness=readiness.status,
            )

        conn: sqlite3.Connection | None = None
        try:
            conn = self._open_connection(cancel_event)
        except KeeperProfileError as exc:
            raise KeeperProfileError(f"Cannot open storage for query: {exc}") from exc

        try:
            with conn:
                root_filter, params = self._root_filter(table_alias="d")

                sql = f"""
                    SELECT
                        d.*,
                        bm25(archivist_corpus_fts, 5.0, 2.0, 1.0, 1.0, 0.75) AS rank_score
                    FROM archivist_corpus_fts
                    JOIN archivist_corpus_documents AS d
                      ON d.candidate_key = archivist_corpus_fts.candidate_key
                    WHERE archivist_corpus_fts MATCH ?
                      AND {root_filter}
                      AND d.source_security_status NOT IN (?, ?, ?, ?)
                      AND d.source_trust_score > 0.0
                    ORDER BY rank_score ASC
                    LIMIT ?
                """
                params = [
                    expression,
                    *params,
                    "blocked",
                    "needs_review",
                    "rejected",
                    "quarantined",
                    int(limit),
                ]
                rows = conn.execute(sql, tuple(params)).fetchall()
        except sqlite3.OperationalError as exc:
            if "interrupted" in str(exc).lower():
                if cancel_event is not None and cancel_event.is_set():
                    raise KeeperProfileError("Query cancelled") from exc
                raise KeeperProfileError("Query timed out") from exc
            raise KeeperProfileError(f"Query failed: {exc}") from exc
        except Exception as exc:
            raise KeeperProfileError(f"Query failed: {exc}") from exc
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

        passages = [
            _materialize_passage(
                _row_to_document(row),
                row["rank_score"],
                self.config.max_passage_chars,
            )
            for row in rows
        ]
        return KeeperQueryResult(
            query=query_text,
            status="ok",
            total=len(passages),
            passages=tuple(passages),
            readiness=readiness.status,
        )


def _parse_roots(roots: list[str]) -> tuple[KeeperRoot, ...]:
    """Parse root specs like ``vault/transcripts`` or ``raw/cissa``."""
    parsed: list[KeeperRoot] = []
    seen: set[tuple[str, str]] = set()
    for spec in roots:
        if not isinstance(spec, str):
            raise KeeperProfileError("Allowed roots must be strings")
        spec = spec.strip().strip("/")
        if not spec:
            raise KeeperProfileError("Empty root spec is not allowed")
        parts = spec.split("/", 1)
        scope = parts[0]
        relative_prefix = parts[1] if len(parts) > 1 else ""
        if scope not in {"vault", "raw", "library"}:
            raise KeeperProfileError(
                f"Unsupported root scope: {scope!r} in {spec!r}; "
                f"expected one of vault, raw, library"
            )
        key = (scope, relative_prefix)
        if key in seen:
            continue
        seen.add(key)
        parsed.append(KeeperRoot(scope=scope, relative_prefix=relative_prefix))
    if not parsed:
        raise KeeperProfileError("At least one allowed root is required")
    return tuple(parsed)


def _bounded_positive_int(
    value: Any,
    *,
    name: str,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise KeeperProfileError(f"{name} must be an integer")
    if value < 1 or (maximum is not None and value > maximum):
        suffix = f" and at most {maximum}" if maximum is not None else ""
        raise KeeperProfileError(f"{name} must be positive{suffix}")
    return value


def _build_match_expression(query_text: str) -> str:
    """Build a safe FTS5 MATCH expression from free text."""
    text = str(query_text or "").strip()
    if not text:
        return ""
    tokens = tuple(dict.fromkeys(_TOKEN_RE.findall(text.lower())))
    return " OR ".join(f'"{token}"' for token in tokens)


def _row_to_document(row: sqlite3.Row) -> _CorpusDocument:
    return _CorpusDocument(
        candidate_key=row["candidate_key"],
        path=Path(row["path"]),
        scope=row["scope"],
        scope_relative_path=row["scope_relative_path"],
        source_type=row["source_type"],
        title=row["title"],
        tags=tuple(json.loads(row["tags_json"] or "[]")),
        content_text=row["content_text"] or "",
        source_hash=row["source_hash"],
        source_id=row["source_id"],
        source_key=row["source_key"] or "",
        source_trust_score=_float_or_default(row["source_trust_score"], 1.0),
        source_trust_reason=row["source_trust_reason"] or "prompt_security_allowed",
        source_security_status=row["source_security_status"] or "allowed",
        artifact_id=row["artifact_id"],
        event_id=row["event_id"],
        privacy_class=row["privacy_class"] or "unspecified",
        retention_class=row["retention_class"] or "unspecified",
    )


def _float_or_default(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _materialize_passage(
    document: _CorpusDocument,
    rank_score: Any,
    max_chars: int,
) -> KeeperPassage:
    snippet = _bounded_snippet(document.content_text, max_chars)
    artifact_id = document.artifact_id or document.candidate_key
    selector = f"{document.candidate_key}#char=0-{len(snippet)}"
    return KeeperPassage(
        candidate_key=document.candidate_key,
        artifact_id=artifact_id,
        event_id=document.event_id,
        source_id=document.source_id,
        source_key=document.source_key,
        source_type=document.source_type,
        scope=document.scope,
        scope_relative_path=document.scope_relative_path,
        path=str(document.path),
        title=document.title,
        tags=document.tags,
        snippet=snippet,
        selector=selector,
        trust_score=document.source_trust_score,
        trust_reason=document.source_trust_reason,
        security_status=document.source_security_status,
        privacy_class=document.privacy_class,
        retention_class=document.retention_class,
        provenance={
            "candidate_key": document.candidate_key,
            "source_type": document.source_type,
            "source_id": document.source_id,
            "source_key": document.source_key,
            "artifact_id": artifact_id,
            "event_id": document.event_id,
            "source_hash": document.source_hash,
            "privacy_class": document.privacy_class,
            "retention_class": document.retention_class,
            "scope": document.scope,
            "scope_relative_path": document.scope_relative_path,
        },
        score=float(rank_score) if rank_score is not None else 0.0,
    )


def _bounded_snippet(text: str, max_chars: int) -> str:
    text = str(text or "").strip()
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    for delimiter in (". ", "? ", "! ", "\n\n"):
        idx = truncated.rfind(delimiter)
        if idx > max_chars // 2:
            return truncated[: idx + len(delimiter)].rstrip()
    space_idx = truncated.rfind(" ")
    if space_idx > max_chars // 2:
        return truncated[:space_idx].rstrip() + " …"
    return truncated.rstrip() + " …"
