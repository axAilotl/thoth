"""Conservative wiki publication and durable revisions outside the reading vault.

The shared MetadataDB owns connections; this capability owns its four tables.
Only an explicit hash-checked adoption can establish a baseline for an old page.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile

from .metadata_db import MetadataDB
from .wiki_feedback import FeedbackBlock, comparable_body, split_feedback


class WikiPublicationConflict(ValueError):
    """The page cannot be replaced without risking human work."""


@dataclass(frozen=True)
class PublicationSnapshot:
    page_key: str
    expected_hash: str | None
    status: str
    feedback: tuple[FeedbackBlock, ...] = ()
    pending_feedback: bool = False

    @property
    def publishable(self) -> bool:
        return self.status in {"new", "clean", "feedback_changed"}


def content_hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def minimal_wiki_frontmatter(metadata: dict) -> dict:
    """Shared reading-surface projection for publication and explicit migration."""
    return {key: metadata[key] for key in (
        "thoth_id", "title", "thoth_kind", "thoth_type", "thoth_updated_at"
    ) if key in metadata}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class WikiPublicationStore:
    def __init__(self, db: MetadataDB, wiki_root: Path):
        self.db = db
        self.wiki_root = wiki_root.absolute()
        with self.db._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS wiki_publications (
                    page_key TEXT PRIMARY KEY, baseline_hash TEXT NOT NULL,
                    baseline_text TEXT NOT NULL, updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS wiki_publication_revisions (
                    page_key TEXT NOT NULL, content_hash TEXT NOT NULL,
                    content_text TEXT NOT NULL, origin TEXT NOT NULL,
                    recorded_at TEXT NOT NULL, PRIMARY KEY(page_key, content_hash)
                );
                CREATE TABLE IF NOT EXISTS wiki_feedback (
                    page_key TEXT NOT NULL, feedback_id TEXT NOT NULL,
                    raw_text TEXT NOT NULL, request_text TEXT NOT NULL,
                    status TEXT NOT NULL, active INTEGER NOT NULL,
                    first_seen_at TEXT NOT NULL, last_seen_at TEXT NOT NULL,
                    included_revision TEXT,
                    PRIMARY KEY(page_key, feedback_id)
                );
                CREATE TABLE IF NOT EXISTS wiki_publication_metadata (
                    page_key TEXT NOT NULL, content_hash TEXT NOT NULL,
                    metadata_json TEXT NOT NULL, PRIMARY KEY(page_key, content_hash)
                );
            """)

    def _path(self, page_path: Path) -> tuple[Path, str]:
        path = Path(page_path).absolute()
        try:
            relative = path.relative_to(self.wiki_root)
        except ValueError as exc:
            raise WikiPublicationConflict("Wiki page is outside the configured wiki root") from exc
        if ".." in relative.parts or path.suffix.lower() != ".md":
            raise WikiPublicationConflict("Wiki publication requires a contained Markdown path")
        # A symlinked vault root can be intentional; descendants must not redirect writes.
        for ancestor in (path, *path.parents):
            if ancestor == self.wiki_root:
                break
            if ancestor.is_symlink():
                raise WikiPublicationConflict("Wiki publication refuses symlinked descendants")
        return path, relative.as_posix()

    @contextmanager
    def _lock(self, page_key: str):
        lock_root = Path(self.db.db_path).absolute().parent / "wiki-publication-locks"
        if lock_root.resolve().is_relative_to(self.wiki_root.resolve()):
            raise WikiPublicationConflict("Wiki publication database/control directory must be outside the wiki")
        lock_root.mkdir(parents=True, exist_ok=True)
        with (lock_root / content_hash(page_key)).open("a") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _read(path: Path) -> str | None:
        try:
            # Preserve exact line endings in revisions and hashes.
            with path.open(encoding="utf-8", newline="") as handle:
                return handle.read()
        except FileNotFoundError:
            return None

    @staticmethod
    def _revision(conn, key: str, text: str, origin: str):
        conn.execute(
            "INSERT OR IGNORE INTO wiki_publication_revisions VALUES (?, ?, ?, ?, ?)",
            (key, content_hash(text), text, origin, _now()),
        )

    def inspect(self, page_path: Path) -> PublicationSnapshot:
        path, key = self._path(page_path)
        with self._lock(key), self.db._get_connection() as conn:
            return self._inspect(conn, path, key)

    def _inspect(self, conn, path: Path, key: str) -> PublicationSnapshot:
        text = self._read(path)
        baseline = conn.execute("SELECT * FROM wiki_publications WHERE page_key = ?", (key,)).fetchone()
        if text is None:
            return PublicationSnapshot(key, None, "missing" if baseline else "new")
        digest = content_hash(text)
        self._revision(conn, key, text, "observed")
        if baseline is None:
            # Do not elevate annotations in an unowned/imported file to user requests.
            return PublicationSnapshot(key, digest, "unowned")
        _, blocks = split_feedback(text)
        ids = {block.id for block in blocks}
        inactive_ids = {row[0] for row in conn.execute(
            "SELECT feedback_id FROM wiki_feedback WHERE page_key = ? AND active = 0", (key,)
        )}
        conn.execute("UPDATE wiki_feedback SET active = 0 WHERE page_key = ?", (key,))
        for block in blocks:
            conn.execute("""
                INSERT INTO wiki_feedback VALUES (?, ?, ?, ?, 'pending', 1, ?, ?, NULL)
                ON CONFLICT(page_key, feedback_id) DO UPDATE SET active = 1, last_seen_at = excluded.last_seen_at
            """, (key, block.id, block.raw_text, block.text, _now(), _now()))
            if block.id in inactive_ids:
                conn.execute("UPDATE wiki_feedback SET status = 'pending' WHERE page_key = ? AND feedback_id = ?",
                             (key, block.id))
        pending = any(row["feedback_id"] in ids and row["status"] == "pending" for row in conn.execute(
            "SELECT feedback_id, status FROM wiki_feedback WHERE page_key = ?", (key,)
        ))
        if digest == baseline["baseline_hash"]:
            status = "clean"
        elif comparable_body(text) == comparable_body(baseline["baseline_text"]):
            status = "feedback_changed"
        else:
            status = "user_modified"
        return PublicationSnapshot(key, digest, status, blocks, pending)

    def adopt_baseline(self, page_path: Path, *, expected_hash: str, metadata: dict | None = None) -> PublicationSnapshot:
        """Explicit operator adoption: preserve bytes, refuse stale/mismatched hashes.

        Adoption does NOT approve overwriting existing human prose. Operators must
        establish that the page is generated-only before making this call.
        """
        path, key = self._path(page_path)
        with self._lock(key), self.db._get_connection() as conn:
            text = self._read(path)
            if text is None or content_hash(text) != expected_hash:
                raise WikiPublicationConflict("Wiki page changed before baseline adoption")
            if conn.execute("SELECT 1 FROM wiki_publications WHERE page_key = ?", (key,)).fetchone():
                raise WikiPublicationConflict("A baseline already exists; adoption cannot clear user-edit conflicts")
            self._revision(conn, key, text, "adopted")
            conn.execute("INSERT INTO wiki_publications VALUES (?, ?, ?, ?)", (key, expected_hash, text, _now()))
            if metadata is not None:
                conn.execute("INSERT INTO wiki_publication_metadata VALUES (?, ?, ?)",
                             (key, expected_hash, json.dumps(metadata, ensure_ascii=False)))
            return self._inspect(conn, path, key)

    def publish(self, page_path: Path, generated_content: str, *, snapshot: PublicationSnapshot,
                metadata: dict | None = None) -> str:
        """Publish only against the exact observed revision, retaining raw feedback.

        DB locks serialize THOTH writers. External editors do not share those locks;
        the final hash check sits immediately before atomic replacement. Snapshots
        retain every observed version for recovery, never in sidecars in the vault.
        """
        path, key = self._path(page_path)
        if key != snapshot.page_key or not snapshot.publishable:
            raise WikiPublicationConflict(f"Wiki publication blocked: {snapshot.status}")
        if split_feedback(generated_content)[1]:
            raise WikiPublicationConflict("Generated output cannot create human feedback callouts")
        content = generated_content.rstrip() + "\n"
        if snapshot.feedback:
            content += "\n" + "\n".join(block.raw_text for block in snapshot.feedback)
        digest = content_hash(content)
        metadata_json = json.dumps(metadata or {}, ensure_ascii=False)
        with self._lock(key), self.db._get_connection() as conn:
            current = self._inspect(conn, path, key)
            if not current.publishable or current.expected_hash != snapshot.expected_hash:
                raise WikiPublicationConflict("Wiki page changed during compilation; publication preserved it")
            path.parent.mkdir(parents=True, exist_ok=True)
            fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
            try:
                with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
                self._path(path)  # Revalidate against a changed symlink parent.
                latest = self._read(path)
                if (content_hash(latest) if latest is not None else None) != snapshot.expected_hash:
                    raise WikiPublicationConflict("Wiki page changed immediately before publication")
                os.replace(temporary, path)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
            self._revision(conn, key, content, "generated")
            conn.execute("INSERT OR REPLACE INTO wiki_publication_metadata VALUES (?, ?, ?)",
                         (key, digest, metadata_json))
            conn.execute("""
                INSERT INTO wiki_publications VALUES (?, ?, ?, ?)
                ON CONFLICT(page_key) DO UPDATE SET baseline_hash=excluded.baseline_hash,
                    baseline_text=excluded.baseline_text, updated_at=excluded.updated_at
            """, (key, digest, content, _now()))
            for block in snapshot.feedback:
                # Inclusion is observable; fulfillment is not inferred from model success.
                conn.execute("""UPDATE wiki_feedback SET status = CASE WHEN status = 'pending'
                    THEN 'included' ELSE status END, included_revision = ?
                    WHERE page_key = ? AND feedback_id = ?""", (digest, key, block.id))
        return digest

    def metadata_for(self, page_path: Path) -> dict:
        _, key = self._path(page_path)
        with self.db._get_connection() as conn:
            row = conn.execute("""SELECT metadata_json FROM wiki_publication_metadata m
                JOIN wiki_publications p ON m.page_key = p.page_key AND m.content_hash = p.baseline_hash
                WHERE p.page_key = ?""", (key,)).fetchone()
            return json.loads(row[0]) if row else {}

    def feedback_records(self, page_path: Path) -> list[dict]:
        _, key = self._path(page_path)
        with self.db._get_connection() as conn:
            return [dict(row) for row in conn.execute(
                "SELECT * FROM wiki_feedback WHERE page_key = ? ORDER BY first_seen_at, feedback_id", (key,)
            )]

    def set_feedback_status(self, page_path: Path, feedback_id: str, status: str) -> None:
        if status not in {"pending", "included", "addressed", "needs_clarification"}:
            raise ValueError("Unsupported wiki feedback status")
        _, key = self._path(page_path)
        with self.db._get_connection() as conn:
            result = conn.execute("UPDATE wiki_feedback SET status = ? WHERE page_key = ? AND feedback_id = ?",
                                  (status, key, feedback_id))
            if not result.rowcount:
                raise ValueError("Unknown wiki feedback record")

    def known_paths(self) -> tuple[Path, ...]:
        with self.db._get_connection() as conn:
            return tuple(self.wiki_root / row[0] for row in conn.execute("SELECT page_key FROM wiki_publications"))
