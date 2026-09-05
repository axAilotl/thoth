"""Durable per-source knowledge, independent of Markdown publication.

The source file remains authoritative for its bytes. These records own THOTH's
derived metadata; whole-record JSON exports do not require a database restore.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

from .metadata_db import MetadataDB
from .time_utils import utc_now_iso


class SourceRecordStore:
    def __init__(self, db: MetadataDB):
        self.db = db
        with db._get_connection() as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS source_records (
                artifact_id TEXT PRIMARY KEY, canonical_id TEXT, source_type TEXT NOT NULL,
                payload_json TEXT NOT NULL, metadata_json TEXT NOT NULL,
                content_hash TEXT NOT NULL, updated_at TEXT NOT NULL)""")
            conn.execute("""CREATE TABLE IF NOT EXISTS source_record_archives (
                sha256 TEXT PRIMARY KEY, original_path TEXT NOT NULL,
                archive_path TEXT NOT NULL, document TEXT NOT NULL,
                metadata_json TEXT NOT NULL, archived_at TEXT NOT NULL)""")

    def record(self, artifact: Any, *, metadata: dict | None = None, canonical_id: str | None = None) -> None:
        self.record_payload(artifact.id, artifact.source_type, artifact.to_dict(), metadata=metadata,
                            canonical_id=canonical_id)

    def record_payload(self, artifact_id: str, source_type: str, value: dict, *, metadata: dict | None = None,
                       canonical_id: str | None = None) -> None:
        payload = json.dumps(value, ensure_ascii=False, sort_keys=True)
        details = json.dumps(metadata or {}, ensure_ascii=False, sort_keys=True)
        digest = hashlib.sha256((payload + details).encode()).hexdigest()
        with self.db._get_connection() as conn:
            conn.execute("""INSERT INTO source_records VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(artifact_id) DO UPDATE SET
                canonical_id=COALESCE(excluded.canonical_id, source_records.canonical_id),
                source_type=excluded.source_type, payload_json=excluded.payload_json,
                metadata_json=excluded.metadata_json, content_hash=excluded.content_hash,
                updated_at=excluded.updated_at
                WHERE source_records.content_hash != excluded.content_hash
                   OR (excluded.canonical_id IS NOT NULL AND source_records.canonical_id IS NULL)""",
                (artifact_id, canonical_id, source_type, payload, details, digest, utc_now_iso()))

    def archive(self, *, original_path: str, archive_path: str, document: str, metadata: dict) -> str:
        digest = hashlib.sha256(document.encode()).hexdigest()
        with self.db._get_connection() as conn:
            conn.execute("""INSERT INTO source_record_archives VALUES (?,?,?,?,?,?)
                ON CONFLICT(sha256) DO NOTHING""", (digest, original_path, archive_path,
                document, json.dumps(metadata, ensure_ascii=False, sort_keys=True), utc_now_iso()))
        return digest

    def get(self, artifact_id: str) -> dict | None:
        with self.db._get_connection() as conn:
            row = conn.execute("SELECT * FROM source_records WHERE artifact_id=?", (artifact_id,)).fetchone()
        return self._decode(row) if row else None

    @staticmethod
    def _decode(row) -> dict:
        data = dict(row)
        for key in ("payload_json", "metadata_json"):
            if key in data:
                data[key.removesuffix("_json")] = json.loads(data.pop(key))
        return data

    def export(self) -> dict:
        """Lossless, versioned control-state export; not a CCF capsule serializer."""
        with self.db._get_connection() as conn:
            records = [self._decode(row) for row in conn.execute("SELECT * FROM source_records ORDER BY artifact_id")]
            archives = [self._decode(row) for row in conn.execute("SELECT * FROM source_record_archives ORDER BY original_path,sha256")]
        return {"schema": "thoth.source-records/v1", "records": records, "archives": archives}
