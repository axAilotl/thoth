"""Small CCF metadata boundary shared by live hooks and native-data backfill.

Native keys, never UUIDs inferred from content, make repeated backfills and live
captures update the same objects. All inputs are native dictionaries; no CCF
fields are required in source files. Callers isolate failures from their native
workflows and report the failing item.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .metadata_values import portable


logger = logging.getLogger(__name__)


class CcfMetadataService:
    def __init__(self, store, *, vault_root: Path, connector_registry=None):
        self.store = store
        self.vault_root = Path(vault_root)
        self.connector_registry = connector_registry
        self.problems: list[dict] = []

    def report_problem(self, operation, native_id, error, *, path=None):
        """Keep exact per-item diagnostics for a backfill caller, and log live failures."""
        problem = {'operation': operation, 'native_id': native_id,
                   'error': f'{type(error).__name__}: {error}'}
        if path is not None:
            problem['path'] = str(path)
        self.problems.append(problem)
        logger.error('CCF %s metadata failed for %s%s: %s', operation, native_id,
                     f' at {path}' if path is not None else '', problem['error'], exc_info=True)

    def capture_queue(self, *, artifact_id, artifact_type, payload, source=None, entities=()):
        """Map a queue payload, optional capture-source dict and canonical entities.

        Reads normalized_metadata.capture_* and raw_payload (path, sha256,
        size_bytes, media_type). Returns the artifact Record. The queue ID is
        the durable native key; callers may replay rows in any order.
        """
        from .metadata_capture import capture_queue
        return capture_queue(self, artifact_id=artifact_id, artifact_type=artifact_type,
                             payload=payload, source=source, entities=entities)

    def record_file(self, path: Path, *, artifact_id=None, artifact_type='document', inferred=False):
        """Map bytes in place; missing/unreadable files raise for item reporting.

        Uses raw:<sha256 of resolved path> and blob:<same digest> native keys,
        shared by capture raw pointers and wiki inputs. An optional native
        queue artifact ID adds a derived_from link to the file Record.
        """
        from .metadata_capture import record_raw
        from .metadata_values import artifact_type as record_type
        record = record_raw(self, {'path': path}, inferred=inferred)
        if artifact_id is not None:
            artifact = self.reference(f'artifact:{artifact_id}', title=artifact_id,
                                      record_type=record_type(artifact_type))
            self.link(artifact, 'derived_from', record)
        return record

    def record_entity(self, entity, *, evidence_ids=()):
        """Map a canonical entity dict (canonical_id, entity_type, display_name)."""
        from .metadata_semantic import record_entity
        return record_entity(self, entity, evidence_ids=evidence_ids)

    def record_candidate(self, candidate, *, evidence=(), review=None):
        """Map a semantic candidate and native evidence dicts; review is actual audit data."""
        from .metadata_semantic import record_candidate
        return record_candidate(self, candidate, evidence=evidence, review=review)

    def record_artifact_review(self, *, artifact_id, artifact_type, payload, review):
        """Map a queue review audit event (action, at, actor, reason)."""
        from .metadata_semantic import record_review
        target = self.store.find(f'artifact:{artifact_id}')
        if target is None:
            target = self.capture_queue(artifact_id=artifact_id, artifact_type=artifact_type,
                                        payload=payload)
        return record_review(self, target, review)

    def record_wiki(self, *, page_path, slug, metadata, source_paths=()):
        """Map an existing wiki file and its native candidate/event/path provenance."""
        from .metadata_wiki import record_wiki
        return record_wiki(self, page_path=page_path, slug=slug, metadata=metadata,
                           source_paths=source_paths)

    def reference(self, native_key, *, title, record_type='thoth.reference', loc=None):
        """Resolve a native reference even when its content has not been ingested."""
        found = self.store.find(native_key)
        if found is not None:
            return found
        fields = {'type': record_type, 'title': str(title), 'by': 'app:thoth'}
        if loc:
            fields['loc'] = loc
        return self.store.upsert(native_key, 'record', fields)

    def link(self, origin, rel, target, *, inferred=False, data=None):
        fields = {'from': origin['id'], 'rel': rel, 'to': target['id'], 'by': 'app:thoth'}
        if inferred:
            fields['status'] = 'inferred'
        if data:
            fields['data'] = portable(data)
        return self.store.upsert(f"link:{origin['id']}:{rel}:{target['id']}", 'link', fields)
