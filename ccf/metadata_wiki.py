"""CCF records for generated wiki files and their native input references."""

from __future__ import annotations

from pathlib import Path

from .metadata_values import content_path, location, portable
from .objects import hash_bytes


def record_wiki(service, *, page_path, slug, metadata, source_paths=()):
    path = Path(page_path).resolve()
    record = service.store.upsert(f'wiki:{path}', 'record', {
        'type': 'note', 'title': str(metadata.get('title') or slug),
        'loc': location(path, service.vault_root), 'hash': hash_bytes(path.read_bytes()),
        'data': portable(metadata), 'status': 'inferred', 'by': 'app:thoth',
    })
    for native_id in metadata.get('thoth_semantic_candidate_ids') or ():
        source = service.reference(f'candidate:{native_id}', title=native_id)
        service.link(record, 'derived_from', source, inferred=True)
    for native_id in metadata.get('thoth_event_ids') or ():
        source = service.reference(f'event:{native_id}', title=native_id, record_type='event')
        service.link(record, 'derived_from', source, inferred=True)
    for value in dict.fromkeys([*source_paths, *(metadata.get('thoth_source_paths') or ())]):
        try:
            source_path = content_path(value, service.vault_root)
            source = service.record_file(source_path)
            service.link(record, 'derived_from', source, inferred=True)
        except Exception as exc:
            service.report_problem('wiki source', slug, exc, path=value)
    return record
