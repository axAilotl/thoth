"""Native queue captures, raw file pointers, and transcript provenance."""

from __future__ import annotations

import hashlib
import logging
import mimetypes

from .metadata_annotations import annotations
from .metadata_values import artifact_type as record_type, content_path, location, mapping, portable

logger = logging.getLogger(__name__)


def capture_queue(service, *, artifact_id, artifact_type, payload, source=None, entities=()):
    metadata = mapping(payload.get('normalized_metadata'))
    source = {key: value for key, value in (source or {}).items()
              if key in ('source_id', 'source_name', 'source_type', 'collector',
                         'native_source_id', 'base_uri', 'uri')}
    source_id = str(metadata.get('capture_source_id') or source.get('source_id')
                    or payload.get('source') or payload.get('source_type') or 'unknown')
    source.setdefault('source_id', source_id)
    source.setdefault('source_name', payload.get('source') or payload.get('source_type') or source_id)
    previous_source = service.store.find(f'source:{source_id}')
    if previous_source:
        source = {**mapping(previous_source.get('data')), **source}
    source_record = service.store.upsert(f'source:{source_id}', 'record', {
        'type': 'thoth.source', 'title': str(source['source_name']),
        'data': portable(source), 'by': 'app:thoth',
    })
    uri = next((payload[key] for key in ('url', 'source_url', 'original_url', 'pdf_url')
                if isinstance(payload.get(key), str) and payload[key]), None)
    if uri is None and payload.get('source_path'):
        uri = location(content_path(payload['source_path'], service.vault_root), service.vault_root)
    fields = {'type': record_type(artifact_type),
              'title': str(payload.get('title') or artifact_id),
              'data': portable({key: value for key, value in payload.items()
                                if key in ('id', 'artifact_id', 'source', 'source_type',
                                           'title', 'summary', 'description', 'language',
                                           'transcript_id', 'raw_transcript',
                                           'processed_transcript', 'segments',
                                           'arxiv_id', 'authors', 'published_at')}),
              'by': 'app:thoth'}
    ext = annotations(service.connector_registry, source, metadata)
    if ext:
        fields['ext'] = ext
    if artifact_type == 'transcript':
        fields['status'] = 'inferred'
    if uri:
        fields['loc'] = uri
    provenance = {'app': 'thoth', 'device': service.store.device,
                  'native_id': f'artifact:{artifact_id}'}
    if uri:
        provenance['uri'] = uri
    artifact = service.store.upsert(f'artifact:{artifact_id}', 'record', fields, source=provenance)
    service.link(artifact, 'derived_from', source_record)
    event_id = metadata.get('capture_event_id')
    if event_id:
        event = service.reference(f'event:{event_id}', title=str(event_id), record_type='event')
        service.link(artifact, 'derived_from', event)
    session_id = metadata.get('capture_session_id')
    if session_id:
        session = service.reference(f'session:{session_id}', title=str(session_id),
                                    record_type='thoth.capture_session')
        service.link(artifact, 'part_of', session)
    raw = mapping(payload.get('raw_payload'))
    if raw.get('path'):
        try:
            raw_record = record_raw(service, raw)
            service.link(raw_record, 'derived_from', source_record)
            service.link(artifact, 'derived_from', raw_record,
                         inferred=artifact_type == 'transcript')
        except Exception as exc:
            service.report_problem('raw', artifact_id, exc, path=raw['path'])
    for entity in entities:
        try:
            record = service.record_entity(entity, evidence_ids=(artifact['id'],))
            service.link(artifact, 'mentions', record, inferred=True)
        except Exception as exc:
            service.report_problem('entity', entity.get('canonical_id'), exc)
    return artifact


def record_raw(service, raw, *, inferred=False):
    path = content_path(raw['path'], service.vault_root).resolve()
    with path.open('rb') as handle:
        digest = 'sha256:' + hashlib.file_digest(handle, 'sha256').hexdigest()
    size = path.stat().st_size
    expected = raw.get('sha256')
    if expected and digest.removeprefix('sha256:') != str(expected).removeprefix('sha256:'):
        logger.warning('CCF raw hash changed at %s: recorded %s, current %s', path, expected, digest)
    loc = location(path, service.vault_root)
    path_key = hashlib.sha256(str(path).encode('utf-8')).hexdigest()
    fields = {'loc': loc, 'hash': digest, 'media_type': raw.get('media_type')
              or mimetypes.guess_type(path.name)[0] or 'application/octet-stream',
              'size': size, 'title': path.name, 'by': 'app:thoth'}
    blob = service.store.upsert(f'blob:{path_key}', 'blob', fields)
    previous = service.store.find(f'raw:{path_key}')
    record_fields = {
        'type': 'note' if path.suffix.lower() == '.md' else
                ('recording' if fields['media_type'].startswith(('audio/', 'video/')) else 'document'),
        'loc': loc, 'hash': digest, 'title': path.name, 'by': 'app:thoth',
    }
    if previous and previous.get('status'):
        record_fields['status'] = previous['status']
    elif inferred:
        record_fields['status'] = 'inferred'
    record = service.store.upsert(f'raw:{path_key}', 'record', record_fields)
    service.link(record, 'has_file', blob)
    return record
