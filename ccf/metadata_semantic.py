"""Semantic inference and actual operator review metadata."""

from __future__ import annotations

from .metadata_values import mapping, portable
from .objects import hash_data


def record_entity(service, entity, *, evidence_ids=()):
    native_id = entity['canonical_id']
    kind = {'person': 'person', 'organization': 'org', 'org': 'org', 'place': 'place',
            'project': 'topic', 'paper': 'paper'}.get(entity.get('entity_type'), 'thoth.entity')
    record = service.store.upsert(f'entity:{native_id}', 'record', {
        'type': kind, 'title': str(entity.get('display_name') or native_id),
        'data': portable(entity), 'by': 'app:thoth', 'status': 'inferred',
    })
    for evidence_id in evidence_ids:
        service.link(record, 'derived_from', {'id': evidence_id}, inferred=True)
    return record


def record_candidate(service, candidate, *, evidence=(), review=None):
    native_id = candidate['candidate_id']
    if review is None:
        review = mapping(candidate.get('metadata')).get('semantic_memory_review')
    review = mapping(review)
    # Promotion can be automatic. Only an actual confirmation audit changes
    # an inference into a human-accepted assertion.
    status = 'inferred'
    if candidate.get('status') == 'rejected' or review.get('action') == 'reject':
        status = 'rejected'
    elif review.get('action') == 'confirm' and review.get('actor'):
        status = 'accepted'
    previous = service.store.find(f'candidate:{native_id}')
    if status == 'inferred' and previous and previous.get('status') == 'accepted':
        status = 'accepted'
    record = service.store.upsert(f'candidate:{native_id}', 'record', {
        'type': 'fact', 'title': str(candidate.get('text') or native_id),
        'data': portable(candidate), 'by': 'app:thoth', 'status': status,
    })
    for item in evidence:
        if item.get('artifact_id'):
            source = service.reference(f"artifact:{item['artifact_id']}",
                                       title=item['artifact_id'])
        elif item.get('capture_event_id'):
            source = service.reference(f"event:{item['capture_event_id']}",
                                       title=item['capture_event_id'], record_type='event')
        else:
            continue
        service.link(record, 'derived_from', source, inferred=status == 'inferred', data=item)
    if candidate.get('entity_id'):
        subject = service.reference(f"entity:{candidate['entity_id']}",
                                    title=candidate.get('entity_name') or candidate['entity_id'])
        service.link(record, 'about', subject, inferred=status == 'inferred')
    if review:
        record_review(service, record, review)
    return record


def record_review(service, target, review):
    review = portable(review)
    native_key = f"review:{target['id']}:{hash_data(review)}"
    record = service.store.upsert(native_key, 'record', {
        'type': 'thoth.review', 'title': str(review['action']), 'data': review,
        'by': 'user' if review.get('actor') else 'app:thoth',
    })
    service.link(record, 'about', target)
    return record
