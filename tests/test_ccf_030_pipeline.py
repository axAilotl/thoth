"""CCF metadata through native capture/review/wiki entrypoints, without Postgres."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ccf.metadata import CcfMetadataService
from ccf.objects import validate_object
from ccf.store import ObjectStore
from core.ccf_metadata import metadata_enabled, open_metadata_service
from core.config import Config
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout


@pytest.fixture
def cfg(tmp_path):
    config = Config()
    config.data = {
        'paths': {'vault_dir': str(tmp_path / 'vault'),
                  'system_dir': str(tmp_path / 'system'), 'cache_dir': 'cache'},
        'database': {'enabled': True, 'path': 'metadata.db',
                     'ccf': {'enabled': True, 'path': 'ccf/metadata.sqlite3', 'device': 'test'}},
    }
    return config


@pytest.fixture
def metadata(cfg):
    service = open_metadata_service(cfg)
    assert service is not None
    return service


def _capture(cfg, *, artifact_id='transcript-1'):
    from core.capture_lifecycle import CaptureLifecycleService
    layout = build_path_layout(cfg)
    layout.ensure_directories()
    db = MetadataDB(str(layout.database_path))
    lifecycle = CaptureLifecycleService(cfg, layout=layout, db=db)
    raw = layout.raw_root / f'{artifact_id}.json'
    raw.write_text('{"text":"Native transcript"}\n')
    result = lifecycle.capture_to_queue(
        artifact_type='transcript',
        payload={'id': artifact_id, 'transcript_id': artifact_id, 'source_type': 'omi',
                 'title': 'Meeting', 'raw_transcript': 'Native transcript',
                 'segments': [{'start': 1.25, 'text': 'Native transcript'}],
                 'api_key': 'not-for-metadata'},
        source={'source_name': 'omi', 'source_type': 'wearable'}, raw_path=raw,
    )
    return result, db, layout


def _candidate(db, artifact_id):
    from core.semantic_memory import SemanticMemoryCandidate, SemanticMemoryEvidence, SemanticMemoryStore
    return SemanticMemoryStore(db).add_candidate(
        SemanticMemoryCandidate(candidate_id='candidate-1', candidate_type='fact',
                                text='Thoth records provenance.', confidence=0.9),
        evidence=(SemanticMemoryEvidence(candidate_id='candidate-1', evidence_id='evidence-1',
                                         artifact_id=artifact_id, artifact_type='transcript',
                                         evidence_text='Native transcript'),),
    )


def test_local_config_uses_system_layout(cfg, tmp_path):
    assert metadata_enabled(cfg)
    service = open_metadata_service(cfg)
    assert service.store.path == tmp_path / 'system/ccf/metadata.sqlite3'
    assert service.store.device == 'test'
    cfg.data['database']['ccf']['enabled'] = False
    assert open_metadata_service(cfg) is None


@pytest.mark.parametrize('settings', [None, {'enabled': 'yes'}, {'enabled': True, 'path': ''}])
def test_bad_optional_config_is_reported_without_raising(cfg, settings, caplog):
    cfg.data['database']['ccf'] = settings
    assert open_metadata_service(cfg) is None
    assert 'CCF metadata store could not be opened' in caplog.text


def test_capture_maps_real_file_provenance_and_preserves_native_numbers(cfg):
    result, db, layout = _capture(cfg)
    service = open_metadata_service(cfg)
    objects = list(service.store.objects())
    artifact = service.store.find(f'artifact:{result.queue_artifact_id}')
    assert artifact['type'] == 'transcript'
    assert artifact['status'] == 'inferred'
    assert artifact['data']['segments'][0]['start'] == '1.25'
    assert 'api_key' not in artifact['data']
    native = json.loads(db.get_ingestion_entry(result.queue_artifact_id).payload_json)
    assert native['segments'][0]['start'] == 1.25
    assert native['api_key'] == 'not-for-metadata'
    blobs = [obj for obj in objects if obj['kind'] == 'blob']
    assert len(blobs) == 1
    assert blobs[0]['loc'] == 'vault:raw/transcript-1.json'
    assert any(obj.get('rel') == 'has_file' and obj['to'] == blobs[0]['id'] for obj in objects)
    assert any(obj.get('rel') == 'derived_from' and obj['from'] == artifact['id'] for obj in objects)
    assert all(not validate_object(obj) for obj in objects)
    assert 'ccf_' not in (layout.raw_root / 'transcript-1.json').read_text()


def test_backfill_replay_keeps_native_object_ids(cfg):
    result, db, _ = _capture(cfg)
    service = open_metadata_service(cfg)
    before = {obj['id'] for obj in service.store.objects()}
    payload = json.loads(db.get_ingestion_entry(result.queue_artifact_id).payload_json)
    first = service.store.find(f'artifact:{result.queue_artifact_id}')
    again = service.capture_queue(artifact_id=result.queue_artifact_id,
                                 artifact_type='transcript', payload=payload)
    assert first['id'] == again['id']
    assert before == {obj['id'] for obj in service.store.objects()}


def test_metadata_open_failure_does_not_break_native_capture_or_review(cfg, monkeypatch, caplog):
    def fail(*args, **kwargs):
        raise OSError('unavailable metadata disk')
    monkeypatch.setattr('ccf.store.ObjectStore', fail)
    result, db, _ = _capture(cfg)
    from core.artifact_review_queue import ArtifactReviewQueueService
    reviewed = ArtifactReviewQueueService(db, config=cfg).reject(
        result.queue_artifact_id, actor='operator', reason='bad audio')
    assert reviewed.status == 'rejected'
    assert 'unavailable metadata disk' in caplog.text


def test_missing_raw_metadata_does_not_drop_other_objects(metadata, caplog):
    artifact = metadata.capture_queue(
        artifact_id='missing-raw', artifact_type='transcript',
        payload={'title': 'Missing raw', 'raw_payload': {'path': 'does-not-exist.json'}},
        entities=({'canonical_id': 'person-1', 'entity_type': 'person', 'display_name': 'A'},),
    )
    assert metadata.store.get(artifact['id'])
    assert metadata.store.find('entity:person-1')['status'] == 'inferred'
    assert 'CCF raw metadata failed' in caplog.text


def test_generated_fact_requires_actual_confirmation(metadata):
    candidate = {'candidate_id': 'fact-1', 'candidate_type': 'fact', 'status': 'promoted',
                 'text': 'Machine suggestion', 'confidence': 0.95}
    inferred = metadata.record_candidate(candidate, review={'action': 'promote', 'actor': 'daemon'})
    assert inferred['status'] == 'inferred'
    accepted = metadata.record_candidate(candidate, review={'action': 'confirm', 'actor': 'operator'})
    assert accepted['status'] == 'accepted'
    assert accepted['data']['confidence'] == '0.95'
    assert candidate['confidence'] == 0.95
    assert metadata.record_candidate(candidate)['status'] == 'accepted'


def test_semantic_review_and_wiki_have_traceable_inputs(cfg):
    result, db, layout = _capture(cfg)
    candidate = _candidate(db, result.queue_artifact_id)
    from core.semantic_memory_review import SemanticMemoryReviewService
    reviewed = SemanticMemoryReviewService(db=db, config=cfg).confirm_candidate(
        candidate.candidate_id, actor='operator', reason='checked source')
    assert reviewed['candidate']['status'] == 'confirmed'
    from core.wiki_updater import CompiledWikiUpdater
    pages = CompiledWikiUpdater(cfg, layout=layout, db=db).update_from_semantic_memory()
    assert pages
    service = open_metadata_service(cfg)
    fact = service.store.find('candidate:candidate-1')
    assert fact['status'] == 'accepted'
    objects = list(service.store.objects())
    reviews = [obj for obj in objects if obj.get('type') == 'thoth.review']
    assert reviews and reviews[0]['by'] == 'user'
    for page in pages:
        record = service.store.find(f'wiki:{page.page_path.resolve()}')
        assert record['status'] == 'inferred'
        assert any(obj.get('rel') == 'derived_from' and obj['from'] == record['id']
                   and obj['to'] == fact['id'] for obj in objects)


def test_failed_metadata_does_not_break_semantic_review_or_wiki(cfg, monkeypatch, caplog):
    result, db, layout = _capture(cfg)
    candidate = _candidate(db, result.queue_artifact_id)
    def fail(*args, **kwargs):
        raise RuntimeError('injected metadata failure')
    monkeypatch.setattr(CcfMetadataService, 'record_candidate', fail)
    monkeypatch.setattr(CcfMetadataService, 'record_wiki', fail)
    from core.semantic_memory_review import SemanticMemoryReviewService
    review = SemanticMemoryReviewService(db=db, config=cfg).confirm_candidate(
        candidate.candidate_id, actor='operator', reason='checked')
    assert review['candidate']['status'] == 'confirmed'
    from core.wiki_updater import CompiledWikiUpdater
    pages = CompiledWikiUpdater(cfg, layout=layout, db=db).update_from_semantic_memory()
    assert pages and all(page.page_path.exists() for page in pages)
    assert 'CCF semantic review metadata failed' in caplog.text
    assert 'CCF wiki metadata failed' in caplog.text


def test_file_mapping_is_shared_with_wiki_and_does_not_rewrite(metadata, tmp_path):
    source = tmp_path / 'reference.md'
    source.write_text('Native source\n')
    file_record = metadata.record_file(source)
    page = tmp_path / 'wiki.md'
    page.write_text('Generated page\n')
    wiki = metadata.record_wiki(page_path=page, slug='wiki', metadata={}, source_paths=(str(source),))
    assert len([obj for obj in metadata.store.objects() if obj['kind'] == 'blob']) == 1
    assert any(obj.get('rel') == 'derived_from' and obj['from'] == wiki['id']
               and obj['to'] == file_record['id'] for obj in metadata.store.objects())
    assert file_record['loc'] == source.as_uri()
    assert source.read_text() == 'Native source\n'
    with pytest.raises(FileNotFoundError):
        metadata.record_file(tmp_path / 'absent.md')


def test_connector_lane_annotations_preserve_validated_overrides(metadata):
    from core.connector_registry import ConnectorCcfBlock
    class Manifest:
        ccf = ConnectorCcfBlock(lane='mixed', artifact_role='skill_result',
                               extensions={'thoth.tool': 'extractor', 'thoth.score': 0.8})
    class Registry:
        def get(self, name):
            assert name == 'skill_outputs'
            return Manifest()
    metadata.connector_registry = Registry()
    record = metadata.capture_queue(
        artifact_id='skill-1', artifact_type='transcript',
        source={'collector': 'skill_outputs'},
        payload={'normalized_metadata': {'ccf': {
            'lane': 'transcript', 'extensions': {'thoth.tool': 'transcriber'}}}},
    )
    assert record['ext']['thoth'] == {
        'lane': 'transcript', 'artifact_role': 'skill_result',
        'extensions': {'thoth.tool': 'transcriber', 'thoth.score': '0.8'},
    }
    with pytest.raises(ValueError, match='unknown ccf lane'):
        metadata.capture_queue(artifact_id='bad-skill', artifact_type='transcript',
                               payload={'normalized_metadata': {'ccf': {'lane': 'unknown'}}})


def test_backfill_diagnostics_identify_failed_raw_file(metadata):
    metadata.capture_queue(artifact_id='raw-failure', artifact_type='document',
                           payload={'raw_payload': {'path': 'gone.pdf'}})
    assert metadata.problems == [{
        'operation': 'raw', 'native_id': 'raw-failure', 'path': 'gone.pdf',
        'error': metadata.problems[0]['error'],
    }]
    assert 'FileNotFoundError' in metadata.problems[0]['error']


def test_file_replays_ignore_caller_classification(metadata, tmp_path):
    path = tmp_path / 'article.md'
    path.write_text('Native article\n')
    first = metadata.record_file(path, artifact_type='paper')
    second = metadata.record_file(path, artifact_type='note')
    assert second == first
    assert first['type'] == 'note'


def test_original_file_location_and_capture_session_survive_mapping(metadata, tmp_path):
    source = tmp_path / 'original-note.md'
    source.write_text('Original content\n')
    record = metadata.capture_queue(artifact_id='note-1', artifact_type='markdown', payload={
        'source_path': str(source), 'normalized_metadata': {'capture_session_id': 'session-1'},
    })
    assert record['loc'] == source.as_uri()
    assert record['source']['uri'] == source.as_uri()
    session = metadata.store.find('session:session-1')
    assert any(obj.get('rel') == 'part_of' and obj['from'] == record['id']
               and obj['to'] == session['id'] for obj in metadata.store.objects())
