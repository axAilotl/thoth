import asyncio
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.artifact_review_api import create_review_router
from core.artifact_review_policy import review_revision
from core.ingestion_runtime import KnowledgeArtifactRuntime
from tests.test_document_enrichment import source
from tests.test_document_source_diagnostics import captured_pdf


@pytest.fixture
def inbox(tmp_path, monkeypatch):
    collector, path, artifact = source(tmp_path, pdf=True)
    collector.config.set('sources.web_clipper.summarize', False)
    (collector.layout.vault_root / '.obsidian').mkdir()
    monkeypatch.setattr('core.document_enrichment.extract_pdf_text',
                        lambda path, max_pages: 'Abstract\nUseful research.\n1 Introduction\nSystem prompt exfiltration is a security risk.')
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    result = asyncio.run(runtime.process_ingestion_entry(collector.db.get_ingestion_entry(artifact.id)))
    assert result.status == 'needs_review'
    app = FastAPI()
    app.include_router(create_review_router(lambda: runtime))
    return TestClient(app), collector, path, artifact, runtime


def decision(client, **overrides):
    item = client.get('/api/review').json()['items'][0]
    return dict(artifact_id=item['artifact_id'], revision=item['revision'],
                actor='Vault owner', reason='Reviewed quoted research content',
                action='approve_security', security_acknowledged=True, **overrides)


def test_review_page_is_discoverable_and_lists_actual_paused_source(inbox):
    client, collector, path, artifact, _ = inbox
    response = client.get('/review', follow_redirects=False)
    assert response.status_code == 307
    assert response.headers['location'] == '/settings#review'
    html = (Path(__file__).resolve().parents[1] / 'static/settings.html').read_text()
    assert 'data-tab="review"' in html
    item, = client.get('/api/review').json()['items']
    assert item['artifact_id'] == artifact.id
    assert item['source_checksum'] == artifact.source_checksum
    assert item['security_required'] is True
    assert 'system_prompt_attack' in {f['pattern_id'] for f in item['findings']}
    assert item['actions'] == ['approve_security', 'reject']
    assert item['obsidian_url'].startswith('obsidian://open?')
    assert 'payload_json' not in item and 'body' not in item


def test_explicit_approval_processes_pdf_without_model_or_original_changes(inbox):
    client, collector, path, artifact, runtime = inbox
    before = path.read_bytes()
    body = decision(client)
    response = client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 200, response.text
    assert response.json()['item']['status'] == 'pending'
    assert client.get('/api/review').json()['items'] == []
    result = asyncio.run(runtime.process_ingestion_entry(collector.db.get_ingestion_entry(artifact.id)))
    assert result.status == 'processed'
    payload = json.loads(collector.db.get_ingestion_entry(artifact.id).payload_json)
    assert payload['custom_metadata']['document_abstract']['text'] == 'Useful research.'
    assert not payload['custom_metadata'].get('document_summary')
    assert path.read_bytes() == before
    history, = client.get('/api/review?status=decided').json()['items']
    assert history['status'] == 'processed'
    assert history['history'][-1]['action'] == 'security_override_approved'
    assert client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'}).status_code == 409


@pytest.mark.parametrize('headers', [{}, {'X-Thoth-Review': '1', 'Origin': 'https://evil.test'},
                                    {'X-Thoth-Review': '1', 'Sec-Fetch-Site': 'cross-site'}])
def test_web_mutations_reject_cross_origin_or_missing_header(inbox, headers):
    client, collector, path, artifact, _ = inbox
    assert client.post('/api/review/decision', json=decision(client), headers=headers).status_code == 403
    assert collector.db.get_ingestion_entry(artifact.id).status == 'needs_review'


@pytest.mark.parametrize('change', ['missing_ack', 'retry', 'stale', 'changed_file', 'blank_reason'])
def test_approval_fails_closed(inbox, change):
    client, collector, path, artifact, _ = inbox
    body = decision(client)
    if change == 'missing_ack': body['security_acknowledged'] = False
    if change == 'retry': body['action'] = 'retry'
    if change == 'stale': body['revision'] = '0' * 64
    if change == 'changed_file': path.write_bytes(b'%PDF-new revision')
    if change == 'blank_reason': body['reason'] = '  '
    response = client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})
    assert response.status_code == (422 if change == 'blank_reason' else 409), response.text
    assert collector.db.get_ingestion_entry(artifact.id).status == 'needs_review'


def test_rejection_is_audited_and_does_not_delete_original(inbox):
    client, collector, path, artifact, _ = inbox
    before = path.read_bytes()
    body = decision(client)
    body['action'] = 'reject'
    body['security_acknowledged'] = False
    response = client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 200, response.text
    item, = client.get('/api/review?status=rejected').json()['items']
    assert item['history'][-1]['actor'] == 'Vault owner'
    assert item['actions'] == []
    assert path.read_bytes() == before


def test_pagination_and_invalid_filters(inbox):
    client, collector, path, artifact, _ = inbox
    assert client.get('/api/review?limit=1').json()['has_more'] is False
    assert client.get('/api/review?offset=1').json()['items'] == []
    assert client.get('/api/review?status=bogus').status_code == 422
    assert client.get('/api/review?limit=501').status_code == 422


def test_container_obsidian_link_uses_configured_desktop_vault(inbox):
    client, collector, *_ = inbox
    collector.config.set('review_ui.obsidian_vault_name', '_vault_v')
    collector.config.set('review_ui.obsidian_content_prefix', 'knowledge_vault')
    item, = client.get('/api/review').json()['items']
    assert item['obsidian_url'] == 'obsidian://open?vault=_vault_v&file=knowledge_vault%2Fclipper-assets%2Fpaper.pdf'


@pytest.mark.parametrize('action', ['reject', 'approve_security'])
def test_competing_decision_is_not_overwritten(inbox, monkeypatch, action):
    client, collector, path, artifact, _ = inbox
    db = collector.db
    body = decision(client)
    body['action'] = action
    original = db.get_ingestion_entry
    raced = False
    def race(artifact_id, **kwargs):
        nonlocal raced
        entry = original(artifact_id)
        # Trigger after the action has read the entry, but before its CAS write.
        if not raced and entry and entry.status == 'needs_review':
            raced = True
            with db._get_connection() as conn:
                conn.execute("UPDATE ingestion_queue SET last_error='competing update' WHERE artifact_id=?", (artifact_id,))
        return entry
    monkeypatch.setattr(db, 'get_ingestion_entry', race)
    response = client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 409
    assert original(artifact.id).last_error == 'competing update'
    assert original(artifact.id).status == 'needs_review'


@pytest.mark.parametrize('action', ['reject', 'approve_security'])
def test_sql_compare_and_swap_rejects_update_after_snapshot(inbox, monkeypatch, action):
    client, collector, path, artifact, _ = inbox
    db = collector.db
    entry = db.get_ingestion_entry(artifact.id)
    original = db.get_ingestion_entry
    def snapshot_then_race(artifact_id, **kwargs):
        snapshot = original(artifact_id, **kwargs)
        with db._get_connection() as conn:
            conn.execute("UPDATE ingestion_queue SET last_error='race after snapshot' WHERE artifact_id=?", (artifact_id,))
        return snapshot
    monkeypatch.setattr(db, 'get_ingestion_entry', snapshot_then_race)
    kwargs = dict(actor='owner', reason='reviewed', expected_revision=review_revision(entry))
    with pytest.raises(ValueError, match='changed'):
        if action == 'approve_security':
            db.approve_ingestion_security_override(artifact.id, **kwargs)
        else:
            db.transition_ingestion_review(artifact.id, action='reject', status='rejected', **kwargs)
    assert original(artifact.id).status == 'needs_review'
    assert original(artifact.id).last_error == 'race after snapshot'


@pytest.mark.parametrize('payload', ['{broken', '[]', 'null'])
def test_malformed_payload_can_be_rejected_but_not_retried(inbox, payload):
    client, collector, path, artifact, _ = inbox
    with collector.db._get_connection() as conn:
        conn.execute('UPDATE ingestion_queue SET payload_json=? WHERE artifact_id=?', (payload, artifact.id))
    body = decision(client)
    body['action'] = 'reject'
    response = client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 200, response.text
    assert collector.db.get_ingestion_entry(artifact.id).status == 'rejected'


def test_exhausted_malformed_payload_does_not_break_review_listing(inbox):
    client, collector, _, artifact, _ = inbox
    entry = collector.db.get_ingestion_entry(artifact.id)
    review = json.loads(entry.review_json)
    review['state'].update(category='processing_failed', reason='Processing failed')
    broken = replace(
        entry, status='failed', attempts=5, last_error='Processing failed',
        payload_json='{broken', review_json=json.dumps(review),
    )
    with collector.db._get_connection() as conn:
        conn.execute(
            'UPDATE ingestion_queue SET status=?, attempts=?, last_error=?, payload_json=?, review_json=? '
            'WHERE artifact_id=?',
            (broken.status, broken.attempts, broken.last_error, broken.payload_json,
             broken.review_json, artifact.id),
        )

    response = client.get('/api/review')

    assert response.status_code == 200
    item, = response.json()['items']
    assert item['category'] == 'malformed_payload'
    assert item['source_status'] == 'unverified'
    assert item['actions'] == ['reject']
    assert 'reconstruct' in item['reason']
    assert 'queued review metadata is malformed' in item['action_note']


def test_listing_database_failure_is_not_reported_as_empty(inbox, monkeypatch):
    client, collector, *_ = inbox
    from contextlib import contextmanager
    @contextmanager
    def broken():
        raise RuntimeError('database unavailable')
        yield
    monkeypatch.setattr(collector.db, '_get_connection', broken)
    with pytest.raises(RuntimeError, match='database unavailable'):
        client.get('/api/review')


def test_mutation_database_failure_is_not_reported_as_missing(inbox, monkeypatch):
    client, collector, *_ = inbox
    body = decision(client)
    from contextlib import contextmanager
    @contextmanager
    def broken():
        raise RuntimeError('database unavailable')
        yield
    monkeypatch.setattr(collector.db, '_get_connection', broken)
    with pytest.raises(RuntimeError, match='database unavailable'):
        client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})


@pytest.mark.parametrize('action', ['approve_security', 'reject'])
def test_console_defaults_preserve_audit_without_claiming_identity_or_inspection(inbox, action):
    client, collector, path, artifact, _ = inbox
    before = path.read_bytes()
    body = decision(client)
    body.pop('actor')
    body.pop('reason')
    body['action'] = action
    response = client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 200, response.text
    stored = json.loads(collector.db.get_ingestion_entry(artifact.id).review_json)['events'][-1]
    assert stored['actor'] == 'review-console'
    assert stored['reason'] == 'Decision submitted through the review console.'
    assert stored['action'] == ('security_override_approved' if action == 'approve_security' else 'reject')
    assert response.json()['item']['history'][-1]['actor'] == stored['actor']
    assert path.read_bytes() == before


@pytest.mark.parametrize('field', ['actor', 'reason'])
@pytest.mark.parametrize('value', ['', '  ', None])
def test_explicit_empty_provenance_is_not_replaced_by_console_defaults(inbox, field, value):
    client, collector, _, artifact, _ = inbox
    body = decision(client)
    body[field] = value
    response = client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 422
    assert collector.db.get_ingestion_entry(artifact.id).status == 'needs_review'


@pytest.mark.parametrize('value', ['true', 'false', 'yes', 1, 0, None, [], {}])
def test_acknowledgement_requires_a_json_boolean(inbox, value):
    client, collector, _, artifact, _ = inbox
    body = decision(client)
    body['security_acknowledged'] = value
    response = client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 422
    assert collector.db.get_ingestion_entry(artifact.id).status == 'needs_review'


def test_omitting_acknowledgement_does_not_approve(inbox):
    client, collector, _, artifact, _ = inbox
    body = decision(client)
    for key in ('actor', 'reason', 'security_acknowledged'):
        body.pop(key)
    response = client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 409
    assert collector.db.get_ingestion_entry(artifact.id).status == 'needs_review'


@pytest.mark.parametrize('payload', ['{broken', '[]', 'null'])
def test_malformed_source_cannot_be_retried_with_console_defaults(inbox, payload):
    client, collector, _, artifact, _ = inbox
    with collector.db._get_connection() as conn:
        conn.execute('UPDATE ingestion_queue SET payload_json=? WHERE artifact_id=?', (payload, artifact.id))
    item, = client.get('/api/review').json()['items']
    response = client.post('/api/review/decision', json={
        'artifact_id': item['artifact_id'], 'revision': item['revision'], 'action': 'retry',
    }, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 409
    assert collector.db.get_ingestion_entry(artifact.id).status == 'needs_review'


@pytest.fixture
def ocr_inbox(tmp_path, monkeypatch):
    collector, path, artifact = source(tmp_path, pdf=True)
    collector.config.set('sources.web_clipper.summarize', False)
    monkeypatch.setattr('core.document_enrichment.extract_pdf_text', lambda path, max_pages: '')
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    result = asyncio.run(runtime.process_ingestion_entry(collector.db.get_ingestion_entry(artifact.id)))
    assert result.status == 'needs_review'
    app = FastAPI()
    app.include_router(create_review_router(lambda: runtime))
    return TestClient(app), collector, path, artifact, runtime


def test_ocr_reason_explains_retry_and_retry_does_not_perform_ocr(ocr_inbox, monkeypatch):
    client, collector, path, artifact, runtime = ocr_inbox
    before = path.read_bytes()
    item, = client.get('/api/review').json()['items']
    assert item['ocr_required'] is True
    assert item['category'] == 'ocr_required'
    assert item['reason_summary'] == 'No PDF text; scanned pages need OCR.'
    assert 'does not run OCR' in item['action_note']
    assert 'rescan the changed PDF' in item['action_note']
    assert item['actions'] == ['retry', 'reject']
    assert item['security_required'] is False
    extracted = []

    def extract(path, max_pages):
        extracted.append((path, max_pages))
        return ''

    monkeypatch.setattr('core.document_enrichment.extract_pdf_text', extract)
    response = client.post('/api/review/decision', json={
        'artifact_id': item['artifact_id'], 'revision': item['revision'], 'action': 'retry',
    }, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 200, response.text
    event = response.json()['item']['history'][-1]
    assert event['actor'] == 'review-console' and event['action'] == 'retry'
    assert event['reason'] == 'Decision submitted through the review console.'
    result = asyncio.run(runtime.process_ingestion_entry(collector.db.get_ingestion_entry(artifact.id)))
    assert result.status == 'needs_review'
    assert len(extracted) == 1
    assert path.read_bytes() == before
    assert client.get('/api/review').json()['items'][0]['ocr_required'] is True


def test_changed_ocr_source_requires_rescan_and_old_decision_stays_stale(ocr_inbox):
    client, collector, path, artifact, _ = ocr_inbox
    item, = client.get('/api/review').json()['items']
    body = {'artifact_id': item['artifact_id'], 'revision': item['revision'], 'action': 'retry'}
    path.write_bytes(b'%PDF-external-text-layer-added')
    response = client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 409 and 'rescan required' in response.text
    collector.collect()
    recaptured = collector.db.get_ingestion_entry(artifact.id)
    assert json.loads(recaptured.payload_json)['source_checksum'] != item['source_checksum']
    assert review_revision(recaptured) != item['revision']
    assert client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'}).status_code == 409


@pytest.mark.parametrize('url,allowed', [
    ('https://example.org/paper?q=research#abstract', True),
    ('http://example.org/paper', True),
    ('javascript:alert(1)', False), ('data:text/html,<script>alert(1)</script>', False),
    ('//example.org/paper', False), ('https://user:secret@example.org', False), ('https://@example.org', False),
    ('https://example.org\\@evil.test', False), ('https://example.org\n', False),
    ('https://[invalid', False), ('https:///paper', False), (None, False), ({}, False),
])
def test_source_links_only_allow_explicit_web_urls(inbox, url, allowed):
    client, collector, _, artifact, _ = inbox
    payload = json.loads(collector.db.get_ingestion_entry(artifact.id).payload_json)
    payload['source_url'] = url
    with collector.db._get_connection() as conn:
        conn.execute('UPDATE ingestion_queue SET payload_json=? WHERE artifact_id=?', (json.dumps(payload), artifact.id))
    item, = client.get('/api/review').json()['items']
    assert item['source_url'] == (url if allowed else None)
    assert item['reason_summary'].startswith('Security flag: ')
    assert 'system prompt attack' in item['reason_summary']
    assert item['ocr_required'] is False


def test_console_defaults_reach_the_existing_audit_mirror(inbox, monkeypatch):
    client, _, _, _, _ = inbox
    mirrored = []

    def mirror(self, entry, *, action):
        mirrored.append((action, json.loads(entry.review_json)['events'][-1]))

    monkeypatch.setattr('core.artifact_review_queue.ArtifactReviewQueueService._mirror_review_decision', mirror)
    body = decision(client)
    body.pop('actor')
    body.pop('reason')
    response = client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 200, response.text
    action, event = mirrored[0]
    assert action == event['action'] == 'security_override_approved'
    assert event['actor'] == 'review-console'
    assert event['reason'] == 'Decision submitted through the review console.'


def test_error_after_recording_is_not_safe_to_retry(inbox, monkeypatch):
    client, collector, _, artifact, _ = inbox

    def mirror_failure(self, entry, *, action):
        raise RuntimeError('Mirror configuration unavailable')

    monkeypatch.setattr('core.artifact_review_queue.ArtifactReviewQueueService._mirror_review_decision', mirror_failure)
    body = decision(client)
    response = client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'})
    assert response.status_code == 409
    # The UI treats all decision errors as unconfirmed, stops the batch and refreshes.
    entry = collector.db.get_ingestion_entry(artifact.id)
    assert entry.status == 'pending'
    assert json.loads(entry.review_json)['events'][-1]['action'] == 'security_override_approved'
    assert client.post('/api/review/decision', json=body, headers={'X-Thoth-Review': '1'}).status_code == 409


@pytest.mark.parametrize('content,source_status,reason', [
    (None, 'missing', 'missing'), (b'', 'empty', 'empty'),
    (b'<!DOCTYPE html><html>Download denied</html>', 'html', 'HTML'),
    (b'Download failed', 'non_pdf', 'PDF header'),
    (b'%PDF-1.7\nTruncated PDF without a trailer\n', 'malformed_pdf', 'xref'),
])
def test_source_failure_reaches_review_from_real_ingestion(
    tmp_path, monkeypatch, content, source_status, reason,
):
    collector, path, artifact = captured_pdf(tmp_path, content or b'%PDF-1.7\n')
    if content is None:
        path.unlink()
    elif not content:
        path.write_bytes(content)
    model = AsyncMock(side_effect=AssertionError('Invalid source reached model'))
    monkeypatch.setattr('core.llm_interface.LLMInterface.generate', model)
    if source_status != 'malformed_pdf':
        poppler = Mock(side_effect=AssertionError('Invalid bytes reached Poppler'))
        monkeypatch.setattr('core.pdf_text.subprocess.run', poppler)
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    app = FastAPI()
    app.include_router(create_review_router(lambda: runtime))
    client = TestClient(app)

    result = asyncio.run(runtime.process_ingestion_entry(collector.db.get_ingestion_entry(artifact.id)))

    assert result.status == 'needs_review'
    item, = client.get('/api/review').json()['items']
    assert item['category'] == f'source_{source_status}'
    assert item['source_status'] == source_status
    assert item['actions'] == ['reject']
    assert 'stay unavailable' in item['action_note']
    assert reason in item['reason']
    assert 'rescan' in item['reason_summary']
    assert item['ocr_required'] is False
    assert item['attempts'] == 1
    assert reason in item['last_error']
    assert reason in item['history'][-1]['reason']
    assert len(item['reason_summary']) <= 160
    model.assert_not_awaited()
    if source_status != 'malformed_pdf':
        poppler.assert_not_called()
    if content is not None:
        assert path.read_bytes() == content


@pytest.mark.parametrize('content,source_status,reason', [
    (None, 'missing', 'missing'), (b'', 'empty', 'empty'),
    (b'<!DOCTYPE html><html>Access denied</html>', 'html', 'HTML'),
    (b'upstream server error', 'non_pdf', 'PDF header'),
    (b'%PDF-1.7\nTruncated download\n', 'malformed_pdf', 'xref'),
])
def test_exhausted_source_diagnostics_are_read_only(
    tmp_path, monkeypatch, content, source_status, reason,
):
    collector, path, artifact = captured_pdf(tmp_path, content or b'%PDF-1.7\n')
    if content is None:
        path.unlink()
    elif not content:
        path.write_bytes(content)
    # Reproduce a stored exhausted row from the pre-diagnostic runtime.
    claimed = collector.db.claim_ingestion_entry(artifact.id)
    recorded_error = ("Syntax Error: Couldn't read xref table" if source_status == 'malformed_pdf'
                      else 'Original extractor failure')
    exhausted = collector.db.mark_ingestion_failed(
        artifact.id, recorded_error, max_attempts=claimed.attempts,
    )
    before = exhausted.__dict__.copy()
    model = AsyncMock(side_effect=AssertionError('Review read reached model'))
    monkeypatch.setattr('core.llm_interface.LLMInterface.generate', model)
    poppler = Mock(side_effect=AssertionError('Review listing reached Poppler'))
    monkeypatch.setattr('core.pdf_text.subprocess.run', poppler)
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    app = FastAPI()
    app.include_router(create_review_router(lambda: runtime))

    response = TestClient(app).get('/api/review')

    assert response.status_code == 200
    item, = response.json()['items']
    assert item['status'] == 'failed'
    assert item['source_status'] == source_status
    assert item['category'] == f'source_{source_status}'
    assert item['actions'] == ['reject']
    assert 'stay unavailable' in item['action_note']
    assert reason in item['reason']
    assert 'rescan' in item['reason_summary']
    assert item['last_error'] == recorded_error
    assert item['revision'] == review_revision(exhausted)
    assert collector.db.get_ingestion_entry(artifact.id).__dict__ == before
    model.assert_not_awaited()
    poppler.assert_not_called()
    if content is not None:
        assert path.read_bytes() == content


def test_exhausted_runtime_preserves_underlying_infrastructure_error(tmp_path, monkeypatch):
    collector, path, artifact = captured_pdf(tmp_path, b'%PDF-1.7\n')
    # Only the process boundary fails; materialization, enrichment and retries run.
    poppler = Mock(side_effect=FileNotFoundError('pdftotext'))
    monkeypatch.setattr('core.pdf_text.subprocess.run', poppler)
    now = datetime.now(timezone.utc)
    monkeypatch.setattr('core.metadata_db.utc_now', lambda: now)
    monkeypatch.setattr('core.metadata_db.utc_now_iso', lambda: now.isoformat().replace('+00:00', 'Z'))
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    for _ in range(5):
        now += timedelta(hours=2)
        entry = collector.db.get_ingestion_entry(artifact.id)
        with pytest.raises(RuntimeError, match='Missing required PDF utility: pdftotext'):
            asyncio.run(runtime.process_ingestion_entry(entry))
    exhausted = collector.db.get_ingestion_entry(artifact.id)
    assert exhausted.status == 'failed'
    app = FastAPI()
    app.include_router(create_review_router(lambda: runtime))

    item, = TestClient(app).get('/api/review').json()['items']

    assert item['category'] == 'processing_failed'
    assert item['source_status'] == 'unverified'
    assert 'Missing required PDF utility: pdftotext' in item['reason']
    assert 'Missing required PDF utility: pdftotext' in item['reason_summary']
    assert 'after 5 attempt' not in item['reason_summary']
    assert item['ocr_required'] is False
    assert collector.db.get_ingestion_entry(artifact.id) == exhausted


@pytest.mark.parametrize('exhausted', [False, True])
def test_malformed_pdf_retry_requires_repaired_source_and_rescan(tmp_path, monkeypatch, exhausted):
    collector, path, artifact = captured_pdf(tmp_path, b'%PDF-1.7\nBroken trailer\n')
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    if exhausted:
        collector.db.claim_ingestion_entry(artifact.id)
        collector.db.mark_ingestion_failed(artifact.id, "Syntax Error: Couldn't read xref table", max_attempts=1)
    else:
        assert asyncio.run(runtime.process_ingestion_entry(
            collector.db.get_ingestion_entry(artifact.id),
        )).status == 'needs_review'
    app = FastAPI()
    app.include_router(create_review_router(lambda: runtime))
    client = TestClient(app)
    poppler = Mock(side_effect=AssertionError('Known malformed PDF reached Poppler again'))
    monkeypatch.setattr('core.pdf_text.subprocess.run', poppler)
    item, = client.get('/api/review').json()['items']

    response = client.post('/api/review/decision', json={
        'artifact_id': item['artifact_id'], 'revision': item['revision'], 'action': 'retry',
    }, headers={'X-Thoth-Review': '1'})

    assert response.status_code == 409
    assert 'rescan' in response.text
    assert review_revision(collector.db.get_ingestion_entry(artifact.id)) == item['revision']
    assert path.read_bytes() == b'%PDF-1.7\nBroken trailer\n'
    poppler.assert_not_called()


def test_retry_fails_closed_when_source_recheck_cannot_verify(tmp_path, monkeypatch):
    collector, _, artifact = captured_pdf(tmp_path, b'%PDF-1.7\nValid enough for storage\n')
    claimed = collector.db.claim_ingestion_entry(artifact.id)
    exhausted = collector.db.mark_ingestion_failed(
        artifact.id, 'Original processing error', max_attempts=claimed.attempts,
    )
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    app = FastAPI()
    app.include_router(create_review_router(lambda: runtime))
    client = TestClient(app)
    item, = client.get('/api/review').json()['items']

    monkeypatch.setattr(
        'core.artifact_review_api.diagnose_document_source',
        lambda *args, **kwargs: {
            'source_status': 'unverified',
            'diagnostic_error': 'source changed while checking',
        },
    )
    response = client.post('/api/review/decision', json={
        'artifact_id': item['artifact_id'], 'revision': item['revision'], 'action': 'retry',
    }, headers={'X-Thoth-Review': '1'})

    assert exhausted.status == 'failed'
    assert response.status_code == 409
    assert 'could not be verified' in response.text
    assert collector.db.get_ingestion_entry(artifact.id).status == 'failed'


def test_restored_source_regains_retry_action(tmp_path):
    collector, path, artifact = captured_pdf(tmp_path, b'%PDF-1.7\nRestored source\n')
    entry = collector.db.get_ingestion_entry(artifact.id)
    path.unlink()
    reviewed = collector.db.mark_ingestion_review_required(
        artifact.id,
        category='source_missing',
        reason='Document source file is missing; restore it and rescan.',
        metadata={'source_status': 'missing', 'reason_summary': 'Source file missing; restore it and rescan.'},
    )
    path.write_bytes(b'%PDF-1.7\nRestored source\n')
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    app = FastAPI()
    app.include_router(create_review_router(lambda: runtime))
    client = TestClient(app)

    item, = client.get('/api/review').json()['items']

    assert reviewed.status == 'needs_review'
    assert item['category'] == 'processing_failed'
    assert item['source_status'] == 'unverified'
    assert item['actions'] == ['retry', 'reject']
    assert 'available' in item['reason']

    response = client.post('/api/review/decision', json={
        'artifact_id': item['artifact_id'], 'revision': item['revision'], 'action': 'retry',
    }, headers={'X-Thoth-Review': '1'})

    assert response.status_code == 200, response.text
    assert collector.db.get_ingestion_entry(artifact.id).status == 'pending'


@pytest.mark.parametrize('error,source_status', [
    ("Syntax Error: Couldn't read xref table", 'malformed_pdf'),
    ('Unknown extraction failure', 'unverified'),
    ('Missing required PDF utility: pdftotext', 'unverified'),
    ("Syntax Error: Couldn't open file: Permission denied", 'unverified'),
])
@pytest.mark.parametrize('state_error_only', [False, True])
def test_review_uses_recorded_pdf_errors_without_extraction(
    tmp_path, monkeypatch, error, source_status, state_error_only,
):
    collector, path, artifact = captured_pdf(tmp_path, b'%PDF-1.7\n')
    collector.db.claim_ingestion_entry(artifact.id)
    entry = collector.db.mark_ingestion_failed(artifact.id, error, max_attempts=1)
    if state_error_only:
        assert collector.db.upsert_ingestion_entry(replace(entry, last_error=None))
    before = collector.db.get_ingestion_entry(artifact.id)
    poppler = Mock(side_effect=AssertionError('Review listing reached Poppler'))
    monkeypatch.setattr('core.pdf_text.subprocess.run', poppler)
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    app = FastAPI()
    app.include_router(create_review_router(lambda: runtime))

    item, = TestClient(app).get('/api/review').json()['items']

    assert item['source_status'] == source_status
    assert item['category'] == ('source_malformed_pdf' if source_status == 'malformed_pdf' else 'processing_failed')
    assert error in item['reason']
    assert item['revision'] == review_revision(before)
    assert collector.db.get_ingestion_entry(artifact.id) == before
    assert path.read_bytes() == b'%PDF-1.7\n'
    poppler.assert_not_called()


def test_persisted_source_summary_takes_precedence_over_security_summary(inbox):
    client, collector, path, artifact, _ = inbox
    path.write_bytes(b'<!DOCTYPE html><html>Denied</html>')
    collector.db.mark_ingestion_review_required(
        artifact.id, category='source_html', reason='Document source contains HTML instead of PDF bytes.',
        metadata={'source_status': 'html', 'reason_summary': 'HTML saved as PDF; download the PDF and rescan.'},
    )
    item, = client.get('/api/review').json()['items']
    assert item['security_required'] is True
    assert item['reason_summary'] == 'HTML saved as PDF; download the PDF and rescan.'


@pytest.mark.parametrize('mode', ['outside', 'symlink', 'changed', 'config'])
def test_exhausted_diagnostics_respect_source_authorization(tmp_path, monkeypatch, mode):
    collector, path, artifact = captured_pdf(tmp_path, b'%PDF-1.7\n')
    if mode == 'outside':
        collector.config.set('sources.web_clipper.attachment_dirs', ['different-assets'])
        (collector.layout.vault_root / 'different-assets').mkdir()
    elif mode == 'symlink':
        original = path.with_name('original.pdf')
        path.rename(original)
        path.symlink_to(original)
    elif mode == 'changed':
        path.write_bytes(b'%PDF-1.7\nChanged since capture\n')
    else:
        collector.config.set('sources.web_clipper.max_source_bytes', 'not an integer')
    collector.db.claim_ingestion_entry(artifact.id)
    exhausted = collector.db.mark_ingestion_failed(artifact.id, 'Original error', max_attempts=1)
    poppler = Mock(side_effect=AssertionError('Unvalidated source reached Poppler'))
    monkeypatch.setattr('core.pdf_text.subprocess.run', poppler)
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    app = FastAPI()
    app.include_router(create_review_router(lambda: runtime))

    item, = TestClient(app).get('/api/review').json()['items']

    assert item['source_status'] == 'unverified'
    assert item['category'] == 'processing_failed'
    assert 'Original error' in item['reason']
    assert 'Source check:' in item['reason']
    assert collector.db.get_ingestion_entry(artifact.id) == exhausted
    poppler.assert_not_called()


def test_textless_pdf_retains_ocr_review_and_explicit_retry(tmp_path, monkeypatch):
    content = (Path(__file__).parent / 'fixtures/pdfs/sample_paper.pdf').read_bytes()
    collector, path, artifact = captured_pdf(tmp_path, content)
    # Simulate a successful Poppler extraction of a scanned page at the OS boundary.
    poppler = Mock(return_value=CompletedProcess([], 0, '\f', ''))
    model = AsyncMock(side_effect=AssertionError('Textless PDF reached model'))
    monkeypatch.setattr('core.pdf_text.subprocess.run', poppler)
    monkeypatch.setattr('core.llm_interface.LLMInterface.generate', model)
    runtime = KnowledgeArtifactRuntime(collector.config, layout=collector.layout, db=collector.db)
    app = FastAPI()
    app.include_router(create_review_router(lambda: runtime))
    client = TestClient(app)

    assert asyncio.run(runtime.process_ingestion_entry(
        collector.db.get_ingestion_entry(artifact.id),
    )).status == 'needs_review'
    item, = client.get('/api/review').json()['items']
    assert item['category'] == 'ocr_required'
    assert item['source_status'] == 'available'
    assert item['ocr_required'] is True
    assert item['reason_summary'] == 'No PDF text; scanned pages need OCR.'
    assert 'does not run OCR' in item['action_note']
    assert client.post('/api/review/decision', json={
        'artifact_id': item['artifact_id'], 'revision': item['revision'], 'action': 'retry',
    }, headers={'X-Thoth-Review': '1'}).status_code == 200
    assert asyncio.run(runtime.process_ingestion_entry(
        collector.db.get_ingestion_entry(artifact.id),
    )).status == 'needs_review'
    assert poppler.call_count == 2
    model.assert_not_awaited()
    assert path.read_bytes() == content
