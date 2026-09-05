import asyncio
import json
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from core.artifact_review_api import create_review_router
from core.artifact_review_policy import review_revision
from core.ingestion_runtime import KnowledgeArtifactRuntime
from tests.test_document_enrichment import source


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
