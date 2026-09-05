"""Revision-aware worker ownership through the metadata queue interface."""
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from core.metadata_db import IngestionQueueEntry, MetadataDB


def entry(checksum='old', status='pending'):
    return IngestionQueueEntry(
        artifact_id='revision-test', artifact_type='paper', source='manual',
        payload_json=json.dumps({'id': 'revision-test', 'title': 'Evidence',
                                 'source_checksum': checksum}),
        status=status,
    )


def test_claim_returns_current_revision_and_prevents_duplicate_worker(tmp_path):
    db = MetadataDB(str(tmp_path / 'meta.db'))
    assert db.upsert_ingestion_entry(entry())
    old_listing = db.get_pending_ingestions()[0]
    assert db.upsert_ingestion_entry(entry('new'))
    claimed = db.claim_ingestion_entry(old_listing.artifact_id)
    assert json.loads(claimed.payload_json)['source_checksum'] == 'new'
    assert claimed.attempts == 1
    assert db.claim_ingestion_entry(old_listing.artifact_id) is None


def test_stale_worker_cannot_overwrite_or_complete_a_new_capture(tmp_path):
    db = MetadataDB(str(tmp_path / 'meta.db'))
    assert db.upsert_ingestion_entry(entry())
    claimed = db.claim_ingestion_entry('revision-test')
    assert db.upsert_ingestion_entry(entry('new'))
    assert db.update_ingestion_payload_json(
        claimed.artifact_id, claimed.payload_json, expected_source_checksum='old'
    ) is None
    assert not db.mark_ingestion_processed(
        claimed.artifact_id, expected_source_checksum='old'
    )
    current = db.get_ingestion_entry('revision-test')
    assert current.status == 'pending'
    assert json.loads(current.payload_json)['source_checksum'] == 'new'


def test_owned_revision_can_publish_payload_and_complete(tmp_path):
    db = MetadataDB(str(tmp_path / 'meta.db'))
    assert db.upsert_ingestion_entry(entry())
    claimed = db.claim_ingestion_entry('revision-test')
    updated = json.loads(claimed.payload_json)
    updated['custom_metadata'] = {'document_abstract': {'text': 'Source abstract'}}
    assert db.update_ingestion_payload_json(
        claimed.artifact_id, json.dumps(updated), expected_source_checksum='old'
    ) is not None
    assert db.mark_ingestion_processed(claimed.artifact_id, expected_source_checksum='old')
    assert db.claim_ingestion_entry('revision-test') is None


def test_atomic_claim_respects_retry_deadline(tmp_path):
    db = MetadataDB(str(tmp_path / 'meta.db'))
    assert db.upsert_ingestion_entry(replace(
        entry(), next_attempt_at=(datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
    ))
    assert db.claim_ingestion_entry('revision-test') is None


def test_guarded_payload_update_does_not_use_error_swallowing_reread(tmp_path, monkeypatch):
    db = MetadataDB(str(tmp_path / 'meta.db'))
    assert db.upsert_ingestion_entry(entry())
    claimed = db.claim_ingestion_entry('revision-test')
    monkeypatch.setattr(db, 'get_ingestion_entry', lambda _: None)
    updated = db.update_ingestion_payload_json(
        claimed.artifact_id, claimed.payload_json, expected_source_checksum='old',
    )
    assert updated.artifact_id == claimed.artifact_id
