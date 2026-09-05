"""The public connector execution surface honors bounded local intake."""
from pathlib import Path

import pytest

from core.agent_surface import AgentSurfaceService
from core.config import Config
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout


def service_for(tmp_path: Path) -> AgentSurfaceService:
    config = Config()
    config.data = {}
    for key, value in {
        'vault_dir': str(tmp_path / 'vault'), 'system_dir': '.thoth_system',
        'raw_dir': 'raw', 'library_dir': 'library', 'wiki_dir': 'wiki',
        'cache_dir': 'cache', 'digests_dir': '_digests',
    }.items():
        config.set(f'paths.{key}', value)
    config.set('database.path', 'meta.db')
    config.set('sources.web_clipper.enabled', True)
    config.set('sources.web_clipper.note_dirs', ['Clippings'])
    config.set('sources.web_clipper.attachment_dirs', [])
    layout = build_path_layout(config, project_root=tmp_path)
    root = layout.vault_root / 'Clippings'
    root.mkdir(parents=True)
    for name in ('one', 'two'):
        (root / f'{name}.md').write_text(f'---\ntitle: {name}\n---\nSource material {name}.\n')
    return AgentSurfaceService(config, layout=layout, db=MetadataDB(str(layout.database_path)))


def test_public_connector_limits_new_work_and_advances_on_next_run(tmp_path):
    service = service_for(tmp_path)
    first = service.run_connector('web_clipper', execute=True, options={'limit': 1})
    assert first['result']['queued_count'] == 1
    second = service.run_connector('web_clipper', execute=True, options={'limit': 1})
    assert second['result']['queued_count'] == 1
    assert first['result']['queued'][0]['artifact_id'] != second['result']['queued'][0]['artifact_id']
    third = service.run_connector('web_clipper', execute=True, options={'limit': 1})
    assert third['result']['queued_count'] == 0


@pytest.mark.parametrize('limit', [0, -1, True, 1.5, 'all'])
def test_public_connector_rejects_invalid_batch_limits(tmp_path, limit):
    service = service_for(tmp_path)
    with pytest.raises(ValueError, match='limit'):
        service.run_connector('web_clipper', execute=True, options={'limit': limit})


def test_public_connector_rejects_unknown_options(tmp_path):
    service = service_for(tmp_path)
    with pytest.raises(ValueError, match='options'):
        service.run_connector('web_clipper', execute=True, options={'limti': 1})


def test_public_connector_counts_queued_pdfs_not_staged_existing_files(tmp_path):
    service = service_for(tmp_path)
    assets = service.layout.vault_root / 'pdfs'
    assets.mkdir()
    source = Path(__file__).parent / 'fixtures/web_clipper/capture_attachment.pdf'
    (assets / 'paper.pdf').write_bytes(source.read_bytes())
    service.config.set('sources.web_clipper.attachment_dirs', ['pdfs'])
    service.config.set('sources.web_clipper.queue_pdfs', True)
    service.config.set('connectors.budgets.per_connector.web_clipper.max_input_tokens_per_run', 100)
    result = service.run_connector('web_clipper', execute=True, options={'limit': 3})
    assert result['result']['queued_count'] == 3
    assert result['result']['staged_count'] == 0
    assert any(item['source_id'] == 'pdfs/paper.pdf' for item in result['result']['queued'])
