import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

from collectors.corpus_index_connector import CorpusIndexConnector
from core.prompt_index import PAGE, identify_prompt, collect_prompt_records, publish_prompt_index
from core.wiki_publication import WikiPublicationStore
from tests.test_corpus_index_connector import setup_corpus, document


def refresh_prompt_index(documents, *, config, layout, db):
    collect_prompt_records(documents, layout=layout, db=db)
    return publish_prompt_index(config=config, layout=layout, db=db)


@pytest.mark.parametrize("text", [
    'Prompt: A small green bird wearing a tiny hat, soft lighting and detailed feathers.',
    '**Prompt for a video model:** A bird flies through a forest as the camera tracks it.',
    'System prompt:\nYou are a helpful writing editor. Rewrite the supplied draft in plain language.',
    '<prompt>Create a detailed photograph of a quiet street in warm evening light.</prompt>',
])
def test_model_agnostic_prompt_markers(text):
    assert identify_prompt(text)['kind'] == 'likely_prompt'


@pytest.mark.parametrize("text", [
    'A study of prompt injection and prompt engineering methods.',
    'We use a prompt for steering and a depth controlnet for continuity.',
    '# Tweet\nAn ordinary capture.\n## Summary\nPrompt: invented generated explanation with many words here.',
])
def test_discussion_and_generated_summary_are_not_prompt_examples(text):
    assert identify_prompt(text, social=True) is None


def test_reference_is_not_claimed_as_extracted_prompt():
    assert identify_prompt('Prompt in the alt text of this image.')['kind'] == 'prompt_reference'
    assert identify_prompt('Prompt: https://example.com/prompts')['kind'] == 'prompt_reference'
    assert identify_prompt('Prompt: see the attached image for the full text of the prompt.')['kind'] == 'prompt_reference'


def test_enrichment_is_not_source_evidence_but_later_thread_tweets_are():
    text = '# Tweet\n## Content\nAn ordinary tweet.\n## Repository READMEs\n- **Summary**: Prompt: Draw a bird on a flowering branch in soft light.\n'
    assert identify_prompt(text, social=True) is None
    text += '\n## Tweet 2\nPrompt: Create a scene with a bird and a bright blue sky.\n'
    assert identify_prompt(text, social=True)['kind'] == 'likely_prompt'


def test_retry_repairs_main_index_after_publication_interruption(tmp_path, monkeypatch):
    config, db, layout, doc = setup_documents(tmp_path)
    from core.wiki_updater import CompiledWikiUpdater
    original = CompiledWikiUpdater.refresh_index
    def fail(_self):
        raise OSError('interrupted index update')
    monkeypatch.setattr(CompiledWikiUpdater, 'refresh_index', fail)
    with pytest.raises(OSError):
        refresh_prompt_index([doc], config=config, layout=layout, db=db)
    page = layout.wiki_root / PAGE
    stamp = page.stat().st_mtime_ns
    monkeypatch.setattr(CompiledWikiUpdater, 'refresh_index', original)
    assert refresh_prompt_index([doc], config=config, layout=layout, db=db)['status'] == 'unchanged'
    assert page.stat().st_mtime_ns == stamp
    assert 'Prompt%20Index.md' in (layout.wiki_root / 'index.md').read_text()


def setup_documents(tmp_path):
    config, db, layout = setup_corpus(tmp_path)
    path = layout.vault_root / 'papers/example.md'
    text = 'Prompt: Draw a green bird on a flowering branch in soft morning light.'
    path.write_text(text)
    doc = replace(document(tmp_path, text), path=path, title='A saved example')
    return config, db, layout, doc


def test_one_link_only_page_no_source_changes_and_no_unchanged_rewrite(tmp_path):
    config, db, layout, doc = setup_documents(tmp_path)
    before = doc.path.read_bytes()
    first = refresh_prompt_index([doc], config=config, layout=layout, db=db)
    page = Path(first['page'])
    assert first['sources'] == 1 and first['status'] == 'published'
    content = page.read_text()
    assert '[A saved example](../../vault/papers/example.md)' in content
    assert 'Draw a green bird' not in content
    assert 'pages/Prompt%20Index.md' in (layout.wiki_root / 'index.md').read_text()
    stamp = page.stat().st_mtime_ns
    assert refresh_prompt_index([doc], config=config, layout=layout, db=db)['status'] == 'unchanged'
    assert page.stat().st_mtime_ns == stamp
    assert doc.path.read_bytes() == before
    with db._get_connection() as conn:
        assert conn.execute('SELECT count(*) FROM prompt_index_records').fetchone()[0] == 1


def test_manual_edits_and_unowned_page_preserved(tmp_path):
    config, db, layout, doc = setup_documents(tmp_path)
    page = layout.wiki_root / PAGE
    page.parent.mkdir(parents=True)
    page.write_text('# My own prompt index\nKeep my edits.')
    assert refresh_prompt_index([doc], config=config, layout=layout, db=db)['reason'] == 'unowned'
    assert page.read_text() == '# My own prompt index\nKeep my edits.'


def test_stale_entries_removed_and_feedback_preserved(tmp_path):
    config, db, layout, doc = setup_documents(tmp_path)
    refresh_prompt_index([doc], config=config, layout=layout, db=db)
    page = layout.wiki_root / PAGE
    feedback = '> [!thoth-feedback]\n> Keep my annotations.\n'
    page.write_text(page.read_text() + '\n' + feedback)
    result = refresh_prompt_index([], config=config, layout=layout, db=db)
    assert result['sources'] == 0
    assert 'example.md' not in page.read_text() and page.read_text().count(feedback) == 1
    assert WikiPublicationStore(db, layout.wiki_root).inspect(page).pending_feedback
    page.write_text(page.read_text() + '\nHuman correction.\n')
    assert refresh_prompt_index([doc], config=config, layout=layout, db=db)['status'] == 'blocked'


def test_policy_missing_and_outside_sources(tmp_path):
    config, db, layout, doc = setup_documents(tmp_path)
    docs = [replace(doc, privacy_class='restricted'), replace(doc, source_security_status='needs_review'),
            replace(doc, path=layout.vault_root / 'gone.md')]
    result = refresh_prompt_index(docs, config=config, layout=layout, db=db)
    assert result['sources'] == 0 and result['excluded_by_policy'] == 2
    with pytest.raises(ValueError, match='escapes vault'):
        refresh_prompt_index([replace(doc, path=tmp_path / 'outside.md')], config=config, layout=layout, db=db)


def test_same_tweet_in_multiple_captures_has_one_visible_link(tmp_path):
    config, db, layout, doc = setup_documents(tmp_path)
    docs = []
    for index, name in enumerate(('tweet_1745545813512663203_a.md', 'thread_1745545813512663203_a.md')):
        path = layout.vault_root / 'papers' / name
        path.write_text(doc.content_text)
        docs.append(replace(doc, candidate_key=str(index), path=path, source_type='tweet'))
    result = refresh_prompt_index(docs, config=config, layout=layout, db=db)
    assert result['sources'] == 1 and result['matched_documents'] == 2


def test_real_corpus_connector_backfills_and_updates_without_provider(tmp_path):
    config, db, layout, doc = setup_documents(tmp_path)
    config.set('sources.corpus_index.embeddings_enabled', False)
    config.set('sources.corpus_index.prompt_index_enabled', True)
    connector = CorpusIndexConnector(config, layout=layout, db=db)
    first = asyncio.run(connector.collect())
    assert first['prompt_index']['sources'] == 1
    assert not (layout.wiki_root / PAGE).exists()  # Collectors cannot publish wiki files.
    from core.agent_surface import AgentSurfaceService
    config.set('connectors.allowlist', ['corpus_index'])
    service = AgentSurfaceService(config, layout=layout, db=db)
    service.run_connector('corpus_index', execute=True)
    assert (layout.wiki_root / PAGE).exists()
    doc.path.write_text('# Ordinary document\nNo reusable example here.')
    assert asyncio.run(connector.collect())['prompt_index']['sources'] == 0
    config.set('sources.corpus_index.prompt_index_enabled', 'yes')
    with pytest.raises(ValueError, match='must be boolean'):
        asyncio.run(connector.collect())
