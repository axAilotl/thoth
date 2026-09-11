from collections import OrderedDict
from pathlib import Path

import pytest

import core.wiki_io as wiki_io


@pytest.fixture
def bounded_cache(monkeypatch):
    monkeypatch.setattr(wiki_io, "_WIKI_DOCUMENT_CACHE", OrderedDict())
    monkeypatch.setattr(wiki_io, "_WIKI_DOCUMENT_CACHE_MAX_SIZE", 2)


def test_wiki_document_cache_evicts_least_recently_read_page(tmp_path: Path, bounded_cache):
    paths = [tmp_path / f"page-{index}.md" for index in range(3)]
    for path in paths:
        path.write_text(path.stem, encoding="utf-8")

    first = wiki_io.read_document_cached(paths[0])
    second = wiki_io.read_document_cached(paths[1])
    assert wiki_io.read_document_cached(paths[0]) is first
    wiki_io.read_document_cached(paths[2])

    assert len(wiki_io._WIKI_DOCUMENT_CACHE) == 2
    assert wiki_io.read_document_cached(paths[0]) is first
    assert wiki_io.read_document_cached(paths[1]) is not second
    assert len(wiki_io._WIKI_DOCUMENT_CACHE) == 2


def test_wiki_document_cache_refreshes_changed_content(tmp_path: Path, bounded_cache):
    path = tmp_path / "page.md"
    path.write_text("---\ntitle: Original\n---\nOriginal body", encoding="utf-8")
    original = wiki_io.read_document_cached(path)
    path.write_text("---\ntitle: Revised\n---\nChanged body with more text", encoding="utf-8")

    revised = wiki_io.read_document_cached(path)

    assert revised is not original
    assert revised.frontmatter["title"] == "Revised"
    assert revised.body == "Changed body with more text"
    assert wiki_io.read_document_cached(path) is revised
    assert len(wiki_io._WIKI_DOCUMENT_CACHE) == 1
