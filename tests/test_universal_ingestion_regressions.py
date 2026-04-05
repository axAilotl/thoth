from copy import deepcopy
from pathlib import Path

import pytest

from collectors.web_clipper_collector import WebClipperCollector
from core.config import config
from core.ingestion_runtime import KnowledgeArtifactRuntime
from core.llm_interface import LLMResponse
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout
from core.translation_companion import EnglishCompanionPublisher
from core.wiki_contract import normalize_wiki_slug


@pytest.fixture
def restore_runtime_config():
    original = deepcopy(config.data)
    yield
    config.data = original


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _configure_runtime_config(tmp_path: Path) -> None:
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    config.set("sources.web_clipper.note_dirs", ["Clippings"])
    config.set("sources.web_clipper.attachment_dirs", ["clipper-assets"])
    config.set("llm.tasks.translation.enabled", True)
    config.set(
        "llm.tasks.translation.fallback",
        [{"provider": "anthropic", "model": "mock-model"}],
    )


class FakeTranslationLLM:
    def resolve_task_route(self, task: str):
        if task != "translation":
            return None
        return ("anthropic", "mock-model", {"max_tokens": 800, "temperature": 0.2})

    async def generate(self, prompt: str, system_prompt: str | None = None, **kwargs):
        assert "Source language: es" in prompt
        return LLMResponse(
            content=(
                '{"title":"Hello world","body":"Translated body for the clipped note."}'
            ),
            model="mock-model",
            provider="anthropic",
        )


@pytest.mark.anyio
async def test_universal_ingestion_loop_keeps_sources_in_vault_and_outputs_in_managed_layers(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)

    layout = build_path_layout(config)
    db = MetadataDB()

    # Canonical runtime state stays under .thoth_system, not in the synced vault.
    assert layout.system_root == tmp_path / ".thoth_system"
    assert layout.database_path == layout.system_root / "meta.db"
    assert layout.cache_root == layout.system_root / "graphql_cache"
    assert layout.temp_root == layout.system_root / "tmp"

    note_path = layout.vault_root / "Clippings" / "capture.md"
    note_path.parent.mkdir(parents=True, exist_ok=True)
    source_text = (
        "---\n"
        "title: hola mundo\n"
        "lang: es\n"
        "url: https://example.com/articulo\n"
        "---\n"
        "\n"
        "# hola mundo\n"
        "\n"
        "Contenido original en español.\n"
    )
    note_path.write_text(source_text, encoding="utf-8")

    attachment_path = layout.vault_root / "clipper-assets" / "capture.pdf"
    attachment_path.parent.mkdir(parents=True, exist_ok=True)
    attachment_path.write_bytes(b"%PDF-1.7\n1 0 obj\n<<>>\nendobj\n")

    ignored_path = layout.vault_root / "ignored" / "outside.md"
    ignored_path.parent.mkdir(parents=True, exist_ok=True)
    ignored_path.write_text("# not scanned\n", encoding="utf-8")

    collector = WebClipperCollector(config, layout=layout, db=db)
    runtime = KnowledgeArtifactRuntime(config, layout=layout, db=db)
    runtime._companion_publisher = EnglishCompanionPublisher(
        config,
        layout=layout,
        db=db,
        llm_interface=FakeTranslationLLM(),
    )

    discovered = collector.collect()

    assert {record.file_type for record in discovered} == {"note", "attachment"}
    assert all("ignored" not in str(record.path) for record in discovered)

    note_record = next(record for record in discovered if record.file_type == "note")
    attachment_record = next(
        record for record in discovered if record.file_type == "attachment"
    )
    assert note_record.artifact is not None
    assert attachment_record.managed_path == (
        layout.vault_root / "clipper-assets" / "capture.pdf"
    )
    assert attachment_record.managed_path.exists()
    assert attachment_record.managed_path.read_bytes() == attachment_path.read_bytes()
    assert not list((layout.temp_root / "downloads").glob("*.part"))

    queued = db.get_pending_ingestions(limit=10)
    assert [entry.artifact_type for entry in queued] == ["web_clipper"]

    results = await runtime.process_pending_ingestions_once()
    assert len(results) == 1
    assert results[0].artifact_type == "web_clipper"
    assert results[0].status == "processed"
    assert db.get_ingestion_entry(note_record.artifact.id).status == "processed"

    wiki_page = (
        layout.wiki_root
        / "pages"
        / f"clip-{normalize_wiki_slug(note_record.artifact.title)}.md"
    )
    assert wiki_page.exists()
    wiki_content = wiki_page.read_text(encoding="utf-8")
    assert "language: es" in wiki_content
    assert "Clippings/capture.md" in wiki_content

    translation = await runtime.publish_english_companion(note_record.artifact)
    translation_path = (
        layout.vault_root
        / "translations"
        / "Clippings"
        / "capture.en.md"
    )
    assert translation.status == "created"
    assert translation.output_path == translation_path
    assert translation_path.exists()
    translation_content = translation_path.read_text(encoding="utf-8")
    assert "translated_from: es" in translation_content
    assert "language: en" in translation_content
    assert "Translated body for the clipped note." in translation_content
    assert note_path.read_text(encoding="utf-8") == source_text

    translation_meta = db.get_file_entry(
        str(translation_path.relative_to(layout.vault_root))
    )
    assert translation_meta is not None
    assert translation_meta.file_type == "translation"
