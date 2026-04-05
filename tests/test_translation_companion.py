from copy import deepcopy
from pathlib import Path

import pytest

from core.artifacts import WebClipperArtifact
from core.config import Config, config
from core.llm_interface import LLMResponse
from core.path_layout import build_path_layout
from core.translation_companion import EnglishCompanionPublisher


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
    config.set("llm.tasks.translation.enabled", True)
    config.set("llm.tasks.translation.fallback", [{"provider": "anthropic", "model": "mock"}])


class FakeTranslationLLM:
    def resolve_task_route(self, task: str):
        if task != "translation":
            return None
        return ("anthropic", "mock-model", {"max_tokens": 800, "temperature": 0.2})

    async def generate(self, prompt: str, system_prompt: str | None = None, **kwargs):
        assert "Source language: es" in prompt
        return LLMResponse(
            content='{"title":"Example note","body":"Translated body with **markdown**."}',
            model="mock-model",
            provider="anthropic",
        )


@pytest.mark.anyio
async def test_translation_companion_publishes_english_note(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)
    layout = build_path_layout(config)

    source_note = layout.vault_root / "Clippings" / "capture.md"
    source_note.parent.mkdir(parents=True, exist_ok=True)
    source_note.write_text(
        "---\n"
        "title: ejemplo\n"
        "lang: es\n"
        "url: https://example.com/articulo\n"
        "---\n"
        "\n"
        "# ejemplo\n"
        "\n"
        "Contenido en español.\n",
        encoding="utf-8",
    )

    artifact = WebClipperArtifact(
        id="webclip:Clippings/capture.md",
        source_type="web_clipper",
        raw_content=source_note.read_text(encoding="utf-8"),
        ingested_at="2026-04-04T00:00:00Z",
        source_path=str(source_note),
        source_relative_path="Clippings/capture.md",
        file_type="note",
        title="ejemplo",
        frontmatter={"title": "ejemplo", "lang": "es", "url": "https://example.com/articulo"},
        body="# ejemplo\n\nContenido en español.\n",
        source_checksum="abc123",
        source_size_bytes=source_note.stat().st_size,
        source_language="es",
        source_url="https://example.com/articulo",
    )

    publisher = EnglishCompanionPublisher(
        config,
        layout=layout,
        llm_interface=FakeTranslationLLM(),
    )

    result = await publisher.publish_web_clipper_artifact(artifact)
    output_path = (
        layout.vault_root
        / "translations"
        / "Clippings"
        / "capture.en.md"
    )

    assert result.status == "created"
    assert result.output_path == output_path
    assert output_path.exists()
    assert "translated_from: es" in output_path.read_text(encoding="utf-8")
    assert "language: en" in output_path.read_text(encoding="utf-8")
    assert "Translated body with **markdown**." in output_path.read_text(encoding="utf-8")
    assert source_note.read_text(encoding="utf-8").startswith("---\n")
    assert publisher.db.get_file_entry(str(output_path.relative_to(layout.vault_root))).file_type == "translation"


@pytest.mark.anyio
async def test_translation_companion_skips_english_source(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_config(tmp_path)
    layout = build_path_layout(config)

    source_note = layout.vault_root / "Clippings" / "capture.md"
    source_note.parent.mkdir(parents=True, exist_ok=True)
    source_note.write_text(
        "---\n"
        "title: example\n"
        "lang: en\n"
        "---\n"
        "\n"
        "English content.\n",
        encoding="utf-8",
    )

    artifact = WebClipperArtifact(
        id="webclip:Clippings/capture.md",
        source_type="web_clipper",
        raw_content=source_note.read_text(encoding="utf-8"),
        ingested_at="2026-04-04T00:00:00Z",
        source_path=str(source_note),
        source_relative_path="Clippings/capture.md",
        file_type="note",
        title="example",
        frontmatter={"title": "example", "lang": "en"},
        body="English content.\n",
        source_checksum="abc123",
        source_size_bytes=source_note.stat().st_size,
        source_language="en",
    )

    publisher = EnglishCompanionPublisher(
        config,
        layout=layout,
        llm_interface=FakeTranslationLLM(),
    )

    result = await publisher.publish_web_clipper_artifact(artifact)

    assert result.status == "skipped"
    assert result.reason == "source already English"
    assert not (layout.vault_root / "translations").exists()


def test_translation_validation_requires_a_fallback_route():
    cfg = Config()
    cfg.set("paths.vault_dir", "/tmp/vault")
    cfg.set("paths.system_dir", ".thoth_system")
    cfg.set("paths.cache_dir", "graphql_cache")
    cfg.set("database.path", "meta.db")
    cfg.set("llm.tasks.translation.enabled", True)

    errors = cfg.validate()

    assert any(
        "llm.tasks.translation.fallback must be a non-empty list" in error
        for error in errors
    )
