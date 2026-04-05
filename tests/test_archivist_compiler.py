from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import subprocess
import sys

from core.archivist_compiler import ArchivistCompiler
from core.archivist_state import load_archivist_topic_state
from core.archivist_topics import ArchivistTopicDefinition
from core.config import config
from core.metadata_db import MetadataDB
from core.path_layout import build_path_layout


class FakeLLMInterface:
    def __init__(self, content: str):
        self.content = content
        self.calls: list[dict[str, str]] = []

    def resolve_task_route(self, task: str):
        assert task == "archivist"
        return (
            "openrouter",
            "anthropic/claude-3-haiku",
            {"max_tokens": 1600, "temperature": 0.2},
        )

    async def generate(self, prompt: str, system_prompt: str = None, **kwargs):
        self.calls.append(
            {
                "prompt": prompt,
                "system_prompt": system_prompt or "",
                "provider": kwargs.get("provider", ""),
                "model": kwargs.get("model", ""),
            }
        )
        return SimpleNamespace(content=self.content, error=None)


def _configure_runtime_config(tmp_path: Path):
    original = deepcopy(config.data)
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    config.set("llm.prompts.archivist.system_file", "prompts/archivist_system.md")
    config.set("llm.prompts.archivist.user_file", "prompts/archivist_user.md")
    return original


def _write_prompt_files(tmp_path: Path):
    prompts_dir = tmp_path / "prompts"
    prompts_dir.mkdir(parents=True, exist_ok=True)
    (prompts_dir / "archivist_system.md").write_text(
        "SYSTEM TEST PROMPT\nUse [S1] style citations.\n",
        encoding="utf-8",
    )
    (prompts_dir / "archivist_user.md").write_text(
        "Topic={topic_title}\nDescription={topic_description}\nCount={candidate_count}\n{source_manifest}\n",
        encoding="utf-8",
    )


def _write_source_files(layout):
    tweets_dir = layout.vault_root / "tweets"
    stars_dir = layout.vault_root / "stars"
    tweets_dir.mkdir(parents=True, exist_ok=True)
    stars_dir.mkdir(parents=True, exist_ok=True)
    (tweets_dir / "companion_note.md").write_text(
        "---\n"
        "title: Companion Memory Notes\n"
        "tags:\n"
        "  - companion_ai\n"
        "  - introspection\n"
        "---\n"
        "\n"
        "# Companion Memory Notes\n"
        "\n"
        "Companion AI systems need stable persona memory and explicit introspection loops.\n",
        encoding="utf-8",
    )
    (stars_dir / "owner_repo_summary.md").write_text(
        "---\n"
        "title: Persona Memory Toolkit\n"
        "tags:\n"
        "  - companion_ai\n"
        "  - personas\n"
        "---\n"
        "\n"
        "# Persona Memory Toolkit\n"
        "\n"
        "This repository summary covers tooling for persona continuity, long-term memory, and agent self-reflection.\n",
        encoding="utf-8",
    )


def _build_topic() -> ArchivistTopicDefinition:
    return ArchivistTopicDefinition(
        id="companion-ai-research",
        title="Companion AI Research",
        output_path="pages/topic-companion-ai-research.md",
        include_roots=("tweets", "stars"),
        source_types=("tweet", "repository"),
        include_tags=("companion_ai",),
        include_terms=("companion ai", "persona"),
        cadence_hours=12.0,
        max_sources=10,
        allow_manual_force=True,
    )


def test_archivist_compiler_writes_topic_page_and_records_state(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    original = _configure_runtime_config(tmp_path)
    try:
        _write_prompt_files(tmp_path)
        layout = build_path_layout(config, project_root=tmp_path)
        _write_source_files(layout)
        db = MetadataDB(str(layout.database_path))
        llm = FakeLLMInterface(
            "## Overview\n"
            "Companion systems depend on stable persona memory [S1] and repository-level tooling for continuity [S2].\n"
            "\n"
            "## Key Signals\n"
            "- Persona continuity matters [S1]\n"
            "- Tooling is becoming more explicit about long-term identity [S2]\n"
        )
        compiler = ArchivistCompiler(
            config,
            project_root=tmp_path,
            layout=layout,
            db=db,
            llm_interface=llm,
        )

        result = __import__("asyncio").run(compiler.compile_topic(_build_topic()))

        page_path = layout.wiki_root / "pages" / "topic-companion-ai-research.md"
        assert result.status == "compiled"
        assert result.page_path == page_path
        assert len(llm.calls) == 1
        content = page_path.read_text(encoding="utf-8")
        assert "# Companion AI Research" in content
        assert "Companion systems depend on stable persona memory [S1]" in content
        assert "## Sources" in content
        assert "tweets/companion_note.md" in content
        assert "stars/owner_repo_summary.md" in content

        state = load_archivist_topic_state("companion-ai-research", db=db)
        assert state.last_success_at is not None
        assert state.last_candidate_count == 2
        assert state.last_model_provider == "openrouter"
        assert state.last_model == "anthropic/claude-3-haiku"
    finally:
        config.data = original


def test_archivist_compiler_skips_up_to_date_topics_without_recalling_llm(
    tmp_path: Path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    original = _configure_runtime_config(tmp_path)
    try:
        _write_prompt_files(tmp_path)
        layout = build_path_layout(config, project_root=tmp_path)
        _write_source_files(layout)
        db = MetadataDB(str(layout.database_path))
        llm = FakeLLMInterface("## Overview\nInitial synthesis [S1].\n")
        compiler = ArchivistCompiler(
            config,
            project_root=tmp_path,
            layout=layout,
            db=db,
            llm_interface=llm,
        )
        topic = _build_topic()

        __import__("asyncio").run(compiler.compile_topic(topic))
        second = __import__("asyncio").run(compiler.compile_topic(topic))

        assert second.status == "skipped"
        assert second.reason == "up_to_date"
        assert len(llm.calls) == 1
    finally:
        config.data = original


def test_archivist_compiler_uses_external_prompt_files(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    original = _configure_runtime_config(tmp_path)
    try:
        _write_prompt_files(tmp_path)
        layout = build_path_layout(config, project_root=tmp_path)
        _write_source_files(layout)
        db = MetadataDB(str(layout.database_path))
        llm = FakeLLMInterface("## Overview\nPrompt plumbing check [S1].\n")
        compiler = ArchivistCompiler(
            config,
            project_root=tmp_path,
            layout=layout,
            db=db,
            llm_interface=llm,
        )

        __import__("asyncio").run(compiler.compile_topic(_build_topic()))

        assert llm.calls
        assert "SYSTEM TEST PROMPT" in llm.calls[0]["system_prompt"]
        assert "Topic=Companion AI Research" in llm.calls[0]["prompt"]
        assert "[S1]" in llm.calls[0]["prompt"]
    finally:
        config.data = original


def test_archivist_cli_help_exposes_runtime_command():
    repo_root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "thoth.py", "archivist", "--help"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--topics" in result.stdout
    assert "--force" in result.stdout
    assert "--dry-run" in result.stdout
    assert "--benchmark" in result.stdout
