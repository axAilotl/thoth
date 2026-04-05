import asyncio
from copy import deepcopy
from pathlib import Path

import pytest

from core.config import config
from core.metadata_db import MetadataDB
from processors.github_stars_processor import GitHubRepo, GitHubStarsProcessor


@pytest.fixture
def restore_runtime_config():
    original = deepcopy(config.data)
    yield
    config.data = original


def _configure_runtime_paths(tmp_path: Path) -> None:
    vault_root = tmp_path / "vault"
    config.data = {}
    config.set("paths.vault_dir", str(vault_root))
    config.set("vault_dir", str(vault_root))
    config.set("llm.tasks.summary.enabled", False)


def _build_repo() -> GitHubRepo:
    return GitHubRepo(
        id=1,
        name="repo",
        full_name="org/repo",
        description="desc",
        html_url="https://github.com/org/repo",
        stargazers_count=10,
        forks_count=2,
        language="Python",
        topics=[],
        created_at="2026-04-04T00:00:00Z",
        updated_at="2026-04-04T00:00:00Z",
        pushed_at="2026-04-04T00:00:00Z",
        license_name="MIT",
    )


def test_github_resume_skips_when_readme_and_summary_exist(
    tmp_path: Path,
    monkeypatch,
    restore_runtime_config,
):
    _configure_runtime_paths(tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", "token-123")
    metadata_db = MetadataDB(db_path=str(tmp_path / "meta.db"))

    processor = GitHubStarsProcessor(
        vault_path=str(tmp_path / "vault"),
        metadata_db=metadata_db,
    )
    readme_file = processor.repos_dir / "github_org_repo_README.md"
    summary_file = processor.stars_dir / "org_repo_summary.md"
    readme_file.parent.mkdir(parents=True, exist_ok=True)
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    readme_file.write_text("# cached readme\n", encoding="utf-8")
    summary_file.write_text("# cached summary\n", encoding="utf-8")

    async def fail_if_called(repo):
        raise AssertionError("_download_readme should not run when cached README and summary exist")

    monkeypatch.setattr(processor, "_download_readme", fail_if_called)

    result = asyncio.run(processor._process_single_repo(_build_repo(), resume=True))

    assert result is False


def test_github_resume_generates_missing_summary_from_cached_readme(
    tmp_path: Path,
    monkeypatch,
    restore_runtime_config,
):
    _configure_runtime_paths(tmp_path)
    monkeypatch.setenv("GITHUB_TOKEN", "token-123")
    metadata_db = MetadataDB(db_path=str(tmp_path / "meta.db"))

    processor = GitHubStarsProcessor(
        vault_path=str(tmp_path / "vault"),
        metadata_db=metadata_db,
    )
    readme_file = processor.repos_dir / "github_org_repo_README.md"
    summary_file = processor.stars_dir / "org_repo_summary.md"
    readme_file.parent.mkdir(parents=True, exist_ok=True)
    readme_file.write_text("# cached readme\nuseful content\n", encoding="utf-8")

    async def fail_if_called(repo):
        raise AssertionError("_download_readme should not run when cached README exists")

    monkeypatch.setattr(processor, "_download_readme", fail_if_called)

    created = []

    async def record_summary(repo, output_path):
        created.append((repo.readme_content, output_path))
        output_path.write_text("# generated summary\n", encoding="utf-8")

    monkeypatch.setattr(processor, "_create_summary_file", record_summary)

    result = asyncio.run(processor._process_single_repo(_build_repo(), resume=True))

    assert result is True
    assert summary_file.exists()
    assert created
    assert created[0][0] == "# cached readme\nuseful content"
