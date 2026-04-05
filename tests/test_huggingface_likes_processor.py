import asyncio
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

from core.config import config
from core.metadata_db import MetadataDB
from processors.huggingface_likes_processor import HuggingFaceLikesProcessor, HuggingFaceRepo


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
    config.set("sources.huggingface.username", "example-user")


def _install_fake_hf_module(
    monkeypatch,
    *,
    likes=None,
    hf_hub_download=None,
    repo_info=None,
):
    fake_module = SimpleNamespace(
        list_liked_repos=likes
        or (lambda user, token=None: SimpleNamespace(models=[], datasets=[], spaces=[])),
        hf_hub_download=hf_hub_download
        or (lambda **kwargs: (_ for _ in ()).throw(AssertionError("hf_hub_download should not be called"))),
        repo_info=repo_info or (lambda *args, **kwargs: None),
    )
    fake_utils = SimpleNamespace(
        RepositoryNotFoundError=RuntimeError,
        EntryNotFoundError=FileNotFoundError,
    )
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_module)
    monkeypatch.setitem(sys.modules, "huggingface_hub.utils", fake_utils)


def _build_repo() -> HuggingFaceRepo:
    return HuggingFaceRepo(
        id="org/repo",
        name="repo",
        full_name="org/repo",
        description="desc",
        html_url="https://huggingface.co/org/repo",
        likes=1,
        downloads=1,
        repo_type="model",
        tags=[],
        created_at=None,
        updated_at=None,
        license=None,
        library=None,
    )


def test_huggingface_resume_skips_when_readme_and_summary_exist(
    tmp_path: Path,
    monkeypatch,
    restore_runtime_config,
):
    _configure_runtime_paths(tmp_path)
    _install_fake_hf_module(monkeypatch)
    metadata_db = MetadataDB(db_path=str(tmp_path / "meta.db"))

    processor = HuggingFaceLikesProcessor(
        vault_path=str(tmp_path / "vault"),
        metadata_db=metadata_db,
        cache_dir=tmp_path / "hf_cache",
    )
    readme_file = processor.repos_dir / "hf_org_repo_README.md"
    summary_file = processor.stars_dir / "hf_org_repo_summary.md"
    readme_file.parent.mkdir(parents=True, exist_ok=True)
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    readme_file.write_text("# cached readme\n", encoding="utf-8")
    summary_file.write_text("# cached summary\n", encoding="utf-8")

    async def fail_if_called(repo):
        raise AssertionError("_download_readme should not run when cached README and summary exist")

    monkeypatch.setattr(processor, "_download_readme", fail_if_called)

    result = asyncio.run(processor._process_single_repo(_build_repo(), resume=True))

    assert result is False


def test_huggingface_resume_generates_missing_summary_from_cached_readme(
    tmp_path: Path,
    monkeypatch,
    restore_runtime_config,
):
    _configure_runtime_paths(tmp_path)
    _install_fake_hf_module(monkeypatch)
    metadata_db = MetadataDB(db_path=str(tmp_path / "meta.db"))

    processor = HuggingFaceLikesProcessor(
        vault_path=str(tmp_path / "vault"),
        metadata_db=metadata_db,
        cache_dir=tmp_path / "hf_cache",
    )
    readme_file = processor.repos_dir / "hf_org_repo_README.md"
    summary_file = processor.stars_dir / "hf_org_repo_summary.md"
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


def test_huggingface_prefilters_cached_repo_before_repo_info(
    tmp_path: Path,
    monkeypatch,
    restore_runtime_config,
):
    _configure_runtime_paths(tmp_path)
    _install_fake_hf_module(
        monkeypatch,
        likes=lambda user, token=None: SimpleNamespace(models=["org/repo"], datasets=[], spaces=[]),
        repo_info=lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("repo_info should not run for cached repos")
        ),
    )
    metadata_db = MetadataDB(db_path=str(tmp_path / "meta.db"))

    processor = HuggingFaceLikesProcessor(
        vault_path=str(tmp_path / "vault"),
        metadata_db=metadata_db,
        cache_dir=tmp_path / "hf_cache",
    )
    readme_file = processor.repos_dir / "hf_org_repo_README.md"
    summary_file = processor.stars_dir / "hf_org_repo_summary.md"
    readme_file.parent.mkdir(parents=True, exist_ok=True)
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    readme_file.write_text("# cached readme\n", encoding="utf-8")
    summary_file.write_text("# cached summary\n", encoding="utf-8")

    stats = asyncio.run(processor.fetch_and_process_liked_repos(limit=1, resume=True))

    assert stats.updated == 0
    assert stats.skipped == 1
    assert stats.errors == 0
    assert stats.total_processed == 1
