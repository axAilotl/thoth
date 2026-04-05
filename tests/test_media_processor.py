from copy import deepcopy
from pathlib import Path

import pytest

from core.config import config
from core.download_tracker import DownloadTracker
from core.data_models import MediaItem
from processors.media_processor import MediaProcessor


@pytest.fixture
def restore_runtime_config():
    original = deepcopy(config.data)
    yield
    config.data = original


def _configure_runtime_paths(tmp_path: Path) -> None:
    vault_root = tmp_path / "vault"
    config.data = {}
    config.set("paths.vault_dir", str(vault_root))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.images_dir", "images")
    config.set("paths.videos_dir", "videos")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")
    config.set("database.enabled", False)

    # MediaProcessor still reads the compatibility aliases directly.
    config.set("vault_dir", str(vault_root))
    config.set("images_dir", "images")
    config.set("videos_dir", "videos")
    config.set("system_dir", ".thoth_system")


def test_media_processor_reuses_tracked_download_without_network(
    tmp_path: Path,
    monkeypatch,
    restore_runtime_config,
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)

    tracked_file = tmp_path / "vault" / "images" / "legacy-image.jpg"
    tracked_file.parent.mkdir(parents=True, exist_ok=True)
    tracked_file.write_bytes(b"\xff\xd8\xff\xe0jpeg-payload")

    tracker = DownloadTracker(str(tmp_path / ".thoth_system" / "download_tracking.json"))
    media_url = "https://pbs.twimg.com/media/example.jpg"
    tracker.record_success(
        media_url,
        tracked_file.name,
        str(tracked_file),
        tracked_file.stat().st_size,
    )

    monkeypatch.setattr("processors.media_processor.get_download_tracker", lambda: tracker)

    processor = MediaProcessor()

    def fail_if_called(*args, **kwargs):
        raise AssertionError("network fetch should not run when tracked media exists")

    monkeypatch.setattr(processor.session, "get", fail_if_called)

    media_item = MediaItem(
        media_id="1",
        media_url=media_url,
        media_type="photo",
    )

    result = processor._download_media(media_item, tweet_id="2038545915020190047")

    assert result is True
    assert media_item.downloaded is True
    assert media_item.filename == tracked_file.name
    assert processor._last_download_was_new is False
