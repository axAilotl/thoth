from copy import deepcopy
from pathlib import Path

import pytest

from core.config import config
from core.path_layout import build_path_layout
from core.staged_assets import (
    StagedAssetPublisher,
    StagedAssetValidationError,
    validate_existing_asset,
)


@pytest.fixture
def restore_runtime_config():
    original = deepcopy(config.data)
    yield
    config.data = original


def _configure_runtime_paths(tmp_path: Path) -> None:
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")


def test_publish_pdf_stages_under_system_tmp(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)
    layout = build_path_layout(config)
    publisher = StagedAssetPublisher(config, layout=layout)
    destination = layout.vault_root / "pdfs" / "paper.pdf"

    published = publisher.publish_bytes(
        destination,
        b"%PDF-1.7\n1 0 obj\n<<>>\nendobj\n",
        asset_type="pdf",
    )

    assert destination.exists()
    assert published.path == destination
    assert published.size_bytes == destination.stat().st_size
    assert published.sha256
    assert not list((layout.temp_root / "downloads").glob("*.part"))
    assert validate_existing_asset(destination, asset_type="pdf") is True


def test_invalid_pdf_never_publishes_destination(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)
    layout = build_path_layout(config)
    publisher = StagedAssetPublisher(config, layout=layout)
    destination = layout.vault_root / "pdfs" / "broken.pdf"

    with pytest.raises(StagedAssetValidationError):
        publisher.publish_bytes(
            destination,
            b"not really a pdf",
            asset_type="pdf",
        )

    assert not destination.exists()
    assert not list((layout.temp_root / "downloads").glob("*.part"))


def test_image_and_video_validation_is_strict(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)
    layout = build_path_layout(config)
    publisher = StagedAssetPublisher(config, layout=layout)

    image_path = layout.vault_root / "images" / "clip.png"
    video_path = layout.vault_root / "videos" / "clip.mp4"

    publisher.publish_bytes(
        image_path,
        b"\x89PNG\r\n\x1a\npayload",
        asset_type="image",
    )
    publisher.publish_bytes(
        video_path,
        b"\x00\x00\x00\x18ftypmp42payload",
        asset_type="video",
    )

    assert validate_existing_asset(image_path, asset_type="image") is True
    assert validate_existing_asset(video_path, asset_type="video") is True

    with pytest.raises(StagedAssetValidationError):
        publisher.publish_bytes(
            layout.vault_root / "videos" / "broken.mp4",
            b"not-an-mp4",
            asset_type="video",
        )


def test_publish_file_copies_source_into_managed_tree(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)
    layout = build_path_layout(config)
    publisher = StagedAssetPublisher(config, layout=layout)

    source = layout.raw_root / "web-clipper" / "assets" / "clip.png"
    destination = layout.library_root / "web-clipper" / "assets" / "clip.png"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(b"\x89PNG\r\n\x1a\npayload")

    published = publisher.publish_file(source, destination, asset_type="image")

    assert published.path == destination
    assert destination.exists()
    assert destination.read_bytes() == source.read_bytes()
    assert source.exists()


def test_publish_cross_device_falls_back_to_destination_temp(
    tmp_path: Path, monkeypatch, restore_runtime_config
):
    monkeypatch.chdir(tmp_path)
    _configure_runtime_paths(tmp_path)
    layout = build_path_layout(config)
    publisher = StagedAssetPublisher(config, layout=layout)
    destination = layout.vault_root / "images" / "clip.png"
    monkeypatch.setattr(publisher, "_same_filesystem", lambda source, target: False)

    published = publisher.publish_bytes(
        destination,
        b"\x89PNG\r\n\x1a\npayload",
        asset_type="image",
    )

    assert published.path == destination
    assert destination.exists()
    assert validate_existing_asset(destination, asset_type="image") is True
    assert not list((layout.temp_root / "downloads").glob("*.part"))
    assert not list(destination.parent.glob(".thoth-publish-*.part"))
