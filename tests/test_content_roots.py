"""Deterministic local tests for behavior-owned content roots and carriers."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from core.config import config
from core.connector_capture import (
    connector_raw_roots,
    write_connector_raw_json,
)
from core.content_root_adapters import (
    ContentAdapterError,
    FilesystemContentAdapter,
    InMemoryContentAdapter,
    ObsidianContentAdapter,
)
from core.content_roots import (
    ArtifactHandle,
    ContentRoot,
    ContentRootError,
    ContentRootMode,
    ContentRootPolicy,
    build_content_root_policy,
    content_root_from_config,
    content_roots_from_config,
    validate_content_roots,
)
from core.path_layout import PathLayout, build_path_layout


def _make_root(
    tmp_path: Path,
    root_id: str,
    mode: ContentRootMode,
    subdir: str,
    adapter: str = "filesystem",
) -> ContentRoot:
    base = tmp_path / subdir
    base.mkdir(parents=True, exist_ok=True)
    return ContentRoot(
        root_id=root_id,
        mode=mode,
        adapter_kind=adapter,
        base_path=base,
    )


@pytest.fixture
def filesystem_adapter() -> FilesystemContentAdapter:
    return FilesystemContentAdapter()


@pytest.fixture
def obsidian_adapter() -> ObsidianContentAdapter:
    return ObsidianContentAdapter()


@pytest.fixture
def memory_adapter() -> InMemoryContentAdapter:
    return InMemoryContentAdapter()


@pytest.fixture
def restore_runtime_config():
    original = deepcopy(config.data)
    yield
    config.data = original


def test_content_root_requires_non_empty_id(tmp_path: Path):
    with pytest.raises(ContentRootError, match="id cannot be empty"):
        ContentRoot(
            root_id="",
            mode=ContentRootMode.WATCH_ONLY,
            adapter_kind="filesystem",
            base_path=tmp_path,
        )


def test_content_root_rejects_unknown_mode(tmp_path: Path):
    with pytest.raises(ContentRootError, match="invalid content root mode"):
        ContentRoot(
            root_id="bad",
            mode="unknown",  # type: ignore[arg-type]
            adapter_kind="filesystem",
            base_path=tmp_path,
        )


def test_validate_content_roots_rejects_duplicate_ids(tmp_path: Path):
    roots = (
        _make_root(tmp_path, "dup", ContentRootMode.WATCH_ONLY, "a"),
        _make_root(tmp_path, "dup", ContentRootMode.MANAGED_INBOX, "b"),
    )
    with pytest.raises(ContentRootError, match="duplicate content root id"):
        validate_content_roots(roots)


def test_validate_content_roots_rejects_overlapping_paths(tmp_path: Path):
    roots = (
        _make_root(tmp_path, "parent", ContentRootMode.WATCH_ONLY, "parent"),
        _make_root(
            tmp_path,
            "child",
            ContentRootMode.MANAGED_INBOX,
            "parent/child",
        ),
    )
    with pytest.raises(ContentRootError, match="overlap"):
        validate_content_roots(roots)


def test_validate_content_roots_rejects_operational_path_inside_root(
    tmp_path: Path,
):
    root = _make_root(tmp_path, "vault", ContentRootMode.MANAGED_INBOX, "vault")
    operational = tmp_path / "vault" / ".thoth_system"
    with pytest.raises(ContentRootError, match="operational path.*overlap"):
        validate_content_roots((root,), operational_paths=(operational,))


def test_validate_content_roots_allows_non_overlapping_siblings(
    tmp_path: Path,
):
    roots = (
        _make_root(tmp_path, "vault", ContentRootMode.MANAGED_INBOX, "vault"),
        _make_root(tmp_path, "wiki", ContentRootMode.PROJECTION_OUTPUT, "wiki"),
    )
    validate_content_roots(roots)


def test_filesystem_adapter_lists_and_reads_artifacts(
    tmp_path: Path,
    filesystem_adapter: FilesystemContentAdapter,
):
    root = _make_root(tmp_path, "vault", ContentRootMode.MANAGED_INBOX, "vault")
    (root.base_path / "tweets").mkdir(parents=True)
    (root.base_path / "tweets" / "a.md").write_text("hello", encoding="utf-8")

    handles = list(filesystem_adapter.list_artifacts(root))
    assert len(handles) == 1
    handle = handles[0]
    assert handle.root_id == "vault"
    assert handle.relpath == "tweets/a.md"
    assert handle.digest is not None
    assert handle.size_bytes == 5
    assert filesystem_adapter.read_artifact(handle) == b"hello"


def test_filesystem_adapter_rejects_traversal(
    tmp_path: Path,
    filesystem_adapter: FilesystemContentAdapter,
):
    root = _make_root(tmp_path, "vault", ContentRootMode.MANAGED_INBOX, "vault")
    with pytest.raises(ContentAdapterError, match="traverse"):
        filesystem_adapter.write_artifact(root, "../escape.txt", b"x")


def test_filesystem_adapter_rejects_absolute_relpath(
    tmp_path: Path,
    filesystem_adapter: FilesystemContentAdapter,
):
    root = _make_root(tmp_path, "vault", ContentRootMode.MANAGED_INBOX, "vault")
    with pytest.raises(ContentAdapterError, match="relative"):
        filesystem_adapter.write_artifact(root, "/etc/passwd", b"x")


def test_filesystem_adapter_rejects_symlinks(
    tmp_path: Path,
    filesystem_adapter: FilesystemContentAdapter,
):
    root = _make_root(tmp_path, "vault", ContentRootMode.MANAGED_INBOX, "vault")
    outside = tmp_path / "outside.txt"
    outside.write_text("secret", encoding="utf-8")
    link = root.base_path / "link.md"
    link.symlink_to(outside)

    with pytest.raises(ContentAdapterError, match="refusing to follow symlink"):
        list(filesystem_adapter.list_artifacts(root))


def test_watch_only_root_refuses_write_and_delete(
    tmp_path: Path,
    filesystem_adapter: FilesystemContentAdapter,
):
    root = _make_root(tmp_path, "watch", ContentRootMode.WATCH_ONLY, "watch")
    with pytest.raises(ContentAdapterError, match="refusing to write"):
        filesystem_adapter.write_artifact(root, "file.md", b"x")


def test_protected_root_refuses_write_and_delete(
    tmp_path: Path,
    filesystem_adapter: FilesystemContentAdapter,
):
    root = _make_root(tmp_path, "protected", ContentRootMode.PROTECTED, "protected")
    with pytest.raises(ContentAdapterError, match="refusing to write"):
        filesystem_adapter.write_artifact(root, "file.md", b"x")


def test_external_root_refuses_write(
    tmp_path: Path,
    filesystem_adapter: FilesystemContentAdapter,
):
    root = _make_root(tmp_path, "external", ContentRootMode.EXTERNAL, "external")
    with pytest.raises(ContentAdapterError, match="refusing to write"):
        filesystem_adapter.write_artifact(root, "file.md", b"x")


def test_managed_inbox_write_is_idempotent(
    tmp_path: Path,
    filesystem_adapter: FilesystemContentAdapter,
):
    root = _make_root(tmp_path, "inbox", ContentRootMode.MANAGED_INBOX, "inbox")
    content = b"payload"
    handle1 = filesystem_adapter.write_artifact(root, "item.txt", content)
    handle2 = filesystem_adapter.write_artifact(root, "item.txt", content)
    assert handle1.relpath == handle2.relpath
    assert handle1.digest == handle2.digest
    artifacts = list(filesystem_adapter.list_artifacts(root))
    assert len(artifacts) == 1


def test_managed_inbox_consumption_is_recoverable(
    tmp_path: Path,
    filesystem_adapter: FilesystemContentAdapter,
):
    """Re-consuming an inbox by digest does not duplicate or mutate source."""
    root = _make_root(tmp_path, "inbox", ContentRootMode.MANAGED_INBOX, "inbox")
    content = b"capture-one"
    handle = filesystem_adapter.write_artifact(root, "capture/one.json", content)

    # Simulate re-consumption: same content yields same digest and locator.
    handles = list(filesystem_adapter.list_artifacts(root, prefix="capture"))
    assert len(handles) == 1
    assert handles[0].digest == handle.digest
    assert filesystem_adapter.read_artifact(handles[0]) == content

    # Source still exists and is unchanged.
    assert (root.base_path / "capture" / "one.json").read_bytes() == content


def test_projection_output_rebuild_does_not_touch_source_root(
    tmp_path: Path,
    filesystem_adapter: FilesystemContentAdapter,
):
    source_root = _make_root(
        tmp_path, "source", ContentRootMode.WATCH_ONLY, "source"
    )
    output_root = _make_root(
        tmp_path, "output", ContentRootMode.PROJECTION_OUTPUT, "output"
    )
    source_path = source_root.base_path / "input.md"
    source_path.write_text("source text", encoding="utf-8")

    output_handle = filesystem_adapter.write_artifact(
        output_root, "compiled/topic.md", b"compiled text"
    )

    # Output was written.
    assert output_handle.relpath == "compiled/topic.md"
    # Source root is untouched.
    source_files = list(filesystem_adapter.list_artifacts(source_root))
    assert len(source_files) == 1
    assert filesystem_adapter.read_artifact(source_files[0]) == b"source text"


def test_obsidian_adapter_skips_hidden_dirs(
    tmp_path: Path,
    obsidian_adapter: ObsidianContentAdapter,
):
    root = _make_root(tmp_path, "vault", ContentRootMode.MANAGED_INBOX, "obsidian-vault")
    (root.base_path / ".obsidian").mkdir()
    (root.base_path / ".obsidian" / "app.json").write_text("{}", encoding="utf-8")
    (root.base_path / "notes").mkdir()
    (root.base_path / "notes" / "visible.md").write_text("note", encoding="utf-8")

    handles = list(obsidian_adapter.list_artifacts(root))
    assert len(handles) == 1
    assert handles[0].relpath == "notes/visible.md"


def test_obsidian_adapter_refuses_internal_writes(
    tmp_path: Path,
    obsidian_adapter: ObsidianContentAdapter,
):
    root = _make_root(tmp_path, "vault", ContentRootMode.MANAGED_INBOX, "obsidian-vault")
    with pytest.raises(ContentAdapterError, match="Obsidian internal path"):
        obsidian_adapter.write_artifact(root, ".obsidian/config", b"{}")


def test_obsidian_behavior_differs_from_plain_filesystem(
    tmp_path: Path,
    filesystem_adapter: FilesystemContentAdapter,
    obsidian_adapter: ObsidianContentAdapter,
):
    """The Obsidian adapter skips hidden internals; the filesystem adapter does not."""
    root = _make_root(tmp_path, "vault", ContentRootMode.MANAGED_INBOX, "vault")
    (root.base_path / ".obsidian").mkdir()
    (root.base_path / ".obsidian" / "app.json").write_text("{}", encoding="utf-8")

    fs_handles = list(filesystem_adapter.list_artifacts(root))
    obs_handles = list(obsidian_adapter.list_artifacts(root))

    assert len(fs_handles) == 1
    assert len(obs_handles) == 0


def test_build_content_root_policy_pairs_roots_and_carriers(
    tmp_path: Path,
):
    roots = (
        _make_root(tmp_path, "vault", ContentRootMode.MANAGED_INBOX, "vault"),
        _make_root(tmp_path, "wiki", ContentRootMode.PROJECTION_OUTPUT, "wiki"),
    )
    policy = build_content_root_policy(roots, {FilesystemContentAdapter.name: FilesystemContentAdapter()})
    assert isinstance(policy, ContentRootPolicy)
    assert policy.root_by_id("vault").mode == ContentRootMode.MANAGED_INBOX


def test_build_content_root_policy_rejects_unsupported_adapter_mode(
    tmp_path: Path,
):
    """A carrier that does not support the requested mode fails closed."""

    class StubCarrier:
        name = "stub"

        def supports_mode(self, mode: ContentRootMode) -> bool:
            return False

    root = _make_root(tmp_path, "vault", ContentRootMode.MANAGED_INBOX, "vault", adapter="stub")
    with pytest.raises(ContentRootError, match="does not support mode"):
        build_content_root_policy((root,), {StubCarrier.name: StubCarrier()})


def test_path_layout_creates_default_content_roots(tmp_path: Path):
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("paths.raw_dir", "raw")
    config.set("paths.library_dir", "library")
    config.set("paths.wiki_dir", "wiki")
    config.set("paths.digests_dir", "_digests")
    config.set("database.path", "meta.db")

    layout = build_path_layout(config, project_root=tmp_path)

    assert layout.content_root_policy is not None
    roots = layout.content_roots_by_mode(
        ContentRootMode.MANAGED_INBOX,
        ContentRootMode.PROJECTION_OUTPUT,
    )
    assert len(roots) == 2


def test_path_layout_loads_configured_content_roots(tmp_path: Path):
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("database.path", "meta.db")
    config.set(
        "content_roots",
        [
            {
                "id": "notes",
                "mode": "managed_inbox",
                "adapter": "obsidian",
                "path": str(tmp_path / "notes"),
            },
            {
                "id": "published",
                "mode": "projection_output",
                "adapter": "filesystem",
                "path": str(tmp_path / "published"),
            },
        ],
    )

    layout = build_path_layout(config, project_root=tmp_path)

    assert layout.content_root_policy is not None
    notes = layout.content_root_policy.root_by_id("notes")
    assert notes.mode == ContentRootMode.MANAGED_INBOX
    assert notes.adapter_kind == "obsidian"


def test_path_layout_fails_when_operational_path_overlaps_content_root(
    tmp_path: Path,
):
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", str(tmp_path / "vault" / ".thoth_system"))
    config.set("paths.cache_dir", "graphql_cache")
    config.set("database.path", "meta.db")

    with pytest.raises(ContentRootError, match="operational path.*overlap"):
        build_path_layout(config, project_root=tmp_path)


def test_connector_raw_roots_uses_content_roots(tmp_path: Path):
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("database.path", "meta.db")
    config.set(
        "content_roots",
        [
            {
                "id": "inbox",
                "mode": "managed_inbox",
                "adapter": "filesystem",
                "path": str(tmp_path / "inbox"),
            },
            {
                "id": "wiki",
                "mode": "projection_output",
                "adapter": "filesystem",
                "path": str(tmp_path / "wiki"),
            },
        ],
    )

    layout = build_path_layout(config, project_root=tmp_path)
    roots = connector_raw_roots(layout)
    assert tmp_path / "inbox" in roots
    assert tmp_path / "wiki" in roots


def test_write_connector_raw_json_is_idempotent(tmp_path: Path):
    config.data = {}
    config.set("paths.vault_dir", str(tmp_path / "vault"))
    config.set("paths.system_dir", ".thoth_system")
    config.set("paths.cache_dir", "graphql_cache")
    config.set("database.path", "meta.db")
    config.set(
        "content_roots",
        [
            {
                "id": "inbox",
                "mode": "managed_inbox",
                "adapter": "filesystem",
                "path": str(tmp_path / "inbox"),
            },
        ],
    )

    layout = build_path_layout(config, project_root=tmp_path)
    payload = {"native_id": "abc", "text": "hello"}
    captured_at = "2026-08-30T05:00:00Z"
    path1 = write_connector_raw_json(
        layout,
        connector_name="test",
        native_id="abc",
        payload=payload,
        captured_at=captured_at,
    )
    path2 = write_connector_raw_json(
        layout,
        connector_name="test",
        native_id="abc",
        payload=payload,
        captured_at=captured_at,
    )
    assert path1 == path2
    assert path1.exists()


def test_memory_adapter_enforces_modes_and_is_deterministic(
    memory_adapter: InMemoryContentAdapter,
):
    root = ContentRoot(
        root_id="mem",
        mode=ContentRootMode.MANAGED_INBOX,
        adapter_kind="memory",
        base_path=Path("/mem"),
    )
    handle = memory_adapter.write_artifact(root, "a/b.txt", b"hello")
    assert handle.digest == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    assert list(memory_adapter.list_artifacts(root))[0].relpath == "a/b.txt"

    protected = ContentRoot(
        root_id="protected",
        mode=ContentRootMode.PROTECTED,
        adapter_kind="memory",
        base_path=Path("/protected"),
    )
    with pytest.raises(ContentAdapterError, match="refusing to write"):
        memory_adapter.write_artifact(protected, "x.txt", b"x")


def test_content_root_from_config_requires_path(tmp_path: Path):
    with pytest.raises(ContentRootError, match="missing path"):
        content_root_from_config({"id": "x", "mode": "watch_only", "adapter": "filesystem"})


def test_content_roots_from_config_builds_tuple(tmp_path: Path):
    roots = content_roots_from_config(
        [
            {
                "id": "vault",
                "mode": "managed_inbox",
                "adapter": "filesystem",
                "path": str(tmp_path / "vault"),
            }
        ]
    )
    assert len(roots) == 1
    assert roots[0].root_id == "vault"
    assert roots[0].mode == ContentRootMode.MANAGED_INBOX
