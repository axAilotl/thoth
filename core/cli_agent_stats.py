"""Structured and human renderers for Thoth stats surfaces."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from core import config, build_path_layout
from core.download_tracker import get_download_tracker
from core.path_layout import resolve_vault_relative_path


def collect_runtime_stats(args: Any) -> dict[str, Any]:
    """Collect runtime stats in a shape that can be rendered as text or JSON."""

    stats: dict[str, Any] = {
        "schema_version": "1.0",
        "tool": "thoth",
        "metadata_db": {"available": False},
        "graphql_cache": {"responses_cached": 0},
        "vault_filesystem": {},
        "vault_db_index": None,
        "media_files": {},
        "downloads": None,
        "source_data": None,
        "bookmark_queue": None,
        "llm_cache": None,
        "transcript_chunk_cache": None,
        "warnings": [],
    }

    db = None
    if config.get("database.enabled", False):
        try:
            from core.metadata_db import get_metadata_db

            db = get_metadata_db()
            stats["metadata_db"] = {"available": True}
        except Exception as exc:
            stats["warnings"].append(f"Metadata DB unavailable: {exc}")

    cache_dir = build_path_layout(config).cache_root
    if cache_dir.exists():
        stats["graphql_cache"]["responses_cached"] = len(
            list(cache_dir.glob("tweet_*.json"))
        )

    vault_dir = build_path_layout(config).vault_root
    if vault_dir.exists():
        tweets_dir = vault_dir / "tweets"
        threads_dir = vault_dir / "threads"
        stats["vault_filesystem"] = {
            "tweet_files": len(list(tweets_dir.glob("*.md")))
            if tweets_dir.exists()
            else 0,
            "thread_files": len(list(threads_dir.glob("*.md")))
            if threads_dir.exists()
            else 0,
        }

    if db:
        stats["vault_db_index"] = db.get_file_stats()

    images_dir = resolve_vault_relative_path(config, "paths.images_dir")
    videos_dir = resolve_vault_relative_path(config, "paths.videos_dir")
    legacy_media_dir = resolve_vault_relative_path(config, "paths.media_dir")
    image_files = len(list(images_dir.glob("*"))) if images_dir.exists() else 0
    video_files = len(list(videos_dir.glob("*"))) if videos_dir.exists() else 0
    legacy_media_files = (
        len(list(legacy_media_dir.glob("*"))) if legacy_media_dir.exists() else 0
    )
    stats["media_files"] = {
        "total": image_files + video_files + legacy_media_files,
        "images": image_files,
        "videos": video_files,
        "legacy_media": legacy_media_files,
    }

    if db:
        stats["downloads"] = {"source": "metadata_db", **(db.get_download_summary() or {})}
    else:
        download_stats = get_download_tracker().get_stats()
        if download_stats["total_tracked"] > 0:
            stats["downloads"] = {"source": "download_tracker", **download_stats}

    bookmarks_file = Path(args.bookmarks)
    if bookmarks_file.exists():
        try:
            with open(bookmarks_file, "r", encoding="utf-8") as f:
                bookmarks = json.load(f)
            stats["source_data"] = {
                "bookmarks_file": args.bookmarks,
                "bookmarks": len(bookmarks),
            }
        except Exception as exc:
            stats["warnings"].append(f"Could not read {args.bookmarks}: {exc}")

    if db:
        stats["bookmark_queue"] = db.get_bookmark_queue_counts()
        stats["llm_cache"] = db.get_llm_cache_stats()
        stats["transcript_chunk_cache"] = db.get_transcript_chunk_stats()

    return stats


def render_runtime_stats(stats: dict[str, Any]) -> None:
    """Render collect_runtime_stats output for humans."""

    print("📊 Thoth Statistics")

    for warning in stats.get("warnings", []):
        print(f"⚠️  {warning}", file=sys.stderr)

    cache_files = stats["graphql_cache"]["responses_cached"]
    if cache_files:
        print(f"📡 GraphQL Cache: {cache_files} responses cached")

    vault_fs = stats.get("vault_filesystem") or {}
    if vault_fs:
        print("📚 Knowledge Vault (filesystem):")
        print(f"   📄 Tweet files: {vault_fs.get('tweet_files', 0)}")
        print(f"   🧵 Thread files: {vault_fs.get('thread_files', 0)}")

    file_stats = stats.get("vault_db_index")
    if file_stats:
        print("📚 Knowledge Vault (DB index):")
        print(f"   Total indexed files: {file_stats.get('total_files', 0):,}")
        print(f"   Total size: {file_stats.get('total_size_mb', 0)} MB")
        by_type = file_stats.get("by_type", {})
        if by_type:
            print("   By type:")
            for file_type, type_stats in by_type.items():
                size_mb = round(
                    (type_stats.get("total_size_bytes", 0)) / (1024 * 1024), 2
                )
                print(
                    f"     {file_type:12} {type_stats.get('count', 0):,} files ({size_mb} MB)"
                )

    media = stats["media_files"]
    print(f"🖼️ Media Files: {media['total']} total")
    if media["images"] > 0:
        print(f"   📸 Images: {media['images']}")
    if media["videos"] > 0:
        print(f"   🎬 Videos: {media['videos']}")
    if media["legacy_media"] > 0:
        print(f"   📁 Legacy media: {media['legacy_media']}")

    downloads = stats.get("downloads") or {}
    if downloads.get("source") == "metadata_db" and downloads.get("total_entries"):
        print("📥 Downloads (DB):")
        print(
            f"   Total entries: {downloads['total_entries']:,} ({downloads['total_mb']} MB)"
        )
        for status, item in downloads.get("by_status", {}).items():
            print(f"   {status:>10}: {item['count']:,} ({item['total_mb']} MB)")
    elif downloads.get("source") == "download_tracker":
        print("📥 Download Tracking:")
        print(f"   ✅ Successful: {downloads['successful']}")
        print(f"   🚫 404 errors: {downloads['404_errors']}")
        print(f"   ❌ Other errors: {downloads['other_errors']}")
        print(f"   ⏳ Pending: {downloads['pending']}")
        print(f"   📊 Total tracked: {downloads['total_tracked']}")

    source_data = stats.get("source_data")
    if source_data:
        print(
            f"📊 Source Data: {source_data['bookmarks']} bookmarks in "
            f"{source_data['bookmarks_file']}"
        )

    queue_counts = stats.get("bookmark_queue")
    if queue_counts:
        print("🗂️  Bookmark Queue:")
        print(f"   Pending: {queue_counts.get('pending', 0)}")
        print(f"   Processing: {queue_counts.get('processing', 0)}")
        print(f"   Processed: {queue_counts.get('processed', 0)}")
        print(f"   Failed: {queue_counts.get('failed', 0)}")

    llm_summary = stats.get("llm_cache")
    if llm_summary and llm_summary.get("total_entries"):
        print("🤖 LLM Cache:")
        print(f"   Total entries: {llm_summary['total_entries']:,}")
        if llm_summary.get("by_task"):
            print("   By task:")
            for task, count in llm_summary["by_task"].items():
                print(f"     {task:15} {count:,}")
        if llm_summary.get("by_provider"):
            print("   By provider:")
            for provider, count in llm_summary["by_provider"].items():
                print(f"     {provider:20} {count:,}")

    chunk_stats = stats.get("transcript_chunk_cache")
    if chunk_stats and chunk_stats.get("total_contexts"):
        print("🎬 Transcript Chunk Cache:")
        print(f"   Contexts tracked: {chunk_stats['total_contexts']:,}")
        print(f"   Contexts with failures: {chunk_stats['contexts_with_failures']:,}")
        print(f"   Contexts with fallback: {chunk_stats['contexts_with_fallback']:,}")
        print(f"   Failed chunks: {chunk_stats['total_failed_chunks']:,}")
        details = chunk_stats.get("context_details", [])
        if details:
            print("   Recent failures:")
            for detail in details[:5]:
                print(
                    f"     {detail['context_id']} -> processed {detail['chunks_processed']}/"
                    f"{detail['chunks_total']} chunks, failures: {detail['failed_count']}"
                    f" (fallback: {'Yes' if detail['fallback'] else 'No'})"
                )
