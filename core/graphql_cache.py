"""GraphQL cache maintenance helpers shared between CLI and API."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, Any

from .config import config


def maybe_cleanup_graphql_cache(
    processed_tweets: Iterable[Any],
    pipeline_stats: Any,
    *,
    logger: logging.Logger,
) -> None:
    """Remove GraphQL cache files when configured and processing succeeded.

    The helper centralises cache-cleanup logic so the CLI and API remain in sync
    about when it is safe to delete `graphql_cache/tweet_*.json` artefacts.
    """

    if config.get("pipeline.keep_graphql_cache", True):
        return

    if not processed_tweets:
        return

    if pipeline_stats is None:
        return

    failed = getattr(pipeline_stats, "failed_tweets", 0)
    transcript_failures = getattr(pipeline_stats, "youtube_transcript_failures", 0)
    if failed or transcript_failures:
        logger.info(
            "Skipping GraphQL cache cleanup (failed_tweets=%s transcript_failures=%s)",
            failed,
            transcript_failures,
        )
        return

    cache_files = {
        Path(getattr(tweet, "_cache_file"))
        for tweet in processed_tweets
        if getattr(tweet, "_cache_file", None)
    }

    if not cache_files:
        return

    removed = 0
    for cache_path in cache_files:
        try:
            cache_path.unlink()
            removed += 1
            logger.debug("Deleted GraphQL cache file %s", cache_path)
        except FileNotFoundError:
            continue
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warning(
                "Failed to delete GraphQL cache file %s: %s", cache_path, exc
            )

    if removed:
        logger.info(
            "🧹 Deleted %s GraphQL cache file(s) after successful pipeline run", removed
        )
