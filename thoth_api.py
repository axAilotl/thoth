#!/usr/bin/env python3
"""
Thoth API Server - Receives real-time bookmark captures from browser extension
"""

import asyncio
from html import escape
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple, Callable, Mapping
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, HTMLResponse
from pydantic import BaseModel, Field
import uvicorn
import os

# Add current directory to path for imports
import sys

sys.path.insert(0, str(Path(__file__).parent))

from core import (
    ArchivistAdminError,
    ArchivistCompilerError,
    ArchivistRuntimeError,
    Tweet,
    ARCHIVIST_JOB_NAME,
    build_archivist_admin_payload,
    config,
    build_graphql_cache_filename,
    build_path_layout,
    XApiAuthConfigError,
    XApiAuthStateError,
    XApiBookmarkSyncConfigError,
    XApiBookmarkSyncStateError,
    XApiTokenError,
    fetch_current_x_user,
    normalize_bookmark_payload,
    validate_tweet_id,
    clear_pending_x_api_auth,
    complete_x_api_auth,
    clear_archivist_topic_force_request,
    refresh_x_api_tokens,
    resolve_x_api_auth_config,
    resolve_x_api_bookmark_sync_config,
    start_x_api_auth,
    store_x_api_token_bundle,
    summarize_x_api_auth,
    load_x_api_token_bundle,
    ensure_wiki_scaffold,
    run_x_api_bookmark_backfill,
    queue_archivist_topic_force,
    resolve_archivist_sync_config as resolve_archivist_runtime_config,
    run_archivist_topics,
    save_archivist_registry_text,
)
from core.api_server_runtime import resolve_api_server_options
from core.bookmark_ingest import (
    build_bookmark_queue_payload,
    build_realtime_bookmark_record,
    merge_realtime_bookmark_record,
)
from core.ingestion_runtime import get_knowledge_artifact_runtime
from core.metadata_db import get_metadata_db, BookmarkQueueEntry
from core.non_live_state import (
    get_non_live_next_run_at,
    mark_non_live_run_finished,
    mark_non_live_run_started,
    validate_non_live_interval_hours,
)
from core.settings_summary import build_settings_runtime_summary
from core.llm_interface import LLMInterface
from core.x_api_monitoring import (
    X_API_MONITOR_SECRET_HEADER,
    XApiMonitoringConfigError,
    XApiMonitoringError,
    process_x_api_monitoring_webhook,
    verify_x_api_monitoring_webhook_secret,
)
from processors.pipeline_processor import PipelineProcessor
from processors.cache_loader import CacheLoader
from processors.github_stars_processor import GitHubStarsProcessor
from processors.huggingface_likes_processor import HuggingFaceLikesProcessor
from core.graphql_cache import maybe_cleanup_graphql_cache

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="Thoth API", version="1.0.0")

# Mount static files for settings UI
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Shared pipeline instance protected by an asyncio lock to avoid overlapping runs
pipeline_runner = PipelineProcessor()
pipeline_lock = asyncio.Lock()
github_trigger_lock = asyncio.Lock()
huggingface_trigger_lock = asyncio.Lock()
x_api_trigger_lock = asyncio.Lock()
archivist_trigger_lock = asyncio.Lock()

RUNTIME_CONFIG_PATH = Path(__file__).parent / "config.json"
CONTROL_CONFIG_PATH = Path(__file__).parent / "control.json"
EXAMPLE_CONFIG_PATH = Path(__file__).parent / "config.example.json"
SOCIAL_SYNC_JOB_NAME = "social_sync"
X_API_SYNC_JOB_NAME = "x_api_sync"


def deep_merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge two dictionaries without mutating the inputs."""
    result = dict(base)
    for key, value in override.items():
        if isinstance(result.get(key), dict) and isinstance(value, dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    return result


def load_json_file(path: Path, *, required: bool = False) -> Dict[str, Any]:
    """Load a JSON file, optionally failing closed when it is required."""
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Required config file not found: {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_runtime_settings() -> Dict[str, Any]:
    """Return live runtime settings overlaid by operator control settings."""
    runtime = load_json_file(RUNTIME_CONFIG_PATH, required=True)
    control = load_json_file(CONTROL_CONFIG_PATH)
    return deep_merge_dicts(runtime, control)


def write_control_updates(updates: Dict[str, Any]) -> Dict[str, Any]:
    """Persist operator overrides into control.json and reload runtime config."""
    control_data = load_json_file(CONTROL_CONFIG_PATH)
    merged = deep_merge_dicts(control_data, updates)
    with open(CONTROL_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2)
    config.reload([str(RUNTIME_CONFIG_PATH), str(CONTROL_CONFIG_PATH)])
    return merged


def normalize_x_api_user_payload(user_payload: Any) -> Optional[Dict[str, Any]]:
    """Flatten X user responses so the UI can consume a stable shape."""
    if user_payload is None:
        return None
    if not isinstance(user_payload, Mapping):
        raise ValueError("X API user payload must be an object")

    nested = user_payload.get("data")
    if nested is None:
        return dict(user_payload)
    if not isinstance(nested, Mapping):
        raise ValueError("X API user payload.data must be an object")
    return dict(nested)


def build_x_api_auth_response(layout, payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Attach stable top-level auth metadata for API and UI consumers."""
    response_payload: Dict[str, Any] = {
        "status": "ok",
        "auth_root": str(layout.auth_root),
        **payload,
    }

    raw_user = payload.get("user")
    token_bundle = payload.get("token_bundle")
    if raw_user is None and isinstance(token_bundle, Mapping):
        raw_user = token_bundle.get("user")
    if raw_user is not None:
        response_payload["user"] = normalize_x_api_user_payload(raw_user)
    return response_payload


def request_prefers_html(request: Request) -> bool:
    """Return True when the caller expects a browser-friendly HTML response."""
    accept = request.headers.get("accept", "")
    return "text/html" in accept or "application/xhtml+xml" in accept


def render_x_api_auth_callback_page(
    *,
    success: bool,
    title: str,
    detail: str,
    user: Optional[Mapping[str, Any]] = None,
    status_code: int = 200,
) -> HTMLResponse:
    """Render a callback page that can return control to the settings UI."""
    username = ""
    if user:
        username = str(user.get("username") or "").strip()

    callback_payload: Dict[str, Any] = {
        "status": "ok" if success else "error",
        "detail": detail,
    }
    if user:
        callback_payload["user"] = dict(user)

    payload_json = json.dumps(callback_payload)
    title_text = escape(title)
    detail_text = escape(detail)
    success_label = "success" if success else "error"
    fallback_copy = "Return to settings"

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title_text}</title>
  <style>
    :root {{
      color-scheme: light;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: #f3f4f6;
      color: #111827;
    }}
    main {{
      width: min(32rem, calc(100vw - 2rem));
      padding: 2rem;
      border-radius: 1rem;
      background: #ffffff;
      box-shadow: 0 16px 48px rgba(17, 24, 39, 0.12);
    }}
    h1 {{
      margin: 0 0 0.75rem;
      font-size: 1.5rem;
    }}
    p {{
      margin: 0;
      line-height: 1.5;
      color: #374151;
    }}
    a {{
      color: #1d4ed8;
      font-weight: 600;
    }}
    .status {{
      display: inline-block;
      margin-bottom: 1rem;
      padding: 0.3rem 0.6rem;
      border-radius: 999px;
      font-size: 0.85rem;
      font-weight: 700;
      text-transform: uppercase;
      background: {"#dcfce7" if success else "#fee2e2"};
      color: {"#166534" if success else "#991b1b"};
    }}
  </style>
</head>
<body>
  <main>
    <div class="status">{escape(success_label)}</div>
    <h1>{title_text}</h1>
    <p>{detail_text}</p>
    <p style="margin-top: 1rem;"><a id="settings-link" href="/settings">{escape(fallback_copy)}</a></p>
  </main>
  <script>
    const payload = {payload_json};
    const settingsUrl = new URL('/settings', window.location.origin);
    settingsUrl.searchParams.set('x_api_auth', payload.status);
    if (payload.user && payload.user.username) {{
      settingsUrl.searchParams.set('x_user', payload.user.username);
    }}
    if (payload.status !== 'ok' && payload.detail) {{
      settingsUrl.searchParams.set('x_error', payload.detail);
    }}
    document.getElementById('settings-link').href = settingsUrl.toString();

    function handoffToSettings() {{
      if (window.opener && !window.opener.closed) {{
        window.opener.postMessage({{ type: 'thoth:x-api-auth-complete', payload }}, window.location.origin);
        window.close();
        window.setTimeout(() => {{
          if (!window.closed) {{
            window.location.replace(settingsUrl.toString());
          }}
        }}, 250);
        return;
      }}
      window.location.replace(settingsUrl.toString());
    }}

    window.setTimeout(handoffToSettings, 400);
  </script>
</body>
</html>
"""
    return HTMLResponse(content=html, status_code=status_code)


async def run_pipeline_for_tweets(
    tweets: List[Tweet],
    url_mappings: Optional[Dict[str, str]] = None,
    resume: bool = True,
    rerun_llm: bool = False,
    llm_only: bool = False,
):
    """Execute the unified pipeline for a set of tweets."""
    if not tweets:
        return None

    batch_size = max(1, len(tweets))

    async with pipeline_lock:
        return await pipeline_runner.process_tweets_pipeline(
            tweets,
            url_mappings=url_mappings,
            resume=resume,
            batch_size=batch_size,
            rerun_llm=rerun_llm,
            llm_only=llm_only,
        )


def upsert_bookmark_queue_entry(bookmark_data: Dict[str, Any]):
    """Persist bookmark metadata in the durable queue."""
    bookmark_data = build_bookmark_queue_payload(bookmark_data)
    tweet_id = bookmark_data["tweet_id"]

    captured_at = bookmark_data.get("timestamp") or datetime.now().isoformat()
    entry = BookmarkQueueEntry(
        tweet_id=tweet_id,
        source=bookmark_data.get("source"),
        captured_at=captured_at,
        status="pending",
        payload_json=json.dumps(bookmark_data, ensure_ascii=False),
        next_attempt_at=captured_at,
    )
    db = get_metadata_db()
    if not db.upsert_bookmark_entry(entry):
        raise RuntimeError(f"Failed to persist bookmark queue entry {tweet_id}")


async def enqueue_bookmark_payload(
    bookmark_data: Dict[str, Any], delay_seconds: float = 0.0
):
    """Schedule a bookmark for processing via the async queue."""
    bookmark_data = normalize_bookmark_payload(bookmark_data)
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)

    tweet_id = bookmark_data["tweet_id"]

    await PROCESSING_QUEUE.put(bookmark_data)
    logger.debug(f"Enqueued bookmark {tweet_id} for processing")


async def ingest_bookmark_capture(
    bookmark_data: Mapping[str, Any],
    *,
    graphql_response: Optional[Mapping[str, Any]] = None,
    process_immediately: bool = False,
    queue_bookmark: bool = True,
    reset_processed: bool = True,
    force: Optional[bool] = None,
) -> Dict[str, Any]:
    """Normalize and persist a bookmark capture through the shared ingest path."""
    graphql_cache_file = bookmark_data.get("graphql_cache_file")
    queue_payload = build_bookmark_queue_payload(
        bookmark_data,
        graphql_cache_file=graphql_cache_file,
    )

    if graphql_response is not None:
        if not isinstance(graphql_response, Mapping):
            raise ValueError("bookmark graphql_response must be an object")
        queue_payload["graphql_cache_file"] = save_graphql_to_cache(
            queue_payload["tweet_id"], dict(graphql_response)
        )

    if force is not None:
        queue_payload["force"] = force

    record = build_realtime_bookmark_record(queue_payload)

    if queue_bookmark:
        upsert_bookmark_queue_entry(queue_payload)

    await mutate_realtime_bookmarks(
        lambda bookmarks: (
            merge_realtime_bookmark_record(
                bookmarks,
                record,
                reset_processed=reset_processed and queue_bookmark,
            ),
            None,
        )
    )

    if queue_bookmark:
        if process_immediately:
            await process_bookmark_async(queue_payload)
        else:
            await enqueue_bookmark_payload(queue_payload)

    return queue_payload


def bookmark_entry_to_payload(
    entry: Optional[BookmarkQueueEntry],
) -> Optional[Dict[str, Any]]:
    """Convert a stored bookmark entry back into a payload dictionary."""
    if not entry:
        return None

    try:
        payload = json.loads(entry.payload_json) if entry.payload_json else {}
    except json.JSONDecodeError as exc:
        logger.error(
            f"Failed to deserialize bookmark payload for {entry.tweet_id}: {exc}"
        )
        payload = {}

    payload.setdefault("tweet_id", entry.tweet_id)
    if entry.source and not payload.get("source"):
        payload["source"] = entry.source
    if entry.captured_at and not payload.get("timestamp"):
        payload["timestamp"] = entry.captured_at

    return payload


async def schedule_retry(tweet_id: str, next_attempt_iso: Optional[str]):
    """Schedule a retry for a bookmark when the next attempt is due."""
    if not next_attempt_iso:
        return

    try:
        next_attempt = datetime.fromisoformat(next_attempt_iso)
    except ValueError:
        logger.debug(f"Invalid next_attempt_at for {tweet_id}: {next_attempt_iso}")
        return

    delay_seconds = max(0.0, (next_attempt - datetime.now()).total_seconds())
    await asyncio.sleep(delay_seconds)

    db = get_metadata_db()
    entry = db.get_bookmark_entry(tweet_id)
    if not entry or entry.status != "pending":
        return

    payload = bookmark_entry_to_payload(entry)
    if payload:
        await PROCESSING_QUEUE.put(payload)
        logger.info(f"Re-queued bookmark {tweet_id} after failure")


def serialize_bookmark_entry(entry: BookmarkQueueEntry) -> Dict[str, Any]:
    """Serialize a bookmark queue entry for API responses."""
    return {
        "tweet_id": entry.tweet_id,
        "source": entry.source,
        "captured_at": entry.captured_at,
        "status": entry.status,
        "attempts": entry.attempts,
        "last_error": entry.last_error,
        "last_attempt_at": entry.last_attempt_at,
        "processed_at": entry.processed_at,
        "next_attempt_at": entry.next_attempt_at,
        "processed_with_graphql": entry.processed_with_graphql,
    }


def serialize_processing_stats(stats) -> Dict[str, Any]:
    """Convert a ProcessingStats-like object into a response dictionary."""
    return {
        "created": getattr(stats, "created", 0),
        "updated": getattr(stats, "updated", 0),
        "skipped": getattr(stats, "skipped", 0),
        "errors": getattr(stats, "errors", 0),
        "total_processed": getattr(stats, "total_processed", 0),
    }


def resolve_social_sync_config() -> Dict[str, Any]:
    """Return normalized social sync settings from runtime config."""
    github_config = config.get("sources.github", {}) or {}
    hf_config = config.get("sources.huggingface", {}) or {}
    social_sync = config.get("automation.social_sync", {}) or {}
    if not isinstance(social_sync, dict):
        raise ValueError("automation.social_sync must be an object")

    return {
        "enabled": social_sync.get("enabled", False),
        "interval_hours": validate_non_live_interval_hours(
            social_sync.get("interval_hours", 8),
            field_name="automation.social_sync.interval_hours",
        ),
        "run_on_startup": bool(social_sync.get("run_on_startup", False)),
        "github": {
            "enabled": github_config.get("enabled", True),
            "limit": github_config.get("limit", 50),
            "resume": github_config.get("resume", True),
        },
        "huggingface": {
            "enabled": hf_config.get("enabled", True),
            "limit": hf_config.get("limit", 50),
            "resume": hf_config.get("resume", True),
            "include_models": hf_config.get("include_models", True),
            "include_datasets": hf_config.get("include_datasets", True),
            "include_spaces": hf_config.get("include_spaces", True),
        },
    }


def resolve_x_api_sync_config() -> Dict[str, Any]:
    """Return normalized X bookmark backfill settings from runtime config."""
    automation_config = config.get("automation.x_api_sync", {}) or {}
    if not isinstance(automation_config, dict):
        raise XApiBookmarkSyncConfigError("automation.x_api_sync must be an object")

    resolved = resolve_x_api_bookmark_sync_config(config)
    return {
        "enabled": resolved.enabled,
        "interval_hours": resolved.interval_hours,
        "run_on_startup": resolved.run_on_startup,
        "max_results": resolved.max_results,
        "max_pages": resolved.max_pages,
        "resume_from_checkpoint": resolved.resume_from_checkpoint,
    }


def resolve_archivist_sync_config() -> Dict[str, Any]:
    """Return normalized archivist automation settings from runtime config."""
    return resolve_archivist_runtime_config(config)


async def run_github_stars_sync(limit: Optional[int] = None, resume: bool = True):
    """Run the GitHub stars processor with shared locking."""
    async with github_trigger_lock:
        processor = GitHubStarsProcessor()
        return await processor.fetch_and_process_starred_repos(limit=limit, resume=resume)


async def run_huggingface_likes_sync(
    limit: Optional[int] = None,
    resume: bool = True,
    include_models: bool = True,
    include_datasets: bool = True,
    include_spaces: bool = True,
):
    """Run the HuggingFace likes processor with shared locking."""
    async with huggingface_trigger_lock:
        processor = HuggingFaceLikesProcessor()
        return await processor.fetch_and_process_liked_repos(
            limit=limit,
            resume=resume,
            include_models=include_models,
            include_datasets=include_datasets,
            include_spaces=include_spaces,
        )


async def run_x_api_bookmark_sync(
    max_results: Optional[int] = None,
    max_pages: Optional[int] = None,
    resume_from_checkpoint: Optional[bool] = None,
    process_immediately: bool = False,
):
    """Run the X bookmark backfill and hand emitted payloads to the queue."""
    async with x_api_trigger_lock:
        sync_config = resolve_x_api_sync_config()
        resolved_layout = build_path_layout(config)
        sync_result = await run_x_api_bookmark_backfill(
            config,
            layout=resolved_layout,
            max_results=(
                sync_config["max_results"] if max_results is None else max_results
            ),
            max_pages=max_pages if max_pages is not None else sync_config["max_pages"],
            resume_from_checkpoint=(
                sync_config["resume_from_checkpoint"]
                if resume_from_checkpoint is None
                else resume_from_checkpoint
            ),
        )

        payloads = sync_result.get("payloads", [])
        queued = 0
        processed = 0
        for payload in payloads:
            await ingest_bookmark_capture(
                payload,
                process_immediately=process_immediately,
                queue_bookmark=True,
                reset_processed=True,
            )
            queued += 1
            if process_immediately:
                processed += 1

        sync_result.update(
            {
                "queued": queued,
                "processed_immediately": processed,
                "sync_config": sync_config,
            }
        )
        return sync_result


async def run_archivist_compilation(
    *,
    topic_ids: Optional[List[str]] = None,
    force: bool = False,
    dry_run: bool = False,
    limit: Optional[int] = None,
):
    """Run archivist topic compilation with shared locking."""
    async with archivist_trigger_lock:
        return await run_archivist_topics(
            load_runtime_settings(),
            project_root=RUNTIME_CONFIG_PATH.parent,
            topic_ids=topic_ids,
            force=force,
            dry_run=dry_run,
            limit=limit,
        )


async def load_pending_bookmarks_from_db():
    """Load pending bookmarks from the durable queue into memory on startup."""
    db = get_metadata_db()
    entries = db.get_unprocessed_bookmarks()
    now = datetime.now()

    for entry in entries:
        payload = bookmark_entry_to_payload(entry)
        if not payload:
            continue

        delay = 0.0
        if entry.status == "pending" and entry.next_attempt_at:
            try:
                next_attempt = datetime.fromisoformat(entry.next_attempt_at)
                delay = max(0.0, (next_attempt - now).total_seconds())
            except ValueError:
                delay = 0.0

        if entry.status == "failed":
            # Do not auto enqueue permanently failed items
            continue

        asyncio.create_task(enqueue_bookmark_payload(payload, delay))


# Configure CORS for browser extension
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://twitter.com", "https://x.com", "chrome-extension://*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Data models
class BookmarkCapture(BaseModel):
    """Bookmark data from browser extension"""

    tweet_id: str
    tweet_data: Optional[Dict[str, Any]] = None
    graphql_response: Optional[Dict[str, Any]] = None
    timestamp: Optional[str] = None
    source: Optional[str] = "browser_extension"
    force: Optional[bool] = False


class ProcessingStatus(BaseModel):
    """Processing status response"""

    status: str
    message: str
    tweet_id: Optional[str] = None
    processed_at: Optional[str] = None


class GitHubTriggerRequest(BaseModel):
    """Request payload for triggering the GitHub stars pipeline."""

    limit: Optional[int] = None
    resume: bool = True


class HuggingFaceTriggerRequest(BaseModel):
    """Request payload for triggering the HuggingFace likes pipeline."""

    limit: Optional[int] = None
    resume: bool = True
    include_models: bool = True
    include_datasets: bool = True
    include_spaces: bool = True


class XApiBackfillRequest(BaseModel):
    """Request payload for triggering X bookmark backfill."""

    max_results: Optional[int] = Field(default=None, gt=0, le=100)
    max_pages: Optional[int] = Field(default=None, gt=0)
    resume_from_checkpoint: bool = True


class ArchivistRegistryUpdateRequest(BaseModel):
    """Request payload for updating the archivist topic registry."""

    content: str = Field(default="")


class ArchivistForceRequest(BaseModel):
    """Request payload for queueing an archivist force run."""

    reason: Optional[str] = None


class ArchivistRunRequest(BaseModel):
    """Request payload for executing archivist compilation."""

    force: bool = False
    dry_run: bool = False
    limit: Optional[int] = Field(default=None, gt=0)


class BookmarkStatusRequest(BaseModel):
    """Request body for bookmark status lookups."""

    tweet_ids: List[str]


class DigestRequest(BaseModel):
    """Request payload for generating digests."""

    digest_type: str = "all"  # weekly, inbox, dashboard, all
    week: Optional[str] = None  # Format: YYYY-WNN (e.g., 2024-W52)
    notify: bool = False


# Storage
def get_realtime_bookmarks_file():
    """Get the path to the realtime bookmarks file under the system root."""
    return build_path_layout(config).realtime_bookmarks_file


def save_graphql_to_cache(tweet_id: str, graphql_response: dict) -> str:
    """Save GraphQL response to cache and return filename.

    Consolidates the repeated GraphQL caching logic into one place.
    """
    normalized_tweet_id = validate_tweet_id(tweet_id)
    cache_filename = build_graphql_cache_filename(normalized_tweet_id)
    cache_path = build_path_layout(config).cache_root / cache_filename
    cache_path.parent.mkdir(exist_ok=True)

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(graphql_response, f, indent=2, ensure_ascii=False)

    logger.info(f"Cached GraphQL response for {tweet_id}")
    return cache_filename


REALTIME_BOOKMARKS_FILE = get_realtime_bookmarks_file()
PROCESSING_QUEUE = asyncio.Queue()
BOOKMARKS_FILE_LOCK = asyncio.Lock()


def load_realtime_bookmarks() -> list:
    """Load existing realtime bookmarks"""
    if REALTIME_BOOKMARKS_FILE.exists():
        try:
            with open(REALTIME_BOOKMARKS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse realtime bookmarks JSON: {e}")
            return []
        except IOError as e:
            logger.error(f"Failed to read realtime bookmarks file: {e}")
            return []
    return []


async def mutate_realtime_bookmarks(
    mutator: Callable[[List[dict]], Tuple[bool, Any]],
) -> Any:
    """Apply a mutation to realtime bookmarks under an async lock."""

    async with BOOKMARKS_FILE_LOCK:
        bookmarks = load_realtime_bookmarks()
        dirty, result = mutator(bookmarks)
        if dirty:
            with open(REALTIME_BOOKMARKS_FILE, "w", encoding="utf-8") as f:
                json.dump(bookmarks, f, indent=2, ensure_ascii=False)
        return result


def save_bookmark(bookmark_data: dict) -> Tuple[bool, dict]:
    """Save bookmark to local storage and return (is_new, stored_data)."""
    bookmark_data = normalize_bookmark_payload(bookmark_data)
    bookmarks = load_realtime_bookmarks()

    existing = None
    for entry in bookmarks:
        if entry.get("tweet_id") == bookmark_data["tweet_id"]:
            existing = entry
            break

    if existing is None:
        # If we have a graphql_response, save it to cache and store only the filename
        if bookmark_data.get("graphql_response"):
            tweet_id = bookmark_data["tweet_id"]
            cache_filename = build_graphql_cache_filename(tweet_id)
            cache_path = build_path_layout(config).cache_root / cache_filename

            # Ensure cache directory exists
            cache_path.parent.mkdir(parents=True, exist_ok=True)

            # Save the GraphQL response to file
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(
                    bookmark_data["graphql_response"], f, indent=2, ensure_ascii=False
                )

            # Replace the full response with just the filename reference
            bookmark_data_to_save = bookmark_data.copy()
            bookmark_data_to_save["graphql_cache_file"] = str(cache_filename)
            bookmark_data_to_save.pop("graphql_response", None)
            bookmark_data_to_save.pop("force", None)

            logger.info(f"Cached GraphQL response for {tweet_id}")
        else:
            bookmark_data_to_save = bookmark_data.copy()
            bookmark_data_to_save.pop("force", None)

        bookmarks.append(bookmark_data_to_save)

        with open(REALTIME_BOOKMARKS_FILE, "w", encoding="utf-8") as f:
            json.dump(bookmarks, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved bookmark for tweet {bookmark_data['tweet_id']}")
        return True, bookmark_data_to_save

    # Existing entry: optionally update cached GraphQL and metadata
    updated = False
    existing_payload = dict(existing)

    if bookmark_data.get("graphql_response"):
        tweet_id = bookmark_data["tweet_id"]
        cache_filename = build_graphql_cache_filename(tweet_id)
        cache_path = build_path_layout(config).cache_root / cache_filename

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(
                bookmark_data["graphql_response"], f, indent=2, ensure_ascii=False
            )

        existing["graphql_cache_file"] = str(cache_filename)
        existing_payload["graphql_cache_file"] = str(cache_filename)
        updated = True

    if bookmark_data.get("tweet_data"):
        existing["tweet_data"] = bookmark_data["tweet_data"]
        updated = True

    if updated:
        with open(REALTIME_BOOKMARKS_FILE, "w", encoding="utf-8") as f:
            json.dump(bookmarks, f, indent=2, ensure_ascii=False)

    existing_payload.pop("graphql_response", None)
    return False, existing_payload


async def process_bookmark_async(bookmark_data: dict):
    """Process a bookmark asynchronously through the shared tweet runtime."""
    tweet_id = None
    db = get_metadata_db()
    runtime = get_knowledge_artifact_runtime(config, layout=build_path_layout(config), db=db)
    try:
        tweet_id = validate_tweet_id(bookmark_data.get("tweet_id"))
        logger.info(f"Processing bookmark {tweet_id}")

        db.mark_bookmark_processing(tweet_id)

        # Ensure cache reference is indexed in the metadata DB when available
        cache_filename = bookmark_data.get("graphql_cache_file")
        if cache_filename and config.get("database.enabled", False):
            try:
                cache_file = build_path_layout(config).cache_root / cache_filename
                db.upsert_graphql_cache_entry(tweet_id, str(cache_file))
            except Exception as exc:
                logger.debug(f"Failed to index graphql cache for {tweet_id}: {exc}")
        bookmark_result = await runtime.process_bookmark_payload(
            bookmark_data,
            # Live captures should fill missing artifacts, not blow through cache gates.
            resume=True,
        )
        pipeline_stats = bookmark_result.pipeline_result
        if pipeline_stats:
            logger.info(
                "Pipeline processed %s/%s tweets for bookmark %s",
                pipeline_stats.processed_tweets,
                bookmark_result.tweet_count,
                tweet_id,
            )

        # Persist processed state
        has_graphql = bool(
            bookmark_data.get("graphql_cache_file")
            or bookmark_data.get("graphql_response")
        )
        db.mark_bookmark_processed(tweet_id, with_graphql=has_graphql)

        try:

            def mark_processed(bookmarks: List[dict]) -> Tuple[bool, None]:
                for entry in bookmarks:
                    if entry.get("tweet_id") == tweet_id:
                        if entry.get("processed") is not True:
                            entry["processed"] = True
                            return True, None
                        return False, None
                return False, None

            await mutate_realtime_bookmarks(mark_processed)
        except Exception as exc:
            logger.debug(f"Failed to update processed flag for {tweet_id}: {exc}")

    except ValueError as exc:
        logger.error(f"Invalid bookmark payload: {exc}")
        return
    except Exception as exc:
        logger.error(f"Error processing bookmark {tweet_id}: {exc}")
        if tweet_id:
            failure_entry = db.mark_bookmark_failed(tweet_id, str(exc))
            if (
                failure_entry
                and failure_entry.status == "pending"
                and failure_entry.next_attempt_at
            ):
                asyncio.create_task(schedule_retry(tweet_id, failure_entry.next_attempt_at))
    else:
        logger.info(f"Bookmark {tweet_id} processed successfully")


# API Endpoints


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "Thoth API"}


@app.get("/settings")
async def settings_page():
    """Serve the settings UI page"""
    settings_file = Path(__file__).parent / "static" / "settings.html"
    if settings_file.exists():
        return FileResponse(settings_file)
    raise HTTPException(status_code=404, detail="Settings page not found")


@app.get("/api/settings")
async def get_settings():
    """Get current configuration (API keys are masked)"""
    try:
        config_data = load_runtime_settings()
        runtime_summary = build_settings_runtime_summary(
            config_data,
            project_root=RUNTIME_CONFIG_PATH.parent,
        )

        # Check which env vars are set (masked)
        env_status = {
            'OPENAI_API_KEY': bool(os.getenv('OPENAI_API_KEY')),
            'ANTHROPIC_API': bool(os.getenv('ANTHROPIC_API')),
            'OPEN_ROUTER_API_KEY': bool(os.getenv('OPEN_ROUTER_API_KEY')),
            'YOUTUBE_API_KEY': bool(os.getenv('YOUTUBE_API_KEY')),
            'DEEPGRAM_API_KEY': bool(os.getenv('DEEPGRAM_API_KEY')),
            'GITHUB_API': bool(os.getenv('GITHUB_API')),
            'X_API_CLIENT_SECRET': bool(os.getenv('X_API_CLIENT_SECRET')),
            'THOTH_X_MONITOR_WEBHOOK_SECRET': bool(os.getenv('THOTH_X_MONITOR_WEBHOOK_SECRET')),
            'HF_TOKEN': bool(os.getenv('HF_TOKEN') or os.getenv('HUGGINGFACEHUB_API_TOKEN')),
            'HF_USER': os.getenv('HF_USER', '')
        }

        return {
            **config_data,
            'runtime': runtime_summary,
            'env': env_status,
            'config_files': {
                'runtime': str(RUNTIME_CONFIG_PATH),
                'control': str(CONTROL_CONFIG_PATH),
                'example_template': str(EXAMPLE_CONFIG_PATH),
            }
        }
    except Exception as e:
        logger.error(f"Error loading settings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/settings/config")
async def update_config(updates: Dict[str, Any]):
    """Update control.json with new settings (deep merge)."""
    try:
        write_control_updates(updates)
        logger.info("Configuration updated via settings UI")
        return {
            "status": "ok",
            "message": "Configuration updated",
            "control_file": str(CONTROL_CONFIG_PATH),
        }

    except Exception as e:
        logger.error(f"Error updating config: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/settings/env")
async def update_env_vars(env_updates: Dict[str, str]):
    """Update .env file with new API keys"""
    try:
        env_path = Path(__file__).parent / ".env"

        # Load existing .env content
        existing_vars = {}
        if env_path.exists():
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        existing_vars[key.strip()] = value.strip()

        # Update with new values (only if provided and non-empty)
        for key, value in env_updates.items():
            if value and value.strip():
                existing_vars[key] = value.strip()
                # Also update current environment
                os.environ[key] = value.strip()

        # Write back to .env
        with open(env_path, 'w') as f:
            f.write("# Thoth API Keys\n")
            f.write("# Updated by settings UI\n\n")
            for key, value in existing_vars.items():
                # Quote values that might have special characters
                if ' ' in value or '"' in value:
                    f.write(f'{key}="{value}"\n')
                else:
                    f.write(f'{key}={value}\n')

        logger.info("Environment variables updated via settings UI")
        return {"status": "ok", "message": "API keys updated"}

    except Exception as e:
        logger.error(f"Error updating env vars: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/archivist/registry")
async def get_archivist_registry():
    """Return the raw and parsed archivist registry for the settings UI."""
    try:
        return build_archivist_admin_payload(
            load_runtime_settings(),
            project_root=RUNTIME_CONFIG_PATH.parent,
        )
    except Exception as e:
        logger.error(f"Error loading archivist registry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/archivist/registry")
async def update_archivist_registry(payload: ArchivistRegistryUpdateRequest):
    """Validate and persist the archivist topic registry."""
    try:
        result = save_archivist_registry_text(
            load_runtime_settings(),
            project_root=RUNTIME_CONFIG_PATH.parent,
            content=payload.content,
        )
        return {"status": "ok", **result}
    except (ArchivistAdminError, ValueError) as e:
        logger.error(f"Error saving archivist registry: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error saving archivist registry: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/archivist/topics/{topic_id}/force")
async def force_archivist_topic(topic_id: str, payload: ArchivistForceRequest):
    """Queue a manual force request for an archivist topic."""
    try:
        return queue_archivist_topic_force(
            load_runtime_settings(),
            project_root=RUNTIME_CONFIG_PATH.parent,
            topic_id=topic_id,
            reason=payload.reason,
        )
    except (ArchivistAdminError, ValueError) as e:
        logger.error(f"Error queueing archivist force for {topic_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error queueing archivist force for {topic_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/archivist/run")
async def run_archivist_now(payload: ArchivistRunRequest):
    """Execute the archivist compiler immediately for due or selected topics."""
    try:
        return await run_archivist_compilation(
            force=payload.force,
            dry_run=payload.dry_run,
            limit=payload.limit,
        )
    except (ArchivistRuntimeError, ArchivistCompilerError, ValueError) as e:
        logger.error(f"Error running archivist compiler: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error running archivist compiler: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/archivist/topics/{topic_id}/run")
async def run_archivist_topic_now(topic_id: str, payload: ArchivistRunRequest):
    """Execute the archivist compiler immediately for a single topic."""
    try:
        return await run_archivist_compilation(
            topic_ids=[topic_id],
            force=payload.force,
            dry_run=payload.dry_run,
            limit=1,
        )
    except (ArchivistRuntimeError, ArchivistCompilerError, ValueError) as e:
        logger.error(f"Error running archivist topic {topic_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error running archivist topic {topic_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/archivist/topics/{topic_id}/force")
async def clear_archivist_topic_force_endpoint(topic_id: str):
    """Clear a queued manual force request for an archivist topic."""
    try:
        return clear_archivist_topic_force_request(
            load_runtime_settings(),
            project_root=RUNTIME_CONFIG_PATH.parent,
            topic_id=topic_id,
        )
    except (ArchivistAdminError, ValueError) as e:
        logger.error(f"Error clearing archivist force for {topic_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error clearing archivist force for {topic_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/x-api/auth/status")
async def get_x_api_auth_status():
    """Return the current X API auth state."""
    try:
        layout = build_path_layout(config)
        status = summarize_x_api_auth(layout)
        return {
            "enabled": bool((config.get("sources.x_api", {}) or {}).get("enabled", False)),
            "auth_root": str(layout.auth_root),
            **(
                {
                    **status,
                    "user": normalize_x_api_user_payload(status.get("user")),
                }
                if status.get("user") is not None
                else status
            ),
        }
    except Exception as exc:
        logger.error(f"Error loading X API auth status: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/x-api/auth/start")
async def start_x_api_auth_flow():
    """Create a PKCE authorization request for X API auth."""
    try:
        layout = build_path_layout(config)
        payload = start_x_api_auth(config, layout=layout)
        return {
            "status": "ok",
            "auth_root": str(layout.auth_root),
            **payload,
        }
    except (XApiAuthConfigError, XApiTokenError, ValueError) as exc:
        logger.error(f"Error starting X API auth: {exc}")
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error(f"Unexpected error starting X API auth: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/x-api/auth/callback")
async def x_api_auth_callback(
    request: Request,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None,
):
    """Handle the X OAuth callback."""
    layout = build_path_layout(config)
    prefers_html = request_prefers_html(request)
    try:
        if error:
            clear_pending_x_api_auth(layout)
            detail = error_description or error
            raise HTTPException(status_code=400, detail=detail)
        if not code or not code.strip():
            raise HTTPException(status_code=400, detail="Missing OAuth code")
        if not state or not state.strip():
            raise HTTPException(status_code=400, detail="Missing OAuth state")

        payload = await complete_x_api_auth(
            config,
            code=code.strip(),
            state=state.strip(),
            layout=layout,
        )
        response_payload = build_x_api_auth_response(layout, payload)
        if prefers_html:
            user = response_payload.get("user")
            username = str((user or {}).get("username") or "").strip()
            detail = (
                f"Connected @{username}. This window can close now."
                if username
                else "Connected your X account. This window can close now."
            )
            return render_x_api_auth_callback_page(
                success=True,
                title="X account connected",
                detail=detail,
                user=user,
            )
        return response_payload
    except HTTPException as exc:
        if prefers_html:
            return render_x_api_auth_callback_page(
                success=False,
                title="X account connection failed",
                detail=str(exc.detail),
                status_code=exc.status_code,
            )
        raise
    except (XApiAuthConfigError, XApiAuthStateError, XApiTokenError, ValueError) as exc:
        logger.error(f"X API auth callback failed: {exc}")
        if prefers_html:
            return render_x_api_auth_callback_page(
                success=False,
                title="X account connection failed",
                detail=str(exc),
                status_code=400,
            )
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error(f"Unexpected X API auth callback error: {exc}")
        if prefers_html:
            return render_x_api_auth_callback_page(
                success=False,
                title="X account connection failed",
                detail="Unexpected error completing X authorization",
                status_code=500,
            )
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/x-api/auth/refresh")
async def refresh_x_api_auth():
    """Refresh the stored X API tokens."""
    try:
        layout = build_path_layout(config)
        auth_config = resolve_x_api_auth_config(config)
        bundle = load_x_api_token_bundle(layout)
        if not bundle:
            raise HTTPException(
                status_code=404, detail="No stored X API token bundle was found"
            )
        refresh_token = str(bundle.get("refresh_token") or "").strip()
        if not refresh_token:
            raise HTTPException(
                status_code=400, detail="Stored X API token bundle is missing a refresh token"
            )

        refreshed_bundle = await refresh_x_api_tokens(
            auth_config, refresh_token=refresh_token
        )
        refreshed_payload = refreshed_bundle.to_dict()
        refreshed_payload["user"] = await fetch_current_x_user(
            auth_config, access_token=refreshed_bundle.access_token
        )
        stored = store_x_api_token_bundle(layout, refreshed_payload)
        return build_x_api_auth_response(layout, {"token_bundle": stored})
    except HTTPException:
        raise
    except (XApiAuthConfigError, XApiTokenError, ValueError) as exc:
        logger.error(f"X API token refresh failed: {exc}")
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error(f"Unexpected X API token refresh error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/x-api/bookmarks/sync")
async def trigger_x_api_bookmark_sync(request: XApiBackfillRequest):
    """Backfill bookmarks from the X API and queue them for processing."""
    try:
        result = await run_x_api_bookmark_sync(
            max_results=request.max_results,
            max_pages=request.max_pages,
            resume_from_checkpoint=request.resume_from_checkpoint,
            process_immediately=False,
        )
        return {
            "status": "ok",
            "sync_config": result["sync_config"],
            "queued": result["queued"],
            "processed_immediately": result["processed_immediately"],
            "backfill": {
                "user_id": result["user_id"],
                "pages_fetched": result["pages_fetched"],
                "bookmarks_emitted": result["bookmarks_emitted"],
                "stopped_at_known_id": result["stopped_at_known_id"],
                "checkpoint": result["checkpoint"],
            },
        }
    except (
        XApiAuthConfigError,
        XApiAuthStateError,
        XApiTokenError,
        XApiBookmarkSyncConfigError,
        XApiBookmarkSyncStateError,
        ValueError,
    ) as exc:
        logger.error(f"X API bookmark sync failed: {exc}")
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error(f"Unexpected X API bookmark sync error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/x-api/monitoring/webhook")
async def receive_x_api_monitoring_webhook(request: Request):
    """Receive monitored-account post webhooks and promote useful posts into bookmarks."""
    try:
        verify_x_api_monitoring_webhook_secret(
            request.headers.get(X_API_MONITOR_SECRET_HEADER),
            runtime_config=config,
        )
        payload = await request.json()
        result = await process_x_api_monitoring_webhook(
            payload,
            runtime_config=config,
            llm_interface=LLMInterface(config.get("llm", {}) or {}),
            layout=build_path_layout(config),
        )
        bookmark_payload = result.pop("bookmark_payload", None)
        if bookmark_payload is not None:
            await ingest_bookmark_capture(
                bookmark_payload,
                process_immediately=False,
                queue_bookmark=True,
                reset_processed=True,
                force=True,
            )
            result["queued"] = True
        else:
            result["queued"] = False
        return result
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid webhook JSON: {exc}") from exc
    except (
        XApiAuthConfigError,
        XApiAuthStateError,
        XApiTokenError,
        XApiMonitoringConfigError,
        XApiMonitoringError,
        ValueError,
    ) as exc:
        logger.error(f"X monitored-account webhook failed: {exc}")
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        logger.error(f"Unexpected monitored-account webhook error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


class ProviderModelsRequest(BaseModel):
    type: str
    api_key: Optional[str] = None
    base_url: Optional[str] = None


@app.post("/api/providers/{provider_name}/models")
async def fetch_provider_models(provider_name: str, request: ProviderModelsRequest):
    """Fetch available models from a provider API"""
    try:
        import httpx

        provider_type = request.type
        api_key = request.api_key
        base_url = request.base_url
        models = []

        if provider_type == 'openai':
            if not api_key:
                api_key = os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise HTTPException(status_code=400, detail="OpenAI API key required")

            async with httpx.AsyncClient() as client:
                response = await client.get(
                    "https://api.openai.com/v1/models",
                    headers={"Authorization": f"Bearer {api_key}"},
                    timeout=30.0
                )
                response.raise_for_status()
                data = response.json()
                # Filter to chat models
                models = sorted([
                    m['id'] for m in data.get('data', [])
                    if 'gpt' in m['id'].lower() or 'o1' in m['id'].lower() or 'o3' in m['id'].lower()
                ])

        elif provider_type == 'anthropic':
            # Anthropic doesn't have a models list API, return known models
            models = [
                "claude-sonnet-4-20250514",
                "claude-3-5-sonnet-20241022",
                "claude-3-5-haiku-20241022",
                "claude-3-opus-20240229",
                "claude-3-sonnet-20240229",
                "claude-3-haiku-20240307"
            ]

        elif provider_type == 'openrouter':
            if not api_key:
                api_key = os.getenv('OPENROUTER_API_KEY') or os.getenv('OPEN_ROUTER_API_KEY')
            if not api_key:
                raise HTTPException(status_code=400, detail="OpenRouter API key required")

            url = (base_url or "https://openrouter.ai/api/v1").rstrip('/') + "/models"
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url,
                    headers={"Authorization": f"Bearer {api_key}"},
                    timeout=30.0
                )
                response.raise_for_status()
                data = response.json()
                models = sorted([m['id'] for m in data.get('data', [])])

        elif provider_type == 'local':
            # Query Ollama API
            url = (base_url or "http://localhost:11434").rstrip('/')
            # Ollama uses /api/tags for model list
            if '/v1' in url:
                url = url.replace('/v1', '')
            url = url + "/api/tags"

            async with httpx.AsyncClient() as client:
                try:
                    response = await client.get(url, timeout=10.0)
                    response.raise_for_status()
                    data = response.json()
                    models = sorted([m['name'] for m in data.get('models', [])])
                except Exception as e:
                    logger.warning(f"Could not fetch Ollama models: {e}")
                    models = ["llama3.2", "gemma3:27b", "mistral", "codellama"]

        else:
            raise HTTPException(status_code=400, detail=f"Unknown provider type: {provider_type}")

        return {"models": models, "provider": provider_name, "count": len(models)}

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching models for {provider_name}: {e}")
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching models for {provider_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/bookmark", response_model=ProcessingStatus)
async def receive_bookmark(bookmark: BookmarkCapture):
    """Receive a bookmark from the browser extension"""
    try:
        bookmark_data = bookmark.dict()
        has_graphql = bool(
            bookmark_data.get("graphql_response")
            or bookmark_data.get("graphql_cache_file")
        )

        if not has_graphql:
            await ingest_bookmark_capture(
                bookmark_data,
                queue_bookmark=False,
                reset_processed=False,
            )
            return ProcessingStatus(
                status="queued",
                message="Bookmark recorded; awaiting GraphQL detail",
                tweet_id=bookmark.tweet_id,
            )

        await ingest_bookmark_capture(
            bookmark_data,
            graphql_response=bookmark_data.get("graphql_response"),
            process_immediately=False,
            queue_bookmark=True,
            reset_processed=True,
            force=True,
        )

        return ProcessingStatus(
            status="accepted",
            message="Bookmark queued with GraphQL detail",
            tweet_id=bookmark.tweet_id,
            processed_at=datetime.now().isoformat(),
        )

    except Exception as e:
        logger.error(f"Error receiving bookmark: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/bookmarks")
async def get_bookmarks(limit: int = 100, processed: Optional[bool] = None):
    """Get recent bookmarks with optional processed filter"""
    bookmarks = load_realtime_bookmarks()
    # Optional filter by processed flag if present
    if processed is not None:
        filtered = [
            b for b in bookmarks if bool(b.get("processed", False)) == processed
        ]
    else:
        filtered = bookmarks
    return {"total": len(filtered), "bookmarks": filtered[-limit:]}


@app.get("/api/bookmarks/pending")
async def get_pending_bookmarks(limit: int = 100):
    """Return bookmarks that have not been processed yet."""
    db = get_metadata_db()
    entries = db.get_unprocessed_bookmarks(limit=limit)
    unprocessed = [entry for entry in entries if entry.status != "processed"]
    return {
        "total": len(unprocessed),
        "bookmarks": [serialize_bookmark_entry(entry) for entry in unprocessed],
    }


@app.post("/api/bookmarks/status")
async def bookmark_status(request: BookmarkStatusRequest):
    """Return processing status for a list of tweet IDs."""
    db = get_metadata_db()
    statuses = db.get_bookmark_statuses(request.tweet_ids)

    # Fill in defaults for tweet IDs that are unknown to the queue
    response = {}
    for tweet_id in request.tweet_ids:
        if tweet_id in statuses:
            response[tweet_id] = statuses[tweet_id]
        else:
            response[tweet_id] = {
                "status": "missing",
                "captured_at": None,
                "processed_at": None,
                "attempts": 0,
                "last_error": None,
                "next_attempt_at": None,
                "processed_with_graphql": False,
            }

    return {"statuses": response}


@app.post("/api/reprocess/{tweet_id}")
async def reprocess_tweet(
    tweet_id: str, no_resume: bool = Query(False, alias="no-resume")
):
    """Force reprocess a tweet (and thread if applicable) using cached GraphQL"""
    try:
        loader = CacheLoader()
        enhanced_map = loader.load_cached_enhancements([tweet_id])
        tweets_to_process = []
        cache_file = None

        if tweet_id in enhanced_map:
            tweets_to_process.append(enhanced_map[tweet_id])
            # Try find cache file
            cache_dir = build_path_layout(config).cache_root
            for f in cache_dir.glob(f"tweet_{tweet_id}_*.json"):
                cache_file = f
                break
        else:
            # Fall back to scanning cache dir for this tweet
            cache_dir = build_path_layout(config).cache_root
            for f in cache_dir.glob(f"tweet_{tweet_id}_*.json"):
                cache_file = f
                break
            if cache_file:
                tw = loader._load_tweet_from_cache(cache_file, tweet_id)
                if tw:
                    tweets_to_process.append(tw)

        if not tweets_to_process:
            raise HTTPException(
                status_code=404, detail="No cached GraphQL found for tweet"
            )

        # If part of a thread, load all (safe access - we know list is not empty here)
        if tweets_to_process and tweets_to_process[0].is_self_thread and cache_file:
            thread_tweets = loader.extract_all_thread_tweets_from_cache(cache_file)
            if len(thread_tweets) > 1:
                tweets_to_process = thread_tweets

        # Build URL mappings
        url_mappings: Dict[str, str] = {}
        for tw in tweets_to_process:
            if hasattr(tw, "url_mappings") and tw.url_mappings:
                for m in tw.url_mappings:
                    su = getattr(m, "short_url", None)
                    eu = getattr(m, "expanded_url", None)
                    if su and eu and su != eu:
                        url_mappings[su] = eu

        try:
            pipeline_stats = await run_pipeline_for_tweets(
                tweets_to_process,
                url_mappings=url_mappings or None,
                resume=not no_resume,
            )
        except Exception as e:
            logger.error(f"Pipeline processing failed: {e}")
            raise

        maybe_cleanup_graphql_cache(tweets_to_process, pipeline_stats, logger=logger)

        try:
            db = get_metadata_db()
            db.mark_bookmark_processed(tweet_id, with_graphql=bool(cache_file))
        except Exception:
            pass

        return {"status": "ok", "reprocessed": len(tweets_to_process)}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reprocessing tweet {tweet_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/triggers/github-stars")
async def trigger_github_stars(request: GitHubTriggerRequest):
    """Trigger the GitHub stars processor manually."""
    try:
        stats = await run_github_stars_sync(limit=request.limit, resume=request.resume)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # pragma: no cover - defensive
        logger.error(f"GitHub stars processing failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

    logger.info(
        "GitHub stars trigger completed: %s processed, %s skipped, %s errors",
        stats.updated,
        stats.skipped,
        stats.errors,
    )

    return {
        "status": "ok",
        "resume": request.resume,
        "limit": request.limit,
        "stats": serialize_processing_stats(stats),
    }


@app.post("/api/triggers/huggingface-likes")
async def trigger_huggingface_likes(request: HuggingFaceTriggerRequest):
    """Trigger the HuggingFace likes processor manually."""
    try:
        stats = await run_huggingface_likes_sync(
            limit=request.limit,
            resume=request.resume,
            include_models=request.include_models,
            include_datasets=request.include_datasets,
            include_spaces=request.include_spaces,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # pragma: no cover - defensive
        logger.error(f"HuggingFace likes processing failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))

    logger.info(
        "HuggingFace likes trigger completed: %s processed, %s skipped, %s errors",
        stats.updated,
        stats.skipped,
        stats.errors,
    )

    return {
        "status": "ok",
        "resume": request.resume,
        "limit": request.limit,
        "include": {
            "models": request.include_models,
            "datasets": request.include_datasets,
            "spaces": request.include_spaces,
        },
        "stats": serialize_processing_stats(stats),
    }


@app.post("/api/digest")
async def generate_digest(request: DigestRequest):
    """Generate digest notes for content discovery in Obsidian."""
    from processors.digest_generator import DigestGenerator, send_ntfy_notification

    try:
        generator = DigestGenerator()
        generated_files = []

        if request.digest_type == "weekly":
            # Calculate week range
            if request.week:
                try:
                    year, week = request.week.split("-W")
                    week_start = datetime.strptime(f"{year}-W{week}-1", "%Y-W%W-%w")
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Invalid week format: {request.week}. Use YYYY-WNN"
                    )
            else:
                today = datetime.now()
                week_start = today - timedelta(days=today.weekday())

            filepath = generator.generate_weekly_digest(week_start=week_start)
            generated_files.append(filepath)

        elif request.digest_type == "inbox":
            filepath = generator.generate_inbox_view()
            generated_files.append(filepath)

        elif request.digest_type == "dashboard":
            filepath = generator.generate_discovery_dashboard()
            generated_files.append(filepath)

        elif request.digest_type == "all":
            generated_files.append(generator.generate_discovery_dashboard())
            generated_files.append(generator.generate_inbox_view())
            generated_files.append(generator.generate_weekly_digest())

        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid digest type: {request.digest_type}"
            )

        # Send notification if requested
        notification_sent = False
        if request.notify and generated_files:
            file_names = [Path(f).name for f in generated_files]
            notification_sent = send_ntfy_notification(
                title="Thoth Digest Ready",
                message=f"Generated {len(generated_files)} digest files: {', '.join(file_names)}"
            )

        return {
            "status": "ok",
            "digest_type": request.digest_type,
            "generated_files": [Path(f).name for f in generated_files],
            "digests_dir": str(generator.digests_dir),
            "notification_sent": notification_sent
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating digest: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/digests")
async def list_digests():
    """List all available digest files."""
    from processors.digest_generator import DigestGenerator

    try:
        generator = DigestGenerator()
        digests_dir = generator.digests_dir

        if not digests_dir.exists():
            return {"digests": [], "digests_dir": str(digests_dir)}

        digests = []
        for md_file in sorted(digests_dir.glob("*.md"), reverse=True):
            stat = md_file.stat()
            digests.append({
                "name": md_file.name,
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
            })

        return {
            "digests": digests,
            "digests_dir": str(digests_dir),
            "total": len(digests)
        }

    except Exception as e:
        logger.error(f"Error listing digests: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/digest/{filename}")
async def get_digest_content(filename: str):
    """Get the content of a specific digest file."""
    from processors.digest_generator import DigestGenerator

    try:
        generator = DigestGenerator()
        filepath = generator.digests_dir / filename

        if not filepath.exists():
            raise HTTPException(status_code=404, detail="Digest not found")

        # Security: ensure path is within digests_dir
        if not filepath.resolve().is_relative_to(generator.digests_dir.resolve()):
            raise HTTPException(status_code=403, detail="Access denied")

        content = filepath.read_text(encoding='utf-8')
        return {
            "filename": filename,
            "content": content,
            "size": len(content)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error reading digest {filename}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/process")
async def trigger_processing():
    """Trigger processing of all pending bookmarks"""
    try:
        bookmarks = load_realtime_bookmarks()

        # Create Tweet objects
        tweets = []
        for bookmark in bookmarks:
            tweet_id = bookmark["tweet_id"]
            tweet_data = bookmark.get("tweet_data", {})

            tweet = Tweet(
                id=tweet_id,
                full_text=tweet_data.get("text", ""),
                created_at=bookmark["timestamp"],
                screen_name=tweet_data.get("author", "unknown"),
                name=tweet_data.get("author", "Unknown"),
            )
            tweets.append(tweet)

        stats = await run_pipeline_for_tweets(tweets)

        try:
            db = get_metadata_db()
            for tweet in tweets:
                db.mark_bookmark_processed(tweet.id, with_graphql=False)
        except Exception:
            pass

        # Mark processed in realtime storage
        try:

            def mark_all_processed(entries: List[dict]) -> Tuple[bool, None]:
                dirty = False
                for entry in entries:
                    if entry.get("processed") is not True:
                        entry["processed"] = True
                        dirty = True
                return dirty, None

            await mutate_realtime_bookmarks(mark_all_processed)
        except Exception as e:
            logger.debug(f"Failed marking bookmarks processed: {e}")

        return {
            "status": "completed",
            "processed": stats.processed_tweets if stats else 0,
            "total": len(tweets),
        }

    except Exception as e:
        logger.error(f"Error processing bookmarks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/bookmark/{tweet_id}")
async def delete_bookmark(tweet_id: str, dry_run: bool = Query(False)):
    """Delete a bookmark and all its associated artifacts"""
    try:
        # Import the delete function from thoth.py
        sys.path.insert(0, str(Path(__file__).parent))
        from thoth import delete_tweet_artifacts
        
        # Perform deletion
        stats = delete_tweet_artifacts(tweet_id, dry_run)
        
        # Calculate totals
        total_files = (
            len(stats["tweet_files"]) + 
            len(stats["thread_files"]) + 
            len(stats["media_files"]) + 
            len(stats["transcript_files"]) +
            len(stats["cache_files"]) +
            len(stats["pdf_files"]) +
            len(stats["repo_files"])
        )
        
        # Remove from processing queue if present
        if not dry_run:
            try:
                db = get_metadata_db()
                db.delete_bookmark_entry(tweet_id)
            except Exception as e:
                logger.warning(f"Failed to remove from bookmark queue: {e}")
        
        return {
            "status": "ok" if not stats["errors"] else "partial",
            "tweet_id": tweet_id,
            "dry_run": dry_run,
            "deleted": {
                "total_files": total_files,
                "tweet_files": len(stats["tweet_files"]),
                "thread_files": len(stats["thread_files"]),
                "media_files": len(stats["media_files"]),
                "transcript_files": len(stats["transcript_files"]),
                "cache_files": len(stats["cache_files"]),
                "database_entries": stats["database_entries"]
            },
            "errors": stats["errors"][:10] if stats["errors"] else []
        }
        
    except Exception as e:
        logger.error(f"Error deleting bookmark {tweet_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stats")
async def get_stats():
    """Get processing statistics"""
    bookmarks = load_realtime_bookmarks()
    realtime_processed = sum(1 for bookmark in bookmarks if bookmark.get("processed"))
    realtime_total = len(bookmarks)
    realtime_pending = max(realtime_total - realtime_processed, 0)

    # Count by source
    sources = {}
    for bookmark in bookmarks:
        source = bookmark.get("source", "unknown")
        sources[source] = sources.get(source, 0) + 1

    # Count by date
    dates = {}
    for bookmark in bookmarks:
        date = bookmark["timestamp"][:10]
        dates[date] = dates.get(date, 0) + 1

    try:
        queue_counts = get_metadata_db().get_bookmark_queue_counts()
    except Exception:
        queue_counts = {"pending": 0, "processing": 0, "processed": 0, "failed": 0}
    queue_total = sum(queue_counts.values())

    return {
        "total_bookmarks": realtime_total,
        "realtime_counts": {
            "total": realtime_total,
            "processed": realtime_processed,
            "pending": realtime_pending,
        },
        "by_source": sources,
        "by_date": dates,
        "queue_size": PROCESSING_QUEUE.qsize(),
        "queue_counts": queue_counts,
        "queue_total": queue_total,
    }


# Background processor with graceful shutdown support
_shutdown_event = asyncio.Event()
_background_task = None
_ingestion_task = None
_social_sync_task = None
_x_api_sync_task = None
_archivist_task = None


async def run_periodic_social_sync(sync_config: Optional[Dict[str, Any]] = None):
    """Run configured social sync jobs once."""
    resolved = sync_config or resolve_social_sync_config()

    if resolved["github"]["enabled"]:
        try:
            stats = await run_github_stars_sync(
                limit=resolved["github"]["limit"],
                resume=resolved["github"]["resume"],
            )
            logger.info(
                "Scheduled GitHub sync completed: %s processed, %s skipped, %s errors",
                stats.updated,
                stats.skipped,
                stats.errors,
            )
        except Exception as exc:
            logger.warning(f"Scheduled GitHub sync skipped or failed: {exc}")

    if resolved["huggingface"]["enabled"]:
        try:
            stats = await run_huggingface_likes_sync(
                limit=resolved["huggingface"]["limit"],
                resume=resolved["huggingface"]["resume"],
                include_models=resolved["huggingface"]["include_models"],
                include_datasets=resolved["huggingface"]["include_datasets"],
                include_spaces=resolved["huggingface"]["include_spaces"],
            )
            logger.info(
                "Scheduled HuggingFace sync completed: %s processed, %s skipped, %s errors",
                stats.updated,
                stats.skipped,
                stats.errors,
            )
        except Exception as exc:
            logger.warning(f"Scheduled HuggingFace sync skipped or failed: {exc}")


async def social_sync_scheduler():
    """Periodically run GitHub and HuggingFace sync jobs."""
    metadata_db = get_metadata_db()

    while not _shutdown_event.is_set():
        try:
            sync_config = resolve_social_sync_config()
        except ValueError as exc:
            logger.warning(f"Social sync scheduler configuration invalid: {exc}")
            try:
                await asyncio.wait_for(_shutdown_event.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                continue
            break

        enabled = sync_config["enabled"]
        interval_seconds = sync_config["interval_hours"] * 3600.0

        if not enabled:
            try:
                await asyncio.wait_for(_shutdown_event.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                continue
            break

        now = datetime.now(timezone.utc)
        next_run_at = get_non_live_next_run_at(
            metadata_db,
            job_name=SOCIAL_SYNC_JOB_NAME,
            interval_hours=sync_config["interval_hours"],
            run_on_startup=sync_config["run_on_startup"],
            now=now,
        )

        if now >= next_run_at:
            run_error: str | None = None
            mark_non_live_run_started(
                metadata_db,
                job_name=SOCIAL_SYNC_JOB_NAME,
                interval_hours=sync_config["interval_hours"],
                now=now,
            )
            try:
                await run_periodic_social_sync(sync_config)
            except Exception as exc:
                run_error = str(exc)
                logger.warning(f"Scheduled social sync failed: {exc}")
            finally:
                mark_non_live_run_finished(
                    metadata_db,
                    job_name=SOCIAL_SYNC_JOB_NAME,
                    success=run_error is None,
                    error=run_error,
                    now=datetime.now(timezone.utc),
                )
            next_run_at = get_non_live_next_run_at(
                metadata_db,
                job_name=SOCIAL_SYNC_JOB_NAME,
                interval_hours=sync_config["interval_hours"],
                run_on_startup=False,
                now=datetime.now(timezone.utc),
            )

        delay = max(
            1.0,
            min(60.0, (next_run_at - datetime.now(timezone.utc)).total_seconds()),
        )
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            continue


async def ingestion_worker():
    """Process queued knowledge artifacts in the background."""
    runtime = get_knowledge_artifact_runtime(config, layout=build_path_layout(config))
    await runtime.run_background(_shutdown_event, poll_interval_seconds=5.0)


async def x_api_sync_scheduler():
    """Periodically run X bookmark backfills."""
    metadata_db = get_metadata_db()

    while not _shutdown_event.is_set():
        try:
            sync_config = resolve_x_api_sync_config()
        except (XApiAuthConfigError, XApiBookmarkSyncConfigError, ValueError) as exc:
            logger.warning(f"X API sync scheduler configuration invalid: {exc}")
            try:
                await asyncio.wait_for(_shutdown_event.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                continue
            break
        enabled = sync_config["enabled"]
        interval_seconds = sync_config["interval_hours"] * 3600.0

        if not enabled:
            try:
                await asyncio.wait_for(_shutdown_event.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                continue
            break

        now = datetime.now(timezone.utc)
        next_run_at = get_non_live_next_run_at(
            metadata_db,
            job_name=X_API_SYNC_JOB_NAME,
            interval_hours=sync_config["interval_hours"],
            run_on_startup=sync_config["run_on_startup"],
            now=now,
        )

        if now >= next_run_at:
            run_error: str | None = None
            mark_non_live_run_started(
                metadata_db,
                job_name=X_API_SYNC_JOB_NAME,
                interval_hours=sync_config["interval_hours"],
                now=now,
            )
            try:
                await run_x_api_bookmark_sync(
                    max_results=sync_config["max_results"],
                    max_pages=sync_config["max_pages"],
                    resume_from_checkpoint=sync_config["resume_from_checkpoint"],
                    process_immediately=False,
                )
            except Exception as exc:
                run_error = str(exc)
                logger.warning(f"Scheduled X API sync failed: {exc}")
            finally:
                mark_non_live_run_finished(
                    metadata_db,
                    job_name=X_API_SYNC_JOB_NAME,
                    success=run_error is None,
                    error=run_error,
                    now=datetime.now(timezone.utc),
                )
            next_run_at = get_non_live_next_run_at(
                metadata_db,
                job_name=X_API_SYNC_JOB_NAME,
                interval_hours=sync_config["interval_hours"],
                run_on_startup=False,
                now=datetime.now(timezone.utc),
            )

        delay = max(
            1.0,
            min(60.0, (next_run_at - datetime.now(timezone.utc)).total_seconds()),
        )
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            continue


async def archivist_scheduler():
    """Periodically run archivist topic compilation."""
    metadata_db = get_metadata_db()

    while not _shutdown_event.is_set():
        try:
            sync_config = resolve_archivist_sync_config()
        except (ArchivistRuntimeError, ValueError) as exc:
            logger.warning(f"Archivist scheduler configuration invalid: {exc}")
            try:
                await asyncio.wait_for(_shutdown_event.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                continue
            break

        if not sync_config["enabled"]:
            try:
                await asyncio.wait_for(_shutdown_event.wait(), timeout=60.0)
            except asyncio.TimeoutError:
                continue
            break

        now = datetime.now(timezone.utc)
        next_run_at = get_non_live_next_run_at(
            metadata_db,
            job_name=ARCHIVIST_JOB_NAME,
            interval_hours=sync_config["interval_hours"],
            run_on_startup=sync_config["run_on_startup"],
            now=now,
        )

        if now >= next_run_at:
            run_error: str | None = None
            mark_non_live_run_started(
                metadata_db,
                job_name=ARCHIVIST_JOB_NAME,
                interval_hours=sync_config["interval_hours"],
                now=now,
            )
            try:
                await run_archivist_compilation()
            except Exception as exc:
                run_error = str(exc)
                logger.warning(f"Scheduled archivist run failed: {exc}")
            finally:
                mark_non_live_run_finished(
                    metadata_db,
                    job_name=ARCHIVIST_JOB_NAME,
                    success=run_error is None,
                    error=run_error,
                    now=datetime.now(timezone.utc),
                )
            next_run_at = get_non_live_next_run_at(
                metadata_db,
                job_name=ARCHIVIST_JOB_NAME,
                interval_hours=sync_config["interval_hours"],
                run_on_startup=False,
                now=datetime.now(timezone.utc),
            )

        delay = max(
            1.0,
            min(60.0, (next_run_at - datetime.now(timezone.utc)).total_seconds()),
        )
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=delay)
        except asyncio.TimeoutError:
            continue


async def background_processor():
    """Process bookmarks from queue in background with graceful shutdown support"""
    while not _shutdown_event.is_set():
        try:
            # Use wait_for with timeout to allow checking shutdown event periodically
            try:
                bookmark_data = await asyncio.wait_for(
                    PROCESSING_QUEUE.get(),
                    timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            tweet_id = bookmark_data.get("tweet_id")
            logger.debug(f"Dequeued bookmark {tweet_id} for processing")

            await process_bookmark_async(bookmark_data)
            PROCESSING_QUEUE.task_done()

        except asyncio.CancelledError:
            logger.info("Background processor cancelled")
            break
        except Exception as e:
            logger.error(f"Background processor error: {e}")
            try:
                PROCESSING_QUEUE.task_done()
            except ValueError:
                pass  # task_done called too many times

    logger.info("Background processor stopped")


@app.on_event("startup")
async def startup_event():
    """Start background processor on startup"""
    global _shutdown_event, _background_task, _ingestion_task, _social_sync_task, _x_api_sync_task, _archivist_task
    _shutdown_event = asyncio.Event()
    ensure_wiki_scaffold(config)
    _background_task = asyncio.create_task(background_processor())
    _ingestion_task = asyncio.create_task(ingestion_worker())
    _social_sync_task = asyncio.create_task(social_sync_scheduler())
    resolve_x_api_sync_config()
    _x_api_sync_task = asyncio.create_task(x_api_sync_scheduler())
    _archivist_task = asyncio.create_task(archivist_scheduler())
    await load_pending_bookmarks_from_db()
    logger.info("Thoth API server started")


@app.on_event("shutdown")
async def shutdown_event():
    """Gracefully shutdown background processor"""
    global _background_task, _ingestion_task, _social_sync_task, _x_api_sync_task, _archivist_task
    logger.info("Shutting down Thoth API server...")
    _shutdown_event.set()
    if _background_task:
        _background_task.cancel()
        try:
            await _background_task
        except asyncio.CancelledError:
            pass
    if _ingestion_task:
        _ingestion_task.cancel()
        try:
            await _ingestion_task
        except asyncio.CancelledError:
            pass
    if _social_sync_task:
        _social_sync_task.cancel()
        try:
            await _social_sync_task
        except asyncio.CancelledError:
            pass
    if _x_api_sync_task:
        _x_api_sync_task.cancel()
        try:
            await _x_api_sync_task
        except asyncio.CancelledError:
            pass
    if _archivist_task:
        _archivist_task.cancel()
        try:
            await _archivist_task
        except asyncio.CancelledError:
            pass
    logger.info("Thoth API server stopped")


def main():
    """Run the API server"""
    server_options = resolve_api_server_options()
    logger.info(
        "Starting Thoth API on http://%s:%s (override with THOTH_API_HOST/THOTH_API_PORT)",
        server_options["host"],
        server_options["port"],
    )
    uvicorn.run("thoth_api:app", log_level="info", **server_options)


if __name__ == "__main__":
    main()
