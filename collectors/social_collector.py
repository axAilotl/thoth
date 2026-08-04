"""
Social Collector - Automates GitHub stars and HuggingFace likes collection.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any, List, Optional, Tuple

import requests

from core.artifacts.repository import RepositoryArtifact
from core.capture_event_store import CaptureEventStore
from core.capture_lifecycle import CaptureLifecycleService
from core.config import Config, config as runtime_config
from core.connector_capture import ConnectorCaptureQueue, write_connector_raw_json
from core.metadata_db import MetadataDB, get_metadata_db
from core.path_layout import PathLayout, build_path_layout

logger = logging.getLogger(__name__)


class SocialCollector:
    """Collector for social knowledge artifacts (GitHub stars, HF likes)."""

    def __init__(
        self,
        db: Optional[MetadataDB] = None,
        *,
        config: Config | None = None,
        layout: PathLayout | None = None,
        capture_event_store: CaptureEventStore | None = None,
    ):
        self.config = config or runtime_config
        self.layout = layout or build_path_layout(self.config)
        self.db = db or get_metadata_db()
        self.capture_queue = ConnectorCaptureQueue(
            self.config,
            layout=self.layout,
            db=self.db,
            capture_event_store=capture_event_store,
        )
        self.session = requests.Session()

    def discover_github_stars(
        self,
        username: Optional[str] = None,
        limit: int = 50,
        token: Optional[str] = None,
    ) -> List[RepositoryArtifact]:
        """Fetch starred GitHub repositories and queue new artifacts."""
        token = (
            token
            or self.config.get("sources.github.token")
            or os.getenv("GITHUB_API")
            or os.getenv("GITHUB_TOKEN")
        )
        if not token and not username:
            logger.warning(
                "GitHub token not configured, skipping authenticated stars collection."
            )
            return []

        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        discovered = []
        page = 1
        per_page = min(max(limit, 1), 100)
        url = (
            f"https://api.github.com/users/{username}/starred"
            if username
            else "https://api.github.com/user/starred"
        )

        run_id = datetime.now().isoformat()
        with self.capture_queue.lifecycle() as lifecycle:
            while len(discovered) < limit:
                response = self.session.get(
                    url,
                    headers=headers,
                    params={"per_page": per_page, "page": page, "sort": "updated"},
                    timeout=30,
                )
                if response.status_code != 200:
                    logger.error(
                        "Failed to fetch GitHub stars: %s %s",
                        response.status_code,
                        response.text[:500],
                    )
                    return discovered

                batch = response.json()
                if not batch:
                    break

                for repo_data in batch:
                    artifact_id = f"gh_{repo_data['id']}"
                    if self.db.get_ingestion_entry(artifact_id):
                        continue

                    raw_repo = self._to_serializable(repo_data)
                    repo = RepositoryArtifact(
                        id=artifact_id,
                        source_type="github",
                        raw_content=json.dumps(raw_repo),
                        created_at=repo_data.get("updated_at"),
                        ingested_at=datetime.now().isoformat(),
                        repo_name=repo_data.get("full_name"),
                        description=repo_data.get("description") or "",
                        stars=repo_data.get("stargazers_count", 0),
                        language=repo_data.get("language"),
                        topics=repo_data.get("topics", []),
                    )

                    if self._queue_repository(
                        repo,
                        source="github",
                        raw_payload=raw_repo,
                        lifecycle=lifecycle,
                        run_id=run_id,
                        account=username,
                        event_type="github_star",
                        base_uri="https://api.github.com/user/starred"
                        if not username
                        else f"https://api.github.com/users/{username}/starred",
                    ):
                        discovered.append(repo)
                        logger.info(f"Queued GitHub repo: {repo.repo_name}")
                        if len(discovered) >= limit:
                            break

                if len(batch) < per_page:
                    break
                page += 1

        return discovered

    def discover_hf_likes(
        self, username: Optional[str] = None, limit: int = 50
    ) -> List[RepositoryArtifact]:
        """Fetch liked HuggingFace repositories and queue new artifacts."""
        hf_user = (
            username
            or self.config.get("sources.huggingface.username")
            or os.getenv("HF_USER")
        )
        if not hf_user:
            logger.warning("HF_USER or sources.huggingface.username is not configured.")
            return []

        hf_token = (
            self.config.get("sources.huggingface.token")
            or os.getenv("HF_TOKEN")
            or os.getenv("HUGGINGFACEHUB_API_TOKEN")
            or os.getenv("HUGGINGFACE_API_TOKEN")
        )

        include_models = self.config.get("sources.huggingface.include_models", True)
        include_datasets = self.config.get("sources.huggingface.include_datasets", True)
        include_spaces = self.config.get("sources.huggingface.include_spaces", True)

        try:
            from huggingface_hub import list_liked_repos, repo_info
        except ImportError:
            logger.error(
                "huggingface_hub package required. Install with: pip install huggingface_hub"
            )
            return []

        try:
            likes = list_liked_repos(hf_user, token=hf_token or None)
        except Exception as exc:
            logger.error(f"Failed to fetch HuggingFace likes for {hf_user}: {exc}")
            return []

        repo_candidates: List[Tuple[Any, str]] = []
        candidate_sets = [
            ("model", getattr(likes, "models", []) if include_models else []),
            ("dataset", getattr(likes, "datasets", []) if include_datasets else []),
            ("space", getattr(likes, "spaces", []) if include_spaces else []),
        ]

        for repo_type, repo_ids in candidate_sets:
            for repo_id in repo_ids:
                try:
                    repo_candidates.append(
                        (
                            repo_info(
                                repo_id,
                                repo_type=repo_type,
                                token=hf_token or None,
                            ),
                            repo_type,
                        )
                    )
                except Exception as exc:
                    logger.warning(
                        "Could not fetch %s info for %s: %s", repo_type, repo_id, exc
                    )

        repo_candidates.sort(
            key=lambda item: getattr(item[0], "likes", 0) or 0,
            reverse=True,
        )
        if limit:
            repo_candidates = repo_candidates[:limit]

        discovered = []
        run_id = datetime.now().isoformat()
        with self.capture_queue.lifecycle() as lifecycle:
            for repo_data, repo_type in repo_candidates:
                repo_name = getattr(repo_data, "id", None)
                if not repo_name:
                    continue

                artifact_id = f"hf_{repo_type}_{repo_name.replace('/', '_')}"
                if self.db.get_ingestion_entry(artifact_id):
                    continue

                created_at = getattr(repo_data, "created_at", None)
                if hasattr(created_at, "isoformat"):
                    created_at = created_at.isoformat()

                raw_repo = self._to_serializable(repo_data)
                repo = RepositoryArtifact(
                    id=artifact_id,
                    source_type="huggingface",
                    raw_content=json.dumps(raw_repo),
                    created_at=created_at or datetime.now().isoformat(),
                    ingested_at=datetime.now().isoformat(),
                    repo_name=repo_name,
                    description=getattr(repo_data, "description", None)
                    or f"HuggingFace {repo_type}: {repo_name}",
                    stars=getattr(repo_data, "likes", 0) or 0,
                    language=getattr(repo_data, "pipeline_tag", None)
                    or getattr(repo_data, "library_name", None),
                    topics=getattr(repo_data, "tags", []) or [],
                )

                if self._queue_repository(
                    repo,
                    source="huggingface",
                    raw_payload=raw_repo,
                    lifecycle=lifecycle,
                    run_id=run_id,
                    account=hf_user,
                    event_type="huggingface_like",
                    base_uri="https://huggingface.co",
                    source_metadata={"repo_type": repo_type},
                ):
                    discovered.append(repo)
                    logger.info("Queued HuggingFace %s: %s", repo_type, repo.repo_name)

        return discovered

    def _queue_repository(
        self,
        repo: RepositoryArtifact,
        source: str,
        *,
        raw_payload: Any,
        lifecycle: CaptureLifecycleService,
        run_id: str,
        account: str | None,
        event_type: str,
        base_uri: str,
        source_metadata: dict[str, Any] | None = None,
    ) -> bool:
        """Persist a repository artifact in the ingestion queue."""
        raw_path = None
        if lifecycle.capture_event_store is not None:
            raw_path = write_connector_raw_json(
                self.layout,
                connector_name=source,
                subdir="stars" if source == "github" else "likes",
                native_id=repo.repo_name or repo.id,
                payload=raw_payload,
                captured_at=repo.ingested_at,
            )

        result = self.capture_queue.queue_artifact(
            lifecycle,
            repo,
            artifact_type="repository",
            source={
                "source_name": source,
                "source_type": source,
                "collector": "social_collector",
                "account": account,
                "native_source_id": repo.repo_name or repo.id,
                "base_uri": base_uri,
                "metadata": source_metadata or {},
            },
            session={
                "session_type": f"{source}_scan",
                "native_session_id": f"{source}:{account or 'authenticated'}:{run_id}",
                "started_at": run_id,
                "metadata": {
                    "account": account,
                    **(source_metadata or {}),
                },
            },
            event={
                "event_type": event_type,
                "native_event_id": repo.repo_name or repo.id,
                "occurred_at": repo.created_at,
                "captured_at": repo.ingested_at,
                "privacy": {
                    "classification": "public" if account else "personal",
                },
                "provenance": {"collector": "social_collector"},
            },
            raw_path=raw_path,
        )
        return self.db.get_ingestion_entry(result.queue_artifact_id) is not None

    def _to_serializable(self, value: Any) -> Any:
        """Convert SDK objects into JSON-serializable structures."""
        if isinstance(value, dict):
            return {key: self._to_serializable(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._to_serializable(item) for item in value]
        if hasattr(value, "__dict__"):
            return {
                key: self._to_serializable(item)
                for key, item in vars(value).items()
                if not key.startswith("_")
            }
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return value
