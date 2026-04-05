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
from core.metadata_db import MetadataDB, IngestionQueueEntry, get_metadata_db
from core.config import config

logger = logging.getLogger(__name__)


class SocialCollector:
    """Collector for social knowledge artifacts (GitHub stars, HF likes)."""

    def __init__(self, db: Optional[MetadataDB] = None):
        self.db = db or get_metadata_db()
        self.session = requests.Session()

    def discover_github_stars(
        self, username: Optional[str] = None, limit: int = 50
    ) -> List[RepositoryArtifact]:
        """Fetch starred GitHub repositories and queue new artifacts."""
        token = (
            config.get("sources.github.token")
            or os.getenv("GITHUB_API")
            or os.getenv("GITHUB_TOKEN")
        )
        if not token:
            logger.warning("GitHub token not configured, skipping stars collection.")
            return []

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        discovered = []
        page = 1
        per_page = min(max(limit, 1), 100)
        url = (
            f"https://api.github.com/users/{username}/starred"
            if username
            else "https://api.github.com/user/starred"
        )

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

                repo = RepositoryArtifact(
                    id=artifact_id,
                    source_type="github",
                    raw_content=json.dumps(self._to_serializable(repo_data)),
                    created_at=repo_data.get("updated_at"),
                    ingested_at=datetime.now().isoformat(),
                    repo_name=repo_data.get("full_name"),
                    description=repo_data.get("description") or "",
                    stars=repo_data.get("stargazers_count", 0),
                    language=repo_data.get("language"),
                    topics=repo_data.get("topics", []),
                )

                if self._queue_repository(repo, source="github"):
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
            or config.get("sources.huggingface.username")
            or os.getenv("HF_USER")
        )
        if not hf_user:
            logger.warning("HF_USER or sources.huggingface.username is not configured.")
            return []

        hf_token = (
            config.get("sources.huggingface.token")
            or os.getenv("HF_TOKEN")
            or os.getenv("HUGGINGFACEHUB_API_TOKEN")
            or os.getenv("HUGGINGFACE_API_TOKEN")
        )

        include_models = config.get("sources.huggingface.include_models", True)
        include_datasets = config.get("sources.huggingface.include_datasets", True)
        include_spaces = config.get("sources.huggingface.include_spaces", True)

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

            repo = RepositoryArtifact(
                id=artifact_id,
                source_type="huggingface",
                raw_content=json.dumps(self._to_serializable(repo_data)),
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

            if self._queue_repository(repo, source="huggingface"):
                discovered.append(repo)
                logger.info("Queued HuggingFace %s: %s", repo_type, repo.repo_name)

        return discovered

    def _queue_repository(self, repo: RepositoryArtifact, source: str) -> bool:
        """Persist a repository artifact in the ingestion queue."""
        queue_entry = IngestionQueueEntry(
            artifact_id=repo.id,
            artifact_type="repository",
            source=source,
            payload_json=json.dumps(repo.to_dict()),
            created_at=repo.ingested_at,
            capabilities_json=json.dumps(list(repo.capabilities)),
        )
        return self.db.upsert_ingestion_entry(queue_entry)

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
