"""
Tests for social discovery collectors.
"""

import os
import sys
from datetime import datetime
from types import SimpleNamespace

from collectors.social_collector import SocialCollector


class FakeDB:
    def __init__(self):
        self.entries = []
        self.existing = {}

    def get_ingestion_entry(self, artifact_id):
        return self.existing.get(artifact_id)

    def upsert_ingestion_entry(self, entry):
        self.entries.append(entry)
        return True


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


def test_github_discovery_uses_authenticated_endpoint_without_username(monkeypatch):
    db = FakeDB()
    collector = SocialCollector(db=db)
    called = []

    monkeypatch.setenv("GITHUB_API", "token")

    def fake_get(url, headers=None, params=None, timeout=None):
        called.append((url, params))
        return FakeResponse(
            200,
            [
                {
                    "id": 123,
                    "full_name": "octo/repo",
                    "description": "Useful repo",
                    "stargazers_count": 42,
                    "language": "Python",
                    "topics": ["agents"],
                    "updated_at": "2026-04-03T00:00:00Z",
                }
            ],
        )

    collector.session = SimpleNamespace(get=fake_get)

    discovered = collector.discover_github_stars(None, limit=5)

    assert len(discovered) == 1
    assert called[0][0] == "https://api.github.com/user/starred"
    assert discovered[0].repo_name == "octo/repo"
    assert db.entries[0].source == "github"


def test_huggingface_discovery_uses_repo_info_enrichment(monkeypatch):
    db = FakeDB()
    collector = SocialCollector(db=db)
    monkeypatch.setenv("HF_USER", "example-user")

    fake_module = SimpleNamespace()
    fake_module.list_liked_repos = lambda user, token=None: SimpleNamespace(
        models=["org/model-a"],
        datasets=["org/dataset-a"],
        spaces=["org/space-a"],
    )

    def fake_repo_info(repo_id, repo_type=None, token=None):
        return SimpleNamespace(
            id=repo_id,
            description=f"{repo_type} description",
            likes={"model": 10, "dataset": 5, "space": 2}[repo_type],
            pipeline_tag="text-generation" if repo_type == "model" else None,
            library_name="transformers" if repo_type == "model" else None,
            tags=[repo_type, "ai"],
            created_at=datetime(2026, 4, 1, 0, 0, 0),
        )

    fake_module.repo_info = fake_repo_info
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_module)

    discovered = collector.discover_hf_likes(None, limit=3)

    assert [repo.id for repo in discovered] == [
        "hf_model_org_model-a",
        "hf_dataset_org_dataset-a",
        "hf_space_org_space-a",
    ]
    assert discovered[0].repo_name == "org/model-a"
    assert discovered[0].stars == 10
    assert db.entries[0].source == "huggingface"
