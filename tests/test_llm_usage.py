import asyncio
import json
from pathlib import Path

from core.llm_cache import LLMCache
from core.llm_interface import LLMInterface, LLMResponse
from core.llm_usage import build_llm_usage_status, record_llm_usage
from core.metadata_db import MetadataDB


def test_llm_usage_record_excludes_prompts_and_secrets(tmp_path: Path):
    db = MetadataDB(str(tmp_path / "meta.db"))
    secret = "sk-proj-" + "a" * 32

    event = record_llm_usage(
        provider="openai",
        model="gpt-test",
        task="summary",
        operation="generate",
        input_text=f"Summarize this secret: {secret}",
        output_text="safe summary",
        provider_tokens=42,
        pricing={
            "input_cost_per_1k_tokens_usd": 0.1,
            "output_cost_per_1k_tokens_usd": 0.2,
        },
        source_connector="x_api",
        run_id="run-1",
        db=db,
    )

    assert event is not None
    payload = build_llm_usage_status(db)
    assert payload["call_count"] == 1
    assert payload["total_tokens_estimate"] == 42
    assert payload["totals_by_source"][0]["source_connector"] == "x_api"
    assert payload["totals_by_task"][0]["task"] == "summary"

    with db._get_connection() as conn:
        rows = [dict(row) for row in conn.execute("SELECT * FROM llm_usage_events")]
    serialized = json.dumps(rows, ensure_ascii=False)
    assert secret not in serialized
    assert "Summarize this secret" not in serialized


def test_llm_interface_records_provider_call_usage(tmp_path: Path):
    db = MetadataDB(str(tmp_path / "meta.db"))
    prompts = []

    class FakeProvider:
        model = "fake-model"

        async def generate(self, prompt, system_prompt=None, **kwargs):
            prompts.append(prompt)
            return LLMResponse(
                content="result body",
                model=self.model,
                provider="fake",
                tokens_used=17,
            )

    interface = LLMInterface.__new__(LLMInterface)
    interface.config = {
        "tasks": {"summary": {"enabled": True, "fallback": [{"provider": "fake"}]}},
        "providers": {},
        "observability": {
            "pricing": {
                "defaults": {
                    "input_cost_per_1k_tokens_usd": 0.1,
                    "output_cost_per_1k_tokens_usd": 0.2,
                }
            }
        },
    }
    interface.providers = {"fake": FakeProvider()}
    interface.provider_models = {"fake": {"default": {"id": "fake-model"}}}
    interface.usage_db = db

    secret = "sk-proj-" + "b" * 32
    response = asyncio.run(
        interface.generate(
            f"Use {secret} carefully",
            provider="fake",
            model="fake-model",
            task="summary",
        )
    )

    assert response.error is None
    assert secret not in prompts[0]
    payload = build_llm_usage_status(db)
    assert payload["call_count"] == 1
    assert payload["totals_by_task"][0]["task"] == "summary"
    assert payload["recent_expensive_runs"][0]["total_tokens_estimate"] == 17

    with db._get_connection() as conn:
        stored = json.dumps(
            [dict(row) for row in conn.execute("SELECT * FROM llm_usage_events")],
            ensure_ascii=False,
        )
    assert secret not in stored


def test_llm_cache_hit_records_zero_cost_usage(tmp_path: Path, monkeypatch):
    db = MetadataDB(str(tmp_path / "meta.db"))

    import core.llm_usage as llm_usage

    monkeypatch.setattr(llm_usage, "get_metadata_db", lambda: db)

    cache = LLMCache(str(tmp_path / "cache"))
    cache.set("content", "tags", {"tags": ["security"]}, "fake:fake-model")

    cached = cache.get("content", "tags", "fake:fake-model")

    assert cached == {"tags": ["security"]}
    payload = build_llm_usage_status(db)
    assert payload["call_count"] == 1
    assert payload["cache_hits"] == 1
    assert payload["total_cost_estimate_usd"] == 0.0
    assert payload["totals_by_task"][0]["task"] == "tags"


def test_llm_usage_schema_is_created_once_per_database(tmp_path: Path, monkeypatch):
    db = MetadataDB(str(tmp_path / "meta.db"))

    import core.llm_usage as llm_usage

    llm_usage._LLM_USAGE_SCHEMA_READY.clear()
    original_ensure = llm_usage.ensure_llm_usage_schema
    ensure_calls = []

    def counted_ensure(metadata_db):
        ensure_calls.append(str(metadata_db.db_path))
        original_ensure(metadata_db)

    monkeypatch.setattr(llm_usage, "ensure_llm_usage_schema", counted_ensure)

    for operation in ("first", "second"):
        event = record_llm_usage(
            provider="fake",
            model="fake-model",
            task="summary",
            operation=operation,
            input_text="short prompt",
            output_text="short result",
            db=db,
        )
        assert event is not None

    payload = build_llm_usage_status(db)

    assert payload["call_count"] == 2
    assert ensure_calls == [str(db.db_path)]


def test_llm_cache_info_uses_stored_task_and_model_metadata(tmp_path: Path):
    cache = LLMCache(str(tmp_path / "cache"))

    cache.set(
        "transcript content",
        "transcript_fmt",
        {"text": "formatted", "summary": "summary", "tags": "alpha"},
        "fake:transcript-model",
    )

    info = cache.get_cache_info()

    assert info["task_type_counts"] == {"transcript_fmt": 1}
    assert info["model_counts"] == {"fake:transcript-model": 1}
    assert info["recent_entries"][0]["task_type"] == "transcript_fmt"
    assert info["recent_entries"][0]["model"] == "fake:transcript-model"
