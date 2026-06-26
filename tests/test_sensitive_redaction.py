import json

from core.llm_cache import LLMCache
from core.sensitive_redaction import redact_sensitive_text


def test_redacts_common_credentials_and_private_pii():
    source = "\n".join(
        [
            "OPENAI_API_KEY=sk-proj-" + "a" * 32,
            "Authorization: Bearer " + "b" * 32,
            "AWS key AKIAIOSFODNN7EXAMPLE",
            "email ada.person@private.test",
            "phone +1 (202) 555-0188",
        ]
    )

    result = redact_sensitive_text(source)

    assert "sk-proj-" not in result.redacted_text
    assert "Bearer " + "b" * 32 not in result.redacted_text
    assert "AKIAIOSFODNN7EXAMPLE" not in result.redacted_text
    assert "ada.person@private.test" not in result.redacted_text
    assert "(202) 555-0188" not in result.redacted_text
    assert "OPENAI_API_KEY=[[REDACTED_ENV_SECRET_1]]" in result.redacted_text
    assert result.to_metadata()["categories"] == {
        "env_secret": 1,
        "bearer_token": 1,
        "api_key": 1,
        "email": 1,
        "phone": 1,
    }


def test_redaction_avoids_example_email_false_positive():
    result = redact_sensitive_text("Contact demo@example.com for sample docs.")

    assert result.redacted_text == "Contact demo@example.com for sample docs."
    assert result.findings == ()


def test_llm_cache_uses_redacted_key_material_and_metadata(tmp_path):
    cache = LLMCache(str(tmp_path))
    content = "TOKEN=" + "c" * 32

    cache.set(content, "summary", {"summary": "ok"}, "fake:model")
    cached = cache.get("TOKEN=" + "d" * 32, "summary", "fake:model")

    assert cached == {"summary": "ok"}
    cache_files = list(tmp_path.glob("*.json"))
    assert len(cache_files) == 1
    payload = json.loads(cache_files[0].read_text(encoding="utf-8"))
    serialized = json.dumps(payload)
    assert "c" * 32 not in serialized
    assert payload["redaction"]["categories"] == {"env_secret": 1}
