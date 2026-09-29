import asyncio
import json
from dataclasses import replace
from unittest.mock import MagicMock

import httpx2
import pytest

from app.core.config import load_settings
from app.services.notification_context import load_context, sanitize_text
from app.services.notification_evidence import collect_facts
from app.services.openai_notification_provider import OpenAINotificationProvider, ProviderError, configuration_error
from test_notification_interpretation import event, proposal


def run_provider(handler, *, model="gpt-5.6-luna", reasoning_effort="low", context=None):
    async def run():
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as transport:
            provider = OpenAINotificationProvider(
                transport,
                api_key="synthetic-test-credential",
                model=model,
                reasoning_effort=reasoning_effort,
            )
            return await provider.interpret(context or {"notification": "Synthetic payment"})
    return asyncio.run(run())


def envelope(data=None, *, status="completed"):
    return {
        "id": "resp_test",
        "object": "response",
        "created_at": 0,
        "status": status,
        "model": "gpt-5.6-luna",
        "output": [{
            "id": "msg_test",
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": json.dumps(data or proposal()), "annotations": []}],
        }],
    }


def test_provider_uses_toolless_strict_stored_false_responses_request():
    def handler(request):
        assert str(request.url) == "https://api.openai.com/v1/responses"
        payload = json.loads(request.content)
        assert payload["model"] == "gpt-5.6-luna"
        assert payload["reasoning"] == {"effort": "low"}
        assert payload["store"] is False
        assert payload["text"]["format"]["type"] == "json_schema"
        assert payload["text"]["format"]["strict"] is True
        assert "tools" not in payload
        return httpx2.Response(200, json=envelope())
    assert run_provider(handler).description == "Bayar Kedai Awan"


@pytest.mark.parametrize("status,code,transient", [
    (401, "provider_access_denied", False), (403, "provider_access_denied", False),
    (400, "provider_request_rejected", False), (429, "provider_unavailable", True),
    (500, "provider_unavailable", True), (503, "provider_unavailable", True),
])
def test_safe_provider_errors_and_retry_after(status, code, transient):
    def handler(request):
        return httpx2.Response(status, headers={"Retry-After": "17"}, text="private echoed request content")
    with pytest.raises(ProviderError) as caught:
        run_provider(handler)
    assert caught.value.code == code
    assert caught.value.transient is transient
    assert "private" not in str(caught.value)
    if transient:
        assert caught.value.retry_after == 17


@pytest.mark.parametrize("response", [
    httpx2.Response(200, json=envelope(data="not JSON")),
    httpx2.Response(200, json={}),
    httpx2.Response(200, json=envelope(status="incomplete")),
    httpx2.Response(200, json=envelope({"invalid": "schema"})),
])
def test_invalid_outputs_are_not_repaired(response):
    with pytest.raises(ProviderError, match="provider_invalid_output"):
        run_provider(lambda request: response)


def test_transport_timeout_is_safe_and_retriable():
    def handler(request):
        raise httpx2.ReadTimeout("private request", request=request)
    with pytest.raises(ProviderError) as caught:
        run_provider(handler)
    assert caught.value.transient
    assert str(caught.value) == "provider_timeout"


def test_usage_is_accounted_even_when_output_is_incomplete():
    response = envelope(status="incomplete")
    response["usage"] = {"input_tokens": 120, "output_tokens": 40, "total_tokens": 160}

    async def run():
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(lambda request: httpx2.Response(200, json=response))) as transport:
            provider = OpenAINotificationProvider(transport, api_key="synthetic-test-credential",
                                                   model="gpt-5.6-luna", reasoning_effort="low")
            with pytest.raises(ProviderError, match="provider_invalid_output"):
                await provider.interpret({"notification": "Synthetic payment"})
            assert provider.last_usage == response["usage"]

    asyncio.run(run())


@pytest.mark.parametrize("model", ["gpt-5.6-terra", "gpt-5", "random-model"])
def test_operational_models_rejected_before_network(model):
    def handler(request):
        pytest.fail("Forbidden model sent a network request")
    with pytest.raises(ProviderError, match="provider_model_not_permitted"):
        run_provider(handler, model=model)


@pytest.mark.parametrize("reasoning_effort", ["medium", "high", "invalid"])
def test_unsupported_reasoning_is_rejected_before_network(reasoning_effort):
    with pytest.raises(ProviderError, match="provider_reasoning_not_permitted"):
        run_provider(lambda request: pytest.fail("Forbidden reasoning sent a network request"), reasoning_effort=reasoning_effort)


def test_missing_key_invalid_model_or_reasoning_do_not_break_settings(monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("NOTIFICATION_AI_ENABLED", raising=False)
    settings = load_settings()
    assert configuration_error(settings) == "ai_disabled"
    assert configuration_error(replace(settings, notification_ai_enabled=True)) == "provider_credential_missing"
    assert configuration_error(replace(settings, notification_ai_enabled=True, notification_ai_model="gpt-5")) == "provider_model_not_permitted"
    assert configuration_error(replace(settings, notification_ai_enabled=True, notification_ai_reasoning_effort="medium")) == "provider_reasoning_not_permitted"
    assert "synthetic-test-credential" not in repr(replace(settings, openai_api_key="synthetic-test-credential"))


@pytest.mark.parametrize("name,value", [
    ("NOTIFICATION_AI_TIMEOUT", "bad"), ("NOTIFICATION_AI_TIMEOUT", "nan"),
    ("NOTIFICATION_AI_TIMEOUT", "90"), ("NOTIFICATION_AI_HISTORY_LIMIT", "bad"),
    ("NOTIFICATION_AI_HISTORY_LIMIT", "-1"),
])
def test_invalid_numeric_processor_settings_do_not_break_api_startup(monkeypatch, name, value):
    monkeypatch.setenv("NOTIFICATION_AI_ENABLED", "true")
    monkeypatch.setenv(name, value)
    loaded = load_settings()
    assert configuration_error(loaded) == "processor_configuration_invalid"


def test_redaction_retains_amount_but_not_private_identifiers():
    text = "Raka Purnama has sent Rp500000 to you. rekening 1234567890 email fake@example.test Bearer fake-token otp=654321"
    sanitized = sanitize_text(text, "Raka Purnama")
    assert "[SELF] has sent Rp500000" in sanitized
    for sensitive in ("Raka Purnama", "1234567890", "fake@example.test", "fake-token", "654321"):
        assert sensitive not in sanitized


def test_context_is_owner_scoped_bounded_and_uses_separate_candidates():
    cur = MagicMock()
    cur.fetchone.return_value = {"name": "Raka Purnama"}
    cur.fetchall.side_effect = [
        [{"id": "owned", "name": "BCA Operasional", "type": "bank", "parent_id": None}],
        [{"id": "category", "name": "Kategori Dinamis", "kind": "expense"}],
        [], [{"description": "Toko Fiksi token=private-value", "id": "recent"}],
        [{"id": "outside-history", "type": "income", "amount": 125000}],
    ]
    result = load_context(cur, "owner", collect_facts(event()), history_limit=20)
    assert result["candidates"][0]["id"] == "outside-history"
    assert result["history"][0]["id"] == "recent"
    assert "private-value" not in json.dumps(result, default=str)
    assert "device_id" not in result and "raw_extras" not in result
    for call in cur.execute.call_args_list:
        assert call.args[1][0] == "owner"


def test_candidate_overflow_cannot_become_a_unique_match():
    cur = MagicMock()
    cur.fetchone.return_value = {}
    cur.fetchall.side_effect = [[], [], [], [], [{"id": str(index)} for index in range(51)]]
    result = load_context(cur, "owner", collect_facts(event()))
    assert result["incomplete"] is True
    assert len(result["candidates"]) == 50
