"""Model output language (localize-server-text D7, tasks 5.1 and 5.2). Offline: every provider call is mocked."""

import asyncio
import hashlib
import json
import socket
from dataclasses import replace
from uuid import UUID

import httpx2
import pytest

from app.core import config
from app.services.capture_normalization import CaptureNormalization, validate_capture_normalization
from app.services.history_suggestions import HistorySuggestionBatch, validated_proposals
from app.services.openai_notification_provider import OpenAINotificationProvider
from app.services.prompt_language import (
    CAPTURE_NORMALIZATION, HISTORY_SUGGESTIONS, INDONESIAN_VERSIONS, NOTIFICATION_INTERPRETATION,
    PURPOSE_NORMALIZATION, load_prompt, output_language, prompt_for_version, select_prompt,
)
from app.services.purpose_normalization import InvalidNormalization, PurposeNormalization, validate_normalization
from tests.golden_server_text import FIXTURE
from test_notification_interpretation import proposal

NAMES = (NOTIFICATION_INTERPRETATION, CAPTURE_NORMALIZATION, HISTORY_SUGGESTIONS, PURPOSE_NORMALIZATION)
GOLDEN_PROMPTS = json.loads(FIXTURE.read_text())["prompts"]
CATEGORY = "00000000-0000-4000-8000-000000000101"


@pytest.fixture
def english_output(monkeypatch):
    def set_flag(enabled: bool):
        monkeypatch.setattr(config, "settings", replace(config.settings, llm_english_output_enabled=enabled))
    return set_flag


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Any real socket connection fails the test: the provider below only ever talks to a mock transport."""
    def refuse(*_args, **_kwargs):
        raise AssertionError("network access attempted")
    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket, "create_connection", refuse)


@pytest.mark.parametrize("name", NAMES)
def test_indonesian_rendering_is_byte_identical_with_its_version(name):
    prompt = load_prompt(name, "id")
    assert hashlib.sha256(prompt.text.encode("utf-8")).hexdigest() == GOLDEN_PROMPTS[name]["sha256"]
    assert prompt.text == GOLDEN_PROMPTS[name]["text"]
    assert prompt.version == INDONESIAN_VERSIONS[name]
    assert prompt_for_version(prompt.version) == prompt


@pytest.mark.parametrize("name", NAMES)
def test_english_rendering_has_an_en_version_and_asks_for_english(name):
    prompt = load_prompt(name, "en")
    assert prompt.version == INDONESIAN_VERSIONS[name] + "-en"
    assert prompt_for_version(prompt.version) == prompt
    assert "English" in prompt.text and "untrusted DATA" in prompt.text
    # Input may still be described as Indonesian; the requested output never is.
    for phrase in ("Indonesian ledger description", "Indonesian transaction description", "Indonesian sentence"):
        assert phrase not in prompt.text
    assert '"Payment"' in prompt.text


def test_versions_are_the_ones_the_change_announced():
    assert {name: load_prompt(name, "en").version for name in NAMES} == {
        NOTIFICATION_INTERPRETATION: "notification-interpretation-12-en",
        CAPTURE_NORMALIZATION: "capture-normalization-2-en",
        HISTORY_SUGGESTIONS: "history-suggestions-1-en",
        PURPOSE_NORMALIZATION: "purpose-normalization-1-en",
    }
    with pytest.raises(ValueError):
        prompt_for_version("notification-interpretation-11")


@pytest.mark.parametrize("enabled,account,expected", [
    (False, "id", "id"), (False, "en", "id"), (True, "id", "id"), (True, "en", "en"),
])
def test_english_output_needs_the_flag_and_an_english_account(english_output, enabled, account, expected):
    english_output(enabled)
    assert output_language(account) == expected
    for name in NAMES:
        assert select_prompt(name, account).language == expected


def test_flag_defaults_to_off():
    assert config.load_settings().llm_english_output_enabled is False


def envelope(data):
    return {
        "id": "resp_test", "object": "response", "created_at": 0, "status": "completed", "model": "gpt-5.6-luna",
        "output": [{"id": "msg_test", "type": "message", "status": "completed", "role": "assistant",
                    "content": [{"type": "output_text", "text": json.dumps(data), "annotations": []}]}],
    }


OUTPUTS = {
    NOTIFICATION_INTERPRETATION: proposal(description="Coffee at Kedai Awan"),
    PURPOSE_NORMALIZATION: {"merchant": "Arena Futsal", "description": "Futsal court rental", "category_id": CATEGORY,
                            "kakeibo": "culture", "offset_category_id": None, "confidence": 0.8},
    CAPTURE_NORMALIZATION: {"merchant": None, "description": "Sweet iced tea", "type": "expense", "type_confidence": 0.9,
                            "category_id": CATEGORY, "kakeibo": "want", "category_confidence": 0.9, "account_id": None,
                            "account_confidence": 0, "time": {"span": None, "local_datetime": None, "confidence": 0}},
    HISTORY_SUGGESTIONS: {"suggestions": []},
}
METHODS = {
    NOTIFICATION_INTERPRETATION: "interpret", PURPOSE_NORMALIZATION: "normalize_purpose",
    CAPTURE_NORMALIZATION: "normalize_capture", HISTORY_SUGGESTIONS: "suggest_history",
}


def call_provider(name, prompt):
    sent = []

    def handler(request):
        sent.append(json.loads(request.content))
        return httpx2.Response(200, json=envelope(OUTPUTS[name]))

    async def run():
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as transport:
            provider = OpenAINotificationProvider(
                transport, api_key="synthetic-test-credential", model="gpt-5.6-luna", reasoning_effort="low",
            )
            return await getattr(provider, METHODS[name])({"synthetic": True}, prompt=prompt)

    result = asyncio.run(run())
    return result, sent


@pytest.mark.parametrize("name", NAMES)
@pytest.mark.parametrize("enabled,account", [(False, "en"), (True, "id"), (True, "en")])
def test_provider_sends_the_selected_rendering(english_output, name, enabled, account):
    english_output(enabled)
    prompt = select_prompt(name, account)
    result, sent = call_provider(name, prompt)
    assert len(sent) == 1, "exactly one request, to the mock transport"
    assert sent[0]["instructions"] == load_prompt(name, "en" if enabled and account == "en" else "id").text
    assert sent[0]["text"]["format"]["strict"] is True
    assert result is not None


def test_provider_refuses_a_prompt_of_another_call():
    with pytest.raises(ValueError):
        call_provider(PURPOSE_NORMALIZATION, load_prompt(CAPTURE_NORMALIZATION, "en"))


def test_english_output_still_validates_the_schema():
    from app.services.openai_notification_provider import ProviderError

    def handler(request):
        return httpx2.Response(200, json=envelope({"merchant": None, "description": "Coffee"}))

    async def run():
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as transport:
            provider = OpenAINotificationProvider(
                transport, api_key="synthetic-test-credential", model="gpt-5.6-luna", reasoning_effort="low",
            )
            return await provider.normalize_purpose({"synthetic": True}, prompt=load_prompt(PURPOSE_NORMALIZATION, "en"))

    with pytest.raises(ProviderError) as error:
        asyncio.run(run())
    assert error.value.code == "provider_invalid_output"


@pytest.mark.parametrize("placeholder", ["Payment", "Pembayaran", "transfer in", "Bank   transaction", "SHOPPING"])
def test_english_capture_drops_a_placeholder_of_either_language(placeholder):
    result = CaptureNormalization.model_validate_json(json.dumps({**OUTPUTS[CAPTURE_NORMALIZATION], "description": placeholder}))
    context = {"categories": [{"id": CATEGORY, "kind": "expense"}], "accounts": []}
    assert validate_capture_normalization(result, context, output_language="en")["description"] is None


@pytest.mark.parametrize("description,kept", [("Pembayaran", None), ("Payment", "Payment"), ("Shopping", "Shopping")])
def test_indonesian_capture_guard_is_unchanged(description, kept):
    result = CaptureNormalization.model_validate_json(json.dumps({**OUTPUTS[CAPTURE_NORMALIZATION], "description": description}))
    context = {"categories": [{"id": CATEGORY, "kind": "expense"}], "accounts": []}
    assert validate_capture_normalization(result, context)["description"] == kept


def purpose_result(description):
    return PurposeNormalization.model_validate_json(json.dumps({**OUTPUTS[PURPOSE_NORMALIZATION], "description": description}))


PURPOSE_CONTEXT = {"categories": [{"id": CATEGORY, "kind": "expense"}], "offset_categories": []}


@pytest.mark.parametrize("placeholder", ["Payment", "Spending", "Transaction"])
def test_english_purpose_answer_rejects_a_placeholder(placeholder):
    with pytest.raises(InvalidNormalization) as error:
        validate_normalization(purpose_result(placeholder), PURPOSE_CONTEXT, "expense", output_language="en")
    assert str(error.value) == "generic_description"
    checked = validate_normalization(purpose_result("Futsal court rental"), PURPOSE_CONTEXT, "expense", output_language="en")
    assert checked["notes"] == "Futsal court rental"


def test_indonesian_purpose_validation_is_unchanged():
    checked = validate_normalization(purpose_result("Pembayaran"), PURPOSE_CONTEXT, "expense", output_language="id")
    assert checked["notes"] == "Pembayaran"


@pytest.mark.parametrize("language,expected", [("en", "Pembayaran QRIS"), ("id", "Payment")])
def test_english_history_placeholder_keeps_the_rows_notes(language, expected):
    batch = HistorySuggestionBatch.model_validate_json(json.dumps({"suggestions": [{
        "ref": "r1", "merchant": None, "description": "Payment", "category_id": CATEGORY, "kakeibo": "want",
        "offset_category_id": None, "confidence": 0.9, "reason": "Same as your choice for Kedai Awan",
    }]}))
    row = {"id": UUID(int=7), "type": "expense", "merchant": None, "notes": "Pembayaran QRIS"}
    context = {"rows": [{"ref": "r1"}], "offset_categories": [],
               "categories": [{"id": CATEGORY, "kind": "expense", "offsets_spending": False, "default_kakeibo": "want"}]}
    valid, invalid = validated_proposals(batch, [row], context, output_language=language)
    assert invalid == 0 and valid[0].notes == expected
