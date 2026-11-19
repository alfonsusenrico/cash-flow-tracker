"""Accounts learned per item across capture phrases, the model's item group and bare clock times
(learn-accounts-by-item). All texts, names and amounts are synthetic; provider calls are mocked.
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx2
import pytest

from app.services.capture_normalization import CaptureItemGroup, CaptureNormalization, validated_item_group
from app.services.capture_parsing import parse_capture
from app.services.capture_time import parse_time, time_word_spans
from app.services.item_learning import first_item_word
from app.services.openai_notification_provider import OpenAINotificationProvider
from app.services.prompt_language import load_prompt
from test_notification_processing_database import notification_owner  # noqa: F401 - fixture
from test_openai_notification_provider import envelope
from test_text_capture import (  # noqa: F401 - fixture
    NO_TIME, captured, owner, provider_with, query, remember, use_provider,
)

JAKARTA = timezone(timedelta(hours=7))
TZ = "Asia/Jakarta"


def parking_result(owner, *, group=None, span=""):
    return CaptureNormalization.model_validate_json(json.dumps({
        "merchant": None, "description": "Parkir", "type": "expense", "type_confidence": 0.95,
        "category_id": owner["parkir"], "kakeibo": "need", "category_confidence": 0.9, "account_id": None,
        "account_confidence": 0.0, "time": NO_TIME, "tags": [], "item_group": {"group": group, "span": span},
    }))


def record(client, text):
    capture = captured(client, text)
    assert capture["status"] == "recorded", capture["question"]
    return capture


def stored_groups(owner):
    rows = query("SELECT match_key, item_group, source FROM capture_item_groups WHERE user_id = %s ORDER BY match_key",
                 (owner["user_id"],))
    return {row["match_key"]: (row["item_group"], row["source"]) for row in rows}


def parking_history(client, accounts):
    places = ["ptc", "k24", "groove", "stasiun", "pasar", "kampus"]
    for place, account in zip(places, accounts):
        record(client, f"parkir {place} 2rb {account}")


def last_context(provider):
    return provider.normalize_capture.await_args.args[0]


# --- Item groups ---------------------------------------------------------------------------------


@pytest.mark.parametrize("phrase, word", [
    ("20 45 parkir ptc", "parkir"), ("07 55 e money", "money"), ("parkir", "parkir"), ("bayar k24 parkir", "parkir"),
    ("potong rambut", "potong"), ("20 45", None), ("", None),
])
def test_first_word_skips_numbers_one_letter_words_and_fillers(phrase, word):
    assert first_item_word(phrase) == word


def test_a_new_parking_place_joins_the_group_without_the_model(client, owner, monkeypatch):
    provider = use_provider(monkeypatch, provider_with(parking_result(owner, group="lenmarc", span="lenmarc")))
    parking_history(client, ["tunai", "tunai"])
    capture = record(client, "parkir lenmarc 5rb")
    assert "item_groups" not in last_context(provider)
    assert capture["transaction"]["account_id"] == owner["cash"]
    assert stored_groups(owner)["merchant:parkir lenmarc"] == ("parkir", "word")


def test_different_wording_joins_the_group_the_model_names_and_keeps_it(client, owner, monkeypatch):
    provider = use_provider(monkeypatch, provider_with(parking_result(owner, group="parkir", span="parkiran")))
    parking_history(client, ["tunai", "tunai"])
    record(client, "parkiran mall 5rb tunai")
    assert last_context(provider)["item_groups"] == [{"group": "parkir", "count": 2}]
    assert stored_groups(owner)["merchant:parkiran mall"] == ("parkir", "ai")
    # Without the phrase's memory the model is asked again, now without the owner's groups.
    query("DELETE FROM merchant_memory WHERE user_id = %s RETURNING match_key", (owner["user_id"],))
    provider.normalize_capture.return_value = parking_result(owner, group="mall", span="mall")
    record(client, "parkiran mall 7rb tunai")
    assert provider.normalize_capture.await_count == 4
    assert "item_groups" not in last_context(provider)
    assert stored_groups(owner)["merchant:parkiran mall"] == ("parkir", "ai")


def test_an_unproven_group_falls_back_to_the_first_word(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(parking_result(owner, group="transportasi", span="parkiran")))
    parking_history(client, ["tunai"])
    record(client, "parkiran mall 5rb tunai")
    assert stored_groups(owner)["merchant:parkiran mall"] == ("parkiran", "word")


def test_owner_with_no_groups_gets_an_empty_list_and_the_group_is_stored_once(client, owner, monkeypatch):
    provider = use_provider(monkeypatch, provider_with(parking_result(owner)))
    record(client, "parkir ptc 2rb tunai")
    assert last_context(provider)["item_groups"] == []
    assert stored_groups(owner) == {"merchant:parkir ptc": ("parkir", "word")}
    record(client, "parkir ptc 3rb tunai")
    assert stored_groups(owner) == {"merchant:parkir ptc": ("parkir", "word")}


def test_undo_removes_the_group_its_capture_stored(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(parking_result(owner, group="parkir", span="parkiran")))
    parking_history(client, ["tunai"])
    capture = record(client, "parkiran mall 5rb tunai")
    response = client.post(f"/api/capture/{capture['id']}/undo")
    assert response.status_code == 200, response.text
    assert "merchant:parkiran mall" not in stored_groups(owner)
    assert stored_groups(owner)["merchant:parkir ptc"] == ("parkir", "word")


def model_group(group, span, *, text="parkiran mall 5rb", item_groups=None):
    proposed = CaptureItemGroup(group=group, span=span)
    return validated_item_group(proposed, {"text": text, "item_groups": item_groups or []})


def test_model_group_checks():
    groups = [{"group": "parkir", "count": 3}]
    assert model_group("parkir", "parkiran", item_groups=groups) == "parkir"
    assert model_group("Parkiran", "parkiran mall") == "parkiran"
    assert model_group("parkir", "parkiran") is None
    assert model_group("parkiran", "parkiran mobil") is None
    assert model_group("parkiran", "") is None
    assert model_group(None, "parkiran") is None
    assert model_group("parkiran mall 5rb x", "parkiran") is None
    assert model_group("5rb", "5rb") is None
    assert model_group("parkir 20", "parkir", text="parkir 20 45 5rb") is None
    assert model_group("potong rambut", "potong rambut", text="tip potong rambut 10rb") == "potong rambut"


def test_older_model_output_without_an_item_group_still_validates():
    result = CaptureNormalization.model_validate_json(json.dumps({
        "merchant": None, "description": "Kopi", "type": "expense", "type_confidence": 0.9, "category_id": None,
        "kakeibo": None, "category_confidence": 0.5, "account_id": None, "account_confidence": 0, "time": NO_TIME,
    }))
    assert result.item_group == CaptureItemGroup(group=None, span="")
    assert validated_item_group(result.item_group, {"text": "kopi 5rb"}) is None


# --- Accounts by item ----------------------------------------------------------------------------


def test_parking_is_paid_in_cash(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(parking_result(owner)))
    parking_history(client, ["tunai"] * 6)
    capture = captured(client, "parkir 10rb")
    assert capture["status"] == "recorded" and capture["question"] is None
    assert capture["transaction"]["account_id"] == owner["cash"]


def test_mixed_history_asks_with_the_leading_account_preselected(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(parking_result(owner)))
    parking_history(client, ["tunai", "gopay", "tunai", "gopay", "tunai"])
    capture = captured(client, "parkir 10rb")
    question = capture["question"]
    assert (question["type"], question["suggested_account_id"]) == ("account", owner["cash"])
    assert question["options"][0]["id"] == owner["cash"]


def test_a_correction_counts_twice(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(parking_result(owner)))
    moved = record(client, "parkir ptc 2rb gopay")
    record(client, "parkir k24 2rb gopay")
    record(client, "parkir groove 2rb gopay")
    edited = client.patch(f"/api/transactions/{moved['transaction']['id']}", json={"account_id": owner["cash"]})
    assert edited.status_code == 200, edited.text
    capture = captured(client, "parkir 10rb")
    assert capture["question"]["suggested_account_id"] == owner["cash"]


def test_a_named_account_wins(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(parking_result(owner)))
    parking_history(client, ["tunai"] * 6)
    capture = captured(client, "parkir 10rb gopay")
    assert capture["status"] == "recorded" and capture["transaction"]["account_id"] == owner["gopay"]


def test_two_named_accounts_ask_with_the_item_account_preselected(client, owner, monkeypatch):
    from app.db.pool import db_conn

    pocket = str(uuid4())
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO accounts (id, user_id, name, type, parent_id, initial_balance)
               VALUES (%s, %s, 'Potong Rambut', 'bank', %s, 0)""",
            (pocket, owner["user_id"], owner["jago"]),
        )
        conn.commit()
    use_provider(monkeypatch, provider_with(parking_result(owner)))
    record(client, "tip 5rb tunai")
    record(client, "tip barista 3rb tunai")
    capture = captured(client, "cash 10rb buat tip potong rambut")
    question = capture["question"]
    assert question["type"] == "account" and {owner["cash"], pocket} <= {item["id"] for item in question["options"]}
    assert question["suggested_account_id"] == owner["cash"] and question["options"][0]["id"] == owner["cash"]


def test_without_item_evidence_the_question_carries_no_suggestion(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(parking_result(owner)))
    capture = captured(client, "parkir 10rb")
    assert capture["question"]["type"] == "account" and "suggested_account_id" not in capture["question"]


# --- Preview -------------------------------------------------------------------------------------


def preview(client, text):
    response = client.post("/api/capture/parse", json={"text": text, "device_time": datetime.now(timezone.utc).isoformat()})
    assert response.status_code == 200, response.text
    return response.json()["preview"]


def test_preview_shows_the_learned_account_from_habit_without_the_model(client, owner, monkeypatch):
    provider = use_provider(monkeypatch, provider_with(parking_result(owner)))
    parking_history(client, ["tunai"] * 6)
    calls = provider.normalize_capture.await_count
    assert preview(client, "parkir 10rb")["account_hint"] == {"account_id": owner["cash"], "label": "Tunai",
                                                               "basis": "habit"}
    assert preview(client, "parkir 10rb gopay")["account_hint"] == {"account_id": owner["gopay"], "label": "GoPay",
                                                                     "basis": "mention"}
    assert preview(client, "bensin 10rb")["account_hint"] is None
    assert provider.normalize_capture.await_count == calls


def test_preview_gives_no_hint_for_a_weak_habit(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(parking_result(owner)))
    parking_history(client, ["tunai", "gopay", "tunai"])
    assert preview(client, "parkir 10rb")["account_hint"] is None


# --- Bare clock time after a day word ------------------------------------------------------------


def test_yesterday_at_a_bare_clock_time():
    facts = parse_capture("kemarin 20.45 bayar parkir ptc cash 10rb", datetime(2026, 11, 18, 9, 0, tzinfo=JAKARTA),
                          accounts=[], aliases=[], tz=TZ)
    assert facts.date == datetime(2026, 11, 17, 20, 45, tzinfo=JAKARTA)
    assert (facts.phrase, facts.amount, facts.time["evidence"]) == ("parkir ptc", 10_000, "kemarin 20.45")


@pytest.mark.parametrize("text, expected", [
    ("tadi 21.15 sate", datetime(2026, 11, 18, 21, 15, tzinfo=JAKARTA)),
    ("semalam 23.15 sate", datetime(2026, 11, 17, 23, 15, tzinfo=JAKARTA)),
    ("hari ini 07:55 e money", datetime(2026, 11, 18, 7, 55, tzinfo=JAKARTA)),
    ("sabtu 19.00 nonton", datetime(2026, 11, 14, 19, 0, tzinfo=JAKARTA)),
    ("kemarin lusa 13.05 bensin", datetime(2026, 11, 16, 13, 5, tzinfo=JAKARTA)),
])
def test_bare_clock_times_after_day_words(text, expected):
    reading = parse_time(text, datetime(2026, 11, 18, 22, 0, tzinfo=JAKARTA), TZ)
    assert (reading.status, reading.moment) == ("resolved", expected)


@pytest.mark.parametrize("text, status, expected", [
    ("parkir 20.45", "none", datetime(2026, 11, 18, 22, 0, tzinfo=JAKARTA)),
    ("minggu 10.15 lari", "none", datetime(2026, 11, 18, 22, 0, tzinfo=JAKARTA)),
    ("kemarin 10.000 parkir", "resolved", datetime(2026, 11, 17, 22, 0, tzinfo=JAKARTA)),
    ("kemarin 24.10 parkir", "resolved", datetime(2026, 11, 17, 22, 0, tzinfo=JAKARTA)),
])
def test_a_number_that_is_no_clock_time_after_a_day_word_is_not_read(text, status, expected):
    reading = parse_time(text, datetime(2026, 11, 18, 22, 0, tzinfo=JAKARTA), TZ)
    assert (reading.status, reading.moment) == (status, expected)


def test_bare_clock_time_is_a_time_word():
    text = "kemarin 20.45 parkir"
    assert "kemarin 20.45" in {text[start:end] for start, end in time_word_spans(text)}


def test_typed_capture_with_a_bare_clock_time(client, owner):
    remember(owner, "merchant:parkir ptc", category="parkir", account=None, notes="Parkir PTC")
    sent = datetime.now(timezone.utc)
    capture = captured(client, "kemarin 20.45 bayar parkir ptc cash 10rb", device_time=sent)
    assert capture["status"] == "recorded" and capture["transaction"]["amount"] == 10_000
    local = datetime.fromisoformat(capture["transaction"]["date"]).astimezone(ZoneInfo(TZ))
    yesterday = sent.astimezone(ZoneInfo(TZ)).date() - timedelta(days=1)
    assert (local.date(), local.hour, local.minute) == (yesterday, 20, 45)
    stored = query("SELECT match_key FROM text_captures WHERE id = %s", (capture["id"],))[0]
    assert stored["match_key"] == "merchant:parkir ptc"


# --- Reasoning effort ----------------------------------------------------------------------------


def test_capture_reasoning_effort_setting(monkeypatch):
    from app.core.config import load_settings

    monkeypatch.setenv("NOTIFICATION_AI_REASONING_EFFORT", "none")
    monkeypatch.delenv("CAPTURE_AI_REASONING_EFFORT", raising=False)
    assert load_settings().capture_ai_reasoning_effort == "none"
    monkeypatch.setenv("CAPTURE_AI_REASONING_EFFORT", "")
    assert load_settings().capture_ai_reasoning_effort == "none"
    monkeypatch.setenv("CAPTURE_AI_REASONING_EFFORT", " Low ")
    current = load_settings()
    assert (current.capture_ai_reasoning_effort, current.notification_ai_reasoning_effort) == ("low", "none")


def test_capture_call_uses_the_capture_reasoning_effort():
    efforts = []
    output = {"merchant": None, "description": "Kopi", "type": "expense", "type_confidence": 0.9,
              "category_id": None, "kakeibo": None, "category_confidence": 0.5, "account_id": None,
              "account_confidence": 0, "time": NO_TIME, "tags": [], "item_group": {"group": "kopi", "span": "kopi"}}

    def handler(request):
        body = json.loads(request.content)
        efforts.append(body["reasoning"]["effort"])
        is_capture = body["text"]["format"]["name"] == "capture_normalization"
        return httpx2.Response(200, json=envelope(output if is_capture else None))

    async def run():
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as transport:
            provider = OpenAINotificationProvider(transport, api_key="synthetic-test-credential", model="gpt-6-luna",
                                                  reasoning_effort="none", capture_reasoning_effort="low")
            await provider.normalize_capture({"text": "kopi 5rb"}, prompt=load_prompt("capture_normalization", "id"))
            await provider.interpret({"text": "synthetic"}, prompt=load_prompt("notification_interpretation", "id"))
            following = OpenAINotificationProvider(transport, api_key="synthetic-test-credential", model="gpt-6-luna",
                                                   reasoning_effort="low")
            await following.normalize_capture({"text": "kopi 5rb"}, prompt=load_prompt("capture_normalization", "id"))

    asyncio.run(run())
    assert efforts == ["low", "none", "low"]
