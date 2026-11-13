"""Capture times end to end: parsed, asked, answered, read by the mocked model, stored and edited.

All texts, names and amounts are synthetic. Provider calls are mocked; no model is reached.
"""
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from test_notification_processing_database import ingest, notification, notification_owner  # noqa: F401 - fixture
from test_text_capture import (  # noqa: F401 - fixture
    answer, answered, capture_rows, model_result, owner, provider_with, query, remember, use_provider,
)

JAKARTA = timezone(timedelta(hours=7))
ALL_TYPES = ["amount", "kind", "account", "category", "time"]


def at(day, hour, minute=0):
    return datetime(2026, 10, day, hour, minute, tzinfo=JAKARTA)


def capture(client, text, device_time, question_types=ALL_TYPES):
    body = {"client_capture_id": str(uuid4()), "text": text, "device_time": device_time.isoformat()}
    if question_types is not None:
        body["question_types"] = question_types
    response = client.post("/api/capture/text", json=body)
    assert response.status_code == 200, response.text
    return response.json()["capture"]


def stored_time(owner):
    return [(row["date"].astimezone(JAKARTA), row["time_basis"], row["time_evidence"]) for row in capture_rows(owner)]


@pytest.fixture
def parking(owner):
    remember(owner, "merchant:parkir ptc", category="parkir", account="cash", notes="Parkir PTC", merchant="Parkir PTC")
    return owner


def test_owner_example_records_two_in_the_afternoon(client, parking):
    recorded = capture(client, "bayar parkir di ptc tadi jam 2 10rb", at(29, 15, 3))
    assert recorded["status"] == "recorded"
    assert stored_time(parking) == [(at(29, 14), "parsed", "tadi jam 2")]
    summary = recorded["summary"]
    assert (summary["time_label"], summary["time_basis"], summary["time_evidence"]) == (
        "Hari ini 14.00", "parsed", "tadi jam 2")
    assert datetime.fromisoformat(summary["date"]) == at(29, 14)
    transaction = recorded["transaction"]
    assert (transaction["time_basis"], transaction["time_evidence"], transaction["amount"]) == (
        "parsed", "tadi jam 2", 10_000)


def test_text_without_time_words_keeps_the_device_time(client, parking):
    recorded = capture(client, "parkir ptc 10rb", at(29, 15, 3))
    assert stored_time(parking) == [(at(29, 15, 3), "device_time", None)]
    assert recorded["summary"]["time_label"] == "Hari ini 15.03"


def test_ambiguous_hour_is_asked_and_answered_with_a_chip(client, parking):
    pending = capture(client, "parkir ptc jam 2 10rb", at(29, 10))
    question = pending["question"]
    assert (pending["status"], question["type"], question["prompt"], question["allows_text"]) == (
        "needs_detail", "time", "Jam berapa?", True)
    assert [option["label"] for option in question["options"]] == ["Hari ini 02.00", "Kemarin 14.00"]
    assert pending["summary"]["date"] is None and pending["summary"]["time_label"] is None
    assert capture_rows(parking) == []

    assert answer(client, pending, account_id=parking["cash"]).status_code == 422
    assert answer(client, pending, option_id="t9").status_code == 422
    recorded = answered(client, pending, option_id="t2")
    assert recorded["status"] == "recorded"
    assert stored_time(parking) == [(at(28, 14), "owner", None)]
    assert recorded["summary"]["time_label"] == "Kemarin 14.00"
    assert recorded["transaction"]["classified_by"] == "rule"


def test_typed_answer_is_read_with_the_open_phrase(client, parking):
    pending = capture(client, "kemarin jam 8 parkir ptc 10rb", at(29, 9))
    assert [option["label"] for option in pending["question"]["options"]] == ["Kemarin 20.00", "Kemarin 08.00"]
    assert answer(client, pending, text="entahlah").status_code == 422
    assert answer(client, pending, text="besok jam 8").status_code == 422
    answered(client, pending, text="malam")
    assert stored_time(parking) == [(at(28, 20), "owner", None)]


def test_time_is_asked_after_the_amount(client, parking):
    pending = capture(client, "parkir ptc jam 2", at(29, 10))
    assert pending["question"]["type"] == "amount"
    pending = answered(client, pending, text="10rb")
    assert pending["question"]["type"] == "time"
    answered(client, pending, option_id="t1")
    assert stored_time(parking) == [(at(29, 2), "owner", None)]


def test_last_week_asks_the_day(client, parking):
    pending = capture(client, "minggu lalu parkir ptc 10rb", at(29, 15))
    question = pending["question"]
    assert (question["type"], question["prompt"], question["reason"]) == ("time", "Hari apa?", "ambiguous_day")
    assert [option["label"] for option in question["options"]][:2] == ["Min 25 Okt 15.00", "Sab 24 Okt 15.00"]
    answered(client, pending, text="senin")
    assert stored_time(parking) == [(at(19, 15), "owner", None)]


def test_older_phone_without_time_questions_keeps_todays_behaviour(client, parking):
    capture(client, "parkir ptc jam 2 10rb", at(29, 10), question_types=None)
    capture(client, "kemarin jam 8 parkir ptc 10rb", at(29, 9), question_types=None)
    assert sorted(stored_time(parking)) == [(at(28, 9), "parsed", "kemarin jam 8"), (at(29, 10), "device_time", None)]


def test_invalid_question_types_are_rejected(client, owner):
    body = {"client_capture_id": str(uuid4()), "text": "parkir 10rb", "device_time": at(29, 10).isoformat(),
            "question_types": ["amount", "photo"]}
    assert client.post("/api/capture/text", json=body).json()["detail"]["code"] == "invalid_question_types"


def time_result(owner, span, moment, confidence):
    return model_result(owner, category="parkir", kakeibo="need", description="Parkir PTC",
                        time={"span": span, "local_datetime": moment, "confidence": confidence})


def test_model_reads_a_flagged_phrase_even_when_memory_classifies(client, parking, monkeypatch):
    provider = use_provider(monkeypatch, provider_with(
        time_result(parking, "abis maghrib", "2026-10-29T18:15:00+07:00", 0.9)))
    recorded = capture(client, "parkir ptc abis maghrib 10rb", at(29, 20))
    assert recorded["status"] == "recorded"
    provider.normalize_capture.assert_awaited_once()
    context = provider.normalize_capture.await_args.args[0]
    assert context["facts"]["time_phrase"] == "abis maghrib" and context["facts"]["time_day"] is None
    assert context["facts"]["sent_at"] == "2026-10-29T20:00+07:00"
    assert stored_time(parking) == [(at(29, 18, 15), "model", "abis maghrib")]
    row = capture_rows(parking)[0]
    assert (row["amount"], row["classified_by"]) == (10_000, "rule")
    stored = query("SELECT classifier, model_calls, prompt_version FROM text_captures WHERE id = %s",
                   (recorded["id"],))[0]
    assert dict(stored) == {"classifier": "memory", "model_calls": 1, "prompt_version": "capture-normalization-2"}


@pytest.mark.parametrize("span, moment, confidence, first_label", [
    ("abis maghrib", "2026-10-29T18:15:00+07:00", 0.5, "Hari ini 18.15"),
    ("abis maghrib", "2026-10-29T21:15:00+07:00", 0.9, "Hari ini 20.00"),
    ("bayar parkir", "2026-10-29T18:15:00+07:00", 0.9, "Hari ini 20.00"),
    ("abis maghrib", "2026-09-01T18:15:00+07:00", 0.9, "Hari ini 20.00"),
])
def test_rejected_model_time_asks(client, parking, monkeypatch, span, moment, confidence, first_label):
    use_provider(monkeypatch, provider_with(time_result(parking, span, moment, confidence)))
    pending = capture(client, "parkir ptc abis maghrib 10rb", at(29, 20))
    assert pending["question"]["type"] == "time"
    assert pending["question"]["options"][0]["label"] == first_label
    assert capture_rows(parking) == []


def test_unavailable_model_asks_the_time(client, parking):
    pending = capture(client, "parkir ptc abis maghrib 10rb", at(29, 20))
    assert (pending["question"]["type"], pending["question"]["reason"]) == ("time", "unresolved")
    answered(client, pending, text="jam 6 sore")
    assert stored_time(parking) == [(at(29, 18), "owner", None)]


def test_model_never_changes_the_amount(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(model_result(owner, category="parkir", kakeibo="need",
                                                         description="Parkir 99rb", time={
                                                             "span": "abis maghrib",
                                                             "local_datetime": "2026-10-29T18:15:00+07:00",
                                                             "confidence": 0.95})))
    recorded = capture(client, "parkir abis maghrib 10rb tunai", at(29, 20))
    assert recorded["status"] == "recorded" and recorded["transaction"]["amount"] == 10_000
    assert capture_rows(owner)[0]["amount"] == 10_000


def test_both_legs_of_a_typed_movement_carry_the_time(client, owner):
    recorded = capture(client, "tarik tunai 200rb dari bca harian kemarin jam 2", at(29, 9))
    assert recorded["status"] == "recorded" and recorded["movement"]
    assert stored_time(owner) == [(at(28, 14), "parsed", "kemarin jam 2")] * 2


def test_owner_date_edit_makes_the_time_the_owners(client, parking):
    recorded = capture(client, "bayar parkir di ptc tadi jam 2 10rb", at(29, 15))
    transaction_id = recorded["transaction"]["id"]
    notes = client.patch(f"/api/transactions/{transaction_id}", json={"notes": "Parkir PTC lantai 2"})
    assert notes.status_code == 200, notes.text
    assert stored_time(parking) == [(at(29, 14), "parsed", "tadi jam 2")]
    edited = client.patch(f"/api/transactions/{transaction_id}", json={"date": at(29, 13).isoformat()})
    assert edited.status_code == 200, edited.text
    assert stored_time(parking) == [(at(29, 13), "owner", None)]


def test_notification_rows_keep_their_post_time(client, notification_owner):
    payload = notification("You've paid Rp125.000 to Parkir Kemarin Jam 2 using your Kantong Utama Pocket")
    result = ingest(client, payload)["results"][0]
    assert result["status"] == "recorded"
    row = query("SELECT date, time_basis, time_evidence FROM transactions WHERE id = %s",
                (str(UUID(result["transaction_id"])),))[0]
    assert row["date"] == datetime.fromisoformat(payload["post_time"])
    assert (row["time_basis"], row["time_evidence"]) == (None, None)


def test_database_rejects_unknown_time_basis_and_stray_evidence(client, owner):
    import psycopg
    from app.db.pool import db_conn
    insert = """INSERT INTO transactions (user_id, account_id, type, amount, notes, source, review_status,
                                          classified_by, time_basis, time_evidence)
                VALUES (%s, %s, 'expense', 1000, 'Contoh', 'capture', 'reviewed', 'owner', %s, %s)"""
    with db_conn() as conn:
        conn.execute(insert, (owner["user_id"], owner["cash"], "model", "abis maghrib"))
        conn.rollback()
        for basis, evidence in [("guess", None), ("owner", "jam 2"), ("device_time", "jam 2")]:
            with pytest.raises(psycopg.errors.CheckViolation):
                conn.execute(insert, (owner["user_id"], owner["cash"], basis, evidence))
            conn.rollback()
