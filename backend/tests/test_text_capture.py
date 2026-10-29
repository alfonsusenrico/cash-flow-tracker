"""Typed transaction capture: recording, questions, movements, undo, learning and boundaries.

All texts, names and amounts are synthetic. Provider calls are mocked; no model is reached.
"""
import asyncio
import json
import logging
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import uuid4
from zoneinfo import ZoneInfo

import httpx2
import psycopg
import pytest

from app.services.capture_normalization import (
    CaptureNormalization, InvalidCaptureNormalization, validate_capture_normalization,
)
from app.services.openai_notification_provider import OpenAINotificationProvider, ProviderError
from test_notification_processing_database import enable_ai, notification_owner  # noqa: F401 - fixture
from test_openai_notification_provider import envelope


@pytest.fixture
def owner(notification_owner):
    from app.db.pool import db_conn

    ids = dict(notification_owner)
    ids["cash"] = str(uuid4())
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, 'Tunai', 'cash', 1000000)",
            (ids["cash"], ids["user_id"]),
        )
        for key, name, kind, pillar in [
            ("parkir", "Parkir", "expense", "need"),
            ("makan", "Makanan & Minuman", "expense", "need"),
            ("jajan", "Jajan & Nongkrong", "expense", "want"),
            ("pulsa", "Pulsa & Internet", "expense", "need"),
            ("gaji", "Gaji", "income", None),
        ]:
            ids[key] = str(uuid4())
            conn.execute(
                "INSERT INTO categories (id, user_id, name, kind, kakeibo_type) VALUES (%s, %s, %s, %s, %s)",
                (ids[key], ids["user_id"], name, kind, pillar),
            )
        conn.commit()
    return ids


def send(client, text, *, client_id=None, device_time=None):
    return client.post("/api/capture/text", json={
        "client_capture_id": client_id or str(uuid4()), "text": text,
        "device_time": (device_time or datetime.now(timezone.utc)).isoformat(),
    })


def captured(client, text, **kwargs):
    response = send(client, text, **kwargs)
    assert response.status_code == 200, response.text
    return response.json()["capture"]


def answer(client, capture, **fields):
    body = {"question_id": capture["question"]["question_id"], "reply_id": str(uuid4()), **fields}
    return client.post(f"/api/capture/{capture['id']}/answer", json=body)


def answered(client, capture, **fields):
    response = answer(client, capture, **fields)
    assert response.status_code == 200, response.text
    return response.json()["capture"]


def model_result(owner, *, category="jajan", confidence=0.9, type_="expense", merchant=None,
                 description="Es teh manis", account=None, account_confidence=0.0, kakeibo="want"):
    return CaptureNormalization.model_validate_json(json.dumps({
        "merchant": merchant, "description": description, "type": type_, "type_confidence": 0.95,
        "category_id": owner[category] if category else None, "kakeibo": kakeibo,
        "category_confidence": confidence, "account_id": owner[account] if account else None,
        "account_confidence": account_confidence,
    }))


def use_provider(monkeypatch, provider):
    from app.main import app

    enable_ai(monkeypatch)
    provider.model = "gpt-5.6-luna"
    monkeypatch.setattr(app.state, "notification_provider", provider)
    return provider


def provider_with(result=None, *, error=None):
    provider = AsyncMock()
    if error:
        provider.normalize_capture.side_effect = error
    else:
        provider.normalize_capture.return_value = result
    return provider


def query(sql, params=()):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute(sql, params).fetchall()


def capture_rows(owner):
    return query("SELECT * FROM transactions WHERE user_id = %s AND source = 'capture' ORDER BY movement_role DESC NULLS FIRST, created_at",
                 (owner["user_id"],))


def balance(owner, key):
    return query(
        """SELECT a.initial_balance + COALESCE(SUM(CASE WHEN t.type = 'income' THEN t.amount ELSE -t.amount END), 0)
                  AS balance
           FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id
           WHERE a.id = %s GROUP BY a.id""",
        (owner[key],),
    )[0]["balance"]


def memory(owner, key):
    rows = query("SELECT * FROM merchant_memory WHERE user_id = %s AND match_key = %s", (owner["user_id"], key))
    return rows[0] if rows else None


def feedback(owner):
    return query("SELECT * FROM classification_feedback WHERE user_id = %s AND kind = 'capture' ORDER BY created_at",
                 (owner["user_id"],))


def remember(owner, key, *, category, account, notes, merchant=None, kakeibo="need", source="owner"):
    from app.db.pool import db_conn
    from app.services.merchant_memory import upsert_memory
    with db_conn() as conn:
        with conn.cursor() as cur:
            upsert_memory(cur, owner["user_id"], key, {
                "merchant": merchant, "notes": notes, "category_id": owner[category], "kakeibo_type": kakeibo,
                "account_id": owner[account] if account else None,
            }, source)
        conn.commit()


def age_capture(capture_id, seconds):
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute(
            """UPDATE text_captures SET recorded_at = NOW() - make_interval(secs => %s),
                   undo_until = NOW() - make_interval(secs => %s) + INTERVAL '10 seconds'
               WHERE id = %s""",
            (seconds, seconds, capture_id),
        )
        conn.commit()


# --- Schema ---------------------------------------------------------------------------------


def test_database_accepts_capture_source_and_feedback_kind(client, owner):
    from app.db.pool import db_conn
    insert = """INSERT INTO transactions (user_id, account_id, type, amount, notes, source, review_status, classified_by)
                VALUES (%s, %s, 'expense', 1000, 'Contoh', %s, 'reviewed', 'owner') RETURNING id"""
    with db_conn() as conn:
        assert conn.execute(insert, (owner["user_id"], owner["cash"], "capture")).fetchone()["id"]
        conn.rollback()
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(insert, (owner["user_id"], owner["cash"], "bot"))
        conn.rollback()
        conn.execute("INSERT INTO classification_feedback (user_id, kind) VALUES (%s, 'capture')", (owner["user_id"],))
        conn.rollback()
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                """INSERT INTO text_captures (user_id, client_capture_id, text, device_time, status)
                   VALUES (%s, %s, 'kopi', NOW(), 'needs_detail')""",
                (owner["user_id"], str(uuid4())),
            )
        conn.rollback()


# --- Recording ------------------------------------------------------------------------------


def test_known_merchant_from_memory_records_without_model(client, owner, monkeypatch):
    remember(owner, "merchant:parkir ptc", category="parkir", account="cash", notes="Parkir PTC", merchant="Parkir PTC")
    provider = use_provider(monkeypatch, provider_with(model_result(owner)))
    capture = captured(client, "bayar parkir ptc 10rb")
    assert capture["status"] == "recorded" and capture["question"] is None
    provider.normalize_capture.assert_not_awaited()
    row = capture_rows(owner)[0]
    assert (row["amount"], str(row["account_id"]), str(row["category_id"]), row["kakeibo_type"]) == (
        10_000, owner["cash"], owner["parkir"], "need")
    assert (row["source"], row["review_status"], row["classified_by"], row["notes"], row["merchant"]) == (
        "capture", "reviewed", "rule", "Parkir PTC", "Parkir PTC")
    assert row["idempotency_key"] == f"capture:{capture['id']}"
    assert capture["transaction"]["account_name"] == "Tunai" and capture["transaction"]["classified_by"] == "rule"
    assert memory(owner, "merchant:parkir ptc")["hit_count"] == 1
    assert balance(owner, "cash") == 990_000


def test_confident_model_records_new_merchant_as_ai(client, owner, monkeypatch):
    provider = use_provider(monkeypatch, provider_with(model_result(owner)))
    capture = captured(client, "es teh manis 5rb tunai")
    assert capture["status"] == "recorded"
    row = capture_rows(owner)[0]
    assert (row["classified_by"], float(row["classification_confidence"]), str(row["category_id"])) == (
        "ai", 0.9, owner["jajan"])
    assert (row["notes"], row["kakeibo_type"], str(row["account_id"]), row["review_status"]) == (
        "Es teh manis", "want", owner["cash"], "reviewed")
    context = provider.normalize_capture.await_args.args[0]
    assert context["text"] == "es teh manis 5rb tunai" and context["facts"]["amount"] == 5_000
    assert context["facts"]["accounts"] == ["Tunai"]
    assert owner["jajan"] in {item["id"] for item in context["categories"]}
    assert owner["stockbit"] not in {item["id"] for item in context["accounts"]}
    assert not any(key.startswith("_") for key in context)
    assert len(json.dumps(context, ensure_ascii=False).encode()) <= 30000
    stored = query("SELECT model, prompt_version, model_calls, classifier FROM text_captures WHERE id = %s",
                   (capture["id"],))[0]
    assert dict(stored) == {"model": "gpt-5.6-luna", "prompt_version": "capture-normalization-1",
                            "model_calls": 1, "classifier": "ok"}
    learned = memory(owner, "merchant:es teh manis")
    assert (learned["source"], str(learned["account_id"])) == ("ai", owner["cash"])


def test_text_naming_the_merchant_teaches_owner_memory(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(model_result(owner, merchant="Kopi Kenangan", description="Kopi Kenangan")))
    capture = captured(client, "kopi kenangan 25k gopay")
    assert capture["status"] == "recorded"
    assert memory(owner, "merchant:kopi kenangan")["source"] == "owner"


def test_low_confidence_or_changed_threshold_asks_category(client, owner, monkeypatch):
    from app.services import notification_processing as processing
    use_provider(monkeypatch, provider_with(model_result(owner, confidence=0.85)))
    monkeypatch.setattr(processing, "settings", replace(processing.settings, capture_confidence_threshold=0.9))
    capture = captured(client, "es teh manis 5rb tunai")
    assert capture["question"]["type"] == "category"
    assert capture["question"]["options"][0]["category_id"] == owner["jajan"]
    assert capture_rows(owner) == []


def test_yesterday_is_parsed_in_jakarta(client, owner):
    sent = datetime.now(timezone.utc) - timedelta(minutes=1)
    remember(owner, "merchant:makan bakso", category="makan", account="cash", notes="Makan bakso")
    capture = captured(client, "kemarin makan bakso 20rb", device_time=sent)
    local = sent.astimezone(ZoneInfo("Asia/Jakarta"))
    assert capture["status"] == "recorded"
    recorded = capture_rows(owner)[0]["date"].astimezone(ZoneInfo("Asia/Jakarta"))
    assert recorded.date() == local.date() - timedelta(days=1)


# --- Questions ------------------------------------------------------------------------------


def test_unknown_account_is_asked_most_recent_first(client, owner, monkeypatch):
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, source,
                   review_status, classified_by) VALUES (%s, %s, %s, 'expense', 1000, 'Contoh', NOW(), 'manual',
                   'reviewed', 'owner')""",
            (owner["user_id"], owner["gopay"], owner["jajan"]),
        )
        conn.commit()
    provider = use_provider(monkeypatch, provider_with(model_result(owner, category="pulsa", kakeibo="need",
                                                                    description="Beli pulsa")))
    capture = captured(client, "beli pulsa 50rb")
    question = capture["question"]
    assert (capture["status"], question["type"], question["role"], question["prompt"]) == (
        "needs_detail", "account", "source", "Dari rekening mana?")
    ids = [item["id"] for item in question["options"]]
    assert ids[0] == owner["gopay"] and len(ids) <= 6 and question["allows_other"] is True
    assert not {owner["stockbit"], owner["jago"]} & set(ids)
    assert capture_rows(owner) == []
    capture = answered(client, capture, option_id=owner["gopay"])
    assert capture["status"] == "recorded"
    row = capture_rows(owner)[0]
    assert (str(row["account_id"]), str(row["category_id"]), row["classified_by"], row["amount"]) == (
        owner["gopay"], owner["pulsa"], "ai", 50_000)
    learned = memory(owner, "merchant:pulsa")
    assert (learned["source"], str(learned["account_id"])) == ("owner", owner["gopay"])
    assert provider.normalize_capture.await_count == 1


@pytest.mark.parametrize("failure", ["provider", "invalid_reference", "ai_disabled"])
def test_provider_unavailable_asks_category_with_ranked_chips(client, owner, monkeypatch, failure):
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, source,
                   review_status, classified_by) VALUES (%s, %s, %s, 'expense', 18000, 'Kopi susu', NOW(), 'manual',
                   'reviewed', 'owner')""",
            (owner["user_id"], owner["cash"], owner["jajan"]),
        )
        conn.commit()
    if failure == "provider":
        provider = use_provider(monkeypatch, provider_with(error=ProviderError("provider_timeout", transient=True)))
    elif failure == "invalid_reference":
        bad = model_result(owner).model_copy(update={"category_id": uuid4()})
        provider = use_provider(monkeypatch, provider_with(bad))
    else:
        provider = provider_with(model_result(owner))
    capture = captured(client, "kopi susu 18rb tunai")
    question = capture["question"]
    assert question["type"] == "category" and question["allows_other"] is True
    assert 1 <= len(question["options"]) <= 5
    assert question["options"][0]["category_id"] == owner["jajan"] and question["options"][0]["kakeibo"] == "want"
    assert all(item["kakeibo"] for item in question["options"])
    assert capture_rows(owner) == []
    classifier = query("SELECT classifier FROM text_captures WHERE id = %s", (capture["id"],))[0]["classifier"]
    assert classifier == {"provider": "provider_timeout", "invalid_reference": "invalid_normalization",
                          "ai_disabled": "ai_disabled"}[failure]
    assert provider.normalize_capture.await_count == (0 if failure == "ai_disabled" else 1)
    capture = answered(client, capture, option_id="c1")
    row = capture_rows(owner)[0]
    assert (capture["status"], row["classified_by"], str(row["category_id"]), row["kakeibo_type"]) == (
        "recorded", "owner", owner["jajan"], "want")
    assert row["notes"] == "Kopi susu"


def test_amount_questions(client, owner):
    several = captured(client, "parkir 10rb dan kopi 25rb tunai")
    assert (several["question"]["type"], several["question"]["reason"]) == ("amount", "several")
    assert "satu transaksi per pesan" in several["question"]["prompt"]
    missing = captured(client, "bayar parkir tunai")
    question = missing["question"]
    assert (question["type"], question["reason"], question["allows_text"]) == ("amount", "missing", True)
    assert answer(client, missing, text="sepuluh").status_code == 422
    assert answer(client, missing, text="10rb 20rb").status_code == 422
    assert answer(client, missing, option_id="c1").status_code == 422
    capture = answered(client, missing, text="10rb")
    assert capture["question"]["type"] == "category" and capture["summary"]["amount"] == 10_000
    capture = answered(client, capture, category_id=owner["parkir"])
    row = capture_rows(owner)[0]
    assert (capture["status"], row["amount"], row["kakeibo_type"], row["classified_by"]) == (
        "recorded", 10_000, "need", "owner")


def test_kind_question_from_words_or_model_flag(client, owner, monkeypatch):
    conflicted = captured(client, "dapat refund tapi bayar ongkir 20rb tunai")
    assert conflicted["question"]["type"] == "kind"
    assert [item["id"] for item in conflicted["question"]["options"]] == ["expense", "income", "movement"]
    assert answer(client, conflicted, option_id="transfer").status_code == 422
    capture = answered(client, conflicted, option_id="income")
    assert capture["question"]["type"] == "category"
    assert {item["category_id"] for item in capture["question"]["options"]} <= {owner["gaji"], owner["income"]}
    assert answer(client, capture, category_id=owner["gaji"], kakeibo="need").status_code == 422
    capture = answered(client, capture, category_id=owner["gaji"])
    row = capture_rows(owner)[0]
    assert (row["type"], row["kakeibo_type"], row["classified_by"]) == ("income", None, "owner")

    use_provider(monkeypatch, provider_with(model_result(owner, type_="income", category="gaji", kakeibo=None)))
    flagged = captured(client, "kopi 20rb tunai")
    assert flagged["question"]["type"] == "kind"


def test_answers_are_idempotent_and_stale_questions_conflict(client, owner):
    capture = captured(client, "bayar parkir tunai")
    first_question = capture["question"]
    body = {"question_id": first_question["question_id"], "reply_id": str(uuid4()), "text": "10rb"}
    after = client.post(f"/api/capture/{capture['id']}/answer", json=body)
    assert after.status_code == 200
    replay = client.post(f"/api/capture/{capture['id']}/answer", json=body)
    assert replay.status_code == 200 and replay.json()["capture"] == after.json()["capture"]
    conflict = client.post(f"/api/capture/{capture['id']}/answer", json={**body, "text": "20rb"})
    assert conflict.status_code == 409 and conflict.json()["detail"]["code"] == "capture_answer_conflict"
    stale = client.post(f"/api/capture/{capture['id']}/answer", json={**body, "reply_id": str(uuid4())})
    assert stale.status_code == 409 and stale.json()["detail"]["code"] == "stale_capture_question"
    current = after.json()["capture"]
    recorded = answered(client, current, option_id="c1")
    category_body = {"question_id": current["question"]["question_id"], "reply_id": str(uuid4()), "option_id": "c1"}
    late = client.post(f"/api/capture/{capture['id']}/answer", json=category_body)
    assert late.status_code == 409 and late.json()["detail"]["code"] == "stale_capture_question"
    assert recorded["status"] == "recorded" and len(capture_rows(owner)) == 1
    stored = query("SELECT answers FROM text_captures WHERE id = %s", (capture["id"],))[0]["answers"]
    assert [item["type"] for item in stored] == ["amount", "category"]


def test_resend_returns_the_same_capture(client, owner, monkeypatch):
    provider = use_provider(monkeypatch, provider_with(model_result(owner)))
    client_id = str(uuid4())
    first = captured(client, "es teh manis 5rb tunai", client_id=client_id)
    again = captured(client, "es teh manis 5rb tunai", client_id=client_id)
    assert again == first and len(capture_rows(owner)) == 1
    assert provider.normalize_capture.await_count == 1
    conflict = send(client, "es teh 6rb tunai", client_id=client_id)
    assert conflict.status_code == 409 and conflict.json()["detail"]["code"] == "capture_id_conflict"
    assert client.get(f"/api/capture/{first['id']}").json()["capture"] == first


# --- Movements --------------------------------------------------------------------------------


def test_cash_withdrawal_is_a_movement_from_the_usual_account(client, owner):
    for _ in range(3):
        moved = client.post("/api/movements", json={"source_account_id": owner["bca"],
                                                     "target_account_id": owner["cash"], "amount": 100_000})
        assert moved.status_code == 200, moved.text
    bca, cash = balance(owner, "bca"), balance(owner, "cash")
    capture = captured(client, "tarik tunai 200rb")
    assert capture["status"] == "recorded"
    legs = capture_rows(owner)
    assert [(row["type"], row["movement_role"], row["amount"]) for row in legs] == [
        ("expense", "outbound", 200_000), ("income", "inbound", 200_000)]
    assert legs[0]["movement_id"] == legs[1]["movement_id"] and legs[0]["kakeibo_type"] is None
    assert [str(row["account_id"]) for row in legs] == [owner["bca"], owner["cash"]]
    assert legs[0]["notes"] == "Tarik tunai" and {row["classified_by"] for row in legs} == {"rule"}
    from app.services.report_figures import reportable_expense_sql, reportable_income_sql
    spending = query(
        f"""SELECT COUNT(*) AS count FROM transactions t LEFT JOIN categories c ON c.id = t.category_id
            WHERE t.movement_id = %s AND ({reportable_expense_sql()} OR {reportable_income_sql()})""",
        (legs[0]["movement_id"],),
    )[0]["count"]
    assert spending == 0
    assert (balance(owner, "bca"), balance(owner, "cash")) == (bca - 200_000, cash + 200_000)
    assert capture["movement"]["source"]["account_id"] == owner["bca"]
    assert capture["movement"]["target"]["account_name"] == "Tunai"
    assert capture["transaction"]["id"] == str(legs[0]["id"]) and capture["transaction"]["kakeibo"] is None
    assert [row["kind"] for row in feedback(owner)] == ["capture"]


def test_movement_without_history_asks_source_and_named_movement_records(client, owner):
    capture = captured(client, "tarik tunai 200rb")
    question = capture["question"]
    assert (question["type"], question["role"]) == ("account", "source")
    assert owner["cash"] not in {item["id"] for item in question["options"]}
    capture = answered(client, capture, option_id=owner["bca"])
    assert capture["status"] == "recorded" and capture["movement"]["source"]["account_id"] == owner["bca"]
    named = captured(client, "pindah 50rb ke GoPay dari bca harian")
    assert named["status"] == "recorded"
    assert (named["movement"]["source"]["account_id"], named["movement"]["target"]["account_id"]) == (
        owner["bca"], owner["gopay"])


# --- Undo -------------------------------------------------------------------------------------


def recorded_capture(client, owner, text="bayar parkir ptc 10rb"):
    remember(owner, "merchant:parkir ptc", category="parkir", account="cash", notes="Parkir PTC")
    capture = captured(client, text)
    assert capture["status"] == "recorded"
    return capture


def test_undo_within_window_removes_rows_and_restores_balance_and_memory(client, owner):
    remember(owner, "merchant:parkir ptc", category="parkir", account="cash", notes="Parkir PTC")
    before_memory = memory(owner, "merchant:parkir ptc")
    before = balance(owner, "cash")
    capture = captured(client, "bayar parkir ptc 10rb")
    assert balance(owner, "cash") == before - 10_000
    age_capture(capture["id"], 4)
    response = client.post(f"/api/capture/{capture['id']}/undo")
    assert response.status_code == 200, response.text
    undone = response.json()["capture"]
    assert (undone["status"], undone["transaction"]) == ("undone", None) and undone["undone_at"]
    assert capture_rows(owner) == [] and balance(owner, "cash") == before
    restored = memory(owner, "merchant:parkir ptc")
    assert (restored["hit_count"], restored["updated_at"]) == (before_memory["hit_count"], before_memory["updated_at"])
    assert client.post(f"/api/capture/{capture['id']}/undo").json()["capture"]["status"] == "undone"


def test_undo_of_a_learned_capture_removes_the_new_memory(client, owner, monkeypatch):
    use_provider(monkeypatch, provider_with(model_result(owner)))
    capture = captured(client, "es teh manis 5rb tunai")
    assert memory(owner, "merchant:es teh manis") is not None
    assert client.post(f"/api/capture/{capture['id']}/undo").status_code == 200
    assert memory(owner, "merchant:es teh manis") is None
    assert len(feedback(owner)) == 1


def test_undo_of_a_movement_removes_both_legs(client, owner):
    capture = captured(client, "pindah 50rb ke GoPay dari bca harian")
    bca = balance(owner, "bca")
    assert client.post(f"/api/capture/{capture['id']}/undo").status_code == 200
    assert capture_rows(owner) == [] and balance(owner, "bca") == bca + 50_000


def test_undo_too_late_or_after_an_edit_changes_nothing(client, owner):
    late = recorded_capture(client, owner)
    age_capture(late["id"], 20)
    response = client.post(f"/api/capture/{late['id']}/undo")
    assert response.status_code == 409 and response.json()["detail"]["code"] == "undo_expired"
    assert len(capture_rows(owner)) == 1
    assert client.get(f"/api/capture/{late['id']}").json()["capture"]["status"] == "recorded"

    edited = captured(client, "parkir ptc 15rb")
    edit = client.patch(f"/api/transactions/{edited['transaction']['id']}", json={"notes": "Parkir mal"})
    assert edit.status_code == 200, edit.text
    response = client.post(f"/api/capture/{edited['id']}/undo")
    assert response.status_code == 409 and response.json()["detail"]["code"] == "capture_transaction_changed"
    assert len(capture_rows(owner)) == 2

    pending = captured(client, "bayar parkir tunai")
    response = client.post(f"/api/capture/{pending['id']}/undo")
    assert response.status_code == 409 and response.json()["detail"]["code"] == "capture_not_recorded"


# --- Learning ---------------------------------------------------------------------------------


def test_answers_teach_memory_and_the_second_capture_needs_no_question(client, owner):
    capture = captured(client, "parkir ptc 10rb")
    assert capture["question"]["type"] == "account"
    capture = answered(client, capture, account_id=owner["cash"])
    capture = answered(client, capture, category_id=owner["parkir"])
    assert capture["status"] == "recorded"
    rows = feedback(owner)
    assert len(rows) == 1
    example = rows[0]
    local = capture_rows(owner)[0]["date"].astimezone(ZoneInfo("Asia/Jakarta"))
    assert (example["local_weekday"], example["local_hour"]) == (local.isoweekday(), local.hour)
    assert (example["amount"], str(example["account_id"]), example["match_key"]) == (
        10_000, owner["cash"], "merchant:parkir ptc")
    assert (example["answer_text"], example["transaction_source"], str(example["after_category_id"])) == (
        "parkir ptc 10rb", "capture", owner["parkir"])
    learned = memory(owner, "merchant:parkir ptc")
    assert (learned["source"], str(learned["account_id"]), str(learned["category_id"])) == (
        "owner", owner["cash"], owner["parkir"])
    second = captured(client, "bayar parkir ptc 12rb")
    assert second["status"] == "recorded" and second["question"] is None
    assert second["transaction"]["classified_by"] == "rule" and second["transaction"]["account_id"] == owner["cash"]
    assert len(feedback(owner)) == 2


def test_account_answer_with_model_category_teaches_memory(client, owner, monkeypatch):
    provider = use_provider(monkeypatch, provider_with(model_result(owner, category="parkir", kakeibo="need",
                                                                    description="Parkir PTC")))
    capture = captured(client, "parkir ptc 10rb")
    capture = answered(client, capture, option_id=owner["cash"])
    assert capture["status"] == "recorded"
    second = captured(client, "parkir ptc 10rb")
    assert second["status"] == "recorded" and provider.normalize_capture.await_count == 1


def test_clear_history_resolves_the_account(client, owner):
    remember(owner, "merchant:fotokopi", category="makan", account=None, notes="Fotokopi")
    for amount in ("5rb", "6rb", "7rb"):
        capture = captured(client, f"fotokopi {amount} gopay")
        assert capture["status"] == "recorded"
    assert memory(owner, "merchant:fotokopi")["account_id"] is not None
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute("UPDATE merchant_memory SET account_id = NULL WHERE user_id = %s", (owner["user_id"],))
        conn.commit()
    capture = captured(client, "fotokopi 8rb")
    assert capture["status"] == "recorded" and capture["transaction"]["account_id"] == owner["gopay"]


# --- Boundaries ---------------------------------------------------------------------------------


def test_invalid_and_foreign_requests_change_nothing(client, owner):
    from app.main import app
    from app.services.auth import get_current_user

    for text in ("", "   ", "x" * 201, "kopi\u0000 5rb"):
        response = send(client, text)
        assert response.status_code == 422, text
    naive = client.post("/api/capture/text", json={"client_capture_id": str(uuid4()), "text": "kopi 5rb",
                                                   "device_time": "2026-10-29T08:00:00"})
    assert naive.status_code == 422
    future = send(client, "kopi 5rb", device_time=datetime.now(timezone.utc) + timedelta(hours=1))
    assert future.status_code == 422 and future.json()["detail"]["code"] == "invalid_device_time"
    bad_date = send(client, "31/2 kopi 5rb")
    assert bad_date.status_code == 422 and bad_date.json()["detail"]["code"] == "invalid_capture_date"
    assert query("SELECT COUNT(*) AS count FROM text_captures WHERE user_id = %s", (owner["user_id"],))[0]["count"] == 0

    capture = captured(client, "bayar parkir ptc tunai")
    assert answer(client, capture, text="10rb", option_id="c1").status_code == 422
    assert client.get(f"/api/capture/{uuid4()}").status_code == 404
    app.dependency_overrides[get_current_user] = lambda: {"id": str(uuid4()), "currency": "IDR"}
    try:
        assert client.get(f"/api/capture/{capture['id']}").status_code == 404
        assert answer(client, capture, text="10rb").status_code == 404
        assert client.post(f"/api/capture/{capture['id']}/undo").status_code == 404
    finally:
        app.dependency_overrides[get_current_user] = lambda: {"id": owner["user_id"], "currency": "IDR"}
    unchanged = client.get(f"/api/capture/{capture['id']}").json()["capture"]
    assert unchanged == capture

    account_question = answered(client, capture, text="10rb")
    assert account_question["question"]["type"] == "category"


def test_account_answer_validation(client, owner):
    capture = captured(client, "beli pulsa 50rb")
    assert capture["question"]["type"] == "account"
    assert answer(client, capture, account_id=owner["stockbit"]).status_code == 422
    assert answer(client, capture, account_id=owner["jago"]).status_code == 422
    assert answer(client, capture, option_id="unknown").status_code == 422
    assert answered(client, capture, account_id=owner["main"])["question"]["type"] == "category"


def test_capture_rate_limit(client, owner, monkeypatch):
    from app.services import text_capture
    monkeypatch.setattr(text_capture, "CAPTURE_RATE_LIMIT", 2)
    assert send(client, "bayar parkir").status_code == 200
    assert send(client, "bayar parkir").status_code == 200
    limited = send(client, "bayar parkir")
    assert limited.status_code == 429 and limited.json()["detail"]["code"] == "capture_rate_limited"


def test_logs_hold_no_text_or_amounts(client, owner, caplog):
    caplog.set_level(logging.DEBUG)
    capture = captured(client, "kopi rahasiaku 123rb tunai")
    capture = answered(client, capture, option_id="c1")
    client.post(f"/api/capture/{capture['id']}/undo")
    messages = [record.getMessage() for record in caplog.records]
    assert any("Text capture" in message for message in messages)
    for message in messages:
        assert "rahasiaku" not in message.casefold()
        assert "123" not in message


# --- Schema validation and provider -----------------------------------------------------------


def test_normalization_validation():
    context = {
        "categories": [{"id": "c-expense", "kind": "expense", "default_kakeibo": "want"},
                       {"id": "c-income", "kind": "income", "default_kakeibo": None}],
        "accounts": [{"id": "a-cash"}],
    }

    def result(**values):
        base = {"merchant": None, "description": "Es teh", "type": "expense", "type_confidence": 0.9,
                "category_id": None, "kakeibo": None, "category_confidence": 0.9, "account_id": None,
                "account_confidence": 0}
        return CaptureNormalization.model_construct(**{**base, **values})

    checked = validate_capture_normalization(result(category_id="c-expense"), context)
    assert (checked["kakeibo"], checked["category_kind"]) == ("want", "expense")
    assert validate_capture_normalization(result(category_id="c-income", kakeibo="need"), context)["kakeibo"] is None
    assert validate_capture_normalization(result(description="Pembayaran"), context)["description"] is None
    with pytest.raises(InvalidCaptureNormalization):
        validate_capture_normalization(result(category_id="c-unknown"), context)
    with pytest.raises(InvalidCaptureNormalization):
        validate_capture_normalization(result(account_id="a-unknown"), context)
    with pytest.raises(ValueError):
        CaptureNormalization.model_validate_json(json.dumps({"description": "x", "amount": 5000}))


def run_capture_normalizer(handler, context):
    async def run():
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(handler)) as transport:
            provider = OpenAINotificationProvider(
                transport, api_key="synthetic-test-credential", model="gpt-5.6-luna", reasoning_effort="low",
            )
            return await provider.normalize_capture(context)
    return asyncio.run(run())


def test_capture_normalizer_uses_its_own_strict_schema_and_prompt():
    sent = {}
    output = {"merchant": None, "description": "Es teh manis", "type": "expense", "type_confidence": 0.9,
              "category_id": "00000000-0000-4000-8000-000000000101", "kakeibo": "want", "category_confidence": 0.9,
              "account_id": None, "account_confidence": 0}

    def handler(request):
        sent.update(json.loads(request.content))
        return httpx2.Response(200, json=envelope(output))

    result = run_capture_normalizer(handler, {"text": "es teh manis 5rb", "_internal": "never-sent"})
    assert result.description == "Es teh manis"
    assert sent["text"]["format"]["name"] == "capture_normalization" and sent["text"]["format"]["strict"] is True
    assert set(sent["text"]["format"]["schema"]["required"]) == set(output)
    assert "never-sent" not in sent["input"] and "typed on the phone" in sent["instructions"]

    def invalid(request):
        return httpx2.Response(200, json=envelope({"description": ""}))

    with pytest.raises(ProviderError) as error:
        run_capture_normalizer(invalid, {"text": "kopi"})
    assert error.value.code == "provider_invalid_output"


def test_capture_threshold_setting(monkeypatch):
    from app.core.config import load_settings
    monkeypatch.setenv("CAPTURE_CONFIDENCE_THRESHOLD", "0.9")
    assert load_settings().capture_confidence_threshold == 0.9
    monkeypatch.setenv("CAPTURE_CONFIDENCE_THRESHOLD", "1.5")
    assert load_settings().capture_configuration_error == "capture_configuration_invalid"
