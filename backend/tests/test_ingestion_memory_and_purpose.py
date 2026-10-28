"""Record-and-flag, merchant memory, classification feedback and purpose questions (phase 3).

All texts, names and amounts are synthetic.
"""
import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import psycopg
import pytest

from app.services.notification_interpretation import Interpretation
from app.services.openai_notification_provider import ProviderError
from app.services.purpose_normalization import PurposeNormalization
from test_notification_processing_database import (  # noqa: F401 - fixture
    count_transactions, enable_ai, ingest, interpretation, notification, notification_owner,
)

BCA = "com.bca.mybca.omni.android"
GROCERIES = "You spent IDR 187,500.00 at Groceries."
SHOPPING = "You spent IDR 45,000.00 at Shopping."
MASKED_INCOMING = "You received IDR 57,000.00 from ***AB ****CD *E at Account Transfer category."
KEDAI = "You've paid Rp125.000 to Kedai Awan"
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def owner(notification_owner):
    from app.db.pool import db_conn

    ids = dict(notification_owner)
    with db_conn() as conn:
        for key, name, kind, pillar, offsets in [
            ("belanja", "Belanja", "expense", "need", False),
            ("jajan", "Jajan & Nongkrong", "expense", "want", False),
            ("makan", "Makanan & Minuman", "expense", "need", False),
            ("olahraga", "Olahraga", "expense", "culture", False),
            ("patungan", "Patungan & Ganti Uang", "income", None, True),
        ]:
            ids[key] = str(uuid4())
            conn.execute(
                """INSERT INTO categories (id, user_id, name, kind, kakeibo_type, offsets_spending)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (ids[key], ids["user_id"], name, kind, pillar, offsets),
            )
        conn.commit()
    return ids


def at(payload, *, days=0, hours=0, version=None, **extra):
    moment = datetime.fromisoformat(payload["post_time"]) + timedelta(days=days, hours=hours)
    updated = {**payload, "post_time": moment.isoformat(), **extra}
    if version:
        updated["source_version"] = version
    return updated


def bca(text, **kwargs):
    return at(notification(text, BCA), **kwargs)


def event_row(event_id):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute("SELECT * FROM notification_events WHERE id = %s", (event_id,)).fetchone()


def tx_for(event_id):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute(
            """SELECT t.* FROM transactions t JOIN notification_events n ON n.transaction_id = t.id
               WHERE n.id = %s""",
            (event_id,),
        ).fetchone()


def feedback_rows(owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute(
            "SELECT * FROM classification_feedback WHERE user_id = %s ORDER BY created_at, id", (owner["user_id"],),
        ).fetchall()


def memory_rows(owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return {row["match_key"]: row for row in conn.execute(
            "SELECT * FROM merchant_memory WHERE user_id = %s", (owner["user_id"],),
        ).fetchall()}


def run_claim(provider=None):
    from app.services import notification_processing as processing
    claimed = processing.claim_event(include_ai=provider is not None)
    assert claimed is not None
    asyncio.run(processing.process_claim(claimed, provider))
    return claimed


def result_of(client, event_id):
    return client.get(f"/api/ingest/notifications/{event_id}/result").json()["result"]


def bca_interpretation(owner, *, category, confidence=0.9, merchant=None, description="Belanja bulanan"):
    return Interpretation.model_validate_json(json.dumps({
        "outcome": "record", "direction": "expense", "description": description, "merchant": merchant,
        "account_id": owner["bca"], "category_id": owner[category], "kakeibo": "need",
        "source_account_id": None, "target_account_id": None,
        "amount_evidence": "IDR 187,500.00", "direction_evidence": "You spent",
        "source_evidence": "", "target_evidence": "", "candidate_transaction_id": None,
        "confidence": confidence, "review_reason": "none",
    }))


def normalization(**values):
    return PurposeNormalization.model_validate_json(json.dumps(values))


def add_feedback(owner, *, notes, category, amount, weekday, hour, signature="bca|expense|groceries", times=1,
                 account="bca"):
    from app.db.pool import db_conn
    with db_conn() as conn:
        for _ in range(times):
            conn.execute(
                """INSERT INTO classification_feedback (user_id, kind, transaction_id, after_notes, after_category_id,
                       after_kakeibo_type, after_type, local_weekday, local_hour, amount, account_id, institution,
                       text_signature)
                   VALUES (%s, 'correction', %s, %s, %s, 'want', 'expense', %s, %s, %s, %s, 'bca', %s)""",
                (owner["user_id"], str(uuid4()), notes, owner[category], weekday, hour, amount, owner[account], signature),
            )
        conn.commit()


# --- Record and flag -------------------------------------------------------------------------


def test_uncertain_category_is_recorded_uncategorised_with_question(client, owner):
    result = ingest(client, bca(GROCERIES))["results"][0]
    assert result["status"] == "recorded" and result["description"] == "Pengeluaran Groceries"
    row = tx_for(result["event_id"])
    assert (row["amount"], str(row["account_id"]), row["category_id"]) == (187500, owner["bca"], None)
    assert row["review_status"] == "needs_review" and row["classified_by"] == "rule" and row["merchant"] is None
    event = event_row(result["event_id"])
    assert event["interpretation"]["review_reasons"] == ["uncertain_category", "purpose_question"]
    assert event["interpretation"]["match_key"] == "signature:bca|expense|groceries"
    assert event["active_question"]["type"] == "purpose"


def test_model_needs_review_resolved_deterministically_is_recorded_flagged(client, owner, monkeypatch):
    enable_ai(monkeypatch)
    event = ingest(client, notification(KEDAI))["results"][0]
    provider = AsyncMock()
    provider.interpret.return_value = interpretation(owner).model_copy(update={
        "outcome": "needs_review", "direction": None, "description": "", "account_id": None, "category_id": None,
        "kakeibo": None, "amount_evidence": "", "direction_evidence": "", "review_reason": "uncertain_facts",
    })
    run_claim(provider)
    row = tx_for(event["event_id"])
    assert row["review_status"] == "needs_review" and row["merchant"] == "Kedai Awan"
    assert event_row(event["event_id"])["interpretation"]["review_reasons"] == ["model_fallback"]
    assert event_row(event["event_id"])["active_question"] is None


@pytest.mark.parametrize("confidence,status,reasons", [(0.92, "auto", []), (0.6, "needs_review", ["low_confidence"])])
def test_confidence_gate(client, owner, monkeypatch, confidence, status, reasons):
    enable_ai(monkeypatch)
    event = ingest(client, notification(KEDAI))["results"][0]
    provider = AsyncMock()
    provider.interpret.return_value = interpretation(owner).model_copy(
        update={"confidence": confidence, "merchant": "Kedai Awan"})
    run_claim(provider)
    row = tx_for(event["event_id"])
    assert row["review_status"] == status and float(row["classification_confidence"]) == confidence
    assert row["classified_by"] == "ai" and row["merchant"] == "Kedai Awan"
    assert event_row(event["event_id"])["interpretation"]["review_reasons"] == reasons
    learned = memory_rows(owner).get("merchant:kedai awan")
    assert (learned is not None and learned["source"] == "ai") is (status == "auto")


def test_ambiguous_counterparts_record_unpaired_leg_flagged(client, owner):
    from app.db.pool import db_conn
    for seconds in (1, 2):
        ingest(client, notification("Rp500.000 udah dikirim ke BCA Raka Purnama.", "com.gojek.gopay", seconds=seconds))
    incoming = ingest(client, notification(
        "Pemasukan sebesar IDR 500,000.00 dari RA** PUR**MA di kategori Transfer Rekening.", "com.bca", seconds=4,
    ))["results"][0]
    assert incoming["status"] == "recorded" and incoming["type"] == "income"
    assert tx_for(incoming["event_id"])["review_status"] == "needs_review"
    assert "ambiguous_movement" in event_row(incoming["event_id"])["interpretation"]["review_reasons"]
    with db_conn() as conn:
        linked = conn.execute("SELECT COUNT(*) AS n FROM transactions WHERE user_id = %s AND movement_id IS NOT NULL",
                              (owner["user_id"],)).fetchone()["n"]
    assert linked == 0


@pytest.mark.parametrize("payload,state", [
    (notification("You've paid Rp50.000, biaya Rp2.500, total Rp52.500"), "needs_review"),
    (notification("Payment of Rp125.000 to Kedai Awan using Liburan Pocket was successful."), "needs_review"),
    ({**notification("You've transferred Rp625.000 to Sinta Wulandari. Need help? Contact Tanya Jago at 1500 746."),
      "source_version": "1.8.0"}, "needs_confirmation"),
], ids=["two-amounts", "unknown-pocket", "jago-source-pocket"])
def test_money_or_account_doubt_holds_without_a_row(client, owner, payload, state):
    result = ingest(client, payload)["results"][0]
    assert result["status"] == state
    assert count_transactions(owner) == 0


def test_automatic_pairing_closes_question_and_clears_flags(client, owner):
    from app.db.pool import db_conn
    debit = ingest(client, bca("You spent IDR 7,490,557.00 at Shopping.", seconds=1))["results"][0]
    assert event_row(debit["event_id"])["active_question"]["type"] == "purpose"
    top_up = notification("Pengisian saldo sebesar Rp7.490.557 telah ditambahkan ke ShopeePay-mu.",
                          "com.shopeepay.id", seconds=5)
    with db_conn() as conn:
        conn.execute("INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'ShopeePay', 'ewallet', 0)",
                     (owner["user_id"],))
        conn.execute("UPDATE accounts SET initial_balance = 9000000 WHERE id = %s", (owner["bca"],))
        conn.commit()
    paired = ingest(client, top_up)["results"][0]
    assert paired["type"] == "internal_movement", paired
    assert event_row(debit["event_id"])["active_question"] is None
    with db_conn() as conn:
        statuses = {row["review_status"] for row in conn.execute(
            "SELECT review_status FROM transactions WHERE user_id = %s", (owner["user_id"],)).fetchall()}
    assert statuses == {"auto"}


# --- Memory and feedback ---------------------------------------------------------------------


def test_owner_edit_writes_feedback_and_memory_and_amount_edit_does_not(client, owner):
    from app.services.merchant_memory import local_time_features
    event = ingest(client, notification(KEDAI))["results"][0]
    row = tx_for(event["event_id"])
    response = client.patch(f"/api/transactions/{row['id']}", json={"amount": 120000})
    assert response.status_code == 200, response.text
    assert feedback_rows(owner) == []
    response = client.patch(f"/api/transactions/{row['id']}", json={
        "category_id": owner["jajan"], "kakeibo_type": "want", "notes": "Ngopi sore",
    })
    assert response.status_code == 200, response.text
    [feedback] = feedback_rows(owner)
    weekday, hour = local_time_features(row["date"])
    assert feedback["kind"] == "correction" and str(feedback["transaction_id"]) == str(row["id"])
    assert (feedback["before_notes"], feedback["after_notes"]) == ("Kedai Awan", "Ngopi sore")
    assert (str(feedback["before_category_id"]), str(feedback["after_category_id"])) == (owner["expense"], owner["jajan"])
    assert (feedback["local_weekday"], feedback["local_hour"], feedback["amount"]) == (weekday, hour, 120000)
    assert (feedback["institution"], feedback["match_key"], feedback["transaction_source"]) == (
        "jago", "merchant:kedai awan", "notification")
    memory = memory_rows(owner)["merchant:kedai awan"]
    assert (memory["source"], str(memory["category_id"]), memory["kakeibo_type"], memory["notes"]) == (
        "owner", owner["jajan"], "want", "Ngopi sore")


def test_feedback_rows_cannot_be_updated(client, owner):
    from app.db.pool import db_conn
    add_feedback(owner, notes="makan siang", category="makan", amount=50000, weekday=1, hour=12)
    with db_conn() as conn:
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            conn.execute("UPDATE classification_feedback SET after_notes = 'diubah' WHERE user_id = %s",
                         (owner["user_id"],))
    assert feedback_rows(owner)[0]["after_notes"] == "makan siang"


def test_movement_rename_writes_feedback_without_memory(client, owner):
    moved = client.post("/api/movements", json={
        "source_account_id": owner["bca"], "target_account_id": owner["gopay"], "amount": 50000,
    })
    assert moved.status_code == 200, moved.text
    movement_id = moved.json()["movement_id"]
    renamed = client.patch(f"/api/movements/{movement_id}", json={
        "source_account_id": owner["bca"], "target_account_id": owner["gopay"], "amount": 50000,
        "notes": "Isi GoPay mingguan",
    })
    assert renamed.status_code == 200, renamed.text
    rows = feedback_rows(owner)
    assert len(rows) == 2 and {row["after_notes"] for row in rows} == {"Isi GoPay mingguan"}
    assert memory_rows(owner) == {}


def test_memory_hit_records_without_calling_the_model(client, owner, monkeypatch):
    first = ingest(client, notification(KEDAI))["results"][0]
    row = tx_for(first["event_id"])
    client.patch(f"/api/transactions/{row['id']}", json={"category_id": owner["jajan"], "kakeibo_type": "want",
                                                          "merchant": "Kedai Awan", "notes": "Ngopi di Kedai Awan"})
    enable_ai(monkeypatch)
    second = ingest(client, notification("You've paid Rp40.000 to Kedai Awan", seconds=40))["results"][0]
    provider = AsyncMock()
    run_claim(provider)
    assert provider.interpret.await_count == 0
    remembered = tx_for(second["event_id"])
    assert str(remembered["category_id"]) == owner["jajan"] and remembered["kakeibo_type"] == "want"
    assert (remembered["classified_by"], remembered["review_status"], remembered["merchant"]) == (
        "rule", "auto", "Kedai Awan")
    assert remembered["notes"] == "Ngopi di Kedai Awan" and remembered["classification_confidence"] is None
    assert event_row(second["event_id"])["interpretation"]["source"] == "memory"
    assert memory_rows(owner)["merchant:kedai awan"]["hit_count"] == 1


def test_archived_memory_category_disables_the_hit(client, owner):
    from app.db.pool import db_conn
    from app.services.merchant_memory import upsert_memory
    with db_conn() as conn:
        with conn.cursor() as cur:
            upsert_memory(cur, owner["user_id"], "merchant:kedai awan",
                          {"merchant": "Kedai Awan", "notes": "Ngopi", "category_id": owner["jajan"]}, "owner")
            cur.execute("UPDATE categories SET is_archived = TRUE WHERE id = %s", (owner["jajan"],))
        conn.commit()
    event = ingest(client, notification(KEDAI))["results"][0]
    row = tx_for(event["event_id"])
    assert str(row["category_id"]) == owner["expense"]
    assert event_row(event["event_id"])["interpretation"]["source"] == "deterministic"


def test_ai_memory_never_replaces_owner_memory(client, owner):
    from app.db.pool import db_conn
    from app.services.merchant_memory import upsert_memory
    with db_conn() as conn:
        with conn.cursor() as cur:
            upsert_memory(cur, owner["user_id"], "merchant:kedai awan",
                          {"merchant": "Kedai Awan", "notes": "Ngopi", "category_id": owner["jajan"]}, "owner")
            upsert_memory(cur, owner["user_id"], "merchant:kedai awan",
                          {"merchant": "Kedai Awan", "notes": "Tagihan", "category_id": owner["expense"]}, "ai")
        conn.commit()
    entry = memory_rows(owner)["merchant:kedai awan"]
    assert (entry["source"], str(entry["category_id"]), entry["notes"]) == ("owner", owner["jajan"], "Ngopi")


def test_signature_memory_does_not_skip_the_model(client, owner, monkeypatch):
    first = ingest(client, bca(GROCERIES))["results"][0]
    question = event_row(first["event_id"])["active_question"]
    provider = AsyncMock()
    provider.normalize_purpose.return_value = normalization(
        merchant="Pasar Segar", description="Belanja sayur di Pasar Segar", category_id=owner["belanja"],
        kakeibo="need", offset_category_id=None, confidence=0.9)
    enable_ai(monkeypatch)
    from app.main import app
    monkeypatch.setattr(app.state, "notification_provider", provider)
    answered = client.post(f"/api/ingest/notifications/{first['event_id']}/answer-purpose", json={
        "question_id": question["question_id"], "reply_id": str(uuid4()), "text": "sayur pasar segar"})
    assert answered.status_code == 200, answered.text
    assert "signature:bca|expense|groceries" in memory_rows(owner)
    second = ingest(client, bca(GROCERIES, days=1))["results"][0]
    provider.interpret.return_value = bca_interpretation(owner, category="belanja", confidence=0.7)
    run_claim(provider)
    assert provider.interpret.await_count == 1
    assert event_row(second["event_id"])["active_question"]["type"] == "purpose"
    assert tx_for(second["event_id"])["classified_by"] == "ai"


# --- Model context -----------------------------------------------------------------------------


def test_similar_history_prefers_owner_confirmed_same_merchant_rows(client, owner):
    from app.db.pool import db_conn
    from app.services.notification_context import load_context
    from app.services.notification_evidence import collect_facts
    corrected = ingest(client, notification(KEDAI, seconds=1))["results"][0]
    other = ingest(client, notification("You've paid Rp90.000 to Kedai Awan", seconds=50))["results"][0]
    ingest(client, bca(SHOPPING, seconds=2))
    with db_conn() as conn:
        for _ in range(25):
            conn.execute("""INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, notes,
                                source, review_status, classified_by)
                            VALUES (%s, %s, %s, 'expense', 125000, NOW(), 'Unrelated', 'manual', 'reviewed', 'owner')""",
                         (owner["user_id"], owner["bca"], owner["belanja"]))
        conn.commit()
    corrected_row = tx_for(corrected["event_id"])
    client.patch(f"/api/transactions/{corrected_row['id']}", json={"category_id": owner["jajan"], "notes": "Ngopi"})
    payload = notification("You've paid Rp100.000 to Kedai Awan", seconds=59)
    payload["post_time"] = datetime.fromisoformat(payload["post_time"]) + timedelta(days=2)
    with db_conn() as conn:
        context = load_context(conn.cursor(), owner["user_id"], collect_facts(payload))
    history = context["similar_history"]
    assert [row["description"] for row in history] == ["Ngopi", "Kedai Awan"]
    assert [row["owner_confirmed"] for row in history] == [True, False]
    assert {row["match"] for row in history} == {"same_key"}
    assert context["memory"][0]["match_key"] == "merchant:kedai awan" and context["memory"][0]["source"] == "owner"
    assert str(tx_for(other["event_id"])["id"]) != str(corrected_row["id"])


def test_context_budget_drops_history_then_memory():
    from app.services.notification_context import CONTEXT_BYTE_LIMIT, context_size, fit_context_budget
    context = {
        "notification": "x", "memory": [{"description": "m" * 500} for _ in range(30)],
        "similar_history": [{"description": "h" * 2000} for _ in range(50)],
        "_classification_key": {"match_key": "k" * 100000},
    }
    fit_context_budget(context)
    assert context_size(context) <= CONTEXT_BYTE_LIMIT
    assert len(context["memory"]) == 30 and 0 < len(context["similar_history"]) < 50


def test_provider_input_excludes_internal_keys():
    from app.services.openai_notification_provider import model_input
    serialised = model_input({"notification": "x", "_owner_memory_hit": {"memory_id": "secret-internal"}})
    assert "secret-internal" not in serialised and "_owner_memory_hit" not in serialised


def test_invalid_review_threshold_disables_ai(monkeypatch):
    from app.core.config import load_settings
    from app.services.openai_notification_provider import configuration_error
    monkeypatch.setenv("NOTIFICATION_AI_ENABLED", "true")
    monkeypatch.setenv("NOTIFICATION_REVIEW_CONFIDENCE_THRESHOLD", "1.5")
    assert configuration_error(load_settings()) == "processor_configuration_invalid"
    monkeypatch.setenv("NOTIFICATION_REVIEW_CONFIDENCE_THRESHOLD", "0.75")
    assert load_settings().notification_review_confidence_threshold == 0.75


# --- Purpose questions -------------------------------------------------------------------------


@pytest.mark.parametrize("text,package", [
    (GROCERIES, BCA),
    ("Transfer QRIS Rp 50.000 berhasil", "com.gojek.gopay"),
])
def test_merchant_less_payments_get_a_purpose_question(client, owner, text, package):
    result = ingest(client, {**notification(text, package), "source_version": "1.8.0"})["results"][0]
    question = result["question"]
    assert question["type"] == "purpose" and question["prompt"].startswith("Apa ini? Rp")
    assert question["transaction_id"] == result["transaction_id"] and question["allows_text"] is True
    assert result["review_status"] == "needs_review"


def test_named_merchant_gets_no_question(client, owner):
    result = ingest(client, {**notification(KEDAI), "source_version": "1.8.0"})["results"][0]
    assert "question" not in result and result["review_status"] == "auto"
    assert event_row(result["event_id"])["active_question"] is None


def test_masked_incoming_after_sender_answer_gets_question_with_repayment(client, owner):
    pending = ingest(client, bca(MASKED_INCOMING, version="1.8.0"))["results"][0]
    assert pending["status"] == "needs_confirmation" and pending["confirmation"]["type"] == "sender"
    confirmed = client.post(f"/api/ingest/notifications/{pending['event_id']}/confirm-sender", json={
        "question_id": pending["confirmation"]["question_id"], "reply_id": str(uuid4()),
        "action": "name", "name": "Budi Contoh"})
    assert confirmed.status_code == 200, confirmed.text
    run_claim()
    result = result_of(client, pending["event_id"])
    question = result["question"]
    assert question["prompt"] == "Apa ini? Rp57.000 masuk ke BCA Harian dari Budi Contoh"
    repayment = [item for item in question["suggestions"] if item["kind"] == "repayment"]
    assert repayment and repayment[0]["category_id"] == owner["patungan"]
    assert repayment[0]["notes"] == "Ganti uang dari Budi Contoh"


def test_suggestions_rank_by_time_amount_and_signature(client, owner):
    from app.services.merchant_memory import local_time_features
    payload = bca(GROCERIES)
    weekday, hour = local_time_features(datetime.fromisoformat(payload["post_time"]))
    add_feedback(owner, notes="makan kafe senja", category="jajan", amount=200000, weekday=weekday, hour=hour, times=3)
    add_feedback(owner, notes="beli roti bakar", category="jajan", amount=30000, weekday=weekday % 7 + 1,
                 hour=(hour + 8) % 24, times=4)
    add_feedback(owner, notes="tidak mirip", category="makan", amount=5, weekday=weekday % 7 + 1,
                 hour=(hour + 8) % 24, signature="gopay|expense|qris", account="gopay")
    suggestions = ingest(client, {**payload, "source_version": "1.8.0"})["results"][0]["question"]["suggestions"]
    assert [item["label"] for item in suggestions] == ["makan kafe senja", "beli roti bakar"]
    assert [item["id"] for item in suggestions] == ["s1", "s2"]
    assert suggestions[0]["category_name"] == "Jajan & Nongkrong" and suggestions[0]["kind"] == "history"


def test_cold_start_offers_only_current_classification(client, owner):
    result = ingest(client, bca(SHOPPING, version="1.8.0"))["results"][0]
    [current] = result["question"]["suggestions"]
    assert current["kind"] == "current" and current["category_id"] == owner["belanja"]


def test_old_companion_sees_plain_recorded_result(client, owner):
    result = ingest(client, bca(GROCERIES, version="1.7.0"))["results"][0]
    assert result["status"] == "recorded" and "question" not in result and "review_status" not in result
    assert event_row(result["event_id"])["active_question"]["type"] == "purpose"


def open_question(client, owner, text=GROCERIES, **kwargs):
    result = ingest(client, bca(text, version="1.8.0", **kwargs))["results"][0]
    return result["event_id"], result["question"]


def answer(client, event_id, body):
    return client.post(f"/api/ingest/notifications/{event_id}/answer-purpose", json=body)


def test_answer_with_suggestion_updates_row_feedback_memory_and_result(client, owner):
    add_feedback(owner, notes="makan kafe senja", category="jajan", amount=200000, weekday=3, hour=19)
    event_id, question = open_question(client, owner)
    body = {"question_id": question["question_id"], "reply_id": str(uuid4()), "suggestion_id": "s1"}
    response = answer(client, event_id, body)
    assert response.status_code == 200, response.text
    payload = response.json()
    assert "question" not in payload["result"] and payload["result"]["review_status"] == "reviewed"
    assert payload["result"]["description"] == "makan kafe senja"
    assert payload["answer"]["status"] == "applied" and payload["answer"]["category_id"] == owner["jajan"]
    row = tx_for(event_id)
    assert (row["notes"], str(row["category_id"]), row["classified_by"], row["review_status"]) == (
        "makan kafe senja", owner["jajan"], "owner", "reviewed")
    assert row["owner_edited_at"] is not None
    stored = [item for item in feedback_rows(owner) if item["kind"] == "purpose_answer"]
    assert len(stored) == 1 and stored[0]["suggestion_id"] == "s1" and stored[0]["after_notes"] == "makan kafe senja"
    assert memory_rows(owner)["signature:bca|expense|groceries"]["source"] == "owner"
    replay = answer(client, event_id, body)
    assert replay.status_code == 200 and replay.json() == payload
    assert len([item for item in feedback_rows(owner) if item["kind"] == "purpose_answer"]) == 1
    conflict = answer(client, event_id, {**body, "suggestion_id": None, "text": "lain"})
    assert conflict.status_code == 409 and conflict.json()["detail"]["code"] == "purpose_answer_conflict"
    again = answer(client, event_id, {**body, "reply_id": str(uuid4())})
    assert again.status_code == 409 and again.json()["detail"]["code"] == "stale_question"


def test_answer_with_text_uses_normaliser(client, owner, monkeypatch):
    from app.main import app
    event_id, question = open_question(client, owner)
    provider = AsyncMock()
    provider.normalize_purpose.return_value = normalization(
        merchant="Arena Futsal", description="Sewa lapangan Arena Futsal", category_id=owner["olahraga"], kakeibo="culture",
        offset_category_id=None, confidence=0.88)
    enable_ai(monkeypatch)
    monkeypatch.setattr(app.state, "notification_provider", provider)
    response = answer(client, event_id, {"question_id": question["question_id"], "reply_id": str(uuid4()),
                                         "text": "  sewa lapangan arena futsal "})
    assert response.status_code == 200, response.text
    context = provider.normalize_purpose.await_args.args[0]
    assert context["answer"] == "sewa lapangan arena futsal" and context["direction"] == "expense"
    assert {row["id"] for row in context["categories"]} >= {owner["olahraga"], owner["jajan"]}
    row = tx_for(event_id)
    assert (row["merchant"], row["notes"], str(row["category_id"]), row["kakeibo_type"], row["review_status"]) == (
        "Arena Futsal", "Sewa lapangan Arena Futsal", owner["olahraga"], "culture", "reviewed")
    feedback = [item for item in feedback_rows(owner) if item["kind"] == "purpose_answer"][0]
    assert feedback["answer_text"] == "sewa lapangan arena futsal"
    memory = memory_rows(owner)
    assert "merchant:arena futsal" in memory and "signature:bca|expense|groceries" in memory
    assert event_row(event_id)["interpretation"]["purpose_answer"]["normalizer"] == "ok"


@pytest.mark.parametrize("failure", ["provider", "invalid_reference", "ai_disabled"])
def test_normaliser_failure_keeps_text_and_flags_row(client, owner, monkeypatch, failure):
    from app.main import app
    event_id, question = open_question(client, owner, SHOPPING)
    provider = AsyncMock()
    if failure == "provider":
        provider.normalize_purpose.side_effect = ProviderError("provider_timeout", transient=True)
    else:
        provider.normalize_purpose.return_value = normalization(
            merchant=None, description="Sesuatu", category_id=str(uuid4()), kakeibo=None,
            offset_category_id=None, confidence=0.5)
    if failure != "ai_disabled":
        enable_ai(monkeypatch)
    monkeypatch.setattr(app.state, "notification_provider", provider)
    response = answer(client, event_id, {"question_id": question["question_id"], "reply_id": str(uuid4()),
                                         "text": "kado ulang tahun"})
    assert response.status_code == 200, response.text
    row = tx_for(event_id)
    assert (row["notes"], str(row["category_id"]), row["review_status"], row["classified_by"]) == (
        "kado ulang tahun", owner["belanja"], "needs_review", "owner")
    assert response.json()["answer"]["status"] == "needs_review"
    assert event_row(event_id)["active_question"] is None
    assert memory_rows(owner) == {}
    assert provider.normalize_purpose.await_count == (0 if failure == "ai_disabled" else 1)


def test_answer_validation_scoping_and_listing(client, owner):
    from app.main import app
    from app.services.auth import get_current_user
    first, question = open_question(client, owner)
    second, _ = open_question(client, owner, SHOPPING, seconds=40)
    third, third_question = open_question(client, owner, "You spent IDR 12,000.00 at Miscellaneous.", seconds=50)
    base = {"question_id": question["question_id"], "reply_id": str(uuid4())}
    assert answer(client, first, base).status_code == 422
    assert answer(client, first, {**base, "suggestion_id": "s1", "text": "dua"}).status_code == 422
    assert answer(client, first, {**base, "text": "  "}).status_code == 422
    assert answer(client, first, {**base, "text": "a\u0000b"}).status_code == 422
    assert answer(client, first, {**base, "suggestion_id": "s9"}).json()["detail"]["code"] == "unknown_suggestion"
    stale = answer(client, first, {**base, "question_id": str(uuid4()), "text": "kopi"})
    assert stale.status_code == 409 and stale.json()["detail"]["code"] == "stale_question"
    listed = client.get("/api/ingest/purpose-questions").json()["questions"]
    assert {item["event_id"] for item in listed} == {first, second, third}
    assert answer(client, third, {"question_id": third_question["question_id"], "reply_id": str(uuid4()),
                                  "text": "parkir"}).status_code == 200
    listed = client.get("/api/ingest/purpose-questions").json()["questions"]
    assert {item["event_id"] for item in listed} == {first, second}
    assert listed[0]["transaction"]["account_name"] == "BCA Harian"
    app.dependency_overrides[get_current_user] = lambda: {"id": str(uuid4()), "currency": "IDR"}
    try:
        assert answer(client, first, {**base, "text": "kopi"}).status_code == 404
        assert client.get("/api/ingest/purpose-questions").json()["questions"] == []
    finally:
        app.dependency_overrides[get_current_user] = lambda: {"id": owner["user_id"], "currency": "IDR"}


def test_ledger_edit_closes_open_question(client, owner):
    event_id, _ = open_question(client, owner)
    row = tx_for(event_id)
    response = client.patch(f"/api/transactions/{row['id']}", json={"notes": "Belanja dapur",
                                                                      "category_id": owner["belanja"]})
    assert response.status_code == 200, response.text
    assert event_row(event_id)["active_question"] is None
    assert [item["kind"] for item in feedback_rows(owner)] == ["correction"]
    assert result_of(client, event_id)["review_status"] == "reviewed"


# --- Keys, corpus and migration ----------------------------------------------------------------


def test_real_corpus_match_keys():
    from app.services.merchant_memory import classification_key
    from app.services.notification_evidence import collect_facts
    from tests.fixtures.real_notifications import OWNER_ALIAS, REAL_NOTIFICATIONS
    moment = datetime.fromisoformat("2026-09-29T13:08:51+07:00")
    kinds = {}
    for label, package, title, text, *_ in REAL_NOTIFICATIONS:
        facts = collect_facts({"package_name": package, "title": title, "body_text": text, "post_time": moment})
        key = classification_key(facts, [OWNER_ALIAS])
        kinds[label] = key["key_kind"] if key else None
    for label in ("mybca-spent-misc", "mybca-spent-shopping", "mybca-spent-payment", "mybca-spent-account-transfer",
                  "mybca-received-third-party", "jago-debit-card", "mybca-salary"):
        assert kinds[label] == "signature", label
    assert kinds["shopeepay-top-up"] == "merchant"
    for label in ("mybca-received-masked-owner", "jago-has-sent-owner", "jago-pocket-to-pocket",
                  "mybca-rdn-earning", "stockbit-deposit-confirmed", "jago-transferred-to-owner"):
        assert kinds[label] is None, label


def test_v29_is_idempotent(client):
    from app.db.pool import db_conn
    versioned = ROOT / "db/migrations/V29__merchant_memory_and_purpose_questions.sql"
    with db_conn() as conn:
        conn.execute(versioned.read_text())
        conn.execute(versioned.read_text())
        triggers = conn.execute(
            "SELECT COUNT(*) AS n FROM pg_trigger WHERE tgname = 'classification_feedback_append_only'").fetchone()["n"]
        conn.rollback()
    assert triggers == 1
