"""History re-enrichment suggestions (phase 7).

All texts, names and amounts are synthetic. The owner of `notification_owner` is "Raka Purnama".
"""
import json
import logging
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4
from zoneinfo import ZoneInfo

import psycopg
import pytest

from app.services.history_suggestions import HistorySuggestionBatch
from app.services.openai_notification_provider import ProviderError
from test_ingestion_memory_and_purpose import (  # noqa: F401 - fixtures
    GROCERIES, bca, feedback_rows, memory_rows, owner, tx_for,
)
from test_notification_processing_database import ingest, notification_owner  # noqa: F401 - fixture

ROOT = Path(__file__).resolve().parents[2]
TZ = ZoneInfo("Asia/Jakarta")


def today():
    return datetime.now(TZ).date()


def window(days=30):
    return {"from_date": (today() - timedelta(days=days)).isoformat(), "to_date": today().isoformat()}


def add_row(owner, *, notes, category, amount=50000, kind="expense", merchant=None, days_ago=1,
            classified_by="ai", pillar=None, **columns):
    from app.db.pool import db_conn
    values = {
        "id": str(uuid4()), "user_id": owner["user_id"], "account_id": owner["bca"], "type": kind,
        "amount": amount, "notes": notes, "merchant": merchant, "category_id": owner[category] if category else None,
        "kakeibo_type": pillar, "date": datetime.now(TZ) - timedelta(days=days_ago, hours=1),
        "source": "notification", "review_status": "auto", "classified_by": classified_by, **columns,
    }
    with db_conn() as conn:
        conn.execute(
            f"INSERT INTO transactions ({', '.join(values)}) VALUES ({', '.join(['%s'] * len(values))})",
            list(values.values()),
        )
        conn.commit()
    return values["id"]


def remember(owner, match_key, *, category, notes, merchant=None, pillar=None, offset=None):
    from app.services.merchant_memory import upsert_memory
    from app.db.pool import db_conn
    with db_conn() as conn:
        with conn.cursor() as cur:
            upsert_memory(cur, owner["user_id"], match_key, {
                "merchant": merchant, "notes": notes, "category_id": owner[category], "kakeibo_type": pillar,
                "offset_category_id": owner[offset] if offset else None,
            }, "owner")
        conn.commit()


def transaction(tx_id):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute("SELECT * FROM transactions WHERE id = %s", (tx_id,)).fetchone()


def suggestion_rows(owner, status=None):
    from app.db.pool import db_conn
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM transaction_suggestions WHERE user_id = %s ORDER BY created_at, id", (owner["user_id"],),
        ).fetchall()
    return [row for row in rows if status is None or row["status"] == status]


def start(client, **body):
    return client.post("/api/review/suggestions/runs", json={**window(), "confirm": True, **body})


def latest(client):
    response = client.get("/api/review/suggestions/runs/latest")
    assert response.status_code == 200, response.text
    return response.json()["run"]


def pending_by_transaction(client):
    response = client.get("/api/review/suggestions")
    assert response.status_code == 200, response.text
    return {item["transaction_id"]: item for group in response.json()["groups"] for item in group["suggestions"]}


@pytest.fixture
def ai(monkeypatch):
    """AI enabled with a mocked provider; no model is ever called."""
    from app.main import app
    from app.services import history_suggestions as history

    monkeypatch.setattr(history, "settings", replace(
        history.settings, notification_ai_enabled=True, notification_ai_model="gpt-5.6-luna",
        notification_ai_reasoning_effort="low", openai_api_key="synthetic-test-credential",
        history_suggestion_batch_size=2,
    ))
    provider = AsyncMock()
    monkeypatch.setattr(app.state, "notification_provider", provider, raising=False)
    return provider


def model_batch(items):
    return HistorySuggestionBatch.model_validate_json(json.dumps({"suggestions": items}))


def model_item(ref, category_id, *, description="Makan siang Warung Bu Sri", merchant="Warung Bu Sri",
               kakeibo="need", confidence=0.9, offset=None):
    return {"ref": ref, "merchant": merchant, "description": description, "category_id": category_id,
            "kakeibo": kakeibo, "offset_category_id": offset, "confidence": confidence,
            "reason": "Mirip transaksi yang Anda klasifikasikan"}


# --- Eligibility and estimate ----------------------------------------------------------------------


def test_eligibility_excludes_owner_movement_fee_system_and_out_of_window_rows(client, owner):
    from app.db.pool import db_conn
    remember(owner, "merchant:kedai awan", category="jajan", notes="Ngopi di Kedai Awan", merchant="Kedai Awan",
             pillar="want")
    eligible = add_row(owner, notes="Pengeluaran makanan dan minuman", merchant="Kedai Awan", category="makan")
    add_row(owner, notes="Kedai Awan", merchant="Kedai Awan", category="makan", classified_by="owner")
    add_row(owner, notes="Kedai Awan", merchant="Kedai Awan", category="makan", owner_edited_at=datetime.now(TZ))
    add_row(owner, notes="Kedai Awan", merchant="Kedai Awan", category="makan", classified_by="system")
    add_row(owner, notes="Kedai Awan", merchant="Kedai Awan", category="makan", days_ago=60)
    moved = client.post("/api/movements", json={
        "source_account_id": owner["bca"], "target_account_id": owner["gopay"], "amount": 40000, "notes": "Kedai Awan",
    })
    assert moved.status_code == 200, moved.text
    movement_id = moved.json()["movement_id"]
    with db_conn() as conn:
        conn.execute("UPDATE transactions SET classified_by = 'ai', merchant = 'Kedai Awan' WHERE movement_id = %s",
                     (movement_id,))
        conn.commit()
    add_row(owner, notes="Biaya transfer", merchant="Kedai Awan", category="expense", fee_for_movement_id=movement_id)

    estimate = client.post("/api/review/suggestions/runs", json=window())
    assert estimate.status_code == 200, estimate.text
    data = estimate.json()["estimate"]
    assert (data["eligible_count"], data["selected_count"], data["memory_count"], data["ai_rows"]) == (1, 1, 1, 0)
    assert data["ai_available"] is False and data["ai_unavailable_reason"] == "ai_disabled"
    assert "run" not in estimate.json() and latest(client) is None and suggestion_rows(owner) == []

    assert start(client).status_code == 200
    [suggestion] = suggestion_rows(owner)
    assert str(suggestion["transaction_id"]) == eligible


def test_estimate_counts_model_calls_and_respects_max_rows(client, owner, ai, monkeypatch):
    from app.services import history_suggestions as history
    monkeypatch.setattr(history, "settings", replace(history.settings, history_suggestion_max_rows=3))
    for day in range(1, 6):
        add_row(owner, notes="Belanja", category="belanja", days_ago=day)
    data = client.post("/api/review/suggestions/runs", json=window()).json()["estimate"]
    assert (data["eligible_count"], data["selected_count"], data["truncated"]) == (5, 3, True)
    assert (data["ai_rows"], data["ai_calls"], data["batch_size"], data["ai_available"]) == (3, 2, 2, True)
    ai.suggest_history.assert_not_awaited()


# --- Generation ------------------------------------------------------------------------------------


def test_memory_first_rule_suggestion_skips_the_model(client, owner, ai):
    remember(owner, "merchant:kedai awan", category="jajan", notes="Ngopi di Kedai Awan", merchant="Kedai Awan",
             pillar="want")
    tx_id = add_row(owner, notes="Pengeluaran makanan dan minuman", merchant="Kedai Awan", category="makan")
    response = start(client)
    assert response.status_code == 200, response.text
    ai.suggest_history.assert_not_awaited()
    item = pending_by_transaction(client)[tx_id]
    assert item["source"] == "memory" and item["confidence"] is None
    assert item["before"] == {"merchant": "Kedai Awan", "notes": "Pengeluaran makanan dan minuman",
                              "category_id": owner["makan"], "category_name": "Makanan & Minuman", "kakeibo": "need"}
    assert (item["after"]["notes"], item["after"]["category_id"], item["after"]["kakeibo"]) == (
        "Ngopi di Kedai Awan", owner["jajan"], "want")
    assert item["reason"] == "Sama dengan pilihan Anda untuk Kedai Awan"
    run = latest(client)
    assert (run["status"], run["memory_count"], run["ai_calls"], run["error_code"]) == ("completed", 1, 0, None)
    assert transaction(tx_id)["category_id"] is not None and str(transaction(tx_id)["category_id"]) == owner["makan"]


def test_signature_memory_and_repayment_and_cafe_rules(client, owner):
    remember(owner, "signature:bca|expense|groceries", category="belanja", notes="Belanja dapur mingguan")
    recorded = ingest(client, bca(GROCERIES, days=(today() - date(2026, 9, 28)).days - 1))["results"][0]
    assert recorded["status"] == "recorded", recorded
    groceries = str(tx_for(recorded["event_id"])["id"])
    repayment = add_row(owner, notes="Transfer masuk tf makan", kind="income", category="income", amount=57000)
    cafe = add_row(owner, notes="Pengeluaran makanan dan minuman", merchant="Kopi Senja", category="makan")
    assert start(client).status_code == 200
    pending = pending_by_transaction(client)
    assert pending[groceries]["source"] == "memory"
    assert (pending[groceries]["after"]["notes"], pending[groceries]["after"]["category_id"]) == (
        "Belanja dapur mingguan", owner["belanja"])
    assert pending[repayment]["source"] == "rule"
    assert (pending[repayment]["after"]["category_id"], pending[repayment]["after"]["offset_category_id"]) == (
        owner["patungan"], owner["makan"])
    assert pending[repayment]["after"]["kakeibo"] is None
    assert (pending[cafe]["after"]["category_id"], pending[cafe]["after"]["kakeibo"]) == (owner["jajan"], "want")
    # The reason never quotes the notification.
    assert all("IDR" not in row["reason"] and "187" not in row["reason"] for row in suggestion_rows(owner))


def test_rules_read_the_recording_notification_text():
    from app.services.history_suggestions import rule_proposal
    categories = {
        "patungan": {"id": "patungan", "name": "Patungan & Ganti Uang", "kind": "income", "kakeibo_type": None,
                     "offsets_spending": True, "is_archived": False, "is_excluded_from_budget": False},
        "makan": {"id": "makan", "name": "Makanan & Minuman", "kind": "expense", "kakeibo_type": "need",
                  "offsets_spending": False, "is_archived": False, "is_excluded_from_budget": False},
    }
    row = {"id": "row", "type": "income", "merchant": None, "notes": "Transfer masuk", "category_id": None,
           "event_text": "Dana masuk Rp57.000 berita: patungan bakso"}
    proposal = rule_proposal(row, categories, None)
    assert (proposal.category_id, proposal.offset_category_id, proposal.source) == ("patungan", "makan", "rule")
    assert rule_proposal({**row, "event_text": "Gaji Oktober"}, categories, None) is None
    # "Pembayaran" contains "bayar" only inside a word, so salary-like texts are not repayments.
    assert rule_proposal({**row, "event_text": "Pembayaran gaji"}, categories, None) is None


def test_groups_never_mix_offsets():
    from app.services.history_suggestions import group_suggestions
    base = {
        "transaction_id": uuid4(), "type": "income", "amount": 57000, "date": datetime.now(TZ), "account_name": "BCA Harian",
        "proposal_source": "rule", "confidence": None, "reason": "r", "before_merchant": None, "before_notes": "Transfer masuk",
        "before_category_id": None, "before_category_name": None, "before_kakeibo_type": None,
        "proposed_merchant": None, "proposed_notes": "Ganti uang patungan", "proposed_category_id": "c-repay",
        "proposed_category_name": "Patungan & Ganti Uang", "proposed_kakeibo_type": None,
    }
    rows = [
        {**base, "id": uuid4(), "proposed_offset_category_id": "c-food", "proposed_offset_category_name": "Makanan & Minuman"},
        {**base, "id": uuid4(), "proposed_offset_category_id": None, "proposed_offset_category_name": None},
    ]
    groups = group_suggestions(rows)
    assert sorted(group["offset_category_names"] for group in groups) == [[], ["Makanan & Minuman"]]


def test_provider_batches_are_validated_and_stored(client, owner, ai, caplog):
    rows = [add_row(owner, notes="Belanja", category="belanja", days_ago=day, merchant=None) for day in (1, 2)]
    rows.append(add_row(owner, notes="Belanja titipan Raka Purnama", category="belanja", days_ago=3))
    seen_contexts = []

    async def suggest(context):
        seen_contexts.append(context)
        refs = [row["ref"] for row in context["rows"]]
        items = [model_item(refs[0], owner["makan"])]
        if len(refs) > 1:
            items.append(model_item(refs[1], str(uuid4())))  # an id the owner does not have
        return model_batch(items)

    ai.suggest_history.side_effect = suggest
    caplog.set_level(logging.INFO)
    assert start(client).status_code == 200
    run = latest(client)
    assert (run["status"], run["ai_calls"], run["ai_count"], run["processed_count"], run["error_code"]) == (
        "completed", 2, 2, 3, None)
    assert run["prompt_version"] == "history-suggestions-1" and run["model"] == "gpt-5.6-luna"
    stored = suggestion_rows(owner)
    assert {str(row["transaction_id"]) for row in stored} == {rows[0], rows[2]}
    assert all(row["proposal_source"] == "ai" and float(row["confidence"]) == 0.9 for row in stored)
    assert all(row["proposed_merchant"] == "Warung Bu Sri" for row in stored)
    first = seen_contexts[0]
    assert set(first) == {"rows", "categories", "offset_categories", "memory", "owner_history"}
    assert {row["direction"] for row in first["rows"]} == {"expense"} and len(first["rows"]) == 2
    assert all(category["default_kakeibo"] is not None or category["kind"] == "income"
               for category in first["categories"] if category["name"] == "Makanan & Minuman")
    # The provider input goes through the existing redaction: the owner's name never leaves.
    assert "Raka" not in json.dumps(seen_contexts) and "[SELF]" in json.dumps(seen_contexts[1])
    # Logs carry counts and codes only.
    assert "Warung" not in caplog.text and "Belanja" not in caplog.text


def test_low_confidence_and_unchanged_proposals_are_skipped(client, owner, ai):
    low = add_row(owner, notes="Belanja", category="belanja", days_ago=1)
    same = add_row(owner, notes="Makan siang Warung Bu Sri", merchant="Warung Bu Sri", category="makan", days_ago=2)

    async def suggest(context):
        refs = {row["description"]: row["ref"] for row in context["rows"]}
        return model_batch([model_item(refs["Belanja"], owner["makan"], confidence=0.3),
                            model_item(refs["Makan siang Warung Bu Sri"], owner["makan"])])

    ai.suggest_history.side_effect = suggest
    assert start(client).status_code == 200
    assert suggestion_rows(owner) == []
    run = latest(client)
    assert (run["unchanged_count"], run["ai_count"], run["processed_count"]) == (1, 0, 2)
    assert transaction(low)["notes"] == "Belanja" and transaction(same)["review_status"] == "auto"


def test_provider_failure_completes_with_memory_suggestions_and_error_code(client, owner, ai):
    remember(owner, "merchant:kedai awan", category="jajan", notes="Ngopi di Kedai Awan", merchant="Kedai Awan",
             pillar="want")
    remembered = add_row(owner, notes="Pengeluaran makanan dan minuman", merchant="Kedai Awan", category="makan")
    add_row(owner, notes="Belanja", category="belanja", days_ago=2)
    ai.suggest_history.side_effect = ProviderError("provider_unavailable", transient=True)
    assert start(client).status_code == 200
    run = latest(client)
    assert (run["status"], run["error_code"], run["memory_count"], run["ai_count"]) == (
        "completed", "provider_unavailable", 1, 0)
    assert [str(row["transaction_id"]) for row in suggestion_rows(owner)] == [remembered]


def test_ai_disabled_records_the_reason_and_keeps_memory_suggestions(client, owner):
    remember(owner, "merchant:kedai awan", category="jajan", notes="Ngopi di Kedai Awan", merchant="Kedai Awan")
    add_row(owner, notes="Pengeluaran makanan dan minuman", merchant="Kedai Awan", category="makan")
    add_row(owner, notes="Belanja", category="belanja", days_ago=2)
    assert start(client).status_code == 200
    run = latest(client)
    assert (run["status"], run["error_code"], run["memory_count"], run["model"]) == ("completed", "ai_disabled", 1, None)


def test_one_active_run_per_owner_and_interrupted_runs_expire(client, owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO suggestion_runs (user_id, from_date, to_date, status, max_rows, lease_token, lease_expires_at)
               VALUES (%s, %s, %s, 'running', 300, %s, NOW() + INTERVAL '5 minutes')""",
            (owner["user_id"], today(), today(), str(uuid4())),
        )
        conn.commit()
    blocked = start(client)
    assert blocked.status_code == 409 and blocked.json()["detail"]["code"] == "suggestion_run_active"
    assert latest(client)["status"] == "running"
    with db_conn() as conn:
        conn.execute("UPDATE suggestion_runs SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE user_id = %s",
                     (owner["user_id"],))
        conn.commit()
    assert latest(client)["error_code"] == "run_interrupted"
    assert start(client).status_code == 200
    assert latest(client)["status"] == "completed"
    with db_conn() as conn:
        with pytest.raises(psycopg.errors.UniqueViolation):
            for _ in range(2):
                conn.execute(
                    """INSERT INTO suggestion_runs (user_id, from_date, to_date, status, max_rows)
                       VALUES (%s, %s, %s, 'running', 300)""",
                    (owner["user_id"], today(), today()),
                )
        conn.rollback()


# --- Decisions -------------------------------------------------------------------------------------


def two_kedai_suggestions(client, owner):
    remember(owner, "merchant:kedai awan", category="jajan", notes="Ngopi di Kedai Awan", merchant="Kedai Awan",
             pillar="want")
    ids = [add_row(owner, notes="Pengeluaran makanan dan minuman", merchant="Kedai Awan", category="makan",
                   days_ago=day) for day in (1, 2)]
    assert start(client).status_code == 200
    groups = client.get("/api/review/suggestions").json()["groups"]
    [group] = groups
    assert (group["count"], group["total"], group["label"], group["category"]["id"]) == (2, 100000, "Kedai Awan", owner["jajan"])
    assert group["before"] == [{"label": "Pengeluaran makanan dan minuman", "category_name": "Makanan & Minuman", "count": 2}]
    return ids, [item["id"] for item in group["suggestions"]]


def test_accept_applies_through_owner_edit_hooks(client, owner):
    from app.db.pool import db_conn
    tx_ids, suggestion_ids = two_kedai_suggestions(client, owner)
    with db_conn() as conn:
        # The owner's later decision for the key must come from the acceptance, not the seed.
        conn.execute("DELETE FROM merchant_memory WHERE user_id = %s", (owner["user_id"],))
        conn.commit()
    response = client.post("/api/review/suggestions/accept", json={"suggestion_ids": suggestion_ids})
    assert response.status_code == 200, response.text
    assert response.json() == {"ok": True, "accepted": 2, "skipped": [], "corrections": 2}
    for tx_id in tx_ids:
        row = transaction(tx_id)
        assert (row["notes"], row["merchant"], str(row["category_id"])) == ("Ngopi di Kedai Awan", "Kedai Awan", owner["jajan"])
        # "want" is the category's own pillar, so the row follows its category.
        assert row["kakeibo_type"] is None
        assert (row["classified_by"], row["review_status"], row["classification_confidence"]) == ("owner", "reviewed", None)
        assert row["owner_edited_at"] is not None
    feedback = feedback_rows(owner)
    assert len(feedback) == 2 and {row["kind"] for row in feedback} == {"accepted_suggestion"}
    assert {str(row["history_suggestion_id"]) for row in feedback} == set(suggestion_ids)
    assert {(row["before_notes"], row["after_notes"], row["after_kakeibo_type"]) for row in feedback} == {
        ("Pengeluaran makanan dan minuman", "Ngopi di Kedai Awan", "want")}
    memory = memory_rows(owner)["merchant:kedai awan"]
    assert (memory["source"], str(memory["category_id"]), memory["notes"]) == ("owner", owner["jajan"], "Ngopi di Kedai Awan")
    assert {row["status"] for row in suggestion_rows(owner)} == {"accepted"}
    again = client.post("/api/review/suggestions/accept", json={"suggestion_ids": suggestion_ids})
    assert again.json()["accepted"] == 0 and sorted(again.json()["skipped"]) == sorted(suggestion_ids)
    assert len(feedback_rows(owner)) == 2


def test_accept_one_row_of_a_group(client, owner):
    tx_ids, suggestion_ids = two_kedai_suggestions(client, owner)
    by_id = {str(row["id"]): str(row["transaction_id"]) for row in suggestion_rows(owner)}
    response = client.post("/api/review/suggestions/accept", json={"suggestion_ids": [suggestion_ids[0]]})
    assert response.status_code == 200 and response.json()["accepted"] == 1
    accepted_tx = by_id[suggestion_ids[0]]
    other_tx = next(tx_id for tx_id in tx_ids if tx_id != accepted_tx)
    assert str(transaction(accepted_tx)["category_id"]) == owner["jajan"]
    assert str(transaction(other_tx)["category_id"]) == owner["makan"]
    assert list(pending_by_transaction(client)) == [other_tx]


def test_reject_is_remembered_by_later_runs(client, owner):
    _, suggestion_ids = two_kedai_suggestions(client, owner)
    response = client.post("/api/review/suggestions/reject", json={"suggestion_ids": suggestion_ids})
    assert response.status_code == 200 and response.json() == {"ok": True, "rejected": 2, "skipped": []}
    assert pending_by_transaction(client) == {}
    assert feedback_rows(owner) == []
    assert start(client).status_code == 200
    assert suggestion_rows(owner, "pending") == []
    assert latest(client)["rejected_before_count"] == 2


def test_later_edit_supersedes_the_suggestion(client, owner):
    tx_ids, suggestion_ids = two_kedai_suggestions(client, owner)
    by_tx = {str(row["transaction_id"]): str(row["id"]) for row in suggestion_rows(owner)}
    edited = client.patch(f"/api/transactions/{tx_ids[0]}", json={"notes": "Makan siang kantor"})
    assert edited.status_code == 200, edited.text
    assert list(pending_by_transaction(client)) == [tx_ids[1]]
    statuses = {str(row["id"]): row["status"] for row in suggestion_rows(owner)}
    assert statuses[by_tx[tx_ids[0]]] == "superseded"
    accepted = client.post("/api/review/suggestions/accept", json={"suggestion_ids": [by_tx[tx_ids[0]]]})
    assert accepted.json()["accepted"] == 0 and accepted.json()["skipped"] == [by_tx[tx_ids[0]]]
    assert transaction(tx_ids[0])["notes"] == "Makan siang kantor"


def test_review_accept_without_change_does_not_supersede(client, owner):
    tx_ids, _ = two_kedai_suggestions(client, owner)
    assert client.post("/api/review/bulk", json={"action": "accept", "transaction_ids": tx_ids}).status_code == 200
    assert set(pending_by_transaction(client)) == set(tx_ids)


def test_other_owners_suggestions_are_not_found(client, owner):
    from app.db.pool import db_conn
    _, suggestion_ids = two_kedai_suggestions(client, owner)
    stranger = str(uuid4())
    with db_conn() as conn:
        conn.execute("INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Sinta', 'x', 'TEST')",
                     (stranger, f"stranger_{uuid4().hex[:8]}"))
        conn.commit()
    from app.main import app
    from app.services.auth import get_current_user
    original = app.dependency_overrides[get_current_user]
    app.dependency_overrides[get_current_user] = lambda: {"id": stranger, "name": "Sinta", "currency": "IDR"}
    try:
        for action in ("accept", "reject"):
            response = client.post(f"/api/review/suggestions/{action}", json={"suggestion_ids": suggestion_ids})
            assert response.status_code == 404, response.text
            assert response.json()["detail"]["code"] == "suggestions_not_found"
        assert client.get("/api/review/suggestions").json()["groups"] == []
        assert client.get("/api/review/suggestions/runs/latest").json()["run"] is None
    finally:
        app.dependency_overrides[get_current_user] = original
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (stranger,))
            conn.commit()
    assert {row["status"] for row in suggestion_rows(owner)} == {"pending"}


@pytest.mark.parametrize("body", [
    {"from_date": "2026-10-10", "to_date": "2026-10-01"},
    {"from_date": "2024-01-01", "to_date": "2026-01-01"},
    {"from_date": "2026-10-01", "to_date": "2999-01-01"},
    {"from_date": "2026-10-01", "to_date": "2026-10-02", "max_rows": 5},
    {"from_date": "not-a-date", "to_date": "2026-10-02"},
])
def test_invalid_windows_are_rejected(client, owner, body):
    assert client.post("/api/review/suggestions/runs", json=body).status_code == 422


@pytest.mark.parametrize("ids", [[], ["same", "same"], ["many"] * 201, ["not-a-uuid"]])
def test_invalid_decision_bodies_are_rejected(client, owner, ids):
    same = str(uuid4())
    values = [same if value == "same" else str(uuid4()) if value == "many" else value for value in ids]
    for action in ("accept", "reject"):
        assert client.post(f"/api/review/suggestions/{action}", json={"suggestion_ids": values}).status_code == 422


def test_invalid_configuration_disables_runs(client, owner, monkeypatch):
    from app.core.config import load_settings
    from app.services import history_suggestions as history
    monkeypatch.setenv("HISTORY_SUGGESTION_BATCH_SIZE", "500")
    broken = load_settings()
    assert broken.history_suggestion_configuration_error == "suggestion_configuration_invalid"
    monkeypatch.setattr(history, "settings", broken)
    response = client.post("/api/review/suggestions/runs", json=window())
    assert response.status_code == 503 and response.json()["detail"]["code"] == "suggestion_configuration_invalid"


# --- Migration -------------------------------------------------------------------------------------


def test_v31_matches_mirror_and_is_idempotent(client, owner):
    from app.db.pool import db_conn
    versioned = ROOT / "db/migrations/V31__history_suggestions.sql"
    mirror = ROOT / "backend/app/db/history_suggestions.sql"
    assert versioned.read_text() == mirror.read_text()
    with db_conn() as conn:
        conn.execute(mirror.read_text())
        conn.execute(mirror.read_text())
        definition = conn.execute(
            """SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint
               WHERE conname = 'classification_feedback_kind_check'""").fetchone()["def"]
        conn.rollback()
    assert "accepted_suggestion" in definition and "purpose_answer" in definition
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO classification_feedback (user_id, kind, transaction_id, history_suggestion_id)
               VALUES (%s, 'accepted_suggestion', %s, %s)""",
            (owner["user_id"], str(uuid4()), str(uuid4())),
        )
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            conn.execute("UPDATE classification_feedback SET after_notes = 'x' WHERE user_id = %s", (owner["user_id"],))
        conn.rollback()
