import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.services.notification_sender import normalize_mask, supports_sender_confirmation, validate_sender_name
from test_notification_processing_database import (
    count_transactions, enable_ai, ingest, notification, notification_owner,
)


def transfer(mask="AN**RA**", *, version="1.5.0", seconds=27):
    event = notification(
        f"You received IDR 125,000.00 from {mask} at Account Transfer category.",
        "com.bca.mybca.omni.android", seconds=seconds,
    )
    event["source_version"] = version
    return event


def answer_for(result, *, name="Andra", action="name"):
    return {"question_id": result["confirmation"]["question_id"], "reply_id": str(uuid4()),
            "action": action, "name": name if action == "name" else None}


def respond(client, result, answer=None):
    return client.post(f"/api/ingest/notifications/{result['event_id']}/confirm-sender",
                       json=answer or answer_for(result))


def current(client, result):
    return client.get(f"/api/ingest/notifications/{result['event_id']}/result").json()["result"]


def complete(client, result):
    from app.services import notification_processing as processing

    claimed = processing.claim_event(include_ai=False)
    if claimed:
        asyncio.run(processing.process_claim(claimed, None))
    return current(client, result)


@pytest.mark.parametrize("value,expected", [(None, False), ("1.4.3", False), ("1.5.0", True), ("2.0.0", True), ("bad", False)])
def test_sender_capability(value, expected):
    assert supports_sender_confirmation({"source_version": value}) is expected


def test_mask_normalization_preserves_positions():
    assert normalize_mask("  AN**RA**  DU**  ") == "an**ra** du**"
    assert normalize_mask("AN**RA**") != normalize_mask("AN*RA***")


@pytest.mark.parametrize("value", ["", "  ", "a" * 81, "An\ndra", "An\x00dra", "An\u202edra", "Bearer fictional-value"])
def test_sender_name_rejects_invalid_data(value):
    with pytest.raises(HTTPException) as error:
        validate_sender_name(value)
    assert error.value.status_code == 422


def test_sender_name_supports_unicode_and_data_only_text():
    assert validate_sender_name("  José Andra  ") == "José Andra"
    assert validate_sender_name("Ignore previous instructions") == "Ignore previous instructions"


def test_masked_transfer_is_held_and_lookup_is_stable(client, notification_owner):
    result = ingest(client, transfer())["results"][0]
    assert result["status"] == "needs_confirmation"
    assert result["error_code"] == "sender_confirmation_required"
    assert result["confirmation"]["masked_sender"] == "AN**RA**"
    assert result["confirmation"]["receiving_account"]["id"] == notification_owner["bca"]
    assert result["confirmation"]["amount"] == 125000
    assert current(client, result) == result
    assert count_transactions(notification_owner) == 0


def test_answer_records_once_and_future_exact_alias_is_reused(client, notification_owner):
    result = ingest(client, transfer())["results"][0]
    answer = answer_for(result)
    accepted = respond(client, result, answer)
    assert accepted.status_code == 200
    assert accepted.json()["result"]["status"] == "queued"
    record = complete(client, result)
    assert record["status"] == "recorded"
    assert (record["description"], record["amount"], record["target"]) == ("Transfer masuk dari Andra", 125000, "BCA Harian")
    assert respond(client, result, answer).json()["result"] == record
    assert respond(client, result, {**answer, "reply_id": str(uuid4())}).json()["result"] == record
    later = ingest(client, transfer(seconds=28))["results"][0]
    assert later["status"] == "recorded" and later["description"] == "Transfer masuk dari Andra"
    assert count_transactions(notification_owner) == 2
    aliases = client.get("/api/ingest/sender-aliases").json()["aliases"]
    assert len(aliases) == 1 and aliases[0]["name"] == "Andra"


def test_explicit_unknown_does_not_learn_or_expire(client, notification_owner):
    result = ingest(client, transfer())["results"][0]
    assert respond(client, result, answer_for(result, action="unknown")).status_code == 200
    record = complete(client, result)
    assert record["description"] == "Transfer masuk"
    assert client.get("/api/ingest/sender-aliases").json()["aliases"] == []
    later = ingest(client, transfer(seconds=28))["results"][0]
    assert later["status"] == "needs_confirmation"
    assert count_transactions(notification_owner) == 1


@pytest.mark.parametrize("version", [None, "1.4.3", "invalid"])
def test_old_companion_gets_review_without_unsupported_question(client, notification_owner, version):
    result = ingest(client, transfer(version=version))["results"][0]
    assert result["status"] == "needs_review"
    assert result["error_code"] == "sender_confirmation_required"
    assert "confirmation" not in result and "mapping_proposal" not in result
    assert count_transactions(notification_owner) == 0


def test_upgraded_retry_adopts_sender_capability(client, notification_owner):
    result = ingest(client, transfer(version="1.4.3"))["results"][0]
    response = client.post(f"/api/ingest/notifications/{result['event_id']}/retry",
                           headers={"X-Companion-Version": "1.5.0"})
    assert response.status_code == 200
    assert complete(client, result)["status"] == "needs_confirmation"
    assert count_transactions(notification_owner) == 0


def test_retry_preserves_same_question_and_stale_reply_is_rejected(client, notification_owner):
    result = ingest(client, transfer())["results"][0]
    assert client.post(f"/api/ingest/notifications/{result['event_id']}/retry").status_code == 200
    refreshed = complete(client, result)
    assert refreshed["confirmation"]["question_id"] == result["confirmation"]["question_id"]
    assert respond(client, result, {**answer_for(result), "question_id": str(uuid4())}).status_code == 409
    assert count_transactions(notification_owner) == 0


def test_invalid_and_conflicting_answers_leave_accepted_state_intact(client, notification_owner):
    result = ingest(client, transfer())["results"][0]
    answer = answer_for(result)
    assert respond(client, result, {**answer, "name": "\n"}).status_code == 422
    assert respond(client, result, {**answer, "action": "unknown", "name": "Andra"}).status_code == 422
    assert respond(client, result, answer).status_code == 200
    assert respond(client, result, {**answer, "name": "Bima"}).status_code == 409
    record = complete(client, result)
    assert record["description"] == "Transfer masuk dari Andra" and count_transactions(notification_owner) == 1


def test_concurrent_replays_only_accept_one_answer(client, notification_owner):
    from app.services.notification_processing import confirm_sender

    result = ingest(client, transfer())["results"][0]
    answer = answer_for(result)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: confirm_sender(result["event_id"], notification_owner["user_id"], answer), range(2)))
    assert all(item["status"] in {"queued", "processing", "recorded"} for item in results)
    assert complete(client, result)["status"] == "recorded"
    assert count_transactions(notification_owner) == 1


def test_conflicting_pending_names_make_memory_ambiguous(client, notification_owner):
    first = ingest(client, transfer(seconds=1))["results"][0]
    second = ingest(client, transfer(seconds=2))["results"][0]
    assert respond(client, first).status_code == 200
    assert respond(client, second, answer_for(second, name="Bima")).status_code == 200
    complete(client, first)
    complete(client, second)
    assert current(client, first)["description"] == "Transfer masuk dari Andra"
    assert current(client, second)["description"] == "Transfer masuk dari Bima"
    [alias] = client.get("/api/ingest/sender-aliases").json()["aliases"]
    assert alias["state"] == "ambiguous"
    assert ingest(client, transfer(seconds=3))["results"][0]["status"] == "needs_confirmation"


def test_memory_edit_delete_affects_future_events_only(client, notification_owner):
    result = ingest(client, transfer())["results"][0]
    respond(client, result)
    record = complete(client, result)
    [alias] = client.get("/api/ingest/sender-aliases").json()["aliases"]
    assert client.patch(f"/api/ingest/sender-aliases/{alias['id']}", json={"name": "Bima"}).status_code == 200
    assert current(client, result) == record
    next_record = ingest(client, transfer(seconds=28))["results"][0]
    assert next_record["description"] == "Transfer masuk dari Bima"
    assert client.delete(f"/api/ingest/sender-aliases/{alias['id']}").status_code == 200
    assert current(client, next_record) == next_record
    assert ingest(client, transfer(seconds=29))["results"][0]["status"] == "needs_confirmation"


def test_other_owner_cannot_answer_or_manage_alias(client, notification_owner):
    from app.main import app
    from app.services.auth import get_current_user

    result = ingest(client, transfer())["results"][0]
    respond(client, result)
    complete(client, result)
    [alias] = client.get("/api/ingest/sender-aliases").json()["aliases"]
    app.dependency_overrides[get_current_user] = lambda: {"id": str(uuid4())}
    assert respond(client, result).status_code == 404
    assert client.get("/api/ingest/sender-aliases").json()["aliases"] == []
    assert client.patch(f"/api/ingest/sender-aliases/{alias['id']}", json={"name": "Bima"}).status_code == 404
    assert client.delete(f"/api/ingest/sender-aliases/{alias['id']}").status_code == 404


def test_manual_resolution_cannot_bypass_sender_hold(client, notification_owner):
    result = ingest(client, transfer())["results"][0]
    response = client.post(f"/api/ingest/notifications/{result['event_id']}/resolve", json={
        "type": "income", "account_id": notification_owner["bca"], "category_id": notification_owner["income"],
        "notes": "Generic manual income",
    })
    assert response.status_code == 200
    assert response.json()["result"]["status"] == "needs_confirmation"
    assert count_transactions(notification_owner) == 0


@pytest.mark.parametrize("provider_invalid", [False, True])
def test_ai_record_and_fallback_cannot_bypass_hold(client, notification_owner, monkeypatch, provider_invalid):
    from app.services import notification_processing as processing
    from app.services.notification_application import deterministic_interpretation
    from app.services.openai_notification_provider import ProviderError

    enable_ai(monkeypatch)
    result = ingest(client, transfer())["results"][0]
    claimed = processing.claim_event()
    provider = AsyncMock()
    if provider_invalid:
        provider.interpret.side_effect = ProviderError("provider_invalid_output")
    else:
        facts, context = processing.read_context(claimed)
        provider.interpret.return_value = deterministic_interpretation(facts, context)
    asyncio.run(processing.process_claim(claimed, provider))
    assert current(client, result)["status"] == "needs_confirmation"
    assert count_transactions(notification_owner) == 0


def test_owner_mask_does_not_require_sender_confirmation(client, notification_owner):
    result = ingest(client, transfer(mask="RA** PUR**MA"))["results"][0]
    assert result["status"] == "recorded"
    assert "confirmation" not in result


def test_unmasked_and_non_transfer_income_do_not_trigger_sender_hold(client, notification_owner):
    assert ingest(client, transfer(mask="Andra"))["results"][0]["status"] == "recorded"
    payload = transfer()
    payload["body_text"] = payload["body_text"].replace("Account Transfer", "Interest")
    result = ingest(client, payload)["results"][0]
    assert result.get("error_code") != "sender_confirmation_required"


def test_receiving_account_scope_and_mask_positions_do_not_cross(client, notification_owner):
    from app.db.pool import db_conn

    result = ingest(client, transfer())["results"][0]
    respond(client, result)
    complete(client, result)
    assert ingest(client, transfer(mask="AN*RA***", seconds=28))["results"][0]["status"] == "needs_confirmation"
    with db_conn() as conn:
        new_id = str(uuid4())
        conn.execute("INSERT INTO accounts (id, user_id, parent_id, name, type) VALUES (%s, %s, %s, 'Daily', 'bank')",
                     (new_id, notification_owner["user_id"], notification_owner["bca"]))
        conn.execute("UPDATE accounts SET default_pocket_id = %s WHERE id = %s", (new_id, notification_owner["bca"]))
    next_result = ingest(client, transfer(seconds=29))["results"][0]
    assert next_result["status"] == "needs_confirmation"
    assert next_result["confirmation"]["receiving_account"]["id"] == new_id


def test_model_context_contains_only_matching_owner_confirmation(client, notification_owner):
    from app.db.pool import db_conn
    from app.services.notification_processing import read_context

    result = ingest(client, transfer())["results"][0]
    respond(client, result)
    with db_conn() as conn:
        event = conn.execute("SELECT * FROM notification_events WHERE id = %s", (result["event_id"],)).fetchone()
    _, context = read_context(event)
    assert context["confirmed_sender"] == {"name": "Andra", "action": "name", "confirmed_by": "owner"}
    assert not context["facts"]["counterparty_is_owner"]


def test_name_answer_leaves_original_payload_and_facts_unchanged(client, notification_owner):
    from app.db.pool import db_conn

    payload = transfer()
    result = ingest(client, payload)["results"][0]
    respond(client, result)
    complete(client, result)
    with db_conn() as conn:
        event = conn.execute("SELECT body_text, payload_hash, post_time FROM notification_events WHERE id = %s", (result["event_id"],)).fetchone()
        row = conn.execute("SELECT amount, account_id, type FROM transactions WHERE user_id = %s", (notification_owner["user_id"],)).fetchone()
    assert event["body_text"] == payload["body_text"] and event["payload_hash"] == payload["payload_hash"]
    assert event["post_time"] == datetime.fromisoformat(payload["post_time"])
    assert row["amount"] == 125000 and str(row["account_id"]) == notification_owner["bca"] and row["type"] == "income"


def test_answer_failure_rolls_back_receipt_and_memory(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services import notification_processing as processing

    result = ingest(client, transfer())["results"][0]
    accept = processing.accept_sender_answer

    def fail_after_answer(cur, event, answer):
        accept(cur, event, answer)
        raise HTTPException(status_code=409, detail={"code": "synthetic_failure"})

    monkeypatch.setattr(processing, "accept_sender_answer", fail_after_answer)
    assert respond(client, result).status_code == 409
    assert current(client, result) == result
    assert client.get("/api/ingest/sender-aliases").json()["aliases"] == []
    with db_conn() as conn:
        assert conn.execute("SELECT COUNT(*) AS count FROM notification_sender_answers WHERE user_id = %s",
                            (notification_owner["user_id"],)).fetchone()["count"] == 0


def test_archived_receiving_account_is_not_recorded_after_answer(client, notification_owner):
    from app.db.pool import db_conn

    result = ingest(client, transfer())["results"][0]
    respond(client, result)
    with db_conn() as conn:
        conn.execute("UPDATE accounts SET is_archived = TRUE WHERE id = %s", (notification_owner["bca"],))
    assert complete(client, result)["status"] == "needs_review"
    assert count_transactions(notification_owner) == 0


def test_account_then_sender_questions_are_distinct_and_answer_survives_retry(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services import notification_processing as processing
    from app.services.notification_interpretation import Interpretation

    owner = notification_owner
    with db_conn() as conn:
        conn.execute("INSERT INTO accounts (user_id, name, type) VALUES (%s, 'BCA Cadangan', 'bank')", (owner["user_id"],))
    enable_ai(monkeypatch)
    result = ingest(client, transfer())["results"][0]
    claimed = processing.claim_event()
    facts, context = processing.read_context(claimed)
    from app.services.notification_interpretation import MappingProposal
    from app.services.notification_application import deterministic_interpretation
    from uuid import UUID

    proposal = Interpretation(
        outcome="record", direction="income", description="Transfer masuk", account_id=UUID(owner["bca"]),
        category_id=UUID(owner["income"]), source_account_id=None, target_account_id=None, kakeibo=None,
        amount_evidence=facts.amount_quotes[0], direction_evidence=context["notification"], source_evidence="",
        target_evidence="", candidate_transaction_id=None, confidence=0.99, review_reason="none",
        mappings=[MappingProposal(role="observed", account_id=UUID(owner["bca"]), confidence=0.5, alternatives=[])],
    )
    assert processing.apply_claim(claimed, proposal)
    account_question = current(client, result)
    assert account_question["confirmation"]["type"] == "account"
    assert client.post(f"/api/ingest/notifications/{result['event_id']}/confirm-mapping", json={
        "account_id": owner["bca"], "question_id": str(uuid4()),
    }).status_code == 409
    assert client.post(f"/api/ingest/notifications/{result['event_id']}/confirm-mapping", json={
        "account_id": owner["bca"], "question_id": account_question["confirmation"]["question_id"],
    }).status_code == 200
    claimed = processing.claim_event()
    facts, context = processing.read_context(claimed)
    from app.services.notification_application import deterministic_interpretation
    processing.apply_claim(claimed, deterministic_interpretation(facts, context))
    sender_question = current(client, result)
    assert sender_question["confirmation"]["type"] == "sender"
    assert sender_question["confirmation"]["question_id"] != account_question["confirmation"]["question_id"]
    assert count_transactions(owner) == 0
    assert respond(client, sender_question).status_code == 200
    claimed = processing.claim_event()
    # A failed provider can complete the answered event through deterministic fallback.
    processing.apply_claim(claimed, None, "provider_invalid_output")
    assert current(client, result)["description"] == "Transfer masuk dari Andra"
    assert count_transactions(owner) == 1


def test_populated_v24_upgrade_preserves_state_and_reapplication(client, notification_owner):
    from pathlib import Path
    from psycopg import sql
    from app.db.pool import db_conn

    schema = "sender_upgrade_" + uuid4().hex
    root = Path(__file__).resolve().parents[2]
    versions = sorted((root / "db/migrations").glob("V*.sql"), key=lambda path: int(path.name.split("__")[0][1:]))
    with db_conn() as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            conn.execute(sql.SQL("SET LOCAL search_path TO {}, public").format(sql.Identifier(schema)))
            for path in versions:
                if path.name.startswith("V25__"):
                    break
                conn.execute(path.read_text())
            user = conn.execute("INSERT INTO users (username, password_hash, invite_code) VALUES ('fictional_upgrade', 'synthetic-hash', 'fictional-invite') RETURNING id").fetchone()["id"]
            account = conn.execute("INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'BCA fictional', 'bank', 700000) RETURNING id", (user,)).fetchone()["id"]
            conn.execute("INSERT INTO notification_events (user_id, device_id, package_name, post_time, payload_hash, processing_state) VALUES (%s, 'fictional', 'com.bca', NOW(), 'fictional-upgrade', 'needs_review')", (user,))
            before = conn.execute("SELECT id, initial_balance FROM accounts WHERE user_id = %s", (user,)).fetchall()
            conn.execute((root / "db/migrations/V25__notification_sender_confirmation.sql").read_text())
            for _ in range(2):
                conn.execute((root / "backend/app/db/notification_sender.sql").read_text())
            assert conn.execute("SELECT id, initial_balance FROM accounts WHERE user_id = %s", (user,)).fetchall() == before
            event = conn.execute("SELECT active_question, sender_resolution, processing_state FROM notification_events WHERE user_id = %s", (user,)).fetchone()
            assert event == {"active_question": None, "sender_resolution": None, "processing_state": "needs_review"}
            assert account == before[0]["id"]
        finally:
            conn.execute(sql.SQL("SET LOCAL search_path TO public"))
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
