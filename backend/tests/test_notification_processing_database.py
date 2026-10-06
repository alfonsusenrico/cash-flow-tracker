import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from app.services.notification_interpretation import Interpretation
from app.services.openai_notification_provider import ProviderError


@pytest.fixture
def notification_owner(client, monkeypatch):
    from app.main import app
    from app.db.pool import db_conn
    from app.services.auth import get_current_user
    from app.services import notification_processing as processing

    user_id = str(uuid4())
    ids = {name: str(uuid4()) for name in ("bca", "jago", "main", "emergency", "gopay", "stockbit", "funding", "expense", "income", "internal_out", "internal_in")}
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Raka Purnama', 'synthetic-hash', 'TEST')", (user_id, f"notify_{uuid4().hex}"))
            for key, name, kind, parent in [
                ("bca", "BCA Harian", "bank", None), ("jago", "Bank Jago", "bank", None),
                ("main", "Kantong Utama", "bank", ids["jago"]), ("emergency", "Dana Darurat", "bank", ids["jago"]),
                ("gopay", "GoPay", "ewallet", None), ("stockbit", "Stockbit", "investment", None),
                ("funding", "RDN BCA", "bank", None),
            ]:
                cur.execute("INSERT INTO accounts (id, user_id, name, type, parent_id, initial_balance) VALUES (%s, %s, %s, %s, %s, 1000000)", (ids[key], user_id, name, kind, parent))
            cur.execute("UPDATE accounts SET default_pocket_id = %s WHERE id = %s", (ids["main"], ids["jago"]))
            cur.execute("UPDATE accounts SET default_funding_account_id = %s WHERE id = %s", (ids["funding"], ids["stockbit"]))
            for key, name, kind in [
                ("expense", "Tagihan & Utilitas", "expense"), ("income", "Transfer Masuk", "income"),
                ("internal_out", "Internal Movement", "expense"), ("internal_in", "Internal Movement", "income"),
            ]:
                cur.execute("INSERT INTO categories (id, user_id, name, kind) VALUES (%s, %s, %s, %s)", (ids[key], user_id, name, kind))
            cur.execute("INSERT INTO goals (user_id, name, target_amount) VALUES (%s, 'Synthetic fund', 500000) RETURNING id", (user_id,))
            goal_id = cur.fetchone()["id"]
            cur.execute("INSERT INTO goal_accounts (goal_id, account_id) VALUES (%s, %s)", (goal_id, ids["emergency"]))
        conn.commit()
    monkeypatch.setattr(processing, "settings", replace(processing.settings, notification_ai_enabled=False))
    app.dependency_overrides[get_current_user] = lambda: {"id": user_id, "name": "Raka Purnama", "currency": "IDR"}
    yield {"user_id": user_id, **ids}
    app.dependency_overrides.clear()
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()


def notification(text="You've paid Rp125.000 to Kedai Awan", package="com.jago.digitalbanking", *, seconds=27, hash_value=None):
    return {
        "device_id": "synthetic-device", "package_name": package, "body_text": text,
        "payload_hash": hash_value or uuid4().hex,
        "post_time": datetime(2026, 9, 28, 10, 15, seconds, tzinfo=timezone.utc).isoformat(),
    }


def ingest(client, *events):
    response = client.post("/api/ingest/notifications", json={"events": list(events)})
    assert response.status_code == 200, response.text
    return response.json()


def count_transactions(owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute("SELECT COUNT(*) AS count FROM transactions WHERE user_id = %s", (owner["user_id"],)).fetchone()["count"]


def ingest_with_source_confirmation(client, event, owner):
    """Explicitly choose main for fictional scenarios where the owner used that pocket."""
    from app.services import notification_processing as processing

    event = {**event, "source_version": "1.6.0"}
    result = ingest(client, event)["results"][0]
    if result["status"] == "needs_confirmation" and result["confirmation"]["type"] == "source_pocket":
        before = count_transactions(owner)
        response = client.post(f"/api/ingest/notifications/{result['event_id']}/confirm-source-pocket", json={
            "question_id": result["confirmation"]["question_id"], "reply_id": str(uuid4()), "account_id": owner["main"],
        })
        assert response.status_code == 200 and count_transactions(owner) == before
        claimed = processing.claim_event(include_ai=False)
        assert str(claimed["id"]) == result["event_id"]
        asyncio.run(processing.process_claim(claimed, None))
        result = client.get(f"/api/ingest/notifications/{result['event_id']}/result").json()["result"]
    return result


def enable_ai(monkeypatch):
    from app.services import notification_processing as processing
    monkeypatch.setattr(processing, "settings", replace(
        processing.settings,
        notification_ai_enabled=True,
        notification_ai_model="gpt-5.6-luna",
        notification_ai_reasoning_effort="low",
        openai_api_key="synthetic-test-credential",
    ))


def interpretation(owner, *, direction="expense", account="main", category="expense", candidate=None):
    return Interpretation.model_validate_json(json.dumps({
        "outcome": "record", "direction": direction, "description": "Bayar Kedai Awan",
        "account_id": owner[account], "category_id": owner[category], "kakeibo": "want" if direction == "expense" else None,
        "source_account_id": None, "target_account_id": None,
        "amount_evidence": "Rp125.000", "direction_evidence": "You've paid",
        "source_evidence": "", "target_evidence": "", "candidate_transaction_id": candidate,
        "confidence": 0.9, "review_reason": "none",
    }))


def test_direct_and_stored_dry_run_preserve_all_owner_state(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.routers import ingest as router

    owner = notification_owner
    event = notification()
    stored_id = ingest(client, event)["results"][0]["event_id"]
    provider = AsyncMock()
    provider.interpret.return_value = interpretation(owner)
    monkeypatch.setattr(router, "dry_run_guard", lambda request: provider)

    def snapshot():
        with db_conn() as conn:
            return {
                table: conn.execute(
                    f"SELECT row_to_json(row) AS state FROM {table} row WHERE user_id = %s ORDER BY id",
                    (owner["user_id"],),
                ).fetchall()
                for table in ("notification_events", "transactions", "accounts", "goals", "obligations", "recurring_rules")
            }

    before = snapshot()
    direct = client.post("/api/ingest/notifications/dry-run", json={"event": event})
    stored = client.post(f"/api/ingest/notifications/{stored_id}/dry-run")
    assert direct.status_code == stored.status_code == 200
    assert direct.json()["result"] == stored.json()["result"]
    assert direct.json()["result"]["status"] == "would_record"
    assert direct.json()["result"]["source"] == "Bank Jago · Kantong Utama"
    assert snapshot() == before
    missing = client.post(f"/api/ingest/notifications/{uuid4()}/dry-run")
    assert missing.status_code == 404
    assert provider.interpret.await_count == 2


def test_stored_dry_run_preserves_captured_hints_after_label_edit(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.routers import ingest as router

    event = notification()
    event_id = ingest(client, event)["results"][0]["event_id"]
    with db_conn() as conn:
        conn.execute("UPDATE notification_events SET expected_amount = 1, expected_direction = 'in' WHERE id = %s", (event_id,))
    provider = AsyncMock()
    provider.interpret.return_value = interpretation(notification_owner)
    monkeypatch.setattr(router, "dry_run_guard", lambda request: provider)
    response = client.post(f"/api/ingest/notifications/{event_id}/dry-run")
    assert response.status_code == 200
    assert response.json()["result"]["status"] == "would_record"
    assert response.json()["result"]["amount"] == 125000


def test_dry_run_session_bearer_and_foreign_event_isolation(client, auth_client, api_key, monkeypatch):
    from fastapi.testclient import TestClient
    from app.core.config import settings
    from app.db.pool import db_conn
    from app.main import app
    from app.services import notification_dry_run

    monkeypatch.setattr(notification_dry_run, "settings", replace(
        settings, app_env="development", notification_ai_dry_run_enabled=True,
        openai_api_key="synthetic-test-credential",
    ))
    provider = AsyncMock()
    provider.interpret.return_value = Interpretation.model_validate_json(json.dumps({
        "outcome": "needs_review", "direction": None, "description": "", "account_id": None,
        "source_account_id": None, "target_account_id": None, "category_id": None,
        "kakeibo": None, "amount_evidence": "", "direction_evidence": "", "source_evidence": "",
        "target_evidence": "", "candidate_transaction_id": None, "confidence": 0.0,
        "review_reason": "uncertain_mapping",
    }))
    monkeypatch.setattr(app.state, "notification_provider", provider)
    event = notification()
    event_id = ingest(auth_client, event)["results"][0]["event_id"]
    bearer = TestClient(app, headers={"Authorization": f"Bearer {api_key}"})
    for caller in (auth_client, bearer):
        direct = caller.post("/api/ingest/notifications/dry-run", json={"event": event})
        stored = caller.post(f"/api/ingest/notifications/{event_id}/dry-run")
        assert direct.status_code == stored.status_code == 200
        assert direct.json()["result"]["status"] == "needs_review"

    foreign_user, foreign_event = str(uuid4()), str(uuid4())
    with db_conn() as conn:
        conn.execute("INSERT INTO users (id, username, password_hash, invite_code) VALUES (%s, %s, 'synthetic-hash', 'TEST')",
                     (foreign_user, f"foreign_{uuid4().hex}"))
        conn.execute("INSERT INTO notification_events (id, user_id, device_id, package_name, post_time, payload_hash) VALUES (%s, %s, 'synthetic', 'com.bca', NOW(), %s)",
                     (foreign_event, foreign_user, uuid4().hex))
    try:
        assert bearer.post(f"/api/ingest/notifications/{foreign_event}/dry-run").status_code == 404
        assert TestClient(app).post("/api/ingest/notifications/dry-run", json={"event": event}).status_code == 401
        assert provider.interpret.await_count == 4
        monkeypatch.setattr(notification_dry_run, "settings", replace(notification_dry_run.settings, app_env="production"))
        assert bearer.post(f"/api/ingest/notifications/{event_id}/dry-run").status_code == 404
        assert provider.interpret.await_count == 4
    finally:
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (foreign_user,))


def test_recorded_result_seconds_reconciliation_and_redelivery(client, notification_owner):
    from app.db.pool import db_conn
    owner = notification_owner
    with db_conn() as conn:
        conn.execute("UPDATE accounts SET initial_balance = 100000 WHERE id = %s", (owner["main"],))
    payload = notification()
    response = ingest(client, payload)
    result = response["results"][0]
    assert result["status"] == "recorded" and result["amount"] == 125000
    assert result["source"] == "Bank Jago · Kantong Utama" and result["target"] is None
    assert str(UUID(result["transaction_id"])) == result["transaction_id"]
    assert response["created_transactions"] == 1
    assert client.get(f"/api/ingest/notifications/{result['event_id']}/result").json()["result"] == result
    payload["body_text"] = "You've paid Rp999.000 to Other Merchant"
    assert ingest(client, payload)["results"][0] == result
    assert count_transactions(owner) == 1
    with db_conn() as conn:
        row = conn.execute("SELECT date, amount FROM transactions WHERE user_id = %s", (owner["user_id"],)).fetchone()
        assert row["date"].second == 27 and row["amount"] == 125000
        assert conn.execute("SELECT reconciliation_required FROM accounts WHERE id = %s", (owner["main"],)).fetchone()["reconciliation_required"]


@pytest.mark.parametrize("text", [
    "Rp500.000 has been moved from your Main Pocket Pocket to your Dana Darurat Pocket.",
    "You've moved Rp500.000 into your Dana Darurat Pocket.",
    "You've moved Rp500.000 out of your Dana Darurat Pocket.",
])
def test_complete_jago_movement_needs_one_notification(client, notification_owner, text):
    from app.db.pool import db_conn
    result = ingest(client, notification(text))["results"][0]
    assert result["status"] == "recorded", result
    assert result["type"] == "internal_movement" and result["amount"] == 500000
    assert "transaction_id" not in result
    assert count_transactions(notification_owner) == 2
    with db_conn() as conn:
        rows = conn.execute("SELECT movement_id, movement_role, kakeibo_type FROM transactions WHERE user_id = %s", (notification_owner["user_id"],)).fetchall()
        assert len({row["movement_id"] for row in rows}) == 1
        assert next(row for row in rows if row["movement_role"] == "outbound")["kakeibo_type"] == "saving"
        assert next(row for row in rows if row["movement_role"] == "inbound")["kakeibo_type"] is None


def test_single_pocket_jago_notification_without_distinct_default_stays_reviewable(client, notification_owner):
    result = ingest(client, notification("You've moved Rp500.000 out of your Kantong Utama Pocket."))["results"][0]
    assert result["status"] == "needs_review" and result["error_code"] == "same_movement_endpoint"
    assert count_transactions(notification_owner) == 0


def test_unmapped_unknown_fraction_and_noise_do_not_write(client, notification_owner):
    events = [notification("You've moved Rp50.000 into your Unknown Pocket."),
              notification("You've paid IDR 50,000.50 to Kedai Awan"),
              notification("Payment Rp50.000 failed"),
              notification("You've paid Rp125.000 to Kedai Awan", package="com.blu.app")]
    response = ingest(client, *events)
    assert [result["status"] for result in response["results"]] == ["needs_review", "needs_review", "ignored", "ignored"]
    assert response["inserted"] == 2 and count_transactions(notification_owner) == 0


def test_queued_then_worker_success_has_no_database_connection_during_inference(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services.notification_processing import claim_event, process_claim
    enable_ai(monkeypatch)
    result = ingest(client, notification())["results"][0]
    assert result["status"] == "queued" and "record_key" not in result
    claimed = claim_event()
    if claimed is None:
        with db_conn() as conn:
            state = conn.execute("SELECT processing_state, processing_mode, attempt_count, next_attempt_at <= clock_timestamp() AS due, EXTRACT(EPOCH FROM next_attempt_at - clock_timestamp()) AS clock_delta FROM notification_events WHERE id = %s", (result["event_id"],)).fetchone()
        pytest.fail(f"Queued synthetic event was not claimed: {state}")

    async def infer(context):
        from app.db.pool import DB_POOL
        assert DB_POOL.get_stats()["pool_available"] == DB_POOL.get_stats()["pool_size"]
        assert context["facts"]["timestamp"].endswith("+00:00")
        return interpretation(notification_owner)

    provider = AsyncMock()
    provider.interpret.side_effect = infer
    asyncio.run(process_claim(claimed, provider))
    completed = client.get(f"/api/ingest/notifications/{result['event_id']}/result").json()["result"]
    assert completed["status"] == "recorded" and count_transactions(notification_owner) == 1


def test_stale_worker_is_fenced_and_recovered_claim_writes_once(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services.notification_processing import apply_claim, claim_event
    enable_ai(monkeypatch)
    ingest(client, notification())
    stale = claim_event()
    with db_conn() as conn:
        conn.execute("UPDATE notification_events SET lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = %s", (stale["id"],))
    fresh = claim_event()
    assert fresh["processing_generation"] > stale["processing_generation"]
    assert not apply_claim(stale, interpretation(notification_owner))
    assert apply_claim(fresh, interpretation(notification_owner))
    assert not apply_claim(fresh, interpretation(notification_owner))
    assert count_transactions(notification_owner) == 1


def test_provider_failures_retry_and_expired_budget_are_explicit(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services.notification_processing import claim_event, fail_claim
    enable_ai(monkeypatch)
    event = ingest(client, notification())["results"][0]
    claim = claim_event()
    fail_claim(claim, ProviderError("provider_unavailable", transient=True, retry_after=25))
    assert client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]["status"] == "queued"
    with db_conn() as conn:
        row = conn.execute("SELECT next_attempt_at FROM notification_events WHERE id = %s", (claim["id"],)).fetchone()
        assert row["next_attempt_at"] > datetime.now(timezone.utc) + timedelta(seconds=20)
        conn.execute("UPDATE notification_events SET processing_state = 'processing', attempt_count = 3, lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = %s", (claim["id"],))
    assert claim_event() is None
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "failed" and result["error_code"] == "worker_attempts_exhausted"


def test_reference_archived_during_inference_causes_review_not_write(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services.notification_processing import claim_event, process_claim
    enable_ai(monkeypatch)
    event = ingest(client, notification())["results"][0]
    claim = claim_event()

    async def infer(context):
        with db_conn() as conn:
            conn.execute("UPDATE accounts SET is_archived = TRUE WHERE id = %s", (notification_owner["main"],))
        return interpretation(notification_owner)

    provider = AsyncMock()
    provider.interpret.side_effect = infer
    asyncio.run(process_claim(claim, provider))
    assert client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]["status"] == "needs_review"
    assert count_transactions(notification_owner) == 0


def test_concurrent_delivery_claims_one_effect(client, notification_owner):
    from app.services.notification_processing import accept_event
    payload = notification()
    payload["post_time"] = datetime.fromisoformat(payload["post_time"])
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(lambda _: accept_event(payload, notification_owner["user_id"]), range(3)))
    assert sum(result[1] for result in results) == 1
    assert sum(result[2] for result in results) == 1
    assert len({result[0]["record_key"] for result in results}) == 1
    assert count_transactions(notification_owner) == 1


@pytest.mark.parametrize("offset,reverse,expected_count", [
    (29, False, 1), (29, True, 1), (600, False, 1), (600, True, 1), (899, False, 1), (900, False, 2),
])
def test_cross_account_window_and_both_orders(client, notification_owner, offset, reverse, expected_count):
    from app.db.pool import db_conn
    outgoing = notification("Rp500.000 udah dikirim ke BCA Raka Purnama.", "com.gojek.gopay", seconds=1)
    incoming = notification("Pemasukan sebesar IDR 500,000.00 dari RA** PUR**MA di kategori Transfer Rekening.", "com.bca", seconds=1)
    incoming["post_time"] = (datetime.fromisoformat(outgoing["post_time"]) + timedelta(seconds=offset)).isoformat()
    # The registered rule selects a category, never overrides source facts.
    with db_conn() as conn:
        conn.execute("INSERT INTO merchant_category_rules (user_id, merchant_pattern, category_id) VALUES (%s, 'FIK', %s)", (notification_owner["user_id"], notification_owner["income"]))
    events = [incoming, outgoing] if reverse else [outgoing, incoming]
    first = ingest(client, events[0])["results"][0]
    second = ingest(client, events[1])["results"][0]
    assert first["status"] == second["status"] == "recorded", (first, second)
    assert count_transactions(notification_owner) == 2
    if expected_count == 1:
        assert second["type"] == "internal_movement"
        assert "transaction_id" not in second
        assert second["record_key"] == first["record_key"]
        linked = client.get(f"/api/ingest/notifications/{first['event_id']}/result").json()["result"]
        assert linked["type"] == "internal_movement" and "transaction_id" not in linked
    else:
        assert second["type"] == "income"
        assert str(UUID(second["transaction_id"])) == second["transaction_id"]


def test_broker_buy_sell_is_deterministic_and_replay_safe(client, notification_owner):
    from app.db.pool import db_conn
    buy = notification("Pembelian 4 lot FIKS match di harga Rp3.340", "com.stockbit.android")
    buy["expected_amount"] = 1336000
    recorded = ingest(client, buy)["results"][0]
    assert recorded["status"] == "recorded" and recorded["amount"] == 1336000
    assert "transaction_id" not in recorded
    assert ingest(client, buy)["created_transactions"] == 0
    sold = ingest(client, notification("Penjualan 1 lot FIKS match di harga Rp3.400", "com.stockbit.android"))["results"][0]
    assert sold["status"] == "recorded" and sold["amount"] == 340000
    with db_conn() as conn:
        position = conn.execute("SELECT units, avg_buy_price FROM accounts WHERE user_id = %s AND instrument_symbol = 'FIKS.JK'", (notification_owner["user_id"],)).fetchone()
        assert position["units"] == 300 and position["avg_buy_price"] == 3340
    assert count_transactions(notification_owner) == 4


def test_label_does_not_rewrite_captured_facts_and_completed_cannot_retry(client, notification_owner, monkeypatch):
    from app.services.notification_processing import claim_event, process_claim
    enable_ai(monkeypatch)
    event = ingest(client, notification())["results"][0]
    assert client.patch(f"/api/ingest/notifications/{event['event_id']}/label", json={"expected_amount": 1}).status_code == 200
    provider = AsyncMock()
    provider.interpret.return_value = interpretation(notification_owner)
    asyncio.run(process_claim(claim_event(), provider))
    assert client.post(f"/api/ingest/notifications/{event['event_id']}/retry").status_code == 409
    assert count_transactions(notification_owner) == 1


def test_foreign_lookup_retry_and_context_are_isolated(client, notification_owner):
    from app.main import app
    from app.db.pool import db_conn
    from app.services.auth import get_current_user
    from app.services.notification_context import load_context
    from app.services.notification_evidence import collect_facts
    other_id = str(uuid4())
    event = ingest(client, notification())["results"][0]
    app.dependency_overrides[get_current_user] = lambda: {"id": other_id, "currency": "IDR"}
    assert client.get(f"/api/ingest/notifications/{event['event_id']}/result").status_code == 404
    assert client.post(f"/api/ingest/notifications/{event['event_id']}/retry").status_code == 404
    payload = notification()
    payload["post_time"] = datetime.fromisoformat(payload["post_time"])
    with db_conn() as conn:
        context = load_context(conn.cursor(), other_id, collect_facts(payload))
    assert context["accounts"] == context["categories"] == context["history"] == context["candidates"] == []


def test_committed_deletion_never_recreates_a_transaction(client, notification_owner):
    from app.db.pool import db_conn
    payload = notification()
    result = ingest(client, payload)["results"][0]
    with db_conn() as conn:
        conn.execute("DELETE FROM transactions WHERE user_id = %s", (notification_owner["user_id"],))
    redelivered = ingest(client, payload)["results"][0]
    assert redelivered["status"] == "recorded" and redelivered["record_key"] == result["record_key"]
    assert "transaction_id" not in redelivered
    assert count_transactions(notification_owner) == 0


def test_confirming_jago_movement_role_has_zero_extra_effect(client, notification_owner):
    original = ingest(client, notification("You've moved Rp500.000 into your Dana Darurat Pocket.", seconds=1))["results"][0]
    confirmation = notification("Payment of Rp500.000 to Dana Darurat using Kantong Utama Pocket was successful.", seconds=4)
    confirmation["title"] = "Transfer berhasil"
    result = ingest(client, confirmation)["results"][0]
    assert result["status"] == "recorded", result
    assert result["record_key"] == original["record_key"]
    assert count_transactions(notification_owner) == 2
    consumed = ingest(client, notification(confirmation["body_text"], seconds=5))["results"][0]
    assert consumed["status"] == "needs_review"
    assert count_transactions(notification_owner) == 2


def test_ambiguous_candidates_do_not_merge(client, notification_owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute("INSERT INTO merchant_category_rules (user_id, merchant_pattern, category_id) VALUES (%s, 'FIK', %s)", (notification_owner["user_id"], notification_owner["income"]))
    for seconds in (1, 2):
        assert ingest(client, notification("Rp500.000 udah dikirim ke BCA Raka Purnama.", "com.gojek.gopay", seconds=seconds))["results"][0]["status"] == "recorded"
    incoming = ingest(client, notification("Pemasukan sebesar IDR 500,000.00 dari RA** PUR**MA di kategori Transfer Rekening.", "com.bca", seconds=4))["results"][0]
    assert incoming["status"] == "recorded" and incoming["type"] == "income"
    with db_conn() as conn:
        assert conn.execute("SELECT COUNT(*) AS count FROM transactions WHERE user_id = %s AND movement_id IS NOT NULL", (notification_owner["user_id"],)).fetchone()["count"] == 0


def test_counterpart_discovery_is_independent_of_recent_history(client, notification_owner):
    from app.db.pool import db_conn
    from app.services.notification_context import load_context
    from app.services.notification_evidence import collect_facts
    first = ingest(client, notification("Rp500.000 udah dikirim ke BCA Raka Purnama.", "com.gojek.gopay", seconds=1))["results"][0]
    with db_conn() as conn:
        for _ in range(25):
            conn.execute("INSERT INTO transactions (user_id, account_id, category_id, type, amount, date) VALUES (%s, %s, %s, 'expense', 1, '2026-09-28T10:15:20Z')", (notification_owner["user_id"], notification_owner["bca"], notification_owner["expense"]))
        conn.execute("INSERT INTO merchant_category_rules (user_id, merchant_pattern, category_id) VALUES (%s, 'FIK', %s)", (notification_owner["user_id"], notification_owner["income"]))
    incoming = notification("Pemasukan sebesar IDR 500,000.00 dari RA** PUR**MA di kategori Transfer Rekening.", "com.bca", seconds=29)
    payload = dict(incoming, post_time=datetime.fromisoformat(incoming["post_time"]))
    with db_conn() as conn:
        context = load_context(conn.cursor(), notification_owner["user_id"], collect_facts(payload))
    assert len(context["history"]) == 20
    assert len(context["candidates"]) == 1
    assert context["candidates"][0]["id"] not in {row["id"] for row in context["history"]}
    result = ingest(client, incoming)["results"][0]
    assert result["type"] == "internal_movement" and result["record_key"] == first["record_key"]


def test_invalid_provider_output_falls_back_to_deterministic_record(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services import notification_processing as processing
    enable_ai(monkeypatch)
    event = ingest(client, notification())["results"][0]
    claimed = processing.claim_event()
    provider = AsyncMock()
    provider.interpret.side_effect = ProviderError("provider_invalid_output")
    asyncio.run(processing.process_claim(claimed, provider))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "recorded" and "error_code" not in result
    assert count_transactions(notification_owner) == 1
    with db_conn() as conn:
        stored = conn.execute("SELECT interpretation FROM notification_events WHERE id = %s", (event["event_id"],)).fetchone()
    assert stored["interpretation"]["source"] == "deterministic_fallback"


def test_application_failure_leaves_no_partial_effect(client, notification_owner, monkeypatch):
    from app.services import notification_processing as processing
    enable_ai(monkeypatch)
    event = ingest(client, notification())["results"][0]
    claimed = processing.claim_event()

    def fail_after_insert(cur, event, facts, proposal, context, **_):
        from app.services.notification_evidence import EvidenceError
        cur.execute("INSERT INTO transactions (user_id, account_id, category_id, type, amount, date) VALUES (%s, %s, %s, 'expense', 125000, NOW())", (notification_owner["user_id"], notification_owner["main"], notification_owner["expense"]))
        raise EvidenceError("synthetic_application_failure")

    monkeypatch.setattr(processing, "apply_interpretation", fail_after_insert)
    provider = AsyncMock()
    provider.interpret.return_value = interpretation(notification_owner)
    asyncio.run(processing.process_claim(claimed, provider))
    assert count_transactions(notification_owner) == 0
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "needs_review" and result["error_code"] == "synthetic_application_failure"


def test_legacy_result_materializes_without_inference_or_replay(client, notification_owner):
    from app.db.pool import db_conn
    event_id = str(uuid4())
    with db_conn() as conn:
        transaction = conn.execute("INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, notes) VALUES (%s, %s, %s, 'expense', 125000, NOW(), 'Synthetic legacy') RETURNING id", (notification_owner["user_id"], notification_owner["main"], notification_owner["expense"])).fetchone()
        conn.execute("INSERT INTO notification_events (id, user_id, device_id, package_name, post_time, payload_hash, transaction_id) VALUES (%s, %s, 'synthetic-device', 'com.jago.digitalbanking', NOW(), %s, %s)", (event_id, notification_owner["user_id"], uuid4().hex, transaction["id"]))
    result = client.get(f"/api/ingest/notifications/{event_id}/result").json()["result"]
    assert result["status"] == "recorded" and result["amount"] == 125000
    assert client.post(f"/api/ingest/notifications/{event_id}/retry").status_code == 409
    assert count_transactions(notification_owner) == 1


def test_registered_renames_rules_and_kakeibo_are_current(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services.notification_processing import claim_event, process_claim
    enable_ai(monkeypatch)
    event = ingest(client, notification())["results"][0]
    with db_conn() as conn:
        conn.execute("UPDATE accounts SET name = 'Jago Operasional' WHERE id = %s", (notification_owner["jago"],))
        conn.execute("UPDATE categories SET name = 'Kesenangan Dinamis' WHERE id = %s", (notification_owner["expense"],))
    provider = AsyncMock()

    async def infer(context):
        assert any(row["name"] == "Jago Operasional" for row in context["accounts"])
        assert any(row["name"] == "Kesenangan Dinamis" for row in context["categories"])
        return interpretation(notification_owner)

    provider.interpret.side_effect = infer
    asyncio.run(process_claim(claim_event(), provider))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["source"] == "Jago Operasional · Kantong Utama"
    with db_conn() as conn:
        assert conn.execute("SELECT kakeibo_type FROM transactions WHERE user_id = %s", (notification_owner["user_id"],)).fetchone()["kakeibo_type"] == "want"


def test_concurrent_counterparts_link_once(client, notification_owner):
    from app.db.pool import db_conn
    from app.services.notification_processing import accept_event

    owner = notification_owner
    with db_conn() as conn:
        conn.execute("INSERT INTO merchant_category_rules (user_id, merchant_pattern, category_id) VALUES (%s, 'FIK', %s)", (owner["user_id"], owner["income"]))
    payloads = [
        notification("Rp500.000 udah dikirim ke BCA Raka Purnama.", "com.gojek.gopay", seconds=1),
        notification("Pemasukan sebesar IDR 500,000.00 dari RA** PUR**MA di kategori Transfer Rekening.", "com.bca", seconds=4),
    ]
    for payload in payloads:
        payload["post_time"] = datetime.fromisoformat(payload["post_time"])
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda payload: accept_event(payload, owner["user_id"]), payloads))
    assert sum(result[2] for result in results) == 2
    with db_conn() as conn:
        rows = conn.execute("SELECT movement_id, movement_role FROM transactions WHERE user_id = %s", (owner["user_id"],)).fetchall()
        keys = conn.execute("SELECT result_key FROM notification_events WHERE user_id = %s", (owner["user_id"],)).fetchall()
    assert len(rows) == 2 and len({row["movement_id"] for row in rows}) == 1
    assert rows[0]["movement_id"] is not None
    assert {row["movement_role"] for row in rows} == {"inbound", "outbound"}
    assert len({row["result_key"] for row in keys}) == 1


def test_literal_self_marker_cannot_prove_own_transfer(client, notification_owner):
    from app.db.pool import db_conn

    owner = notification_owner
    with db_conn() as conn:
        conn.execute("INSERT INTO merchant_category_rules (user_id, merchant_pattern, category_id) VALUES (%s, '[SELF]', %s)", (owner["user_id"], owner["expense"]))
        conn.execute("INSERT INTO merchant_category_rules (user_id, merchant_pattern, category_id) VALUES (%s, 'FIK', %s)", (owner["user_id"], owner["income"]))
    first = ingest(client, notification("Rp500.000 udah dikirim ke BCA [SELF].", "com.gojek.gopay", seconds=1))["results"][0]
    second = ingest(client, notification("Pemasukan sebesar IDR 500,000.00 dari FIK SISI di kategori Transfer Rekening.", "com.bca", seconds=4))["results"][0]
    assert first["status"] in {"recorded", "needs_review"}
    assert second["status"] == "recorded" and second["type"] == "income"
    with db_conn() as conn:
        assert conn.execute("SELECT COUNT(*) AS count FROM transactions WHERE user_id = %s AND movement_id IS NOT NULL", (owner["user_id"],)).fetchone()["count"] == 0


def test_broker_accumulation_rejects_fractional_lots_and_preserves_funding(client, notification_owner):
    from app.db.pool import db_conn

    owner = notification_owner
    assert ingest(client, notification("Pembelian 10 lot FIKS match di harga Rp3.300", "com.stockbit.android"))["created_transactions"] == 2
    assert ingest(client, notification("Pembelian 4 lot FIKS match di harga Rp3.400", "com.stockbit.android"))["created_transactions"] == 2
    fraction = ingest(client, notification("Pembelian 4.25 lot FIKS match di harga Rp3.400", "com.stockbit.android"))["results"][0]
    assert fraction["status"] == "needs_review"
    with db_conn() as conn:
        position = conn.execute("SELECT units, avg_buy_price, last_price FROM accounts WHERE user_id = %s AND instrument_symbol = 'FIKS.JK'", (owner["user_id"],)).fetchone()
        assert position == {"units": 1400, "avg_buy_price": 3329, "last_price": 3400}
        expenses = conn.execute("SELECT account_id, SUM(amount) AS total FROM transactions WHERE user_id = %s AND type = 'expense' GROUP BY account_id", (owner["user_id"],)).fetchall()
        assert len(expenses) == 1 and str(expenses[0]["account_id"]) == owner["funding"]
        assert expenses[0]["total"] == 4660000
    assert count_transactions(owner) == 4


def test_broker_explicit_retry_uses_deterministic_worker_without_ai(client, notification_owner):
    from app.db.pool import db_conn
    from app.services.notification_processing import claim_event, process_claim

    owner = notification_owner
    with db_conn() as conn:
        conn.execute("UPDATE accounts SET default_funding_account_id = NULL WHERE id = %s", (owner["stockbit"],))
    event = ingest(client, notification("Pembelian 4 lot FIKS match di harga Rp3.340", "com.stockbit.android"))["results"][0]
    assert event["status"] == "needs_review"
    with db_conn() as conn:
        conn.execute("UPDATE accounts SET default_funding_account_id = %s WHERE id = %s", (owner["funding"], owner["stockbit"]))
    retry = client.post(f"/api/ingest/notifications/{event['event_id']}/retry").json()["result"]
    assert retry["status"] == "queued"
    claimed = claim_event(include_ai=False)
    assert claimed["processing_mode"] == "deterministic" and claimed["provider"] is None
    asyncio.run(process_claim(claimed, None))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "recorded" and result["amount"] == 1336000
    assert client.post(f"/api/ingest/notifications/{event['event_id']}/retry").status_code == 409
    assert count_transactions(owner) == 2


def test_deterministic_jago_retry_preserves_mode_while_ai_is_disabled(client, notification_owner):
    from app.db.pool import db_conn
    from app.services.notification_processing import claim_event, process_claim

    owner = notification_owner
    event = ingest(client, notification("You've moved Rp50.000 into your Unknown Pocket."))["results"][0]
    assert event["status"] == "needs_review"
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO accounts (user_id, parent_id, name, type, initial_balance) VALUES (%s, %s, 'Unknown', 'bank', 0)",
            (owner["user_id"], owner["jago"]),
        )
    retried = client.post(f"/api/ingest/notifications/{event['event_id']}/retry")
    assert retried.status_code == 200
    assert retried.json()["result"]["status"] == "queued"
    claim = claim_event(include_ai=False)
    assert claim and claim["processing_mode"] == "deterministic"
    asyncio.run(process_claim(claim, None))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "recorded" and result["type"] == "internal_movement"
    assert count_transactions(owner) == 2


def test_ai_retry_does_not_queue_when_provider_is_disabled(client, notification_owner, monkeypatch):
    from app.services import notification_processing as processing

    enable_ai(monkeypatch)
    event = ingest(client, notification())["results"][0]
    claim = processing.claim_event()
    processing.fail_claim(claim, ProviderError("provider_access_denied"))
    monkeypatch.setattr(processing, "settings", replace(processing.settings, notification_ai_enabled=False))
    retry = client.post(f"/api/ingest/notifications/{event['event_id']}/retry")
    assert retry.status_code == 409
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "failed" and result["error_code"] == "provider_access_denied"
    assert processing.claim_event(include_ai=False) is None


def test_retry_serialization_and_stale_failure_cannot_replace_new_claim(client, notification_owner, monkeypatch):
    from app.main import app
    from fastapi.testclient import TestClient
    from app.services.notification_processing import claim_event, fail_claim

    enable_ai(monkeypatch)
    event = ingest(client, notification())["results"][0]
    old = claim_event()
    fail_claim(old, ProviderError("provider_access_denied"))
    clients = [TestClient(app), TestClient(app)]
    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(lambda c: c.post(f"/api/ingest/notifications/{event['event_id']}/retry"), clients))
    assert sorted(response.status_code for response in responses) == [200, 409]
    fresh = claim_event()
    fail_claim(old, ProviderError("provider_access_denied"))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "processing" and fresh["processing_generation"] > old["processing_generation"]
    assert count_transactions(notification_owner) == 0


def test_outbound_request_minimizes_owned_context_and_preserves_exact_facts(client, notification_owner, monkeypatch):
    import httpx2
    from app.db.pool import db_conn
    from app.services.notification_processing import claim_event, process_claim
    from app.services.openai_notification_provider import OpenAINotificationProvider

    enable_ai(monkeypatch)
    owner = notification_owner
    with db_conn() as conn:
        conn.execute("INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, notes, receipt_path) VALUES (%s, %s, %s, 'expense', 1, NOW(), 'token=fictional-sensitive-token account 987654321098765', '/private/fictional-receipt')", (owner["user_id"], owner["main"], owner["expense"]))
    payload = notification("You've paid Rp125.000 to Kedai Awan. account 987654321098765 token=fictional-sensitive-token")
    payload["raw_extras"] = {"secret": "fictional-extra-value"}
    event = ingest(client, payload)["results"][0]

    def transport(request):
        body = request.content.decode()
        for forbidden in ("987654321098765", "fictional-sensitive-token", "fictional-extra-value",
                          "synthetic-device", "/private/fictional-receipt", owner["user_id"]):
            assert forbidden not in body
        assert "Rp125.000" in body and "10:15:27" in body
        return httpx2.Response(200, json={
            "id": "resp_test",
            "object": "response",
            "created_at": 0,
            "status": "completed",
            "model": "gpt-5.6-luna",
            "output": [{
                "id": "msg_test",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [{"type": "output_text", "text": interpretation(owner).model_dump_json(), "annotations": []}],
            }],
        })

    async def run():
        async with httpx2.AsyncClient(transport=httpx2.MockTransport(transport)) as provider_client:
            provider = OpenAINotificationProvider(
                provider_client,
                api_key="synthetic-test-credential",
                model="gpt-5.6-luna",
                reasoning_effort="low",
            )
            await process_claim(claim_event(), provider)

    asyncio.run(run())
    assert client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]["status"] == "recorded"


def test_truncated_candidate_context_does_not_turn_ambiguity_into_uniqueness(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services.notification_context import load_context
    from app.services.notification_evidence import collect_facts
    from app.services.notification_processing import claim_event, process_claim

    original = ingest(client, notification("Rp500.000 udah dikirim ke BCA Raka Purnama.", "com.gojek.gopay", seconds=1))["results"][0]
    with db_conn() as conn:
        conn.execute("""INSERT INTO notification_events
            (user_id, device_id, package_name, post_time, payload_hash, transaction_id,
             processing_state, result_key, result_snapshot, provenance_kind, interpretation)
            SELECT n.user_id, n.device_id, n.package_name, n.post_time, gen_random_uuid()::text,
                   n.transaction_id, n.processing_state, n.result_key, n.result_snapshot,
                   n.provenance_kind, n.interpretation
            FROM notification_events n CROSS JOIN generate_series(1, 50) WHERE n.id = %s""", (original["event_id"],))
    incoming = notification("Pemasukan sebesar IDR 500,000.00 dari Raka Purnama di kategori Transfer Rekening.", "com.bca", seconds=4)
    facts = collect_facts(dict(incoming, post_time=datetime.fromisoformat(incoming["post_time"])))
    with db_conn() as conn:
        context = load_context(conn.cursor(), notification_owner["user_id"], facts)
    assert context["incomplete"] and len(context["candidates"]) == 50
    enable_ai(monkeypatch)
    event = ingest(client, incoming)["results"][0]
    provider = AsyncMock()
    asyncio.run(process_claim(claim_event(), provider))
    provider.interpret.assert_not_called()
    assert client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]["status"] == "needs_review"
    assert count_transactions(notification_owner) == 1


def test_legacy_paired_notification_is_not_guessed_as_unconsumed_confirmation(client, notification_owner):
    from app.db.pool import db_conn

    original = ingest(client, notification("You've moved Rp500.000 into your Dana Darurat Pocket.", seconds=1))["results"][0]
    with db_conn() as conn:
        conn.execute("""UPDATE notification_events SET processing_state = NULL, provenance_kind = NULL,
                     movement_id = NULL, result_key = NULL, result_snapshot = NULL, interpretation = NULL
                     WHERE id = %s""", (original["event_id"],))
    confirmation = notification("Payment of Rp500.000 to Dana Darurat using Kantong Utama Pocket was successful.", seconds=4)
    confirmation["title"] = "Transfer berhasil"
    result = ingest(client, confirmation)["results"][0]
    assert result["status"] == "needs_review" and result["error_code"] == "ambiguous_existing_movement"
    legacy = client.get(f"/api/ingest/notifications/{original['event_id']}/result").json()["result"]
    assert legacy["status"] == "recorded" and legacy["type"] == "internal_movement"
    assert count_transactions(notification_owner) == 2


def add_account(owner, key, name, kind="ewallet"):
    from app.db.pool import db_conn
    owner[key] = str(uuid4())
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, %s, %s, 1000000)",
            (owner[key], owner["user_id"], name, kind),
        )
    return owner[key]


def add_category(owner, key, name, kind="expense"):
    from app.db.pool import db_conn
    owner[key] = str(uuid4())
    with db_conn() as conn:
        conn.execute("INSERT INTO categories (id, user_id, name, kind) VALUES (%s, %s, %s, %s)", (owner[key], owner["user_id"], name, kind))
    return owner[key]


def at(event, seconds):
    event["post_time"] = (datetime(2026, 9, 29, 6, 0, tzinfo=timezone.utc) + timedelta(seconds=seconds)).isoformat()
    return event


def transactions_of(owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute(
            """SELECT t.id, t.type, t.account_id, t.movement_id, t.movement_role, t.category_id, t.notes, c.name AS category
               FROM transactions t JOIN categories c ON c.id = t.category_id
               WHERE t.user_id = %s ORDER BY t.type""",
            (owner["user_id"],),
        ).fetchall()


BCA_TOP_UP_DEBIT = "You spent IDR 7,490,557.00 at Shopping."
SHOPEEPAY_TOP_UP = "Pengisian saldo sebesar Rp7.490.557 telah ditambahkan ke ShopeePay-mu. Saldo saat ini sebesar Rp7.491.557."


@pytest.fixture
def top_up_owner(notification_owner):
    add_account(notification_owner, "shopeepay", "ShopeePay")
    add_category(notification_owner, "shopping", "Belanja")
    return notification_owner


@pytest.mark.parametrize("reverse", [False, True])
def test_bca_debit_and_shopeepay_top_up_link_into_one_movement(client, top_up_owner, reverse):
    debit = at(notification(BCA_TOP_UP_DEBIT, "com.bca.mybca.omni.android"), 3)
    top_up = at(notification(SHOPEEPAY_TOP_UP, "com.shopeepay.id"), 0)
    first, second = (debit, top_up) if reverse else (top_up, debit)
    ingest(client, first)
    result = ingest(client, second)["results"][0]
    assert result["status"] == "recorded" and result["type"] == "internal_movement", result
    assert result["source"] == "BCA Harian" and result["target"] == "ShopeePay"
    rows = transactions_of(top_up_owner)
    assert len(rows) == 2 and len({row["movement_id"] for row in rows}) == 1 and rows[0]["movement_id"]
    assert {row["category"] for row in rows} == {"Internal Movement"}
    assert {str(row["account_id"]) for row in rows} == {top_up_owner["bca"], top_up_owner["shopeepay"]}


def test_self_transfer_legs_minutes_apart_link(client, top_up_owner):
    outgoing = at(notification("You've sent Rp1.500.000 to RAKA PURNAMA.", "com.jago.digitalbanking"), 0)
    incoming = at(notification(
        "RAKA PURNAMA mengirimkan dana sebesar Rp1.500.000 ke ShopeePay-mu melalui BI-Fast.", "com.shopeepay.id"), 240)
    assert ingest_with_source_confirmation(client, outgoing, top_up_owner)["status"] == "recorded"
    result = ingest(client, incoming)["results"][0]
    assert result["type"] == "internal_movement" and result["target"] == "ShopeePay"
    assert result["source"] == "Bank Jago · Kantong Utama"


def test_third_party_payment_is_not_linked_to_equal_income(client, top_up_owner):
    add_category(top_up_owner, "transfer_out", "Transfer Keluar")
    ingest(client, at(notification("Rp500.000 udah dikirim ke BCA Mira Langit.", "com.gojek.gopay"), 0))
    result = ingest(client, at(notification(
        "Pengisian saldo sebesar Rp500.000 telah ditambahkan ke ShopeePay-mu.", "com.shopeepay.id"), 60))["results"][0]
    assert result["type"] == "income"
    assert all(row["movement_id"] is None for row in transactions_of(top_up_owner))


def test_two_qualifying_counterparts_are_flagged_ambiguous(client, top_up_owner):
    from app.db.pool import db_conn
    ingest(client, at(notification(SHOPEEPAY_TOP_UP, "com.shopeepay.id"), 0))
    ingest(client, at(notification(SHOPEEPAY_TOP_UP, "com.shopeepay.id"), 60))
    result = ingest(client, at(notification(BCA_TOP_UP_DEBIT, "com.bca.mybca.omni.android"), 120))["results"][0]
    assert result["type"] == "expense"
    with db_conn() as conn:
        stored = conn.execute("SELECT error_code FROM notification_events WHERE id = %s", (result["event_id"],)).fetchone()
    assert stored["error_code"] == "ambiguous_movement"
    assert all(row["movement_id"] is None for row in transactions_of(top_up_owner))


def test_model_needs_review_falls_back_to_deterministic_record(client, notification_owner, monkeypatch):
    from app.services import notification_processing as processing
    enable_ai(monkeypatch)
    event = ingest(client, notification())["results"][0]
    claimed = processing.claim_event()
    provider = AsyncMock()
    provider.interpret.return_value = Interpretation.model_validate_json(json.dumps({
        "outcome": "needs_review", "direction": None, "description": "", "account_id": None,
        "source_account_id": None, "target_account_id": None, "category_id": None, "kakeibo": None,
        "amount_evidence": "", "direction_evidence": "", "source_evidence": "", "target_evidence": "",
        "candidate_transaction_id": None, "confidence": 0.3, "review_reason": "uncertain_mapping",
    }))
    asyncio.run(processing.process_claim(claimed, provider))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "recorded" and result["type"] == "expense"
    assert count_transactions(notification_owner) == 1


def test_unresolvable_model_uncertainty_reports_backend_reason(client, notification_owner, monkeypatch):
    from app.services import notification_processing as processing
    enable_ai(monkeypatch)
    event = ingest(client, notification("You spent IDR 45,000.00 at Shopping.", "com.bca.mybca.omni.android"))["results"][0]
    claimed = processing.claim_event()
    provider = AsyncMock()
    provider.interpret.side_effect = ProviderError("provider_invalid_output")
    asyncio.run(processing.process_claim(claimed, provider))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result == {**result, "status": "needs_review", "error_code": "uncertain_category_mapping"}
    assert processing.claim_event() is None


def test_provider_outage_becomes_failed_and_is_retried_automatically(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services import notification_processing as processing
    enable_ai(monkeypatch)
    event = ingest(client, notification())["results"][0]
    provider = AsyncMock()
    provider.interpret.side_effect = ProviderError("provider_unavailable", transient=True)
    for _ in range(processing.MAX_ATTEMPTS):
        with db_conn() as conn:
            conn.execute("UPDATE notification_events SET next_attempt_at = NOW() WHERE id = %s", (event["event_id"],))
        asyncio.run(processing.process_claim(processing.claim_event(), provider))
    with db_conn() as conn:
        stored = conn.execute(
            "SELECT processing_state, error_code, next_attempt_at > NOW() + INTERVAL '5 minutes' AS deferred FROM notification_events WHERE id = %s",
            (event["event_id"],),
        ).fetchone()
    assert stored["processing_state"] == "failed" and stored["error_code"] == "provider_unavailable" and stored["deferred"]
    assert processing.claim_event() is None
    with db_conn() as conn:
        conn.execute("UPDATE notification_events SET next_attempt_at = NOW() WHERE id = %s", (event["event_id"],))
    provider.interpret.side_effect = None
    provider.interpret.return_value = interpretation(notification_owner)
    asyncio.run(processing.process_claim(processing.claim_event(), provider))
    assert client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]["status"] == "recorded"


def test_failed_events_stop_retrying_after_horizon(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services import notification_processing as processing
    enable_ai(monkeypatch)
    event = ingest(client, notification())["results"][0]
    processing.fail_claim(processing.claim_event(), ProviderError("provider_access_denied"))
    with db_conn() as conn:
        conn.execute(
            "UPDATE notification_events SET next_attempt_at = NOW(), created_at = NOW() - INTERVAL '49 hours' WHERE id = %s",
            (event["event_id"],),
        )
    assert processing.claim_event() is None


def test_manual_resolution_records_once_and_pairs(client, top_up_owner):
    from app.db.pool import db_conn
    ingest(client, at(notification(SHOPEEPAY_TOP_UP, "com.shopeepay.id"), 0))
    with db_conn() as conn:
        conn.execute("UPDATE categories SET is_archived = TRUE WHERE id = %s", (top_up_owner["shopping"],))
    stuck = ingest(client, at(notification(BCA_TOP_UP_DEBIT, "com.bca.mybca.omni.android"), 3))["results"][0]
    assert stuck["status"] == "needs_review" and stuck["error_code"] == "uncertain_category_mapping"
    body = {"type": "expense", "account_id": top_up_owner["bca"], "category_id": top_up_owner["expense"], "notes": "Top up ShopeePay"}
    resolved = client.post(f"/api/ingest/notifications/{stuck['event_id']}/resolve", json=body)
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["result"]["type"] == "internal_movement"
    repeated = client.post(f"/api/ingest/notifications/{stuck['event_id']}/resolve", json=body)
    assert repeated.status_code == 200 and repeated.json()["result"] == resolved.json()["result"]
    assert len(transactions_of(top_up_owner)) == 2
    with db_conn() as conn:
        stored = conn.execute("SELECT interpretation FROM notification_events WHERE id = %s", (stuck["event_id"],)).fetchone()
    assert stored["interpretation"]["source"] == "manual"


def test_manual_resolution_validates_references_and_state(client, top_up_owner):
    stuck = ingest(client, notification("You've paid Rp50.000, biaya Rp2.500, total Rp52.500"))["results"][0]
    assert stuck["status"] == "needs_review"
    path = f"/api/ingest/notifications/{stuck['event_id']}/resolve"
    base = {"type": "expense", "account_id": top_up_owner["main"], "category_id": top_up_owner["expense"]}
    assert client.post(path, json=base).json()["detail"]["code"] == "amount_required"
    wrong_kind = client.post(path, json={**base, "amount": 52500, "category_id": top_up_owner["income"]})
    assert wrong_kind.status_code == 422 and wrong_kind.json()["detail"]["code"] == "invalid_category_reference"
    investment = client.post(path, json={**base, "amount": 52500, "account_id": top_up_owner["stockbit"]})
    assert investment.status_code == 422
    assert client.post(f"/api/ingest/notifications/{uuid4()}/resolve", json={**base, "amount": 1}).status_code == 404
    ok = client.post(path, json={**base, "amount": 52500})
    assert ok.status_code == 200 and ok.json()["result"]["amount"] == 52500
    recorded = ingest(client, notification())["results"][0]
    assert recorded["status"] == "recorded"
    assert client.post(f"/api/ingest/notifications/{recorded['event_id']}/resolve", json={**base}).status_code == 409


def test_manual_resolution_rejects_amount_conflicting_with_notification(client, top_up_owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute("UPDATE categories SET is_archived = TRUE WHERE id = %s", (top_up_owner["shopping"],))
    stuck = ingest(client, notification(BCA_TOP_UP_DEBIT, "com.bca.mybca.omni.android"))["results"][0]
    response = client.post(f"/api/ingest/notifications/{stuck['event_id']}/resolve", json={
        "type": "expense", "account_id": top_up_owner["bca"], "category_id": top_up_owner["expense"], "amount": 1,
    })
    assert response.status_code == 422 and response.json()["detail"]["code"] == "conflicting_amount"


def test_split_restores_notification_categories_and_results(client, top_up_owner):
    ingest(client, at(notification(SHOPEEPAY_TOP_UP, "com.shopeepay.id"), 0))
    linked = ingest(client, at(notification(BCA_TOP_UP_DEBIT, "com.bca.mybca.omni.android"), 3))["results"][0]
    movement_id = transactions_of(top_up_owner)[0]["movement_id"]
    response = client.post(f"/api/movements/{movement_id}/split")
    assert response.status_code == 200, response.text
    rows = {row["type"]: row for row in transactions_of(top_up_owner)}
    assert all(row["movement_id"] is None and row["movement_role"] is None for row in rows.values())
    assert rows["expense"]["category"] == "Belanja" and rows["income"]["category"] == "Transfer Masuk"
    result = client.get(f"/api/ingest/notifications/{linked['event_id']}/result").json()["result"]
    assert result["type"] == "expense" and result["transaction_id"] == str(rows["expense"]["id"])
    assert client.post(f"/api/movements/{movement_id}/split").status_code == 404
    merged = client.post("/api/movements/merge", json={
        "expense_transaction_id": str(rows["expense"]["id"]), "income_transaction_id": str(rows["income"]["id"]),
    })
    assert merged.status_code == 200


def test_split_rejects_investment_trades(client, notification_owner):
    ingest(client, notification("Pembelian 4 lot FIKS match di harga Rp3.340", "com.stockbit.android"))
    movement_id = next(row["movement_id"] for row in transactions_of(notification_owner) if row["movement_id"])
    response = client.post(f"/api/movements/{movement_id}/split")
    assert response.status_code == 422 and response.json()["detail"]["code"] == "investment_trade_required"


def test_owner_aliases_drive_self_transfer_recognition(client, top_up_owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute("UPDATE users SET name_aliases = %s WHERE id = %s", (["Alfonsus Enrico Soebijanto"], top_up_owner["user_id"]))
    ingest_with_source_confirmation(client, at(notification("You've sent Rp1.500.000 to ALFONSUS ENRICO SOEBIJANTO.", "com.jago.digitalbanking"), 0), top_up_owner)
    result = ingest(client, at(notification(
        "You received IDR 1,500,000.00 from ALFO**US ***ICO *O at Account Transfer category.", "com.bca.mybca.omni.android"), 90))["results"][0]
    assert result["type"] == "internal_movement" and result["target"] == "BCA Harian"


def test_settings_round_trip_normalizes_name_aliases(client, notification_owner):
    response = client.patch("/api/auth/settings", json={"name_aliases": ["  Raka   Purnama ", "raka purnama", "RAKA P SENTOSA"]})
    assert response.status_code == 200, response.text
    assert response.json()["user"]["name_aliases"] == ["Raka Purnama", "RAKA P SENTOSA"]
    assert client.patch("/api/auth/settings", json={"name_aliases": ["ab"]}).status_code == 422
    assert client.patch("/api/auth/settings", json={"name_aliases": [f"Alias {n}" for n in range(11)]}).status_code == 422
    cleared = client.patch("/api/auth/settings", json={"name_aliases": []})
    assert cleared.status_code == 200 and cleared.json()["user"]["name_aliases"] == []


@pytest.mark.parametrize("bank_first", [False, True])
def test_pocket_move_then_main_pocket_to_bca_books_two_movements(client, notification_owner, bank_first):
    """Owner-confirmed 2026-09-27 flow: Emergency Fund -> Main Pocket, then Main Pocket -> BCA.

    The pocket-move and BCA texts are verbatim device captures (names fictionalised); the Jago
    outbound wording was never captured, so this uses a plausible form until the monitored run.
    """
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute("UPDATE users SET name_aliases = %s WHERE id = %s", (["Raka Purnama"], notification_owner["user_id"]))
    pocket_move = at(notification(
        "You've moved Rp1.500.000 out of your My Emergency Fund Pocket. Need help? Contact Tanya Jago at 1500 746.",
        "com.jago.digitalBanking"), 22)
    jago_out = at(notification("You've sent Rp1.500.000 to RAKA PURNAMA.", "com.jago.digitalBanking"), 35)
    bca_in = at(notification(
        "You received IDR 1,500,000.00 from RA*A ***NAMA at Account Transfer category.", "com.bca.mybca.omni.android"), 52)
    moved = ingest(client, pocket_move)["results"][0]
    assert moved["type"] == "internal_movement"
    assert (moved["source"], moved["target"]) == ("Bank Jago · Dana Darurat", "Bank Jago · Kantong Utama")
    later = [bca_in, jago_out] if bank_first else [jago_out, bca_in]
    ingest_with_source_confirmation(client, later[0], notification_owner)
    paired = ingest_with_source_confirmation(client, later[1], notification_owner)
    assert paired["type"] == "internal_movement"
    assert (paired["source"], paired["target"]) == ("Bank Jago · Kantong Utama", "BCA Harian")
    rows = transactions_of(notification_owner)
    assert len(rows) == 4 and len({row["movement_id"] for row in rows}) == 2
    assert all(row["category"] == "Internal Movement" for row in rows)


def test_paired_legs_are_renamed_as_one_movement(client, top_up_owner):
    ingest(client, at(notification(SHOPEEPAY_TOP_UP, "com.shopeepay.id"), 0))
    result = ingest(client, at(notification(BCA_TOP_UP_DEBIT, "com.bca.mybca.omni.android"), 3))["results"][0]
    assert result["description"] == "Pindah saldo ke ShopeePay"
    assert {row["notes"] for row in transactions_of(top_up_owner)} == {"Pindah saldo ke ShopeePay"}


def test_unpaired_self_transfer_is_named_neutrally(client, notification_owner):
    result = ingest(client, notification("Raka Purnama has sent Rp125.000 to you. Need help? Contact Tanya Jago at 1500 746."))["results"][0]
    assert result["type"] == "income" and result["description"] == "Pindah saldo masuk"
    [row] = transactions_of(notification_owner)
    assert row["notes"] == "Pindah saldo masuk" and row["category"] == "Internal Movement"


def test_ai_self_transfer_description_is_replaced(client, notification_owner, monkeypatch):
    from app.services import notification_processing as processing
    enable_ai(monkeypatch)
    event = ingest(client, notification("Raka Purnama has sent Rp125.000 to you."))["results"][0]
    provider = AsyncMock()
    provider.interpret.return_value = Interpretation.model_validate_json(json.dumps({
        "outcome": "record", "direction": "income", "description": "Transfer masuk dari Raka",
        "account_id": notification_owner["main"], "category_id": notification_owner["income"], "kakeibo": None,
        "source_account_id": None, "target_account_id": None, "amount_evidence": "Rp125.000",
        "direction_evidence": "has sent", "source_evidence": "", "target_evidence": "",
        "candidate_transaction_id": None, "confidence": 0.9, "review_reason": "none",
    }))
    asyncio.run(processing.process_claim(processing.claim_event(), provider))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["description"] == "Pindah saldo masuk"
    assert transactions_of(notification_owner)[0]["category"] == "Internal Movement"


def test_manual_merge_renames_notification_legs_only(client, top_up_owner):
    from app.db.pool import db_conn
    add_category(top_up_owner, "transfer_out", "Transfer Keluar")
    ingest(client, at(notification("Rp500.000 udah dikirim ke BCA Mira Langit.", "com.gojek.gopay"), 0))
    ingest(client, at(notification("Pengisian saldo sebesar Rp500.000 telah ditambahkan ke ShopeePay-mu.", "com.shopeepay.id"), 60))
    rows = {row["type"]: row for row in transactions_of(top_up_owner)}
    merged = client.post("/api/movements/merge", json={
        "expense_transaction_id": str(rows["expense"]["id"]), "income_transaction_id": str(rows["income"]["id"]),
    })
    assert merged.status_code == 200, merged.text
    assert {row["notes"] for row in transactions_of(top_up_owner)} == {"Pindah saldo ke ShopeePay"}

    with db_conn() as conn:
        manual = []
        for kind, account, category in [("expense", "bca", "shopping"), ("income", "gopay", "income")]:
            manual.append(str(conn.execute(
                """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date)
                   VALUES (%s, %s, %s, %s, 77000, 'Catatan sendiri', NOW()) RETURNING id""",
                (top_up_owner["user_id"], top_up_owner[account], top_up_owner[category], kind),
            ).fetchone()["id"]))
    assert client.post("/api/movements/merge", json={
        "expense_transaction_id": manual[0], "income_transaction_id": manual[1],
    }).status_code == 200
    with db_conn() as conn:
        notes = {row["notes"] for row in conn.execute("SELECT notes FROM transactions WHERE id = ANY(%s)", (manual,)).fetchall()}
    assert notes == {"Catatan sendiri"}


def test_wallet_moved_under_bank_receives_both_apps_notifications(client, notification_owner):
    moved = client.patch(f"/api/accounts/{notification_owner['gopay']}", json={
        "parent_id": notification_owner["jago"], "name": "GoPay Tabungan",
    })
    assert moved.status_code == 200, moved.text
    from app.db.pool import db_conn
    add_category(notification_owner, "belanja", "Belanja")
    with db_conn() as conn:
        conn.execute("INSERT INTO merchant_category_rules (user_id, merchant_pattern, category_id) VALUES (%s, 'Kedai Awan', %s)",
                     (notification_owner["user_id"], notification_owner["belanja"]))
    payment = ingest(client, notification("QRIS Rp32.000 ke Kedai Awan berhasil", "com.gopay.wallet"))["results"][0]
    assert (payment["status"], payment["type"], payment["source"]) == ("recorded", "expense", "Bank Jago · GoPay Tabungan"), payment
    pocket_move = ingest(client, notification(
        "Rp1.250.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket.", "com.jago.digitalBanking",
        seconds=40))["results"][0]
    assert pocket_move["type"] == "internal_movement"
    assert (pocket_move["source"], pocket_move["target"]) == ("Bank Jago · Kantong Utama", "Bank Jago · GoPay Tabungan")


GOPAY_TABUNGAN_MOVE = "Rp1.250.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket."


def mapped_move(owner, target_key, confidence, *, alternatives=(), with_mapping=True):
    return Interpretation.model_validate_json(json.dumps({
        "outcome": "record", "direction": "internal_movement", "description": "Pindah saldo",
        "account_id": None, "source_account_id": owner["main"], "target_account_id": owner[target_key],
        "category_id": None, "kakeibo": None, "amount_evidence": "Rp1.250.000",
        "direction_evidence": "has been moved", "source_evidence": "", "target_evidence": "",
        "candidate_transaction_id": None, "confidence": 0.9, "review_reason": "none",
        "mappings": [{"role": "target", "account_id": owner[target_key], "confidence": confidence,
                      "alternatives": [owner[key] for key in alternatives]}] if with_mapping else [],
    }))


def ingest_move(client, version="1.3.0", seconds=27):
    event = notification(GOPAY_TABUNGAN_MOVE, "com.jago.digitalBanking", seconds=seconds)
    event["source_version"] = version
    return ingest(client, event)["results"][0]


def process_with(proposal):
    from app.services import notification_processing as processing
    provider = AsyncMock()
    provider.interpret.return_value = proposal
    asyncio.run(processing.process_claim(processing.claim_event(), provider))


def aliases_of(owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute(
            "SELECT institution, name_normalized, account_id, source FROM notification_account_aliases WHERE user_id = %s",
            (owner["user_id"],),
        ).fetchall()


def test_confident_mapping_records_labels_and_learns_alias(client, notification_owner, monkeypatch):
    from app.services import notification_processing as processing
    enable_ai(monkeypatch)
    event = ingest_move(client)
    process_with(mapped_move(notification_owner, "gopay", 0.93))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "recorded" and result["type"] == "internal_movement"
    assert (result["source"], result["target"]) == ("Bank Jago · Kantong Utama", "GoPay")
    assert result["mapped"] == [{"name": "GoPay Tabungan", "account": "GoPay", "mode": "auto", "confidence": 0.93}]
    assert [(row["institution"], row["name_normalized"], str(row["account_id"]), row["source"]) for row in aliases_of(notification_owner)] == [
        ("jago", "gopay tabungan", notification_owner["gopay"], "ai")]
    second = ingest_move(client, seconds=40)
    facts, context = processing.read_context(processing.claim_event())
    assert "unresolved_names" not in context["facts"]
    assert context["facts"]["target_account_id"] == notification_owner["gopay"]


def test_uncertain_mapping_waits_for_owner_and_confirmation_records(client, notification_owner, monkeypatch):
    from app.services import notification_processing as processing
    enable_ai(monkeypatch)
    event = ingest_move(client)
    process_with(mapped_move(notification_owner, "gopay", 0.55, alternatives=("emergency", "bca")))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "needs_confirmation" and count_transactions(notification_owner) == 0
    proposal = result["mapping_proposal"]
    assert (proposal["name"], proposal["role"], proposal["confidence"]) == ("GoPay Tabungan", "target", 0.55)
    assert proposal["proposed"] == {"id": notification_owner["gopay"], "label": "GoPay"}
    assert [item["label"] for item in proposal["alternatives"]] == ["Bank Jago · Dana Darurat", "BCA Harian"]
    assert processing.claim_event() is None and aliases_of(notification_owner) == []

    confirm = client.post(f"/api/ingest/notifications/{event['event_id']}/confirm-mapping",
                          json={"account_id": notification_owner["emergency"]})
    assert confirm.status_code == 200 and confirm.json()["result"]["status"] == "queued"
    assert client.post(f"/api/ingest/notifications/{event['event_id']}/confirm-mapping",
                       json={"account_id": notification_owner["emergency"]}).status_code == 409
    process_with(mapped_move(notification_owner, "emergency", 0.0, with_mapping=False))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "recorded" and result["target"] == "Bank Jago · Dana Darurat"
    assert [(str(row["account_id"]), row["source"]) for row in aliases_of(notification_owner)] == [
        (notification_owner["emergency"], "owner")]


def test_owner_alias_is_not_replaced_by_a_later_ai_mapping(client, notification_owner):
    from app.db.pool import db_conn
    from app.services.notification_mapping import store_aliases
    entry = {"institution": "jago", "name_normalized": "gopay tabungan", "name": "GoPay Tabungan"}
    with db_conn() as conn:
        with conn.cursor() as cur:
            store_aliases(cur, notification_owner["user_id"], [{**entry, "account_id": notification_owner["gopay"]}], "owner")
            store_aliases(cur, notification_owner["user_id"], [{**entry, "account_id": notification_owner["bca"]}], "ai")
        conn.commit()
    [row] = aliases_of(notification_owner)
    assert (str(row["account_id"]), row["source"]) == (notification_owner["gopay"], "owner")


def test_old_companion_builds_keep_receiving_needs_review(client, notification_owner, monkeypatch):
    enable_ai(monkeypatch)
    event = ingest_move(client, version="1.2.0")
    process_with(mapped_move(notification_owner, "gopay", 0.55))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert (result["status"], result["error_code"]) == ("needs_review", "uncertain_pocket_mapping")


def test_ineligible_mapping_is_rejected_even_when_confident(client, notification_owner, monkeypatch):
    enable_ai(monkeypatch)
    event = ingest_move(client)
    investment_target = json.loads(mapped_move(notification_owner, "gopay", 0.99).model_dump_json())
    investment_target["target_account_id"] = notification_owner["stockbit"]
    investment_target["mappings"][0]["account_id"] = notification_owner["stockbit"]
    process_with(Interpretation.model_validate_json(json.dumps(investment_target)))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "needs_review" and count_transactions(notification_owner) == 0
    assert aliases_of(notification_owner) == []


def test_confirmation_is_owner_scoped_and_validates_account(client, notification_owner, monkeypatch):
    enable_ai(monkeypatch)
    event = ingest_move(client)
    process_with(mapped_move(notification_owner, "gopay", 0.55))
    path = f"/api/ingest/notifications/{event['event_id']}/confirm-mapping"
    assert client.post(f"/api/ingest/notifications/{uuid4()}/confirm-mapping", json={"account_id": notification_owner["gopay"]}).status_code == 404
    assert client.post(path, json={"account_id": notification_owner["stockbit"]}).status_code == 422
    assert client.post(path, json={"account_id": str(uuid4())}).status_code == 422


def test_alias_list_and_removal_only_affect_future_events(client, notification_owner, monkeypatch):
    enable_ai(monkeypatch)
    ingest_move(client)
    process_with(mapped_move(notification_owner, "gopay", 0.93))
    [alias] = client.get("/api/ingest/aliases").json()["aliases"]
    assert (alias["name"], alias["account"], alias["source"], alias["institution"]) == ("GoPay Tabungan", "GoPay", "ai", "jago")
    assert client.delete(f"/api/ingest/aliases/{alias['id']}").status_code == 200
    assert client.delete(f"/api/ingest/aliases/{alias['id']}").status_code == 404
    assert client.get("/api/ingest/aliases").json()["aliases"] == []
    assert count_transactions(notification_owner) == 2
    from app.services import notification_processing as processing
    ingest_move(client, seconds=50)
    facts, context = processing.read_context(processing.claim_event())
    assert context["facts"]["unresolved_names"][0]["name"] == "GoPay Tabungan"


def test_needs_confirmation_event_can_still_be_recorded_manually_or_retried(client, notification_owner, monkeypatch):
    enable_ai(monkeypatch)
    event = ingest_move(client)
    process_with(mapped_move(notification_owner, "gopay", 0.55))
    retried = client.post(f"/api/ingest/notifications/{event['event_id']}/retry")
    assert retried.status_code == 200 and retried.json()["result"]["status"] == "queued"


def test_retry_from_newer_app_enables_confirmation_for_old_events(client, notification_owner, monkeypatch):
    enable_ai(monkeypatch)
    event = ingest_move(client, version="1.1.0")
    process_with(mapped_move(notification_owner, "gopay", 0.6))
    assert client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]["status"] == "needs_review"
    retried = client.post(f"/api/ingest/notifications/{event['event_id']}/retry", headers={"X-Companion-Version": "1.3.1"})
    assert retried.status_code == 200
    process_with(mapped_move(notification_owner, "gopay", 0.6))
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "needs_confirmation" and result["mapping_proposal"]["proposed"]["label"] == "GoPay"


def test_confirmed_names_reach_later_model_context_with_their_source(client, notification_owner, monkeypatch):
    from app.services import notification_processing as processing
    enable_ai(monkeypatch)
    event = ingest_move(client)
    process_with(mapped_move(notification_owner, "gopay", 0.55))
    client.post(f"/api/ingest/notifications/{event['event_id']}/confirm-mapping", json={"account_id": notification_owner["gopay"]})
    process_with(mapped_move(notification_owner, "gopay", 0.0, with_mapping=False))
    later = notification("Rp200.000 has been moved from your Main Pocket Pocket to your Tabungan GoPay Pocket.",
                         "com.jago.digitalBanking", seconds=50)
    later["source_version"] = "1.3.1"
    ingest(client, later)
    facts, context = processing.read_context(processing.claim_event())
    [unresolved] = context["facts"]["unresolved_names"]
    assert unresolved["name"] == "Tabungan GoPay"
    gopay = next(row for row in context["accounts"] if str(row["id"]) == notification_owner["gopay"])
    assert gopay["learned_notification_names"] == [{
        "key": "jago|gopay tabungan", "name": "GoPay Tabungan", "institution": "jago", "confirmed_by": "owner",
    }]


def test_unusable_model_answer_is_asked_again_once(client, notification_owner, monkeypatch):
    from app.services import notification_processing as processing
    enable_ai(monkeypatch)
    event = ingest(client, notification())["results"][0]
    provider = AsyncMock()
    provider.interpret.side_effect = [ProviderError("provider_invalid_output"), interpretation(notification_owner)]
    asyncio.run(processing.process_claim(processing.claim_event(), provider))
    assert provider.interpret.await_count == 2
    result = client.get(f"/api/ingest/notifications/{event['event_id']}/result").json()["result"]
    assert result["status"] == "recorded"
    assert processing.claim_event() is None


RDN_TOP_UP = "RDN earning of IDR 225,180.00 at Account Transfer category."


@pytest.mark.parametrize("rdn_first", [True, False])
def test_rdn_top_up_is_an_own_transfer_and_pairs_with_the_sending_leg(client, notification_owner, rdn_first):
    rdn = at(notification(RDN_TOP_UP, "com.bca.mybca.omni.android"), 60)
    jago = at(notification("You've sent Rp225.180 to RAKA PURNAMA.", "com.jago.digitalBanking"), 30)
    first, second = (rdn, jago) if rdn_first else (jago, rdn)
    alone = ingest_with_source_confirmation(client, first, notification_owner)
    if rdn_first:
        assert (alone["type"], alone["target"], alone["description"]) == ("income", "RDN BCA", "Pindah saldo masuk")
        [row] = transactions_of(notification_owner)
        assert row["category"] == "Internal Movement"
    paired = ingest_with_source_confirmation(client, second, notification_owner)
    assert paired["type"] == "internal_movement"
    assert (paired["source"], paired["target"]) == ("Bank Jago · Kantong Utama", "RDN BCA")


def test_ordinary_bca_notifications_never_land_on_rdn(client, notification_owner):
    add_category(notification_owner, "belanja", "Belanja")
    result = ingest(client, notification("You spent IDR 45,000.00 at Shopping.", "com.bca.mybca.omni.android"))["results"][0]
    assert result["source"] == "BCA Harian"


def test_two_rdn_accounts_send_rdn_notifications_to_review(client, notification_owner):
    add_account(notification_owner, "second_rdn", "RDN Mandiri Sekuritas", "bank")
    result = ingest(client, notification(RDN_TOP_UP, "com.bca.mybca.omni.android"))["results"][0]
    assert (result["status"], result["error_code"]) == ("needs_review", "uncertain_pocket_mapping")



@pytest.mark.parametrize("jago_last", [True, False])
def test_jago_transfer_to_bca_pairs_with_the_bca_credit(client, notification_owner, jago_last):
    """Device sequence of 2026-09-30 00:13 with fictional names: pocket move, BCA credit, Jago transfer."""
    ingest(client, at(notification("You've moved Rp625.000 out of your My Emergency Fund Pocket.", "com.jago.digitalBanking"), 0))
    bca = at(notification("You received IDR 625,000.00 from RA*A ***NAMA at Account Transfer category.", "com.bca.mybca.omni.android"), 42)
    jago = at(notification("You've transferred Rp625.000 to RAKA PURNAMA. Need help? Contact Tanya Jago at 1500 746.", "com.jago.digitalBanking"), 46)
    first, second = (bca, jago) if jago_last else (jago, bca)
    ingest_with_source_confirmation(client, first, notification_owner)
    paired = ingest_with_source_confirmation(client, second, notification_owner)
    assert paired["type"] == "internal_movement"
    assert (paired["source"], paired["target"]) == ("Bank Jago · Kantong Utama", "BCA Harian")
    rows = transactions_of(notification_owner)
    assert len(rows) == 4 and len({row["movement_id"] for row in rows}) == 2


STOCKBIT_DEPOSIT = "Dana kamu senilai Rp225,180 sudah dapat digunakan"


def deposit_events(bank_seconds, broker_seconds, broker_amount="225,180"):
    bank = at(notification(RDN_TOP_UP, "com.bca.mybca.omni.android"), bank_seconds)
    broker = at(notification(f"Dana kamu senilai Rp{broker_amount} sudah dapat digunakan", "com.stockbit.android"), broker_seconds)
    broker["title"] = "Deposit Berhasil"
    return bank, broker


@pytest.mark.parametrize("bank_first", [True, False])
def test_bank_and_broker_reports_of_one_rdn_deposit_record_once(client, notification_owner, bank_first):
    from app.db.pool import db_conn
    bank, broker = deposit_events(0, 3 * 3600 + 120)
    first, second = (bank, broker) if bank_first else (broker, bank)
    recorded = ingest(client, first)["results"][0]
    confirmed = ingest(client, second)["results"][0]
    assert (recorded["type"], recorded["target"]) == ("income", "RDN BCA")
    assert confirmed["status"] == "recorded" and confirmed["record_key"] == recorded["record_key"]
    [row] = transactions_of(notification_owner)
    assert row["category"] == "Internal Movement" and str(row["account_id"]) == notification_owner["funding"]
    with db_conn() as conn:
        kinds = conn.execute(
            "SELECT provenance_kind FROM notification_events WHERE user_id = %s ORDER BY post_time",
            (notification_owner["user_id"],),
        ).fetchall()
    assert sorted(kind["provenance_kind"] for kind in kinds) == ["deposit_confirmation", "observed_leg"]


def test_rdn_deposit_reports_with_different_amounts_or_far_apart_stay_separate(client, notification_owner):
    bank, broker = deposit_events(0, 600, broker_amount="225,181")
    ingest(client, bank)
    ingest(client, broker)
    assert len(transactions_of(notification_owner)) == 2
    later_bank, far_broker = deposit_events(100, 100 + 49 * 3600)
    later_bank["body_text"] = "RDN earning of IDR 99,000.00 at Account Transfer category."
    far_broker["body_text"] = "Dana kamu senilai Rp99,000 sudah dapat digunakan"
    ingest(client, later_bank)
    ingest(client, far_broker)
    assert len(transactions_of(notification_owner)) == 4


def test_broker_deposit_alone_records_into_the_funding_account(client, notification_owner):
    _, broker = deposit_events(0, 0)
    result = ingest(client, broker)["results"][0]
    assert (result["status"], result["type"], result["target"], result["description"]) == (
        "recorded", "income", "RDN BCA", "Pindah saldo masuk")


def test_broker_trades_keep_the_trade_path(client, notification_owner):
    result = ingest(client, notification("Pembelian 4 lot FIKS match di harga Rp3.340", "com.stockbit.android"))["results"][0]
    assert result["status"] == "recorded" and result["type"] == "internal_movement"
