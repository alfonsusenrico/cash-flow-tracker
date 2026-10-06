import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from uuid import uuid4

import pytest

from app.services.notification_evidence import collect_facts, requires_source_pocket
from app.services.notification_resolution import observed_endpoint
from test_notification_processing_database import count_transactions, enable_ai, ingest, notification, notification_owner


@pytest.fixture(autouse=True)
def transfer_category(request):
    if "notification_owner" not in request.fixturenames:
        return
    from app.db.pool import db_conn

    owner = request.getfixturevalue("notification_owner")
    with db_conn() as conn:
        conn.execute("INSERT INTO categories (user_id, name, kind) VALUES (%s,'Transfer Keluar','expense')", (owner["user_id"],))


def transfer(*, version="1.6.0", seconds=27, recipient="Andra", package="com.jago.digitalbanking"):
    payload = notification(f"You've transferred Rp125.000 to {recipient}. Need help?", package, seconds=seconds)
    payload["source_version"] = version
    return payload


def current(client, result):
    return client.get(f"/api/ingest/notifications/{result['event_id']}/result").json()["result"]


def complete(client, result):
    from app.services import notification_processing as processing

    claimed = processing.claim_event(include_ai=False)
    if claimed:
        asyncio.run(processing.process_claim(claimed, None))
    return current(client, result)


def answer(result, account_id):
    return {"question_id": result["confirmation"]["question_id"], "reply_id": str(uuid4()), "account_id": account_id}


def respond(client, result, payload):
    return client.post(f"/api/ingest/notifications/{result['event_id']}/confirm-source-pocket", json=payload)


@pytest.mark.parametrize("package", ["com.jago.digitalbanking", "com.jago.digitalBanking"])
def test_missing_source_is_not_an_endpoint(package):
    payload = transfer(package=package)
    payload["post_time"] = datetime.fromisoformat(payload["post_time"])
    facts = collect_facts(payload)
    assert requires_source_pocket(facts)
    with pytest.raises(ValueError, match="source_pocket_confirmation_required"):
        observed_endpoint(facts, [])


@pytest.mark.parametrize("text", [
    "You've paid Rp125.000 to Kedai Awan.",
    "You've moved Rp125.000 out of your Dana Darurat Pocket.",
    "Andra has sent Rp125.000 to you.",
    "You've transferred Rp125.000 to Andra using your Dana Darurat Pocket.",
])
def test_informative_or_other_jago_events_do_not_trigger(text):
    payload = notification(text)
    payload["post_time"] = datetime.fromisoformat(payload["post_time"])
    assert not requires_source_pocket(collect_facts(payload))


def test_hold_and_complete_options_are_event_scoped(client, notification_owner):
    from app.db.pool import db_conn

    owner = notification_owner
    extra = str(uuid4())
    with db_conn() as conn:
        conn.execute("INSERT INTO accounts (id, user_id, parent_id, name, type) VALUES (%s,%s,%s,'Belanja','bank')",
                     (extra, owner["user_id"], owner["jago"]))
    result = ingest(client, transfer())["results"][0]
    assert result["status"] == "needs_confirmation" and result["confirmation"]["type"] == "source_pocket"
    assert count_transactions(owner) == 0
    options = client.get(f"/api/ingest/notifications/{result['event_id']}/source-pockets").json()["result"]
    assert options["question_id"] == result["confirmation"]["question_id"]
    assert {row["id"] for row in options["options"]} == {owner["main"], owner["emergency"], extra}
    accepted = answer(result, owner["emergency"])
    assert respond(client, result, accepted).json()["result"]["status"] == "queued"
    recorded = complete(client, result)
    assert recorded["status"] == "recorded" and recorded["source"] == "Bank Jago · Dana Darurat"
    assert respond(client, result, accepted).json()["result"] == recorded
    assert count_transactions(owner) == 1
    assert ingest(client, transfer(seconds=28))["results"][0]["status"] == "needs_confirmation"
    with db_conn() as conn:
        assert conn.execute("SELECT default_pocket_id FROM accounts WHERE id=%s", (owner["jago"],)).fetchone()["default_pocket_id"] == uuid_value(owner["main"])
        assert conn.execute("SELECT COUNT(*) AS count FROM notification_account_aliases WHERE user_id=%s", (owner["user_id"],)).fetchone()["count"] == 0


def uuid_value(value):
    from uuid import UUID
    return UUID(value)


@pytest.mark.parametrize("version", [None, "1.5.0", "bad"])
def test_old_client_is_reviewed_without_defaulting(client, notification_owner, version):
    result = ingest(client, transfer(version=version))["results"][0]
    assert result["status"] == "needs_review" and "confirmation" not in result
    assert count_transactions(notification_owner) == 0


def test_upgrade_retry_and_question_identity(client, notification_owner):
    result = ingest(client, transfer(version="1.5.0"))["results"][0]
    assert client.post(f"/api/ingest/notifications/{result['event_id']}/retry", headers={"X-Companion-Version": "1.6.0"}).status_code == 200
    question = complete(client, result)
    assert question["confirmation"]["type"] == "source_pocket"
    assert client.post(f"/api/ingest/notifications/{result['event_id']}/retry").status_code == 200
    assert complete(client, result)["confirmation"]["question_id"] == question["confirmation"]["question_id"]


def test_invalid_and_conflicting_answers_cannot_write(client, notification_owner):
    result = ingest(client, transfer())["results"][0]
    owner = notification_owner
    payload = answer(result, owner["emergency"])
    assert respond(client, result, {**payload, "account_id": owner["bca"]}).status_code == 409
    assert respond(client, result, {**payload, "question_id": str(uuid4())}).status_code == 409
    assert respond(client, result, {**payload, "account_id": "invalid"}).status_code == 422
    assert respond(client, result, payload).status_code == 200
    assert respond(client, result, {**payload, "account_id": owner["main"]}).status_code == 409
    assert respond(client, result, {**payload, "reply_id": str(uuid4())}).status_code == 409
    assert complete(client, result)["source"] == "Bank Jago · Dana Darurat"
    assert count_transactions(owner) == 1


def test_generic_resolution_cannot_bypass_source_question(client, notification_owner):
    result = ingest(client, transfer())["results"][0]
    assert client.post(f"/api/ingest/notifications/{result['event_id']}/resolve", json={
        "type": "expense", "account_id": notification_owner["main"], "category_id": notification_owner["expense"],
    }).status_code == 409
    assert client.post(f"/api/ingest/notifications/{result['event_id']}/confirm-mapping", json={
        "account_id": notification_owner["main"],
    }).status_code == 409
    assert count_transactions(notification_owner) == 0


def test_other_owner_cannot_read_or_answer(client, notification_owner):
    from app.main import app
    from app.services.auth import get_current_user

    result = ingest(client, transfer())["results"][0]
    payload = answer(result, notification_owner["emergency"])
    app.dependency_overrides[get_current_user] = lambda: {"id": str(uuid4())}
    assert respond(client, result, payload).status_code == 404
    assert client.get(f"/api/ingest/notifications/{result['event_id']}/source-pockets").status_code == 404


def test_archived_source_after_ack_asks_again(client, notification_owner):
    from app.db.pool import db_conn

    result = ingest(client, transfer())["results"][0]
    payload = answer(result, notification_owner["emergency"])
    assert respond(client, result, payload).status_code == 200
    with db_conn() as conn:
        conn.execute("UPDATE accounts SET is_archived=TRUE WHERE id=%s", (notification_owner["emergency"],))
    refreshed = complete(client, result)
    assert refreshed["status"] == "needs_confirmation"
    assert refreshed["confirmation"]["question_id"] != payload["question_id"]
    assert respond(client, result, payload).json()["result"] == refreshed
    assert count_transactions(notification_owner) == 0


def test_concurrent_replays_and_provider_fallback_use_selected_source(client, notification_owner, monkeypatch):
    from app.services import notification_processing as processing

    enable_ai(monkeypatch)
    result = ingest(client, transfer())["results"][0]
    assert result["status"] == "needs_confirmation"
    payload = answer(result, notification_owner["emergency"])
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: processing.confirm_source_pocket(result["event_id"], notification_owner["user_id"], payload), range(2)))
    assert all(row["status"] == "queued" for row in results)
    claimed = processing.claim_event()
    assert processing.apply_claim(claimed, None, "provider_invalid_output")
    assert current(client, result)["source"] == "Bank Jago · Dana Darurat"
    assert count_transactions(notification_owner) == 1


@pytest.mark.parametrize("recipient", ["MAIN POCKET", "FROM SAVINGS POCKET", "DANA DARURAT"])
def test_recipient_pocket_words_are_not_source_evidence(recipient):
    payload = transfer(recipient=recipient)
    payload["post_time"] = datetime.fromisoformat(payload["post_time"])
    assert requires_source_pocket(collect_facts(payload))


@pytest.mark.parametrize("credit_first", [True, False])
def test_counterpart_during_hold_pairs_only_after_non_main_choice(client, notification_owner, credit_first):
    from app.db.pool import db_conn
    from test_notification_processing_database import transactions_of

    owner = notification_owner
    debit = transfer(recipient="RAKA PURNAMA")
    credit = notification("You received IDR 125,000.00 from RA*A ***NAMA at Account Transfer category.",
                          "com.bca.mybca.omni.android", seconds=29)
    if credit_first:
        ingest(client, credit)
    held = ingest(client, debit)["results"][0]
    assert held["status"] == "needs_confirmation"
    if not credit_first:
        ingest(client, credit)
    rows = transactions_of(owner)
    assert len(rows) == 1 and str(rows[0]["account_id"]) == owner["bca"]
    with db_conn() as conn:
        assert conn.execute("SELECT COUNT(*) AS count FROM notification_events WHERE user_id=%s AND (movement_id IS NOT NULL OR confirmed_role IS NOT NULL)", (owner["user_id"],)).fetchone()["count"] == 0
    assert respond(client, held, answer(held, owner["emergency"])).status_code == 200
    recorded = complete(client, held)
    assert recorded["type"] == "internal_movement"
    assert (recorded["source"], recorded["target"]) == ("Bank Jago · Dana Darurat", "BCA Harian")
    rows = transactions_of(owner)
    assert len(rows) == 2 and len({row["movement_id"] for row in rows}) == 1
    assert {str(row["account_id"]) for row in rows} == {owner["bca"], owner["emergency"]}


def test_options_exceed_model_context_and_changed_list_keeps_question(client, notification_owner):
    from app.db.pool import db_conn

    owner = notification_owner
    held = ingest(client, transfer())["results"][0]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO accounts (user_id,parent_id,name,type) VALUES (%s,%s,%s,'bank')",
                            [(owner["user_id"], owner["jago"], f"Kantong {index}") for index in range(205)])
    options = client.get(f"/api/ingest/notifications/{held['event_id']}/source-pockets").json()["result"]
    assert len(options["options"]) == 207
    assert options["question_id"] == held["confirmation"]["question_id"]
    assert count_transactions(owner) == 0


def test_single_eligible_source_still_requires_explicit_choice(client, notification_owner):
    from app.db.pool import db_conn

    owner = notification_owner
    with db_conn() as conn:
        conn.execute("UPDATE accounts SET is_archived=TRUE WHERE id=%s", (owner["emergency"],))
    held = ingest(client, transfer())["results"][0]
    options = client.get(f"/api/ingest/notifications/{held['event_id']}/source-pockets").json()["result"]
    assert len(options["options"]) == 1 and options["options"][0]["id"] == owner["main"]
    assert held["status"] == "needs_confirmation" and count_transactions(owner) == 0


def test_model_cannot_override_confirmed_pocket(client, notification_owner, monkeypatch):
    from app.services import notification_processing as processing
    from test_notification_processing_database import interpretation

    enable_ai(monkeypatch)
    held = ingest(client, transfer())["results"][0]
    assert respond(client, held, answer(held, notification_owner["emergency"])).status_code == 200
    claimed = processing.claim_event()
    proposal = interpretation(notification_owner).model_copy(update={"direction_evidence": "You've transferred"})
    assert processing.apply_claim(claimed, proposal)
    result = current(client, held)
    assert (result["status"], result["error_code"]) == ("needs_review", "conflicting_source_pocket")
    assert count_transactions(notification_owner) == 0


def test_auth_and_origin_checks_protect_source_endpoints(client, notification_owner):
    from fastapi.testclient import TestClient
    from app.main import app
    from app.services.auth import get_current_user

    held = ingest(client, transfer())["results"][0]
    payload = answer(held, notification_owner["emergency"])
    overridden = app.dependency_overrides.pop(get_current_user)
    unauthenticated = TestClient(app, base_url="https://testserver")
    try:
        assert unauthenticated.get(f"/api/ingest/notifications/{held['event_id']}/source-pockets").status_code == 401
        assert respond(unauthenticated, held, payload).status_code == 401
        unauthenticated.cookies.set("ledger_session", "fictional-invalid-session")
        assert respond(unauthenticated, held, payload).status_code == 403
    finally:
        unauthenticated.close()
        app.dependency_overrides[get_current_user] = overridden
    assert count_transactions(notification_owner) == 0


def test_stale_context_read_cannot_reopen_recorded_event(client, notification_owner):
    from app.db.pool import db_conn
    from app.services import notification_processing as processing

    held = ingest(client, transfer())["results"][0]
    assert respond(client, held, answer(held, notification_owner["emergency"])).status_code == 200
    claimed = processing.claim_event(include_ai=False)
    asyncio.run(processing.process_claim(claimed, None))
    with db_conn() as conn:
        conn.execute("UPDATE accounts SET is_archived=TRUE WHERE id=%s", (notification_owner["emergency"],))
    _, context = processing.read_context(claimed)
    assert context["source_confirmation_pending"]
    assert current(client, held)["status"] == "recorded"
    assert count_transactions(notification_owner) == 1


def test_non_main_confirmation_preserves_same_amount_pairing_ambiguity(client, notification_owner):
    from app.db.pool import db_conn
    from test_notification_processing_database import transactions_of

    for seconds in (26, 28):
        credit = notification("You received IDR 125,000.00 from RA*A ***NAMA at Account Transfer category.",
                              "com.bca.mybca.omni.android", seconds=seconds)
        assert ingest(client, credit)["results"][0]["status"] == "recorded"
    held = ingest(client, transfer(recipient="RAKA PURNAMA"))["results"][0]
    assert count_transactions(notification_owner) == 2
    assert respond(client, held, answer(held, notification_owner["emergency"])).status_code == 200
    result = complete(client, held)
    assert (result["status"], result["type"], result["source"]) == ("recorded", "expense", "Bank Jago · Dana Darurat")
    with db_conn() as conn:
        assert conn.execute("SELECT error_code FROM notification_events WHERE id=%s", (held["event_id"],)).fetchone()["error_code"] == "ambiguous_movement"
    assert count_transactions(notification_owner) == 3
    assert all(row["movement_id"] is None for row in transactions_of(notification_owner))


def test_legacy_queued_event_is_held_before_provider_or_financial_application(client, notification_owner, monkeypatch):
    from app.db.pool import db_conn
    from app.services import notification_processing as processing
    from unittest.mock import AsyncMock

    enable_ai(monkeypatch)
    held = ingest(client, transfer())["results"][0]
    with db_conn() as conn:
        conn.execute("UPDATE notification_events SET processing_state='queued',active_question=NULL WHERE id=%s", (held["event_id"],))
    claimed = processing.claim_event()
    provider = AsyncMock()
    asyncio.run(processing.process_claim(claimed, provider))
    assert provider.interpret.await_count == 0
    assert current(client, held)["confirmation"]["type"] == "source_pocket"
    assert count_transactions(notification_owner) == 0
