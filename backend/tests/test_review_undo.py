"""Server-side undo of Tinjau review actions (redesign-review-triage).

All names, texts and amounts are synthetic. The owner of `notification_owner` is "Raka Purnama"; the
products are "Reksa Dana A" and "Reksa Dana B" under "Bibit Contoh".
"""
import base64
import hashlib
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient

from test_history_suggestions import add_row, remember, suggestion_rows, two_kedai_suggestions
from test_ingestion_memory_and_purpose import (  # noqa: F401 - fixtures
    KEDAI, add_feedback, answer, enable_ai, event_row, feedback_rows, memory_rows, normalization, open_question, owner, tx_for,
)
from test_movement_pairing import bca_from_owner, gopay_to_own_bca, insert_leg, repair, tx_of_event
from test_notification_processing_database import ingest, notification, notification_owner  # noqa: F401
from test_review_queue import ambiguous_transfer, bulk, kedai
from test_topup_learning import AMOUNT, awaiting, convert, debit_event, product, rdn_debit, reject

TOKEN_LENGTH = 43


def db():
    from app.db.pool import db_conn
    return db_conn()


def tx(tx_id):
    with db() as conn:
        return conn.execute("SELECT * FROM transactions WHERE id = %s", (tx_id,)).fetchone()


def image(tx_id):
    """The columns undo must put back, plus the ones it must never touch."""
    row = tx(tx_id)
    if row is None:
        return None
    return {key: row[key] for key in (
        "type", "amount", "account_id", "date", "category_id", "kakeibo_type", "notes", "merchant",
        "offset_category_id", "movement_id", "movement_role", "fee_for_movement_id", "goal_id", "obligation_id",
        "recurring_rule_id", "idempotency_key", "source", "review_status", "classified_by",
        "classification_confidence", "owner_edited_at", "owner_split_at")}


def event_state(event_id):
    row = event_row(event_id)
    return {key: row[key] for key in ("active_question", "movement_id", "confirmed_role", "result_key",
                                      "result_snapshot")} | {
        "purpose_answer": (row["interpretation"] or {}).get("purpose_answer")}


def undo(client, token):
    return client.post("/api/review/undo", json={"undo_token": token})


def token_of(response):
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["undo_token"] and len(body["undo_token"]) == TOKEN_LENGTH
    from app.services import review_undo
    expires = datetime.fromisoformat(body["expires_at"])
    assert timedelta(minutes=9, seconds=55) < expires - review_undo.now() <= timedelta(minutes=10)
    return body["undo_token"]


def balances(client):
    flat = {}
    for account in client.get("/api/accounts").json()["accounts"]:
        flat[account["id"]] = int(account["balance"])
        for child in account.get("children") or []:
            flat[child["id"]] = int(child["balance"])
    return flat


def records(owner):
    with db() as conn:
        return conn.execute("SELECT * FROM review_actions WHERE user_id = %s ORDER BY created_at", (owner["user_id"],)).fetchall()


def live_labels(owner):
    from app.services.merchant_memory import live_feedback_sql
    with db() as conn:
        return conn.execute(
            f"SELECT * FROM classification_feedback f WHERE f.user_id = %s AND {live_feedback_sql('f')} ORDER BY created_at, id",
            (owner["user_id"],),
        ).fetchall()


def later(monkeypatch, delta):
    from app.services import review_undo
    real = review_undo.now
    monkeypatch.setattr(review_undo, "now", lambda: real() + delta)


@pytest.fixture
def other_owner(notification_owner):
    """A second owner; `as_other` switches the request identity to them."""
    from app.main import app
    from app.services.auth import get_current_user
    user_id = str(uuid4())
    with db() as conn:
        conn.execute("INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Dewi Lain', 'x', 'TEST')",
                     (user_id, f"lain_{uuid4().hex}"))
        conn.commit()
    owner_identity = app.dependency_overrides[get_current_user]

    class Switch:
        id = user_id

        def __enter__(self):
            app.dependency_overrides[get_current_user] = lambda: {"id": user_id, "name": "Dewi Lain", "currency": "IDR"}

        def __exit__(self, *_):
            app.dependency_overrides[get_current_user] = owner_identity

    yield Switch()
    with db() as conn:
        conn.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()


# --- V44 schema ------------------------------------------------------------------------------------


def test_v44_feedback_checks_and_the_append_only_trigger(client, owner):
    first, _ = kedai(client, 10)
    assert bulk(client, "recategorise", [first], category_id=owner["jajan"]).status_code == 200
    [label] = feedback_rows(owner)
    with pytest.raises(psycopg.errors.CheckViolation):
        with db() as conn:
            conn.execute("INSERT INTO classification_feedback (user_id, kind) VALUES (%s, 'undo')", (owner["user_id"],))
    with pytest.raises(psycopg.errors.CheckViolation):
        with db() as conn:
            conn.execute("INSERT INTO classification_feedback (user_id, kind, reverts_feedback_id) VALUES (%s, 'correction', %s)",
                         (owner["user_id"], label["id"]))
    with db() as conn:
        conn.execute("INSERT INTO classification_feedback (user_id, kind, reverts_feedback_id) VALUES (%s, 'undo', %s)",
                     (owner["user_id"], label["id"]))
        conn.commit()
    with pytest.raises(psycopg.errors.UniqueViolation):
        with db() as conn:
            conn.execute("INSERT INTO classification_feedback (user_id, kind, reverts_feedback_id) VALUES (%s, 'undo', %s)",
                         (owner["user_id"], label["id"]))
    with pytest.raises(psycopg.errors.RaiseException):
        with db() as conn:
            conn.execute("UPDATE classification_feedback SET after_notes = 'x' WHERE id = %s", (label["id"],))


def test_v44_review_action_checks(client, owner):
    base = "INSERT INTO review_actions (user_id, token_hash, kind, transaction_ids, snapshot, expires_at, undone_at) VALUES (%s, %s, %s, '{}', %s, %s, %s)"
    soon = datetime.now(timezone.utc) + timedelta(minutes=10)
    for kind, digest, snapshot, undone in [
        ("review_accept", b"x" * 32, '{"a": 1}', datetime.now(timezone.utc)),
        ("review_accept", b"short", None, None),
        ("unknown_kind", b"y" * 32, None, None),
    ]:
        with pytest.raises(psycopg.errors.CheckViolation):
            with db() as conn:
                conn.execute(base, (owner["user_id"], digest, kind, snapshot, soon, undone))


# --- Tokens and the record -------------------------------------------------------------------------


def test_token_is_stored_only_as_its_hash(client, owner):
    first, _ = kedai(client, 10)
    token = token_of(bulk(client, "accept", [first]))
    assert len(base64.urlsafe_b64decode(token + "=")) == 32
    [record] = records(owner)
    assert bytes(record["token_hash"]) == hashlib.sha256(token.encode()).digest()
    assert token not in str(dict(record))
    assert record["kind"] == "review_accept" and [str(value) for value in record["transaction_ids"]] == [first]
    assert record["expires_at"] - record["created_at"] == timedelta(minutes=10)


def test_a_failing_record_write_fails_the_action(client, owner, monkeypatch):
    from app.services import review_undo
    first, _ = kedai(client, 10)
    monkeypatch.setattr(review_undo, "token_hash", lambda token: b"too-short")
    with pytest.raises(psycopg.errors.CheckViolation):
        bulk(client, "recategorise", [first], category_id=owner["jajan"])
    assert tx(first)["review_status"] == "auto" and feedback_rows(owner) == [] and records(owner) == []


def test_no_token_when_nothing_changed(client, owner):
    first, _ = kedai(client, 10)
    token_of(bulk(client, "accept", [first]))
    again = bulk(client, "accept", [first])
    assert again.status_code == 200 and (again.json()["undo_token"], again.json()["expires_at"]) == (None, None)
    assert len(records(owner)) == 1


# --- Endpoint and refusals ---------------------------------------------------------------------------


def test_undo_an_accept_restores_status_and_answers_the_documented_shape(client, owner):
    first, _ = kedai(client, 10)
    second, _ = kedai(client, 20)
    before = {tx_id: image(tx_id) for tx_id in (first, second)}
    token = token_of(bulk(client, "accept", [first, second]))
    response = undo(client, token)
    assert response.status_code == 200, response.text
    body = response.json()
    assert (body["ok"], body["kind"], body["already_undone"], body["removed_transaction_ids"]) == (
        True, "review_accept", False, [])
    assert sorted(body["transaction_ids"]) == sorted([first, second])
    assert {row["id"] for row in body["restored"]} == {first, second}
    assert set(body["restored"][0]) == {"id", "type", "amount", "date", "account_id", "category_id", "kakeibo_type",
                                        "review_status", "movement_id"}
    assert {tx_id: image(tx_id) for tx_id in (first, second)} == before
    [record] = records(owner)
    assert record["undone_at"] is not None and record["snapshot"] is None
    replay = undo(client, token)
    assert replay.status_code == 200 and replay.json()["already_undone"] is True and replay.json()["restored"] == []
    assert {tx_id: image(tx_id) for tx_id in (first, second)} == before


@pytest.mark.parametrize("body", [{}, {"undo_token": "short"}, {"undo_token": "x" * 42 + "!"}, {"undo_token": "x" * 44}])
def test_malformed_undo_bodies_are_rejected(client, owner, body):
    assert client.post("/api/review/undo", json=body).status_code == 422


def test_unknown_expired_and_day_old_tokens(client, owner, monkeypatch):
    unknown = undo(client, base64.urlsafe_b64encode(b"\x01" * 32).decode().rstrip("="))
    assert unknown.status_code == 404 and unknown.json()["detail"]["code"] == "undo_not_found"
    first, _ = kedai(client, 10)
    token = token_of(bulk(client, "accept", [first]))
    later(monkeypatch, timedelta(minutes=10))
    expired = undo(client, token)
    assert expired.status_code == 410 and expired.json()["detail"]["code"] == "undo_expired"
    assert tx(first)["review_status"] == "reviewed"
    [record] = records(owner)
    assert record["snapshot"] is None and record["undone_at"] is None
    later(monkeypatch, timedelta(hours=25))
    assert undo(client, token).status_code == 404
    assert records(owner) == []


def test_another_owners_token_is_not_found_and_changes_nothing(client, owner, other_owner):
    first, _ = kedai(client, 10)
    token = token_of(bulk(client, "recategorise", [first], category_id=owner["jajan"]))
    with other_owner:
        refused = undo(client, token)
        assert refused.status_code == 404 and refused.json()["detail"]["code"] == "undo_not_found"
        # Owner B never reaches A's rows through the covered endpoints either.
        assert bulk(client, "accept", [first]).status_code == 404
    assert str(tx(first)["category_id"]) == owner["jajan"]
    assert undo(client, token).status_code == 200


def test_session_undo_needs_the_app_origin(client, owner):
    from passlib.hash import bcrypt
    from app.main import app
    from app.services.auth import get_current_user
    first, _ = kedai(client, 10)
    token = token_of(bulk(client, "accept", [first]))
    username = f"undo_{uuid4().hex[:10]}"
    with db() as conn:
        conn.execute("UPDATE users SET username = %s, password_hash = %s WHERE id = %s",
                     (username, bcrypt.hash("synthetic-pass-1"), owner["user_id"]))
        conn.commit()
    app.dependency_overrides.pop(get_current_user, None)
    browser = TestClient(app, base_url="https://testserver")
    origin = {"Origin": "https://testserver"}
    assert browser.post("/api/auth/login", json={"username": username, "password": "synthetic-pass-1"}, headers=origin).status_code == 200
    body = {"undo_token": token}
    assert browser.post("/api/review/undo", json=body).status_code == 403
    assert browser.post("/api/review/undo", json=body, headers={"Origin": "https://evil.example"}).status_code == 403
    assert tx(first)["review_status"] == "reviewed"
    assert browser.post("/api/review/undo", json=body, headers=origin).status_code == 200
    assert tx(first)["review_status"] == "auto"


def test_two_parallel_undos_restore_once(client, owner):
    from app.services.review_undo import undo_action
    first, _ = kedai(client, 10)
    token = token_of(bulk(client, "recategorise", [first], category_id=owner["jajan"]))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: undo_action(owner["user_id"], token), range(2)))
    assert sorted(result["already_undone"] for result in results) == [False, True]
    assert [row["kind"] for row in feedback_rows(owner)] == ["correction", "undo"]


# --- Bulk actions ----------------------------------------------------------------------------------


def test_recategorise_undo_restores_rows_memory_feedback_and_the_question(client, owner):
    first, first_event = kedai(client, 10)
    second, _ = kedai(client, 20)
    question_event, _ = open_question(client, owner)
    question_tx = str(tx_for(question_event)["id"])
    ids = [first, second, question_tx]
    before = {tx_id: image(tx_id) for tx_id in ids}
    events_before = event_state(question_event)
    memory_before = memory_rows(owner)
    token = token_of(bulk(client, "recategorise", [first, second], category_id=owner["jajan"], kakeibo_type="want"))
    question_token = token_of(bulk(client, "recategorise", [question_tx], category_id=owner["makan"]))
    assert event_row(question_event)["active_question"] is None
    assert memory_rows(owner)["merchant:kedai awan"]["source"] == "owner"

    assert undo(client, question_token).status_code == 200
    assert undo(client, token).status_code == 200
    assert {tx_id: image(tx_id) for tx_id in ids} == before
    assert event_state(question_event) == events_before
    assert event_state(question_event)["active_question"]["question_id"] == events_before["active_question"]["question_id"]
    assert memory_rows(owner).keys() == memory_before.keys()
    for key, entry in memory_before.items():
        assert {k: v for k, v in memory_rows(owner)[key].items()} == dict(entry)
    kinds = [row["kind"] for row in feedback_rows(owner)]
    assert kinds.count("correction") == 3 and kinds.count("undo") == 3 and live_labels(owner) == []
    queue = client.get("/api/review/queue").json()
    assert [item["event_id"] for item in queue["questions"]] == [question_event]
    assert first_event


def test_rename_undo_and_newer_memory_is_kept(client, owner):
    first, _ = kedai(client, 10)
    before = image(first)
    token = token_of(bulk(client, "rename", [first], merchant="Kedai Awan Baru", notes="Ngopi"))
    assert memory_rows(owner)["merchant:kedai awan baru"]["notes"] == "Ngopi"
    # A later owner write to the same key is the owner's newer knowledge and stays.
    remember(owner, "merchant:kedai awan baru", category="makan", notes="Makan siang", merchant="Kedai Awan Baru")
    assert undo(client, token).status_code == 200
    assert image(first) == before
    assert memory_rows(owner)["merchant:kedai awan baru"]["notes"] == "Makan siang"


def test_keep_separate_undo_lets_the_repair_pass_pair_again(client, notification_owner):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    leg = str(tx_of_event(outgoing["event_id"])["id"])
    before = image(leg)
    token = token_of(bulk(client, "keep_separate", [leg]))
    assert tx(leg)["owner_split_at"] is not None
    assert undo(client, token).status_code == 200
    assert image(leg) == before and tx(leg)["owner_split_at"] is None
    ingest(client, bca_from_owner(500000, 3 * 3600))
    assert repair()["paired"] == 1 and tx(leg)["movement_id"] is not None
    assert owner


def test_an_edit_in_transaksi_meanwhile_refuses_the_undo(client, owner):
    first, _ = kedai(client, 10)
    token = token_of(bulk(client, "recategorise", [first], category_id=owner["jajan"]))
    edited = client.patch(f"/api/transactions/{first}", json={"notes": "Diubah di Transaksi"})
    assert edited.status_code == 200, edited.text
    edited_image = image(first)
    refused = undo(client, token)
    assert refused.status_code == 409
    assert refused.json()["detail"] == {"code": "undo_conflict", "transaction_ids": [first]}
    assert image(first) == edited_image
    assert [row["kind"] for row in feedback_rows(owner)].count("undo") == 0


def test_two_tabs_the_later_change_wins(client, owner):
    first, _ = kedai(client, 10)
    tab_a = token_of(bulk(client, "recategorise", [first], category_id=owner["jajan"]))
    after_a = image(first)
    tab_b = token_of(bulk(client, "rename", [first], merchant="Kedai Awan Dua"))
    assert undo(client, tab_a).status_code == 409
    assert undo(client, tab_b).status_code == 200
    assert image(first) == after_a


def test_a_notification_processed_meanwhile_refuses_the_undo(client, notification_owner):
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    leg = str(tx_of_event(outgoing["event_id"])["id"])
    token = token_of(bulk(client, "accept", [leg]))
    ingest(client, bca_from_owner(500000, 3 * 3600))
    assert repair()["paired"] == 1
    paired = image(leg)
    refused = undo(client, token)
    assert refused.status_code == 409 and leg in refused.json()["detail"]["transaction_ids"]
    assert image(leg) == paired


def test_debt_and_goal_rows_recategorised_then_undone(client, owner):
    debt = client.post("/api/obligations", json={"name": "Cicilan Contoh", "total_amount": 1_000_000,
                                                 "remaining_amount": 600_000, "minimum_payment": 100_000})
    debt_id = debt.json()["obligation"]["id"]
    with db() as conn:
        goal_id = conn.execute("SELECT id FROM goals WHERE user_id = %s", (owner["user_id"],)).fetchone()["id"]
    paid = add_row(owner, notes="Bayar cicilan", category="belanja", amount=100_000, merchant="Toko Cicilan")
    saved = add_row(owner, notes="Tabung", category="belanja", amount=50_000, merchant="Toko Cicilan", goal_id=goal_id)
    assert client.post(f"/api/obligations/{debt_id}/payments", json={"transaction_id": paid}).status_code == 200

    def progress():
        with db() as conn:
            return (
                dict(conn.execute("SELECT remaining_amount, is_archived FROM obligations WHERE id = %s", (debt_id,)).fetchone()),
                [dict(row) for row in conn.execute("SELECT obligation_id, amount FROM transaction_obligation_allocations WHERE transaction_id = %s", (paid,)).fetchall()],
                [dict(row) for row in conn.execute("SELECT id, current_amount FROM goals WHERE user_id = %s ORDER BY id", (owner["user_id"],)).fetchall()],
            )

    start = progress()
    before = {tx_id: image(tx_id) for tx_id in (paid, saved)}
    token = token_of(bulk(client, "recategorise", [paid, saved], category_id=owner["jajan"]))
    assert progress() == start
    assert undo(client, token).status_code == 200
    assert progress() == start
    assert {tx_id: image(tx_id) for tx_id in (paid, saved)} == before


# --- Movements -------------------------------------------------------------------------------------


def test_completion_with_a_new_leg_is_undone_and_can_be_completed_again(client, notification_owner):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    leg = str(tx_of_event(outgoing["event_id"])["id"])
    before, event_before, money_before = image(leg), event_state(outgoing["event_id"]), balances(client)
    body = {"transaction_id": leg, "target_account_id": owner["bca"]}
    response = client.post("/api/movements/complete", json=body)
    token = token_of(response)
    created = response.json()["created_transaction_id"]
    money_after = balances(client)
    assert money_after[owner["bca"]] == money_before[owner["bca"]] + 500000
    replay = client.post("/api/movements/complete", json=body)
    assert replay.status_code == 200 and replay.json()["idempotent"] is True and replay.json()["undo_token"] is None

    result = undo(client, token)
    assert result.status_code == 200, result.text
    assert result.json()["removed_transaction_ids"] == [created] and tx(created) is None
    assert image(leg) == before and event_state(outgoing["event_id"]) == event_before
    assert balances(client) == money_before
    assert leg in awaiting(client)
    print("balances", {"before": money_before[owner["bca"]], "after": money_after[owner["bca"]],
                       "undone": balances(client)[owner["bca"]]})
    again = client.post("/api/movements/complete", json=body)
    assert again.status_code == 200 and again.json()["created_transaction_id"] not in (None, created)
    assert tx(leg)["movement_id"] is not None


def test_completion_that_linked_an_existing_row_is_undone(client, notification_owner):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    leg = str(tx_of_event(outgoing["event_id"])["id"])
    existing = insert_leg(owner, "bca", "income", 500000, category_key="internal_in", days_from_row=1, row_date=tx(leg)["date"])
    before = {tx_id: image(tx_id) for tx_id in (leg, existing)}
    money = balances(client)
    response = client.post("/api/movements/complete", json={"transaction_id": leg, "target_account_id": owner["bca"]})
    token = token_of(response)
    assert response.json()["linked_transaction_id"] == existing
    result = undo(client, token)
    assert result.status_code == 200 and result.json()["removed_transaction_ids"] == []
    assert {tx_id: image(tx_id) for tx_id in (leg, existing)} == before and balances(client) == money


def test_counterpart_candidates_refusal_stores_nothing(client, notification_owner):
    owner = notification_owner
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    leg = str(tx_of_event(outgoing["event_id"])["id"])
    insert_leg(owner, "bca", "income", 500000, category_key="income", days_from_row=1, row_date=tx(leg)["date"])
    response = client.post("/api/movements/complete", json={"transaction_id": leg, "target_account_id": owner["bca"]})
    assert response.status_code == 409 and response.json()["detail"]["code"] == "counterpart_candidates"
    assert records(owner) == []


def test_merge_undo_restores_both_legs_without_a_split_mark(client, owner):
    incoming = ambiguous_transfer(client)
    queue = client.get("/api/review/queue").json()
    [credit] = [item for item in queue["ambiguous"] if item["id"] == incoming]
    send = credit["candidates"][0]["id"]
    before = {tx_id: image(tx_id) for tx_id in (incoming, send)}
    money = balances(client)
    token = token_of(client.post("/api/movements/merge", json={"expense_transaction_id": send, "income_transaction_id": incoming}))
    assert tx(send)["movement_id"] is not None
    result = undo(client, token)
    assert result.status_code == 200, result.text
    assert {tx_id: image(tx_id) for tx_id in (incoming, send)} == before
    assert tx(send)["owner_split_at"] is None and tx(incoming)["owner_split_at"] is None
    assert balances(client) == money
    assert live_labels(owner) == []


def test_a_repairing_between_merge_and_undo_is_a_conflict(client, owner):
    incoming = ambiguous_transfer(client)
    queue = client.get("/api/review/queue").json()
    [credit] = [item for item in queue["ambiguous"] if item["id"] == incoming]
    send = credit["candidates"][0]["id"]
    response = client.post("/api/movements/merge", json={"expense_transaction_id": send, "income_transaction_id": incoming})
    token = token_of(response)
    split = client.post(f"/api/movements/{response.json()['movement_id']}/split")
    assert split.status_code == 200, split.text
    assert undo(client, token).status_code == 409


# --- Top-ups ---------------------------------------------------------------------------------------


@pytest.fixture
def funds(client, notification_owner):
    parent = client.post("/api/accounts", json={"name": "Bibit Contoh", "type": "investment", "initial_balance": 0})
    parent_id = parent.json()["account"]["id"]
    return {"a": product(client, "Reksa Dana A", parent_id), "b": product(client, "Reksa Dana B", parent_id),
            "parent": parent_id, **notification_owner}


def product_state(account_id):
    with db() as conn:
        return dict(conn.execute(
            """SELECT investment_tracking_mode, investment_cost_basis, last_price, investment_value_estimated
               FROM accounts WHERE id = %s""", (account_id,)).fetchone())


def topup_answers(owner):
    with db() as conn:
        return conn.execute("SELECT * FROM investment_topup_feedback WHERE user_id = %s ORDER BY created_at, id",
                            (owner["user_id"],)).fetchall()


def test_conversion_undo_deletes_the_topup_and_restores_debit_and_product(client, funds):
    when = datetime(2026, 10, 1, 3, tzinfo=timezone.utc)
    event = debit_event(when)
    result = ingest(client, event)["results"][0]
    debit = str(tx_for(result["event_id"])["id"])
    before, product_before, money = image(debit), product_state(funds["a"]), balances(client)
    response = convert(client, debit, funds["a"])
    token = token_of(response)
    topup = response.json()["topup"]
    inbound = topup["income_transaction_id"]
    assert product_state(funds["a"])["last_price"] == AMOUNT
    replay = convert(client, debit, funds["a"])
    assert replay.status_code == 200 and replay.json()["undo_token"] is None

    undone = undo(client, token)
    assert undone.status_code == 200, undone.text
    assert undone.json()["removed_transaction_ids"] == [inbound] and tx(inbound) is None
    with db() as conn:
        assert conn.execute("SELECT is_deleted FROM investment_topups WHERE id = %s", (topup["id"],)).fetchone()["is_deleted"]
    assert image(debit) == before
    assert product_state(funds["a"]) == product_before
    assert balances(client)[funds["funding"]] == money[funds["funding"]]
    assert debit in awaiting(client)
    # The original notification sent again records nothing: the debit kept its idempotency key.
    retry = ingest(client, {**event, "payload_hash": event["payload_hash"]})["results"][0]
    assert retry["status"] in {"duplicate", "recorded"}
    with db() as conn:
        count = conn.execute("SELECT COUNT(*) AS n FROM transactions WHERE user_id = %s AND amount = %s AND type = 'expense'",
                             (funds["user_id"], AMOUNT)).fetchone()["n"]
    assert count == 1
    reconverted = convert(client, debit, funds["b"])
    assert reconverted.status_code == 200 and tx(debit)["movement_id"] is not None


def test_conversion_undo_after_a_newer_valuation_keeps_the_delete_arithmetic(client, funds):
    debit = rdn_debit(client, datetime(2026, 10, 1, 3, tzinfo=timezone.utc))
    token = token_of(convert(client, debit, funds["a"]))
    with db() as conn:
        conn.execute("UPDATE accounts SET last_price = 200000, investment_cost_basis = 150000, updated_at = NOW() WHERE id = %s",
                     (funds["a"],))
        conn.commit()
    assert undo(client, token).status_code == 200
    state = product_state(funds["a"])
    assert (state["last_price"], state["investment_cost_basis"]) == (200000 - AMOUNT, 150000 - AMOUNT)


def test_undone_suggested_conversion_makes_the_earlier_rejection_latest_again(client, funds):
    from app.db.pool import db_conn
    from app.services.topup_learning import load_labels
    debit = rdn_debit(client, datetime(2026, 10, 1, 3, tzinfo=timezone.utc))
    assert reject(client, debit, funds["a"]).json()["recorded"] is True
    token = token_of(convert(client, debit, funds["a"], suggested=funds["a"]))
    assert [row["outcome"] for row in topup_answers(funds)] == ["rejected", "accepted"]
    assert undo(client, token).status_code == 200
    assert [row["outcome"] for row in topup_answers(funds)] == ["rejected"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            labels = load_labels(cur, funds["user_id"], account_id=funds["funding"], amount=AMOUNT, match_key=None)
    assert [(label.transaction_id, label.target_id) for label in labels] == [(debit, None)]


def test_bukan_top_up_undo_removes_only_its_answer(client, funds):
    earlier = rdn_debit(client, datetime(2026, 9, 1, 3, tzinfo=timezone.utc))
    assert convert(client, earlier, funds["a"]).status_code == 200
    debit = rdn_debit(client, datetime(2026, 10, 1, 3, tzinfo=timezone.utc))
    suggestion = awaiting(client)[debit]["topup_suggestion"]
    assert suggestion is not None
    token = token_of(reject(client, debit, funds["a"]))
    assert [row["outcome"] for row in topup_answers(funds)] == ["rejected"]
    repeat = reject(client, debit, funds["a"])
    assert repeat.json()["recorded"] is False and repeat.json()["undo_token"] is None
    assert undo(client, token).status_code == 200
    assert topup_answers(funds) == []
    assert awaiting(client)[debit]["topup_suggestion"] == suggestion


def test_a_refused_conversion_stores_no_record(client, funds):
    with db() as conn:
        plain = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, goal_id)
               VALUES (%s, %s, %s, 'expense', 10000, NOW(), (SELECT id FROM goals WHERE user_id = %s)) RETURNING id""",
            (funds["user_id"], funds["funding"], funds["expense"], funds["user_id"])).fetchone()["id"]
        conn.commit()
    response = convert(client, str(plain), funds["a"])
    assert response.status_code == 409 and response.json()["detail"]["code"] == "transaction_not_convertible"
    assert records(funds) == []


# --- Purpose answers -------------------------------------------------------------------------------


def question_with_history(owner):
    """An earlier synthetic label, so the next Groceries question offers a suggestion."""
    add_feedback(owner, notes="makan kafe senja", category="jajan", amount=200000, weekday=3, hour=19)


def test_purpose_answer_undo_reopens_the_question_and_refuses_a_replay(client, owner):
    question_with_history(owner)
    event_id, question = open_question(client, owner)
    row_id = str(tx_for(event_id)["id"])
    before, event_before, memory_before = image(row_id), event_state(event_id), memory_rows(owner)
    body = {"question_id": question["question_id"], "reply_id": str(uuid4()), "suggestion_id": question["suggestions"][0]["id"]}
    response = answer(client, event_id, body)
    token = token_of(response)
    # Today's response keys stay; only the two undo fields are added.
    assert set(response.json()) == {"ok", "result", "answer"} | {"undo_token", "expires_at"}
    replay = answer(client, event_id, body)
    assert replay.status_code == 200 and replay.json()["undo_token"] is None

    assert undo(client, token).status_code == 200
    assert image(row_id) == before
    assert event_state(event_id) == event_before
    assert event_row(event_id)["active_question"]["question_id"] == question["question_id"]
    assert memory_rows(owner).keys() == memory_before.keys()
    assert [item["event_id"] for item in client.get("/api/review/queue").json()["questions"]] == [event_id]
    late = answer(client, event_id, body)
    assert late.status_code == 409 and late.json()["detail"]["code"] == "purpose_answer_undone"
    assert image(row_id) == before
    fresh = answer(client, event_id, {**body, "reply_id": str(uuid4())})
    assert fresh.status_code == 200 and fresh.json()["undo_token"]


def test_text_answer_that_stays_needs_review_is_undoable(client, owner):
    event_id, question = open_question(client, owner)
    row_id = str(tx_for(event_id)["id"])
    before = image(row_id)
    response = answer(client, event_id, {"question_id": question["question_id"], "reply_id": str(uuid4()),
                                         "text": "belanja dapur mingguan"})
    token = token_of(response)
    assert tx(row_id)["review_status"] == "needs_review" and tx(row_id)["notes"] == "belanja dapur mingguan"
    assert undo(client, token).status_code == 200
    assert image(row_id) == before


def test_text_answer_with_a_mocked_normaliser_is_undone(client, owner, monkeypatch):
    from app.main import app
    provider = AsyncMock()
    provider.normalize_purpose.return_value = normalization(
        merchant="Arena Futsal", description="Sewa lapangan Arena Futsal", category_id=owner["olahraga"],
        kakeibo="culture", offset_category_id=None, confidence=0.9)
    event_id, question = open_question(client, owner)
    enable_ai(monkeypatch)
    monkeypatch.setattr(app.state, "notification_provider", provider)
    row_id = str(tx_for(event_id)["id"])
    before, memory_before = image(row_id), memory_rows(owner)
    token = token_of(answer(client, event_id, {"question_id": question["question_id"], "reply_id": str(uuid4()),
                                               "text": "sewa lapangan arena futsal"}))
    assert "merchant:arena futsal" in memory_rows(owner)
    assert undo(client, token).status_code == 200
    assert image(row_id) == before and memory_rows(owner).keys() == memory_before.keys()
    assert provider.normalize_purpose.await_count == 1


# --- History suggestions ---------------------------------------------------------------------------


def test_suggestion_accept_undo_returns_rows_and_suggestions(client, owner):
    tx_ids, suggestion_ids = two_kedai_suggestions(client, owner)
    before = {tx_id: image(tx_id) for tx_id in tx_ids}
    token = token_of(client.post("/api/review/suggestions/accept", json={"suggestion_ids": suggestion_ids}))
    assert undo(client, token).status_code == 200
    assert {tx_id: image(tx_id) for tx_id in tx_ids} == before
    assert {row["status"] for row in suggestion_rows(owner)} == {"pending"}
    assert live_labels(owner) == []
    assert len(client.get("/api/review/suggestions").json()["groups"]) == 1


def test_suggestion_reject_undo_and_a_decision_in_between(client, owner):
    tx_ids, suggestion_ids = two_kedai_suggestions(client, owner)
    before = {tx_id: image(tx_id) for tx_id in tx_ids}
    token = token_of(client.post("/api/review/suggestions/reject", json={"suggestion_ids": suggestion_ids[:1]}))
    assert undo(client, token).status_code == 200
    assert {row["status"] for row in suggestion_rows(owner)} == {"pending"}
    assert {tx_id: image(tx_id) for tx_id in tx_ids} == before
    again = token_of(client.post("/api/review/suggestions/reject", json={"suggestion_ids": suggestion_ids[:1]}))
    with db() as conn:
        conn.execute("UPDATE transaction_suggestions SET status = 'superseded' WHERE id = %s", (suggestion_ids[0],))
        conn.commit()
    assert undo(client, again).status_code == 200
    statuses = {str(row["id"]): row["status"] for row in suggestion_rows(owner)}
    assert statuses[suggestion_ids[0]] == "superseded"
    skipped = client.post("/api/review/suggestions/accept", json={"suggestion_ids": suggestion_ids[:1]})
    assert skipped.status_code == 200 and skipped.json()["skipped"] == suggestion_ids[:1]
    assert skipped.json()["undo_token"] is None


# --- Learning ---------------------------------------------------------------------------------------


def test_undone_correction_restores_the_earlier_label_and_redo_counts_once(client, owner):
    from app.db.pool import db_conn
    from app.services.habits import load_labels
    from app.services.purpose_questions import rank_suggestions
    question_with_history(owner)
    event_id, question = open_question(client, owner)
    row_id = str(tx_for(event_id)["id"])
    assert answer(client, event_id, {"question_id": question["question_id"], "reply_id": str(uuid4()),
                                     "suggestion_id": question["suggestions"][0]["id"]}).status_code == 200

    def evidence():
        stored = event_row(event_id)["interpretation"]
        with db_conn() as conn:
            with conn.cursor() as cur:
                labels = load_labels(cur, owner["user_id"], institution="bca", signature=stored["text_signature"],
                                     account_id=owner["bca"], amount=int(tx(row_id)["amount"]))
                ranked = rank_suggestions(cur, owner["user_id"], direction="expense", signature=stored["text_signature"],
                                          account_id=owner["bca"], amount=int(tx(row_id)["amount"]),
                                          moment=tx(row_id)["date"], current=None, repayment_note=None)
        own = [(label.transaction_id, label.kind, label.category_id) for label in labels if label.transaction_id == row_id]
        return own, [item["category_id"] for item in ranked]

    answered = evidence()
    assert answered[0] == [(row_id, "purpose_answer", owner["jajan"])]
    token = token_of(bulk(client, "recategorise", [row_id], category_id=owner["makan"]))
    corrected = evidence()
    assert corrected[0] == [(row_id, "correction", owner["makan"])]
    assert undo(client, token).status_code == 200
    assert evidence() == answered
    token_of(bulk(client, "recategorise", [row_id], category_id=owner["makan"]))
    assert evidence()[0] == corrected[0]
    assert len([row for row in live_labels(owner) if row["kind"] == "correction" and str(row["transaction_id"]) == row_id]) == 1


def test_undone_third_answer_drops_habit_support(client, owner, monkeypatch):
    from test_habit_learning import answer_by_text, pay
    first = pay(client, 12_000, 0)
    answer_by_text(client, monkeypatch, first, category=owner["makan"], kakeibo="need", notes="makan siang")
    tokens = []
    for day, amount in ((1, 11_500), (2, 12_500)):
        result = pay(client, amount, day)
        guess = result["question"]["suggestions"][0]
        tokens.append(token_of(answer(client, result["event_id"], {
            "question_id": result["question"]["question_id"], "reply_id": str(uuid4()), "suggestion_id": guess["id"]})))
    assert undo(client, tokens[-1]).status_code == 200
    nxt = pay(client, 12_000, 3, hours=2)
    assert "question" in nxt
    assert nxt["question"]["suggestions"][0]["basis"] == {"agreeing": 2, "support": 2}


def test_question_rate_ignores_undone_answers_and_corrections(client, owner):
    from app.db.pool import db_conn
    from app.services.question_rate import question_rate
    question_with_history(owner)
    event_id, question = open_question(client, owner)
    row_id = str(tx_for(event_id)["id"])
    answered = token_of(answer(client, event_id, {"question_id": question["question_id"], "reply_id": str(uuid4()),
                                                  "suggestion_id": question["suggestions"][0]["id"]}))
    corrected = token_of(bulk(client, "recategorise", [row_id], category_id=owner["makan"]))
    assert undo(client, corrected).status_code == 200
    assert undo(client, answered).status_code == 200
    moment = tx(row_id)["date"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            rate = question_rate(cur, owner["user_id"], weeks=26, today=datetime.now(timezone.utc).date(), tz="UTC")
            week = next(item for item in rate["weeks"] if item["recorded"])
    assert (week["asked"], week["answered"]) == (1, 0)
    assert sum(item["corrections"] for item in rate["weeks"]) == 0
    assert moment


def test_repayment_preference_ignores_undone_labels(client, owner):
    from app.db.pool import db_conn
    from app.services.history_suggestions import preferred_repayment_offset
    eligible = {owner["makan"], owner["jajan"]}
    with db_conn() as conn:
        for offset in (owner["makan"], owner["jajan"], owner["jajan"]):
            conn.execute(
                """INSERT INTO classification_feedback (user_id, kind, transaction_id, after_type, after_category_id,
                       after_offset_category_id) VALUES (%s, 'correction', %s, 'income', %s, %s)""",
                (owner["user_id"], str(uuid4()), owner["patungan"], offset))
        conn.commit()
        with conn.cursor() as cur:
            assert preferred_repayment_offset(cur, owner["user_id"], eligible) == owner["jajan"]
        for row in conn.execute("""SELECT id FROM classification_feedback WHERE user_id = %s
                                   AND after_offset_category_id = %s""", (owner["user_id"], owner["jajan"])).fetchall():
            conn.execute("INSERT INTO classification_feedback (user_id, kind, reverts_feedback_id) VALUES (%s, 'undo', %s)",
                         (owner["user_id"], row["id"]))
        conn.commit()
        with conn.cursor() as cur:
            assert preferred_repayment_offset(cur, owner["user_id"], eligible) == owner["makan"]


# --- Retention and logs ------------------------------------------------------------------------------


def test_sweep_clears_expired_snapshots_and_deletes_day_old_records(client, owner, monkeypatch):
    from app.services import review_undo
    first, _ = kedai(client, 10)
    token = token_of(bulk(client, "rename", [first], merchant="Kedai Awan Baru", notes="Ngopi"))
    later(monkeypatch, timedelta(minutes=11))
    assert review_undo.sweep_expired()["cleared"] >= 1
    [record] = records(owner)
    assert record["snapshot"] is None
    assert "Kedai" not in str(dict(record)) and "125000" not in str(dict(record))
    assert undo(client, token).status_code == 410
    later(monkeypatch, timedelta(hours=24))
    assert review_undo.sweep_expired()["deleted"] >= 1
    assert records(owner) == [] and undo(client, token).status_code == 404


def test_lazy_clear_without_background_jobs(client, owner, monkeypatch):
    first, _ = kedai(client, 10)
    second, _ = kedai(client, 20)
    token_of(bulk(client, "rename", [first], notes="Ngopi"))
    later(monkeypatch, timedelta(minutes=11))
    token_of(bulk(client, "rename", [second], notes="Ngopi juga"))
    old, new = records(owner)
    assert old["snapshot"] is None and new["snapshot"] is not None


def test_undo_logs_carry_no_content(client, owner, caplog):
    caplog.set_level(logging.DEBUG)
    first, _ = kedai(client, 10)
    token = token_of(bulk(client, "rename", [first], merchant="Kedai Awan Baru", notes="Catatan rahasia"))
    assert undo(client, token).status_code == 200
    assert undo(client, token).status_code == 200
    text = caplog.text
    for secret in ("Kedai Awan Baru", "Catatan rahasia", "125000", "125.000", token):
        assert secret not in text
    assert "Review undo kind=review_rename outcome=restored" in text


def test_undo_statement_count_does_not_grow_with_rows(client, owner, monkeypatch):
    from app.services import review_undo

    def count(ids):
        token = token_of(bulk(client, "recategorise", ids, category_id=owner["jajan"]))
        statements = []
        original = psycopg.Cursor.execute

        def counting(self, query, *args, **kwargs):
            statements.append(str(query)[:40])
            return original(self, query, *args, **kwargs)

        monkeypatch.setattr(psycopg.Cursor, "execute", counting)
        try:
            assert review_undo.undo_action(owner["user_id"], token)["already_undone"] is False
        finally:
            monkeypatch.setattr(psycopg.Cursor, "execute", original)
        return len(statements)

    one = count([kedai(client, 1)[0]])
    many = [kedai(client, second)[0] for second in range(2, 52)]
    fifty = count(many)
    # Every row shares one merchant and one signature key, so the counts are equal.
    assert fifty == one, (one, fifty)


def test_another_owner_reaches_no_covered_endpoint(client, funds, other_owner):
    debit = rdn_debit(client, datetime(2026, 10, 1, 3, tzinfo=timezone.utc))
    outgoing = ingest(client, gopay_to_own_bca(500000))["results"][0]
    leg = str(tx_of_event(outgoing["event_id"])["id"])
    before = {tx_id: image(tx_id) for tx_id in (debit, leg)}
    with other_owner:
        assert client.post("/api/movements/complete", json={"transaction_id": leg, "target_account_id": funds["bca"]}).status_code == 404
        assert client.post("/api/movements/merge", json={"expense_transaction_id": leg, "income_transaction_id": debit}).status_code == 404
        assert convert(client, debit, funds["a"]).status_code == 404
        assert reject(client, debit).status_code == 404
        assert client.get("/api/review/queue").json()["awaiting"] == []
    assert {tx_id: image(tx_id) for tx_id in (debit, leg)} == before
    assert records(funds) == []
