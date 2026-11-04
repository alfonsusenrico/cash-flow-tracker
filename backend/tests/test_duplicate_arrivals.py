"""One arrival reported by two apps books once (recognise-one-arrival-seen-twice).

All texts, names and amounts are synthetic. The incident's shape: a BCA account "BCA Harian", a Jago
parent "Bank Jago" whose default pocket is "Kantong Utama", and an e-wallet "GoPay". One top-up is
reported by GoPay ("menerima Tabungan by Jago"), by Jago ("received from GoPay") and by myBCA.
"""
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from test_notification_processing_database import (  # noqa: F401 - fixture
    ingest, notification, notification_owner,
)

GOPAY = "com.gojek.gopay"
JAGO = "com.jago.digitalBanking"
MYBCA = "com.bca.mybca.omni.android"

GOPAY_CREDIT = "Anda telah menerima Tabungan by Jago sebanyak Rp500.000."
JAGO_CREDIT = "You've received Rp500.000 from GoPay. Need help? Contact Tanya Jago at 1500 746."
MYBCA_DEBIT = "You spent IDR 500,000.00 at Payment."
START = datetime(2026, 10, 31, 11, 39, 18, tzinfo=timezone.utc)


def event(text, package, seconds, *, version=None):
    payload = notification(text, package)
    payload["post_time"] = (START + timedelta(seconds=seconds)).isoformat()
    if version:
        payload["source_version"] = version
    return payload


def send(client, text, package, seconds, **kwargs):
    return ingest(client, event(text, package, seconds, **kwargs))["results"][0]


def balances(owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        rows = conn.execute(
            """SELECT a.id, a.initial_balance
                      + COALESCE(SUM(CASE WHEN t.type = 'income' THEN t.amount ELSE 0 END), 0)
                      - COALESCE(SUM(CASE WHEN t.type = 'expense' THEN t.amount ELSE 0 END), 0) AS balance
               FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id
               WHERE a.user_id = %s GROUP BY a.id""",
            (owner["user_id"],),
        ).fetchall()
    return {str(row["id"]): int(row["balance"]) for row in rows}


def legs(owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute(
            """SELECT t.*, c.name AS category FROM transactions t LEFT JOIN categories c ON c.id = t.category_id
               WHERE t.user_id = %s ORDER BY t.type, t.date""",
            (owner["user_id"],),
        ).fetchall()


def event_row(event_id):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute("SELECT * FROM notification_events WHERE id = %s", (event_id,)).fetchone()


def memory_rows(owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return {row["match_key"]: row for row in conn.execute(
            "SELECT * FROM merchant_memory WHERE user_id = %s", (owner["user_id"],),
        ).fetchall()}


def assert_one_movement_bca_to_gopay(owner, jago_event_id):
    rows = legs(owner)
    assert [(str(row["account_id"]), row["type"]) for row in rows] == [
        (owner["bca"], "expense"), (owner["gopay"], "income"),
    ]
    assert rows[0]["movement_id"] is not None and rows[0]["movement_id"] == rows[1]["movement_id"]
    duplicate = event_row(jago_event_id)
    assert duplicate["processing_state"] == "recorded"
    assert duplicate["provenance_kind"] == "duplicate_observation"
    assert str(duplicate["transaction_id"]) == str(rows[1]["id"])
    money = balances(owner)
    assert (money[owner["bca"]], money[owner["gopay"]], money[owner["main"]]) == (500000, 1500000, 1000000)


# --- 1.1 The incident sequence ------------------------------------------------------------------------


def test_gopay_then_jago_then_mybca_books_one_movement(client, notification_owner):
    owner = notification_owner
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    send(client, MYBCA_DEBIT, MYBCA, 3.4)
    assert_one_movement_bca_to_gopay(owner, jago["event_id"])


def test_jago_first_keeps_the_named_gopay_leg(client, notification_owner):
    owner = notification_owner
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    send(client, GOPAY_CREDIT, GOPAY, 1)
    send(client, MYBCA_DEBIT, MYBCA, 3.4)
    assert_one_movement_bca_to_gopay(owner, jago["event_id"])


# --- 1.2 Transfer answers and transfer memory ----------------------------------------------------------


def movement_history(owner, times=2):
    """Earlier owner answers that filed a Jago merchant-less credit as Internal Movement."""
    from app.db.pool import db_conn
    with db_conn() as conn:
        for _ in range(times):
            conn.execute(
                """INSERT INTO classification_feedback (user_id, kind, transaction_id, after_notes, after_category_id,
                       after_type, local_weekday, local_hour, amount, account_id, institution, text_signature)
                   VALUES (%s, 'correction', %s, 'Pindah uang dari BCA', %s, 'income', 6, 18, 500000, %s, 'jago',
                           'jago|income|unknown')""",
                (owner["user_id"], str(uuid4()), owner["internal_in"], owner["main"]),
            )
        conn.commit()


def movement_suggestion(question, owner):
    return next(item for item in question["suggestions"] if item["category_id"] == owner["internal_in"])


def answer(client, event_id, question, suggestion_id):
    return client.post(f"/api/ingest/notifications/{event_id}/answer-purpose", json={
        "question_id": question["question_id"], "reply_id": str(uuid4()), "suggestion_id": suggestion_id,
    })


def test_transfer_answer_without_counterpart_keeps_awaiting(client, notification_owner):
    owner = notification_owner
    movement_history(owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0, version="1.8.0")
    choice = movement_suggestion(jago["question"], owner)
    response = answer(client, jago["event_id"], jago["question"], choice["id"])
    assert response.status_code == 200, response.text
    row = next(row for row in legs(owner) if str(row["account_id"]) == owner["main"])
    assert (row["category"], row["classified_by"], row["review_status"], row["movement_id"]) == (
        "Internal Movement", "owner", "needs_review", None)
    stored = event_row(jago["event_id"])
    assert stored["active_question"] is None
    assert "awaiting_counterpart" in stored["interpretation"]["review_reasons"]
    queue = client.get("/api/review/queue").json()
    assert str(row["id"]) in {item["id"] for item in queue["awaiting"]}


def test_transfer_answer_teaches_no_memory(client, notification_owner):
    owner = notification_owner
    movement_history(owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0, version="1.8.0")
    choice = movement_suggestion(jago["question"], owner)
    assert answer(client, jago["event_id"], jago["question"], choice["id"]).status_code == 200
    assert "signature:jago|income|unknown" not in memory_rows(owner)
    from app.db.pool import db_conn
    with db_conn() as conn:
        kinds = [row["kind"] for row in conn.execute(
            "SELECT kind FROM classification_feedback WHERE user_id = %s AND event_id = %s",
            (owner["user_id"], jago["event_id"]),
        ).fetchall()]
    assert kinds == ["purpose_answer"]
