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
    assert duplicate["transaction_id"] is None
    assert str(duplicate["duplicate_of_transaction_id"]) == str(rows[1]["id"])
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


# --- skip-redundant-notifications 1.1: a pattern skips the redundant report ---------------------------


def add_pattern(owner, redundant="jago", direction="income", kept="gopay", *, stopped=False, user_id=None):
    """A skip pattern as the owner adds it in Pengaturan (synthetic owner)."""
    from app.db.pool import db_conn
    with db_conn() as conn:
        row = conn.execute(
            """INSERT INTO notification_skip_patterns (user_id, redundant_institution, direction, kept_institution,
                   source, stopped_at)
               VALUES (%s, %s, %s, %s, 'manual', CASE WHEN %s THEN NOW() ELSE NULL END) RETURNING id""",
            (user_id or owner["user_id"], redundant, direction, kept, stopped),
        ).fetchone()
        conn.commit()
    return str(row["id"])


def assert_skipped(event_id, pattern_id):
    stored = event_row(event_id)
    assert stored["provenance_kind"] == "duplicate_observation" and stored["transaction_id"] is None
    assert str(stored["skip_pattern_id"]) == pattern_id
    assert stored["reviewed_at"] is not None


def test_pattern_skips_the_jago_report_without_a_tinjau_item(client, notification_owner):
    owner = notification_owner
    pattern = add_pattern(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    send(client, MYBCA_DEBIT, MYBCA, 3.4)
    assert_one_movement_bca_to_gopay(owner, jago["event_id"])
    assert_skipped(jago["event_id"], pattern)
    queue = client.get("/api/review/queue").json()
    assert queue["seen_twice"] == [] and queue["counts"]["seen_twice"] == 0


def test_pattern_skips_the_jago_report_that_arrived_first(client, notification_owner):
    owner = notification_owner
    pattern = add_pattern(owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    send(client, GOPAY_CREDIT, GOPAY, 1)
    send(client, MYBCA_DEBIT, MYBCA, 3.4)
    assert_one_movement_bca_to_gopay(owner, jago["event_id"])
    assert_skipped(jago["event_id"], pattern)
    assert client.get("/api/review/queue").json()["seen_twice"] == []


def test_pattern_keeps_an_owner_edited_jago_leg_and_asks(client, notification_owner):
    owner = notification_owner
    add_pattern(owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    row = next(row for row in incomes(owner) if str(row["account_id"]) == owner["main"])
    edited = client.patch(f"/api/transactions/{row['id']}", json={"notes": "Uang dari GoPay"})
    assert edited.status_code == 200, edited.text
    gopay = send(client, GOPAY_CREDIT, GOPAY, 1)
    assert [str(item["id"]) for item in incomes(owner)] == [str(row["id"])]
    stored = event_row(gopay["event_id"])
    assert (stored["provenance_kind"], stored["skip_pattern_id"], stored["reviewed_at"]) == (
        "duplicate_observation", None, None)
    assert [item["event_id"] for item in client.get("/api/review/queue").json()["seen_twice"]] == [gopay["event_id"]]


# --- skip-redundant-notifications 1.2: what a pattern never skips --------------------------------------


def assert_not_skipped(event_id):
    stored = event_row(event_id)
    assert stored["skip_pattern_id"] is None and stored["reviewed_at"] is None


def test_without_a_pattern_the_guard_still_holds_the_report(client, notification_owner):
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    assert event_row(jago["event_id"])["provenance_kind"] == "duplicate_observation"
    assert_not_skipped(jago["event_id"])
    assert [item["event_id"] for item in client.get("/api/review/queue").json()["seen_twice"]] == [jago["event_id"]]


def test_jago_income_without_a_gopay_report_records(client, notification_owner):
    owner = notification_owner
    add_pattern(owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    assert event_row(jago["event_id"])["provenance_kind"] == "observed_leg"
    assert_not_skipped(jago["event_id"])
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["main"]]


def test_pattern_skips_nothing_for_another_amount(client, notification_owner):
    owner = notification_owner
    add_pattern(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, "You've received Rp400.000 from GoPay.", JAGO, 0.13)
    assert_not_skipped(jago["event_id"])
    assert sorted(str(row["account_id"]) for row in incomes(owner)) == sorted([owner["gopay"], owner["main"]])


def test_pattern_for_another_direction_skips_nothing(client, notification_owner):
    owner = notification_owner
    add_pattern(owner, direction="expense")
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    # The plain guard still holds it for the owner, but nothing is skipped.
    assert event_row(jago["event_id"])["provenance_kind"] == "duplicate_observation"
    assert_not_skipped(jago["event_id"])


def test_pattern_skips_nothing_when_the_sender_is_not_the_kept_account(client, notification_owner):
    owner = notification_owner
    add_pattern(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, "You've received Rp500.000 from Rina Wulandari.", JAGO, 30)
    assert_not_skipped(jago["event_id"])
    assert sorted(str(row["account_id"]) for row in incomes(owner)) == sorted([owner["gopay"], owner["main"]])


def test_pattern_skips_nothing_with_two_possible_kept_reports(client, notification_owner):
    owner = notification_owner
    add_pattern(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    send(client, GOPAY_CREDIT, GOPAY, 2)
    jago = send(client, JAGO_CREDIT, JAGO, 3)
    stored = event_row(jago["event_id"])
    assert stored["provenance_kind"] == "observed_leg"
    assert "ambiguous_movement" in stored["interpretation"]["review_reasons"]
    assert_not_skipped(jago["event_id"])
    assert len(incomes(owner)) == 3


def test_stopped_pattern_skips_nothing(client, notification_owner):
    owner = notification_owner
    add_pattern(owner, stopped=True)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    assert event_row(jago["event_id"])["provenance_kind"] == "duplicate_observation"
    assert_not_skipped(jago["event_id"])


def test_another_owners_pattern_skips_nothing(client, notification_owner):
    from app.db.pool import db_conn

    other = str(uuid4())
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Lain', 'x', 'TEST')",
            (other, f"other_{uuid4().hex}"),
        )
        conn.commit()
    try:
        add_pattern(notification_owner, user_id=other)
        send(client, GOPAY_CREDIT, GOPAY, 0)
        jago = send(client, JAGO_CREDIT, JAGO, 0.13)
        assert event_row(jago["event_id"])["provenance_kind"] == "duplicate_observation"
        assert_not_skipped(jago["event_id"])
    finally:
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (other,))
            conn.commit()


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


# --- 2.1–2.3 The guard's limits, the deferred pass and the companion result -----------------------------


def incomes(owner):
    return [row for row in legs(owner) if row["type"] == "income"]


def test_real_transfers_that_do_not_name_each_other_both_book(client, notification_owner):
    owner = notification_owner
    send(client, "Anda telah menerima saldo sebesar Rp500.000.", GOPAY, 0)
    send(client, "You've received Rp500.000 from Rina Wulandari.", JAGO, 30)
    assert sorted(str(row["account_id"]) for row in incomes(owner)) == sorted([owner["gopay"], owner["main"]])


def test_same_app_twice_is_not_a_second_observation(client, notification_owner):
    owner = notification_owner
    first = send(client, GOPAY_CREDIT, GOPAY, 0)
    second = send(client, GOPAY_CREDIT, GOPAY, 5)
    assert len(incomes(owner)) == 2
    assert {event_row(item["event_id"])["provenance_kind"] for item in (first, second)} == {"observed_leg"}


def test_mirror_outside_the_window_books_and_is_flagged(client, notification_owner):
    owner = notification_owner
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 121)
    stored = event_row(jago["event_id"])
    assert stored["provenance_kind"] == "observed_leg"
    assert "ambiguous_movement" in stored["interpretation"]["review_reasons"]
    row = next(row for row in incomes(owner) if str(row["account_id"]) == owner["main"])
    assert row["review_status"] == "needs_review"


def test_two_possible_originals_flag_instead_of_merging(client, notification_owner):
    owner = notification_owner
    send(client, GOPAY_CREDIT, GOPAY, 0)
    send(client, GOPAY_CREDIT, GOPAY, 2)
    jago = send(client, JAGO_CREDIT, JAGO, 3)
    stored = event_row(jago["event_id"])
    assert stored["provenance_kind"] == "observed_leg"
    assert "ambiguous_movement" in stored["interpretation"]["review_reasons"]
    assert len(incomes(owner)) == 3


def test_owner_edited_earlier_leg_is_never_removed(client, notification_owner):
    owner = notification_owner
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    row = next(row for row in incomes(owner) if str(row["account_id"]) == owner["main"])
    edited = client.patch(f"/api/transactions/{row['id']}", json={"notes": "Uang dari GoPay"})
    assert edited.status_code == 200, edited.text
    gopay = send(client, GOPAY_CREDIT, GOPAY, 1)
    assert [str(item["id"]) for item in incomes(owner)] == [str(row["id"])]
    assert event_row(jago["event_id"])["provenance_kind"] == "observed_leg"
    assert event_row(gopay["event_id"])["provenance_kind"] == "duplicate_observation"
    assert str(event_row(gopay["event_id"])["duplicate_of_transaction_id"]) == str(row["id"])


def test_companion_result_of_a_duplicate_is_the_kept_leg(client, notification_owner):
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    assert jago["status"] == "recorded" and jago["target"] == "GoPay" and "question" not in jago
    send(client, MYBCA_DEBIT, MYBCA, 3.4)
    result = client.get(f"/api/ingest/notifications/{jago['event_id']}/result").json()["result"]
    assert result["type"] == "internal_movement"
    assert (result["source"], result["target"]) == ("BCA Harian", "GoPay")


def test_deferred_pass_applies_the_rule_once(client, notification_owner, monkeypatch):
    from app.services import notification_application
    from app.services.movement_pairing import ArrivalDecision, repair_unpaired_movements

    owner = notification_owner
    with monkeypatch.context() as patched:
        # Legs recorded before the rule existed.
        patched.setattr(notification_application, "same_arrival", lambda *args, **kwargs: ArrivalDecision())
        send(client, GOPAY_CREDIT, GOPAY, 0)
        jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    assert len(incomes(owner)) == 2
    first = repair_unpaired_movements(now=START + timedelta(minutes=5))
    assert first["duplicates"] == 1
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["gopay"]]
    assert event_row(jago["event_id"])["provenance_kind"] == "duplicate_observation"
    again = repair_unpaired_movements(now=START + timedelta(minutes=6))
    assert again["duplicates"] == 0 and len(incomes(owner)) == 1
    assert balances(owner)[owner["main"]] == 1000000


# --- 3.1–3.2 Transfer answers pair at once; links retire the model's memory ------------------------------


def typed_movement_leg(owner, account_key, tx_type, amount, moment):
    """A one-sided transfer typed by the owner (a capture), which no notification pairing linked."""
    from app.db.pool import db_conn
    category = owner["internal_out" if tx_type == "expense" else "internal_in"]
    tx_id = str(uuid4())
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO transactions (id, user_id, account_id, category_id, type, amount, notes, date, source,
                   review_status, classified_by)
               VALUES (%s, %s, %s, %s, %s, %s, 'Pindah saldo', %s, 'manual', 'reviewed', 'owner')""",
            (tx_id, owner["user_id"], owner[account_key], category, tx_type, amount, moment),
        )
        conn.commit()
    return tx_id


def test_transfer_answer_links_the_only_counterpart_and_undo_unlinks(client, notification_owner):
    owner = notification_owner
    movement_history(owner)
    jago = send(client, JAGO_CREDIT, JAGO, 30, version="1.8.0")
    bca_leg = typed_movement_leg(owner, "bca", "expense", 500000, START)
    choice = movement_suggestion(jago["question"], owner)
    response = answer(client, jago["event_id"], jago["question"], choice["id"])
    assert response.status_code == 200, response.text
    rows = {row["type"]: row for row in legs(owner)}
    assert str(rows["expense"]["id"]) == bca_leg
    assert rows["expense"]["movement_id"] is not None and rows["expense"]["movement_id"] == rows["income"]["movement_id"]
    undone = client.post("/api/review/undo", json={"undo_token": response.json()["undo_token"]})
    assert undone.status_code == 200, undone.text
    assert all(row["movement_id"] is None for row in legs(owner))


def ai_memory(owner, match_key, category_key):
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO merchant_memory (user_id, match_key, key_kind, display_merchant, notes, category_id, source)
               VALUES (%s, %s, 'merchant', 'Tabungan by Jago', 'Tabungan by Jago', %s, %s)""",
            (owner["user_id"], match_key, owner[category_key], "ai" if category_key == "income" else "owner"),
        )
        conn.commit()


def test_linking_a_leg_retires_the_model_memory_only(client, notification_owner):
    owner = notification_owner
    ai_memory(owner, "merchant:tabungan by jago", "income")
    send(client, GOPAY_CREDIT, GOPAY, 0)
    send(client, MYBCA_DEBIT, MYBCA, 3.4)
    assert legs(owner)[0]["movement_id"] is not None
    assert "merchant:tabungan by jago" not in memory_rows(owner)


def test_owner_memory_survives_a_link(client, notification_owner):
    owner = notification_owner
    ai_memory(owner, "merchant:tabungan by jago", "expense")
    send(client, GOPAY_CREDIT, GOPAY, 0)
    send(client, MYBCA_DEBIT, MYBCA, 3.4)
    assert memory_rows(owner)["merchant:tabungan by jago"]["source"] == "owner"


def test_owner_merge_retires_the_model_memory(client, notification_owner, monkeypatch):
    from app.services import notification_application
    from app.services.movement_pairing import PairingDecision

    owner = notification_owner
    ai_memory(owner, "merchant:tabungan by jago", "income")
    with monkeypatch.context() as patched:
        patched.setattr(notification_application, "find_counterpart", lambda *args, **kwargs: PairingDecision())
        send(client, GOPAY_CREDIT, GOPAY, 0)
        send(client, MYBCA_DEBIT, MYBCA, 3.4)
    rows = {row["type"]: row for row in legs(owner)}
    merged = client.post("/api/movements/merge", json={
        "expense_transaction_id": str(rows["expense"]["id"]), "income_transaction_id": str(rows["income"]["id"]),
    })
    assert merged.status_code == 200, merged.text
    assert "merchant:tabungan by jago" not in memory_rows(owner)


# --- 5.2 Tinjau "Terlihat dua kali" ---------------------------------------------------------------------


def seen_twice_queue(client):
    response = client.get("/api/review/queue")
    assert response.status_code == 200, response.text
    return response.json()


def test_seen_twice_lists_the_duplicate_and_counts_as_automatic(client, notification_owner):
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    send(client, MYBCA_DEBIT, MYBCA, 3.4)
    queue = seen_twice_queue(client)
    [item] = queue["seen_twice"]
    assert (item["event_id"], item["kind"], item["app"], item["type"], item["amount"]) == (
        jago["event_id"], "duplicate", "Bank Jago", "income", 500000)
    assert item["account_name"] == "Bank Jago · Kantong Utama"
    assert item["kept"]["account_name"] == "GoPay"
    assert item["kept"]["movement"] == {"source": "BCA Harian", "target": "GoPay"}
    assert queue["counts"]["seen_twice"] == 1
    summary = client.get("/api/review/summary").json()
    assert summary["unreviewed"] == queue["counts"]["unreviewed"] >= 1
    assert summary["attention"] == queue["counts"]["attention"]
    # The movement's legs need nothing, so the duplicate is the only automatic item and no attention.
    assert (summary["attention"], summary["unreviewed"]) == (0, 1)


def test_acknowledging_removes_the_item_and_books_nothing(client, notification_owner):
    owner = notification_owner
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    response = client.post(f"/api/review/seen-twice/{jago['event_id']}/acknowledge")
    assert response.status_code == 200, response.text
    assert seen_twice_queue(client)["seen_twice"] == []
    assert client.post(f"/api/review/seen-twice/{jago['event_id']}/acknowledge").status_code == 200
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["gopay"]]


def test_record_separately_books_its_own_leg_and_undo_restores_the_duplicate(client, notification_owner):
    owner = notification_owner
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    response = client.post(f"/api/review/seen-twice/{jago['event_id']}/record-separately")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["transaction_id"] and body["undo_token"]
    assert sorted(str(row["account_id"]) for row in incomes(owner)) == sorted([owner["gopay"], owner["main"]])
    stored = event_row(jago["event_id"])
    assert (stored["provenance_kind"], str(stored["transaction_id"])) == ("observed_leg", body["transaction_id"])
    assert stored["duplicate_of_transaction_id"] is None
    assert seen_twice_queue(client)["seen_twice"] == []
    undone = client.post("/api/review/undo", json={"undo_token": body["undo_token"]})
    assert undone.status_code == 200, undone.text
    assert undone.json()["removed_transaction_ids"] == [body["transaction_id"]]
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["gopay"]]
    restored = event_row(jago["event_id"])
    assert (restored["provenance_kind"], restored["transaction_id"], restored["reviewed_at"]) == (
        "duplicate_observation", None, None)
    assert str(restored["duplicate_of_transaction_id"]) == str(incomes(owner)[0]["id"])
    assert [item["event_id"] for item in seen_twice_queue(client)["seen_twice"]] == [jago["event_id"]]


def test_another_owner_cannot_act_on_the_item(client, notification_owner):
    from app.main import app
    from app.services.auth import get_current_user

    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    app.dependency_overrides[get_current_user] = lambda: {"id": str(uuid4()), "name": "Lain", "currency": "IDR"}
    for action in ("acknowledge", "record-separately"):
        response = client.post(f"/api/review/seen-twice/{jago['event_id']}/{action}")
        assert response.status_code == 404 and response.json()["detail"]["code"] == "seen_twice_not_found"


def test_held_proposal_is_listed_and_can_be_recorded(client, notification_owner):
    from app.db.pool import db_conn

    owner = notification_owner
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    with db_conn() as conn:
        # As the model left it (OD116): held, nothing booked.
        conn.execute("DELETE FROM transactions WHERE user_id = %s", (owner["user_id"],))
        conn.execute(
            """UPDATE notification_events SET processing_state = 'needs_review', transaction_id = NULL,
                   provenance_kind = NULL, error_code = 'possible_duplicate_observation' WHERE id = %s""",
            (jago["event_id"],),
        )
        conn.commit()
    [item] = seen_twice_queue(client)["seen_twice"]
    assert (item["kind"], item["kept"]) == ("held", None)
    response = client.post(f"/api/review/seen-twice/{jago['event_id']}/record-separately")
    assert response.status_code == 200, response.text
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["main"]]


def test_movement_memory_learned_earlier_is_not_applied(client, notification_owner):
    from app.db.pool import db_conn
    from app.services.merchant_memory import load_memory

    owner = notification_owner
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO merchant_memory (user_id, match_key, key_kind, display_merchant, category_id, source)
               VALUES (%s, 'merchant:gopay', 'merchant', 'GoPay', %s, 'owner'),
                      (%s, 'signature:jago|income|unknown', 'signature', NULL, %s, 'owner')""",
            (owner["user_id"], owner["internal_out"], owner["user_id"], owner["internal_in"]),
        )
        conn.commit()
        assert load_memory(conn.cursor(), owner["user_id"], "signature:jago|income|unknown") == []
    jago = send(client, JAGO_CREDIT, JAGO, 0, version="1.8.0")
    row = next(row for row in incomes(owner) if str(row["account_id"]) == owner["main"])
    assert row["category"] != "Internal Movement" and "question" in jago
