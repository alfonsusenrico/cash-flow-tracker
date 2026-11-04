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


# --- reconcile-linked-account-reports 1.1: a linked account skips its second report ------------------


def add_link(owner, account="gopay", institution="jago", *, stopped=False, user_id=None):
    """An account link as the owner's answer in Tinjau creates it (synthetic owner)."""
    from app.db.pool import db_conn
    with db_conn() as conn:
        row = conn.execute(
            """INSERT INTO account_report_links (user_id, account_id, institution, source, stopped_at)
               VALUES (%s, %s, %s, 'confirmation', CASE WHEN %s THEN NOW() ELSE NULL END) RETURNING id""",
            (user_id or owner["user_id"], owner[account], institution, stopped),
        ).fetchone()
        conn.commit()
    return str(row["id"])


def assert_skipped(event_id, link_id):
    stored = event_row(event_id)
    assert stored["provenance_kind"] == "duplicate_observation" and stored["transaction_id"] is None
    assert str(stored["reporting_link_id"]) == link_id
    assert stored["reviewed_at"] is not None


def assert_not_skipped(event_id):
    stored = event_row(event_id)
    assert stored["reporting_link_id"] is None and stored["reviewed_at"] is None


def test_linked_account_skips_the_jago_report_without_a_tinjau_item(client, notification_owner):
    owner = notification_owner
    link = add_link(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    send(client, MYBCA_DEBIT, MYBCA, 3.4)
    assert_one_movement_bca_to_gopay(owner, jago["event_id"])
    assert_skipped(jago["event_id"], link)
    queue = client.get("/api/review/queue").json()
    assert queue["seen_twice"] == [] and queue["counts"]["seen_twice"] == 0


def test_linked_account_skips_the_jago_report_that_arrived_first(client, notification_owner):
    owner = notification_owner
    link = add_link(owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    send(client, GOPAY_CREDIT, GOPAY, 1)
    send(client, MYBCA_DEBIT, MYBCA, 3.4)
    assert_one_movement_bca_to_gopay(owner, jago["event_id"])
    assert_skipped(jago["event_id"], link)
    assert client.get("/api/review/queue").json()["seen_twice"] == []


def test_link_keeps_an_owner_edited_jago_leg_and_asks(client, notification_owner):
    owner = notification_owner
    add_link(owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    row = next(row for row in incomes(owner) if str(row["account_id"]) == owner["main"])
    edited = client.patch(f"/api/transactions/{row['id']}", json={"notes": "Uang dari GoPay"})
    assert edited.status_code == 200, edited.text
    gopay = send(client, GOPAY_CREDIT, GOPAY, 1)
    assert [str(item["id"]) for item in incomes(owner)] == [str(row["id"])]
    stored = event_row(gopay["event_id"])
    assert (stored["provenance_kind"], stored["reporting_link_id"], stored["reviewed_at"]) == (
        "duplicate_observation", None, None)
    assert event_row(jago["event_id"])["provenance_kind"] == "observed_leg"
    assert [item["event_id"] for item in client.get("/api/review/queue").json()["seen_twice"]] == [gopay["event_id"]]


def test_late_report_within_five_minutes_is_skipped(client, notification_owner):
    owner = notification_owner
    link = add_link(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 240)
    assert_skipped(jago["event_id"], link)
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["gopay"]]
    assert balances(owner)[owner["main"]] == 1000000


def test_report_beyond_the_window_is_a_doubt(client, notification_owner):
    owner = notification_owner
    add_link(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 360)
    stored = event_row(jago["event_id"])
    assert stored["provenance_kind"] == "observed_leg"
    assert "ambiguous_movement" in stored["interpretation"]["review_reasons"]
    assert_not_skipped(jago["event_id"])
    assert sorted(str(row["account_id"]) for row in incomes(owner)) == sorted([owner["gopay"], owner["main"]])


def test_without_a_link_the_guard_still_holds_the_report(client, notification_owner):
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    assert event_row(jago["event_id"])["provenance_kind"] == "duplicate_observation"
    assert_not_skipped(jago["event_id"])
    assert [item["event_id"] for item in client.get("/api/review/queue").json()["seen_twice"]] == [jago["event_id"]]


def test_jago_income_without_a_gopay_report_records(client, notification_owner):
    owner = notification_owner
    add_link(owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    assert event_row(jago["event_id"])["provenance_kind"] == "observed_leg"
    assert_not_skipped(jago["event_id"])
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["main"]]


def test_link_skips_nothing_for_another_amount(client, notification_owner):
    owner = notification_owner
    add_link(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, "You've received Rp400.000 from GoPay.", JAGO, 0.13)
    assert_not_skipped(jago["event_id"])
    assert sorted(str(row["account_id"]) for row in incomes(owner)) == sorted([owner["gopay"], owner["main"]])


def test_link_skips_nothing_when_the_sender_is_not_the_linked_account(client, notification_owner):
    owner = notification_owner
    add_link(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, "You've received Rp500.000 from Rina Wulandari.", JAGO, 30)
    assert_not_skipped(jago["event_id"])
    assert sorted(str(row["account_id"]) for row in incomes(owner)) == sorted([owner["gopay"], owner["main"]])


def test_possible_transfer_under_a_link_is_not_skipped(client, notification_owner):
    owner = notification_owner
    add_link(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    send(client, "Rp500.000 udah dikirim ke Bank Jago Raka Purnama.", GOPAY, 60)
    jago = send(client, JAGO_CREDIT, JAGO, 61)
    assert_not_skipped(jago["event_id"])
    assert event_row(jago["event_id"])["provenance_kind"] != "duplicate_observation"
    assert owner["main"] in [str(row["account_id"]) for row in incomes(owner)]


def test_two_top_ups_of_the_same_amount_each_absorb_one_report(client, notification_owner):
    owner = notification_owner
    link = add_link(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    first = send(client, JAGO_CREDIT, JAGO, 0.2)
    send(client, GOPAY_CREDIT, GOPAY, 120)
    second = send(client, JAGO_CREDIT, JAGO, 120.2)
    assert_skipped(first["event_id"], link)
    assert_skipped(second["event_id"], link)
    gopay_legs = [row for row in incomes(owner) if str(row["account_id"]) == owner["gopay"]]
    assert len(gopay_legs) == 2 and len(incomes(owner)) == 2
    kept = {str(event_row(item["event_id"])["duplicate_of_transaction_id"]) for item in (first, second)}
    assert kept == {str(row["id"]) for row in gopay_legs}


def test_stopped_link_skips_nothing(client, notification_owner):
    owner = notification_owner
    add_link(owner, stopped=True)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    assert event_row(jago["event_id"])["provenance_kind"] == "duplicate_observation"
    assert_not_skipped(jago["event_id"])


def test_another_owners_link_skips_nothing(client, notification_owner):
    from app.db.pool import db_conn

    other = str(uuid4())
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Lain', 'x', 'TEST')",
            (other, f"other_{uuid4().hex}"),
        )
        conn.commit()
    try:
        # A link stored under another owner never decides for this owner, even on this owner's account.
        add_link(notification_owner, user_id=other, account="gopay")
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


def link_count(owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute("SELECT COUNT(*) AS count FROM account_report_links WHERE user_id = %s",
                            (owner["user_id"],)).fetchone()["count"]


def test_acknowledging_learns_the_account_link_once(client, notification_owner):
    owner = notification_owner
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    first = client.post(f"/api/review/seen-twice/{jago['event_id']}/acknowledge").json()
    link = first["link"]
    assert (link["account_id"], link["institution"], link["source"], link["created"]) == (
        owner["gopay"], "jago", "confirmation", True)
    assert (link["account_label"], link["institution_label"]) == ("GoPay", "Bank Jago")
    again = client.post(f"/api/review/seen-twice/{jago['event_id']}/acknowledge").json()
    assert (again["link"]["id"], again["link"]["created"]) == (link["id"], False)
    assert link_count(owner) == 1
    # The next repeat is skipped without a Tinjau item.
    send(client, GOPAY_CREDIT, GOPAY, 600)
    later = send(client, JAGO_CREDIT, JAGO, 600.2)
    assert_skipped(later["event_id"], link["id"])
    assert client.get("/api/review/queue").json()["seen_twice"] == []


def test_acknowledging_links_the_named_account_when_the_kept_leg_was_the_fallback(client, notification_owner):
    owner = notification_owner
    send(client, JAGO_CREDIT, JAGO, 0)
    row = next(row for row in incomes(owner) if str(row["account_id"]) == owner["main"])
    assert client.patch(f"/api/transactions/{row['id']}", json={"notes": "Uang dari GoPay"}).status_code == 200
    gopay = send(client, GOPAY_CREDIT, GOPAY, 1)
    link = client.post(f"/api/review/seen-twice/{gopay['event_id']}/acknowledge").json()["link"]
    assert (link["account_id"], link["institution"]) == (owner["gopay"], "jago")


def test_acknowledging_a_duplicate_of_a_typed_row_learns_nothing(client, notification_owner):
    from app.db.pool import db_conn

    owner = notification_owner
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    with db_conn() as conn:
        # As if the kept leg had been typed: no notification is linked to it.
        conn.execute("UPDATE notification_events SET transaction_id = NULL WHERE user_id = %s AND package_name = %s",
                     (owner["user_id"], GOPAY))
        conn.commit()
    body = client.post(f"/api/review/seen-twice/{jago['event_id']}/acknowledge").json()
    assert body["link"] is None and link_count(owner) == 0


def test_skipped_reports_are_listed_and_can_be_recorded_separately(client, notification_owner):
    owner = notification_owner
    link = add_link(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    send(client, MYBCA_DEBIT, MYBCA, 3.4)
    queue = client.get("/api/review/queue").json()
    [item] = queue["skipped"]
    assert (item["event_id"], item["kind"], item["app"], item["amount"]) == (jago["event_id"], "skipped", "Bank Jago", 500000)
    assert item["kept"]["movement"] == {"source": "BCA Harian", "target": "GoPay"}
    assert item["link"] == {"id": link, "account_label": "GoPay", "institution_label": "Bank Jago"}
    assert queue["counts"]["skipped"] == 1 and queue["counts"]["seen_twice"] == 0
    # Skipped reports are not work: the movement's legs need nothing, so nothing is counted.
    summary = client.get("/api/review/summary").json()
    assert (summary["attention"], summary["unreviewed"]) == (0, 0) == (queue["counts"]["attention"], queue["counts"]["unreviewed"])
    response = client.post(f"/api/review/seen-twice/{jago['event_id']}/record-separately")
    assert response.status_code == 200, response.text
    body = response.json()
    assert sorted(str(row["account_id"]) for row in incomes(owner)) == sorted([owner["gopay"], owner["main"]])
    stored = event_row(jago["event_id"])
    assert (stored["provenance_kind"], stored["reporting_link_id"]) == ("observed_leg", None)
    assert client.get("/api/review/queue").json()["skipped"] == []
    undone = client.post("/api/review/undo", json={"undo_token": body["undo_token"]})
    assert undone.status_code == 200, undone.text
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["gopay"]]
    assert_skipped(jago["event_id"], link)
    assert [entry["event_id"] for entry in client.get("/api/review/queue").json()["skipped"]] == [jago["event_id"]]


def test_another_owner_cannot_record_a_skipped_report(client, notification_owner):
    from app.main import app
    from app.services.auth import get_current_user

    add_link(notification_owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    jago = send(client, JAGO_CREDIT, JAGO, 0.13)
    app.dependency_overrides[get_current_user] = lambda: {"id": str(uuid4()), "name": "Lain", "currency": "IDR"}
    response = client.post(f"/api/review/seen-twice/{jago['event_id']}/record-separately")
    assert response.status_code == 404 and response.json()["detail"]["code"] == "seen_twice_not_found"
    assert client.get("/api/review/queue").json()["skipped"] == []


def test_deferred_pass_applies_a_link_once(client, notification_owner, monkeypatch):
    from app.services import notification_application
    from app.services.movement_pairing import ArrivalDecision, repair_unpaired_movements

    owner = notification_owner
    link = add_link(owner)
    with monkeypatch.context() as patched:
        # Legs recorded before the link could apply.
        patched.setattr(notification_application, "same_arrival", lambda *args, **kwargs: ArrivalDecision())
        jago = send(client, JAGO_CREDIT, JAGO, 0)
        send(client, GOPAY_CREDIT, GOPAY, 1)
    assert len(incomes(owner)) == 2
    first = repair_unpaired_movements(now=START + timedelta(minutes=5))
    assert first["duplicates"] == 1
    assert [str(row["account_id"]) for row in incomes(owner)] == [owner["gopay"]]
    assert_skipped(jago["event_id"], link)
    again = repair_unpaired_movements(now=START + timedelta(minutes=6))
    assert again["duplicates"] == 0 and len(incomes(owner)) == 1
    assert balances(owner)[owner["main"]] == 1000000


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
