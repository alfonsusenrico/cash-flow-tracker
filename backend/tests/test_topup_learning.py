"""Tinjauan top-up settling and learned investment top-ups (clarify-review-and-learn-topups).

All names, amounts and texts are synthetic: "RDN BCA" is the fixture's funding account, the products are
"Reksa Dana A" and "Reksa Dana B" under "Bibit Contoh".
"""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from test_ingestion_memory_and_purpose import event_row, tx_for
from test_notification_processing_database import ingest, notification_owner  # noqa: F401 - fixture

AMOUNT = 112_590
RDN_PACKAGE = "com.bca.mybca.omni.android"
RDN_TEXT = "RDN spending of IDR 112,590.00 at Account Transfer category."


def debit_event(when: datetime, text: str = RDN_TEXT, hash_value: str | None = None) -> dict:
    return {
        "device_id": "synthetic-device", "package_name": RDN_PACKAGE, "body_text": text,
        "payload_hash": hash_value or uuid4().hex, "post_time": when.isoformat(),
    }


def at(month: int, day: int = 1, second: int = 0) -> datetime:
    return datetime(2026, month, day, 3, 0, second, tzinfo=timezone.utc)


def rdn_debit(client, when: datetime, **kwargs) -> str:
    result = ingest(client, debit_event(when, **kwargs))["results"][0]
    assert result["status"] == "recorded", result
    return str(tx_for(result["event_id"])["id"])


def product(client, name: str, parent: str | None = None, **extra) -> str:
    payload = {"name": name, "type": "investment", "initial_balance": 0, "instrument_type": "mutual_fund", **extra}
    if parent:
        payload["parent_id"] = parent
    response = client.post("/api/accounts", json=payload)
    assert response.status_code == 200, response.text
    return response.json()["account"]["id"]


@pytest.fixture
def funds(client, notification_owner):
    parent = client.post("/api/accounts", json={"name": "Bibit Contoh", "type": "investment", "initial_balance": 0})
    assert parent.status_code == 200, parent.text
    parent_id = parent.json()["account"]["id"]
    return {"a": product(client, "Reksa Dana A", parent_id), "b": product(client, "Reksa Dana B", parent_id),
            "parent": parent_id, **notification_owner}


def convert(client, transaction_id: str, target: str, suggested: str | None = None):
    body = {"transaction_id": transaction_id, "target_account_id": target}
    if suggested:
        body["suggested_target_account_id"] = suggested
    return client.post("/api/investment-topups/from-transaction", json=body)


def converted(client, when: datetime, target: str, **kwargs) -> str:
    transaction_id = rdn_debit(client, when)
    response = convert(client, transaction_id, target, **kwargs)
    assert response.status_code == 200, response.text
    return transaction_id


def queue(client) -> dict:
    response = client.get("/api/review/queue")
    assert response.status_code == 200, response.text
    return response.json()


def awaiting(client) -> dict[str, dict]:
    return {item["id"]: item for item in queue(client)["awaiting"]}


def balances(client) -> dict[str, dict]:
    return {item["id"]: item for item in client.get("/api/accounts").json()["accounts"]}


def answers(user_id: str) -> list[dict]:
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute(
            "SELECT * FROM investment_topup_feedback WHERE user_id = %s ORDER BY created_at, id", (user_id,),
        ).fetchall()


def row(transaction_id: str) -> dict:
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute("SELECT * FROM transactions WHERE id = %s", (transaction_id,)).fetchone()


def reject(client, transaction_id: str, suggested: str | None = None):
    body = {"transaction_id": transaction_id}
    if suggested:
        body["suggested_target_account_id"] = suggested
    return client.post("/api/review/topup-suggestions/reject", json=body)


def switch(monkeypatch, enabled: bool) -> None:
    from app.core import config
    monkeypatch.setattr(config, "settings", replace(config.settings, topup_auto_record_enabled=enabled))


# --- Pure decision rules ----------------------------------------------------------------------


def label(n: int, target: str | None, *, weight: int = 1, suggested: str | None = None):
    from app.services.topup_learning import TopupLabel
    moment = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(days=n)
    return TopupLabel(transaction_id=f"t{n}", target_id=target, suggested_id=suggested, weight=weight,
                      labelled_at=moment, date=moment)


def test_three_confirmations_qualify_and_two_only_suggest():
    from app.services.topup_learning import product_stats
    two = product_stats([label(1, "A"), label(2, "A")])["A"]
    three = product_stats([label(1, "A"), label(2, "A"), label(3, "A")])["A"]
    assert two.learned and not two.qualifies
    assert three.learned and three.qualifies and three.basis()["agreeing"] == 3


def test_a_correction_weighs_twice_against_the_product_left_behind():
    from app.services.topup_learning import product_stats
    stats = product_stats([label(1, "A"), label(2, "A"), label(3, "A"), label(4, "B", weight=2, suggested="A")])
    assert (stats["A"].agree, stats["A"].against, stats["A"].qualifies) == (3, 2, False)
    assert (stats["B"].agree, stats["B"].against) == (2, 0)


def test_a_rejection_outweighs_one_confirmation_and_counts_against_every_product():
    from app.services.topup_learning import product_stats
    stats = product_stats([label(1, "A"), label(2, "B"), label(3, None, weight=2)])
    assert not stats["A"].learned and not stats["B"].learned


def test_nine_of_ten_weight_passes_exactly_at_the_threshold():
    from app.services.topup_learning import ProductStats
    assert ProductStats("A", support=9, agree=9, against=1).qualifies
    assert not ProductStats("A", support=8, agree=8, against=1).qualifies
    assert not ProductStats("A", support=3, agree=3, against=1).qualifies


def test_only_the_latest_ten_labels_count():
    from app.services.topup_learning import latest_labels, product_stats
    labels = [label(n, None, weight=2) for n in range(5)] + [label(n, "A") for n in range(5, 15)]
    stats = product_stats(latest_labels(labels))
    assert (stats["A"].support, stats["A"].against, stats["A"].qualifies) == (10, 0, True)


def test_one_to_one_assignment_and_its_limits():
    from app.services.topup_learning import assign
    assert assign("d1", ["d1", "d2"], ["A", "B"], set()).target_id == "A"
    assert assign("d2", ["d1", "d2"], ["A", "B"], set()).target_id == "B"
    lone = assign("d1", ["d1"], ["A", "B"], set())
    assert (lone.target_id, lone.choices, lone.rule) == (None, ("A", "B"), "ask")
    assert assign("d2", ["d2"], ["A", "B"], {"A"}).target_id == "B"
    assert assign("d3", ["d1", "d2", "d3"], ["A", "B"], set()).target_id is None
    assert assign("d1", ["d1", "d2"], ["A"], set()).target_id == "A"
    assert assign("d1", ["d1"], [], set()) is None


# --- Tinjauan conversion and suggestions ------------------------------------------------------


def test_tinjau_conversion_settles_an_awaiting_debit(client, funds):
    debit = rdn_debit(client, at(10))
    assert debit in awaiting(client)
    assert awaiting(client)[debit]["topup_suggestion"] is None
    attention = client.get("/api/review/summary").json()["attention"]
    before = balances(client)

    response = convert(client, debit, funds["a"])

    assert response.status_code == 200, response.text
    after = balances(client)
    assert after[funds["funding"]]["balance"] == before[funds["funding"]]["balance"]
    assert after[funds["a"]]["balance"] == before[funds["a"]]["balance"] + AMOUNT
    assert debit not in awaiting(client)
    assert client.get("/api/review/summary").json()["attention"] == attention - 1
    # A conversion without a suggestion stores no answer; the top-up itself is the evidence.
    assert answers(funds["user_id"]) == []
    assert row(debit)["idempotency_key"].startswith("notification:")


def test_existing_conversion_is_the_first_confirmation(client, funds):
    # A conversion made the way October's was, before any suggestion existed: it is the backfill.
    converted(client, at(9), funds["a"])
    debit = rdn_debit(client, at(10))

    suggestion = awaiting(client)[debit]["topup_suggestion"]

    assert suggestion["target_account_id"] == funds["a"]
    assert suggestion["target_label"] == "Bibit Contoh · Reksa Dana A"
    assert suggestion["prompt"] == "Sepertinya top up ke Reksa Dana A, kayak bulan lalu?"
    assert suggestion["basis"]["agreeing"] == 1 and suggestion["basis"]["support"] == 1
    assert [choice["id"] for choice in suggestion["choices"]] == [funds["a"]]
    assert row(debit)["review_status"] == "needs_review" and row(debit)["movement_id"] is None


def test_older_confirmation_is_worded_as_before(client, funds):
    converted(client, at(7), funds["a"])
    debit = rdn_debit(client, at(10))
    assert awaiting(client)[debit]["topup_suggestion"]["prompt"] == "Sepertinya top up ke Reksa Dana A, kayak sebelumnya?"


def test_accept_and_correct_record_one_answer_each(client, funds):
    converted(client, at(8), funds["a"])
    first = rdn_debit(client, at(9))
    second = rdn_debit(client, at(10))

    assert convert(client, first, funds["a"], suggested=funds["a"]).status_code == 200
    assert convert(client, first, funds["a"], suggested=funds["a"]).json()["idempotent"] is True
    assert convert(client, second, funds["b"], suggested=funds["a"]).status_code == 200
    assert convert(client, second, funds["b"], suggested=funds["a"]).status_code == 200

    stored = [(str(item["transaction_id"]), item["outcome"]) for item in answers(funds["user_id"])]
    assert stored == [(first, "accepted"), (second, "corrected")]


def test_suggested_product_of_another_owner_is_refused(client, funds):
    from app.db.pool import db_conn
    with db_conn() as conn:
        other = conn.execute(
            "INSERT INTO users (username, password_hash, invite_code, language) VALUES (%s, 'unused', 'fixture', 'id') RETURNING id",
            (f"other_{uuid4().hex}",),
        ).fetchone()["id"]
        foreign = conn.execute(
            """INSERT INTO accounts (user_id, name, type, instrument_type) VALUES (%s, 'Dana Lain', 'investment',
               'mutual_fund') RETURNING id""", (other,),
        ).fetchone()["id"]
        conn.commit()
    debit = rdn_debit(client, at(10))
    before = balances(client)
    response = convert(client, debit, funds["a"], suggested=str(foreign))
    assert response.status_code == 404
    assert balances(client) == before and answers(funds["user_id"]) == []
    assert reject(client, debit, str(foreign)).status_code == 404


def test_rejection_is_remembered_and_weighs_twice(client, funds):
    converted(client, at(8), funds["a"])
    debit = rdn_debit(client, at(9))
    assert awaiting(client)[debit]["topup_suggestion"]["target_account_id"] == funds["a"]

    first = reject(client, debit, funds["a"])
    again = reject(client, debit, funds["a"])

    assert first.status_code == 200 and first.json()["recorded"] is True
    assert again.status_code == 200 and again.json()["recorded"] is False
    assert [item["outcome"] for item in answers(funds["user_id"])] == ["rejected"]
    # The debit still waits for its counterpart, without a suggestion.
    assert awaiting(client)[debit]["topup_suggestion"] is None
    # One confirmation (1) against one rejection (2): the pattern is no longer suggested.
    later = rdn_debit(client, at(10))
    assert awaiting(client)[later]["topup_suggestion"] is None


def test_rejecting_a_foreign_or_linked_transaction_changes_nothing(client, funds):
    linked = converted(client, at(9), funds["a"])
    assert reject(client, linked).status_code == 409
    assert reject(client, str(uuid4())).status_code == 404
    assert answers(funds["user_id"]) == []


def test_undone_conversion_is_no_longer_evidence(client, funds):
    converted(client, at(9), funds["a"])
    topup = client.get("/api/transactions?limit=50").json()["transactions"]
    topup_id = next(item["investment_topup_id"] for item in topup if item.get("investment_topup_id"))
    assert client.delete(f"/api/investment-topups/{topup_id}").status_code == 200
    debit = rdn_debit(client, at(10))
    assert awaiting(client)[debit]["topup_suggestion"] is None
    assert answers(funds["user_id"]) == []


def test_archived_product_is_never_suggested(client, funds):
    converted(client, at(9), funds["a"])
    assert client.patch(f"/api/accounts/{funds['a']}", json={"is_archived": True}).status_code == 200
    debit = rdn_debit(client, at(10))
    assert awaiting(client)[debit]["topup_suggestion"] is None


def test_different_notification_text_is_another_pattern(client, funds):
    from app.db.pool import db_conn
    first = converted(client, at(9), funds["a"])
    debit = rdn_debit(client, at(10))
    with db_conn() as conn:
        for transaction_id, key in ((first, "signature:bca|rdn|transfer"), (debit, "signature:bca|rdn|payment")):
            conn.execute(
                """UPDATE notification_events SET interpretation = interpretation || jsonb_build_object('match_key', %s::text)
                   WHERE transaction_id = %s""", (key, transaction_id),
            )
        conn.commit()
    assert awaiting(client)[debit]["topup_suggestion"] is None


def test_other_users_conversions_are_never_evidence(client, funds):
    from app.db.pool import db_conn
    from app.services.investment_topups import convert_transaction_to_topup
    from app.services.topup_learning import load_labels

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (username, password_hash, invite_code, language) VALUES (%s, 'unused', 'fixture', 'id') RETURNING id",
                        (f"other_{uuid4().hex}",))
            other = str(cur.fetchone()["id"])
            cur.execute("INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'RDN Lain', 'bank', 500000) RETURNING id", (other,))
            rdn = str(cur.fetchone()["id"])
            cur.execute("""INSERT INTO accounts (user_id, name, type, instrument_type) VALUES (%s, 'Dana Lain', 'investment',
                           'mutual_fund') RETURNING id""", (other,))
            fund = str(cur.fetchone()["id"])
            cur.execute("""INSERT INTO transactions (user_id, account_id, type, amount, date, source, review_status)
                           VALUES (%s, %s, 'expense', %s, %s, 'manual', 'reviewed') RETURNING id""", (other, rdn, AMOUNT, at(9)))
            spent = str(cur.fetchone()["id"])
            convert_transaction_to_topup(cur, user_id=other, transaction_id=spent, target_id=fund)
            # Even asked with the other owner's account id, the owner's pattern reads nothing of theirs.
            assert load_labels(cur, funds["user_id"], account_id=rdn, amount=AMOUNT, match_key=None) == []
        conn.commit()
    debit = rdn_debit(client, at(10))
    assert awaiting(client)[debit]["topup_suggestion"] is None


# --- One-to-one -------------------------------------------------------------------------------


def learn_pair(client, funds, month: int) -> None:
    """Two equal debits in the same second, converted after both were recorded, as the owner did."""
    first, second = rdn_debit(client, at(month)), rdn_debit(client, at(month))
    assert convert(client, first, funds["a"]).status_code == 200
    assert convert(client, second, funds["b"]).status_code == 200


def test_same_amount_pair_is_assigned_one_per_product(client, funds, monkeypatch):
    learn_pair(client, funds, 9)
    first = rdn_debit(client, at(10, second=5))
    lone = awaiting(client)[first]["topup_suggestion"]
    # A lone debit cannot know which of the two products it is for: the owner chooses.
    assert lone["target_account_id"] is None and {choice["id"] for choice in lone["choices"]} == {funds["a"], funds["b"]}
    second = rdn_debit(client, at(10, second=5))

    items = awaiting(client)
    ordered = sorted([first, second], key=lambda item: item)
    targets = [items[item]["topup_suggestion"]["target_account_id"] for item in ordered]
    assert targets == [funds["a"], funds["b"]]
    assert "satu debit untuk satu produk" in items[ordered[0]]["topup_suggestion"]["explanation"]


def test_a_filled_partner_leaves_the_other_product(client, funds):
    learn_pair(client, funds, 9)
    first = rdn_debit(client, at(10, second=5))
    second = rdn_debit(client, at(10, second=5))
    assert convert(client, first, funds["a"]).status_code == 200
    assert awaiting(client)[second]["topup_suggestion"]["target_account_id"] == funds["b"]


# --- Automatic recording ----------------------------------------------------------------------


def test_third_confirmation_records_the_next_debit_automatically(client, funds):
    for month in (7, 8, 9):
        converted(client, at(month), funds["a"])
    before = balances(client)
    event = debit_event(at(10))
    result = ingest(client, event)["results"][0]

    debit = tx_for(result["event_id"])
    assert debit["classified_by"] == "habit" and debit["review_status"] == "reviewed"
    assert debit["movement_id"] is not None and debit["amount"] == AMOUNT
    after = balances(client)
    assert after[funds["funding"]]["balance"] == before[funds["funding"]]["balance"] - AMOUNT
    assert after[funds["a"]]["balance"] == before[funds["a"]]["balance"] + AMOUNT
    assert after[funds["a"]]["cost_basis"] == before[funds["a"]]["cost_basis"] + AMOUNT
    assert str(debit["id"]) not in awaiting(client)
    assert client.get("/api/review/summary").json()["attention"] == 0
    habit = event_row(result["event_id"])["interpretation"]["habit"]
    assert (habit["kind"], habit["target_account_id"], habit["agreeing"], habit["support"]) == (
        "investment_topup", funds["a"], 3, 3)
    # The learner writes no owner answer, correction or memory of its own.
    assert answers(funds["user_id"]) == []
    from app.db.pool import db_conn
    with db_conn() as conn:
        feedback = conn.execute("SELECT COUNT(*) AS n FROM classification_feedback WHERE transaction_id = %s",
                                (debit["id"],)).fetchone()["n"]
    assert feedback == 0

    # Replaying the notification records nothing more and keeps the debit's idempotency key.
    count = len(client.get("/api/transactions?limit=100").json()["transactions"])
    replay = ingest(client, event)["results"][0]
    assert replay["event_id"] == result["event_id"]
    assert len(client.get("/api/transactions?limit=100").json()["transactions"]) == count
    assert balances(client)[funds["a"]]["balance"] == after[funds["a"]]["balance"]
    assert row(str(debit["id"]))["idempotency_key"] == f"notification:{result['event_id']}"
    # An automatic top-up is not evidence: the basis stays three confirmations.
    later = ingest(client, debit_event(at(11)))["results"][0]
    assert event_row(later["event_id"])["interpretation"]["habit"]["agreeing"] == 3


def test_switch_off_only_suggests(client, funds, monkeypatch):
    for month in (7, 8, 9):
        converted(client, at(month), funds["a"])
    switch(monkeypatch, False)
    debit = rdn_debit(client, at(10))
    assert row(debit)["movement_id"] is None and row(debit)["review_status"] == "needs_review"
    assert awaiting(client)[debit]["topup_suggestion"]["target_account_id"] == funds["a"]


def test_below_the_threshold_only_suggests(client, funds):
    for month in (8, 9):
        converted(client, at(month), funds["a"])
    debit = rdn_debit(client, at(10))
    assert row(debit)["movement_id"] is None
    assert awaiting(client)[debit]["topup_suggestion"]["target_account_id"] == funds["a"]


def test_pair_is_recorded_one_per_product(client, funds):
    for month in (7, 8, 9):
        learn_pair(client, funds, month)
    first = rdn_debit(client, at(10, second=5))
    # Alone, the first debit waits: the learner never guesses which product it is for.
    assert row(first)["movement_id"] is None
    second = rdn_debit(client, at(10, second=5))

    from app.db.pool import db_conn
    with db_conn() as conn:
        targets = conn.execute(
            """SELECT t.id, it.target_account_id FROM transactions t JOIN investment_topups it ON it.id = t.movement_id
               WHERE t.id = ANY(%s::uuid[])""", ([first, second],),
        ).fetchall()
    assigned = {str(item["id"]): str(item["target_account_id"]) for item in targets}
    ordered = sorted([first, second])
    assert assigned == {ordered[0]: funds["a"], ordered[1]: funds["b"]}
    assert row(first)["classified_by"] == row(second)["classified_by"] == "habit"


def test_deleting_an_automatic_topup_returns_the_debit_and_rejects(client, funds):
    for month in (7, 8, 9):
        converted(client, at(month), funds["a"])
    debit = rdn_debit(client, at(10))
    original = row(debit)
    assert original["classified_by"] == "habit"
    value = balances(client)[funds["a"]]["balance"]

    assert client.delete(f"/api/investment-topups/{original['movement_id']}").status_code == 200

    restored = row(debit)
    assert restored["movement_id"] is None and restored["review_status"] == "needs_review"
    assert restored["classified_by"] == "rule"
    assert balances(client)[funds["a"]]["balance"] == value - AMOUNT
    assert [(str(item["transaction_id"]), item["outcome"]) for item in answers(funds["user_id"])] == [(debit, "rejected")]
    # Back in Tinjau without a suggestion, and the next debit of its window does not convert it again.
    assert awaiting(client)[debit]["topup_suggestion"] is None
    rdn_debit(client, at(10, day=2))
    assert row(debit)["movement_id"] is None


def test_a_refused_conversion_leaves_the_debit_awaiting(client, funds, monkeypatch):
    from fastapi import HTTPException
    from app.services import investment_topups

    for month in (7, 8, 9):
        converted(client, at(month), funds["a"])

    def refuse(*args, **kwargs):
        raise HTTPException(status_code=422, detail={"code": "amount_product_required"})

    monkeypatch.setattr(investment_topups, "convert_transaction_to_topup", refuse)
    debit = rdn_debit(client, at(10))
    assert row(debit)["movement_id"] is None and row(debit)["review_status"] == "needs_review"
    assert debit in awaiting(client)
