"""The web review queue and its bulk actions (phase 6).

All texts, names and amounts are synthetic. The owner of `notification_owner` is "Raka Purnama".
"""
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from test_ingestion_memory_and_purpose import (  # noqa: F401 - fixtures
    KEDAI, event_row, feedback_rows, memory_rows, open_question, owner, tx_for,
)
from test_notification_processing_database import ingest, notification, notification_owner  # noqa: F401

GOPAY = "com.gojek.gopay"


def kedai(client, seconds):
    """A Jago payment to a named merchant: recorded automatically by the rules, with no question."""
    result = ingest(client, notification(KEDAI, seconds=seconds))["results"][0]
    assert result["status"] == "recorded", result
    return str(tx_for(result["event_id"])["id"]), result["event_id"]


def row(tx_id):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute("SELECT * FROM transactions WHERE id = %s", (tx_id,)).fetchone()


def bulk(client, action, ids, **fields):
    return client.post("/api/review/bulk", json={"action": action, "transaction_ids": ids, **fields})


def manual_movement(client, owner, amount=40000):
    response = client.post("/api/movements", json={
        "source_account_id": owner["bca"], "target_account_id": owner["gopay"], "amount": amount,
    })
    assert response.status_code == 200, response.text
    from app.db.pool import db_conn
    with db_conn() as conn:
        legs = conn.execute(
            "SELECT id FROM transactions WHERE user_id = %s AND movement_id IS NOT NULL ORDER BY type",
            (owner["user_id"],),
        ).fetchall()
    return [str(leg["id"]) for leg in legs]


def ambiguous_transfer(client):
    """Two GoPay sends to the owner's BCA and one BCA credit of that amount: the credit cannot choose."""
    for seconds in (1, 2):
        ingest(client, notification("Rp500.000 udah dikirim ke BCA Raka Purnama.", GOPAY, seconds=seconds))
    incoming = ingest(client, notification(
        "Pemasukan sebesar IDR 500,000.00 dari RA** PUR**MA di kategori Transfer Rekening.", "com.bca", seconds=4,
    ))["results"][0]
    return str(tx_for(incoming["event_id"])["id"])


# --- Queue -----------------------------------------------------------------------------------


def test_queue_lists_every_section_and_the_badge_counts(client, owner):
    question_event, _ = open_question(client, owner)
    first, _ = kedai(client, 10)
    second, _ = kedai(client, 20)
    incoming = ambiguous_transfer(client)

    queue = client.get("/api/review/queue")
    assert queue.status_code == 200, queue.text
    data = queue.json()
    assert [item["event_id"] for item in data["questions"]] == [question_event]
    kedai_group = next(group for group in data["groups"] if group["label"] == "Kedai Awan")
    assert kedai_group["kind"] == "merchant" and kedai_group["type"] == "expense"
    assert kedai_group["count"] == 2 and kedai_group["total"] == 250000
    assert {item["id"] for item in kedai_group["transactions"]} == {first, second}
    assert kedai_group["first_date"] <= kedai_group["last_date"] and kedai_group["editable"] is True
    assert (kedai_group["auto_count"], kedai_group["needs_review_count"], kedai_group["reasons"]) == (2, 0, [])
    # The question row is answered in its own section, never also in a group.
    grouped_ids = {item["id"] for group in data["groups"] for item in group["transactions"]}
    assert str(tx_for(question_event)["id"]) not in grouped_ids
    # The credit and both sends compete for one transfer, so all three wait for the owner's choice.
    assert data["awaiting"] == []
    ambiguous = {item["id"]: item for item in data["ambiguous"]}
    assert len(ambiguous) == 3 and incoming in ambiguous
    sends = [item for tx_id, item in ambiguous.items() if tx_id != incoming]
    assert {item["id"] for item in ambiguous[incoming]["candidates"]} == {item["id"] for item in sends}
    assert all(item["account_name"] == "GoPay" and item["amount"] == 500000 for item in ambiguous[incoming]["candidates"])
    assert all([candidate["id"] for candidate in item["candidates"]] == [incoming] for item in sends)

    summary = client.get("/api/review/summary").json()
    # The question row, two awaiting legs and the ambiguous credit need attention; Kedai rows are unreviewed.
    assert summary["attention"] == 4 and summary["unreviewed"] == 2
    ambiguous_reasons = {reason for item in data["ambiguous"] for reason in item["reasons"]}
    assert "ambiguous_movement" in ambiguous_reasons


def test_queue_groups_merchant_less_rows_by_signature_and_movements_apart(client, owner):
    legs = manual_movement(client, owner)
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute("UPDATE transactions SET review_status = 'auto' WHERE id = ANY(%s::uuid[])", (legs,))
        conn.commit()
    data = client.get("/api/review/queue").json()
    movement = next(group for group in data["groups"] if group["kind"] == "movement")
    assert movement["editable"] is False and movement["count"] == 2 and movement["total"] == 40000


def test_resolved_reasons_are_not_shown(client, owner):
    from app.routers.review import review_reasons

    assert review_reasons({"review_reasons": ["awaiting_counterpart", "low_confidence"], "movement_id": "m"}) == ["low_confidence"]
    assert review_reasons({"review_reasons": ["purpose_question", "uncertain_category"], "movement_id": None,
                           "has_open_question": False}) == ["uncertain_category"]
    assert review_reasons({"review_reasons": ["purpose_question"], "movement_id": None, "has_open_question": True}) == ["purpose_question"]
    assert review_reasons({"review_reasons": None}) == []


def test_answer_endpoint_accepts_a_browser_session_with_origin_check(client, owner):
    from passlib.hash import bcrypt
    from app.db.pool import db_conn
    from app.main import app
    from app.services.auth import get_current_user

    event_id, question = open_question(client, owner)
    username = f"review_{uuid4().hex[:10]}"
    with db_conn() as conn:
        conn.execute("UPDATE users SET username = %s, password_hash = %s WHERE id = %s",
                     (username, bcrypt.hash("synthetic-pass-1"), owner["user_id"]))
        conn.commit()
    app.dependency_overrides.pop(get_current_user, None)
    browser = TestClient(app, base_url="https://testserver")
    origin = {"Origin": "https://testserver"}
    login = browser.post("/api/auth/login", json={"username": username, "password": "synthetic-pass-1"}, headers=origin)
    assert login.status_code == 200, login.text
    body = {"question_id": question["question_id"], "reply_id": str(uuid4()), "text": "belanja dapur mingguan"}
    path = f"/api/ingest/notifications/{event_id}/answer-purpose"
    assert browser.post(path, json=body).status_code == 403
    assert browser.post(path, json=body, headers={"Origin": "https://evil.example"}).status_code == 403
    answered = browser.post(path, json=body, headers=origin)
    assert answered.status_code == 200, answered.text
    assert event_row(event_id)["active_question"] is None
    assert [item["kind"] for item in feedback_rows(owner)] == ["purpose_answer"]


# --- Bulk actions ----------------------------------------------------------------------------


def test_accept_marks_rows_reviewed_without_reclassifying(client, owner):
    first, _ = kedai(client, 10)
    second, _ = kedai(client, 20)
    response = bulk(client, "accept", [first, second])
    assert response.status_code == 200, response.text
    assert response.json() == {"ok": True, "action": "accept", "transaction_count": 2, "corrections": 0}
    for tx_id in (first, second):
        stored = row(tx_id)
        assert (stored["review_status"], stored["classified_by"], stored["owner_edited_at"]) == ("reviewed", "rule", None)
    assert feedback_rows(owner) == []


def test_recategorise_writes_one_correction_per_row_and_teaches_memory(client, owner):
    first, first_event = kedai(client, 10)
    second, _ = kedai(client, 20)
    original = str(row(first)["category_id"])
    response = bulk(client, "recategorise", [first, second], category_id=owner["jajan"], kakeibo_type="want")
    assert response.status_code == 200, response.text
    assert response.json()["corrections"] == 2
    for tx_id in (first, second):
        stored = row(tx_id)
        assert str(stored["category_id"]) == owner["jajan"] and stored["kakeibo_type"] == "want"
        assert (stored["review_status"], stored["classified_by"]) == ("reviewed", "owner")
        assert stored["owner_edited_at"] is not None and stored["classification_confidence"] is None
    corrections = feedback_rows(owner)
    assert sorted(str(item["transaction_id"]) for item in corrections) == sorted([first, second])
    assert all(item["kind"] == "correction" and str(item["after_category_id"]) == owner["jajan"]
               and str(item["before_category_id"]) == original and item["after_kakeibo_type"] == "want"
               for item in corrections)
    assert str(next(item for item in corrections if str(item["transaction_id"]) == first)["event_id"]) == first_event
    learned = memory_rows(owner)["merchant:kedai awan"]
    assert (learned["source"], str(learned["category_id"]), learned["kakeibo_type"]) == ("owner", owner["jajan"], "want")

    again = bulk(client, "recategorise", [first, second], category_id=owner["jajan"], kakeibo_type="want")
    assert again.status_code == 200 and again.json()["corrections"] == 0
    assert len(feedback_rows(owner)) == 2


def test_rename_sets_merchant_and_notes_with_feedback(client, owner):
    first, _ = kedai(client, 10)
    second, _ = kedai(client, 20)
    response = bulk(client, "rename", [first, second], merchant="  Kedai   Awan Senja ", notes="makan siang")
    assert response.status_code == 200, response.text
    assert response.json()["corrections"] == 2
    for tx_id in (first, second):
        stored = row(tx_id)
        assert (stored["merchant"], stored["notes"], stored["review_status"]) == ("Kedai Awan Senja", "makan siang", "reviewed")
    assert {item["after_merchant"] for item in feedback_rows(owner)} == {"Kedai Awan Senja"}


def test_recategorise_rejects_a_category_of_the_other_kind(client, owner):
    first, _ = kedai(client, 10)
    before = dict(row(first))
    response = bulk(client, "recategorise", [first], category_id=owner["patungan"])
    assert response.status_code == 422 and response.json()["detail"]["code"] == "category_type_mismatch"
    response = bulk(client, "recategorise", [first], category_id=str(uuid4()))
    assert response.status_code == 404 and response.json()["detail"]["code"] == "category_not_found"
    assert row(first)["category_id"] == before["category_id"] and row(first)["review_status"] == "auto"
    assert feedback_rows(owner) == []


def test_foreign_rows_reject_the_whole_batch(client, owner):
    from app.db.pool import db_conn
    first, _ = kedai(client, 10)
    stranger, account, foreign = str(uuid4()), str(uuid4()), str(uuid4())
    with db_conn() as conn:
        conn.execute("INSERT INTO users (id, username, password_hash, invite_code) VALUES (%s, %s, 'x', 'TEST')",
                     (stranger, f"stranger_{uuid4().hex}"))
        conn.execute("INSERT INTO accounts (id, user_id, name, type) VALUES (%s, %s, 'Kas Lain', 'cash')", (account, stranger))
        conn.execute(
            """INSERT INTO transactions (id, user_id, account_id, type, amount, source, review_status)
               VALUES (%s, %s, %s, 'expense', 1000, 'manual', 'needs_review')""",
            (foreign, stranger, account),
        )
        conn.commit()
    try:
        for action, fields in [("accept", {}), ("recategorise", {"category_id": owner["jajan"]}), ("keep_separate", {})]:
            response = bulk(client, action, [first, foreign], **fields)
            assert response.status_code == 404, response.text
            assert response.json()["detail"]["code"] == "transactions_not_found"
            assert response.json()["detail"]["transaction_ids"] == [foreign]
        assert row(first)["review_status"] == "auto" and row(foreign)["review_status"] == "needs_review"
    finally:
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (stranger,))
            conn.commit()


def test_movement_legs_accept_but_never_reclassify(client, owner):
    first, _ = kedai(client, 10)
    original = row(first)["category_id"]
    legs = manual_movement(client, owner)
    response = bulk(client, "recategorise", [first, *legs], category_id=owner["jajan"])
    assert response.status_code == 409
    assert response.json()["detail"] == {
        "code": "movement_leg_not_editable", "message": "Pindah saldo diubah lewat detail pindah saldo.",
        "transaction_ids": sorted(legs),
    }
    assert bulk(client, "rename", [first, legs[0]], notes="x").status_code == 409
    assert bulk(client, "keep_separate", [legs[0]]).json()["detail"]["code"] == "already_linked"
    assert row(first)["category_id"] == original and row(first)["review_status"] == "auto"

    accepted = bulk(client, "accept", [first, *legs])
    assert accepted.status_code == 200
    assert {row(tx_id)["review_status"] for tx_id in [first, *legs]} == {"reviewed"}
    assert all(row(leg)["movement_id"] is not None for leg in legs)


def test_keep_separate_stops_automatic_pairing(client, owner):
    incoming = ambiguous_transfer(client)
    response = bulk(client, "keep_separate", [incoming])
    assert response.status_code == 200, response.text
    stored = row(incoming)
    assert stored["owner_split_at"] is not None and stored["review_status"] == "reviewed"
    from app.services.movement_pairing import repair_unpaired_movements
    repair_unpaired_movements()
    assert row(incoming)["movement_id"] is None
    queue = client.get("/api/review/queue").json()
    # With the credit kept apart, both sends only wait for their counterpart account.
    assert queue["ambiguous"] == [] and len(queue["awaiting"]) == 2


@pytest.mark.parametrize("body", [
    {"action": "accept", "transaction_ids": []},
    {"action": "accept", "transaction_ids": [str(uuid4()) for _ in range(201)]},
    {"action": "accept", "transaction_ids": ["11111111-1111-1111-1111-111111111111"] * 2},
    {"action": "accept", "transaction_ids": [str(uuid4())], "category_id": str(uuid4())},
    {"action": "recategorise", "transaction_ids": [str(uuid4())]},
    {"action": "rename", "transaction_ids": [str(uuid4())]},
    {"action": "rename", "transaction_ids": [str(uuid4())], "merchant": "x" * 121},
    {"action": "recategorise", "transaction_ids": [str(uuid4())], "category_id": str(uuid4()), "kakeibo_type": "luxury"},
    {"action": "delete", "transaction_ids": [str(uuid4())]},
], ids=["empty", "over-limit", "duplicates", "extra-field", "no-category", "no-rename-field", "long-merchant",
        "bad-pillar", "unknown-action"])
def test_invalid_bulk_bodies_are_rejected(client, owner, body):
    assert client.post("/api/review/bulk", json=body).status_code == 422


def test_batch_of_two_hundred_rows_is_accepted(client, owner):
    from app.db.pool import db_conn
    ids = [str(uuid4()) for _ in range(200)]
    with db_conn() as conn:
        for tx_id in ids:
            conn.execute(
                """INSERT INTO transactions (id, user_id, account_id, type, amount, source, review_status)
                   VALUES (%s, %s, %s, 'expense', 1000, 'notification', 'auto')""",
                (tx_id, owner["user_id"], owner["bca"]),
            )
        conn.commit()
    response = bulk(client, "accept", ids)
    assert response.status_code == 200 and response.json()["transaction_count"] == 200
    with db_conn() as conn:
        remaining = conn.execute(
            "SELECT COUNT(*) AS n FROM transactions WHERE id = ANY(%s::uuid[]) AND review_status <> 'reviewed'", (ids,),
        ).fetchone()["n"]
    assert remaining == 0
