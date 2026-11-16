"""A transaction's name, the owner's note, and the `notes` alias for older clients.

All names, notes and amounts are synthetic. Provider calls are mocked; no model is reached.
"""
import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from psycopg import sql

from test_notification_processing_database import (  # noqa: F401 - fixture
    enable_ai, ingest, interpretation, notification, notification_owner,
)
from test_text_capture import captured, model_result, owner, provider_with, use_provider  # noqa: F401 - fixture

ROOT = Path(__file__).resolve().parents[2]
NOTE_MARKER = "fellowship coach catatan pribadi"


def query(statement, params=()):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute(statement, params).fetchall()


def create(client, owner, **text):
    body = {"type": "expense", "account_id": owner["main"], "category_id": owner["expense"], "amount": 12_500, **text}
    response = client.post("/api/transactions", json=body)
    assert response.status_code == 200, response.text
    return response.json()["transaction_id"]


def detail(client, transaction_id):
    response = client.get(f"/api/transactions/{transaction_id}")
    assert response.status_code == 200, response.text
    return response.json()["transaction"]


def stored(transaction_id):
    return query("SELECT name, note, classified_by FROM transactions WHERE id = %s", (transaction_id,))[0]


def text_of(row):
    return {"name": row["name"], "note": row["note"], "notes": row["notes"]}


def test_older_client_sending_notes_names_the_transaction(client, notification_owner):
    transaction_id = create(client, notification_owner, notes="Parkir Contoh")

    assert text_of(detail(client, transaction_id)) == {"name": "Parkir Contoh", "note": None, "notes": "Parkir Contoh"}
    edited = client.patch(f"/api/transactions/{transaction_id}", json={"notes": "Parkir Contoh Mall"})
    assert edited.status_code == 200, edited.text
    assert (stored(transaction_id)["name"], stored(transaction_id)["note"]) == ("Parkir Contoh Mall", None)


def test_name_wins_when_both_name_and_notes_are_sent(client, notification_owner):
    transaction_id = create(client, notification_owner, name="Makan di Kafe Contoh", notes="Teks lama")
    assert stored(transaction_id)["name"] == "Makan di Kafe Contoh"

    client.patch(f"/api/transactions/{transaction_id}", json={"name": "Makan siang", "notes": "Teks lama"})
    assert stored(transaction_id)["name"] == "Makan siang"


def test_note_on_create_update_and_detail(client, notification_owner):
    transaction_id = create(client, notification_owner, name="Bayar makan di Kafe Contoh", note="  fellowship coach  ")
    assert text_of(detail(client, transaction_id)) == {
        "name": "Bayar makan di Kafe Contoh", "note": "fellowship coach", "notes": "Bayar makan di Kafe Contoh",
    }
    listed = client.get("/api/transactions", params={"q": "Kafe Contoh"}).json()["transactions"]
    assert [text_of(row) for row in listed if row["id"] == transaction_id] == [text_of(detail(client, transaction_id))]

    before = stored(transaction_id)
    edited = client.patch(f"/api/transactions/{transaction_id}", json={"note": "fellowship coach, dibayar bersama"})
    assert edited.status_code == 200, edited.text
    after = stored(transaction_id)
    assert (after["name"], after["note"]) == ("Bayar makan di Kafe Contoh", "fellowship coach, dibayar bersama")
    # A note is the owner's remark, not a classification: it does not change who classified the row.
    assert after["classified_by"] == before["classified_by"]

    client.patch(f"/api/transactions/{transaction_id}", json={"name": "Makan bersama"})
    assert (stored(transaction_id)["name"], stored(transaction_id)["note"]) == (
        "Makan bersama", "fellowship coach, dibayar bersama")

    client.patch(f"/api/transactions/{transaction_id}", json={"note": "   "})
    assert stored(transaction_id)["note"] is None
    assert detail(client, transaction_id)["note"] is None

    too_long = client.patch(f"/api/transactions/{transaction_id}", json={"note": "x" * 501})
    assert too_long.status_code == 422


def test_search_finds_a_note(client, notification_owner):
    noted = create(client, notification_owner, name="Bayar makan di Kafe Contoh", note="Fellowship coach")
    named = create(client, notification_owner, name="Fellowship snack")
    create(client, notification_owner, name="Bayar parkir")

    found = client.get("/api/transactions", params={"q": "fellowship"}).json()["transactions"]
    assert {row["id"] for row in found} == {noted, named}


def stuck_event(client):
    stuck = ingest(client, notification("You've paid Rp50.000, biaya Rp2.500, total Rp52.500"))["results"][0]
    assert stuck["status"] == "needs_review"
    return stuck["event_id"]


def recorded_by(event_id):
    return query(
        """SELECT t.name, t.note FROM notification_events n JOIN transactions t ON t.id = n.transaction_id
           WHERE n.id = %s""",
        (event_id,),
    )[0]


def test_an_older_companion_resolves_an_event_with_notes(client, notification_owner):
    event_id = stuck_event(client)
    body = {"type": "expense", "account_id": notification_owner["main"], "category_id": notification_owner["expense"],
            "amount": 52_500, "notes": "Parkir Contoh"}
    response = client.post(f"/api/ingest/notifications/{event_id}/resolve", json=body)
    assert response.status_code == 200, response.text
    assert dict(recorded_by(event_id)) == {"name": "Parkir Contoh", "note": None}


def test_tinjau_resolution_writes_name_and_note(client, notification_owner):
    event_id = stuck_event(client)
    body = {"type": "expense", "account_id": notification_owner["main"], "category_id": notification_owner["expense"],
            "amount": 52_500, "name": "Bayar makan di Kafe Contoh", "notes": "Teks lama", "note": " fellowship coach "}
    response = client.post(f"/api/ingest/notifications/{event_id}/resolve", json=body)
    assert response.status_code == 200, response.text
    assert dict(recorded_by(event_id)) == {"name": "Bayar makan di Kafe Contoh", "note": "fellowship coach"}


def test_bulk_rename_accepts_the_alias_and_a_note(client, notification_owner):
    first = create(client, notification_owner, name="Bayar Kedai", review_status="needs_review")
    second = create(client, notification_owner, name="Bayar Kedai", review_status="needs_review")

    renamed = client.post("/api/review/bulk", json={"action": "rename", "transaction_ids": [first], "notes": "Ngopi"})
    assert renamed.status_code == 200, renamed.text
    assert (stored(first)["name"], stored(first)["note"]) == ("Ngopi", None)

    noted = client.post("/api/review/bulk", json={"action": "rename", "transaction_ids": [first, second],
                                                   "note": "fellowship coach"})
    assert noted.status_code == 200, noted.text
    assert [(stored(row)["name"], stored(row)["note"]) for row in (first, second)] == [
        ("Ngopi", "fellowship coach"), ("Bayar Kedai", "fellowship coach")]


def add_noted_history(owner):
    """An owner-classified earlier payment at the same merchant, with a private note."""
    from app.db.pool import db_conn

    with db_conn() as conn:
        conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, name, note, merchant,
                   source, review_status, classified_by)
               VALUES (%s, %s, %s, 'expense', 120000, NOW() - INTERVAL '2 days', 'Makan di Kedai Awan', %s,
                       'Kedai Awan', 'manual', 'reviewed', 'owner')""",
            (owner["user_id"], owner["main"], owner["expense"], NOTE_MARKER),
        )
        conn.commit()


def test_the_ai_never_writes_the_note_and_never_sees_one(client, notification_owner, monkeypatch):
    from app.services.notification_processing import claim_event, process_claim

    enable_ai(monkeypatch)
    add_noted_history(notification_owner)
    event = ingest(client, notification())["results"][0]
    provider = AsyncMock()
    provider.interpret.return_value = interpretation(notification_owner)
    asyncio.run(process_claim(claim_event(), provider))

    context = json.dumps(provider.interpret.await_args.args[0], ensure_ascii=False, default=str)
    # The earlier row is in the model input by its name, and its note is not.
    assert "Makan di Kedai Awan" in context
    assert NOTE_MARKER not in context and '"note"' not in context
    assert dict(recorded_by(event["event_id"])) == {"name": "Bayar Kedai Awan", "note": None}


def test_text_capture_names_the_row_and_its_model_input_has_no_note(client, owner, monkeypatch):
    from app.db.pool import db_conn

    with db_conn() as conn:
        conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, name, note,
                   source, review_status, classified_by)
               VALUES (%s, %s, %s, 'expense', 6000, NOW() - INTERVAL '1 day', 'Es teh manis', %s,
                       'manual', 'reviewed', 'owner')""",
            (owner["user_id"], owner["cash"], owner["jajan"], NOTE_MARKER),
        )
        conn.commit()
    provider = use_provider(monkeypatch, provider_with(model_result(owner)))

    capture = captured(client, "es teh manis 5rb tunai")
    assert capture["status"] == "recorded"
    context = json.dumps(provider.normalize_capture.await_args.args[0], ensure_ascii=False, default=str)
    assert "Es teh manis" in context
    assert NOTE_MARKER not in context and '"note"' not in context
    row = query("SELECT name, note FROM transactions WHERE id = %s", (capture["transaction"]["id"],))[0]
    assert dict(row) == {"name": "Es teh manis", "note": None}
    assert (capture["transaction"]["name"], capture["transaction"]["note"], capture["transaction"]["notes"]) == (
        "Es teh manis", None, "Es teh manis")


def migrate_before_v57(conn):
    paths = sorted((ROOT / "db/migrations").glob("V*.sql"), key=lambda path: int(path.name.split("__")[0][1:]))
    for path in paths:
        if path.name.startswith("V57__"):
            return
        conn.execute(path.read_text())


def test_v57_keeps_every_existing_text_as_the_name_and_starts_notes_empty(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.pool import db_conn

    schema = "fictional_name_note_upgrade_" + uuid4().hex
    with db_conn() as conn:
        # The scratch schema and everything in it are rolled back after verification.
        with conn.transaction(force_rollback=True):
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            conn.execute(sql.SQL("SET LOCAL search_path TO {}, public").format(sql.Identifier(schema)))
            migrate_before_v57(conn)

            def one(statement, params=()):
                return conn.execute(statement, params).fetchone()

            user = one("INSERT INTO users (username, password_hash, invite_code) VALUES ('fictional-owner', 'x', 'x') RETURNING id")["id"]
            bank = one("INSERT INTO accounts (user_id, name, type) VALUES (%s, 'Bank Fiksi', 'bank') RETURNING id", (user,))["id"]
            named = one(
                """INSERT INTO transactions (user_id, account_id, type, amount, notes, date)
                   VALUES (%s, %s, 'expense', 150000, 'Isi bensin di SPBU Contoh', NOW()) RETURNING id""",
                (user, bank),
            )["id"]
            unnamed = one(
                """INSERT INTO transactions (user_id, account_id, type, amount, date)
                   VALUES (%s, %s, 'expense', 2000, NOW()) RETURNING id""",
                (user, bank),
            )["id"]

            conn.execute((ROOT / "db/migrations/V57__transaction_name_and_note.sql").read_text())

            columns = {row["column_name"] for row in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = 'transactions'",
                (schema,),
            ).fetchall()}
            assert {"name", "note"} <= columns and "notes" not in columns
            rows = {row["id"]: (row["name"], row["note"]) for row in conn.execute(
                "SELECT id, name, note FROM transactions ORDER BY id").fetchall()}
            assert rows == {named: ("Isi bensin di SPBU Contoh", None), unnamed: (None, None)}
