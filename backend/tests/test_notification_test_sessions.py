"""Capture-only test sessions ("Sesi uji", notification-test-session). Synthetic owners and texts.

The incident's shape from `test_duplicate_arrivals`: GoPay reports "menerima Tabungan by Jago", Jago reports
"received from GoPay" and myBCA reports the debit. During a session none of them may book anything.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from test_duplicate_arrivals import (
    GOPAY, GOPAY_CREDIT, JAGO, JAGO_CREDIT, MYBCA, MYBCA_DEBIT, START, add_link, event, event_row, legs, memory_rows,
    send,
)
from test_notification_processing_database import ingest, notification_owner  # noqa: F401 - fixture

STOCKBIT = "com.stockbit.android"
STOCKBIT_BUY = "Pembelian 4 lot FIKS match di harga Rp3.340"
JAGO_POCKET_MOVE = "You've moved Rp50.000 from Kantong Utama to your Dana Darurat Pocket."
V49 = Path(__file__).resolve().parents[2] / "db" / "migrations" / "V49__notification_test_sessions.sql"


@pytest.fixture
def session_owner(notification_owner):
    """The synthetic owner, signed in through a browser session as Pengaturan uses it."""
    from app.main import app
    from app.services.auth import get_session_user

    app.dependency_overrides[get_session_user] = lambda: {
        "id": notification_owner["user_id"], "name": "Raka Purnama", "currency": "IDR"}
    yield notification_owner


def add_session(owner, start_offset=-60, end_offset=600, *, ended=False, user_id=None):
    """A session window around START (seconds from START); `ended` closes it at its planned end."""
    from app.db.pool import db_conn
    started = START + timedelta(seconds=start_offset)
    planned = START + timedelta(seconds=end_offset)
    with db_conn() as conn:
        row = conn.execute(
            """INSERT INTO notification_test_sessions (user_id, started_at, planned_end_at, ended_at)
               VALUES (%s, %s, %s, %s) RETURNING id""",
            (user_id or owner["user_id"], started, planned, planned if ended else None),
        ).fetchone()
        conn.commit()
    return str(row["id"])


def tagged_events(owner):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute(
            "SELECT * FROM notification_events WHERE user_id = %s AND test_session_id IS NOT NULL ORDER BY post_time",
            (owner["user_id"],),
        ).fetchall()


def assert_capture_only(row, session_id):
    assert row["processing_state"] == "ignored"
    assert row["error_code"] == "test_session"
    assert str(row["test_session_id"]) == session_id
    assert row["transaction_id"] is None and row["movement_id"] is None
    assert row["interpretation"] is None and row["active_question"] is None and row["next_attempt_at"] is None


# --- 2.1 An active session books nothing ---------------------------------------------------------------


def test_an_active_session_books_nothing_for_every_kind_of_report(client, notification_owner):
    owner = notification_owner
    session_id = add_session(owner)
    response = ingest(client, *[
        event(GOPAY_CREDIT, GOPAY, 0), event(JAGO_CREDIT, JAGO, 0.13), event(MYBCA_DEBIT, MYBCA, 3.4),
        event(JAGO_POCKET_MOVE, JAGO, 30), event(STOCKBIT_BUY, STOCKBIT, 45),
    ])
    assert response["ok"] is True and response["created_transactions"] == 0 and response["inserted"] == 5
    assert legs(owner) == []
    assert memory_rows(owner) == {}
    rows = tagged_events(owner)
    assert len(rows) == 5
    for row in rows:
        assert_capture_only(row, session_id)


def test_the_phone_gets_a_settled_ignored_result(client, notification_owner):
    """Companion contract: `ignored` settles the row ("Diabaikan"); nothing is polled, asked or retried."""
    add_session(notification_owner)
    payload = event(JAGO_CREDIT, JAGO, 0)
    response = ingest(client, payload)
    [result] = response["results"]
    assert set(result) == {"payload_hash", "event_id", "status", "error_code"}
    assert (result["payload_hash"], result["status"], result["error_code"]) == (payload["payload_hash"], "ignored", "test_session")
    # The same upload again (a phone retry) gets the same settled answer and stores nothing new.
    again = ingest(client, payload)
    assert again["results"][0] == result and again["inserted"] == 0
    assert len(tagged_events(notification_owner)) == 1


def test_a_late_upload_of_a_session_notification_stays_capture_only(client, notification_owner):
    owner = notification_owner
    session_id = add_session(owner, ended=True)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    assert legs(owner) == []
    assert_capture_only(tagged_events(owner)[0], session_id)


def test_a_notification_posted_before_the_session_books_normally(client, notification_owner):
    owner = notification_owner
    add_session(owner, start_offset=10)
    result = send(client, GOPAY_CREDIT, GOPAY, 0)
    assert result["status"] == "recorded"
    assert len(legs(owner)) == 1 and tagged_events(owner) == []


def test_capture_resumes_after_the_session(client, notification_owner):
    owner = notification_owner
    add_session(owner, start_offset=-60, end_offset=60)
    inside = send(client, JAGO_CREDIT, JAGO, 0)
    after = send(client, GOPAY_CREDIT, GOPAY, 120)
    assert inside["status"] == "ignored" and after["status"] == "recorded"
    assert [str(row["account_id"]) for row in legs(owner)] == [owner["gopay"]]


def test_another_owners_session_does_not_capture_this_owner(client, notification_owner):
    other = str(uuid4())
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute("INSERT INTO users (id, username, name, password_hash, invite_code, language) VALUES (%s, %s, 'Lain', 'x', 'TEST', 'id')",
                     (other, f"other_{uuid4().hex}"))
        conn.commit()
    try:
        add_session(notification_owner, user_id=other)
        assert send(client, GOPAY_CREDIT, GOPAY, 0)["status"] == "recorded"
    finally:
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (other,))
            conn.commit()


# --- 2.1 No later path processes a session event ------------------------------------------------------


def test_the_worker_retry_resolve_and_pairing_leave_session_events_alone(client, notification_owner):
    from app.services import notification_processing as processing
    from app.services.movement_pairing import repair_owner

    owner = notification_owner
    session_id = add_session(owner, start_offset=-60, end_offset=60)
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    send(client, MYBCA_DEBIT, MYBCA, 3.4)

    assert processing.claim_event(include_ai=True) is None
    retry = client.post(f"/api/ingest/notifications/{jago['event_id']}/retry")
    assert retry.status_code == 409
    resolve = client.post(f"/api/ingest/notifications/{jago['event_id']}/resolve", json={
        "type": "income", "account_id": owner["main"], "category_id": owner["income"]})
    assert resolve.status_code == 409
    repair_owner(owner["user_id"], START - timedelta(hours=1))

    # A linked account must not treat the session's Jago report as the kept report of a later GoPay one.
    add_link(owner)
    later = send(client, GOPAY_CREDIT, GOPAY, 120)
    assert later["status"] == "recorded"
    assert [str(row["account_id"]) for row in legs(owner)] == [owner["gopay"]]
    for row in tagged_events(owner):
        assert_capture_only(row, session_id)


def test_with_the_ai_on_a_session_event_never_reaches_the_ai_queue(client, notification_owner, monkeypatch):
    from dataclasses import replace
    from app.services import notification_processing as processing

    monkeypatch.setattr(processing, "settings", replace(processing.settings, notification_ai_enabled=True))
    session_id = add_session(notification_owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    assert jago["status"] == "ignored"
    assert_capture_only(event_row(jago["event_id"]), session_id)
    assert processing.claim_event(include_ai=True) is None


def test_the_database_refuses_to_book_a_session_event(client, notification_owner):
    from app.db.pool import db_conn

    add_session(notification_owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    with db_conn() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute("UPDATE notification_events SET processing_state = 'queued' WHERE id = %s", (jago["event_id"],))
        conn.rollback()
    assert event_row(jago["event_id"])["processing_state"] == "ignored"


# --- 2.4 The session API ------------------------------------------------------------------------------


def test_owner_starts_ends_labels_exports_and_deletes_a_session(client, session_owner):
    owner = session_owner
    started = client.post("/api/ingest/test-sessions", json={"duration_minutes": 120})
    assert started.status_code == 200, started.text
    session = started.json()["session"]
    assert session["status"] == "open" and session["event_count"] == 0
    assert client.post("/api/ingest/test-sessions", json={"duration_minutes": 30}).status_code == 409
    assert client.get("/api/ingest/test-sessions/current").json()["session"]["id"] == session["id"]

    # A notification posted now is captured for the running session.
    payload = event(JAGO_CREDIT, JAGO, 0)
    payload["post_time"] = datetime.now(timezone.utc).isoformat()
    [result] = ingest(client, payload)["results"]
    assert (result["status"], result["error_code"]) == ("ignored", "test_session")

    [listed] = client.get(f"/api/ingest/test-sessions/{session['id']}/events").json()["events"]
    assert (listed["event_id"], listed["app_label"], listed["direction"], listed["amount"]) == (
        result["event_id"], "Bank Jago", "income", 500000)
    labelled = client.patch(f"/api/ingest/test-sessions/{session['id']}/events/{result['event_id']}",
                            json={"test_case": "5", "label_notes": "BCA ke Kantong Utama"})
    assert labelled.status_code == 200 and labelled.json()["event"]["test_case"] == "5"

    export = client.get(f"/api/ingest/test-sessions/{session['id']}/export.csv")
    assert export.status_code == 200 and export.headers["content-type"].startswith("text/csv")
    lines = export.text.strip().splitlines()
    assert lines[0].startswith("nomor_kasus,") and lines[1].startswith("5,")

    ended = client.post(f"/api/ingest/test-sessions/{session['id']}/end")
    assert ended.status_code == 200 and ended.json()["session"]["status"] == "ended"
    assert client.get("/api/ingest/test-sessions/current").json()["session"] is None
    assert client.post(f"/api/ingest/test-sessions/{session['id']}/end").status_code == 200

    assert client.delete(f"/api/ingest/test-sessions/{session['id']}").status_code == 200
    assert client.get("/api/ingest/test-sessions").json()["sessions"] == []
    assert tagged_events(owner) == []
    # The window stays: the same notification uploaded late is captured again, never booked.
    again = ingest(client, {**payload, "payload_hash": uuid4().hex})["results"][0]
    assert again["status"] == "ignored" and legs(owner) == []


def test_an_expired_session_counts_as_ended_and_a_new_one_can_start(client, session_owner):
    add_session(session_owner)  # open on paper, but its planned end lies in the past
    assert client.get("/api/ingest/test-sessions/current").json()["session"] is None
    [old] = client.get("/api/ingest/test-sessions").json()["sessions"]
    assert old["status"] == "ended"
    assert client.post("/api/ingest/test-sessions", json={"duration_minutes": 15}).status_code == 200
    statuses = sorted(item["status"] for item in client.get("/api/ingest/test-sessions").json()["sessions"])
    assert statuses == ["ended", "open"]


@pytest.mark.parametrize("body", [{"duration_minutes": 14}, {"duration_minutes": 361}, {}])
def test_an_invalid_duration_is_refused(client, session_owner, body):
    assert client.post("/api/ingest/test-sessions", json=body).status_code == 422


def test_an_invalid_label_is_refused(client, session_owner):
    session_id = add_session(session_owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    url = f"/api/ingest/test-sessions/{session_id}/events/{jago['event_id']}"
    assert client.patch(url, json={"test_case": "123456789"}).status_code == 422
    assert client.patch(url, json={"label_notes": "x" * 401}).status_code == 422


def test_another_owner_sees_none_of_the_sessions(client, session_owner):
    from app.main import app
    from app.services.auth import get_session_user

    session_id = add_session(session_owner)
    jago = send(client, JAGO_CREDIT, JAGO, 0)
    app.dependency_overrides[get_session_user] = lambda: {"id": str(uuid4()), "name": "Lain", "currency": "IDR"}
    assert client.get("/api/ingest/test-sessions").json()["sessions"] == []
    for method, url, body in (
        ("get", f"/api/ingest/test-sessions/{session_id}/events", None),
        ("post", f"/api/ingest/test-sessions/{session_id}/end", None),
        ("patch", f"/api/ingest/test-sessions/{session_id}/events/{jago['event_id']}", {"test_case": "1"}),
        ("get", f"/api/ingest/test-sessions/{session_id}/export.csv", None),
        ("delete", f"/api/ingest/test-sessions/{session_id}", None),
    ):
        response = client.request(method.upper(), url, json=body)
        assert response.status_code == 404 and response.json()["detail"]["code"] == "test_session_not_found", (method, url)


def test_the_companion_api_key_cannot_manage_sessions(client, notification_owner):
    headers = {"Authorization": "Bearer synthetic-companion-key"}
    for response in (
        client.get("/api/ingest/test-sessions", headers=headers),
        client.get("/api/ingest/test-sessions/current", headers=headers),
        client.post("/api/ingest/test-sessions", json={"duration_minutes": 60}, headers=headers),
        client.delete(f"/api/ingest/test-sessions/{uuid4()}", headers=headers),
    ):
        assert response.status_code == 403 and response.json()["detail"]["code"] == "session_required"


def test_v49_runs_twice_on_a_database_with_events(client, notification_owner):
    from app.db.pool import db_conn

    send(client, GOPAY_CREDIT, GOPAY, 0)
    for _ in range(2):
        with db_conn() as conn:
            conn.execute(V49.read_text())
            conn.commit()
    with db_conn() as conn:
        row = conn.execute("SELECT test_session_id, test_case FROM notification_events WHERE user_id = %s",
                           (notification_owner["user_id"],)).fetchone()
    assert (row["test_session_id"], row["test_case"]) == (None, None)
