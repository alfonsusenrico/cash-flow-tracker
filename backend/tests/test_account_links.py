"""Account links: the API, the Rekening detail's data and the V47 conversion of skip patterns
(reconcile-linked-account-reports D6, D7). Synthetic owners and notifications."""
from pathlib import Path
from uuid import uuid4

import pytest

from test_duplicate_arrivals import GOPAY, GOPAY_CREDIT, JAGO, JAGO_CREDIT, add_link, send
from test_notification_processing_database import notification_owner  # noqa: F401 - fixture

V47 = Path(__file__).resolve().parents[2] / "db" / "migrations" / "V47__account_report_links.sql"


@pytest.fixture
def session_owner(notification_owner):
    """The synthetic owner, signed in through a browser session."""
    from app.main import app
    from app.services.auth import get_session_user

    app.dependency_overrides[get_session_user] = lambda: {
        "id": notification_owner["user_id"], "name": "Raka Purnama", "currency": "IDR"}
    yield notification_owner


def test_owner_lists_and_stops_a_link(client, session_owner):
    owner = session_owner
    link = add_link(owner)
    send(client, GOPAY_CREDIT, GOPAY, 0)
    send(client, JAGO_CREDIT, JAGO, 0.13)
    [listed] = client.get("/api/ingest/account-links").json()["links"]
    assert (listed["id"], listed["account_id"], listed["account_label"], listed["institution_label"],
            listed["source"], listed["skipped_count"]) == (link, owner["gopay"], "GoPay", "Bank Jago", "confirmation", 1)
    assert client.get(f"/api/ingest/account-links?account_id={owner['gopay']}").json()["links"][0]["id"] == link
    assert client.get(f"/api/ingest/account-links?account_id={owner['main']}").json()["links"] == []

    assert client.delete(f"/api/ingest/account-links/{link}").status_code == 200
    assert client.delete(f"/api/ingest/account-links/{link}").status_code == 200
    assert client.get("/api/ingest/account-links").json()["links"] == []
    # A stopped link no longer decides: the next mirror waits in Terlihat dua kali.
    send(client, GOPAY_CREDIT, GOPAY, 900)
    later = send(client, JAGO_CREDIT, JAGO, 900.1)
    assert [item["event_id"] for item in client.get("/api/review/queue").json()["seen_twice"]] == [later["event_id"]]


def test_another_owner_cannot_see_or_stop_the_link(client, session_owner):
    from app.main import app
    from app.services.auth import get_session_user

    link = add_link(session_owner)
    app.dependency_overrides[get_session_user] = lambda: {"id": str(uuid4()), "name": "Lain", "currency": "IDR"}
    assert client.get("/api/ingest/account-links").json()["links"] == []
    for target in (link, str(uuid4())):
        response = client.delete(f"/api/ingest/account-links/{target}")
        assert response.status_code == 404 and response.json()["detail"]["code"] == "account_link_not_found"


def test_the_companion_api_key_cannot_manage_links(client, notification_owner):
    headers = {"Authorization": "Bearer synthetic-companion-key"}
    for response in (
        client.get("/api/ingest/account-links", headers=headers),
        client.delete(f"/api/ingest/account-links/{uuid4()}", headers=headers),
    ):
        assert response.status_code == 403 and response.json()["detail"]["code"] == "session_required"


def test_skip_pattern_endpoints_are_gone(client, session_owner):
    assert client.get("/api/ingest/skip-patterns").status_code == 404
    assert client.post("/api/ingest/skip-patterns", json={}).status_code in {404, 405}


def run_v47():
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute(V47.read_text())
        conn.commit()


def add_pattern_row(owner, redundant="jago", kept="gopay"):
    from app.db.pool import db_conn
    with db_conn() as conn:
        row = conn.execute(
            """INSERT INTO notification_skip_patterns (user_id, redundant_institution, direction, kept_institution, source)
               VALUES (%s, %s, 'income', %s, 'manual') RETURNING id""",
            (owner["user_id"], redundant, kept),
        ).fetchone()
        conn.commit()
    return str(row["id"])


def add_alias(owner, institution, account_key, name):
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO notification_account_aliases (user_id, institution, name_normalized, name_display, account_id, source)
               VALUES (%s, %s, %s, %s, %s, 'owner')""",
            (owner["user_id"], institution, name.lower(), name, owner[account_key]),
        )
        conn.commit()


def pattern_and_links(owner, pattern_id):
    from app.db.pool import db_conn
    with db_conn() as conn:
        pattern = conn.execute("SELECT stopped_at FROM notification_skip_patterns WHERE id = %s", (pattern_id,)).fetchone()
        links = conn.execute(
            "SELECT account_id, institution, source, converted_from_pattern_id FROM account_report_links WHERE user_id = %s",
            (owner["user_id"],),
        ).fetchall()
    return pattern, links


def test_v47_converts_a_resolvable_pattern_and_stops_it(notification_owner):
    owner = notification_owner
    add_alias(owner, "gopay", "gopay", "GoPay")
    pattern_id = add_pattern_row(owner)
    run_v47()
    pattern, links = pattern_and_links(owner, pattern_id)
    assert pattern["stopped_at"] is not None
    assert [(str(row["account_id"]), row["institution"], row["source"], str(row["converted_from_pattern_id"]))
            for row in links] == [(owner["gopay"], "jago", "converted", pattern_id)]
    run_v47()
    assert len(pattern_and_links(owner, pattern_id)[1]) == 1


def test_v47_stops_an_unresolvable_pattern_without_a_link(notification_owner):
    owner = notification_owner
    add_alias(owner, "gopay", "gopay", "GoPay")
    add_alias(owner, "gopay", "main", "GoPay Tabungan")
    pattern_id = add_pattern_row(owner)
    run_v47()
    pattern, links = pattern_and_links(owner, pattern_id)
    assert pattern["stopped_at"] is not None and links == []
