"""Skip patterns in Pengaturan (skip-redundant-notifications D7). Synthetic owners and notifications."""
from uuid import uuid4

import pytest

from test_duplicate_arrivals import GOPAY, GOPAY_CREDIT, JAGO, JAGO_CREDIT, send
from test_notification_processing_database import notification_owner  # noqa: F401 - fixture

JAGO_FROM_GOPAY = {"redundant_institution": "jago", "direction": "income", "kept_institution": "gopay"}


@pytest.fixture
def session_owner(notification_owner):
    """The synthetic owner, signed in through a browser session."""
    from app.main import app
    from app.services.auth import get_session_user

    app.dependency_overrides[get_session_user] = lambda: {
        "id": notification_owner["user_id"], "name": "Raka Purnama", "currency": "IDR"}
    yield notification_owner


def seen_both_apps(client):
    send(client, GOPAY_CREDIT, GOPAY, 0)
    send(client, JAGO_CREDIT, JAGO, 400)


def test_owner_adds_lists_and_stops_a_pattern(client, session_owner):
    seen_both_apps(client)
    empty = client.get("/api/ingest/skip-patterns").json()
    assert empty["patterns"] == []
    assert empty["institutions"] == [{"institution": "gopay", "label": "GoPay"}, {"institution": "jago", "label": "Bank Jago"}]

    created = client.post("/api/ingest/skip-patterns", json=JAGO_FROM_GOPAY)
    assert created.status_code == 201, created.text
    pattern = created.json()["pattern"]
    assert (pattern["redundant_label"], pattern["kept_label"], pattern["source"], pattern["skipped_count"]) == (
        "Bank Jago", "GoPay", "manual", 0)

    listed = client.get("/api/ingest/skip-patterns").json()["patterns"]
    assert [item["id"] for item in listed] == [pattern["id"]]

    assert client.delete(f"/api/ingest/skip-patterns/{pattern['id']}").status_code == 200
    assert client.delete(f"/api/ingest/skip-patterns/{pattern['id']}").status_code == 200
    assert client.get("/api/ingest/skip-patterns").json()["patterns"] == []
    # A stopped pattern can be added again.
    assert client.post("/api/ingest/skip-patterns", json=JAGO_FROM_GOPAY).status_code == 201


def test_listing_counts_what_a_pattern_skipped(client, session_owner):
    seen_both_apps(client)
    pattern = client.post("/api/ingest/skip-patterns", json=JAGO_FROM_GOPAY).json()["pattern"]
    send(client, GOPAY_CREDIT, GOPAY, 900)
    send(client, JAGO_CREDIT, JAGO, 900.13)
    [listed] = client.get("/api/ingest/skip-patterns").json()["patterns"]
    assert (listed["id"], listed["skipped_count"]) == (pattern["id"], 1)
    assert listed["last_skipped_at"] is not None


def test_an_active_duplicate_is_409(client, session_owner):
    seen_both_apps(client)
    assert client.post("/api/ingest/skip-patterns", json=JAGO_FROM_GOPAY).status_code == 201
    again = client.post("/api/ingest/skip-patterns", json=JAGO_FROM_GOPAY)
    assert again.status_code == 409 and again.json()["detail"]["code"] == "skip_pattern_exists"


@pytest.mark.parametrize("payload", [
    {**JAGO_FROM_GOPAY, "kept_institution": "jago"},
    {**JAGO_FROM_GOPAY, "direction": "sideways"},
    {**JAGO_FROM_GOPAY, "redundant_institution": "bank-lain"},
    # The owner has never received a ShopeePay notification.
    {**JAGO_FROM_GOPAY, "kept_institution": "shopeepay"},
])
def test_an_invalid_pattern_is_422(client, session_owner, payload):
    seen_both_apps(client)
    response = client.post("/api/ingest/skip-patterns", json=payload)
    assert response.status_code == 422 and response.json()["detail"]["code"] == "invalid_skip_pattern"


def test_another_owner_cannot_see_or_stop_the_pattern(client, session_owner):
    from app.main import app
    from app.services.auth import get_session_user

    seen_both_apps(client)
    pattern = client.post("/api/ingest/skip-patterns", json=JAGO_FROM_GOPAY).json()["pattern"]
    app.dependency_overrides[get_session_user] = lambda: {"id": str(uuid4()), "name": "Lain", "currency": "IDR"}
    assert client.get("/api/ingest/skip-patterns").json()["patterns"] == []
    for target in (pattern["id"], str(uuid4())):
        response = client.delete(f"/api/ingest/skip-patterns/{target}")
        assert response.status_code == 404 and response.json()["detail"]["code"] == "skip_pattern_not_found"


def test_the_companion_api_key_cannot_manage_patterns(client, notification_owner):
    headers = {"Authorization": "Bearer synthetic-companion-key"}
    for response in (
        client.get("/api/ingest/skip-patterns", headers=headers),
        client.post("/api/ingest/skip-patterns", json=JAGO_FROM_GOPAY, headers=headers),
        client.delete(f"/api/ingest/skip-patterns/{uuid4()}", headers=headers),
    ):
        assert response.status_code == 403 and response.json()["detail"] == {"code": "session_required"}
