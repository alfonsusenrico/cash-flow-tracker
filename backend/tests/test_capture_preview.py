"""Parse-only preview for the web Catat dialog (redesign-transactions-workspace D11).

All texts, names and amounts are synthetic. Provider calls are mocked; no model is reached.
"""
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from test_notification_processing_database import enable_ai, notification_owner  # noqa: F401 - fixture
from test_text_capture import owner, query, remember, send  # noqa: F401 - fixture

JAKARTA = timezone(timedelta(hours=7))
AFTERNOON = datetime(2026, 11, 3, 15, 0, tzinfo=JAKARTA)
WRITTEN_TABLES = ("transactions", "text_captures", "merchant_memory", "classification_feedback", "notification_events")


def parse(client, text, device_time=AFTERNOON):
    return client.post("/api/capture/parse", json={"text": text, "device_time": device_time.isoformat()})


def preview(client, text, device_time=AFTERNOON):
    response = parse(client, text, device_time)
    assert response.status_code == 200, response.text
    return response.json()["preview"]


def row_counts(owner):
    return {table: query(f"SELECT COUNT(*) AS n FROM {table} WHERE user_id = %s", (owner["user_id"],))[0]["n"]
            for table in WRITTEN_TABLES}


def test_preview_of_a_short_expense(client, owner):
    remember(owner, "merchant:parkir", category="parkir", account="cash", notes="Parkir", merchant=None)
    result = preview(client, "parkir 5rb jam 2")
    assert result["amount"] == 5_000 and result["problems"] == [] and result["kind"] == "expense"
    assert result["occurred_at"] == "2026-11-03T14:00:00+07:00" and result["time_phrase"] == "jam 2"
    assert result["category_hint"] == {"category_id": owner["parkir"], "label": "Parkir", "pillar": "need"}
    assert result["description"] == "Parkir"


def test_named_account_is_a_hint_only_when_it_is_the_one_candidate(client, owner):
    assert preview(client, "kopi 20rb tunai")["account_hint"] == {"account_id": owner["cash"], "label": "Tunai"}
    assert preview(client, "kopi 20rb")["account_hint"] is None


def test_missing_amount(client, owner):
    result = preview(client, "bayar parkir")
    assert result["amount"] is None and result["problems"] == ["missing_amount"]
    assert preview(client, "kopi 20rb 30rb")["problems"] == ["several_amounts"]


def test_ambiguous_time_offers_options(client, owner):
    morning = datetime(2026, 11, 3, 11, 30, tzinfo=JAKARTA)
    result = preview(client, "parkir 5rb jam 2", morning)
    assert result["occurred_at"] is None
    assert result["time_options"] == ["2026-11-03T02:00:00+07:00", "2026-11-02T14:00:00+07:00"]


def test_preview_writes_nothing_and_asks_no_model(client, owner, monkeypatch):
    from app.main import app
    from app.services import capture_preview
    enable_ai(monkeypatch)
    provider = AsyncMock()
    monkeypatch.setattr(app.state, "notification_provider", provider)
    statements = []
    real = capture_preview.db_conn

    class RecordingCursor:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, *args, **kwargs):
            statements.append(str(sql))
            return self._cur.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._cur, name)

    class RecordingConn:
        def __init__(self, conn):
            self._conn = conn

        @contextmanager
        def cursor(self, *args, **kwargs):
            with self._conn.cursor(*args, **kwargs) as cur:
                yield RecordingCursor(cur)

    @contextmanager
    def recording_conn():
        with real() as conn:
            yield RecordingConn(conn)
    monkeypatch.setattr(capture_preview, "db_conn", recording_conn)
    remember(owner, "merchant:parkir", category="parkir", account="cash", notes="Parkir")
    before = row_counts(owner)
    for text in ("parkir 5rb jam 2", "bayar parkir", "gaji 5jt", "tarik tunai 100rb"):
        preview(client, text)
    assert row_counts(owner) == before
    assert statements and statements[0] == "SET TRANSACTION READ ONLY"
    assert not [sql for sql in statements if sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))]
    provider.normalize_capture.assert_not_awaited()


def test_another_owners_memory_and_accounts(client, owner):
    from app.db.pool import db_conn
    from app.main import app
    from app.services.auth import get_current_user
    other = str(uuid4())
    with db_conn() as conn:
        conn.execute("INSERT INTO users (id, username, name, password_hash, invite_code, language) VALUES (%s, %s, 'Dewi Lain', 'x', 'TEST', 'id')",
                     (other, f"lain_{uuid4().hex}"))
        category = conn.execute("INSERT INTO categories (user_id, name, kind, kakeibo_type) VALUES (%s, 'Kopi Lain', 'expense', 'want') RETURNING id",
                                (other,)).fetchone()["id"]
        conn.execute("INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'Kenari', 'bank', 0)", (other,))
        conn.execute("""INSERT INTO merchant_memory (user_id, match_key, key_kind, notes, category_id, source)
                        VALUES (%s, 'merchant:kopi senja', 'merchant', 'Kopi Senja', %s, 'owner')""", (other, category))
        conn.commit()
    try:
        result = preview(client, "kopi senja 20rb kenari")
        assert result["category_hint"] is None and result["account_hint"] is None
        identity = app.dependency_overrides[get_current_user]
        app.dependency_overrides[get_current_user] = lambda: {"id": other, "name": "Dewi Lain", "currency": "IDR"}
        try:
            theirs = preview(client, "kopi senja 20rb")
        finally:
            app.dependency_overrides[get_current_user] = identity
        assert theirs["category_hint"] == {"category_id": str(category), "label": "Kopi Lain", "pillar": "want"}
    finally:
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (other,))
            conn.commit()


@pytest.mark.parametrize("text", ["", " ", "x" * 201, "kopi\u0007 5rb"])
def test_text_bounds(client, owner, text):
    response = parse(client, text)
    assert response.status_code == 422 and response.json()["detail"]["code"] == "invalid_capture_text"


def test_naive_device_time_is_refused(client, owner):
    response = client.post("/api/capture/parse", json={"text": "kopi 5rb", "device_time": "2026-11-03T15:00:00"})
    assert response.status_code == 422


def test_own_rate_limit_leaves_the_capture_budget(client, owner, monkeypatch):
    from app.services import capture_preview
    monkeypatch.setattr(capture_preview, "PARSE_RATE_LIMIT", 120)
    for _ in range(120):
        assert parse(client, "kopi 5rb").status_code == 200
    limited = parse(client, "kopi 5rb")
    assert limited.status_code == 429 and limited.json()["detail"]["code"] == "capture_rate_limited"
    assert send(client, "bayar parkir 5rb").status_code == 200


def test_preview_logs(client, owner, caplog):
    caplog.set_level(logging.DEBUG)
    preview(client, "kopi rahasiaku 123rb tunai")
    preview(client, "bayar rahasiaku")
    messages = [record.getMessage() for record in caplog.records if "Capture preview" in record.getMessage()]
    assert messages == [
        f"Capture preview user={owner['user_id']} outcome=read problems=none",
        f"Capture preview user={owner['user_id']} outcome=problem problems=missing_amount",
    ]
    for message in [record.getMessage() for record in caplog.records]:
        # The owner's random UUID may itself contain "123"; only the typed text must never reach the log.
        typed = message.replace(owner["user_id"], "")
        assert "rahasiaku" not in typed.casefold() and "123" not in typed
