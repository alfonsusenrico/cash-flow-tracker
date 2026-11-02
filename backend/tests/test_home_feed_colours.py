"""Home feed top-up marker and the default colour of new accounts, on a fresh synthetic owner."""

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest


class UnlimitedLimiter:
    def exceeded(self, *_args) -> bool:
        return False

    def reset(self, *_keys) -> None:
        pass


@pytest.fixture(autouse=True)
def unlimited_rate_limits(monkeypatch):
    import app.routers.auth as auth_router

    monkeypatch.setattr(auth_router, "rate_limiter", UnlimitedLimiter())


@pytest.fixture
def owner(client):
    """A browser session for a new owner, so the home feed shows only this test's rows."""
    from app.db.pool import db_conn
    from app.main import app
    from passlib.hash import bcrypt

    username = f"home_feed_{uuid4().hex[:12]}"
    password = "synthetic-pass-1"
    with db_conn() as conn:
        user_id = conn.execute(
            "INSERT INTO users (username, password_hash, invite_code) VALUES (%s, %s, 'TEST') RETURNING id",
            (username, bcrypt.hash(password)),
        ).fetchone()["id"]
        conn.commit()
    session = TestClient(app, base_url="https://testserver")
    session.headers["Origin"] = "https://testserver"
    assert session.post("/api/auth/login", json={"username": username, "password": password}).status_code == 200
    yield session
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()


def create_account(session, **payload):
    body = {"name": f"Akun contoh {uuid4().hex[:8]}", "type": "bank", **payload}
    response = session.post("/api/accounts", json=body)
    assert response.status_code == 200, response.text
    return response.json()["account"]


def record_topup(session, source, target, minutes_ago):
    response = session.post("/api/investment-topups", json={
        "source_account_id": source, "target_account_id": target, "amount": 50_000,
        "date": (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat(),
        "notes": "", "idempotency_key": str(uuid4()),
    })
    assert response.status_code == 200, response.text
    return response.json()["topup"]["id"]


@pytest.fixture
def portfolio(owner):
    funding = create_account(owner, initial_balance=2_000_000)["id"]
    platform = create_account(owner, type="investment")["id"]
    fund = create_account(owner, type="investment", parent_id=platform, instrument_type="mutual_fund", initial_balance=500_000)["id"]
    return funding, fund


def recent(session):
    response = session.get("/api/dashboard/overview")
    assert response.status_code == 200, response.text
    return response.json()["recent_transactions"]


@contextmanager
def counted_statements(monkeypatch):
    """Count every statement the dashboard router sends through its own connections."""
    import app.routers.dashboard as dashboard
    from app.db.pool import db_conn

    statements: list[str] = []

    class CountingCursor:
        def __init__(self, cursor):
            self._cursor = cursor

        def execute(self, query, *args, **kwargs):
            statements.append(str(query))
            self._cursor.execute(query, *args, **kwargs)
            return self

        def __getattr__(self, name):
            return getattr(self._cursor, name)

        def __iter__(self):
            return iter(self._cursor)

        def __enter__(self):
            self._cursor.__enter__()
            return self

        def __exit__(self, *exc):
            return self._cursor.__exit__(*exc)

    class CountingConnection:
        def __init__(self, conn):
            self._conn = conn

        def cursor(self, *args, **kwargs):
            return CountingCursor(self._conn.cursor(*args, **kwargs))

        def execute(self, query, *args, **kwargs):
            statements.append(str(query))
            return self._conn.execute(query, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._conn, name)

    @contextmanager
    def counting_db_conn():
        with db_conn() as conn:
            yield CountingConnection(conn)

    monkeypatch.setattr(dashboard, "db_conn", counting_db_conn)
    yield statements
    monkeypatch.setattr(dashboard, "db_conn", db_conn)


def test_home_feed_marks_topup_legs_and_leaves_other_rows_unmarked(owner, portfolio):
    funding, fund = portfolio
    expense = owner.post("/api/transactions", json={
        "account_id": funding, "type": "expense", "amount": 15_000, "notes": "Kopi contoh",
        "date": (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat(),
    })
    assert expense.status_code == 200, expense.text
    cash = create_account(owner, type="cash")["id"]
    movement = owner.post("/api/movements", json={
        "source_account_id": funding, "target_account_id": cash, "amount": 20_000, "notes": "Tarik tunai contoh",
        "date": (datetime.now(timezone.utc) - timedelta(minutes=20)).isoformat(),
    })
    assert movement.status_code == 200, movement.text
    topup_id = record_topup(owner, funding, fund, minutes_ago=10)

    rows = recent(owner)
    topup_rows = [row for row in rows if row["investment_topup_id"]]
    assert {row["account_id"] for row in topup_rows} == {funding, fund}
    assert {row["investment_topup_id"] for row in topup_rows} == {topup_id}
    others = [row for row in rows if not row["investment_topup_id"]]
    # The ordinary expense and both Pindah Saldo legs carry the field as null, never a missing key.
    assert len(others) == 3
    assert all("investment_topup_id" in row and row["investment_topup_id"] is None for row in others)


def test_home_feed_marker_adds_no_query_per_row(owner, portfolio, monkeypatch):
    funding, fund = portfolio
    record_topup(owner, funding, fund, minutes_ago=40)
    with counted_statements(monkeypatch) as statements:
        assert len([row for row in recent(owner) if row["investment_topup_id"]]) == 2
        with_one = len(statements)

    for minutes_ago in (30, 20, 10):
        record_topup(owner, funding, fund, minutes_ago=minutes_ago)
    with counted_statements(monkeypatch) as statements:
        assert len([row for row in recent(owner) if row["investment_topup_id"]]) == 8
        with_four = len(statements)

    assert with_one > 0
    assert with_four == with_one


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"type": "investment"}, "#8B5CF6"),
        ({"type": "investment", "color": None}, "#8B5CF6"),
        ({"type": "investment", "instrument_type": "mutual_fund"}, "#8B5CF6"),
        ({"type": "bank"}, "#3b82f6"),
        ({"type": "cash", "color": None}, "#3b82f6"),
        ({"type": "investment", "color": "#059669"}, "#059669"),
    ],
)
def test_new_account_colour_defaults_by_type_and_keeps_a_chosen_colour(owner, payload, expected):
    account = create_account(owner, **payload)
    assert account["color"] == expected
    listed = {item["id"]: item for item in owner.get("/api/accounts").json()["accounts"]}
    assert listed[account["id"]]["color"] == expected


def test_investment_pocket_gets_the_lavender_default(owner):
    platform = create_account(owner, type="investment", color="#2563EB")["id"]
    pocket = create_account(owner, type="investment", parent_id=platform, instrument_type="mutual_fund")
    assert pocket["color"] == "#8B5CF6"


def test_existing_account_colours_are_not_rewritten(owner):
    from app.db.pool import db_conn

    legacy = create_account(owner, type="investment", color="#3b82f6")
    with db_conn() as conn:
        stored = conn.execute("SELECT color FROM accounts WHERE id = %s", (legacy["id"],)).fetchone()["color"]
    assert stored == "#3b82f6"
    renamed = owner.patch(f"/api/accounts/{legacy['id']}", json={"name": f"Akun lama {uuid4().hex[:6]}"})
    assert renamed.status_code == 200, renamed.text
    listed = {item["id"]: item for item in owner.get("/api/accounts").json()["accounts"]}
    assert listed[legacy["id"]]["color"] == "#3b82f6"
