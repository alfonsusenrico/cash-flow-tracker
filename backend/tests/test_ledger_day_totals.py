"""Transaksi list contract of redesign-transactions-workspace: day totals, tab counts, local days, stable
pages and the new tab and waiting filters.

All names, texts and amounts are synthetic. The owner of `notification_owner` is "Raka Purnama".
"""
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from test_ingestion_memory_and_purpose import open_question, owner  # noqa: F401 - fixtures
from test_notification_processing_database import notification_owner  # noqa: F401
from test_review_queue import manual_movement
from test_topup_learning import convert, funds, rdn_debit  # noqa: F401

JAKARTA = timezone(timedelta(hours=7))


def db():
    from app.db.pool import db_conn
    return db_conn()


def insert(owner, amount, when, *, kind="expense", category=None, account=None, review="reviewed", created=None,
           offset_category=None):
    tx_id = str(uuid4())
    with db() as conn:
        conn.execute(
            """INSERT INTO transactions (id, user_id, account_id, category_id, type, amount, notes, date, source,
                                         review_status, created_at, offset_category_id)
               VALUES (%s, %s, %s, %s, %s, %s, 'Contoh', %s, 'manual', %s, %s, %s)""",
            (tx_id, owner["user_id"], account or owner["bca"], category or owner["expense"], kind, amount, when,
             review, created or when, offset_category),
        )
        conn.commit()
    return tx_id


def listing(client, query):
    response = client.get(f"/api/transactions?{query}")
    assert response.status_code == 200, response.text
    return response.json()


def all_pages(client, query, limit):
    pages, offset = [], 0
    while True:
        page = listing(client, f"{query}&limit={limit}&offset={offset}")
        pages.append(page)
        offset += limit
        if offset >= page["total"]:
            return pages


def merged_days(pages):
    """Day totals of every page; a day split across pages must carry the same figures on each."""
    days = {}
    for page in pages:
        for day in page["daily_totals"]:
            assert days.setdefault(day["date"], day) == day, day["date"]
    return days


def at(day, hour, minute=0):
    return datetime(2026, 10, day, hour, minute, tzinfo=JAKARTA)


@pytest.fixture
def ledger(client, owner):
    """Three days of rows: spending, an income, a reimbursement of part of the spending, a waiting row."""
    with db() as conn:
        refund_category = str(uuid4())
        conn.execute("INSERT INTO categories (id, user_id, name, kind, offsets_spending) VALUES (%s, %s, 'Penggantian Contoh', 'income', TRUE)",
                     (refund_category, owner["user_id"]))
        conn.commit()
    rows = {
        "a": insert(owner, 30_000, at(20, 9)),
        "b": insert(owner, 45_000, at(20, 12)),
        "c": insert(owner, 12_000, at(20, 18)),
        "d": insert(owner, 500_000, at(21, 8), kind="income", category=owner["income"]),
        "e": insert(owner, 20_000, at(21, 10), category=owner["belanja"]),
        "refund": insert(owner, 10_000, at(21, 11), kind="income", category=refund_category, offset_category=owner["belanja"]),
        "waiting": insert(owner, 60_000, at(22, 7), review="needs_review"),
    }
    return {**owner, "rows": rows}


def test_day_totals_add_up_to_the_summary(client, ledger):
    window = "from_date=2026-10-20T00:00:00%2B07:00&to_date=2026-10-22T23:59:59%2B07:00&include=daily_totals"
    pages = all_pages(client, window, limit=2)
    days = merged_days(pages)
    period = pages[0]["summary"]["period"]
    assert sum(day["inflow"] for day in days.values()) == period["inflow"] == 500_000
    assert sum(day["outflow"] for day in days.values()) == period["gross_outflow"] == 167_000
    # The KPI is net of the reimbursement; day headings stay gross because an offset is capped per category.
    assert period["outflow"] == period["gross_outflow"] - period["reimbursement_offset"] == 157_000
    assert sum(day["count"] for day in days.values()) == pages[0]["total"] == 7


def test_split_day_carries_the_whole_day_on_each_page(client, ledger):
    window = "from_date=2026-10-20T00:00:00%2B07:00&to_date=2026-10-20T23:59:59%2B07:00&include=daily_totals"
    first, second = listing(client, f"{window}&limit=2&offset=0"), listing(client, f"{window}&limit=2&offset=2")
    assert [len(first["transactions"]), len(second["transactions"])] == [2, 1]
    assert first["daily_totals"] == second["daily_totals"] == [
        {"date": "2026-10-20", "count": 3, "inflow": 0, "outflow": 87_000, "movements": 0, "waiting_count": 0, "waiting_amount": 0},
    ]


def test_movement_counted_once(client, owner):
    manual_movement(client, owner, amount=40_000)
    page = listing(client, "include=daily_totals&limit=50")
    (day,) = page["daily_totals"]
    assert day["movements"] == 40_000 and day["count"] == 2
    assert day["inflow"] == day["outflow"] == 0


def test_local_day_boundary(client, owner):
    late = insert(owner, 15_000, datetime(2026, 10, 2, 23, 30, tzinfo=JAKARTA))
    early = insert(owner, 25_000, datetime(2026, 10, 3, 0, 30, tzinfo=JAKARTA))
    page = listing(client, "include=daily_totals&limit=50")
    local = {row["id"]: row["local_date"] for row in page["transactions"]}
    assert local == {late: "2026-10-02", early: "2026-10-03"}
    assert {day["date"]: day["outflow"] for day in page["daily_totals"]} == {"2026-10-03": 25_000, "2026-10-02": 15_000}


def test_waiting_row_is_counted_in_its_day(client, ledger):
    page = listing(client, "from_date=2026-10-22T00:00:00%2B07:00&include=daily_totals&limit=50")
    (day,) = page["daily_totals"]
    assert day == {"date": "2026-10-22", "count": 1, "inflow": 0, "outflow": 60_000, "movements": 0,
                   "waiting_count": 1, "waiting_amount": 60_000}


def test_tab_counts(client, ledger, owner):
    manual_movement(client, owner, amount=40_000)
    page = listing(client, "include=type_counts&limit=1")
    assert page["type_counts"] == {"all": 9, "expense": 5, "income": 2, "movement": 2, "investment": 0}
    assert page["type_counts"]["all"] == page["total"]
    # The tab filter itself does not change the counts; every other filter does.
    assert listing(client, "include=type_counts&type=expense&limit=1")["type_counts"] == page["type_counts"]
    assert listing(client, "include=type_counts&review_status=needs_review&limit=1")["type_counts"]["expense"] == 1


def test_only_the_owners_rows(client, ledger, notification_owner):
    from app.main import app
    from app.services.auth import get_current_user
    other = str(uuid4())
    with db() as conn:
        conn.execute("INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Dewi Lain', 'x', 'TEST')",
                     (other, f"lain_{uuid4().hex}"))
        account = conn.execute("INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'Dompet Lain', 'cash', 0) RETURNING id",
                               (other,)).fetchone()["id"]
        conn.execute("INSERT INTO transactions (user_id, account_id, type, amount, date, source, review_status) VALUES (%s, %s, 'expense', 99000, %s, 'manual', 'needs_review')",
                     (other, account, at(20, 10)))
        conn.commit()
    try:
        page = listing(client, "from_date=2026-10-20T00:00:00%2B07:00&to_date=2026-10-20T23:59:59%2B07:00&include=daily_totals,type_counts&limit=50")
        assert page["daily_totals"][0]["outflow"] == 87_000
        assert page["type_counts"]["all"] == 3
        identity = app.dependency_overrides[get_current_user]
        app.dependency_overrides[get_current_user] = lambda: {"id": other, "name": "Dewi Lain", "currency": "IDR"}
        try:
            theirs = listing(client, "include=daily_totals,type_counts&waiting=true&limit=50")
        finally:
            app.dependency_overrides[get_current_user] = identity
        assert theirs["daily_totals"] == [{"date": "2026-10-20", "count": 1, "inflow": 0, "outflow": 99_000, "movements": 0,
                                           "waiting_count": 1, "waiting_amount": 99_000}]
    finally:
        with db() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (other,))
            conn.commit()


def test_unknown_include_value(client, owner):
    response = client.get("/api/transactions?include=daily_totals,everything")
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "unknown_include"


def test_stable_order_across_pages(client, owner):
    when = at(15, 10)
    ids = {insert(owner, 1_000, when, created=when) for _ in range(120)}
    seen = [row["id"] for page in all_pages(client, "include=daily_totals", limit=50) for row in page["transactions"]]
    assert len(seen) == 120 and set(seen) == ids


def test_response_without_include_keeps_its_shape(client, ledger):
    page = listing(client, "limit=5")
    assert "daily_totals" not in page and "type_counts" not in page
    assert set(page) == {"ok", "total", "limit", "offset", "summary", "transactions"}
    assert all(row["local_date"] for row in page["transactions"])


def test_waiting_filter_matches_the_badge(client, ledger, owner):
    event_id, question = open_question(client, owner)
    awaiting_send = insert(owner, 70_000, at(23, 9), category=owner["internal_out"], review="needs_review")
    summary = client.get("/api/review/summary").json()
    page = listing(client, "waiting=true&limit=200")
    assert page["total"] == summary["attention"] == 3
    waiting_ids = {row["id"] for row in page["transactions"]}
    assert ledger["rows"]["waiting"] in waiting_ids and awaiting_send in waiting_ids


def test_investment_tab_after_a_conversion(client, funds):
    debit = rdn_debit(client, datetime(2026, 10, 5, 3, 0, tzinfo=timezone.utc))
    assert convert(client, debit, funds["a"]).status_code == 200
    investment = listing(client, "type=investment&include=type_counts&limit=50")
    assert {row["id"] for row in investment["transactions"]} >= {debit}
    assert all(row["movement_kind"] == "investment_topup" for row in investment["transactions"])
    assert investment["type_counts"]["investment"] == investment["total"] == 2
    # Top-up legs never appear under Pindah saldo.
    assert listing(client, "type=movement&limit=50")["total"] == 0


def test_movement_filter_excludes_topup_legs(client, funds):
    legs = manual_movement(client, funds, amount=40_000)
    debit = rdn_debit(client, datetime(2026, 10, 6, 3, 0, tzinfo=timezone.utc))
    assert convert(client, debit, funds["b"]).status_code == 200
    movement = listing(client, "type=movement&limit=50")
    assert {row["id"] for row in movement["transactions"]} == set(legs)


def test_transfer_filter_is_unchanged(client, funds):
    legs = manual_movement(client, funds, amount=40_000)
    debit = rdn_debit(client, datetime(2026, 10, 7, 3, 0, tzinfo=timezone.utc))
    assert convert(client, debit, funds["a"]).status_code == 200
    insert(funds, 5_000, at(8, 9))
    legacy = """(t.type = 'transfer' OR c.name IN ('Internal Movement', 'Investasi') OR c.is_excluded_from_budget = true)"""
    with db() as conn:
        expected = {str(r["id"]) for r in conn.execute(
            f"SELECT t.id FROM transactions t LEFT JOIN categories c ON c.id = t.category_id WHERE t.user_id = %s AND {legacy}",
            (funds["user_id"],)).fetchall()}
    got = {row["id"] for row in listing(client, "type=transfer&limit=200")["transactions"]}
    assert got == expected and set(legs) <= got and debit in got


def test_statement_count_is_fixed(client, owner, monkeypatch):
    from contextlib import contextmanager
    from app.routers import transactions as router
    statements = []
    real = router.db_conn

    class Cursor:
        def __init__(self, cur):
            self._cur = cur

        def execute(self, sql, *args, **kwargs):
            statements.append(str(sql))
            return self._cur.execute(sql, *args, **kwargs)

        def __getattr__(self, name):
            return getattr(self._cur, name)

    class Conn:
        def __init__(self, conn):
            self._conn = conn

        @contextmanager
        def cursor(self, *args, **kwargs):
            with self._conn.cursor(*args, **kwargs) as cur:
                yield Cursor(cur)

    @contextmanager
    def recording():
        with real() as conn:
            yield Conn(conn)
    monkeypatch.setattr(router, "db_conn", recording)

    def count(limit):
        statements.clear()
        listing(client, f"include=daily_totals,type_counts&limit={limit}")
        return len(statements)

    insert(owner, 1_000, at(1, 9))
    one = count(1)
    for day in range(2, 28):
        insert(owner, 1_000, at(day, 9))
        insert(owner, 2_000, at(day, 10))
    # summary, two offset lookups (cycle and all time), page, allocations, days, types
    assert one == count(50) == 7


def test_detail_carries_waiting_local_day_and_origin(client, owner):
    from test_notification_processing_database import ingest, notification
    from test_text_capture import captured
    event = ingest(client, notification("You've paid Rp42.000 to Kedai Contoh", seconds=5))["results"][0]
    with db() as conn:
        noted = str(conn.execute("SELECT transaction_id FROM notification_events WHERE id = %s", (event["event_id"],)).fetchone()["transaction_id"])
    detail = client.get(f"/api/transactions/{noted}").json()["transaction"]
    assert detail["origin"]["kind"] == "notification" and "Kedai Contoh" in detail["origin"]["text"]
    assert detail["local_date"] and detail["waiting"] is False

    waiting = insert(owner, 9_000, at(12, 9), review="needs_review")
    detail = client.get(f"/api/transactions/{waiting}").json()["transaction"]
    assert (detail["waiting"], detail["origin"], detail["local_date"]) == (True, None, "2026-10-12")

    from test_text_capture import remember
    remember(owner, "merchant:parkir contoh", category="belanja", account="bca", notes="Parkir Contoh")
    from test_text_capture import answered
    capture = captured(client, "parkir contoh 5rb bca")
    if capture["status"] == "needs_detail":
        # "bca" names two of this owner's accounts (BCA Harian and RDN BCA), so the account is asked.
        assert capture["question"]["type"] == "account", capture["question"]
        capture = answered(client, capture, account_id=owner["bca"])
    assert capture["status"] == "recorded", capture
    typed = capture["transaction"]["id"]
    assert client.get(f"/api/transactions/{typed}").json()["transaction"]["origin"] == {"kind": "capture", "text": "parkir contoh 5rb bca"}


def test_detail_of_another_owners_row_is_not_found(client, ledger):
    from app.main import app
    from app.services.auth import get_current_user
    identity = app.dependency_overrides[get_current_user]
    app.dependency_overrides[get_current_user] = lambda: {"id": str(uuid4()), "name": "Dewi Lain", "currency": "IDR"}
    try:
        assert client.get(f"/api/transactions/{ledger['rows']['a']}").status_code == 404
    finally:
        app.dependency_overrides[get_current_user] = identity
