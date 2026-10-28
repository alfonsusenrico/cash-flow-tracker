from datetime import datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

import psycopg
import pytest


@pytest.fixture
def owner(client):
    from app.main import app
    from app.db.pool import db_conn
    from app.services.auth import get_current_user

    user_id, stranger_id = str(uuid4()), str(uuid4())
    ids = {name: str(uuid4()) for name in (
        "cash", "dinner", "groceries", "misc", "salary", "reimbursement", "movement", "investment", "stranger_dinner",
    )}
    with db_conn() as conn:
        with conn.cursor() as cur:
            for uid in (user_id, stranger_id):
                cur.execute(
                    "INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Dina Contoh', 'synthetic-hash', 'TEST')",
                    (uid, f"reimburse_{uuid4().hex}"),
                )
            cur.execute(
                "INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, 'Kas Contoh', 'bank', 10000000)",
                (ids["cash"], user_id),
            )
            for key, uid, name, kind, pillar, offsets, excluded in [
                ("dinner", user_id, "Makan Bareng", "expense", "want", False, False),
                ("groceries", user_id, "Belanja Dapur", "expense", "need", False, False),
                ("misc", user_id, "Lain-lain", "expense", None, False, False),
                ("salary", user_id, "Gaji", "income", None, False, False),
                ("reimbursement", user_id, "Patungan & Ganti Uang", "income", None, True, False),
                ("movement", user_id, "Internal Movement", "expense", None, False, True),
                ("investment", user_id, "Investasi", "expense", None, False, False),
                ("stranger_dinner", stranger_id, "Makan Bareng", "expense", "want", False, False),
            ]:
                cur.execute(
                    """INSERT INTO categories (id, user_id, name, kind, kakeibo_type, offsets_spending, is_excluded_from_budget)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (ids[key], uid, name, kind, pillar, offsets, excluded),
                )
        conn.commit()
    app.dependency_overrides[get_current_user] = lambda: {
        "id": user_id, "username": "reimburse-owner", "name": "Dina Contoh", "currency": "IDR",
        "payday_day": 25, "monthly_spending_budget": 3_000_000,
    }
    yield {"user_id": user_id, **ids}
    app.dependency_overrides.clear()
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = ANY(%s)", ([user_id, stranger_id],))
        conn.commit()


def record(client, owner, category, amount, *, tx_type="expense", offset=None, expect=200):
    body = {"type": tx_type, "account_id": owner["cash"], "category_id": owner[category], "amount": amount}
    if offset:
        body["offset_category_id"] = owner[offset]
    response = client.post("/api/transactions", json=body)
    assert response.status_code == expect, response.text
    return response.json()


def offset_of(tx_id):
    from app.db.pool import db_conn

    with db_conn() as conn:
        row = conn.execute("SELECT offset_category_id FROM transactions WHERE id = %s", (tx_id,)).fetchone()
    return str(row["offset_category_id"]) if row["offset_category_id"] else None


def shared_dinner_cycle(client, owner):
    record(client, owner, "salary", 5_000_000, tx_type="income")
    record(client, owner, "dinner", 300_000)
    record(client, owner, "groceries", 200_000)
    record(client, owner, "reimbursement", 200_000, tx_type="income", offset="dinner")
    record(client, owner, "reimbursement", 80_000, tx_type="income")


def test_offset_link_is_stored_and_validated(client, owner):
    linked = record(client, owner, "reimbursement", 200_000, tx_type="income", offset="dinner")["transaction_id"]
    assert offset_of(linked) == owner["dinner"]
    detail = client.get(f"/api/transactions/{linked}").json()["transaction"]
    assert detail["is_reimbursement"] is True
    assert detail["offset_category_id"] == owner["dinner"] and detail["offset_category_name"] == "Makan Bareng"

    for category, tx_type, offset, code in [
        ("dinner", "expense", "groceries", "offset_not_allowed"),
        ("salary", "income", "dinner", "offset_not_allowed"),
        ("reimbursement", "income", "stranger_dinner", "invalid_offset_category"),
        ("reimbursement", "income", "salary", "invalid_offset_category"),
        ("reimbursement", "income", "movement", "invalid_offset_category"),
        ("reimbursement", "income", "investment", "invalid_offset_category"),
    ]:
        rejected = record(client, owner, category, 10_000, tx_type=tx_type, offset=offset, expect=422)
        assert rejected["detail"]["code"] == code, (category, offset)

    expense = record(client, owner, "dinner", 10_000)["transaction_id"]
    patched = client.patch(f"/api/transactions/{expense}", json={"offset_category_id": owner["groceries"]})
    assert patched.status_code == 422 and patched.json()["detail"]["code"] == "offset_not_allowed"
    foreign = client.patch(f"/api/transactions/{linked}", json={"offset_category_id": owner["stranger_dinner"]})
    assert foreign.status_code == 422 and foreign.json()["detail"]["code"] == "invalid_offset_category"
    assert offset_of(linked) == owner["dinner"]

    moved_away = client.patch(f"/api/transactions/{linked}", json={"category_id": owner["salary"]})
    assert moved_away.status_code == 200, moved_away.text
    assert offset_of(linked) is None


def test_database_rejects_offset_on_expense(client, owner):
    from app.db.pool import db_conn

    with pytest.raises(psycopg.errors.CheckViolation):
        with db_conn() as conn:
            conn.execute(
                """INSERT INTO transactions (user_id, account_id, category_id, type, amount, offset_category_id,
                       source, review_status, classified_by)
                   VALUES (%s, %s, %s, 'expense', 1000, %s, 'manual', 'reviewed', 'owner')""",
                (owner["user_id"], owner["cash"], owner["dinner"], owner["groceries"]),
            )


def test_categories_used_by_offsets_are_protected(client, owner):
    record(client, owner, "reimbursement", 50_000, tx_type="income", offset="dinner")
    listed = {row["id"]: row for row in client.get("/api/categories").json()["categories"]}
    assert listed[owner["reimbursement"]]["offsets_spending"] is True
    assert listed[owner["salary"]]["offsets_spending"] is False
    assert listed[owner["movement"]]["is_excluded_from_budget"] is True

    assert client.patch(f"/api/categories/{owner['reimbursement']}", json={"kind": "expense"}).status_code == 409
    assert client.patch(f"/api/categories/{owner['dinner']}", json={"kind": "income"}).status_code == 409
    deleted = client.delete(f"/api/categories/{owner['dinner']}")
    assert deleted.status_code == 200 and "archived" in deleted.json()["message"]
    archived = {row["id"]: row for row in client.get("/api/categories?include_archived=true").json()["categories"]}
    assert archived[owner["dinner"]]["is_archived"] is True


def test_dashboard_overview_nets_reimbursements(client, owner):
    shared_dinner_cycle(client, owner)
    overview = client.get("/api/dashboard/overview").json()
    kpis = overview["kpis"]
    assert kpis["total_inflow"] == 5_000_000
    assert kpis["gross_outflow"] == 500_000
    assert kpis["reimbursement_offset"] == 200_000
    assert kpis["total_outflow"] == 300_000
    assert kpis["reimbursement_unallocated"] == 80_000
    assert kpis["reimbursement_excess"] == 0
    assert kpis["net_cashflow"] == 4_700_000
    dinner = next(row for row in overview["categories"] if row["id"] == owner["dinner"])
    assert (dinner["spent"], dinner["gross_spent"], dinner["reimbursed"]) == (100_000, 300_000, 200_000)
    groceries = next(row for row in overview["categories"] if row["id"] == owner["groceries"])
    assert (groceries["spent"], groceries["reimbursed"]) == (200_000, 0)
    kakeibo = overview["kakeibo"]
    assert (kakeibo["want_spent"], kakeibo["need_spent"]) == (100_000, 200_000)
    assert (kakeibo["want_pct"], kakeibo["need_pct"]) == (2.0, 4.0)


def test_analytics_insights_and_ledger_follow_the_same_figures(client, owner):
    shared_dinner_cycle(client, owner)
    analytics = client.get("/api/dashboard/analytics").json()
    comparison = analytics["comparison"]
    assert (comparison["total_inflow"], comparison["total_outflow"]) == (5_000_000, 300_000)
    assert comparison["reimbursement_offset"] == 200_000 and comparison["reimbursement_unallocated"] == 80_000
    dinner = next(row for row in analytics["category_variance"] if row["id"] == owner["dinner"])
    assert dinner["spent"] == 100_000 and dinner["reimbursed"] == 200_000
    assert analytics["kakeibo"]["want_spent"] == 100_000
    assert sum(day["income"] for day in analytics["daily_cadence"]) == 5_000_000

    insights = client.get("/api/insights").json()
    assert (insights["total_income"], insights["total_expense"]) == (5_000_000, 300_000)
    assert next(row for row in insights["categories"] if row["id"] == owner["dinner"])["spent"] == 100_000

    summary = client.get("/api/transactions").json()["summary"]
    assert summary["cycle"]["inflow"] == 5_000_000
    # Phase 6 nets the ledger summary like the dashboard total (phase 2 open decision 6 resolved).
    assert summary["cycle"]["outflow"] == 300_000
    assert summary["cycle"]["gross_outflow"] == 500_000 and summary["cycle"]["reimbursement_offset"] == 200_000


def test_pulse_safe_to_spend_uses_net_cycle_spending(client, owner):
    from app.core.config import settings
    from app.routers.pulse import get_cycle_window

    shared_dinner_cycle(client, owner)
    pulse = client.get("/api/pulse").json()
    assert pulse["cycle_spent"] == 300_000
    assert pulse["cycle_reimbursed"] == 200_000
    assert pulse["today_spent"] == 500_000
    today = datetime.now(ZoneInfo(settings.tz)).date()
    _, _, elapsed, total = get_cycle_window(25, today)
    remaining = max(1, total - elapsed + 1)
    assert pulse["safe_to_spend_today"] == 2_700_000 // remaining


def test_excess_reimbursement_floors_at_zero_and_is_reported(client, owner):
    record(client, owner, "salary", 5_000_000, tx_type="income")
    record(client, owner, "dinner", 100_000)
    record(client, owner, "reimbursement", 150_000, tx_type="income", offset="dinner")
    overview = client.get("/api/dashboard/overview").json()
    kpis = overview["kpis"]
    assert kpis["total_inflow"] == 5_000_000
    assert kpis["total_outflow"] == 0
    assert kpis["reimbursement_offset"] == 100_000 and kpis["reimbursement_excess"] == 50_000
    dinner = next(row for row in overview["categories"] if row["id"] == owner["dinner"])
    assert dinner["spent"] == 0
    assert overview["kakeibo"]["want_spent"] == 0
    assert client.get("/api/pulse").json()["cycle_spent"] == 0


def test_category_without_pillar_reduces_need(client, owner):
    record(client, owner, "salary", 5_000_000, tx_type="income")
    record(client, owner, "groceries", 200_000)
    record(client, owner, "misc", 50_000)
    record(client, owner, "reimbursement", 20_000, tx_type="income", offset="misc")
    kakeibo = client.get("/api/dashboard/overview").json()["kakeibo"]
    assert kakeibo["need_spent"] == 230_000
