"""Essential monthly cost and Ketahanan Dana coverage; synthetic names and amounts only."""
from datetime import date, datetime, time, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from app.services.essential_cost import EssentialCost, coverage, median_rupiah
from app.services.report_windows import shift_month

JAKARTA = ZoneInfo("Asia/Jakarta")


@pytest.fixture
def household(client):
    from app.db.pool import db_conn
    from app.main import app
    from app.services.auth import get_current_user

    user_id = str(uuid4())
    ids = {name: str(uuid4()) for name in (
        "cash", "jago", "stocks", "groceries", "hobby", "repair", "shared", "excluded", "salary", "reimbursement",
    )}
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO users (id, username, name, password_hash, invite_code) VALUES (%s, %s, 'Sari Contoh', 'synthetic-hash', 'TEST')",
            (user_id, f"essential_{uuid4().hex}"),
        )
        for key, name, kind, balance in (
            ("cash", "Kas Contoh", "bank", 20_000_000), ("jago", "Bank Contoh", "bank", 0), ("stocks", "Saham Contoh", "investment", 50_000_000),
        ):
            conn.execute(
                "INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, %s, %s, %s)",
                (ids[key], user_id, name, kind, balance),
            )
        for key, name, kind, pillar, budget, offsets, excluded in (
            ("groceries", "Belanja Dapur Contoh", "expense", "need", None, False, False),
            ("hobby", "Hobi Contoh", "expense", "want", None, False, False),
            ("repair", "Perbaikan Contoh", "expense", "unexpected", None, False, False),
            ("shared", "Makan Bersama Contoh", "expense", "want", None, False, False),
            ("excluded", "Titipan Contoh", "expense", "need", None, False, True),
            ("salary", "Gaji Contoh", "income", None, None, False, False),
            ("reimbursement", "Patungan & Ganti Uang", "income", None, None, True, False),
        ):
            conn.execute(
                """INSERT INTO categories (id, user_id, name, kind, kakeibo_type, monthly_budget, offsets_spending, is_excluded_from_budget)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (ids[key], user_id, name, kind, pillar, budget, offsets, excluded),
            )
        conn.commit()
    app.dependency_overrides[get_current_user] = lambda: {
        "id": user_id, "username": "essential-owner", "name": "Sari Contoh", "currency": "IDR",
        "payday_day": 25, "emergency_fund_multiplier": 6,
    }
    yield {"user_id": user_id, **ids}
    app.dependency_overrides.clear()
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE id = %s", (user_id,))
        conn.commit()


def month_day(months_back: int, day: int, hour: int = 12) -> datetime:
    today = datetime.now(JAKARTA).date()
    year, month = shift_month(today.year, today.month, -months_back)
    return datetime.combine(date(year, month, day), time(hour), JAKARTA)


def insert(owner, category, amount, when, *, tx_type="expense", account="cash", pillar=None, offset=None, obligation=None):
    from app.db.pool import db_conn

    with db_conn() as conn:
        row = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, notes, kakeibo_type,
                   offset_category_id, obligation_id)
               VALUES (%s, %s, %s, %s, %s, %s, 'Transaksi contoh', %s, %s, %s) RETURNING id""",
            (owner["user_id"], owner[account], owner[category] if category else None, tx_type, amount, when, pillar,
             owner[offset] if offset else None, obligation),
        ).fetchone()
        conn.commit()
    return str(row["id"])


def start_tracking(owner, months_back: int):
    """The first recorded transaction, on the first day of a month, so that month counts."""
    insert(owner, "salary", 1_000_000, month_day(months_back, 1, hour=8), tx_type="income")


def estimate(owner):
    from app.db.pool import db_conn
    from app.services.essential_cost import estimate_essential_cost

    with db_conn() as conn:
        with conn.cursor() as cur:
            return estimate_essential_cost(cur, owner["user_id"])


def runway(client):
    response = client.get("/api/dashboard/net-worth")
    assert response.status_code == 200, response.text
    return response.json()["runway"]


def test_median_rounds_half_up_and_rejects_empty():
    assert median_rupiah([3, 1, 2]) == 2
    assert median_rupiah([1_000_001, 1_000_002]) == 1_000_002
    assert median_rupiah([2_000_000, 3_000_000]) == 2_500_000
    with pytest.raises(ValueError):
        median_rupiah([])


def test_a_huge_unexpected_month_does_not_move_the_essential_cost(client, household):
    start_tracking(household, 4)
    for months_back in (1, 2, 3, 4):
        insert(household, "groceries", 2_000_000, month_day(months_back, 10))
        insert(household, "hobby", 700_000, month_day(months_back, 12))
    insert(household, "repair", 30_000_000, month_day(2, 15))
    insert(household, "groceries", 900_000, month_day(2, 16), pillar="unexpected")

    result = estimate(household)
    assert result.method == "median_need_periods"
    assert [item["amount"] for item in result.periods] == [2_000_000] * 4
    assert result.amount == 2_000_000
    assert result.limited_history is False
    assert [item["kind"] for item in result.periods] == ["month"] * 4


def test_a_large_need_one_off_moves_only_its_own_period(client, household):
    start_tracking(household, 5)
    for months_back, amount in ((1, 2_000_000), (2, 2_100_000), (3, 1_900_000), (4, 2_000_000), (5, 2_000_000)):
        insert(household, "groceries", amount, month_day(months_back, 10))
    insert(household, "groceries", 23_000_000, month_day(3, 20))

    result = estimate(household)
    assert sorted(item["amount"] for item in result.periods) == [2_000_000, 2_000_000, 2_000_000, 2_100_000, 24_900_000]
    assert result.amount == 2_000_000


def test_limited_history_is_labelled_and_periods_before_tracking_never_count(client, household):
    # Tracking started on the 5th two months ago: that month is incomplete history and is left out.
    insert(household, "salary", 1_000_000, month_day(2, 5), tx_type="income")
    insert(household, "groceries", 9_000_000, month_day(2, 20))
    insert(household, "groceries", 1_500_000, month_day(1, 10))

    result = estimate(household)
    assert len(result.periods) == 1
    assert result.limited_history is True
    assert result.amount == 1_500_000
    described = result.describe()
    assert described["basis"] == "history" and described["period_count"] == 1
    assert described["periods_used"][0]["gross_need"] == 1_500_000


def test_need_budget_is_the_fallback_and_unknown_is_null(client, household):
    from app.db.pool import db_conn

    insert(household, "groceries", 400_000, datetime.now(JAKARTA) - timedelta(minutes=5))
    unknown = estimate(household)
    assert (unknown.amount, unknown.method) == (None, "unknown")
    coverage_unknown = runway(client)
    assert coverage_unknown["coverage_months"] is None and coverage_unknown["runway_days"] is None
    assert coverage_unknown["runway_months"] is None and coverage_unknown["status"] == "unknown"
    assert coverage_unknown["monthly_primary_expense"] is None
    assert coverage_unknown["essential_cost"]["basis"] == "unknown"

    with db_conn() as conn:
        conn.execute("UPDATE categories SET monthly_budget = 1500000 WHERE id = %s", (household["groceries"],))
        conn.execute("UPDATE categories SET monthly_budget = 9000000 WHERE id = %s", (household["hobby"],))
        conn.execute("UPDATE categories SET monthly_budget = 7000000 WHERE id = %s", (household["excluded"],))
        conn.commit()
    budget = estimate(household)
    assert (budget.amount, budget.method, budget.describe()["basis"]) == (1_500_000, "need_budget", "budget")


def test_reimbursements_are_apportioned_to_the_need_share(client, household):
    start_tracking(household, 1)
    insert(household, "groceries", 1_000_000, month_day(1, 3))
    insert(household, "shared", 100_000, month_day(1, 4), pillar="need")
    insert(household, "shared", 300_000, month_day(1, 5))
    insert(household, "reimbursement", 200_000, month_day(1, 6), tx_type="income", offset="shared")

    period = estimate(household).periods[0]
    # 200k over need 100k and want 300k: need is reduced by 50k.
    assert (period["gross_need"], period["reimbursement_offset"], period["amount"]) == (1_100_000, 50_000, 1_050_000)


def test_debt_is_counted_once_through_minimums(client, household):
    from app.db.pool import db_conn

    with db_conn() as conn:
        debt_id = str(conn.execute(
            """INSERT INTO obligations (user_id, name, total_amount, remaining_amount, minimum_payment)
               VALUES (%s, 'Cicilan Contoh', 6000000, 4000000, 500000) RETURNING id""",
            (household["user_id"],),
        ).fetchone()["id"])
        paid_id = str(conn.execute(
            """INSERT INTO obligations (user_id, name, total_amount, remaining_amount, minimum_payment, is_archived)
               VALUES (%s, 'Lunas Contoh', 1000000, 0, 300000, TRUE) RETURNING id""",
            (household["user_id"],),
        ).fetchone()["id"])
        conn.commit()
    start_tracking(household, 3)
    for months_back in (1, 2, 3):
        insert(household, "groceries", 2_000_000, month_day(months_back, 10))
        insert(household, "groceries", 500_000, month_day(months_back, 25), obligation=debt_id)
    allocated = insert(household, "groceries", 700_000, month_day(2, 26))
    with db_conn() as conn:
        conn.execute(
            "INSERT INTO transaction_obligation_allocations (transaction_id, obligation_id, amount) VALUES (%s, %s, 700000)",
            (allocated, paid_id),
        )
        conn.commit()

    result = estimate(household)
    assert [item["amount"] for item in result.periods] == [2_000_000] * 3
    assert (result.living_amount, result.debt_minimum, result.amount) == (2_000_000, 500_000, 2_500_000)


def test_movements_investments_and_excluded_categories_are_not_essential(client, household):
    from app.db.pool import db_conn

    start_tracking(household, 1)
    insert(household, "groceries", 1_000_000, month_day(1, 3))
    insert(household, "excluded", 800_000, month_day(1, 4))
    insert(household, "groceries", 600_000, month_day(1, 5), account="stocks")
    leg = insert(household, "groceries", 900_000, month_day(1, 6))
    with db_conn() as conn:
        conn.execute("UPDATE transactions SET movement_id = %s WHERE id = %s", (str(uuid4()), leg))
        conn.commit()
    assert estimate(household).periods[0]["amount"] == 1_000_000


def test_salary_cycles_are_periods_and_earlier_months_fill_the_rest(client, household):
    start_tracking(household, 6)
    for months_back in (4, 5, 6):
        insert(household, "groceries", 1_000_000, month_day(months_back, 10))
    receipts = [month_day(months_back, 25, hour=10) for months_back in (3, 2, 1)]
    for receipt in receipts:
        transaction_id = insert(household, "salary", 5_000_000, receipt, tx_type="income")
        response = client.post("/api/salary/receipts", json={"transaction_id": transaction_id})
        assert response.status_code == 200, response.text
    insert(household, "groceries", 3_000_000, receipts[0] + timedelta(days=2))
    insert(household, "groceries", 3_200_000, receipts[1] + timedelta(days=2))
    insert(household, "groceries", 1_000_000, receipts[0] - timedelta(days=3))

    result = estimate(household)
    kinds = [item["kind"] for item in result.periods]
    assert kinds[:2] == ["cycle", "cycle"]
    assert datetime.fromisoformat(result.periods[0]["start"]) == receipts[1]
    assert datetime.fromisoformat(result.periods[1]["start"]) == receipts[0]
    assert [item["amount"] for item in result.periods[:2]] == [3_200_000, 3_000_000]
    # Calendar months before the first confirmed salary complete the six periods.
    assert all(kind == "month" for kind in kinds[2:])
    assert len(result.periods) == 5
    first_cycle_start = receipts[0]
    assert all(datetime.fromisoformat(item["end"]) < first_cycle_start for item in result.periods[2:])
    assert result.cycle_status == "confirmed"


def test_coverage_never_invents_a_value():
    known = EssentialCost(5_000_000, 5_000_000, 0, "need_budget")
    assert coverage(30_000_000, known, 6)["coverage_months"] == 6.0
    assert coverage(30_000_000, known, 6)["runway_days"] == 182
    assert coverage(0, known, 6)["status"] == "zero"
    assert coverage(-1, known, 6)["coverage_months"] == 0.0
    unknown = coverage(30_000_000, EssentialCost(None, None, 0, "unknown"), 6)
    assert unknown["coverage_months"] is None and unknown["runway_days"] is None and unknown["status"] == "unknown"
    assert unknown["coverage_unavailable_reason"] == "essential_cost_unknown"


def test_coverage_reports_its_balance_basis(client, household):
    from app.db.pool import db_conn

    start_tracking(household, 1)
    insert(household, "groceries", 2_000_000, month_day(1, 10))
    without_goal = runway(client)
    # 20M liquid cash; the investment account is not a reserve.
    assert without_goal["balance_basis"] == "all_liquid"
    assert without_goal["emergency_fund_balance"] == 20_000_000 - 2_000_000 + 1_000_000
    assert without_goal["coverage_months"] == round((19_000_000) / 2_000_000, 1)
    assert without_goal["target_months"] == 6.0

    with db_conn() as conn:
        conn.execute(
            """INSERT INTO goals (user_id, name, target_amount, current_amount, is_emergency)
               VALUES (%s, 'Cadangan Contoh', 30000000, 4000000, TRUE)""",
            (household["user_id"],),
        )
        conn.commit()
    with_goal = runway(client)
    assert with_goal["balance_basis"] == "emergency_goals" and with_goal["is_flagged"] is True
    assert (with_goal["emergency_fund_balance"], with_goal["coverage_months"], with_goal["runway_days"]) == (4_000_000, 2.0, 61)
    assert with_goal["status"] == "critical"
    assert with_goal["primary_spent_30d"] is None

    overview = client.get("/api/dashboard/overview?timeframe=month").json()["kpis"]
    assert overview["coverage_months"] == 2.0 and overview["emergency_balance_basis"] == "emergency_goals"
    assert overview["essential_cost"]["method"] == "median_need_periods"
