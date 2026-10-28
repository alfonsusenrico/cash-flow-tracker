"""Report figures that add up: "Tanpa kategori", calendar months, reimbursement-net series (phase 6).

All names and amounts are synthetic.
"""
from datetime import date, datetime, timedelta, timezone

import pytest

from test_reimbursements import owner, record, shared_dinner_cycle  # noqa: F401 - fixture


def add_expense(owner, amount, *, category=None, when=None):
    from app.db.pool import db_conn
    with db_conn() as conn:
        row = conn.execute(
            """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, source, review_status)
               VALUES (%s, %s, %s, 'expense', %s, %s, 'manual', 'reviewed') RETURNING id""",
            (owner["user_id"], owner["cash"], owner[category] if category else None, amount,
             when or datetime.now(timezone.utc)),
        ).fetchone()
        conn.commit()
    return str(row["id"])


def archive(owner, category):
    from app.db.pool import db_conn
    with db_conn() as conn:
        conn.execute("UPDATE categories SET is_archived = TRUE WHERE id = %s", (owner[category],))
        conn.commit()


def test_breakdown_adds_up_to_total_outflow_with_uncategorised_and_archived(client, owner):
    shared_dinner_cycle(client, owner)
    add_expense(owner, 70_000)
    record(client, owner, "misc", 30_000)
    archive(owner, "misc")
    record(client, owner, "investment", 400_000)
    record(client, owner, "movement", 900_000)

    analytics = client.get("/api/dashboard/analytics").json()
    variance = analytics["category_variance"]
    total = analytics["comparison"]["total_outflow"]
    assert total == 300_000 + 70_000 + 30_000
    assert sum(item["spent"] for item in variance) == total
    bucket = next(item for item in variance if item["is_uncategorized"])
    assert (bucket["id"], bucket["name"], bucket["spent"]) == (None, "Tanpa kategori", 70_000)
    archived = next(item for item in variance if item["id"] == owner["misc"])
    assert archived["is_archived"] is True and archived["spent"] == 30_000
    assert {item["name"] for item in variance}.isdisjoint({"Investasi", "Internal Movement"})

    overview = client.get("/api/dashboard/overview").json()
    assert sum(item["spent"] for item in overview["categories"]) == overview["kpis"]["total_outflow"] == total
    assert next(item for item in overview["categories"] if item["is_uncategorized"])["spent"] == 70_000


def test_series_heatmap_burn_and_ledger_follow_reimbursement_offsets(client, owner):
    shared_dinner_cycle(client, owner)
    analytics = client.get("/api/dashboard/analytics").json()
    total = analytics["comparison"]["total_outflow"]
    assert total == 300_000
    assert sum(day["expense"] for day in analytics["daily_cadence"]) == total
    assert all(day["expense"] >= 0 for day in analytics["daily_cadence"])
    assert sum(day["total_spent"] for day in analytics["day_of_week_heatmap"]) == total
    assert analytics["burn_rate"]["projected_30d_outflow"] == 300_000
    assert analytics["burn_rate"]["daily_burn_7d"] == round(300_000 / 7)

    overview = client.get("/api/dashboard/overview").json()
    trend = overview["cumulative_trendline"]
    assert trend[-1]["cumulative_outflow"] == overview["kpis"]["total_outflow"] == 300_000
    assert sum(day["expense"] for day in overview["burn_cadence"]) == 300_000

    summary = client.get("/api/transactions").json()["summary"]
    assert (summary["cumulative"]["outflow"], summary["cumulative"]["gross_outflow"]) == (300_000, 500_000)
    assert summary["cumulative"]["reimbursement_offset"] == 200_000 and summary["period"] is None
    dinner = client.get(f"/api/transactions?category_id={owner['dinner']}").json()["summary"]["cumulative"]
    assert (dinner["outflow"], dinner["reimbursement_offset"]) == (100_000, 200_000)
    want = client.get("/api/transactions?kakeibo_type=want&type=expense").json()["summary"]["cumulative"]
    assert want["outflow"] == 100_000
    by_account = client.get(f"/api/transactions?account_id={owner['cash']}").json()["summary"]["cumulative"]
    assert (by_account["outflow"], by_account["reimbursement_offset"]) == (500_000, 0)


def test_offset_is_spread_over_the_days_the_category_was_spent(client, owner):
    from app.core.config import settings
    from zoneinfo import ZoneInfo

    now = datetime.now(timezone.utc)
    # Two dinner days in the same report window: 60.000 and 240.000; a 150.000 repayment offsets them 1:4.
    record(client, owner, "salary", 5_000_000, tx_type="income")
    earlier = now - timedelta(days=1)
    add_expense(owner, 60_000, category="dinner", when=earlier)
    add_expense(owner, 240_000, category="dinner", when=now)
    record(client, owner, "reimbursement", 150_000, tx_type="income", offset="dinner")
    analytics = client.get("/api/dashboard/analytics?timeframe=30d").json()
    by_day = {day["date"]: day["expense"] for day in analytics["daily_cadence"]}
    tz = ZoneInfo(settings.tz)
    assert by_day[earlier.astimezone(tz).date().isoformat()] == 30_000
    assert by_day[now.astimezone(tz).date().isoformat()] == 120_000


def test_apportion_is_exact_and_never_exceeds_a_weight():
    from app.services.report_figures import apportion

    assert apportion(100, {"a": 1, "b": 1, "c": 1}) == {"a": 34, "b": 33, "c": 33}
    assert apportion(300, {"a": 100, "b": 200}) == {"a": 100, "b": 200}
    assert apportion(0, {"a": 5}) == {}
    shares = apportion(7, {"a": 3, "b": 3, "c": 3})
    assert sum(shares.values()) == 7 and all(value <= 3 for value in shares.values())


@pytest.mark.parametrize("ref,offset,start,end,label", [
    (date(2026, 2, 10), 0, date(2026, 2, 1), date(2026, 2, 28), "Februari 2026"),
    (date(2026, 1, 31), -1, date(2025, 12, 1), date(2025, 12, 31), "Desember 2025"),
    (date(2024, 11, 5), 3, date(2025, 2, 1), date(2025, 2, 28), "Februari 2025"),
    (date(2024, 2, 29), 0, date(2024, 2, 1), date(2024, 2, 29), "Februari 2024"),
])
def test_calendar_month_window(ref, offset, start, end, label):
    from app.services.report_windows import get_timeframe_window

    start_utc, end_utc, start_date, end_date, window_label = get_timeframe_window(25, "month", ref, cycle_offset=offset)
    assert (start_date, end_date, window_label) == (start, end, label)
    assert start_utc < end_utc


def test_month_timeframe_and_ledger_period_share_one_window(client, owner):
    from app.services.report_windows import get_timeframe_window

    start_utc, end_utc, _, _, label = get_timeframe_window(25, "month", cycle_offset=-1)
    inside = add_expense(owner, 45_000, category="groceries", when=start_utc + timedelta(hours=1))
    add_expense(owner, 99_000, category="groceries", when=start_utc - timedelta(minutes=1))
    add_expense(owner, 11_000, category="groceries", when=end_utc + timedelta(minutes=1))

    analytics = client.get("/api/dashboard/analytics?timeframe=month&cycle_offset=-1").json()
    assert analytics["timeframe"] == "month" and analytics["timeframe_label"] == label
    groceries = next(item for item in analytics["category_variance"] if item["id"] == owner["groceries"])
    assert groceries["spent"] == 45_000
    assert len(analytics["daily_cadence"]) == (end_utc - start_utc).days + 1

    ledger = client.get(f"/api/transactions?period=month&period_offset=-1&category_id={owner['groceries']}").json()
    assert [item["id"] for item in ledger["transactions"]] == [inside]
    period = ledger["summary"]["period"]
    assert (period["basis"], period["offset"], period["label"], period["outflow"]) == ("month", -1, label, 45_000)

    overview = client.get("/api/dashboard/overview?timeframe=month&cycle_offset=-1").json()
    assert overview["timeframe_label"] == label

    clash = client.get("/api/transactions?period=month&from_date=2026-01-01T00:00:00Z")
    assert clash.status_code == 422 and clash.json()["detail"]["code"] == "period_with_dates"
    assert client.get("/api/transactions?period=week").status_code == 422


def test_uncategorized_filter_lists_the_bucket_rows(client, owner):
    loose = add_expense(owner, 70_000)
    add_expense(owner, 20_000, category="groceries")
    listed = client.get("/api/transactions?uncategorized=true").json()
    assert [item["id"] for item in listed["transactions"]] == [loose]
    both = client.get(f"/api/transactions?uncategorized=true&category_id={owner['groceries']}")
    assert both.status_code == 422 and both.json()["detail"]["code"] == "uncategorized_with_category"


def test_fee_rows_and_their_movement_link_each_other(client, owner):
    from app.db.pool import db_conn
    from app.services.movement_pairing import link_pair

    with db_conn() as conn:
        bank = conn.execute(
            "INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'Bank Contoh', 'bank', 0) RETURNING id",
            (owner["user_id"],),
        ).fetchone()["id"]
        legs = {}
        for tx_type, account, amount, category in [
            ("expense", owner["cash"], 102_500, None), ("income", bank, 100_000, None),
        ]:
            legs[tx_type] = str(conn.execute(
                """INSERT INTO transactions (user_id, account_id, category_id, type, amount, source, review_status)
                   VALUES (%s, %s, %s, %s, %s, 'manual', 'reviewed') RETURNING id""",
                (owner["user_id"], account, category, tx_type, amount),
            ).fetchone()["id"])
        with conn.cursor() as cur:
            linked = link_pair(cur, owner["user_id"], legs["expense"], legs["income"], fee=2_500, notes=None)
        conn.commit()

    items = {item["id"]: item for item in client.get("/api/transactions").json()["transactions"]}
    fee = items[linked["fee_transaction_id"]]
    assert fee["fee_movement"] == {
        "movement_id": linked["movement_id"], "source_account_name": "Kas Contoh",
        "target_account_name": "Bank Contoh", "amount": 100_000,
    }
    assert fee["movement_fee"] is None and fee["review_status"] == "auto"
    for leg in legs.values():
        assert items[leg]["movement_fee"] == {"transaction_id": linked["fee_transaction_id"], "amount": 2_500}
        assert items[leg]["fee_movement"] is None
