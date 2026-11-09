"""One spending basis on Analisis (apply-markas-analisis D31, owner decision 2026-11-08).

The analytics, the calendar and the flow return Pengeluaran beside their Belanja-basis fields; every Pengeluaran
total equals the whole-period comparison's `totals.spending`, and the Belanja-basis fields keep their values.
Synthetic data only.
"""
from test_monthly_report import at, insert, month_start, owners  # noqa: F401

from app.services.calendar import heat_level, heat_thresholds


def seed(owner):
    """Last month: Pengeluaran in three pillars with a reimbursement, and Rp 340.000 of saving-pillar expenses."""
    m1 = month_start(-1)
    insert(owner, "bonus", 1, at(month_start(-2)), tx_type="income")
    insert(owner, "groceries", 300_000, at(m1.replace(day=2)))
    insert(owner, "dinner", 200_000, at(m1.replace(day=3)))
    insert(owner, "dinner", 100_000, at(m1.replace(day=3), 14), pillar="need")
    insert(owner, "reimbursement", 60_000, at(m1.replace(day=4)), tx_type="income", offset="dinner")
    insert(owner, "misc", 45_000, at(m1.replace(day=5)))
    # A category whose pillar is Tabungan, and a row given the Tabungan pillar in another category.
    insert(owner, "saving", 40_000, at(m1.replace(day=7)))
    insert(owner, "groceries", 300_000, at(m1.replace(day=7), 15), pillar="saving")
    return m1


def get(client, path, **params):
    response = client.get(path, params=params)
    assert response.status_code == 200, response.text
    return response.json()


def test_every_pengeluaran_total_equals_the_comparison(client, owners):
    owner, _ = owners
    m1 = seed(owner)
    month = {"timeframe": "month", "cycle_offset": -1}
    spending = get(client, "/api/reports/monthly/comparison", start_date=m1.isoformat())["comparison"]["totals"]["spending"]
    # 300.000 + 200.000 + 100.000 − 60.000 + 45.000; the Rp 340.000 with the Tabungan pillar is not Pengeluaran.
    assert spending == 585_000

    analytics = get(client, "/api/dashboard/analytics", **month)
    assert sum(day["spending"] for day in analytics["daily_cadence"]) == spending
    assert sum(row["spending"] for row in analytics["category_variance"]) == spending
    assert analytics["explanations"]["spending"]["total"] == spending

    calendar = get(client, "/api/dashboard/calendar", **month)
    assert calendar["totals"]["spending"] == spending
    assert sum(day["spending"] for day in calendar["days"]) == spending

    flow = get(client, "/api/dashboard/flow", **month)
    assert flow["summary"]["spending"] == spending


def test_belanja_fields_keep_their_values(client, owners):
    owner, _ = owners
    seed(owner)
    month = {"timeframe": "month", "cycle_offset": -1}
    belanja = 585_000 + 340_000
    analytics = get(client, "/api/dashboard/analytics", **month)
    assert analytics["comparison"]["total_outflow"] == belanja
    assert sum(day["expense"] for day in analytics["daily_cadence"]) == belanja
    assert sum(row["spent"] for row in analytics["category_variance"]) == belanja
    calendar = get(client, "/api/dashboard/calendar", **month)
    assert calendar["totals"]["spend"] == belanja
    assert sum(day["spend"] for day in calendar["days"]) == belanja
    assert get(client, "/api/dashboard/flow", **month)["summary"]["belanja"] == belanja


def test_days_categories_and_levels_on_the_pengeluaran_basis(client, owners):
    owner, _ = owners
    m1 = seed(owner)
    month = {"timeframe": "month", "cycle_offset": -1}
    analytics = get(client, "/api/dashboard/analytics", **month)
    days = {day["date"]: day for day in analytics["daily_cadence"]}
    # Day 3 carries the dinner reimbursement (Rp 60.000 of its Rp 200.000 Keinginan; the Kebutuhan dinner row keeps
    # its own pillar and gets none); day 7 holds only saving-pillar expenses.
    assert days[m1.replace(day=3).isoformat()]["spending"] == 240_000
    assert days[m1.replace(day=7).isoformat()]["spending"] == 0
    assert days[m1.replace(day=7).isoformat()]["expense"] == 340_000
    rows = {row["name"]: row for row in analytics["category_variance"]}
    assert rows["Dana Sosial"]["spending"] == 0 and rows["Dana Sosial"]["spent"] == 40_000
    assert rows["Belanja Dapur"]["spending"] == 300_000 and rows["Belanja Dapur"]["spent"] == 600_000
    assert rows["Makan Bareng"]["spending"] == 240_000

    calendar = get(client, "/api/dashboard/calendar", **month)
    thresholds = heat_thresholds([day["spending"] for day in calendar["days"]])
    assert calendar["spending_heat_thresholds"] == thresholds
    for day in calendar["days"]:
        assert day["spending_heat"] == heat_level(day["spending"], thresholds)
    seventh = next(day for day in calendar["days"] if day["date"] == m1.replace(day=7).isoformat())
    assert seventh["spending"] == 0 and seventh["spending_heat"] == 0 and seventh["heat"] > 0


def test_budget_share_on_the_pengeluaran_basis(client, owners):
    from app.db.pool import db_conn

    owner, _ = owners
    seed(owner)
    with db_conn() as conn:
        conn.execute("UPDATE categories SET monthly_budget = 400000 WHERE id = %s", (owner["groceries"],))
        conn.commit()
    analytics = get(client, "/api/dashboard/analytics", timeframe="month", cycle_offset=-1)
    groceries = next(row for row in analytics["category_variance"] if row["name"] == "Belanja Dapur")
    assert groceries["percentage_used"] == 150.0
    assert groceries["spending_percentage_used"] == 75.0


def test_another_owner_is_never_counted(client, owners):
    owner, stranger = owners
    seed(owner)
    seed(stranger)
    calendar = get(client, "/api/dashboard/calendar", timeframe="month", cycle_offset=-1)
    assert calendar["totals"]["spending"] == 585_000
