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


def seed_daily_detail(owner):
    """Last month: every spending bucket, an uncategorised expense, saving-pillar rows and a reimbursed want."""
    m1 = month_start(-1)
    insert(owner, "groceries", 412_000, at(m1.replace(day=2)))
    insert(owner, "dinner", 600_000, at(m1.replace(day=2), 13))
    insert(owner, "dinner", 77_649, at(m1.replace(day=2), 19))
    insert(owner, "reimbursement", 50_000, at(m1.replace(day=4)), tx_type="income", offset="dinner")
    insert(owner, "groceries", 25_000, at(m1.replace(day=5)), pillar="culture")
    insert(owner, "repair", 120_000, at(m1.replace(day=5), 15))
    insert(owner, "misc", 45_000, at(m1.replace(day=6)))
    insert(owner, None, 30_000, at(m1.replace(day=6), 16))
    insert(owner, "saving", 40_000, at(m1.replace(day=7)))
    insert(owner, "groceries", 300_000, at(m1.replace(day=7), 15), pillar="saving")
    return m1


def test_daily_pillars_and_categories_add_up_to_pengeluaran(client, owners):
    owner, _ = owners
    m1 = seed_daily_detail(owner)
    for period in ({"timeframe": "month", "cycle_offset": -1}, {"timeframe": "cycle", "cycle_offset": -1},
                   {"timeframe": "cycle", "cycle_offset": 0}):
        for day in get(client, "/api/dashboard/analytics", **period)["daily_cadence"]:
            assert set(day["by_pillar"]) == {"need", "want", "culture", "unexpected", "unclassified"}, day
            assert sum(day["by_pillar"].values()) == day["spending"], day
            assert sum(row["amount"] for row in day["by_category"]) == day["spending"], day
            assert all(row["amount"] > 0 for row in day["by_category"]), day

    analytics = get(client, "/api/dashboard/analytics", timeframe="month", cycle_offset=-1)
    days = {day["date"]: day for day in analytics["daily_cadence"]}
    assert sum(day["spending"] for day in days.values()) == 412_000 + 627_649 + 25_000 + 120_000 + 45_000 + 30_000


def test_a_reimbursed_want_lowers_its_day_and_pillar(client, owners):
    owner, _ = owners
    m1 = seed_daily_detail(owner)
    analytics = get(client, "/api/dashboard/analytics", timeframe="month", cycle_offset=-1)
    second = next(day for day in analytics["daily_cadence"] if day["date"] == m1.replace(day=2).isoformat())
    assert second["spending"] == 1_039_649
    assert second["by_pillar"] == {"need": 412_000, "want": 627_649, "culture": 0, "unexpected": 0, "unclassified": 0}
    assert second["by_category"] == [
        {"category_id": owner["dinner"], "name": "Makan Bareng", "amount": 627_649},
        {"category_id": owner["groceries"], "name": "Belanja Dapur", "amount": 412_000},
    ]


def test_uncategorised_and_saving_rows_in_the_daily_detail(client, owners):
    owner, _ = owners
    m1 = seed_daily_detail(owner)
    analytics = get(client, "/api/dashboard/analytics", timeframe="month", cycle_offset=-1)
    days = {day["date"]: day for day in analytics["daily_cadence"]}
    uncategorised_name = next(row["name"] for row in analytics["category_variance"] if row["id"] is None)

    fifth = days[m1.replace(day=5).isoformat()]
    assert fifth["by_pillar"] == {"need": 0, "want": 0, "culture": 25_000, "unexpected": 120_000, "unclassified": 0}
    assert [row["category_id"] for row in fifth["by_category"]] == [owner["repair"], owner["groceries"]]

    # A category without a pillar and an expense without a category are both "unclassified".
    sixth = days[m1.replace(day=6).isoformat()]
    assert sixth["by_pillar"]["unclassified"] == 75_000
    assert sixth["by_category"] == [
        {"category_id": owner["misc"], "name": "Lain-lain", "amount": 45_000},
        {"category_id": None, "name": uncategorised_name, "amount": 30_000},
    ]

    # Saving-pillar expenses are not Pengeluaran, so the day has no breakdown.
    seventh = days[m1.replace(day=7).isoformat()]
    assert seventh["expense"] == 340_000 and seventh["spending"] == 0
    assert set(seventh["by_pillar"].values()) == {0}
    assert seventh["by_category"] == []


def link(owner, reimbursement_id, expense_id, amount):
    from app.db.pool import db_conn

    with db_conn() as conn:
        conn.execute(
            "INSERT INTO reimbursement_links (reimbursement_id, expense_id, user_id, amount) VALUES (%s, %s, %s, %s)",
            (reimbursement_id, expense_id, owner["user_id"], amount),
        )
        conn.commit()


def seed_repaid_meal(owner):
    """Last month: a shared meal repaid twice by link on day 3, and more of the same category on day 20."""
    m1 = month_start(-1)
    meal = insert(owner, "dinner", 205_350, at(m1.replace(day=3)))
    insert(owner, "dinner", 53_290, at(m1.replace(day=20), 18))
    insert(owner, "dinner", 35_000, at(m1.replace(day=20), 20))
    for minute in (38, 40):
        repaid = insert(owner, "reimbursement", 68_450, at(m1.replace(day=3), 21, minute), tx_type="income")
        link(owner, repaid, meal, 68_450)
    return m1


def test_a_repaid_meal_stays_on_its_day(client, owners):
    owner, _ = owners
    m1 = seed_repaid_meal(owner)
    month = {"timeframe": "month", "cycle_offset": -1}
    analytics = get(client, "/api/dashboard/analytics", **month)
    days = {day["date"]: day for day in analytics["daily_cadence"]}
    meal_day, later = days[m1.replace(day=3).isoformat()], days[m1.replace(day=20).isoformat()]
    assert meal_day["spending"] == 205_350 - 2 * 68_450
    assert later["spending"] == 53_290 + 35_000
    assert later["by_pillar"]["want"] == 88_290 and later["by_category"][0]["amount"] == 88_290
    # The period figure is the same as before: gross less both links.
    total = 205_350 + 53_290 + 35_000 - 2 * 68_450
    assert sum(day["spending"] for day in days.values()) == total
    comparison = get(client, "/api/reports/monthly/comparison", start_date=m1.isoformat())["comparison"]
    assert comparison["totals"]["spending"] == total
    # Kalender reads the same days.
    calendar = {day["date"]: day for day in get(client, "/api/dashboard/calendar", **month)["days"]}
    assert calendar[m1.replace(day=20).isoformat()]["spending"] == 88_290
    assert calendar[m1.replace(day=3).isoformat()]["spending"] == 205_350 - 2 * 68_450


def test_an_unlinked_reimbursement_still_spreads(client, owners):
    owner, _ = owners
    m1 = month_start(-1)
    insert(owner, "dinner", 60_000, at(m1.replace(day=2)))
    insert(owner, "dinner", 240_000, at(m1.replace(day=9)))
    insert(owner, "reimbursement", 150_000, at(m1.replace(day=10)), tx_type="income", offset="dinner")
    days = {day["date"]: day for day in get(client, "/api/dashboard/analytics", timeframe="month", cycle_offset=-1)["daily_cadence"]}
    assert days[m1.replace(day=2).isoformat()]["spending"] == 30_000
    assert days[m1.replace(day=9).isoformat()]["spending"] == 120_000
