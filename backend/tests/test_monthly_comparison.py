"""Whole-period comparison (compare-whole-months): baselines, progress, facts and ledger equality.

Synthetic data only. Months are relative to today in the application timezone: M0 is the running month,
M1 the latest completed one.
"""
from datetime import datetime, time, timedelta

from test_monthly_report import TZ, as_user, at, insert, label, month_end, month_start, owners  # noqa: F401

from app.services.monthly_comparison import round_half_up_div, share_percent

SPENDING_PILLARS = ("need", "want", "culture", "unexpected", "unclassified")


def comparison(client, **params):
    response = client.get("/api/reports/monthly/comparison", params=params)
    assert response.status_code == 200, response.text
    return response.json()["comparison"]


def ledger_outflow(client, ref):
    params = {"from_date": ref["from"], "to_date": ref["to"], "limit": 1}
    for key in ("category_id", "kakeibo_type", "q", "uncategorized"):
        if key in ref:
            params[key] = ref[key]
    response = client.get("/api/transactions", params=params)
    assert response.status_code == 200, response.text
    return response.json()["summary"]["period"]["outflow"]


def start_tracking(owner, offset):
    insert(owner, "bonus", 1, at(month_start(offset)), tx_type="income")


def seed_four_months(owner):
    """The owner's example: Juli to Oktober become M4 to M1, with tracking from the first day of M4."""
    start_tracking(owner, -4)
    for offset, amount in ((-4, 3_810_000), (-3, 3_710_000), (-2, 3_218_500), (-1, 4_802_000)):
        insert(owner, "groceries", amount, at(month_start(offset).replace(day=10)), merchant="Pasar Contoh")
    insert(owner, "groceries", 1_450_000, at(month_start(0)), merchant="Pasar Contoh")


def test_rounding_is_half_up_and_shares_are_whole_percent():
    assert round_half_up_div(10_738_500, 3) == 3_579_500
    assert round_half_up_div(5, 2) == 3 and round_half_up_div(4, 3) == 1
    assert share_percent(1_450_000, 4_802_000) == 30
    assert share_percent(1, 0) is None


def test_baseline_is_the_average_of_the_three_whole_months_before(client, owners):
    owner, _ = owners
    seed_four_months(owner)
    body = comparison(client, start_date=month_start(-1).isoformat())
    assert body["running"] is False and body["progress"] is None
    assert body["totals"]["spending"] == 4_802_000
    assert body["previous"]["totals"]["spending"] == 3_218_500
    assert body["previous"]["change"]["spending"] == 1_583_500
    assert body["baseline"]["period_count"] == 3
    assert body["baseline"]["labels"] == [label(month_start(o)) for o in (-4, -3, -2)]
    assert body["baseline"]["totals"]["spending"] == 3_579_500
    assert body["baseline"]["change"]["spending"] == 1_222_500
    assert [(item["label"], item["complete"], item["viewed"]) for item in body["history"]] == [
        (label(month_start(-4)), True, False), (label(month_start(-3)), True, False), (label(month_start(-2)), True, False),
        (label(month_start(-1)), True, True), (label(month_start(0)), False, False),
    ]


def test_running_month_shows_progress_and_no_verdict(client, owners):
    owner, _ = owners
    seed_four_months(owner)
    insert(owner, "repair", 80_000, at(month_start(0)), merchant="Bengkel Contoh")
    body = comparison(client, start_date=month_start(0).isoformat())
    assert body["running"] is True
    assert body["progress"]["previous_label"] == label(month_start(-1))
    assert body["progress"]["spending"] == 1_530_000
    assert body["progress"]["share"] == share_percent(1_530_000, 4_802_000)
    assert set(body["previous"]["change"].values()) == {None}
    assert set(body["baseline"]["change"].values()) == {None}
    for pillar in body["pillars"]:
        assert pillar["change"] is None and pillar["baseline_change"] is None
        assert all(item["change"] is None and item["baseline_change"] is None for item in pillar["categories"])
    assert {fact["kind"] for fact in body["facts"]} <= {"top_merchant", "recurring", "unexpected"}


def test_running_share_of_the_average_is_a_whole_percent_of_the_baseline(client, owners):
    owner, _ = owners
    seed_four_months(owner)
    body = comparison(client, start_date=month_start(0).isoformat())
    # Agustus to Oktober (M3 to M1) average Rp 3.910.167; Rp 1.450.000 so far is 37 % of it.
    assert body["baseline"]["totals"]["spending"] == 3_910_167
    assert body["progress"]["baseline_share"] == 37
    assert body["progress"]["share"] == share_percent(1_450_000, 4_802_000)
    assert set(body["progress"]) == {"previous_label", "previous_spending", "spending", "share", "baseline_share", "pillars"}
    assert set(body["baseline"]["change"].values()) == {None}


def test_no_baseline_share_without_an_average_or_for_a_completed_period(client, owners):
    owner, _ = owners
    start_tracking(owner, -1)
    insert(owner, "groceries", 400_000, at(month_start(-1).replace(day=5)))
    insert(owner, "groceries", 100_000, at(month_start(0)))
    running = comparison(client, start_date=month_start(0).isoformat())
    assert running["baseline"] is None
    assert running["progress"]["baseline_share"] is None
    completed = comparison(client, start_date=month_start(-1).isoformat())
    assert completed["progress"] is None


def test_a_two_period_baseline_gives_its_share_and_a_zero_baseline_none(client, owners):
    owner, other = owners
    start_tracking(owner, -2)
    for offset, amount in ((-2, 1_000_000), (-1, 3_000_000)):
        insert(owner, "groceries", amount, at(month_start(offset).replace(day=5)))
    insert(owner, "groceries", 500_000, at(month_start(0)))
    body = comparison(client, start_date=month_start(0).isoformat())
    assert body["baseline"]["period_count"] == 2
    assert body["progress"]["baseline_share"] == share_percent(500_000, 2_000_000) == 25

    as_user(other)
    start_tracking(other, -2)
    insert(other, "bonus", 900_000, at(month_start(-1).replace(day=5)), tx_type="income")
    insert(other, "groceries", 10_000, at(month_start(0)))
    zero = comparison(client, start_date=month_start(0).isoformat())
    assert zero["baseline"]["totals"]["spending"] == 0
    assert zero["progress"]["baseline_share"] is None


def test_two_usable_months_give_a_two_month_average_and_fewer_give_none(client, owners):
    owner, _ = owners
    start_tracking(owner, -3)
    for offset in (-3, -2, -1):
        insert(owner, "groceries", 1_000_000 * (-offset), at(month_start(offset).replace(day=5)))
    body = comparison(client, start_date=month_start(-1).isoformat())
    assert body["baseline"]["period_count"] == 2
    assert body["baseline"]["totals"]["spending"] == round_half_up_div(3_000_000 + 2_000_000, 2)

    single = comparison(client, start_date=month_start(-2).isoformat())
    assert single["baseline"] is None


def test_every_figure_equals_the_ledger_summary(client, owners):
    owner, _ = owners
    start_tracking(owner, -4)
    for offset in (-4, -3, -2, -1, 0):
        day = month_start(offset)
        insert(owner, "groceries", 300_000 + 1_000 * -offset, at(day.replace(day=2)), merchant="Pasar Contoh")
        insert(owner, "dinner", 200_000, at(day.replace(day=3)), merchant="Resto Contoh")
        # A dinner row filed as a need: the category's reimbursement is split over both pillars.
        insert(owner, "dinner", 100_000, at(day.replace(day=3), 14), pillar="need", merchant="Resto Contoh")
        insert(owner, "reimbursement", 60_000, at(day.replace(day=4)), tx_type="income", offset="dinner")
        insert(owner, "misc", 45_000, at(day.replace(day=5)))
        insert(owner, None, 12_000, at(day.replace(day=5), 15))
        insert(owner, "repair", 80_000, at(day.replace(day=6)), merchant="Bengkel Contoh")
        insert(owner, "saving", 40_000, at(day.replace(day=7)))
        insert(owner, "investment", 500_000, at(day.replace(day=8)))
    body = comparison(client, start_date=month_start(-1).isoformat())
    checked = 0
    for pillar in body["pillars"]:
        assert ledger_outflow(client, pillar["ref"]) == pillar["amount"]
        assert sum(item["amount"] for item in pillar["categories"]) == pillar["amount"]
        for item in pillar["categories"]:
            assert ledger_outflow(client, item["ref"]) == item["amount"]
            checked += 1
    assert checked >= 5
    assert body["totals"]["spending"] == sum(pillar["amount"] for pillar in body["pillars"])
    for item in body["history"]:
        ref = {"from": item["start"], "to": item["end"]}
        pillars = sum(ledger_outflow(client, {**ref, "kakeibo_type": pillar}) for pillar in SPENDING_PILLARS)
        assert pillars == item["spending"]
    for fact in body["facts"]:
        if fact["ref"] is not None and fact["kind"] != "top_merchant":
            assert ledger_outflow(client, fact["ref"]) == fact["amount"]


def test_category_facts_respect_the_threshold_edge_and_new_categories(client, owners):
    owner, _ = owners
    start_tracking(owner, -4)
    for offset in (-4, -3, -2):
        insert(owner, "groceries", 400_000, at(month_start(offset).replace(day=3)))
        insert(owner, "dinner", 400_000, at(month_start(offset).replace(day=3)))
    m1 = month_start(-1)
    insert(owner, "groceries", 449_999, at(m1.replace(day=3)))  # Rp 49.999 above: not a fact
    insert(owner, "dinner", 450_000, at(m1.replace(day=3)))  # Rp 50.000 above: a fact
    insert(owner, "repair", 100_000, at(m1.replace(day=4)))  # new from Rp 100.000
    insert(owner, "misc", 99_999, at(m1.replace(day=4)))  # new but below Rp 100.000
    facts = comparison(client, start_date=m1.isoformat())["facts"]
    by_kind = {}
    for fact in facts:
        by_kind.setdefault(fact["kind"], []).append(fact)
    above = by_kind["category_above_average"]
    assert [(fact["label"], fact["difference"], fact["amount"]) for fact in above] == [
        ("Makan Bareng di atas rata-rata 3 bulan", 50_000, 450_000),
    ]
    assert [(fact["label"], fact["amount"]) for fact in by_kind["category_new"]] == [("Servis: kategori baru", 100_000)]
    for fact in above + by_kind["category_new"]:
        assert ledger_outflow(client, fact["ref"]) == fact["amount"]
    assert len(facts) <= 7
    want = by_kind["want_share"][0]
    assert want["share"] == share_percent(450_000, 449_999 + 450_000 + 100_000 + 99_999)


def test_a_new_category_counts_as_zero_in_the_previous_month(client, owners):
    owner, _ = owners
    start_tracking(owner, -2)
    insert(owner, "groceries", 100_000, at(month_start(-2).replace(day=3)))
    insert(owner, "repair", 70_000, at(month_start(-1).replace(day=3)))
    body = comparison(client, start_date=month_start(-1).isoformat())
    unexpected = next(pillar for pillar in body["pillars"] if pillar["pillar"] == "unexpected")
    category = unexpected["categories"][0]
    assert (category["previous"], category["change"]) == (0, 70_000)


def test_the_last_local_evening_belongs_to_its_month(client, owners):
    owner, _ = owners
    start_tracking(owner, -2)
    last_day = month_end(month_start(-2))
    insert(owner, "groceries", 33_000, datetime.combine(last_day, time(23, 30), TZ))
    insert(owner, "groceries", 1_000, datetime.combine(last_day + timedelta(days=1), time(0, 30), TZ))
    body = comparison(client, start_date=month_start(-1).isoformat())
    assert body["previous"]["totals"]["spending"] == 33_000
    assert body["totals"]["spending"] == 1_000


def test_salary_cycles_compare_closed_cycles_and_show_progress_for_the_open_one(client, owners):
    owner, _ = owners
    from app.db.pool import db_conn

    starts = [at(month_start(offset).replace(day=25), 9) for offset in (-3, -2, -1)]
    for when in starts:
        salary = insert(owner, "salary", 5_000_000, when, tx_type="income")
        with db_conn() as conn:
            conn.execute(
                "INSERT INTO salary_receipts (user_id, transaction_id, confirmation_method) VALUES (%s, %s, 'owner')",
                (owner["user_id"], salary),
            )
            conn.commit()
    insert(owner, "groceries", 50_000, starts[0] + timedelta(days=2))
    insert(owner, "groceries", 70_000, starts[1] + timedelta(days=3))
    insert(owner, "groceries", 9_000, starts[2] + timedelta(days=1))
    closed = comparison(client, basis="cycle", start_date=starts[1].date().isoformat())
    assert closed["running"] is False
    assert closed["totals"]["spending"] == 70_000 and closed["previous"]["totals"]["spending"] == 50_000
    assert closed["previous"]["change"]["spending"] == 20_000
    open_cycle = comparison(client, basis="cycle", start_date=starts[2].date().isoformat())
    assert open_cycle["running"] is True
    assert open_cycle["progress"]["spending"] == 9_000 and open_cycle["progress"]["previous_spending"] == 70_000


def test_route_is_owner_scoped_and_validates_the_period(client, owners):
    owner, stranger = owners
    from app.main import app
    from app.services.auth import get_current_user

    seed_four_months(owner)
    insert(stranger, "groceries", 9_999_000, at(month_start(-1).replace(day=10)))
    body = comparison(client, start_date=month_start(-1).isoformat())
    assert body["totals"]["spending"] == 4_802_000
    assert all(item["spending"] < 9_999_000 for item in body["history"])

    unknown = client.get("/api/reports/monthly/comparison", params={"start_date": month_start(-1).replace(day=2).isoformat()})
    assert unknown.status_code == 404 and unknown.json()["detail"]["code"] == "period_not_found"
    cycle = client.get("/api/reports/monthly/comparison", params={"basis": "cycle"})
    assert cycle.status_code == 409 and cycle.json()["detail"]["code"] == "cycle_unavailable"

    from fastapi.testclient import TestClient

    app.dependency_overrides.pop(get_current_user, None)
    # A fresh client carries no session cookie left by earlier tests.
    anonymous = TestClient(app)  # without `with`, so the app's lifespan keeps the shared pool open
    assert anonymous.get("/api/reports/monthly/comparison").status_code == 401
    as_user(owner)


def test_statement_count_does_not_grow_with_rows(client, owners):
    owner, stranger = owners
    from datetime import timezone

    from app.db.pool import db_conn
    from app.routers.dashboard import get_kakeibo_breakdown
    from app.services.monthly_comparison import build_comparison
    from app.services.monthly_report import list_periods, recurring_in_period, resolve_period

    class Counting:
        def __init__(self, cur):
            self.cur, self.count = cur, 0

        def execute(self, *args, **kwargs):
            self.count += 1
            return self.cur.execute(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self.cur, name)

    def statements(user_id):
        now = datetime.now(timezone.utc)
        with db_conn() as conn:
            with conn.cursor() as cur:
                listing = list_periods(cur, user_id, "month", now=now)
                period = resolve_period(listing, "month", month_start(-1))
                counting = Counting(cur)
                build_comparison(counting, user_id, basis="month", period=period, listing=listing, now=now,
                                 kakeibo_breakdown=get_kakeibo_breakdown, recurring_in_period=recurring_in_period)
                return counting.count

    for who, rows in ((owner, 1), (stranger, 200)):
        start_tracking(who, -4)
        for offset in (-4, -3, -2, -1):
            for i in range(rows):
                insert(who, "groceries", 1_000 + i, at(month_start(offset).replace(day=1 + i % 27)), merchant=f"Toko {i % 9}")
    assert statements(owner["user_id"]) == statements(stranger["user_id"])


def test_pengeluaran_is_one_figure_in_analytics_the_comparison_and_the_ledger(client, owners):
    owner, _ = owners
    start_tracking(owner, -2)
    m1 = month_start(-1)
    insert(owner, "groceries", 300_000, at(m1.replace(day=2)))
    insert(owner, "dinner", 200_000, at(m1.replace(day=3)))
    insert(owner, "dinner", 100_000, at(m1.replace(day=3), 14), pillar="need")
    insert(owner, "reimbursement", 60_000, at(m1.replace(day=4)), tx_type="income", offset="dinner")
    insert(owner, "saving", 40_000, at(m1.replace(day=7)))
    insert(owner, "misc", 45_000, at(m1.replace(day=5)))
    body = comparison(client, start_date=m1.isoformat())
    analytics = client.get("/api/dashboard/analytics", params={"timeframe": "month", "cycle_offset": -1}).json()
    spending = analytics["explanations"]["spending"]
    assert spending["total"] == body["totals"]["spending"] == sum(line["amount"] for line in spending["lines"])
    assert all(line["key"] != "saving" for line in spending["lines"])
    window = {"from_date": body["period"]["start"], "to_date": body["period"]["end"], "limit": 1}
    ledger = client.get("/api/transactions", params=window).json()["summary"]["period"]
    assert ledger["spending"] == body["totals"]["spending"]


def test_a_saving_category_and_pillar_filter_counts_as_saving(client, owners):
    owner, _ = owners
    start_tracking(owner, -2)
    m1 = month_start(-1)
    insert(owner, "saving", 90_000, at(m1.replace(day=3)))
    insert(owner, "reimbursement", 30_000, at(m1.replace(day=4)), tx_type="income", offset="saving")
    params = {"from_date": at(m1).isoformat(), "to_date": at(month_end(m1), 23, 59).isoformat(), "limit": 1,
              "category_id": owner["saving"], "kakeibo_type": "saving"}
    period = client.get("/api/transactions", params=params).json()["summary"]["period"]
    assert (period["outflow"], period["saving"], period["spending"]) == (60_000, 60_000, 0)
