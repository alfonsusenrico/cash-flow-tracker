"""One spending basis on Laporan (apply-markas-laporan D29, owner decision 2026-11-08).

The monthly report returns Pengeluaran beside its Belanja-basis merchant, budget and reimbursement figures, so every
"Pengeluaran" Laporan shows equals the whole-period comparison's `totals.spending`, which Beranda, Transaksi and
Analisis show for the same period. The Belanja-basis fields keep their values. Synthetic data only.
"""
from test_monthly_report import at, insert, month_start, owners  # noqa: F401


def seed(owner):
    """Last month: Pengeluaran with a reimbursement, and Rp 1.240.000 of expenses with the Tabungan pillar."""
    m1 = month_start(-1)
    insert(owner, "salary", 5_000_000, at(m1, 9), tx_type="income")
    insert(owner, "groceries", 300_000, at(m1.replace(day=2)), merchant="Pasar Contoh")
    insert(owner, "groceries", 150_000, at(m1.replace(day=9)), merchant="Pasar Contoh")
    insert(owner, "dinner", 200_000, at(m1.replace(day=3)), merchant="Resto Contoh")
    insert(owner, "reimbursement", 60_000, at(m1.replace(day=4)), tx_type="income", offset="dinner")
    insert(owner, "misc", 45_000, at(m1.replace(day=5)), merchant="Toko Contoh")
    # The largest merchant by gross expenses holds only saving-pillar rows: one in a Tabungan category and one row
    # given the Tabungan pillar in a Kebutuhan category.
    insert(owner, "saving", 340_000, at(m1.replace(day=7)), merchant="Reksa Contoh")
    insert(owner, "groceries", 900_000, at(m1.replace(day=8)), merchant="Reksa Contoh", pillar="saving")
    return m1


def get(client, path, **params):
    response = client.get(path, params=params)
    assert response.status_code == 200, response.text
    return response.json()


def set_budget(owner, groceries: int):
    from app.db.pool import db_conn

    with db_conn() as conn:
        conn.execute("UPDATE categories SET monthly_budget = %s WHERE id = %s", (groceries, owner["groceries"]))
        conn.commit()


def report(client, m1):
    return get(client, "/api/reports/monthly", basis="month", start_date=m1.isoformat())


def test_report_pengeluaran_equals_analisis_for_a_closed_month(client, owners):
    owner, _ = owners
    m1 = seed(owner)
    data = report(client, m1)
    spending = data["comparison"]["totals"]["spending"]
    # 300.000 + 150.000 + 200.000 − 60.000 + 45.000; the Rp 1.240.000 with the Tabungan pillar is not Pengeluaran.
    assert spending == 635_000
    assert data["comparison"]["totals"]["difference"] == 5_000_000 - spending
    assert sum(pillar["amount"] for pillar in data["comparison"]["pillars"]) == spending
    viewed = next(item for item in data["comparison"]["history"] if item["viewed"])
    assert viewed["spending"] == spending

    # Analisis for the same month: its hero (the comparison), daily, category, calendar and flow Pengeluaran.
    assert get(client, "/api/reports/monthly/comparison", start_date=m1.isoformat())["comparison"]["totals"]["spending"] == spending
    month = {"timeframe": "month", "cycle_offset": -1}
    analytics = get(client, "/api/dashboard/analytics", **month)
    assert sum(day["spending"] for day in analytics["daily_cadence"]) == spending
    assert sum(row["spending"] for row in analytics["category_variance"]) == spending
    assert get(client, "/api/dashboard/calendar", **month)["totals"]["spending"] == spending
    assert get(client, "/api/dashboard/flow", **month)["summary"]["spending"] == spending


def test_top_places_leave_out_saving_pillar_expenses(client, owners):
    owner, _ = owners
    m1 = seed(owner)
    spending = report(client, m1)["spending"]
    assert [(item["name"], item["amount"]) for item in spending["top_spending_merchants"]] == [
        ("Pasar Contoh", 450_000), ("Resto Contoh", 200_000), ("Toko Contoh", 45_000),
    ]
    # The Belanja-basis list keeps its value.
    assert spending["top_merchants"][0] == {"name": "Reksa Contoh", "search_text": spending["top_merchants"][0]["search_text"], "amount": 1_240_000}


def test_top_merchant_fact_on_the_pengeluaran_basis(client, owners):
    owner, _ = owners
    m1 = seed(owner)
    comparison = report(client, m1)["comparison"]
    belanja = next(fact for fact in comparison["facts"] if fact["kind"] == "top_merchant")
    assert belanja["label"] == "Tempat belanja terbesar: Reksa Contoh" and belanja["amount"] == 1_240_000
    fact = comparison["spending_top_merchant"]
    assert fact["kind"] == "top_merchant"
    assert fact["label"] == "Tempat belanja terbesar: Pasar Contoh"
    assert fact["label_code"] == "facts.comparison.top_merchant" and fact["label_params"] == {"merchant": "Pasar Contoh"}
    assert fact["amount"] == 450_000
    assert fact["ref"]["q"] == "Pasar Contoh"
    # The comparison endpoint carries the same fact.
    alone = get(client, "/api/reports/monthly/comparison", start_date=m1.isoformat())["comparison"]
    assert alone["spending_top_merchant"] == fact


def test_no_spending_merchant_gives_no_fact(client, owners):
    owner, _ = owners
    m1 = month_start(-1)
    insert(owner, "saving", 340_000, at(m1.replace(day=7)), merchant="Reksa Contoh")
    data = report(client, m1)
    assert data["spending"]["top_spending_merchants"] == []
    assert data["comparison"]["spending_top_merchant"] is None
    assert data["spending"]["top_merchants"][0]["amount"] == 340_000


def test_budgets_on_the_pengeluaran_basis(client, owners):
    owner, _ = owners
    m1 = seed(owner)
    set_budget(owner, 1_000_000)
    data = report(client, m1)
    groceries = next(row for row in data["budget"]["categories"] if row["category_id"] == owner["groceries"])
    # Belanja keeps the Rp 900.000 row with the Tabungan pillar; Pengeluaran leaves it out.
    assert (groceries["spent"], groceries["remaining"]) == (1_350_000, -350_000)
    assert (groceries["spending"], groceries["spending_remaining"]) == (450_000, 550_000)
    assert data["budget"]["total_spending"] == sum(row["spending"] for row in data["budget"]["categories"])
    assert data["budget"]["total_spent"] == sum(row["spent"] for row in data["budget"]["categories"])


def test_overall_budget_on_the_pengeluaran_basis(client, owners):
    from app.main import app
    from app.services.auth import get_current_user

    owner, _ = owners
    m1 = seed(owner)
    user = app.dependency_overrides[get_current_user]()
    app.dependency_overrides[get_current_user] = lambda: {**user, "monthly_spending_budget": 2_000_000}
    data = report(client, m1)
    overall = data["budget"]["overall"]
    assert overall["budget"] == 2_000_000
    assert overall["spent"] == data["flow"]["outflow"] == 635_000 + 1_240_000
    assert overall["spending"] == data["comparison"]["totals"]["spending"] == 635_000
    assert overall["spending_remaining"] == 1_365_000


def test_reimbursement_subtracted_from_pengeluaran(client, owners):
    owner, _ = owners
    m1 = seed(owner)
    flow = report(client, m1)["flow"]
    assert flow["reimbursement_offset"] == 60_000
    assert flow["spending_reimbursement_offset"] == 60_000


def test_reimbursement_of_a_saving_expense_is_not_subtracted_from_pengeluaran(client, owners):
    owner, _ = owners
    m1 = month_start(-1)
    insert(owner, "saving", 340_000, at(m1.replace(day=7)))
    insert(owner, "groceries", 100_000, at(m1.replace(day=8)))
    insert(owner, "reimbursement", 40_000, at(m1.replace(day=9)), tx_type="income", offset="saving")
    data = report(client, m1)
    assert data["flow"]["reimbursement_offset"] == 40_000
    assert data["flow"]["spending_reimbursement_offset"] == 0
    assert data["comparison"]["totals"]["spending"] == 100_000


def test_another_owner_is_never_counted(client, owners):
    owner, stranger = owners
    m1 = seed(owner)
    seed(stranger)
    data = report(client, m1)
    assert data["comparison"]["totals"]["spending"] == 635_000
    assert data["spending"]["top_spending_merchants"][0]["amount"] == 450_000
