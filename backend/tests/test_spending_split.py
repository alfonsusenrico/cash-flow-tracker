"""Pengeluaran, Ditabung & investasi and the platform gain (refine-workspace-layout D4, D8).

Every figure is additive: the split adds up to the existing totals, and no existing field changes.
All names and amounts are synthetic.
"""
from datetime import timedelta
from urllib.parse import quote
from uuid import uuid4

from test_cycle_summary import make_owner, now_utc, owner, unlimited_rate_limits  # noqa: F401 - fixtures


def lines_add_up(figure):
    assert sum(line["amount"] for line in figure["lines"]) == figure["amount"]


def ledger(owner, query):
    response = owner.session.get(f"/api/transactions?{query}")
    assert response.status_code == 200, response.text
    return response.json()


def window_query():
    start = (now_utc() - timedelta(days=1)).isoformat()
    end = (now_utc() + timedelta(minutes=1)).isoformat()
    return f"from_date={quote(start)}&to_date={quote(end)}"


# --- Cycle summary -------------------------------------------------------------------------------


def test_spending_and_saving_add_up_to_keluar(owner):
    owner.category("tabung", "Tabungan Contoh", pillar="saving")
    owner.expense(120_000, "makan")
    owner.expense(45_000, "jajan")
    owner.expense(30_000, "lain")
    owner.expense(200_000, "tabung")
    data = owner.summary()

    spending, saving = data["spending"], data["fresh_saving"]
    lines_add_up(spending)
    lines_add_up(saving)
    assert spending["amount"] + saving["amount"] == data["keluar"]["total"]
    # A saving-classified expense is Ditabung, never Pengeluaran.
    assert saving["amount"] == 200_000
    assert spending["amount"] == 195_000
    assert {line["key"] for line in spending["lines"]} == {"need", "want", "culture", "unexpected", "unclassified"}
    assert [line["key"] for line in saving["lines"]] == ["saving"]


def test_remaining_identity_is_unchanged(owner):
    owner.category("tabung", "Tabungan Contoh", pillar="saving")
    owner.expense(80_000, "makan")
    owner.expense(50_000, "tabung")
    data = owner.summary()
    assert data["remaining"]["total"] == (
        data["masuk"]["total"] - data["spending"]["amount"] - data["fresh_saving"]["amount"] - data["scheduled"]["total"]
    )
    assert data["remaining"]["total"] == data["masuk"]["total"] - data["keluar"]["total"] - data["scheduled"]["total"]


def test_net_fresh_saving_through_a_movement_is_saving(owner):
    owner.goal("liburan", "Liburan Contoh", 5_000_000, now_utc().date() + timedelta(days=200), accounts=["pocket"])
    owner.move("bank", "pocket", 300_000)
    owner.expense(60_000, "makan")
    data = owner.summary()
    assert data["fresh_saving"]["amount"] == next(line["amount"] for line in data["keluar"]["lines"] if line["key"] == "saving")
    assert data["spending"]["amount"] == 60_000


def test_another_owners_rows_are_excluded(make_owner):
    first, second = make_owner(), make_owner()
    second.category("tabung", "Tabungan Contoh", pillar="saving")
    second.expense(999_000, "tabung")
    second.expense(111_000, "makan")
    first.expense(25_000, "makan")
    data = first.summary()
    assert data["spending"]["amount"] == 25_000
    assert data["fresh_saving"]["amount"] == 0


# --- Ledger summary and day totals ---------------------------------------------------------------


def test_ledger_outflow_splits_into_spending_and_saving(owner):
    owner.category("tabung", "Tabungan Contoh", pillar="saving")
    owner.expense(70_000, "makan")
    owner.expense(40_000, "jajan")
    owner.expense(150_000, "tabung")
    period = ledger(owner, f"{window_query()}&limit=50")["summary"]["period"]
    assert period["outflow"] == 260_000
    assert period["spending"] + period["saving"] == period["outflow"]
    assert (period["spending"], period["saving"]) == (110_000, 150_000)
    # Existing fields keep their values.
    assert period["inflow"] == 0 and period["gross_outflow"] == 260_000 and period["net"] == -260_000


def test_day_spending_adds_up_across_pages(owner):
    owner.category("tabung", "Tabungan Contoh", pillar="saving")
    for amount in (11_000, 22_000, 33_000, 44_000, 55_000):
        owner.expense(amount, "makan")
    owner.expense(100_000, "tabung")
    query = f"{window_query()}&include=daily_totals"
    pages, offset, days = [], 0, {}
    while True:
        page = ledger(owner, f"{query}&limit=2&offset={offset}")
        pages.append(page)
        for day in page["daily_totals"]:
            assert days.setdefault(day["date"], day) == day
        offset += 2
        if offset >= page["total"]:
            break
    assert len(pages) == 3
    period = pages[0]["summary"]["period"]
    assert sum(day["spending"] for day in days.values()) == period["spending"] == 165_000
    assert sum(day["outflow"] for day in days.values()) == period["outflow"] == 265_000


def test_ledger_spending_equals_cycle_summary_spending(owner):
    owner.category("tabung", "Tabungan Contoh", pillar="saving")
    owner.expense(90_000, "makan")
    owner.expense(35_000, "servis")
    owner.expense(60_000, "tabung")
    summary = owner.summary()
    period = ledger(owner, f"period={summary['period']['basis']}&limit=50")["summary"]["period"]
    # Without saving moves the ledger's saving is the cycle summary's saving bucket.
    assert period["spending"] == summary["spending"]["amount"] == 125_000
    assert period["saving"] == summary["fresh_saving"]["amount"] == 60_000


def test_ledger_spending_for_another_owner_is_untouched(make_owner):
    first, second = make_owner(), make_owner()
    second.expense(500_000, "makan")
    first.expense(10_000, "makan")
    period = ledger(first, f"{window_query()}&limit=50")["summary"]["period"]
    assert (period["spending"], period["saving"]) == (10_000, 0)


# --- Platform gain on investment parents ---------------------------------------------------------


def investment(owner, key, name, *, parent=None, units=None, avg=None, last=None):
    account_id = str(uuid4())
    owner.db(
        """INSERT INTO accounts (id, user_id, name, type, initial_balance, parent_id, instrument_type, units,
                                 avg_buy_price, last_price)
           VALUES (%s, %s, %s, 'investment', 0, %s, %s, %s, %s, %s)""",
        (account_id, owner.user_id, name, owner.ids.get(parent) if parent else None,
         "stock" if units else None, units, avg, last),
    )
    owner.ids[key] = account_id
    return account_id


def accounts_by_id(owner):
    response = owner.session.get("/api/accounts")
    assert response.status_code == 200, response.text
    found = {}

    def walk(rows):
        for row in rows:
            found[row["id"]] = row
            walk(row.get("children") or [])

    walk(response.json()["accounts"])
    return found


def test_platform_gain_is_the_sum_of_its_products(owner):
    investment(owner, "platform", "Platform Contoh")
    investment(owner, "gain", "Saham Naik Contoh", parent="platform", units=300, avg=1_000, last=1_200)
    investment(owner, "loss", "Saham Turun Contoh", parent="platform", units=100, avg=2_000, last=1_500)
    rows = accounts_by_id(owner)
    products = [rows[owner["gain"]], rows[owner["loss"]]]
    assert [p["capital_gain"] for p in products] == [60_000, -50_000]
    platform = rows[owner["platform"]]
    assert platform["group_capital_gain"] == sum(p["capital_gain"] for p in products) == 10_000
    assert platform["group_cost_basis"] == 300_000 + 200_000


def test_platform_gain_is_null_when_a_product_lacks_a_cost_basis(owner):
    investment(owner, "platform", "Platform Contoh")
    investment(owner, "known", "Saham Contoh", parent="platform", units=100, avg=1_000, last=1_100)
    investment(owner, "unknown", "Produk Tanpa Modal", parent="platform")
    rows = accounts_by_id(owner)
    assert rows[owner["unknown"]]["capital_gain"] is None
    assert rows[owner["platform"]]["group_capital_gain"] is None
    assert rows[owner["platform"]]["group_cost_basis"] is None


def test_liquid_parents_carry_no_group_fields(owner):
    owner.account("child", "Kantong Contoh", parent="bank", initial=50_000)
    rows = accounts_by_id(owner)
    assert "group_capital_gain" not in rows[owner["bank"]]
    assert "group_cost_basis" not in rows[owner["bank"]]


def test_platform_gain_is_owner_scoped(make_owner):
    first, second = make_owner(), make_owner()
    investment(second, "platform", "Platform Lain")
    investment(second, "product", "Saham Lain", parent="platform", units=10, avg=1_000, last=9_000)
    investment(first, "platform", "Platform Contoh")
    investment(first, "product", "Saham Contoh", parent="platform", units=10, avg=1_000, last=1_500)
    rows = accounts_by_id(first)
    assert second["platform"] not in rows
    assert rows[first["platform"]]["group_capital_gain"] == 5_000
