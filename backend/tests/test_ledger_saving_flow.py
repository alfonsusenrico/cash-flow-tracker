"""Ledger saving flow, the "Tabungan & investasi" and "Kredit" types and `saving_net` (apply-markas-transaksi
D27, D28, D30), on fresh synthetic owners.

Every name and amount is synthetic: "Kantong Liburan" is a tabungan pocket, "RDN Contoh" investment cash
(dana_investasi), "Reksa Dana Contoh" an amount-tracked product and "SPayLater Contoh" a credit account.
"""
from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest

from test_credit_spending_flow import add_credit, pay, purchase
from test_cycle_summary import at_local, make_owner, now_utc, owner, today, unlimited_rate_limits  # noqa: F401 - fixtures

COUNTED = {"saved", "invested", "taken", "paid_from_savings", "saving_pillar"}
OTHER_FILTERS = (
    "account_id={pocket}", "category_id={makan}", "kakeibo_type=need", "q=Contoh", "source=manual",
    "review_status=reviewed", "uncategorized=true", "possible_duplicates=true", "waiting=true", "goal_id={goal}",
)


def set_role(owner, key, role):
    response = owner.session.patch(f"/api/accounts/{owner[key]}", json={"role": role})
    assert response.status_code == 200, response.text


def product(owner, key, name, instrument):
    body = {"name": name, "type": "investment", "instrument_type": instrument, "initial_balance": 0}
    if instrument == "stock":
        body["units"] = 0
    response = owner.session.post("/api/accounts", json=body)
    assert response.status_code == 200, response.text
    owner.ids[key] = response.json()["account"]["id"]
    return owner.ids[key]


def topup(owner, source, target, amount):
    response = owner.session.post("/api/investment-topups", json={
        "source_account_id": owner[source], "target_account_id": owner[target], "amount": amount,
        "date": (now_utc() - timedelta(minutes=3)).isoformat(), "notes": "Setoran Contoh",
        "idempotency_key": str(uuid4()),
    })
    assert response.status_code == 200, response.text
    return response.json()["topup"]["id"]


def buy(owner, source, target, amount):
    response = owner.session.post("/api/transactions", json={
        "type": "expense", "account_id": owner[source], "target_account_id": owner[target], "amount": amount,
        "investment_action": "buy", "units": 100, "price_per_unit": amount // 100,
    })
    assert response.status_code == 200, response.text


def listing(owner, query=""):
    response = owner.session.get(f"/api/transactions?limit=200&{query}")
    assert response.status_code == 200, response.text
    return response.json()


def legs_by_movement(rows):
    legs = {}
    for row in rows:
        if row["movement_id"]:
            legs.setdefault(row["movement_id"], []).append(row)
    return legs


def flow_of(owner, rows, movement_id):
    """The movement's single saving flow, after checking both legs carry it."""
    legs = legs_by_movement(rows)[movement_id]
    assert len(legs) == 2
    assert legs[0]["saving_flow"] == legs[1]["saving_flow"], legs
    for leg in legs:
        detail = owner.session.get(f"/api/transactions/{leg['id']}")
        assert detail.status_code == 200, detail.text
        assert detail.json()["transaction"]["saving_flow"] == leg["saving_flow"]
    return legs[0]["saving_flow"]


def saving_total(owner, start, end):
    """Beranda's "Ditabung & investasi" over a window, computed as the cycle summary computes it."""
    from app.db.pool import db_conn
    from app.routers.dashboard import get_kakeibo_breakdown
    from app.services.cycle_summary import savings_change
    from app.services.reimbursements import load_spending_offsets

    with db_conn() as conn:
        with conn.cursor() as cur:
            offsets = load_spending_offsets(cur, owner.user_id, start, end)
            return savings_change(get_kakeibo_breakdown(cur, owner.user_id, start, end, 0, offsets))


@pytest.fixture
def saver(owner):
    """An owner with a harian bank, a tabungan pocket, a goal-linked harian pocket, investment cash, a fund,
    a stock and a credit account."""
    owner.account("goal_pocket", "Kantong Target Contoh")
    owner.account("rdn", "RDN Contoh")
    owner.category("zakat", "Dana Sosial Contoh", pillar="saving")
    set_role(owner, "pocket", "tabungan")
    set_role(owner, "rdn", "dana_investasi")
    owner.goal("goal", "Target Contoh", 5_000_000, today() + timedelta(days=400), accounts=("goal_pocket",))
    product(owner, "fund", "Reksa Dana Contoh", "mutual_fund")
    product(owner, "stock", "Saham Contoh", "stock")
    add_credit(owner, name="SPayLater Contoh")
    return owner


# --- Ledger Saving Flow ----------------------------------------------------------------------------------


def test_each_movement_carries_its_saving_flow_on_both_legs(saver):
    moves = {
        "saved": saver.move("bank", "pocket", 1_000_000)["movement_id"],
        "taken": saver.move("pocket", "bank", 300_000)["movement_id"],
        "goal_saved": saver.move("bank", "goal_pocket", 200_000)["movement_id"],
        "into_dana_investasi": saver.move("bank", "rdn", 2_000_000)["movement_id"],
        "harian_to_harian": saver.move("bank", "wallet", 150_000)["movement_id"],
        "harian_to_credit": pay(saver, 100_000)["movement_id"],
        "tabungan_to_credit": pay(saver, 50_000, source="pocket")["movement_id"],
    }
    moves["rdn_topup"] = topup(saver, "rdn", "fund", 500_000)
    moves["tabungan_topup"] = topup(saver, "pocket", "fund", 100_000)
    buy(saver, "bank", "stock", 400_000)
    rows = listing(saver)["transactions"]
    trade = next(row for row in rows if row["movement_id"] and row["transfer_target_account_id"] == saver["stock"])
    moves["beli_trade"] = trade["movement_id"]

    assert {case: flow_of(saver, rows, movement_id) for case, movement_id in moves.items()} == {
        "saved": "saved",
        "taken": "taken",
        "goal_saved": "saved",
        "into_dana_investasi": None,
        "harian_to_harian": None,
        "harian_to_credit": "debt_payment",
        "tabungan_to_credit": "taken",
        "rdn_topup": "invested",
        "tabungan_topup": None,
        "beli_trade": None,
    }

    listed = {row["movement_id"] for row in listing(saver, "type=saving")["transactions"]}
    assert listed == {moves["saved"], moves["taken"], moves["goal_saved"], moves["tabungan_to_credit"], moves["rdn_topup"]}


def test_expenses_paid_from_savings_and_saving_pillar(saver):
    rows = {
        "from_pocket_saving_pillar": saver.expense(70_000, "zakat", account="pocket"),
        "from_pocket": saver.expense(50_000, "makan", account="pocket"),
        "from_goal_pocket": saver.expense(20_000, "jajan", account="goal_pocket"),
        "saving_pillar": saver.expense(40_000, "zakat"),
        "ordinary": saver.expense(30_000, "makan"),
        "uncategorised": saver.expense(10_000),
        "income": saver.tx("income", 500_000, category="gaji"),
    }
    by_id = {row["id"]: row["saving_flow"] for row in listing(saver)["transactions"]}
    assert {case: by_id[tx_id] for case, tx_id in rows.items()} == {
        "from_pocket_saving_pillar": "paid_from_savings",
        "from_pocket": "paid_from_savings",
        "from_goal_pocket": "paid_from_savings",
        "saving_pillar": "saving_pillar",
        "ordinary": None,
        "uncategorised": None,
        "income": None,
    }
    for case, tx_id in rows.items():
        detail = saver.session.get(f"/api/transactions/{tx_id}").json()["transaction"]
        assert detail["saving_flow"] == by_id[tx_id], case


def test_consolidated_and_account_filtered_views_show_the_same_flow(saver):
    saved = saver.move("bank", "pocket", 400_000)["movement_id"]
    taken = saver.move("pocket", "bank", 100_000)["movement_id"]
    consolidated = listing(saver, "type=saving&logical_movements=true")["transactions"]
    assert [(row["movement_id"], row["movement_role"], row["saving_flow"]) for row in consolidated] == [
        (taken, "outbound", "taken"), (saved, "outbound", "saved"),
    ]
    # In the pocket's own view the inbound leg of the deposit shows the same label.
    pocket_rows = listing(saver, f"account_id={saver['pocket']}&logical_movements=true")["transactions"]
    assert {(row["movement_id"], row["movement_role"], row["saving_flow"]) for row in pocket_rows} == {
        (saved, "inbound", "saved"), (taken, "outbound", "taken"),
    }


def test_another_owners_rows_and_accounts_are_never_read(saver, make_owner):
    other = make_owner()
    set_role(other, "pocket", "tabungan")
    other_move = other.move("bank", "pocket", 900_000)["movement_id"]
    other_expense = other.expense(80_000, "makan", account="pocket")
    mine = saver.move("bank", "wallet", 60_000)["movement_id"]

    page = listing(saver, "include=type_counts")
    assert {row["movement_id"] for row in page["transactions"]} == {mine}
    assert page["type_counts"]["saving"] == 0 and page["type_counts"]["credit"] == 0
    assert listing(saver, "type=saving")["total"] == 0
    assert listing(saver, "type=saving")["summary"]["cumulative"]["saving_net"] == 0
    assert saver.session.get(f"/api/transactions/{other_expense}").status_code == 404
    other_rows = listing(other)["transactions"]
    assert {row["saving_flow"] for row in other_rows if row["movement_id"] == other_move} == {"saved"}


def test_aggregate_unchanged(saver):
    start = now_utc() - timedelta(hours=1)
    saver.move("bank", "pocket", 1_000_000)
    saver.move("pocket", "bank", 300_000)
    saver.move("bank", "goal_pocket", 250_000)
    saver.move("bank", "rdn", 2_000_000)
    pay(saver, 120_000)
    pay(saver, 40_000, source="pocket")
    topup(saver, "rdn", "fund", 500_000)
    topup(saver, "pocket", "fund", 100_000)
    buy(saver, "bank", "stock", 300_000)
    saver.expense(60_000, "makan", account="pocket")
    saver.expense(45_000, "zakat")
    end = now_utc() + timedelta(minutes=1)

    rows = listing(saver, "logical_movements=true")["transactions"]
    movements = [row for row in rows if row["movement_role"] == "outbound"]
    totals = saving_total(saver, start, end)

    from app.db.pool import db_conn
    from app.routers.dashboard import get_kakeibo_breakdown
    with db_conn() as conn:
        with conn.cursor() as cur:
            kakeibo = get_kakeibo_breakdown(cur, saver.user_id, start, end, 0)
    assert sum(row["amount"] for row in movements if row["saving_flow"] in ("saved", "invested")) == kakeibo["saving_into"]
    assert sum(row["amount"] for row in movements if row["saving_flow"] == "taken") == kakeibo["saving_out"]
    assert kakeibo["saving_into"] == 1_000_000 + 250_000 + 500_000
    assert kakeibo["saving_out"] == 300_000 + 40_000
    assert totals["total"] == saver.summary()["tabungan"]["total"]


# --- Tabungan & investasi: type=saving and saving_net ------------------------------------------------------


def test_savings_filter_equals_beranda(owner):
    owner.regular_salaries(today() - timedelta(days=5))
    set_role(owner, "pocket", "tabungan")
    owner.category("zakat", "Dana Sosial Contoh", pillar="saving")
    saved = owner.move("bank", "pocket", 1_000_000)["movement_id"]
    taken = owner.move("pocket", "bank", 300_000)["movement_id"]
    spent = owner.expense(50_000, "makan", account="pocket")
    pillar = owner.expense(200_000, "zakat")
    owner.move("bank", "wallet", 75_000)
    owner.expense(90_000, "jajan")

    page = listing(owner, "type=saving&period=cycle&logical_movements=true")
    assert page["total"] == 4
    assert {row["movement_id"] or row["id"] for row in page["transactions"]} == {saved, taken, spent, pillar}
    tabungan = owner.summary()["tabungan"]
    assert page["summary"]["period"]["saving_net"] == page["summary"]["cumulative"]["saving_net"] == 850_000
    assert tabungan["total"] == 850_000


def seed_window(owner, when=None):
    """Saved, taken, spent from savings and a saving-pillar expense that a reimbursement partly offsets."""
    moment = when or (now_utc() - timedelta(minutes=10))
    owner.move("bank", "pocket", 700_000, when=moment)
    owner.move("pocket", "bank", 200_000, when=moment + timedelta(minutes=1))
    owner.expense(30_000, "makan", account="pocket", when=moment + timedelta(minutes=2))
    owner.expense(120_000, "zakat", when=moment + timedelta(minutes=3))
    refund = owner.tx("income", 50_000, category="patungan", when=moment + timedelta(minutes=4))
    owner.db("UPDATE transactions SET offset_category_id = %s WHERE id = %s", (owner["zakat"], refund))
    # 700.000 - 200.000 - 30.000 + (120.000 - 50.000)
    return 540_000


def saving_owner(make_owner):
    owner = make_owner()
    set_role(owner, "pocket", "tabungan")
    owner.category("zakat", "Dana Sosial Contoh", pillar="saving")
    return owner


def test_saving_net_equals_beranda_for_the_open_and_previous_cycle_and_all_time(make_owner):
    from app.services.report_windows import get_timeframe_window

    owner = saving_owner(make_owner)
    latest = today() - timedelta(days=5)
    owner.regular_salaries(latest)
    current = seed_window(owner)
    previous = seed_window(owner, at_local(latest - timedelta(days=10), 10))

    open_cycle = listing(owner, "type=saving&period=cycle")["summary"]
    tabungan = owner.summary()["tabungan"]
    assert tabungan["pilar_tabungan"] == 70_000
    assert open_cycle["period"]["saving_net"] == open_cycle["cumulative"]["saving_net"] == tabungan["total"] == current

    # The previous cycle: the window Beranda's previous-period figures use (get_previous_timeframe_window).
    previous_cycle = listing(owner, "type=saving&period=cycle&period_offset=-1")["summary"]["period"]
    start, end, *_ = get_timeframe_window(25, "cycle", cycle_offset=-1, user_id=owner.user_id, language="id")
    assert previous_cycle["start"] == start.isoformat()
    assert previous_cycle["saving_net"] == saving_total(owner, start, end)["total"] == previous

    everything = listing(owner, "type=saving")["summary"]
    assert everything["period"] is None
    assert everything["cumulative"]["saving_net"] == current + previous


def test_saving_net_equals_beranda_for_the_current_month_and_all_time(make_owner):
    owner = saving_owner(make_owner)
    expected = seed_window(owner)
    tabungan = owner.summary()["tabungan"]
    month = listing(owner, "type=saving&period=month")["summary"]["period"]
    assert month["saving_net"] == tabungan["total"] == expected
    # Every row is in this month, so all time is Beranda's figure too.
    assert listing(owner, "type=saving")["summary"]["cumulative"]["saving_net"] == tabungan["total"]


def test_saving_net_only_without_other_filters(saver):
    saver.move("bank", "pocket", 500_000)
    saver.expense(40_000, "zakat")
    plain = listing(saver, "type=saving&period=month")["summary"]
    assert plain["period"]["saving_net"] == 540_000
    for template in OTHER_FILTERS:
        query = template.format(pocket=saver["pocket"], makan=saver["makan"], goal=saver["goal"])
        summary = listing(saver, f"type=saving&period=month&{query}")["summary"]
        assert summary["period"]["saving_net"] is None, query
        assert summary["cumulative"]["saving_net"] is None, query
    filtered = listing(saver, f"type=saving&period=month&account_id={saver['pocket']}")
    assert [row["saving_flow"] for row in filtered["transactions"]] == ["saved"]
    for other_type in ("expense", "income", "transfer", "movement", "investment", "credit"):
        assert listing(saver, f"type={other_type}&period=month")["summary"]["period"]["saving_net"] is None
    assert listing(saver, "period=month")["summary"]["period"]["saving_net"] is None


def test_saving_summary_keeps_the_existing_figures_over_its_rows(saver):
    saver.move("bank", "pocket", 500_000)
    saver.expense(40_000, "zakat")
    saver.expense(25_000, "makan", account="pocket")
    saver.expense(90_000, "jajan")
    summary = listing(saver, "type=saving&period=month")["summary"]["period"]
    # Gross over the listed rows, as for every type other than expense: two expenses, no income.
    assert (summary["inflow"], summary["gross_outflow"], summary["saving"]) == (0, 65_000, 40_000)


@pytest.mark.parametrize("value", ["savings", "credits", "tabungan", "foo"])
def test_unknown_type_is_refused(owner, value):
    assert owner.session.get(f"/api/transactions?type={value}").status_code == 422


# --- Tab counts and the Kredit type ---------------------------------------------------------------------


def test_tab_counts_include_saving_and_credit(saver):
    saver.move("bank", "pocket", 300_000)
    saver.move("bank", "rdn", 1_000_000)
    topup(saver, "rdn", "fund", 200_000)
    bought = purchase(saver, 120_000)
    saver.expense(30_000, "makan")
    counts = listing(saver, "include=type_counts&limit=1")["type_counts"]
    # Two movements and a top-up are six legs; the purchase and the expense are two rows.
    assert counts == {"all": 8, "expense": 2, "income": 0, "movement": 4, "investment": 2, "saving": 4, "credit": 1}
    for tab in ("expense", "saving", "credit", "investment"):
        assert listing(saver, f"include=type_counts&limit=1&type={tab}")["type_counts"] == counts
    assert listing(saver, "type=credit")["transactions"][0]["id"] == bought


def test_credit_type_filter(saver, make_owner):
    bought = purchase(saver, 120_000)
    paid = pay(saver, 470_000, fee=5_000)
    refund = saver.tx("income", 20_000, category="patungan", account="credit")
    ordinary = saver.expense(45_000, "makan")
    other = make_owner()
    add_credit(other, name="Kredivo Contoh")
    purchase(other, 99_000)

    page = listing(saver, "type=credit&logical_movements=true&include=type_counts")
    ids = {row["id"] for row in page["transactions"]}
    assert ids == {bought, paid["fee_transaction_id"], refund, paid["expense_transaction_id"]}
    assert ordinary not in ids
    roles = sorted(row["credit"]["role"] for row in page["transactions"])
    assert roles == ["fee", "payment", "purchase", "refund"]
    assert page["total"] == 4
    # The existing summary over these rows: the purchase and the fee are spending; the refund offsets, not income.
    cumulative = page["summary"]["cumulative"]
    assert (cumulative["inflow"], cumulative["gross_outflow"], cumulative["saving_net"]) == (0, 125_000, None)
    # Without consolidation both payment legs are listed: the cash leg and the leg on the credit account.
    legs = listing(saver, "type=credit")["transactions"]
    assert {row["id"] for row in legs} == ids | {paid["income_transaction_id"]}
    assert page["type_counts"]["credit"] == 4
    assert listing(saver, "include=type_counts&limit=1")["type_counts"]["credit"] == 5
    # A cash advance out of the credit account lists its cash leg too.
    advance = pay(saver, 30_000, source="credit", target="bank")
    legs = listing(saver, "type=credit&logical_movements=true")["transactions"]
    assert advance["expense_transaction_id"] in {row["id"] for row in legs}
