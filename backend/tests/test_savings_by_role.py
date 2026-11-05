"""Savings by role, read at report time (organize-accounts-by-use D3, D4), on fresh synthetic owners.

Every name and amount is synthetic. "Kantong Liburan" is set to role tabungan where a test needs a savings
pocket without a goal.
"""
from datetime import datetime, time, timedelta

from test_cycle_summary import JAKARTA, make_owner, owner, today, unlimited_rate_limits  # noqa: F401 - fixtures


def set_role(owner, key, role):
    response = owner.session.patch(f"/api/accounts/{owner[key]}", json={"role": role})
    assert response.status_code == 200, response.text


def stored_outbound_pillar(owner, movement_id):
    from app.db.pool import db_conn
    with db_conn() as conn:
        return conn.execute(
            "SELECT kakeibo_type FROM transactions WHERE movement_id = %s AND movement_role = 'outbound'",
            (movement_id,),
        ).fetchone()["kakeibo_type"]


def test_moving_money_into_a_tabungan_pocket_without_a_goal(owner):
    set_role(owner, "pocket", "tabungan")
    moved = owner.move("bank", "pocket", 1_000_000)
    data = owner.summary()
    assert data["fresh_saving"]["amount"] == 1_000_000
    assert data["tabungan"] == {"total": 1_000_000, "masuk": 1_000_000, "diambil": 0, "belanja": 0, "pilar_tabungan": 0}
    # The stored pillar agrees going forward, for readers such as the ledger's pillar filter.
    assert stored_outbound_pillar(owner, moved["movement_id"]) == "saving"


def test_paying_from_savings(owner):
    set_role(owner, "pocket", "tabungan")
    owner.tx("income", 10_000_000, category="gaji")
    owner.move("bank", "pocket", 3_000_000)
    owner.expense(1_000_000, "servis", account="pocket")
    data = owner.summary()
    assert data["spending"]["amount"] == 1_000_000
    assert next(line for line in data["spending"]["lines"] if line["key"] == "unexpected")["amount"] == 1_000_000
    assert data["fresh_saving"]["amount"] == 2_000_000
    assert data["tabungan"] == {"total": 2_000_000, "masuk": 3_000_000, "diambil": 0, "belanja": 1_000_000, "pilar_tabungan": 0}
    assert data["keluar"]["total"] == 3_000_000
    assert data["scheduled"]["total"] == 0
    # Aman dipakai before Terjadwal: the cash still in daily accounts.
    assert data["remaining"]["total"] == 7_000_000


def test_more_taken_out_than_saved_is_a_negative_change(owner):
    set_role(owner, "pocket", "tabungan")
    owner.db("UPDATE accounts SET initial_balance = 1000000 WHERE id = %s", (owner["pocket"],))
    owner.expense(200_000, "makan")
    owner.move("bank", "pocket", 300_000)
    owner.move("pocket", "bank", 800_000)
    data = owner.summary()
    assert data["tabungan"] == {"total": -500_000, "masuk": 300_000, "diambil": 800_000, "belanja": 0, "pilar_tabungan": 0}
    assert data["fresh_saving"]["amount"] == -500_000
    assert data["keluar"]["total"] == data["spending"]["amount"] - 500_000 == -300_000
    overview = owner.session.get("/api/dashboard/overview?timeframe=month").json()["kakeibo"]
    assert (overview["saving_spent"], overview["saving_into"], overview["saving_out"]) == (-500_000, 300_000, 800_000)


def test_spending_from_a_goal_linked_pocket_also_lowers_savings(owner):
    owner.goal("target", "Target Contoh", 5_000_000, today() + timedelta(days=400), accounts=("pocket",))
    owner.move("bank", "pocket", 600_000)
    owner.expense(150_000, "makan", account="pocket")
    data = owner.summary()
    assert data["tabungan"] == {"total": 450_000, "masuk": 600_000, "diambil": 0, "belanja": 150_000, "pilar_tabungan": 0}


def test_saving_pillar_expenses_are_their_own_part(owner):
    owner.category("zakat", "Dana Sosial Contoh", pillar="saving")
    owner.expense(40_000, "zakat")
    data = owner.summary()
    assert data["tabungan"] == {"total": 40_000, "masuk": 0, "diambil": 0, "belanja": 0, "pilar_tabungan": 40_000}


def test_moves_between_daily_accounts_do_not_affect_savings(owner):
    owner.move("bank", "pocket", 250_000)
    owner.move("bank", "wallet", 100_000)
    assert owner.summary()["tabungan"]["total"] == 0


def test_role_applies_to_past_movements(owner):
    from app.db.pool import db_conn
    from app.routers.dashboard import get_kakeibo_breakdown

    first_of_month = today().replace(day=1)
    last_month_day = first_of_month - timedelta(days=5)
    start = datetime.combine(last_month_day.replace(day=1), time.min, JAKARTA)
    end = datetime.combine(first_of_month, time.min, JAKARTA) - timedelta(microseconds=1)
    owner.move("bank", "pocket", 400_000, when=datetime.combine(last_month_day, time(10), JAKARTA))

    def last_month_saving():
        with db_conn() as conn:
            with conn.cursor() as cur:
                return get_kakeibo_breakdown(cur, owner.user_id, start, end, 0)["saving_spent"]

    assert last_month_saving() == 0
    set_role(owner, "pocket", "tabungan")
    assert last_month_saving() == 400_000


def test_a_tabungan_pocket_is_a_saving_node_on_the_money_map(owner):
    set_role(owner, "pocket", "tabungan")
    owner.move("bank", "pocket", 700_000)
    flow = owner.session.get("/api/dashboard/flow?timeframe=month")
    assert flow.status_code == 200, flow.text
    body = flow.json()
    assert f"sav:{owner['pocket']}" in {node["id"] for node in body["nodes"]}
    assert body["summary"]["saving_net"] == 700_000
    assert body["summary"]["keluar"] == owner.summary()["keluar"]["total"]
