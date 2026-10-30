"""Five-pillar amounts and fresh savings reconcile to ledger rows; synthetic data only."""
from uuid import uuid4

from test_reimbursements import owner, record  # noqa: F401 - fixture

PILLARS = ("need", "want", "culture", "unexpected")


def account(client, name, *, balance=0, **extra):
    response = client.post("/api/accounts", json={"name": f"{name} {uuid4().hex[:6]}", "type": "bank", "initial_balance": balance, **extra})
    assert response.status_code == 200, response.text
    return response.json()["account"]["id"]


def categorised(client, owner, name, pillar):
    response = client.post("/api/categories", json={"name": f"{name} {uuid4().hex[:6]}", "kind": "expense", "kakeibo_type": pillar})
    assert response.status_code == 200, response.text
    return response.json()["category"]["id"]


def spend(client, owner, category_id, amount, *, pillar=None):
    body = {"type": "expense", "account_id": owner["cash"], "category_id": category_id, "amount": amount}
    if pillar:
        body["kakeibo_type"] = pillar
    response = client.post("/api/transactions", json=body)
    assert response.status_code == 200, response.text


def paired_with_fee(owner, pocket):
    """A captured send of 102.500 paired with a 100.000 credit leaves a 2.500 fee expense."""
    from app.db.pool import db_conn
    from app.services.movement_pairing import link_pair

    with db_conn() as conn:
        legs = {}
        for tx_type, account_id, amount in (("expense", owner["cash"], 102_500), ("income", pocket, 100_000)):
            legs[tx_type] = str(conn.execute(
                """INSERT INTO transactions (user_id, account_id, type, amount, source, review_status)
                   VALUES (%s, %s, %s, %s, 'manual', 'reviewed') RETURNING id""",
                (owner["user_id"], account_id, tx_type, amount),
            ).fetchone()["id"])
        with conn.cursor() as cur:
            linked = link_pair(cur, owner["user_id"], legs["expense"], legs["income"], fee=2_500, notes=None)
        conn.commit()
    assert linked["fee_transaction_id"]


def test_pillars_reconcile_to_net_spending_and_savings_count_once(client, owner):
    record(client, owner, "salary", 5_000_000, tx_type="income")
    # One category with mixed transaction pillars and a partial category reimbursement.
    spend(client, owner, owner["dinner"], 100_000, pillar="need")
    spend(client, owner, owner["dinner"], 300_000)
    record(client, owner, "reimbursement", 200_000, tx_type="income", offset="dinner")
    spend(client, owner, categorised(client, owner, "Olahraga", "culture"), 90_000)
    spend(client, owner, categorised(client, owner, "Servis", "unexpected"), 60_000)
    spend(client, owner, categorised(client, owner, "Dana Sosial", "saving"), 40_000)

    pocket = account(client, "Kantong Harian")
    goal_pocket = account(client, "Kantong Target")
    goal = client.post("/api/goals", json={"name": f"Target {uuid4().hex[:6]}", "target_amount": 1_000_000, "account_ids": [goal_pocket]})
    assert goal.status_code == 200, goal.text
    operational = client.post("/api/movements", json={"source_account_id": owner["cash"], "target_account_id": pocket, "amount": 250_000})
    assert operational.status_code == 200, operational.text
    deposit = client.post("/api/movements", json={"source_account_id": owner["cash"], "target_account_id": goal_pocket, "amount": 300_000})
    assert deposit.status_code == 200, deposit.text
    withdrawal = client.post("/api/movements", json={"source_account_id": goal_pocket, "target_account_id": owner["cash"], "amount": 50_000})
    assert withdrawal.status_code == 200, withdrawal.text
    paired_with_fee(owner, pocket)

    stock = client.post("/api/accounts", json={
        "name": f"Saham Contoh {uuid4().hex[:6]}", "type": "investment", "instrument_type": "stock", "initial_balance": 0, "units": 0,
    })
    assert stock.status_code == 200, stock.text
    trade = client.post("/api/transactions", json={
        "type": "expense", "account_id": owner["cash"], "target_account_id": stock.json()["account"]["id"],
        "amount": 500_000, "investment_action": "buy", "units": 100, "price_per_unit": 5_000,
    })
    assert trade.status_code == 200, trade.text

    overview = client.get("/api/dashboard/overview?timeframe=month").json()
    analytics = client.get("/api/dashboard/analytics?timeframe=month").json()
    kakeibo = overview["kakeibo"]
    fee = 2_500

    # The 200.000 offset splits 1:3 across need and want within the category.
    assert kakeibo["need_spent"] == 100_000 - 50_000 + fee
    assert kakeibo["want_spent"] == 300_000 - 150_000
    assert kakeibo["culture_spent"] == 90_000
    assert kakeibo["unexpected_spent"] == 60_000
    assert kakeibo["saving_expenses"] == 40_000
    expense_pillars = sum(kakeibo[f"{pillar}_spent"] for pillar in PILLARS) + kakeibo["saving_expenses"]
    assert expense_pillars == overview["kpis"]["total_outflow"]
    assert analytics["kakeibo"] == kakeibo

    # Only the goal deposit and its withdrawal are savings movements; the pocket transfer, its fee
    # and the stock purchase are not new savings.
    assert kakeibo["net_saving_transfers"] == 300_000 - 50_000
    assert kakeibo["saving_spent"] == 40_000 + 250_000
    assert kakeibo["total_allocated"] == expense_pillars + 250_000

    # Each pillar figure is traceable to the ledger rows behind it.
    ledger = client.get("/api/transactions", params={"period": "month", "type": "expense", "kakeibo_type": "culture"}).json()
    assert sum(item["amount"] for item in ledger["transactions"]) == kakeibo["culture_spent"]
