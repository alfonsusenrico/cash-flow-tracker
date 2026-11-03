"""The delete paths the Transaksi drawer chooses by row kind (redesign-transactions-workspace D10).

All names and amounts are synthetic. A top-up row is deleted through its own lifecycle, which restores a
converted expense; the movements path refuses it; a goal link moves no progress.
"""
from datetime import datetime, timezone
from uuid import uuid4

from test_notification_processing_database import notification_owner  # noqa: F401 - fixture
from test_topup_learning import AMOUNT, convert, funds, rdn_debit, row  # noqa: F401


def products(client):
    flat = {}
    for account in client.get("/api/accounts").json()["accounts"]:
        for item in [account, *(account.get("children") or [])]:
            flat[item["id"]] = {"balance": int(item["balance"]), "cost_basis": item.get("cost_basis")}
    return flat


def test_deleting_a_converted_topup_restores_the_expense(client, funds):
    # An earlier conversion puts the product in amount mode, so the comparison is of the second one only.
    earlier = rdn_debit(client, datetime(2026, 9, 9, 3, 0, tzinfo=timezone.utc))
    assert convert(client, earlier, funds["a"]).status_code == 200
    debit = rdn_debit(client, datetime(2026, 10, 9, 3, 0, tzinfo=timezone.utc))
    original = row(debit)
    before = products(client)
    converted = convert(client, debit, funds["a"])
    assert converted.status_code == 200, converted.text
    topup_id = converted.json()["topup"]["id"]
    assert products(client)[funds["a"]]["balance"] == before[funds["a"]]["balance"] + AMOUNT

    listed = client.get("/api/transactions?type=investment&limit=50").json()["transactions"]
    assert topup_id in {item["investment_topup_id"] for item in listed}

    deleted = client.delete(f"/api/investment-topups/{topup_id}")
    assert deleted.status_code == 200, deleted.text
    restored = row(debit)
    assert (restored["movement_id"], restored["category_id"], restored["amount"], restored["idempotency_key"]) == (
        None, original["category_id"], original["amount"], original["idempotency_key"])
    after = products(client)
    assert after[funds["a"]] == before[funds["a"]]
    assert after[funds["funding"]] == before[funds["funding"]]


def test_movements_path_refuses_a_topup(client, funds):
    debit = rdn_debit(client, datetime(2026, 10, 10, 3, 0, tzinfo=timezone.utc))
    topup = convert(client, debit, funds["b"]).json()["topup"]
    movement_id = row(debit)["movement_id"]
    refused = client.delete(f"/api/movements/{movement_id}")
    assert refused.status_code == 409
    assert refused.json()["detail"]["code"] == "investment_topup_lifecycle_required"
    assert client.get(f"/api/investment-topups/{topup['id']}").status_code == 200


def test_goal_link_is_not_a_delete_effect(client, notification_owner):
    """A goal's progress follows its linked accounts or manual amount, never a row's goal link, so the
    drawer's delete confirmation names balance and debt effects only (debt reversal: test_multi_debt_allocations)."""
    goal = client.post("/api/goals", json={"name": f"Liburan Contoh {uuid4().hex[:6]}", "target_amount": 1_000_000})
    assert goal.status_code == 200, goal.text
    goal_id = goal.json()["goal"]["id"]

    def progress():
        return next(item for item in client.get("/api/goals").json()["goals"] if item["id"] == goal_id)["current_amount"]

    start = progress()
    created = client.post("/api/transactions", json={
        "type": "expense", "account_id": notification_owner["bca"], "amount": 150_000, "goal_id": goal_id,
    })
    assert created.status_code == 200, created.text
    assert progress() == start
    deleted = client.delete(f"/api/transactions/{created.json()['transaction_id']}")
    assert deleted.status_code == 200, deleted.text
    assert progress() == start
