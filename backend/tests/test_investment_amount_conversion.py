from datetime import datetime, timezone
from uuid import uuid4

import pytest


def account(client, *, balance=0, instrument=None, units=None, avg_buy_price=None, parent=None):
    payload = {"name": f"Conversion fixture {uuid4().hex[:10]}", "type": "investment" if instrument else "bank", "initial_balance": balance}
    if instrument:
        payload.update(instrument_type=instrument, units=units, avg_buy_price=avg_buy_price)
    if parent:
        payload["parent_id"] = parent
    response = client.post("/api/accounts", json=payload)
    assert response.status_code == 200, response.text
    return response.json()["account"]["id"]


def balances(client):
    response = client.get("/api/accounts")
    assert response.status_code == 200
    return {item["id"]: item for item in response.json()["accounts"]}


def unit_fund(client, parent=None):
    """The owner's Sucorinvest Money Market Fund position as of 2026-10-01."""
    product = account(
        client, instrument="mutual_fund", balance=5_518_820, units=2871.1295, avg_buy_price=1922.1773, parent=parent,
    )
    assert client.patch(f"/api/accounts/{product}", json={"last_price": 1994.18}).status_code == 200
    return product


def convert_product(client, product):
    return client.post(f"/api/accounts/{product}/amount-tracking")


def expense(client, source, *, amount=112_590, notes="Pindah saldo keluar"):
    response = client.post("/api/transactions", json={
        "account_id": source, "type": "expense", "amount": amount, "notes": notes,
        "date": datetime.now(timezone.utc).isoformat(),
    })
    assert response.status_code == 200, response.text
    body = response.json()
    return body["transaction_id"]


def link_notification(transaction_id):
    from app.db.pool import db_conn

    with db_conn() as conn:
        user_id = conn.execute("SELECT user_id FROM transactions WHERE id = %s", (transaction_id,)).fetchone()["user_id"]
        conn.execute(
            """INSERT INTO notification_events (user_id, device_id, package_name, post_time, payload_hash, transaction_id)
               VALUES (%s, 'fictional-device', 'com.bca.mybca.omni.android', NOW(), %s, %s)""",
            (user_id, uuid4().hex, transaction_id),
        )
        conn.commit()


def row(transaction_id):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute("SELECT * FROM transactions WHERE id = %s", (transaction_id,)).fetchone()


def convert_expense(client, transaction_id, target):
    return client.post("/api/investment-topups/from-transaction", json={"transaction_id": transaction_id, "target_account_id": target})


@pytest.fixture
def converted_fund(auth_client):
    product = unit_fund(auth_client)
    assert convert_product(auth_client, product).status_code == 200
    return product


def test_unit_fund_keeps_value_and_cost_then_accepts_topups(auth_client):
    parent = account(auth_client, instrument="mutual_fund")
    product = unit_fund(auth_client, parent=parent)
    before = balances(auth_client)
    net_worth = auth_client.get("/api/dashboard/net-worth").json()["net_worth"]

    response = convert_product(auth_client, product)

    assert response.status_code == 200, response.text
    state = response.json()["account"]
    assert state["balance"] == 5_725_549
    assert state["cost_basis"] == 5_518_820
    assert state["capital_gain"] == 206_729
    assert state["units"] is None
    assert state["avg_buy_price"] is None
    assert state["investment_tracking_mode"] == "amount"
    assert state["investment_value_estimated"] is False
    assert state["last_price_at"] == before[product]["last_price_at"]
    assert balances(auth_client)[parent]["balance"] == before[parent]["balance"]
    assert auth_client.get("/api/dashboard/net-worth").json()["net_worth"] == net_worth

    source = account(auth_client, balance=500_000)
    topup = auth_client.post("/api/investment-topups", json={
        "source_account_id": source, "target_account_id": product, "amount": 112_590,
        "date": datetime.now(timezone.utc).isoformat(), "idempotency_key": str(uuid4()),
    })
    assert topup.status_code == 200, topup.text
    after = balances(auth_client)[product]
    assert after["balance"] == 5_838_139
    assert after["cost_basis"] == 5_631_410
    trade = auth_client.post("/api/transactions", json={
        "account_id": source, "type": "expense", "amount": 19_942, "target_account_id": product,
        "investment_action": "buy", "units": 10, "price_per_unit": 1994.2,
    })
    assert trade.status_code in (409, 422), trade.text


@pytest.mark.parametrize("case", ["stock", "unit_free", "already", "archived", "foreign", "parent"])
def test_ineligible_product_conversion_changes_nothing(auth_client, case):
    from app.db.pool import db_conn

    product = unit_fund(auth_client)
    expected = 422
    if case == "stock":
        product = account(auth_client, instrument="stock", units=100, avg_buy_price=6400)
    elif case == "unit_free":
        product = account(auth_client, instrument="mutual_fund")
    elif case == "already":
        assert convert_product(auth_client, product).status_code == 200
        expected = 409
    elif case == "archived":
        assert auth_client.patch(f"/api/accounts/{product}", json={"is_archived": True}).status_code == 200
        expected = 404
    elif case == "foreign":
        with db_conn() as conn:
            owner = conn.execute(
                "INSERT INTO users (username, password_hash, invite_code) VALUES (%s, 'unused', 'fixture') RETURNING id",
                (f"Other fixture {uuid4().hex}",),
            ).fetchone()["id"]
            product = str(conn.execute(
                """INSERT INTO accounts (user_id, name, type, instrument_type, units, last_price)
                   VALUES (%s, 'Foreign fund', 'investment', 'mutual_fund', 5, 1000) RETURNING id""",
                (owner,),
            ).fetchone()["id"])
            conn.commit()
        expected = 404
    elif case == "parent":
        account(auth_client, instrument="mutual_fund", parent=product)
    before = balances(auth_client)

    response = convert_product(auth_client, product)

    assert response.status_code == expected, response.text
    assert balances(auth_client) == before


def test_notification_expense_becomes_topup_without_moving_funding(auth_client, converted_fund):
    source = account(auth_client, balance=500_000)
    transaction_id = expense(auth_client, source)
    link_notification(transaction_id)
    original_key = row(transaction_id)["idempotency_key"]
    before = balances(auth_client)

    response = convert_expense(auth_client, transaction_id, converted_fund)

    assert response.status_code == 200, response.text
    topup = response.json()["topup"]
    assert topup["expense_transaction_id"] == transaction_id
    assert topup["amount"] == 112_590
    after = balances(auth_client)
    assert after[source]["balance"] == before[source]["balance"] == 387_410
    assert after[converted_fund]["balance"] == before[converted_fund]["balance"] + 112_590
    assert after[converted_fund]["cost_basis"] == before[converted_fund]["cost_basis"] + 112_590
    leg = row(transaction_id)
    assert str(leg["movement_id"]) == topup["id"]
    assert leg["movement_role"] == "outbound"
    assert leg["notes"] == "Top up Investasi"
    assert leg["idempotency_key"] == original_key
    listing = auth_client.get("/api/transactions", params={"account_id": source}).json()["transactions"]
    shown = next(item for item in listing if item["id"] == transaction_id)
    assert shown["movement_kind"] == "investment_topup"

    repeat = convert_expense(auth_client, transaction_id, converted_fund)
    assert repeat.status_code == 200
    assert repeat.json()["idempotent"] is True
    assert balances(auth_client) == after

    other_fund = unit_fund(auth_client)
    assert convert_product(auth_client, other_fund).status_code == 200
    unchanged = balances(auth_client)
    assert convert_expense(auth_client, transaction_id, other_fund).status_code == 409
    assert balances(auth_client) == unchanged


def test_owner_notes_are_kept_for_manual_expense(auth_client, converted_fund):
    source = account(auth_client, balance=500_000)
    transaction_id = expense(auth_client, source, notes="Autodebet Bibit Oktober")

    response = convert_expense(auth_client, transaction_id, converted_fund)

    assert response.status_code == 200, response.text
    assert response.json()["topup"]["notes"] == "Autodebet Bibit Oktober"
    assert row(transaction_id)["notes"] == "Autodebet Bibit Oktober"


@pytest.mark.parametrize("case", ["income", "movement", "debt", "goal", "unit_product", "investment_source"])
def test_ineligible_expense_conversion_changes_nothing(auth_client, converted_fund, case):
    from app.db.pool import db_conn

    source = account(auth_client, balance=500_000)
    target = converted_fund
    expected = (409,)
    if case == "income":
        response = auth_client.post("/api/transactions", json={"account_id": source, "type": "income", "amount": 112_590})
        assert response.status_code == 200, response.text
        transaction_id = response.json()["transaction_id"]
    elif case == "movement":
        other = account(auth_client)
        moved = auth_client.post("/api/movements", json={
            "source_account_id": source, "target_account_id": other, "amount": 112_590,
            "date": datetime.now(timezone.utc).isoformat(),
        })
        assert moved.status_code == 200, moved.text
        transaction_id = moved.json()["expense_transaction_id"]
    elif case in ("debt", "goal"):
        transaction_id = expense(auth_client, source)
        with db_conn() as conn:
            user_id = row(transaction_id)["user_id"]
            if case == "debt":
                obligation = conn.execute(
                    "INSERT INTO obligations (user_id, name, total_amount, remaining_amount) VALUES (%s, 'Fixture debt', 500000, 500000) RETURNING id",
                    (user_id,),
                ).fetchone()["id"]
                conn.execute(
                    "INSERT INTO transaction_obligation_allocations (transaction_id, obligation_id, amount) VALUES (%s, %s, 112590)",
                    (transaction_id, obligation),
                )
            else:
                goal = conn.execute(
                    "INSERT INTO goals (user_id, name, target_amount) VALUES (%s, 'Fixture goal', 1000000) RETURNING id",
                    (user_id,),
                ).fetchone()["id"]
                conn.execute("UPDATE transactions SET goal_id = %s WHERE id = %s", (goal, transaction_id))
            conn.commit()
    elif case == "unit_product":
        transaction_id = expense(auth_client, source)
        target = unit_fund(auth_client)
        expected = (422,)
    else:
        transaction_id = expense(auth_client, source)
        with db_conn() as conn:
            fund = conn.execute("SELECT user_id FROM transactions WHERE id = %s", (transaction_id,)).fetchone()
            investment = str(conn.execute(
                "INSERT INTO accounts (user_id, name, type) VALUES (%s, 'Fixture brokerage', 'investment') RETURNING id",
                (fund["user_id"],),
            ).fetchone()["id"])
            conn.execute("UPDATE transactions SET account_id = %s WHERE id = %s", (investment, transaction_id))
            conn.commit()
        expected = (422,)
    before = balances(auth_client)
    original = row(transaction_id)

    response = convert_expense(auth_client, transaction_id, target)

    assert response.status_code in expected, response.text
    assert balances(auth_client) == before
    assert row(transaction_id) == original


def test_deleting_converted_topup_restores_corrected_bank_debit(auth_client, converted_fund):
    source = account(auth_client, balance=500_000)
    transaction_id = expense(auth_client, source)
    link_notification(transaction_id)
    original = row(transaction_id)
    start = balances(auth_client)
    topup = convert_expense(auth_client, transaction_id, converted_fund).json()["topup"]

    corrected = auth_client.patch(f"/api/investment-topups/{topup['id']}", json={"amount": 100_000})
    assert corrected.status_code == 200, corrected.text
    assert balances(auth_client)[converted_fund]["balance"] == start[converted_fund]["balance"] + 100_000

    deleted = auth_client.delete(f"/api/investment-topups/{topup['id']}")

    assert deleted.status_code == 200, deleted.text
    assert deleted.json()["topup"]["is_deleted"] is True
    restored = row(transaction_id)
    assert restored["movement_id"] is None
    assert restored["movement_role"] is None
    assert restored["category_id"] == original["category_id"]
    assert restored["kakeibo_type"] == original["kakeibo_type"]
    assert restored["notes"] == original["notes"]
    assert restored["amount"] == 100_000
    assert restored["idempotency_key"] == original["idempotency_key"]
    after = balances(auth_client)
    assert after[converted_fund]["balance"] == start[converted_fund]["balance"]
    assert after[converted_fund]["cost_basis"] == start[converted_fund]["cost_basis"]
    assert after[source]["balance"] == start[source]["balance"] + 12_590

    again = convert_expense(auth_client, transaction_id, converted_fund)
    assert again.status_code == 200, again.text
    assert again.json()["topup"]["id"] != topup["id"]
    assert balances(auth_client)[converted_fund]["balance"] == start[converted_fund]["balance"] + 100_000
