"""Credit accounts such as paylater (add-credit-accounts), on fresh synthetic owners.

Every name and amount is synthetic: "PayLater Contoh" stands for a paylater account and "Kantong Bayar Kredit"
for the pocket the owner keeps the cash for the bill in.
"""
from datetime import timedelta
from uuid import uuid4

from test_cycle_summary import make_owner, now_utc, owner, unlimited_rate_limits  # noqa: F401 - fixtures


def add_credit(owner, *, owed=350_000, limit=2_000_000, payment_day=11, name="PayLater Contoh"):
    response = owner.session.post("/api/accounts", json={
        "name": name, "type": "credit", "initial_balance": -owed, "credit_limit": limit, "payment_day": payment_day,
    })
    assert response.status_code == 200, response.text
    account_id = response.json()["account"]["id"]
    owner.ids["credit"] = account_id
    return account_id


def account_row(owner, key):
    response = owner.session.get("/api/accounts")
    assert response.status_code == 200, response.text
    rows = response.json()
    rows = rows.get("accounts", rows) if isinstance(rows, dict) else rows
    flat = [row for row in rows] + [child for row in rows for child in row.get("children", [])]
    return next(row for row in flat if row["id"] == owner[key])


def spend(owner, account, amount, category="makan"):
    response = owner.session.post("/api/transactions", json={
        "account_id": owner[account], "category_id": owner[category], "type": "expense", "amount": amount,
        "date": (now_utc() - timedelta(minutes=3)).isoformat(), "notes": "Makan malam",
    })
    assert response.status_code == 200, response.text
    return response.json()


def pay(owner, source, amount, **extra):
    return owner.session.post("/api/movements", json={
        "source_account_id": owner[source], "target_account_id": owner["credit"], "amount": amount,
        "date": (now_utc() - timedelta(minutes=2)).isoformat(), **extra,
    })


def figures(owner):
    data = owner.summary()
    return {"keluar": data["keluar"]["total"], "masuk": data["masuk"]["total"], "remaining": data["remaining"]["total"],
            "net": data["net_worth"]}


# --- Credit Account Type -------------------------------------------------------------------------------


def test_adding_a_credit_account_shows_what_it_owes(owner):
    add_credit(owner)
    row = account_row(owner, "credit")
    assert row["type"] == "credit" and row["initial_balance"] == -350_000 and row["balance"] == -350_000
    assert row["credit_owed"] == 350_000 and row["credit_limit"] == 2_000_000 and row["payment_day"] == 11
    assert row["over_limit"] is False
    # Owing money is what a credit account is for, not a balance to reconcile.
    assert row["reconciliation_discrepancy"] == 0


def test_credit_account_rules_refuse_with_codes(owner):
    credit = add_credit(owner)
    cases = [
        ({"name": "Kantong Kredit", "type": "credit", "parent_id": credit}, "credit_parent_not_allowed"),
        ({"name": "Kantong Kredit", "type": "bank", "parent_id": credit}, "credit_parent_not_allowed"),
        ({"name": "Bank Minus", "type": "bank", "initial_balance": -1000}, "credit_balance_sign"),
        ({"name": "Bank Limit", "type": "bank", "credit_limit": 500_000}, "credit_fields_not_allowed"),
        ({"name": "Kredit Plus", "type": "credit", "initial_balance": 1000}, "credit_balance_sign"),
    ]
    for payload, code in cases:
        response = owner.session.post("/api/accounts", json=payload)
        assert response.status_code == 422, (payload, response.text)
        assert response.json()["detail"]["code"] == code


def test_editing_limit_and_payment_day(owner):
    add_credit(owner)
    response = owner.session.patch(f"/api/accounts/{owner['credit']}", json={"credit_limit": 3_000_000, "payment_day": 25})
    assert response.status_code == 200, response.text
    row = account_row(owner, "credit")
    assert (row["credit_limit"], row["payment_day"]) == (3_000_000, 25)
    refused = owner.session.patch(f"/api/accounts/{owner['bank']}", json={"payment_day": 11})
    assert refused.status_code == 422 and refused.json()["detail"]["code"] == "credit_fields_not_allowed"


def test_type_change_is_refused_once_the_account_has_transactions(owner):
    add_credit(owner, owed=0)
    spend(owner, "credit", 50_000)
    response = owner.session.patch(f"/api/accounts/{owner['credit']}", json={"type": "wallet", "credit_limit": None, "payment_day": None})
    assert response.status_code == 422 and response.json()["detail"]["code"] == "credit_type_change_has_transactions"
    empty = owner.session.post("/api/accounts", json={"name": "Dompet Baru", "type": "wallet"}).json()["account"]["id"]
    assert owner.session.patch(f"/api/accounts/{empty}", json={"type": "credit"}).status_code == 200


def test_another_owners_credit_account_is_not_found(make_owner):
    first, second = make_owner(), make_owner()
    add_credit(first)
    response = second.session.patch(f"/api/accounts/{first['credit']}", json={"credit_limit": 9_000_000})
    assert response.status_code == 404
    assert account_row(first, "credit")["credit_limit"] == 2_000_000


# --- Credit Purchases ---------------------------------------------------------------------------------------


def test_a_purchase_on_credit_is_spending_now_and_grows_what_is_owed(owner):
    add_credit(owner)
    before = figures(owner)
    bank_before = account_row(owner, "bank")["balance"]
    result = spend(owner, "credit", 120_000)
    assert result["over_limit"] is False
    after = figures(owner)
    assert after["keluar"] == before["keluar"] + 120_000
    assert after["remaining"] == before["remaining"] - 120_000
    assert account_row(owner, "credit")["credit_owed"] == 470_000
    assert account_row(owner, "bank")["balance"] == bank_before
    assert after["net"]["total"] == before["net"]["total"] - 120_000


def test_a_purchase_over_the_limit_is_saved_and_flagged(owner):
    add_credit(owner, owed=1_950_000)
    result = spend(owner, "credit", 120_000)
    assert result["over_limit"] is True
    row = account_row(owner, "credit")
    assert row["over_limit"] is True and row["credit_owed"] == 2_070_000


def test_a_refund_lowers_what_is_owed(owner):
    add_credit(owner)
    response = owner.session.post("/api/transactions", json={
        "account_id": owner["credit"], "category_id": owner["patungan"], "type": "income", "amount": 50_000,
        "date": (now_utc() - timedelta(minutes=3)).isoformat(),
    })
    assert response.status_code == 200, response.text
    assert account_row(owner, "credit")["credit_owed"] == 300_000


# --- Credit Payments and Fees --------------------------------------------------------------------------


def test_paying_the_bill_from_the_pocket_is_a_movement_not_spending(owner):
    add_credit(owner, owed=0)
    owner.account("bayar", "Kantong Bayar Kredit", initial=500_000)
    spend(owner, "credit", 470_000)
    before = figures(owner)
    response = pay(owner, "bayar", 470_000)
    assert response.status_code == 200, response.text
    after = figures(owner)
    assert account_row(owner, "credit")["credit_owed"] == 0
    assert account_row(owner, "bayar")["balance"] == 30_000
    assert (after["keluar"], after["masuk"], after["remaining"]) == (before["keluar"], before["masuk"], before["remaining"])
    assert after["net"]["total"] == before["net"]["total"]


def test_a_payment_with_interest_books_the_fee_as_its_own_expense(owner):
    from app.db.pool import db_conn

    add_credit(owner, owed=470_000)
    before = figures(owner)
    response = pay(owner, "bank", 480_000, fee=10_000)
    assert response.status_code == 200, response.text
    with db_conn() as conn:
        fee = conn.execute(
            """SELECT t.amount, t.account_id, c.name, t.kakeibo_type FROM transactions t
               JOIN categories c ON c.id = t.category_id WHERE t.id = %s""",
            (response.json()["fee_transaction_id"],),
        ).fetchone()
    assert (fee["amount"], str(fee["account_id"]), fee["name"], fee["kakeibo_type"]) == (
        10_000, owner["credit"], "Biaya kredit", "unexpected")
    assert account_row(owner, "credit")["credit_owed"] == 0
    assert figures(owner)["keluar"] == before["keluar"] + 10_000


def test_deleting_or_splitting_a_payment_handles_its_fee(owner):
    add_credit(owner, owed=470_000)
    bank_before = account_row(owner, "bank")["balance"]
    paid = pay(owner, "bank", 480_000, fee=10_000).json()
    split = owner.session.post(f"/api/movements/{paid['movement_id']}/split")
    assert split.status_code == 200, split.text
    # The fee stays as a real expense on the credit account; the bank leg keeps the 480.000 that left it.
    assert account_row(owner, "bank")["balance"] == bank_before - 480_000
    assert account_row(owner, "credit")["credit_owed"] == 0
    again = pay(owner, "bank", 100_000, fee=5_000).json()
    assert owner.session.delete(f"/api/movements/{again['movement_id']}").status_code == 200
    assert account_row(owner, "credit")["credit_owed"] == 0
    assert account_row(owner, "bank")["balance"] == bank_before - 480_000


def test_a_fee_needs_a_credit_target(owner):
    add_credit(owner)
    response = owner.session.post("/api/movements", json={
        "source_account_id": owner["bank"], "target_account_id": owner["pocket"], "amount": 10_000, "fee": 1_000,
    })
    assert response.status_code == 422 and response.json()["detail"]["code"] == "credit_fee_not_allowed"


def test_a_cash_advance_grows_what_is_owed(owner):
    add_credit(owner)
    bank_before = account_row(owner, "bank")["balance"]
    response = owner.session.post("/api/movements", json={
        "source_account_id": owner["credit"], "target_account_id": owner["bank"], "amount": 200_000,
    })
    assert response.status_code == 200, response.text
    assert account_row(owner, "credit")["credit_owed"] == 550_000
    assert account_row(owner, "bank")["balance"] == bank_before + 200_000


def test_credit_to_credit_movement_is_refused(owner):
    first = add_credit(owner)
    second = add_credit(owner, name="Kredit Kedua")
    response = owner.session.post("/api/movements", json={
        "source_account_id": first, "target_account_id": second, "amount": 10_000,
    })
    assert response.status_code == 422 and response.json()["detail"]["code"] == "credit_movement_needs_cash"


def test_an_insufficient_bank_side_is_refused(owner):
    add_credit(owner)
    owner.account("kosong", "Kantong Kosong", initial=1_000)
    response = pay(owner, "kosong", 470_000)
    assert response.status_code == 409 and response.json()["detail"]["code"] == "insufficient_funds"


def test_completing_a_one_sided_debit_with_the_credit_account(owner):
    add_credit(owner, owed=470_000)
    debit = owner.tx("expense", 470_000, category="movement", account="bank", notes="Transfer ke PayLater")
    response = owner.session.post("/api/movements/complete", json={
        "transaction_id": debit, "target_account_id": owner["credit"],
    })
    assert response.status_code == 200, response.text
    assert account_row(owner, "credit")["credit_owed"] == 0


# --- Credit Accounts in Pickers (server refusals) ----------------------------------------------------------


def test_credit_cannot_fund_an_investment_pay_a_debt_or_be_balance_checked(owner):
    add_credit(owner)
    owner.account("fund", "Reksa Dana Contoh", kind="investment")
    buy = owner.session.post("/api/transactions", json={
        "account_id": owner["credit"], "target_account_id": owner["fund"], "type": "expense", "amount": 100_000,
        "investment_action": "buy", "units": 100, "price_per_unit": 1000,
    })
    assert buy.status_code == 422 and buy.json()["detail"]["code"] == "credit_account_not_allowed"
    debt_id = str(uuid4())
    owner.db(
        """INSERT INTO obligations (id, user_id, name, total_amount, remaining_amount) VALUES (%s, %s, 'Cicilan Contoh', 1000000, 1000000)""",
        (debt_id, owner.user_id),
    )
    payment = owner.session.post("/api/transactions", json={
        "account_id": owner["credit"], "category_id": owner["lain"], "type": "expense", "amount": 100_000,
        "obligation_id": debt_id,
    })
    assert payment.status_code == 422 and payment.json()["detail"]["code"] == "credit_account_not_allowed"
    check = owner.session.post(f"/api/accounts/{owner['credit']}/balance-check", json={"actual_balance": 0})
    assert check.status_code == 409 and check.json()["detail"]["code"] == "balance_check_not_liquid"


# --- Net worth ---------------------------------------------------------------------------------------------


def test_credit_owed_is_utang_in_net_worth(owner):
    owner.db("UPDATE accounts SET initial_balance = 10000000 WHERE id = %s", (owner["bank"],))
    owner.db(
        """INSERT INTO obligations (id, user_id, name, total_amount, remaining_amount) VALUES (%s, %s, 'Cicilan Contoh', 2200200, 2200200)""",
        (str(uuid4()), owner.user_id),
    )
    add_credit(owner, owed=470_000)
    net = owner.summary()["net_worth"]
    assert net["liquid"] == 10_000_000 and net["investment"] == 0 and net["credit_owed"] == 470_000
    assert net["total"] == 10_000_000 - 2_200_200 - 470_000
    assert {
        "key": f"credit:{owner['credit']}", "label": "PayLater Contoh", "label_source": "user", "amount": -470_000,
    } in net["lines"]
    page = owner.session.get("/api/dashboard/net-worth").json()
    assert page["net_worth"] == net["total"] and page["total_liabilities"] == 2_670_200
    assert page["credit_owed"] == 470_000


def test_an_overpaid_credit_account_owes_nothing_and_is_not_an_asset(owner):
    add_credit(owner, owed=100_000)
    assert pay(owner, "bank", 150_000).status_code == 200
    row = account_row(owner, "credit")
    assert row["balance"] == 50_000 and row["credit_owed"] == 0
    net = owner.summary()["net_worth"]
    assert net["credit_owed"] == 0
    assert not any(line["key"].startswith("credit:") for line in net["lines"])
    assert net["liquid"] == account_row(owner, "bank")["balance"] + account_row(owner, "wallet")["balance"] + account_row(owner, "pocket")["balance"]


def test_credit_cannot_be_reconciled_or_fund_a_goal(owner):
    add_credit(owner)
    reconcile = owner.session.post(f"/api/accounts/{owner['credit']}/reconcile", json={"actual_balance": 0})
    assert reconcile.status_code == 422 and reconcile.json()["detail"]["code"] == "credit_account_not_allowed"
    goal = owner.session.post("/api/goals", json={
        "name": "Dana Contoh", "target_amount": 1_000_000, "account_ids": [owner["credit"]],
    })
    assert goal.status_code == 422 and goal.json()["detail"]["code"] == "credit_account_not_allowed"
    assert account_row(owner, "credit")["color"] == "#475569"
