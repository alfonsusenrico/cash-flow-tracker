"""Credit spending flow (add-credit-spending-flow) through the API, on fresh synthetic owners.

Every name and amount is synthetic: "SPayLater Contoh" and "Kredivo Contoh" stand for paylater accounts and
"Jago Contoh" for the bank account that pays them. Cases with absolute dates freeze the bills' clock
(`credit_bills.local_today`); cases about the current period use dates relative to the real clock.
"""
from __future__ import annotations

import json
import random
import time as clock
from datetime import date, timedelta
from uuid import uuid4

from test_category_system_keys import differences, without_volatile
from test_cycle_summary import JAKARTA, at_local, make_owner, now_utc, owner, today, unlimited_rate_limits  # noqa: F401 - fixtures

CREDIT_CODES = (
    "credit_terms_not_credit_purchase", "credit_terms_count_range", "credit_terms_due_before_purchase",
    "credit_account_required", "credit_period_invalid", "account_not_found", "transaction_not_found",
)


def freeze(monkeypatch, day: date) -> None:
    monkeypatch.setattr("app.services.credit_bills.local_today", lambda: day)


def add_credit(owner, key="credit", name="SPayLater Contoh", *, owed=0, limit=2_000_000, payment_day=11,
               statement_day=1, created: date | None = None):
    body = {"name": name, "type": "credit", "initial_balance": -owed, "credit_limit": limit}
    if payment_day is not None:
        body["payment_day"] = payment_day
    if statement_day is not None:
        body["statement_day"] = statement_day
    response = owner.session.post("/api/accounts", json=body)
    assert response.status_code == 200, response.text
    account_id = response.json()["account"]["id"]
    owner.ids[key] = account_id
    if created is not None:
        owner.db("UPDATE accounts SET created_at = %s WHERE id = %s", (at_local(created), account_id))
    return account_id


def purchase(owner, amount, on: date | None = None, *, account="credit", terms=None, category="makan"):
    body = {
        "account_id": owner[account], "category_id": owner[category], "type": "expense", "amount": amount,
        "date": (at_local(on) if on else now_utc() - timedelta(minutes=3)).isoformat(), "notes": "Makan malam",
    }
    if terms is not None:
        body["credit_terms"] = terms
    response = owner.session.post("/api/transactions", json=body)
    assert response.status_code == 200, response.text
    return response.json()["transaction_id"]


def pay(owner, amount, on: date | None = None, *, source="bank", target="credit", fee=None):
    body = {"source_account_id": owner[source], "target_account_id": owner[target], "amount": amount,
            "date": (at_local(on, 12) if on else now_utc() - timedelta(minutes=2)).isoformat()}
    if fee is not None:
        body["fee"] = fee
    response = owner.session.post("/api/movements", json=body)
    assert response.status_code == 200, response.text
    return response.json()


def overview(owner, key="credit", **params):
    query = "&".join(f"{name}={value}" for name, value in params.items())
    response = owner.session.get(f"/api/credit/accounts/{owner[key]}" + (f"?{query}" if query else ""))
    assert response.status_code == 200, response.text
    return response.json()


def accounts_by_id(owner):
    response = owner.session.get("/api/accounts")
    assert response.status_code == 200, response.text
    rows = response.json()["accounts"]
    return {row["id"]: row for row in rows}


def ledger_row(owner, transaction_id, **params):
    query = "&".join(f"{name}={value}" for name, value in {"limit": 200, **params}.items())
    response = owner.session.get(f"/api/transactions?{query}")
    assert response.status_code == 200, response.text
    return next(row for row in response.json()["transactions"] if row["id"] == transaction_id)


def refused(response, status, code):
    assert response.status_code == status, response.text
    detail = response.json()["detail"]
    assert detail["code"] == code and isinstance(detail["params"], dict) and detail["message"], detail
    return detail


def refused_code(response, status, code):
    """An existing `add-credit-accounts` refusal: its code, in whatever shape the error contract gives it."""
    assert response.status_code == status, response.text
    assert response.json()["detail"]["code"] == code, response.text


def put_terms(owner, transaction_id, **body):
    return owner.session.put(f"/api/transactions/{transaction_id}/credit-terms", json=body)


def figures(owner):
    """Every figure a credit change must not move: Beranda, Kakeibo, the ledger totals and the month report."""
    summary = owner.summary()
    month_start = today().replace(day=1).isoformat()
    report = owner.session.get(f"/api/reports/monthly?basis=month&start_date={month_start}")
    assert report.status_code == 200, report.text
    ledger = owner.session.get("/api/transactions?limit=1&period=month").json()["summary"]
    return {
        "spending": {key: value for key, value in summary["spending"].items() if key != "credit"},
        "keluar": summary["keluar"], "masuk": summary["masuk"], "fresh_saving": summary["fresh_saving"],
        "remaining": summary["remaining"], "scheduled": summary["scheduled"]["total"],
        "report": without_volatile(report.json()),
        "ledger": {key: ledger[key] for key in ("cycle", "cumulative")},
    }


def orphan_terms() -> int:
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute(
            """SELECT COUNT(*) AS n FROM credit_purchase_terms pt
               WHERE NOT EXISTS (SELECT 1 FROM transactions t WHERE t.id = pt.transaction_id)""",
        ).fetchone()["n"]


def stored_terms(owner, transaction_id):
    from app.db.pool import db_conn

    with db_conn() as conn:
        return conn.execute(
            "SELECT installment_count, first_due_date, first_due_date_set_by FROM credit_purchase_terms WHERE transaction_id = %s",
            (transaction_id,),
        ).fetchone()


# --- Credit Account Type: the bill day --------------------------------------------------------------------


def test_bill_day_is_credit_only(owner, make_owner):
    add_credit(owner, statement_day=None)
    response = owner.session.patch(f"/api/accounts/{owner['credit']}", json={"statement_day": 1})
    assert response.status_code == 200 and response.json()["account"]["statement_day"] == 1
    refused_code(owner.session.post("/api/accounts", json={"name": "Bank Contoh 2", "type": "bank", "statement_day": 1}),
                 422, "credit_fields_not_allowed")
    refused_code(owner.session.patch(f"/api/accounts/{owner['bank']}", json={"statement_day": 1}), 422,
                 "credit_fields_not_allowed")
    stranger = make_owner()
    assert stranger.session.patch(f"/api/accounts/{owner['credit']}", json={"statement_day": 25}).status_code == 404
    assert accounts_by_id(owner)[owner["credit"]]["statement_day"] == 1
    for value in (0, 32):
        assert owner.session.patch(f"/api/accounts/{owner['credit']}", json={"statement_day": value}).status_code == 422


def test_credit_rows_gain_statement_day_limit_left_and_next_due(owner, monkeypatch):
    freeze(monkeypatch, date(2026, 11, 21))
    add_credit(owner)
    add_credit(owner, "kredivo", "Kredivo Contoh", owed=1_950_000, limit=1_000_000, payment_day=None, statement_day=None)
    owner.tx("expense", 120_000, category="makan", account="credit", when=at_local(date(2026, 11, 5)))
    rows = accounts_by_id(owner)
    spaylater, kredivo, bank = rows[owner["credit"]], rows[owner["kredivo"]], rows[owner["bank"]]
    assert (spaylater["statement_day"], spaylater["limit_left"]) == (1, 1_880_000)
    assert spaylater["next_due"] == {"due_date": "2026-12-11", "amount": 120_000, "left_to_pay": 120_000, "status": "open"}
    # Over the limit, what is left of it is negative; without a payment day there is no next bill.
    assert (kredivo["limit_left"], kredivo["over_limit"], kredivo["next_due"]) == (-950_000, True, None)
    assert (bank["statement_day"], bank["limit_left"], bank["next_due"]) == (None, None, None)


# --- Credit Purchase Terms ------------------------------------------------------------------------------


def test_six_instalments_at_purchase_count_in_full_on_the_purchase_day(owner):
    add_credit(owner)
    before = owner.summary()
    tx = purchase(owner, 1_200_000, terms={"installment_count": 6})
    after = owner.summary()
    assert after["spending"]["amount"] == before["spending"]["amount"] + 1_200_000
    assert after["remaining"]["total"] == before["remaining"]["total"] - 1_200_000
    terms = owner.session.get(f"/api/transactions/{tx}").json()["transaction"]["credit"]
    assert terms["installment_count"] == 6 and [item["amount"] for item in terms["installments"]] == [200_000] * 6
    assert terms["left_to_pay"] == 1_200_000


def test_changing_and_resetting_terms_moves_no_figure(owner):
    add_credit(owner)
    owner.salary(today() - timedelta(days=2))
    tx = purchase(owner, 1_200_000, terms={"installment_count": 6})
    baseline = figures(owner)
    response = put_terms(owner, tx, installment_count=3)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["credit_terms"] == {"installment_count": 3, "first_due_date": None, "first_due_date_set_by": None}
    assert [item["installment"] for item in body["installments"]] == [
        {"number": 1, "count": 3}, {"number": 2, "count": 3}, {"number": 3, "count": 3},
    ]
    assert differences(baseline, figures(owner)) == []
    reset = owner.session.delete(f"/api/transactions/{tx}/credit-terms")
    assert reset.status_code == 200 and len(reset.json()["installments"]) == 1
    assert reset.json()["credit_terms"]["installment_count"] == 1 and stored_terms(owner, tx) is None
    assert differences(baseline, figures(owner)) == []


def test_detail_carries_the_stored_first_due_date_and_who_set_it(owner):
    """`GET /transactions/{id}` exposes the stored terms so an edit can keep an owner-set date (apply-markas-transaksi)."""
    add_credit(owner)
    tx = purchase(owner, 300_000)
    credit = owner.session.get(f"/api/transactions/{tx}").json()["transaction"]["credit"]
    assert (credit["first_due_date"], credit["first_due_date_set_by"]) == (None, None)
    due = (today() + timedelta(days=40)).isoformat()
    assert put_terms(owner, tx, installment_count=3, first_due_date=due).status_code == 200
    credit = owner.session.get(f"/api/transactions/{tx}").json()["transaction"]["credit"]
    assert (credit["first_due_date"], credit["first_due_date_set_by"], credit["installment_count"]) == (due, "owner", 3)
    # The list rows stay as they were: the stored terms are the detail's only.
    listed = next(row for row in owner.session.get("/api/transactions?limit=50").json()["transactions"] if row["id"] == tx)
    assert "first_due_date" not in listed["credit"] and "first_due_date_set_by" not in listed["credit"]


def test_terms_refusals_carry_codes_and_change_nothing(owner):
    add_credit(owner)
    bank_expense = owner.expense(50_000, "makan")
    refund = owner.tx("income", 20_000, category="patungan", account="credit")
    on_credit = purchase(owner, 100_000, date(2026, 11, 5))
    refused(put_terms(owner, bank_expense, installment_count=3), 422, "credit_terms_not_credit_purchase")
    refused(put_terms(owner, refund, installment_count=3), 422, "credit_terms_not_credit_purchase")
    detail = refused(put_terms(owner, on_credit, installment_count=40), 422, "credit_terms_count_range")
    assert detail["params"] == {"min": 1, "max": 36}
    refused(put_terms(owner, on_credit, installment_count=0), 422, "credit_terms_count_range")
    detail = refused(put_terms(owner, on_credit, installment_count=3, first_due_date="2026-11-04"), 422,
                     "credit_terms_due_before_purchase")
    assert detail["params"] == {"purchase_date": "2026-11-05"}
    assert stored_terms(owner, on_credit) is None
    # The same refusals at creation, before anything is written.
    count = len(owner.session.get("/api/transactions?limit=200").json()["transactions"])
    response = owner.session.post("/api/transactions", json={
        "account_id": owner["bank"], "category_id": owner["makan"], "type": "expense", "amount": 10_000,
        "credit_terms": {"installment_count": 3},
    })
    refused(response, 422, "credit_terms_not_credit_purchase")
    response = owner.session.post("/api/transactions", json={
        "account_id": owner["credit"], "category_id": owner["makan"], "type": "expense", "amount": 10_000,
        "credit_terms": {"installment_count": 37},
    })
    refused(response, 422, "credit_terms_count_range")
    assert len(owner.session.get("/api/transactions?limit=200").json()["transactions"]) == count


def test_another_owners_purchase_is_not_found(owner, make_owner):
    add_credit(owner)
    tx = purchase(owner, 100_000)
    stranger = make_owner()
    refused(put_terms(stranger, tx, installment_count=3), 404, "transaction_not_found")
    refused(stranger.session.delete(f"/api/transactions/{tx}/credit-terms"), 404, "transaction_not_found")
    assert stored_terms(owner, tx) is None


def test_purchase_moved_off_the_credit_account_loses_its_terms(owner):
    add_credit(owner)
    tx = purchase(owner, 300_000, terms={"installment_count": 3})
    assert stored_terms(owner, tx)["installment_count"] == 3
    response = owner.session.patch(f"/api/transactions/{tx}", json={"account_id": owner["bank"]})
    assert response.status_code == 200, response.text
    assert stored_terms(owner, tx) is None
    assert ledger_row(owner, tx)["credit"] is None


def test_purchase_date_moved_past_the_owner_due_date_gets_its_derived_date(owner):
    add_credit(owner)
    tx = purchase(owner, 300_000, date(2026, 11, 5))
    assert put_terms(owner, tx, installment_count=1, first_due_date="2026-12-01").status_code == 200
    kept = owner.session.patch(f"/api/transactions/{tx}", json={"date": at_local(date(2026, 11, 20)).isoformat()})
    assert kept.status_code == 200 and kept.json()["credit_terms_reset"] is False
    assert stored_terms(owner, tx)["first_due_date"] == date(2026, 12, 1)
    response = owner.session.patch(f"/api/transactions/{tx}", json={"date": at_local(date(2026, 12, 3)).isoformat()})
    assert response.status_code == 200, response.text
    assert response.json()["credit_terms_reset"] is True
    assert stored_terms(owner, tx) is None
    assert ledger_row(owner, tx)["credit"]["next_due_date"] == "2027-01-11"


def test_deleting_purchases_and_movements_leaves_no_orphan_terms(owner):
    add_credit(owner)
    first = purchase(owner, 300_000, terms={"installment_count": 3})
    purchase(owner, 200_000, terms={"installment_count": 2, "first_due_date": (today() + timedelta(days=40)).isoformat()})
    assert owner.session.delete(f"/api/transactions/{first}").status_code == 200
    assert orphan_terms() == 0
    assert stored_terms(owner, first) is None


def test_payment_day_change_keeps_bills_already_due(owner, monkeypatch):
    freeze(monkeypatch, date(2026, 11, 20))
    add_credit(owner, created=date(2026, 10, 1))
    paid = owner.tx("expense", 100_000, category="makan", account="credit", when=at_local(date(2026, 10, 5)))
    open_purchase = owner.tx("expense", 80_000, category="makan", account="credit", when=at_local(date(2026, 11, 5)))
    pay(owner, 100_000, date(2026, 11, 10))
    before = {bill["due_date"]: bill for bill in overview(owner, **{"from": "2026-10-01", "to": "2026-12-31"})["bills"]}
    assert set(before) == {"2026-11-11", "2026-12-11"} and before["2026-11-11"]["status"] == "paid"

    response = owner.session.patch(f"/api/accounts/{owner['credit']}", json={"payment_day": 5})
    assert response.status_code == 200, response.text
    after = {bill["due_date"]: bill for bill in overview(owner, **{"from": "2026-10-01", "to": "2026-12-31"})["bills"]}
    assert set(after) == {"2026-11-11", "2026-12-05"}
    assert after["2026-11-11"]["status"] == "paid" and after["2026-11-11"]["items"][0]["transaction_id"] == paid
    assert after["2026-11-11"]["items"][0]["settled_on_time"] is True
    assert after["2026-12-05"]["items"][0]["transaction_id"] == open_purchase
    assert stored_terms(owner, paid)["first_due_date_set_by"] == "system"
    assert stored_terms(owner, open_purchase) is None
    # A later date change of the kept purchase clears the kept date.
    owner.session.patch(f"/api/transactions/{paid}", json={"date": at_local(date(2026, 10, 6)).isoformat()})
    assert stored_terms(owner, paid) is None


# --- Credit Facts on Ledger Rows ----------------------------------------------------------------------


def test_purchase_payment_and_ordinary_rows(owner, monkeypatch):
    freeze(monkeypatch, date(2026, 11, 21))
    add_credit(owner)
    dinner = owner.tx("expense", 120_000, category="makan", account="credit", when=at_local(date(2026, 11, 5)))
    bank_meal = owner.expense(40_000, "makan")
    movement = pay(owner, 50_000, date(2026, 11, 6), source="bank", target="pocket")
    payment = pay(owner, 470_000, date(2026, 11, 7))
    row = ledger_row(owner, dinner)
    assert row["credit"] == {
        "role": "purchase", "account_id": owner["credit"], "account_name": "SPayLater Contoh",
        "installment_count": 1, "installments_settled": 1, "next_due_date": None, "left_to_pay": 0,
        "settled_on": "2026-11-07", "days_to_settle": 2, "settled_on_time": True, "due_dates_unavailable": False,
    }
    assert ledger_row(owner, bank_meal)["credit"] is None
    assert ledger_row(owner, movement["expense_transaction_id"])["credit"] is None
    payment_rows = [row for row in owner.session.get("/api/transactions?limit=200&logical_movements=true").json()["transactions"]
                    if row["movement_id"] == payment["movement_id"]]
    assert len(payment_rows) == 1
    assert payment_rows[0]["credit"] == {"role": "payment", "account_id": owner["credit"], "account_name": "SPayLater Contoh"}


def test_unpaid_dinner_row_says_when_it_is_due(owner, monkeypatch):
    freeze(monkeypatch, date(2026, 11, 21))
    add_credit(owner)
    dinner = owner.tx("expense", 120_000, category="makan", account="credit", when=at_local(date(2026, 11, 5)))
    block = ledger_row(owner, dinner)["credit"]
    assert (block["role"], block["account_name"], block["installment_count"], block["installments_settled"]) == (
        "purchase", "SPayLater Contoh", 1, 0)
    assert (block["next_due_date"], block["left_to_pay"], block["settled_on"]) == ("2026-12-11", 120_000, None)


def test_account_filter_is_the_per_provider_view(owner):
    add_credit(owner)
    add_credit(owner, "kredivo", "Kredivo Contoh")
    mine = {
        purchase(owner, 100_000),
        owner.tx("income", 20_000, category="patungan", account="credit"),
    }
    paid = pay(owner, 50_000, fee=5_000)
    mine |= {paid["income_transaction_id"], paid["fee_transaction_id"]}
    advance = pay(owner, 30_000, source="credit", target="bank")
    mine.add(advance["expense_transaction_id"])
    purchase(owner, 70_000, account="kredivo")
    owner.expense(10_000, "makan")
    rows = owner.session.get(f"/api/transactions?account_id={owner['credit']}&limit=200").json()["transactions"]
    assert {row["id"] for row in rows} == mine
    roles = {row["id"]: row["credit"]["role"] for row in rows}
    assert sorted(roles.values()) == ["cash_advance", "fee", "payment", "purchase", "refund"]


def test_one_schedule_build_per_credit_account_per_request(owner, monkeypatch):
    import app.services.credit_bills as credit_bills

    add_credit(owner)
    add_credit(owner, "kredivo", "Kredivo Contoh")
    for _ in range(4):
        purchase(owner, 10_000)
        purchase(owner, 10_000, account="kredivo")
    built = []
    original = credit_bills.build_schedule
    monkeypatch.setattr(credit_bills, "build_schedule", lambda account, *args: built.append(str(account["id"])) or original(account, *args))
    response = owner.session.get("/api/transactions?limit=200")
    assert response.status_code == 200
    assert sorted(built) == sorted([owner["credit"], owner["kredivo"]])


# --- Credit Account Overview ---------------------------------------------------------------------------


def test_overview_totals(owner):
    add_credit(owner)
    purchase(owner, 200_000)
    pay(owner, 480_000, fee=10_000)
    body = overview(owner, **{"from": (today() - timedelta(days=1)).isoformat(), "to": today().isoformat()})
    assert body["totals"] == {
        "spent_on_credit": 210_000, "fees": 10_000, "refunds": 0, "paid_to_credit": 480_000,
        "items_settled": 2, "items_settled_on_time": 2,
    }
    assert body["account"] == {
        "id": owner["credit"], "name": "SPayLater Contoh", "owed": 0, "credit_limit": 2_000_000,
        "limit_left": 2_000_000, "over_limit": False, "statement_day": 1, "payment_day": 11, "credit_balance": 270_000,
    }
    assert set(body) == {"ok", "account", "due_dates_unavailable", "next_due", "bills", "period", "totals"}
    bill = body["bills"][0]
    assert set(bill) == {"due_date", "statement_date", "amount", "paid", "left_to_pay", "status", "settled_on", "items"}
    assert set(bill["items"][0]) == {
        "kind", "transaction_id", "date", "due_date", "installment", "amount", "left_to_pay", "settled_on",
        "days_to_settle", "settled_on_time",
    }


def test_bills_due_soon_across_accounts(owner, monkeypatch):
    freeze(monkeypatch, date(2026, 11, 21))
    add_credit(owner)
    add_credit(owner, "kredivo", "Kredivo Contoh", payment_day=1, statement_day=21)
    owner.tx("expense", 120_000, category="makan", account="credit", when=at_local(date(2026, 11, 5)))
    owner.tx("expense", 90_000, category="makan", account="kredivo", when=at_local(date(2026, 11, 10)))
    owner.tx("expense", 60_000, category="makan", account="credit", when=at_local(date(2026, 12, 5)))
    response = owner.session.get("/api/credit/bills?to=2026-12-13")
    assert response.status_code == 200, response.text
    assert response.json()["bills"] == [
        {"account_id": owner["kredivo"], "account_name": "Kredivo Contoh", "due_date": "2026-12-01",
         "statement_date": "2026-11-21", "amount": 90_000, "paid": 0, "left_to_pay": 90_000, "status": "upcoming",
         "settled_on": None},
        {"account_id": owner["credit"], "account_name": "SPayLater Contoh", "due_date": "2026-12-11",
         "statement_date": "2026-12-01", "amount": 120_000, "paid": 0, "left_to_pay": 120_000, "status": "open",
         "settled_on": None},
    ]
    only_open = owner.session.get("/api/credit/bills?status=open").json()["bills"]
    assert {bill["due_date"] for bill in only_open} == {"2026-12-11", "2027-01-11"}
    assert owner.session.get("/api/credit/bills?status=late").status_code == 422
    refused(owner.session.get("/api/credit/bills?from=2026-12-02&to=2026-12-01"), 422, "credit_period_invalid")


def test_overview_refuses_a_bank_account_and_another_owners_account(owner, make_owner):
    add_credit(owner)
    refused(owner.session.get(f"/api/credit/accounts/{owner['bank']}"), 422, "credit_account_required")
    stranger = make_owner()
    refused(stranger.session.get(f"/api/credit/accounts/{owner['credit']}"), 404, "account_not_found")
    refused(owner.session.get(f"/api/credit/accounts/{uuid4()}"), 404, "account_not_found")


def list_spending(owner, key="credit", **params):
    query = "&".join(f"{name}={value}" for name, value in {"limit": 1, "type": "credit", "account_id": owner[key], **params}.items())
    response = owner.session.get(f"/api/transactions?{query}")
    assert response.status_code == 200, response.text
    return response.json()["summary"]["period"]


def test_overview_with_the_list_window_counts_exactly_the_listed_rows(owner):
    """The Kredit header reads the list's own window (apply-markas-cleanup D9): a purchase before the salary instant
    on the cycle's first day, or timed later than now, is in neither; whole local days still count both."""
    from urllib.parse import quote

    add_credit(owner)
    start = today() - timedelta(days=2)
    owner.salary(start)
    owner.tx("expense", 20_000, category="makan", account="credit", when=at_local(start, 7))
    purchase(owner, 459_000)
    later = now_utc() + timedelta(minutes=10)
    owner.tx("expense", 77_000, category="makan", account="credit", when=later)

    listed = list_spending(owner, period="cycle", period_offset=0)
    windowed = overview(owner, period="cycle", period_offset=0)
    assert listed["spending"] == windowed["totals"]["spent_on_credit"] == 459_000
    assert windowed["period"] == {"from": start.isoformat(), "to": today().isoformat()}

    instants = {"from_date": quote(listed["start"]), "to_date": quote(listed["end"])}
    assert overview(owner, **instants)["totals"]["spent_on_credit"] == 459_000
    assert list_spending(owner, **instants)["spending"] == 459_000

    whole_days = overview(owner, **{"from": start.isoformat(), "to": later.astimezone(JAKARTA).date().isoformat()})
    assert whole_days["totals"]["spent_on_credit"] == 459_000 + 20_000 + 77_000

    month = overview(owner, period="month", period_offset=0)
    assert month["totals"]["spent_on_credit"] == list_spending(owner, period="month", period_offset=0)["spending"]


def test_overview_refuses_two_period_forms_and_half_a_range(owner, make_owner):
    add_credit(owner)
    base = f"/api/credit/accounts/{owner['credit']}"
    instant = "2026-11-01T00%3A00%3A00%2B07%3A00"
    refused(owner.session.get(f"{base}?period=cycle&from=2026-11-01"), 422, "period_with_dates")
    refused(owner.session.get(f"{base}?period=month&to_date={instant}"), 422, "period_with_dates")
    refused(owner.session.get(f"{base}?from=2026-11-01&from_date={instant}"), 422, "period_with_dates")
    refused(owner.session.get(f"{base}?from_date={instant}"), 422, "credit_period_invalid")
    later = "2026-11-02T00%3A00%3A00%2B07%3A00"
    refused(owner.session.get(f"{base}?from_date={later}&to_date={instant}"), 422, "credit_period_invalid")
    assert owner.session.get(f"{base}?period=week").status_code == 422
    stranger = make_owner()
    refused(stranger.session.get(f"{base}?period=cycle"), 404, "account_not_found")


def test_paid_in_full_terms_from_catat_store_nothing(owner):
    """Catat sends `{installment_count: 1}` for "Sekali bayar"; it stores no terms row, like a notification
    purchase, and both purchases sit on the same bill (apply-markas-cleanup D14)."""
    add_credit(owner)
    with_terms = purchase(owner, 50_000, terms={"installment_count": 1})
    without_terms = purchase(owner, 50_000)
    assert stored_terms(owner, with_terms) is None and stored_terms(owner, without_terms) is None
    bills = overview(owner)["bills"]
    holding = {item["transaction_id"]: (bill["due_date"], item["installment"], item["amount"]) for bill in bills for item in bill["items"]}
    assert holding[with_terms] == holding[without_terms]


def test_no_payment_day_reports_due_dates_unavailable(owner):
    add_credit(owner, owed=50_000, payment_day=None)
    purchase(owner, 120_000)
    body = overview(owner)
    assert body["due_dates_unavailable"] is True and body["bills"] == [] and body["next_due"] is None
    assert (body["account"]["owed"], body["account"]["limit_left"]) == (170_000, 1_830_000)
    assert owner.session.get("/api/credit/bills").json()["due_dates_unavailable"] == [
        {"account_id": owner["credit"], "account_name": "SPayLater Contoh"},
    ]


def test_errors_are_in_the_accounts_language(owner):
    add_credit(owner)
    owner.db("UPDATE users SET language = 'en' WHERE id = %s", (owner.user_id,))
    bank_expense = owner.expense(50_000, "makan")
    detail = refused(put_terms(owner, bank_expense, installment_count=3), 422, "credit_terms_not_credit_purchase")
    assert detail["message"] == "Installments can only be set on a purchase made with a credit account."
    owner.db("UPDATE users SET language = 'id' WHERE id = %s", (owner.user_id,))
    detail = refused(put_terms(owner, bank_expense, installment_count=3), 422, "credit_terms_not_credit_purchase")
    assert detail["message"] == "Cicilan hanya bisa diatur untuk pembelian dengan rekening kredit."


def test_every_credit_code_is_an_error_code_in_both_server_and_web_catalogues():
    from pathlib import Path

    from app.errors import ErrorCode

    messages = Path(__file__).resolve().parents[2]
    for catalogue in ("backend/app/messages", "frontend/src/messages"):
        for language in ("en", "id"):
            errors = json.loads((messages / catalogue / f"{language}.json").read_text())["errors"]
            for code in CREDIT_CODES:
                assert ErrorCode(code) and errors[code], (catalogue, language, code)


# --- Cycle summary: credit figures, F1 and F2 ------------------------------------------------------------


def credit_plan(owner, key, name, amount, due: date, *, target="credit", schedule="monthly_day"):
    rule_id = str(uuid4())
    day = due.isoweekday() if schedule == "weekly" else due.day
    owner.db(
        """INSERT INTO recurring_rules (id, user_id, name, type, amount, source_account_id, target_account_id,
                                        schedule_type, schedule_day, next_due_date)
           VALUES (%s, %s, %s, 'transfer', %s, %s, %s, %s, %s, %s)""",
        (rule_id, owner.user_id, name, amount, owner["bank"], owner[target], schedule, day, due),
    )
    owner.ids[key] = rule_id
    return rule_id


def plan_line(summary, rule_id):
    return [line for line in summary["scheduled"]["lines"] if line.get("rule_id") == rule_id]


def test_purchase_and_payment_in_one_cycle(owner):
    add_credit(owner)
    owner.expense(880_000, "makan")
    purchase(owner, 120_000)
    before = owner.summary()
    pay(owner, 470_000)
    after = owner.summary()
    assert after["spending"]["amount"] == before["spending"]["amount"] == 1_000_000
    credit = after["spending"]["credit"]
    assert (credit["spent_on_credit"], credit["paid_to_credit"]) == (120_000, 470_000)
    assert credit["lines"] == [{
        "key": f"credit:{owner['credit']}", "account_id": owner["credit"], "label": "SPayLater Contoh",
        "label_source": "user", "spent_on_credit": 120_000, "paid_to_credit": 470_000,
    }]
    for figure in ("keluar", "masuk", "fresh_saving", "remaining"):
        assert after[figure] == before[figure], figure


def test_no_credit_accounts_gives_zero_credit_figures(owner):
    owner.expense(100_000, "makan")
    summary = owner.summary()
    assert summary["spending"]["credit"] == {"spent_on_credit": 0, "paid_to_credit": 0, "lines": []}
    assert set(summary["spending"]) == {"amount", "lines", "credit"}
    assert summary["spending"]["amount"] == sum(line["amount"] for line in summary["keluar"]["lines"] if line["key"] != "saving")


def test_reimbursed_credit_spending_stays_a_part_of_pengeluaran(owner):
    add_credit(owner)
    purchase(owner, 100_000)
    response = owner.session.post("/api/transactions", json={
        "account_id": owner["bank"], "category_id": owner["patungan"], "type": "income", "amount": 150_000,
        "offset_category_id": owner["makan"],
    })
    assert response.status_code == 200, response.text
    summary = owner.summary()
    assert summary["spending"]["amount"] == 0 and summary["spending"]["credit"]["spent_on_credit"] == 0


def test_f1_credit_bill_plan_is_not_counted_twice(owner):
    """The double-count test (D9): Rp 470.000 bought on SPayLater and a Rp 470.000 plan to pay it."""
    add_credit(owner)
    start = owner.summary()
    purchase(owner, 470_000)
    bought = owner.summary()
    assert bought["remaining"]["total"] == start["remaining"]["total"] - 470_000
    due = today() + timedelta(days=3)
    rule = credit_plan(owner, "bayar", "Bayar SPayLater", 470_000, due)
    planned = owner.summary()
    [line] = plan_line(planned, rule)
    assert (line["plan_amount"], line["amount"], line["counted_elsewhere"]) == (470_000, 0, 470_000)
    assert line["credit_account_id"] == owner["credit"]
    # Lower by Rp 470.000 only, through Pengeluaran: not Rp 940.000.
    assert planned["remaining"]["total"] == start["remaining"]["total"] - 470_000
    assert planned["scheduled"]["plans_total"] == start["scheduled"]["plans_total"]

    payment = pay(owner, 470_000)
    response = owner.session.post(f"/api/recurring/{rule}/occurrences/link",
                                  json={"scheduled_for": due.isoformat(), "transaction_id": payment["expense_transaction_id"]})
    assert response.status_code == 200, response.text
    paid = owner.summary()
    assert plan_line(paid, rule) == []
    assert paid["remaining"]["total"] == planned["remaining"]["total"]


def test_f1_worked_numbers_from_the_research_note(owner):
    """Masuk 10.000.000, cash spending 7.000.000, SPayLater 2.000.000 and a 2.000.000 "Bayar SPayLater" plan."""
    owner.ids["jago"] = owner.account("jago", "Jago Contoh", initial=0)
    add_credit(owner, limit=5_000_000)
    owner.tx("income", 10_000_000, category="gaji", account="jago", notes="Gaji contoh")
    owner.expense(7_000_000, "makan", account="jago")
    purchase(owner, 2_000_000)
    rule_id = str(uuid4())
    due = today() + timedelta(days=5)
    owner.db(
        """INSERT INTO recurring_rules (id, user_id, name, type, amount, source_account_id, target_account_id,
                                        schedule_type, schedule_day, next_due_date)
           VALUES (%s, %s, 'Bayar SPayLater', 'transfer', 2000000, %s, %s, 'monthly_day', %s, %s)""",
        (rule_id, owner.user_id, owner["jago"], owner["credit"], due.day, due),
    )
    summary = owner.summary()
    assert (summary["masuk"]["total"], summary["keluar"]["total"]) == (10_000_000, 9_000_000)
    # Before F1 the plan subtracted the bill again: 10.000.000 - 9.000.000 - 2.000.000 = -1.000.000.
    assert summary["scheduled"]["total"] == 0
    assert summary["remaining"]["total"] == 1_000_000
    [line] = plan_line(summary, rule_id)
    assert (line["plan_amount"], line["amount"], line["counted_elsewhere"]) == (2_000_000, 0, 2_000_000)


def test_f1_counts_plans_for_opening_debt_in_due_date_order(owner):
    add_credit(owner, "kredivo", "Kredivo Contoh", owed=300_000, payment_day=11)
    first = today() + timedelta(days=1)
    rule = credit_plan(owner, "bayar", "Bayar Kredivo", 200_000, first, target="kredivo", schedule="weekly")
    summary = owner.summary()
    lines = plan_line(summary, rule)
    assert [line["due_date"] for line in lines][:2] == [first.isoformat(), (first + timedelta(days=7)).isoformat()]
    assert [(line["amount"], line["counted_elsewhere"]) for line in lines][:2] == [(200_000, 0), (100_000, 100_000)]
    assert all(line["amount"] == 0 for line in lines[2:])
    assert summary["scheduled"]["total"] == 300_000


def test_plans_into_other_accounts_keep_their_line_unchanged(owner):
    add_credit(owner)
    due = today() + timedelta(days=3)
    rule = owner.plan("dompet", "Isi Dompet", 150_000, due, kind="transfer")
    [line] = plan_line(owner.summary(), rule)
    assert line == {"kind": "plan", "rule_id": rule, "label": "Isi Dompet", "label_source": "user",
                    "due_date": due.isoformat(), "overdue": False, "amount": 150_000}


def test_f2_payment_from_a_savings_pocket(owner):
    """Characterisation (D10): money leaving a savings pocket lowers "Ditabung & investasi", as for any use."""
    add_credit(owner, owed=200_000)
    owner.account("tabungan", "Tabungan Contoh")
    owner.db("UPDATE accounts SET role = 'tabungan' WHERE id = %s", (owner["tabungan"],))
    start = owner.summary()
    owner.move("bank", "tabungan", 200_000)
    saved = owner.summary()
    assert saved["fresh_saving"]["amount"] == start["fresh_saving"]["amount"] + 200_000
    assert saved["remaining"]["total"] == start["remaining"]["total"] - 200_000
    owner.move("tabungan", "credit", 200_000)
    paid = owner.summary()
    assert paid["fresh_saving"]["amount"] == saved["fresh_saving"]["amount"] - 200_000 == start["fresh_saving"]["amount"]
    assert paid["spending"]["credit"]["paid_to_credit"] == saved["spending"]["credit"]["paid_to_credit"] + 200_000
    assert paid["spending"]["amount"] == start["spending"]["amount"]


# --- Credit fee identity --------------------------------------------------------------------------------


def fee_figures(owner):
    summary = owner.summary()
    month_start = today().replace(day=1).isoformat()
    report = owner.session.get(f"/api/reports/monthly?basis=month&start_date={month_start}").json()
    kakeibo = {line["key"]: line["amount"] for line in summary["keluar"]["lines"]}
    return summary["spending"]["amount"], kakeibo["unexpected"], without_volatile(report)


def test_renaming_the_fee_category_moves_no_figure_and_is_still_reused(owner):
    from app.db.pool import db_conn

    add_credit(owner, owed=470_000)
    pay(owner, 480_000, fee=10_000)
    before = fee_figures(owner)
    with db_conn() as conn:
        category = conn.execute(
            "SELECT id, name FROM categories WHERE user_id = %s AND system_key = 'credit_fee'", (owner.user_id,),
        ).fetchone()
    assert category["name"] == "Biaya kredit"
    assert owner.session.patch(f"/api/categories/{category['id']}", json={"name": "Bunga Contoh"}).status_code == 200
    spending, unexpected, report = fee_figures(owner)
    assert (spending, unexpected) == before[:2] and before[1] >= 10_000
    renamed_back = json.loads(json.dumps(report).replace('"Bunga Contoh"', '"Biaya kredit"'))
    assert differences(before[2], renamed_back) == []
    pay(owner, 105_000, fee=5_000)
    with db_conn() as conn:
        rows = conn.execute(
            "SELECT id FROM categories WHERE user_id = %s AND kind = 'expense' AND system_key = 'credit_fee'", (owner.user_id,),
        ).fetchall()
    assert [str(row["id"]) for row in rows] == [str(category["id"])]
    assert fee_figures(owner)[0] == spending + 5_000


def test_an_english_account_books_its_first_fee_as_credit_fee(owner):
    from app.db.pool import db_conn

    owner.db("UPDATE users SET language = 'en' WHERE id = %s", (owner.user_id,))
    add_credit(owner, owed=100_000)
    paid = pay(owner, 105_000, fee=5_000)
    with db_conn() as conn:
        fee = conn.execute(
            """SELECT c.name, c.system_key, c.kakeibo_type, c.is_excluded_from_budget, t.notes FROM transactions t
               JOIN categories c ON c.id = t.category_id WHERE t.id = %s""",
            (paid["fee_transaction_id"],),
        ).fetchone()
    assert (fee["name"], fee["system_key"], fee["kakeibo_type"], fee["is_excluded_from_budget"], fee["notes"]) == (
        "Credit fee", "credit_fee", "unexpected", False, "Credit fee")


# --- Invariant, partial, early and over-payment --------------------------------------------------------


def assert_bills_add_up(owner, key):
    body = overview(owner, key, **{"from": "2000-01-01", "to": "2100-01-01"})
    left = sum(bill["left_to_pay"] for bill in body["bills"])
    balance = accounts_by_id(owner)[owner[key]]["balance"]
    assert left - body["account"]["credit_balance"] == -balance, (key, left, body["account"], balance)
    assert left == body["account"]["owed"]
    return body


def test_partial_early_and_over_payment(owner):
    add_credit(owner)
    tx = purchase(owner, 1_200_000, terms={"installment_count": 6})
    pay(owner, 300_000)
    body = assert_bills_add_up(owner, "credit")
    assert ledger_row(owner, tx)["credit"]["installments_settled"] == 1
    pay(owner, 900_000)
    body = assert_bills_add_up(owner, "credit")
    assert all(bill["status"] == "paid" for bill in body["bills"])
    facts = ledger_row(owner, tx)["credit"]
    assert (facts["installments_settled"], facts["left_to_pay"], facts["settled_on_time"]) == (6, 0, True)
    pay(owner, 50_000)
    body = assert_bills_add_up(owner, "credit")
    assert (body["account"]["owed"], body["account"]["credit_balance"]) == (0, 50_000)
    purchase(owner, 80_000)
    body = assert_bills_add_up(owner, "credit")
    assert (body["account"]["owed"], body["account"]["credit_balance"]) == (30_000, 0)


def test_bills_always_add_up_to_what_is_owed(owner):
    """A seeded random sequence of 500 writes on two credit accounts; the invariant holds after every one."""
    generator = random.Random(20261106)
    add_credit(owner, owed=150_000)
    add_credit(owner, "kredivo", "Kredivo Contoh", owed=0, payment_day=1, statement_day=21)
    purchases: dict[str, list[str]] = {"credit": [], "kredivo": []}
    movements: dict[str, list[str]] = {"credit": [], "kredivo": []}

    def some_day():
        return today() - timedelta(days=generator.randint(0, 90))

    for step in range(500):
        key = generator.choice(("credit", "kredivo"))
        action = generator.choice(("buy", "buy", "terms", "edit", "delete", "refund", "pay", "pay", "split", "unpay"))
        if action == "buy" or not purchases[key] and action in ("terms", "edit", "delete"):
            count = generator.choice((None, 1, 3, 6, 12))
            purchases[key].append(purchase(
                owner, generator.randint(1, 400) * 1_000, some_day(), account=key,
                terms={"installment_count": count} if count else None,
            ))
        elif action == "terms":
            tx = generator.choice(purchases[key])
            if generator.random() < 0.3:
                assert owner.session.delete(f"/api/transactions/{tx}/credit-terms").status_code == 200
            else:
                assert put_terms(owner, tx, installment_count=generator.randint(1, 36)).status_code == 200
        elif action == "edit":
            tx = generator.choice(purchases[key])
            change = generator.choice(({"amount": generator.randint(1, 400) * 1_000}, {"date": at_local(some_day()).isoformat()}))
            assert owner.session.patch(f"/api/transactions/{tx}", json=change).status_code == 200
        elif action == "delete":
            tx = purchases[key].pop(generator.randrange(len(purchases[key])))
            assert owner.session.delete(f"/api/transactions/{tx}").status_code == 200
        elif action == "refund":
            owner.tx("income", generator.randint(1, 50) * 1_000, category="patungan", account=key,
                     when=at_local(some_day()))
        elif action == "pay":
            amount = generator.randint(10, 300) * 1_000
            fee = generator.choice((None, None, generator.randint(1, 5) * 1_000))
            movements[key].append(pay(owner, amount, some_day(), target=key, fee=fee)["movement_id"])
        elif action == "split" and movements[key]:
            movement = movements[key].pop(generator.randrange(len(movements[key])))
            assert owner.session.post(f"/api/movements/{movement}/split").status_code == 200
        elif action == "unpay" and movements[key]:
            movement = movements[key].pop(generator.randrange(len(movements[key])))
            assert owner.session.delete(f"/api/movements/{movement}").status_code == 200
        for checked in ("credit", "kredivo"):
            assert_bills_add_up(owner, checked)
    assert orphan_terms() == 0


# --- Read-time cost (risk R3) ------------------------------------------------------------------------


def test_overview_of_2000_rows_reads_under_300_ms(owner, capsys):
    from app.db.pool import db_conn

    add_credit(owner, owed=500_000)
    generator = random.Random(2000)
    with db_conn() as conn:
        with conn.cursor() as cur:
            for index in range(2_000):
                when = at_local(today() - timedelta(days=generator.randint(0, 700)))
                if index % 5 == 4:
                    cur.execute(
                        """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, source, review_status)
                           VALUES (%s, %s, %s, 'income', %s, %s, 'manual', 'reviewed')""",
                        (owner.user_id, owner["credit"], owner["patungan"], generator.randint(1, 100) * 1_000, when),
                    )
                else:
                    cur.execute(
                        """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, source, review_status)
                           VALUES (%s, %s, %s, 'expense', %s, %s, 'manual', 'reviewed') RETURNING id""",
                        (owner.user_id, owner["credit"], owner["makan"], generator.randint(1, 300) * 1_000, when),
                    )
                    if index % 7 == 0:
                        cur.execute(
                            "INSERT INTO credit_purchase_terms (transaction_id, user_id, installment_count) VALUES (%s, %s, %s)",
                            (cur.fetchone()["id"], owner.user_id, generator.choice((3, 6, 12))),
                        )
        conn.commit()
    overview(owner)
    timings = []
    for _ in range(5):
        started = clock.perf_counter()
        overview(owner)
        timings.append((clock.perf_counter() - started) * 1000)
    median = sorted(timings)[len(timings) // 2]
    with capsys.disabled():
        print(f"\ncredit overview, 2000 rows: median {median:.1f} ms of {[round(value, 1) for value in timings]}")
    assert median < 300
