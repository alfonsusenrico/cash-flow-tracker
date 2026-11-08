"""Credit bills and oldest-due-first settlement (add-credit-spending-flow D4, D5, D6), without a database.

Each case builds one synthetic credit account's ledger rows and reads its schedule on a fixed day, so the
statuses do not depend on the real clock. "SPayLater Contoh" stands for a paylater account.
"""
from datetime import date
from uuid import uuid4

import pytest

from app.services.credit_bills import (
    CreditInvariantError, build_schedule, purchase_facts,
)
from app.services.system_categories import CREDIT_FEE


def account(*, owed=0, payment_day=11, statement_day=1, created=date(2026, 11, 3), rows=()):
    balance = -owed + sum(row["amount"] if row["type"] == "income" else -row["amount"] for row in rows)
    return {
        "id": "credit-1", "name": "SPayLater Contoh", "type": "credit", "initial_balance": -owed,
        "credit_limit": 2_000_000, "payment_day": payment_day, "statement_day": statement_day,
        "is_archived": False, "created_on": created, "ledger_balance": balance,
    }


def row(kind, amount, on, *, count=None, first_due=None, set_by=None, fee_for=None, key=None):
    tx_type, movement = {
        "purchase": ("expense", None), "fee": ("expense", None), "cash_advance": ("expense", "m"),
        "payment": ("income", "m"), "refund": ("income", None),
    }[kind]
    return {
        "id": str(uuid4()), "type": tx_type, "amount": amount, "local_date": on,
        "movement_id": movement, "movement_role": None, "fee_for_movement_id": fee_for,
        "system_key": key if key else (CREDIT_FEE if kind == "fee" and fee_for is None else None),
        "installment_count": count, "first_due_date": first_due, "first_due_date_set_by": set_by,
    }


def schedule(rows, today, **fields):
    return build_schedule(account(rows=rows, **fields), list(rows), today)


def test_november_purchases_make_the_december_bill():
    dinner, other = row("purchase", 120_000, date(2026, 11, 5)), row("purchase", 80_000, date(2026, 11, 20))
    result = schedule([dinner, other], date(2026, 11, 21))
    assert [bill.as_dict(with_items=False) for bill in result.bills] == [{
        "due_date": "2026-12-11", "statement_date": "2026-12-01", "amount": 200_000, "paid": 0,
        "left_to_pay": 200_000, "status": "open", "settled_on": None,
    }]
    assert result.owed == 200_000


@pytest.mark.parametrize("today,status", [
    (date(2026, 12, 1), "upcoming"), (date(2026, 12, 11), "due_today"), (date(2026, 12, 12), "overdue"),
])
def test_bill_status_follows_the_statement_and_due_dates(today, status):
    result = schedule([row("purchase", 200_000, date(2026, 11, 5))], today)
    assert result.bills[0].status == status


def test_opening_debt_is_due_on_the_first_payment_day():
    result = schedule([], date(2026, 11, 4), owed=350_000)
    [bill] = result.bills
    assert (bill.due_date, bill.amount, bill.status) == (date(2026, 11, 11), 350_000, "upcoming")
    assert bill.items[0].kind == "opening" and result.opening_left() == 350_000


def test_overdue_bill_keeps_what_is_left():
    rows = [row("purchase", 200_000, date(2026, 11, 5)), row("payment", 150_000, date(2026, 12, 10))]
    [bill] = schedule(rows, date(2026, 12, 12)).bills
    assert (bill.status, bill.left_to_pay, bill.amount - bill.left_to_pay) == ("overdue", 50_000, 150_000)


def test_paying_the_december_bill_in_two_parts():
    dinner = row("purchase", 120_000, date(2026, 11, 5))
    other = row("purchase", 80_000, date(2026, 11, 20))
    rows = [dinner, other, row("payment", 120_000, date(2026, 12, 10)), row("payment", 80_000, date(2026, 12, 13))]
    result = schedule(rows, date(2026, 12, 14))
    [bill] = result.bills
    first, second = sorted(bill.items, key=lambda item: item.date)
    assert (first.settled_on, first.settled_on_time) == (date(2026, 12, 10), True)
    assert (second.settled_on, second.settled_on_time) == (date(2026, 12, 13), False)
    assert bill.status == "paid_late" and bill.settled_on == date(2026, 12, 13)


def test_dinner_settled_after_35_days():
    dinner = row("purchase", 120_000, date(2026, 11, 5))
    result = schedule([dinner, row("payment", 120_000, date(2026, 12, 10))], date(2026, 12, 10))
    [item] = result.items
    assert item.as_dict() == {
        "kind": "purchase", "transaction_id": dinner["id"], "date": "2026-11-05", "due_date": "2026-12-11",
        "installment": {"number": 1, "count": 1}, "amount": 120_000, "left_to_pay": 0,
        "settled_on": "2026-12-10", "days_to_settle": 35, "settled_on_time": True,
    }
    assert result.bills[0].status == "paid"


def test_early_payment_in_full_of_six_instalments():
    purchase = row("purchase", 1_200_000, date(2026, 11, 5), count=6)
    result = schedule([purchase, row("payment", 1_200_000, date(2026, 11, 15))], date(2026, 11, 16))
    assert [item.due_date for item in result.items] == [
        date(2026, 12, 11), date(2027, 1, 11), date(2027, 2, 11), date(2027, 3, 11), date(2027, 4, 11), date(2027, 5, 11),
    ]
    assert all((item.amount, item.settled_on, item.settled_on_time) == (200_000, date(2026, 11, 15), True)
               for item in result.items)
    assert result.owed == 0 and all(bill.status == "paid" for bill in result.bills)
    facts = purchase_facts(result, purchase["id"], 6)
    assert (facts["installments_settled"], facts["left_to_pay"], facts["days_to_settle"]) == (6, 0, 10)


def test_partial_payment_settles_the_oldest_instalment_first():
    purchase = row("purchase", 1_000_000, date(2026, 11, 5), count=3)
    result = schedule([purchase, row("payment", 400_000, date(2026, 12, 9))], date(2026, 12, 9))
    assert [item.left_to_pay for item in result.items] == [0, 266_667, 333_333]
    facts = purchase_facts(result, purchase["id"], 3)
    assert facts == {
        "installment_count": 3, "installments_settled": 1, "next_due_date": "2027-01-11", "left_to_pay": 600_000,
        "settled_on": None, "days_to_settle": None, "settled_on_time": None, "due_dates_unavailable": False,
    }


def test_payment_with_a_fee_settles_the_fee_and_the_bill():
    rows = [
        row("purchase", 470_000, date(2026, 11, 5)),
        row("payment", 480_000, date(2026, 12, 10)),
        row("fee", 10_000, date(2026, 12, 10), fee_for="movement-1"),
    ]
    result = schedule(rows, date(2026, 12, 10))
    assert result.owed == 0
    assert all(item.settled_on == date(2026, 12, 10) and item.settled_on_time for item in result.items)
    assert {item.kind for item in result.items} == {"purchase", "fee"}


def test_overpayment_is_kept_for_later_items():
    rows = [row("purchase", 100_000, date(2026, 11, 5)), row("payment", 150_000, date(2026, 11, 6))]
    result = schedule(rows, date(2026, 11, 7))
    assert (result.owed, result.credit_balance) == (0, 50_000)
    later = rows + [row("purchase", 80_000, date(2026, 11, 20))]
    after = schedule(later, date(2026, 11, 21))
    assert after.owed == 30_000 and after.credit_balance == 0
    # The later purchase is covered first by the overpayment, never before its own date.
    newest = max(after.items, key=lambda item: item.date)
    assert newest.left_to_pay == 30_000


def test_a_payment_before_a_purchase_settles_it_on_the_purchase_day():
    rows = [row("payment", 100_000, date(2026, 11, 1)), row("purchase", 100_000, date(2026, 11, 5))]
    [item] = schedule(rows, date(2026, 11, 6)).items
    assert (item.settled_on, item.days_to_settle) == (date(2026, 11, 5), 0)


def test_a_refund_settles_the_oldest_due_item_and_is_not_a_payment():
    rows = [
        row("purchase", 300_000, date(2026, 11, 5), count=3),
        row("refund", 100_000, date(2026, 11, 8)),
    ]
    result = schedule(rows, date(2026, 11, 9))
    assert [item.left_to_pay for item in result.items] == [0, 100_000, 100_000]
    assert [credit.kind for credit in result.credits] == ["refund"]


def test_a_cash_advance_is_due_like_a_purchase():
    rows = [row("cash_advance", 500_000, date(2026, 11, 5))]
    [item] = schedule(rows, date(2026, 11, 6)).items
    assert (item.kind, item.due_date) == ("cash_advance", date(2026, 12, 11))


def test_without_a_payment_day_no_bills_are_computed():
    result = schedule([row("purchase", 120_000, date(2026, 11, 5))], date(2026, 11, 6), payment_day=None, owed=50_000)
    assert not result.due_dates_available and result.bills == [] and result.owed == 170_000
    facts = purchase_facts(result, "missing", 1)
    assert facts["due_dates_unavailable"] is True and facts["left_to_pay"] is None


def test_a_broken_invariant_raises():
    rows = [row("purchase", 120_000, date(2026, 11, 5))]
    broken = account(rows=rows)
    broken["ledger_balance"] = -100_000
    with pytest.raises(CreditInvariantError):
        build_schedule(broken, rows, date(2026, 11, 6))
