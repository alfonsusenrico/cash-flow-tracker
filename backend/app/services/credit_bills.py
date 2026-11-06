"""Credit bills, computed at read time from a credit account's ledger (add-credit-spending-flow D4, D5, D6).

Items are what the account was charged: purchase instalments, fees, cash advances and the debt it was added
with. Credits are what lowered it: payments (movements into it) and refunds (incomes on it). Credits settle
items oldest due first, then by date, so partial, early and over-payments need no link. Bills group items
per due date. What is left to pay always equals what the account owes; a violated invariant raises.
"""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from itertools import accumulate
from typing import Any
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.services.credit_accounts import CREDIT_TYPE, credit_owed
from app.services.credit_terms import (
    SYSTEM, Terms, derived_first_due, installment_amounts, installment_due_dates, opening_due, statement_date,
)
from app.services.system_categories import CREDIT_FEE

PURCHASE = "purchase"
FEE = "fee"
CASH_ADVANCE = "cash_advance"
OPENING = "opening"
PAYMENT = "payment"
REFUND = "refund"

OPEN = "open"
UPCOMING = "upcoming"
DUE_TODAY = "due_today"
OVERDUE = "overdue"
PAID = "paid"
PAID_LATE = "paid_late"
BILL_STATUSES = (OPEN, UPCOMING, DUE_TODAY, OVERDUE, PAID, PAID_LATE)
UNPAID_STATUSES = (OPEN, UPCOMING, DUE_TODAY, OVERDUE)


class CreditInvariantError(RuntimeError):
    """What the bills leave to pay differs from what the account owes: a schedule rule is wrong."""


@dataclass
class Item:
    kind: str
    transaction_id: str | None
    date: date
    due_date: date
    amount: int
    number: int = 1
    count: int = 1
    left_to_pay: int = 0
    settled_on: date | None = None

    @property
    def days_to_settle(self) -> int | None:
        return (self.settled_on - self.date).days if self.settled_on else None

    @property
    def settled_on_time(self) -> bool | None:
        return self.settled_on <= self.due_date if self.settled_on else None

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "transaction_id": self.transaction_id,
            "date": self.date.isoformat(),
            "due_date": self.due_date.isoformat(),
            "installment": {"number": self.number, "count": self.count} if self.kind == PURCHASE else None,
            "amount": self.amount,
            "left_to_pay": self.left_to_pay,
            "settled_on": self.settled_on.isoformat() if self.settled_on else None,
            "days_to_settle": self.days_to_settle,
            "settled_on_time": self.settled_on_time,
        }


@dataclass
class Credit:
    kind: str
    transaction_id: str
    date: date
    amount: int


@dataclass
class Bill:
    due_date: date
    statement_date: date
    items: list[Item]
    status: str = OPEN

    @property
    def amount(self) -> int:
        return sum(item.amount for item in self.items)

    @property
    def left_to_pay(self) -> int:
        return sum(item.left_to_pay for item in self.items)

    @property
    def settled_on(self) -> date | None:
        if self.left_to_pay:
            return None
        return max((item.settled_on for item in self.items if item.settled_on), default=None)

    def as_dict(self, *, with_items: bool = True) -> dict[str, Any]:
        result = {
            "due_date": self.due_date.isoformat(),
            "statement_date": self.statement_date.isoformat(),
            "amount": self.amount,
            "paid": self.amount - self.left_to_pay,
            "left_to_pay": self.left_to_pay,
            "status": self.status,
            "settled_on": self.settled_on.isoformat() if self.settled_on else None,
        }
        if with_items:
            result["items"] = [item.as_dict() for item in self.items]
        return result


@dataclass
class Schedule:
    account: dict[str, Any]
    balance: int
    due_dates_available: bool
    items: list[Item] = field(default_factory=list)
    credits: list[Credit] = field(default_factory=list)
    bills: list[Bill] = field(default_factory=list)

    @property
    def owed(self) -> int:
        return credit_owed(self.balance)

    @property
    def credit_balance(self) -> int:
        """Paid beyond everything owed ("Lebih bayar"), kept for later items."""
        return max(self.balance, 0)

    @property
    def next_due(self) -> Bill | None:
        return next((bill for bill in self.bills if bill.left_to_pay > 0), None)

    def opening_left(self) -> int:
        """Debt owed before tracking that is still unpaid: the only credit bill that never was spending (D9)."""
        return sum(item.left_to_pay for item in self.items if item.kind == OPENING)

    def purchase_items(self, transaction_id: str) -> list[Item]:
        return [item for item in self.items if item.transaction_id == transaction_id]


def row_role(row: dict[str, Any]) -> str:
    """What a ledger row on a credit account is to its bills."""
    if row["type"] == "income":
        return PAYMENT if row["movement_id"] else REFUND
    if row["movement_id"]:
        return CASH_ADVANCE
    if row["fee_for_movement_id"] or row["system_key"] == CREDIT_FEE:
        return FEE
    return PURCHASE


def _terms(row: dict[str, Any]) -> Terms:
    if row.get("installment_count") is None:
        return Terms()
    return Terms(int(row["installment_count"]), row["first_due_date"], row["first_due_date_set_by"])


def _items_for_row(row: dict[str, Any], role: str, statement_day: int | None, payment_day: int | None) -> list[Item]:
    transaction_id = str(row["id"])
    on = row["local_date"]
    amount = int(row["amount"])
    if role == FEE:
        return [Item(FEE, transaction_id, on, on, amount)]
    # A cash advance is due like a purchase, and keeps a due date the system stored for it (D2.7).
    terms = _terms(row) if role == PURCHASE else Terms(1, row.get("first_due_date"), row.get("first_due_date_set_by"))
    dates = installment_due_dates(terms, on, statement_day, payment_day)
    if dates is None:
        return []
    amounts = installment_amounts(amount, terms.installment_count)
    count = terms.installment_count
    return [
        Item(role, transaction_id, on, due, part, number=index + 1, count=count)
        for index, (due, part) in enumerate(zip(dates, amounts))
    ]


def settle(items: list[Item], credits: list[Credit]) -> None:
    """Apply credits in date order to items oldest due first (D5).

    An item is settled on the date of the credit at which the running credit total first covers the
    running item total through it, never before the item's own date.
    """
    items.sort(key=lambda item: (item.due_date, item.date, item.transaction_id or "", item.number))
    credits.sort(key=lambda credit: (credit.date, credit.transaction_id))
    covered = list(accumulate(credit.amount for credit in credits))
    total_credit = covered[-1] if covered else 0
    running = 0
    for item in items:
        before = running
        running += item.amount
        item.left_to_pay = item.amount - min(max(total_credit - before, 0), item.amount)
        if item.left_to_pay:
            item.settled_on = None
        elif running == 0:
            item.settled_on = item.date
        else:
            item.settled_on = max(credits[bisect_left(covered, running)].date, item.date)


def bill_status(bill: Bill, today: date) -> str:
    if bill.left_to_pay == 0:
        return PAID if all(item.settled_on_time for item in bill.items) else PAID_LATE
    if today < bill.statement_date:
        return OPEN
    if bill.due_date > today:
        return UPCOMING
    if bill.due_date == today:
        return DUE_TODAY
    return OVERDUE


def build_schedule(account: dict[str, Any], rows: list[dict[str, Any]], today: date) -> Schedule:
    """One credit account's items, credits and bills from all its ledger rows."""
    payment_day = account["payment_day"]
    statement_day = account["statement_day"]
    balance = int(account["ledger_balance"])
    schedule = Schedule(account=account, balance=balance, due_dates_available=payment_day is not None)
    if not schedule.due_dates_available:
        return schedule
    opening = -int(account["initial_balance"])
    if opening > 0:
        created_on = account["created_on"]
        schedule.items.append(Item(OPENING, None, created_on, opening_due(created_on, payment_day), opening))
    for row in rows:
        role = row_role(row)
        if role in (PAYMENT, REFUND):
            schedule.credits.append(Credit(role, str(row["id"]), row["local_date"], int(row["amount"])))
        else:
            schedule.items.extend(_items_for_row(row, role, statement_day, payment_day))
    settle(schedule.items, schedule.credits)

    owed_by_items = sum(item.amount for item in schedule.items) - sum(credit.amount for credit in schedule.credits)
    if owed_by_items != -balance:
        raise CreditInvariantError(f"credit account {account['id']}: items less credits {owed_by_items} != owed {-balance}")
    # Left to pay less any overpayment is what the account owes, signed: an overpaid account owes a negative.
    if sum(item.left_to_pay for item in schedule.items) - schedule.credit_balance != -balance:
        raise CreditInvariantError(f"credit account {account['id']}: left to pay does not equal what is owed")

    by_due: dict[date, list[Item]] = {}
    for item in schedule.items:
        by_due.setdefault(item.due_date, []).append(item)
    for due, items in sorted(by_due.items()):
        bill = Bill(due, statement_date(due, statement_day), items)
        bill.status = bill_status(bill, today)
        schedule.bills.append(bill)
    return schedule


ACCOUNT_SQL = """
    SELECT a.id, a.name, a.type, a.initial_balance, a.credit_limit, a.payment_day, a.statement_day,
           a.is_archived, (a.created_at AT TIME ZONE %s)::date AS created_on,
           a.initial_balance
             + COALESCE((SELECT SUM(CASE WHEN t.type = 'income' THEN t.amount WHEN t.type = 'expense' THEN -t.amount ELSE 0 END)
                         FROM transactions t WHERE t.account_id = a.id), 0) AS ledger_balance
    FROM accounts a
    WHERE a.user_id = %s AND a.type = 'credit'
"""

ROWS_SQL = """
    SELECT t.id, t.account_id, t.type, t.amount, (t.date AT TIME ZONE %s)::date AS local_date,
           t.movement_id, t.movement_role, t.fee_for_movement_id, c.system_key,
           pt.installment_count, pt.first_due_date, pt.first_due_date_set_by
    FROM transactions t
    LEFT JOIN categories c ON c.id = t.category_id
    LEFT JOIN credit_purchase_terms pt ON pt.transaction_id = t.id AND pt.user_id = t.user_id
    WHERE t.user_id = %s AND t.account_id = ANY(%s::uuid[])
"""


def load_credit_accounts(cur, user_id: str, account_ids: list[str] | None = None, *, include_archived: bool = False) -> list[dict[str, Any]]:
    query = ACCOUNT_SQL
    params: list[Any] = [settings.tz, user_id]
    if account_ids is not None:
        query += " AND a.id = ANY(%s::uuid[])"
        params.append(account_ids)
    if not include_archived:
        query += " AND a.is_archived = FALSE"
    cur.execute(query + " ORDER BY a.display_order, a.created_at, a.name", params)
    return cur.fetchall()


def load_schedules(
    cur, user_id: str, today: date, account_ids: list[str] | None = None, *, include_archived: bool = False,
) -> dict[str, Schedule]:
    """Schedules of the owner's credit accounts (or the given ones), from one query for all their rows."""
    accounts = load_credit_accounts(cur, user_id, account_ids, include_archived=include_archived)
    if not accounts:
        return {}
    ids = [str(account["id"]) for account in accounts]
    cur.execute(ROWS_SQL, (settings.tz, user_id, ids))
    rows_by_account: dict[str, list[dict[str, Any]]] = {account_id: [] for account_id in ids}
    for row in cur.fetchall():
        rows_by_account[str(row["account_id"])].append(row)
    return {str(account["id"]): build_schedule(account, rows_by_account[str(account["id"])], today) for account in accounts}


def keep_past_due_dates(cur, user_id: str, account: dict[str, Any], today: date) -> int:
    """Store the derived first due date of every purchase and cash advance already due, before the account's
    bill day or payment day changes (D2), so only bills not yet due move. Returns how many were kept.

    The caller holds the account row lock and calls this with the account's days before the change.
    """
    if account.get("type") != CREDIT_TYPE or account.get("payment_day") is None:
        return 0
    cur.execute(ROWS_SQL, (settings.tz, user_id, [str(account["id"])]))
    kept = 0
    for row in cur.fetchall():
        role = row_role(row)
        if role not in (PURCHASE, CASH_ADVANCE) or row["first_due_date"] is not None:
            continue
        due = derived_first_due(row["local_date"], account.get("statement_day"), account["payment_day"])
        if due is None or due > today:
            continue
        cur.execute(
            """
            INSERT INTO credit_purchase_terms (transaction_id, user_id, installment_count, first_due_date, first_due_date_set_by)
            VALUES (%s, %s, 1, %s, %s)
            ON CONFLICT (transaction_id) DO UPDATE
            SET first_due_date = EXCLUDED.first_due_date, first_due_date_set_by = EXCLUDED.first_due_date_set_by,
                updated_at = NOW()
            """,
            (str(row["id"]), user_id, due, SYSTEM),
        )
        kept += 1
    return kept


def account_summary(schedule: Schedule) -> dict[str, Any]:
    account = schedule.account
    limit = int(account["credit_limit"]) if account["credit_limit"] is not None else None
    return {
        "id": str(account["id"]),
        "name": account["name"],
        "owed": schedule.owed,
        "credit_limit": limit,
        "limit_left": limit - schedule.owed if limit is not None else None,
        "over_limit": limit is not None and schedule.owed > limit,
        "statement_day": account["statement_day"],
        "payment_day": account["payment_day"],
        "credit_balance": schedule.credit_balance,
    }


def next_due_summary(schedule: Schedule) -> dict[str, Any] | None:
    bill = schedule.next_due
    if bill is None:
        return None
    return {"due_date": bill.due_date.isoformat(), "amount": bill.amount, "left_to_pay": bill.left_to_pay, "status": bill.status}


def purchase_facts(schedule: Schedule | None, transaction_id: str, terms_count: int) -> dict[str, Any]:
    """A purchase's instalment and settlement facts for its ledger row (data only, never a score)."""
    items = schedule.purchase_items(transaction_id) if schedule else []
    if not items:
        return {
            "installment_count": terms_count, "installments_settled": 0, "next_due_date": None,
            "left_to_pay": None, "settled_on": None, "days_to_settle": None, "settled_on_time": None,
            "due_dates_unavailable": True,
        }
    items = sorted(items, key=lambda item: item.number)
    settled = [item for item in items if item.left_to_pay == 0]
    unpaid = [item for item in items if item.left_to_pay > 0]
    complete = not unpaid
    settled_on = max(item.settled_on for item in items) if complete else None
    return {
        "installment_count": items[0].count,
        "installments_settled": len(settled),
        "next_due_date": unpaid[0].due_date.isoformat() if unpaid else None,
        "left_to_pay": sum(item.left_to_pay for item in items),
        "settled_on": settled_on.isoformat() if settled_on else None,
        "days_to_settle": (settled_on - items[0].date).days if settled_on else None,
        "settled_on_time": all(item.settled_on_time for item in items) if complete else None,
        "due_dates_unavailable": False,
    }


def local_day(value: datetime) -> date:
    return value.astimezone(ZoneInfo(settings.tz)).date()


def local_today() -> date:
    """Today's local date for bill status and due-date keeping; tests replace it to freeze the clock."""
    return local_day(datetime.now(timezone.utc))


def purchase_installments(schedule: Schedule | None, transaction_id: str) -> list[dict[str, Any]]:
    """A purchase's instalments with their due dates and settlement, for its dialog and the terms response."""
    if schedule is None:
        return []
    bills = {bill.due_date: bill.status for bill in schedule.bills}
    return [
        {**item.as_dict(), "bill_status": bills.get(item.due_date)}
        for item in sorted(schedule.purchase_items(transaction_id), key=lambda item: item.number)
    ]
