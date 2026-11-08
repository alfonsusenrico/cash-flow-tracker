"""Credit accounts such as paylater and credit cards (add-credit-accounts).

A credit account is a top-level account of type `credit`. Its balance is negative while money is owed:
purchases are expenses on it, a refund is an income on it, and paying the bill is a movement into it. What
it owes is a liability, never an asset (D3, D4).
"""
from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from app.errors import api_error

CREDIT_TYPE = "credit"
CREDIT_FEE_CATEGORY_NAME = "Biaya kredit"


def is_credit(account: dict[str, Any] | None) -> bool:
    return bool(account) and account.get("type") == CREDIT_TYPE


def credit_owed(balance: int) -> int:
    """What a credit account owes, as a positive amount; an overpaid account owes nothing (D3)."""
    return max(-int(balance), 0)


def over_limit(balance: int, limit: int | None) -> bool:
    return limit is not None and credit_owed(balance) > int(limit)


def refuse(code: str) -> HTTPException:
    return api_error(422, code)


def validate_credit_fields(
    *, account_type: str, parent_id: str | None, initial_balance: int | None,
    credit_limit: int | None, payment_day: int | None,
) -> None:
    """The account rules of D2: a credit account is top-level, never positive at the start, and only it has
    a limit or a payment day; every other type starts at zero or more."""
    if account_type == CREDIT_TYPE:
        if parent_id is not None:
            raise refuse("credit_parent_not_allowed")
        if initial_balance is not None and initial_balance > 0:
            raise refuse("credit_balance_sign")
        return
    if credit_limit is not None or payment_day is not None:
        raise refuse("credit_fields_not_allowed")
    if initial_balance is not None and initial_balance < 0:
        raise refuse("credit_balance_sign")


def ensure_not_credit(account: dict[str, Any] | None, code: str = "credit_account_not_allowed") -> None:
    """Refuse a credit account where only cash can stand: investment funding, debt payments, balance checks."""
    if is_credit(account):
        raise refuse(code)


def ensure_credit_fee_category(cur, user_id: str) -> dict[str, Any]:
    """The owner's "Biaya kredit" expense category, created on first use (D6)."""
    cur.execute(
        """INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type, is_excluded_from_budget)
           VALUES (%s, %s, 'receipt', '#64748b', 'expense', 'unexpected', FALSE)
           ON CONFLICT (user_id, name, kind) DO UPDATE SET name = EXCLUDED.name
           RETURNING id, kakeibo_type""",
        (user_id, CREDIT_FEE_CATEGORY_NAME),
    )
    return cur.fetchone()


def book_credit_fee(cur, user_id: str, credit_account_id: str, movement_id: str, fee: int, date) -> str:
    """Book a credit bill's interest or fee on the credit account, tied to the paying movement so deleting
    the movement removes it. The payment lowers what is owed by its amount less this fee."""
    category = ensure_credit_fee_category(cur, user_id)
    cur.execute(
        """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date, kakeibo_type,
               idempotency_key, fee_for_movement_id, source, review_status, classified_by, classification_confidence)
           VALUES (%s, %s, %s, 'expense', %s, %s, %s, %s, %s, %s, 'movement', 'auto', 'system', NULL) RETURNING id""",
        (user_id, credit_account_id, category["id"], fee, CREDIT_FEE_CATEGORY_NAME, date, category["kakeibo_type"],
         f"credit-fee:{movement_id}", movement_id),
    )
    return str(cur.fetchone()["id"])
