from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4

from fastapi import HTTPException


LIQUID_ACCOUNT_TYPES = {"cash", "bank", "ewallet", "wallet"}


def lock_owned_accounts(cur, user_id: str, account_ids: list[str]) -> dict[str, dict[str, Any]]:
    ordered_ids = sorted(set(account_ids))
    cur.execute(
        """
        SELECT a.id, a.user_id, a.name, a.type, a.instrument_type,
               a.initial_balance, a.units, a.avg_buy_price, a.last_price,
               a.reconciliation_required,
               EXISTS (
                   SELECT 1 FROM goal_accounts ga WHERE ga.account_id = a.id
               ) AS is_savings
        FROM accounts a
        WHERE a.user_id = %s AND a.id = ANY(%s) AND a.is_archived = FALSE
        ORDER BY a.id
        FOR UPDATE
        """,
        (user_id, ordered_ids),
    )
    rows = cur.fetchall()
    accounts = {str(row["id"]): row for row in rows}
    if len(accounts) != len(ordered_ids):
        raise HTTPException(status_code=404, detail="Account not found")
    return accounts


def get_locked_ledger_balance(
    cur,
    user_id: str,
    account_id: str,
    *,
    exclude_movement_id: str | None = None,
) -> int:
    cur.execute(
        """
        SELECT a.initial_balance
             + COALESCE(SUM(CASE WHEN t.type = 'income' THEN t.amount ELSE 0 END), 0)
             - COALESCE(SUM(CASE WHEN t.type = 'expense' THEN t.amount ELSE 0 END), 0) AS balance
        FROM accounts a
        LEFT JOIN transactions t
          ON t.account_id = a.id
         AND (%s::uuid IS NULL OR t.movement_id IS DISTINCT FROM %s::uuid)
        WHERE a.user_id = %s AND a.id = %s
        GROUP BY a.id, a.initial_balance
        """,
        (exclude_movement_id, exclude_movement_id, user_id, account_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Account not found")
    return int(row["balance"] or 0)


def ensure_sufficient_funds(
    cur,
    user_id: str,
    account: dict[str, Any],
    required: int,
    *,
    exclude_movement_id: str | None = None,
) -> int:
    if account.get("type") not in LIQUID_ACCOUNT_TYPES or account.get("instrument_type"):
        return 0
    available = get_locked_ledger_balance(
        cur,
        user_id,
        str(account["id"]),
        exclude_movement_id=exclude_movement_id,
    )
    if available < required:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "insufficient_funds",
                "required_amount": required,
                "available_amount": available,
                "account_id": str(account["id"]),
            },
        )
    return available


def ensure_generic_movement_accounts(
    source_account: dict[str, Any],
    target_account: dict[str, Any],
) -> None:
    investment_position = any(
        account.get("type") == "investment" or account.get("instrument_type")
        for account in (source_account, target_account)
    )
    if investment_position:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "investment_trade_required",
                "message": "Investment positions can only be changed through a Beli/Jual trade.",
            },
        )


def movement_kakeibo(source: dict[str, Any], target: dict[str, Any], *, is_trade: bool = False) -> str | None:
    if is_trade:
        return None
    source_is_savings = (
        source.get("type") == "investment"
        or bool(source.get("instrument_type"))
        or bool(source.get("is_savings"))
    )
    target_is_savings = (
        target.get("type") == "investment"
        or bool(target.get("instrument_type"))
        or bool(target.get("is_savings"))
    )
    if source_is_savings != target_is_savings:
        return "saving"
    return None


def create_bilateral_movement(
    cur,
    *,
    user_id: str,
    source_id: str,
    target_id: str,
    amount: int,
    notes: str,
    tx_date: datetime,
    expense_category_id: str,
    income_category_id: str,
    source_account: dict[str, Any],
    target_account: dict[str, Any],
    idempotency_key: str | None = None,
    recurring_rule_id: str | None = None,
    obligation_id: str | None = None,
    is_trade: bool = False,
    allow_negative: bool = False,
) -> dict[str, str]:
    if source_id == target_id:
        raise HTTPException(status_code=400, detail="Source and target accounts must be different")
    if not is_trade:
        ensure_generic_movement_accounts(source_account, target_account)
    if not allow_negative:
        ensure_sufficient_funds(cur, user_id, source_account, amount)

    movement_id = str(uuid4())
    base_key = idempotency_key[:118] if idempotency_key else None
    out_key = f"{base_key}:out" if base_key else None
    in_key = f"{base_key}:in" if base_key else None
    kakeibo = movement_kakeibo(source_account, target_account, is_trade=is_trade)

    cur.execute(
        """
        INSERT INTO transactions (
            user_id, account_id, category_id, obligation_id, type, amount, notes, date,
            recurring_rule_id, kakeibo_type, idempotency_key, movement_id, movement_role
        ) VALUES (%s, %s, %s, %s, 'expense', %s, %s, %s, %s, %s, %s, %s, 'outbound')
        RETURNING id
        """,
        (
            user_id, source_id, expense_category_id, obligation_id, amount, notes, tx_date,
            recurring_rule_id, kakeibo, out_key, movement_id,
        ),
    )
    outbound_id = str(cur.fetchone()["id"])
    cur.execute(
        """
        INSERT INTO transactions (
            user_id, account_id, category_id, obligation_id, type, amount, notes, date,
            recurring_rule_id, kakeibo_type, idempotency_key, movement_id, movement_role
        ) VALUES (%s, %s, %s, %s, 'income', %s, %s, %s, %s, NULL, %s, %s, 'inbound')
        RETURNING id
        """,
        (
            user_id, target_id, income_category_id, obligation_id, amount, notes, tx_date,
            recurring_rule_id, in_key, movement_id,
        ),
    )
    inbound_id = str(cur.fetchone()["id"])
    return {
        "movement_id": movement_id,
        "expense_transaction_id": outbound_id,
        "income_transaction_id": inbound_id,
    }
