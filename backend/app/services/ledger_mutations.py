from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import uuid4


from app.errors import ErrorCode, api_error
from app.services.account_roles import TABUNGAN
from app.services.debt_allocations import load_allocation_rows, lock_debts, reverse_allocations
from app.services.provenance import Provenance


LIQUID_ACCOUNT_TYPES = {"cash", "bank", "ewallet", "wallet"}
# A movement links cash accounts, or cash and a credit account: a bill payment or a cash advance
# (add-credit-accounts D6). ensure_generic_movement_accounts refuses credit on both sides.
MOVEMENT_ACCOUNT_TYPES = LIQUID_ACCOUNT_TYPES | {"credit"}


def lock_owned_accounts(
    cur, user_id: str, account_ids: list[str], *, include_archived: bool = False
) -> dict[str, dict[str, Any]]:
    ordered_ids = sorted(set(account_ids))
    cur.execute(
        """
        SELECT a.id, a.user_id, a.name, a.type, a.instrument_type,
               a.initial_balance, a.units, a.avg_buy_price, a.last_price,
               a.parent_id, a.default_pocket_id, a.default_funding_account_id, a.instrument_symbol, a.is_archived,
               a.investment_tracking_mode, a.investment_cost_basis, a.credit_limit, a.payment_day, a.statement_day, a.role,
               EXISTS (SELECT 1 FROM accounts child WHERE child.parent_id = a.id) AS has_children,
               a.reconciliation_required,
               EXISTS (
                   SELECT 1 FROM goal_accounts ga WHERE ga.account_id = a.id
               ) AS is_savings
        FROM accounts a
        WHERE a.user_id = %s AND a.id = ANY(%s) AND (a.is_archived = FALSE OR %s)
        ORDER BY a.id
        FOR UPDATE
        """,
        (user_id, ordered_ids, include_archived),
    )
    rows = cur.fetchall()
    accounts = {str(row["id"]): row for row in rows}
    if len(accounts) != len(ordered_ids):
        raise api_error(404, ErrorCode.ACCOUNT_NOT_FOUND)
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
        raise api_error(404, ErrorCode.ACCOUNT_NOT_FOUND)
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
        raise api_error(409, ErrorCode.INSUFFICIENT_FUNDS, {"required_amount": required, "available_amount": available}, extra={"required_amount": required, "available_amount": available, "account_id": str(account['id'])})
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
        raise api_error(422, ErrorCode.INVESTMENT_TRADE_REQUIRED)
    if source_account.get("type") == "credit" and target_account.get("type") == "credit":
        # A payment or an advance moves cash; one credit account cannot pay another (add-credit-accounts D6).
        raise api_error(422, ErrorCode.CREDIT_MOVEMENT_NEEDS_CASH)


def is_savings_destination(account: dict[str, Any]) -> bool:
    """Role tabungan, a goal link (`is_savings`) or an investment product (organize-accounts-by-use D3)."""
    return (
        account.get("type") == "investment"
        or bool(account.get("instrument_type"))
        or bool(account.get("is_savings"))
        or account.get("role") == TABUNGAN
    )


def movement_kakeibo(source: dict[str, Any], target: dict[str, Any], *, is_trade: bool = False) -> str | None:
    """The stored pillar of a movement's outbound leg, for readers such as the ledger's pillar filter.

    Kakeibo's saving figure does not read it: it classifies movements from the accounts' current roles.
    """
    if is_trade:
        return None
    if is_savings_destination(source) != is_savings_destination(target):
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
    is_investment_topup: bool = False,
    movement_id: str | None = None,
    allow_negative: bool = False,
    provenance: Provenance,
) -> dict[str, str]:
    if source_id == target_id:
        raise api_error(400, ErrorCode.SAME_ACCOUNT)
    if is_investment_topup:
        from app.services.investment_topups import validate_topup_accounts

        validate_topup_accounts(source_account, target_account)
    elif not is_trade:
        ensure_generic_movement_accounts(source_account, target_account)
    if not allow_negative:
        ensure_sufficient_funds(cur, user_id, source_account, amount)

    movement_id = movement_id or str(uuid4())
    base_key = idempotency_key[:118] if idempotency_key else None
    out_key = f"{base_key}:out" if base_key else None
    in_key = f"{base_key}:in" if base_key else None
    kakeibo = movement_kakeibo(source_account, target_account, is_trade=is_trade)

    cur.execute(
        """
        INSERT INTO transactions (
            user_id, account_id, category_id, obligation_id, type, amount, notes, date,
            recurring_rule_id, kakeibo_type, idempotency_key, movement_id, movement_role,
            source, review_status, classified_by, classification_confidence
        ) VALUES (%s, %s, %s, %s, 'expense', %s, %s, %s, %s, %s, %s, %s, 'outbound', %s, %s, %s, %s)
        RETURNING id
        """,
        (
            user_id, source_id, expense_category_id, obligation_id, amount, notes, tx_date,
            recurring_rule_id, kakeibo, out_key, movement_id, *provenance.columns(),
        ),
    )
    outbound_id = str(cur.fetchone()["id"])
    cur.execute(
        """
        INSERT INTO transactions (
            user_id, account_id, category_id, obligation_id, type, amount, notes, date,
            recurring_rule_id, kakeibo_type, idempotency_key, movement_id, movement_role,
            source, review_status, classified_by, classification_confidence
        ) VALUES (%s, %s, %s, %s, 'income', %s, %s, %s, %s, NULL, %s, %s, 'inbound', %s, %s, %s, %s)
        RETURNING id
        """,
        (
            user_id, target_id, income_category_id, obligation_id, amount, notes, tx_date,
            recurring_rule_id, in_key, movement_id, *provenance.columns(),
        ),
    )
    inbound_id = str(cur.fetchone()["id"])
    return {
        "movement_id": movement_id,
        "expense_transaction_id": outbound_id,
        "income_transaction_id": inbound_id,
    }


def delete_transaction_rows(cur, user_id: str, transaction_id: str) -> dict[str, Any]:
    """Delete one plain transaction and reverse its debt effects; balances follow from the remaining rows.

    Returns the deleted row's type, amount, links and receipt path; the caller removes the receipt file
    after its commit.
    """
    cur.execute(
        "SELECT type, amount, goal_id, obligation_id, movement_id, receipt_path FROM transactions WHERE user_id = %s AND id = %s FOR UPDATE",
        (user_id, transaction_id),
    )
    tx = cur.fetchone()
    if not tx:
        raise api_error(404, ErrorCode.TRANSACTION_NOT_FOUND)
    if tx.get("movement_id"):
        raise api_error(409, ErrorCode.LINKED_MOVEMENT_ENDPOINT)

    allocation_rows = load_allocation_rows(cur, transaction_id)
    allocations = [
        (str(row["obligation_id"]), int(row["amount"]))
        for row in allocation_rows
    ]
    if allocations:
        lock_debts(cur, user_id, [debt_id for debt_id, _ in allocations])
        reverse_allocations(cur, user_id, allocations)

    # Reverse obligation adjustment
    if tx["obligation_id"]:
        if tx["type"] == "expense":
            cur.execute(
                """
                UPDATE obligations
                SET remaining_amount = remaining_amount + %s,
                    is_archived = CASE WHEN (remaining_amount + %s) > 0 THEN false ELSE is_archived END,
                    updated_at = NOW()
                WHERE id = %s AND user_id = %s
                """,
                (tx["amount"], tx["amount"], str(tx["obligation_id"]), user_id),
            )
        elif tx["type"] == "income":
            cur.execute(
                """
                UPDATE obligations
                SET remaining_amount = GREATEST(0, remaining_amount - %s),
                    is_archived = CASE WHEN (remaining_amount - %s) <= 0 THEN true ELSE is_archived END,
                    updated_at = NOW()
                WHERE id = %s AND user_id = %s
                """,
                (tx["amount"], tx["amount"], str(tx["obligation_id"]), user_id),
            )

    cur.execute(
        "DELETE FROM transactions WHERE user_id = %s AND id = %s",
        (user_id, transaction_id),
    )
    return tx


def delete_movement_rows(cur, user_id: str, movement_id: str) -> None:
    """Delete both legs of a movement and the transfer fee recorded with its outbound debit."""
    cur.execute(
        "DELETE FROM transactions WHERE user_id = %s AND movement_id = %s RETURNING id",
        (user_id, movement_id),
    )
    if len(cur.fetchall()) != 2:
        raise api_error(404, ErrorCode.MOVEMENT_NOT_FOUND)
    cur.execute(
        """DELETE FROM transactions
           WHERE user_id = %s AND fee_for_movement_id = %s AND movement_id IS NULL AND type = 'expense'""",
        (user_id, movement_id),
    )
