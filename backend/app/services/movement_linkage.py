from uuid import uuid4

from fastapi import HTTPException

from app.services.ledger_mutations import (
    MOVEMENT_ACCOUNT_TYPES, ensure_generic_movement_accounts, lock_owned_accounts, movement_kakeibo,
)
from app.services.merchant_memory import record_owner_corrections, retire_automatic_memory, snapshot_rows
from app.services.provenance import OWNER_EDIT_ASSIGNMENTS
from app.services.system_categories import INTERNAL_MOVEMENT, ensure_system_category


def ensure_internal_movement_categories(cur, user_id: str) -> tuple[str, str]:
    """The owner's expense and income movement categories (key `internal_movement`), created when missing.

    Returns (expense_category_id, income_category_id).
    """
    return (
        ensure_system_category(cur, user_id, INTERNAL_MOVEMENT, "expense"),
        ensure_system_category(cur, user_id, INTERNAL_MOVEMENT, "income"),
    )


SELF_TRANSFER_NOTES = {"income": "Pindah saldo masuk", "expense": "Pindah saldo keluar"}


def movement_note(cur, user_id: str, target_account_id: str) -> str:
    """Neutral description for a linked movement, naming its destination account."""
    cur.execute(
        """SELECT a.name, p.name AS parent_name FROM accounts a
           LEFT JOIN accounts p ON p.id = a.parent_id AND p.user_id = a.user_id
           WHERE a.user_id = %s AND a.id = %s""",
        (user_id, target_account_id),
    )
    row = cur.fetchone()
    if not row:
        return "Pindah saldo"
    label = f"{row['parent_name']} · {row['name']}" if row.get("parent_name") else row["name"]
    return f"Pindah saldo ke {label}"[:160]


def link_transactions_as_movement(
    cur, user_id: str, expense_id: str, income_id: str, *, notes: str | None = None, owner_edit: bool = False,
) -> dict:
    """Link an expense and an income as one movement.

    `owner_edit` is true when the owner merges the rows; automatic pairing of notification legs is
    not an owner classification.
    """
    if expense_id == income_id:
        raise HTTPException(status_code=422, detail="Choose two different transactions")
    cur.execute(
        """
        SELECT t.id, t.type, t.account_id, t.amount, t.movement_id,
               t.goal_id, t.obligation_id, t.recurring_rule_id, t.fee_for_movement_id,
               EXISTS (
                   SELECT 1 FROM transaction_obligation_allocations a
                   WHERE a.transaction_id = t.id
               ) AS has_debt_allocations
        FROM transactions t
        WHERE t.user_id = %s AND t.id = ANY(%s)
        ORDER BY t.id
        FOR UPDATE OF t
        """,
        (user_id, [expense_id, income_id]),
    )
    rows = {str(row["id"]): row for row in cur.fetchall()}
    if len(rows) != 2:
        raise HTTPException(status_code=404, detail="Transaction not found")

    expense = rows[expense_id]
    income = rows[income_id]
    if expense["type"] != "expense" or income["type"] != "income":
        raise HTTPException(
            status_code=422,
            detail="Choose one outgoing and one incoming transaction",
        )
    if expense["amount"] <= 0 or expense["amount"] != income["amount"]:
        raise HTTPException(
            status_code=422,
            detail="Both transactions must have the same positive amount",
        )
    source_id = str(expense["account_id"])
    target_id = str(income["account_id"])
    if source_id == target_id:
        raise HTTPException(status_code=422, detail="Accounts must be different")
    if any(
        row["movement_id"]
        or row["goal_id"]
        or row["obligation_id"]
        or row["recurring_rule_id"]
        or row["has_debt_allocations"]
        or row["fee_for_movement_id"]
        for row in (expense, income)
    ):
        raise HTTPException(
            status_code=409,
            detail="A linked, scheduled, goal, debt, or transfer-fee transaction cannot be merged",
        )

    accounts = lock_owned_accounts(cur, user_id, [source_id, target_id])
    ensure_generic_movement_accounts(accounts[source_id], accounts[target_id])
    if any(account["type"] not in MOVEMENT_ACCOUNT_TYPES for account in accounts.values()):
        raise HTTPException(status_code=422, detail="Both accounts must be liquid, or one may be a credit account")

    expense_category_id, income_category_id = ensure_internal_movement_categories(cur, user_id)
    before_classification = snapshot_rows(cur, user_id, [expense_id, income_id]) if owner_edit else {}
    movement_id = str(uuid4())
    cur.execute(
        """
        UPDATE transactions
        SET movement_id = %s, movement_role = 'outbound',
            category_id = %s, kakeibo_type = %s
        WHERE user_id = %s AND id = %s
        """,
        (
            movement_id,
            expense_category_id,
            movement_kakeibo(accounts[source_id], accounts[target_id]),
            user_id,
            expense_id,
        ),
    )
    cur.execute(
        """
        UPDATE transactions
        SET movement_id = %s, movement_role = 'inbound',
            category_id = %s, kakeibo_type = NULL
        WHERE user_id = %s AND id = %s
        """,
        (movement_id, income_category_id, user_id, income_id),
    )
    if notes:
        cur.execute(
            "UPDATE transactions SET notes = %s WHERE user_id = %s AND movement_id = %s",
            (notes, user_id, movement_id),
        )
    if owner_edit:
        cur.execute(
            f"""UPDATE transactions SET review_status = 'reviewed', {OWNER_EDIT_ASSIGNMENTS}
                WHERE user_id = %s AND movement_id = %s""",
            (user_id, movement_id),
        )
        record_owner_corrections(cur, user_id, before_classification)
    retire_automatic_memory(cur, user_id, [expense_id, income_id])
    return {
        "ok": True, "movement_id": movement_id,
        "expense_transaction_id": expense_id, "income_transaction_id": income_id,
    }
