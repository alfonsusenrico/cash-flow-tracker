from uuid import uuid4

from fastapi import HTTPException

from app.services.ledger_mutations import (
    LIQUID_ACCOUNT_TYPES, ensure_generic_movement_accounts, lock_owned_accounts, movement_kakeibo,
)


def ensure_internal_movement_categories(cur, user_id: str) -> tuple[str, str]:
    """Ensures both expense and income categories for 'Internal Movement' exist.
    Returns (expense_category_id, income_category_id).
    """
    cur.execute(
        """
        UPDATE categories
        SET is_excluded_from_budget = TRUE, kakeibo_type = NULL
        WHERE user_id = %s AND name = 'Internal Movement' AND is_archived = FALSE
          AND (is_excluded_from_budget = FALSE OR kakeibo_type IS NOT NULL)
        """,
        (user_id,),
    )
    cur.execute(
        """
        SELECT id, kind FROM categories
        WHERE user_id = %s AND name = 'Internal Movement' AND is_archived = FALSE
        """,
        (user_id,),
    )
    rows = cur.fetchall()
    cat_map = {r["kind"]: str(r["id"]) for r in rows}

    expense_cat_id = cat_map.get("expense")
    if not expense_cat_id:
        cur.execute(
            """
            INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type, is_excluded_from_budget)
            VALUES (%s, 'Internal Movement', 'arrows-right-left', '#3b82f6', 'expense', NULL, TRUE)
            ON CONFLICT (user_id, name, kind) DO UPDATE SET is_excluded_from_budget = TRUE, kakeibo_type = NULL
            RETURNING id
            """,
            (user_id,),
        )
        expense_cat_id = str(cur.fetchone()["id"])

    income_cat_id = cat_map.get("income")
    if not income_cat_id:
        cur.execute(
            """
            INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type, is_excluded_from_budget)
            VALUES (%s, 'Internal Movement', 'arrows-right-left', '#3b82f6', 'income', NULL, TRUE)
            ON CONFLICT (user_id, name, kind) DO UPDATE SET is_excluded_from_budget = TRUE, kakeibo_type = NULL
            RETURNING id
            """,
            (user_id,),
        )
        income_cat_id = str(cur.fetchone()["id"])

    return expense_cat_id, income_cat_id


def link_transactions_as_movement(cur, user_id: str, expense_id: str, income_id: str) -> dict:
    if expense_id == income_id:
        raise HTTPException(status_code=422, detail="Choose two different transactions")
    cur.execute(
        """
        SELECT t.id, t.type, t.account_id, t.amount, t.movement_id,
               t.goal_id, t.obligation_id, t.recurring_rule_id,
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
        for row in (expense, income)
    ):
        raise HTTPException(
            status_code=409,
            detail="A linked, scheduled, goal, or debt transaction cannot be merged",
        )

    accounts = lock_owned_accounts(cur, user_id, [source_id, target_id])
    ensure_generic_movement_accounts(accounts[source_id], accounts[target_id])
    if any(account["type"] not in LIQUID_ACCOUNT_TYPES for account in accounts.values()):
        raise HTTPException(status_code=422, detail="Both accounts must be liquid")

    expense_category_id, income_category_id = ensure_internal_movement_categories(cur, user_id)
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
    return {
        "ok": True, "movement_id": movement_id,
        "expense_transaction_id": expense_id, "income_transaction_id": income_id,
    }
