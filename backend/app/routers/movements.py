from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.db.pool import db_conn
from app.services.ledger_mutations import (
    LIQUID_ACCOUNT_TYPES,
    create_bilateral_movement,
    ensure_generic_movement_accounts,
    lock_owned_accounts,
    movement_kakeibo,
)
from app.services.auth import get_current_user

router = APIRouter(prefix="/movements", tags=["Movements"])


class MovementCreate(BaseModel):
    source_account_id: UUID | None = None
    target_account_id: UUID | None = None
    source_account_name: str | None = None
    target_account_name: str | None = None
    amount: int = Field(gt=0)
    notes: str | None = Field(default=None, max_length=500)
    date: datetime | None = None
    idempotency_key: str | None = Field(default=None, max_length=64)


class MovementUpdate(BaseModel):
    source_account_id: UUID
    target_account_id: UUID
    amount: int = Field(gt=0)
    notes: str | None = Field(default=None, max_length=500)
    date: datetime | None = None


class MovementMerge(BaseModel):
    expense_transaction_id: UUID
    income_transaction_id: UUID


def _ensure_internal_movement_categories(cur, user_id: str) -> tuple[str, str]:
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


@router.post("")
def create_movement(payload: MovementCreate, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    from app.routers.transactions import _match_account_by_name, resolve_effective_account

    if payload.source_account_id and payload.target_account_id and payload.source_account_id == payload.target_account_id:
        raise HTTPException(status_code=400, detail="Source and target accounts must be different")

    tx_date = payload.date or datetime.now(timezone.utc)
    notes_val = payload.notes.strip() if payload.notes else "Internal Movement"

    with db_conn() as conn:
        with conn.cursor() as cur:
            # Fetch all user accounts
            cur.execute(
                "SELECT id, parent_id, name, type, instrument_type, instrument_symbol, default_funding_account_id, default_pocket_id FROM accounts WHERE user_id = %s AND is_archived = FALSE",
                (user_id,),
            )
            accounts = cur.fetchall()
            found_accounts = {str(r["id"]): r for r in accounts}

            # 1. Resolve source account
            source_id = None
            matched_source = None
            if payload.source_account_id:
                sid = str(payload.source_account_id)
                matched_source = found_accounts.get(sid)
                if not matched_source:
                    raise HTTPException(status_code=404, detail="Source account not found")
            elif payload.source_account_name:
                matched_source = _match_account_by_name(cur, user_id, payload.source_account_name, accounts)
                if not matched_source:
                    raise HTTPException(status_code=400, detail=f"Source account '{payload.source_account_name}' could not be resolved")
            else:
                raise HTTPException(status_code=422, detail="Either source_account_id or source_account_name must be provided")

            effective_source = resolve_effective_account(cur, user_id, matched_source, accounts)
            if not effective_source:
                raise HTTPException(status_code=404, detail="Effective source pocket not found")
            source_id = str(effective_source["id"])

            # 2. Resolve target account
            target_id = None
            matched_target = None
            if payload.target_account_id:
                tid = str(payload.target_account_id)
                matched_target = found_accounts.get(tid)
                if not matched_target:
                    raise HTTPException(status_code=404, detail="Target account not found")
            elif payload.target_account_name:
                matched_target = _match_account_by_name(cur, user_id, payload.target_account_name, accounts)
                if not matched_target:
                    raise HTTPException(status_code=400, detail=f"Target account '{payload.target_account_name}' could not be resolved")
            else:
                raise HTTPException(status_code=422, detail="Either target_account_id or target_account_name must be provided")

            effective_target = resolve_effective_account(cur, user_id, matched_target, accounts)
            if not effective_target:
                raise HTTPException(status_code=404, detail="Effective target pocket not found")
            target_id = str(effective_target["id"])

            if source_id == target_id:
                raise HTTPException(status_code=400, detail="Source and target accounts must be different")

            # 3. Get/seed internal movement categories
            expense_cat_id, income_cat_id = _ensure_internal_movement_categories(cur, user_id)

            # 3. Idempotency keys if specified.
            if payload.idempotency_key:
                base_key = payload.idempotency_key[:58]
                out_key = f"{base_key}:out"
                in_key = f"{base_key}:in"
            else:
                out_key = None
                in_key = None

            if payload.idempotency_key:
                cur.execute(
                    "SELECT id, movement_id FROM transactions WHERE user_id = %s AND idempotency_key = %s",
                    (user_id, out_key),
                )
                existing_out = cur.fetchone()
                if existing_out:
                    cur.execute(
                        "SELECT id FROM transactions WHERE user_id = %s AND idempotency_key = %s",
                        (user_id, in_key),
                    )
                    existing_in = cur.fetchone()
                    return {
                        "ok": True,
                        "movement_id": str(existing_out["movement_id"]) if existing_out.get("movement_id") else None,
                        "expense_transaction_id": str(existing_out["id"]),
                        "income_transaction_id": str(existing_in["id"]) if existing_in else None,
                        "idempotent": True,
                        "message": "Movement already recorded (idempotent)",
                    }

            locked = lock_owned_accounts(cur, user_id, [source_id, target_id])
            ensure_generic_movement_accounts(locked[source_id], locked[target_id])
            result = create_bilateral_movement(
                cur,
                user_id=user_id,
                source_id=source_id,
                target_id=target_id,
                amount=payload.amount,
                notes=notes_val,
                tx_date=tx_date,
                expense_category_id=expense_cat_id,
                income_category_id=income_cat_id,
                source_account=locked[source_id],
                target_account=locked[target_id],
                idempotency_key=payload.idempotency_key,
            )

            conn.commit()

    return {
        "ok": True,
        **result,
        "message": f"Moved {payload.amount} from {effective_source['name']} to {effective_target['name']}",
    }


@router.post("/merge")
def merge_transactions_as_movement(
    payload: MovementMerge,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    expense_id = str(payload.expense_transaction_id)
    income_id = str(payload.income_transaction_id)
    if expense_id == income_id:
        raise HTTPException(status_code=422, detail="Choose two different transactions")

    with db_conn() as conn:
        with conn.cursor() as cur:
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

            expense_category_id, income_category_id = _ensure_internal_movement_categories(cur, user_id)
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
            conn.commit()

    return {
        "ok": True,
        "movement_id": movement_id,
        "expense_transaction_id": expense_id,
        "income_transaction_id": income_id,
    }


@router.patch("/{movement_id}")
def update_movement(
    movement_id: UUID,
    payload: MovementUpdate,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    source_id = str(payload.source_account_id)
    target_id = str(payload.target_account_id)
    if source_id == target_id:
        raise HTTPException(status_code=400, detail="Source and target accounts must be different")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, movement_role FROM transactions WHERE user_id = %s AND movement_id = %s FOR UPDATE",
                (user_id, str(movement_id)),
            )
            movement_rows = cur.fetchall()
            if {row["movement_role"] for row in movement_rows} != {"outbound", "inbound"}:
                raise HTTPException(status_code=404, detail="Movement not found")
            locked = lock_owned_accounts(cur, user_id, [source_id, target_id])
            ensure_generic_movement_accounts(locked[source_id], locked[target_id])
            from app.services.ledger_mutations import ensure_sufficient_funds

            ensure_sufficient_funds(
                cur,
                user_id,
                locked[source_id],
                payload.amount,
                exclude_movement_id=str(movement_id),
            )
            tx_date = payload.date or datetime.now(timezone.utc)
            notes = payload.notes.strip() if payload.notes else "Internal Movement"
            from app.services.ledger_mutations import movement_kakeibo

            kakeibo = movement_kakeibo(locked[source_id], locked[target_id])
            cur.execute(
                """
                UPDATE transactions
                SET account_id = CASE movement_role WHEN 'outbound' THEN %s::uuid ELSE %s::uuid END,
                    amount = %s, notes = %s, date = %s,
                    kakeibo_type = CASE movement_role WHEN 'outbound' THEN %s ELSE NULL END,
                    updated_at = NOW()
                WHERE user_id = %s AND movement_id = %s
                """,
                (source_id, target_id, payload.amount, notes, tx_date, kakeibo, user_id, str(movement_id)),
            )
            conn.commit()
    return {"ok": True, "movement_id": str(movement_id)}


@router.delete("/{movement_id}")
def delete_movement(movement_id: UUID, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM transactions WHERE user_id = %s AND movement_id = %s RETURNING id",
                (user_id, str(movement_id)),
            )
            deleted = cur.fetchall()
            if len(deleted) != 2:
                raise HTTPException(status_code=404, detail="Movement not found")
            conn.commit()
    return {"ok": True, "movement_id": str(movement_id)}
