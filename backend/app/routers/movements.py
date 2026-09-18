from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.db.pool import db_conn
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


def _ensure_internal_movement_categories(cur, user_id: str) -> tuple[str, str]:
    """Ensures both expense and income categories for 'Internal Movement' exist.
    Returns (expense_category_id, income_category_id).
    """
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

            # 2. Get/seed internal movement categories
            expense_cat_id, income_cat_id = _ensure_internal_movement_categories(cur, user_id)

            # 3. Idempotency keys if specified (ensure total length never exceeds varchar(64) limit)
            if payload.idempotency_key:
                base_key = payload.idempotency_key[:58]
                out_key = f"{base_key}:out"
                in_key = f"{base_key}:in"
            else:
                out_key = None
                in_key = None

            if payload.idempotency_key:
                cur.execute(
                    "SELECT id FROM transactions WHERE user_id = %s AND idempotency_key = %s",
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
                        "expense_transaction_id": str(existing_out["id"]),
                        "income_transaction_id": str(existing_in["id"]) if existing_in else None,
                        "idempotent": True,
                        "message": "Movement already recorded (idempotent)",
                    }

            # 4. Insert outbound expense transaction FIRST
            target_acc = next((a for a in accounts if str(a["id"]) == target_id), None)
            is_target_investment = bool(
                target_acc and (target_acc.get("type") == "investment" or target_acc.get("instrument_type"))
            )
            kakeibo_val = "saving" if is_target_investment else None

            cur.execute(
                """
                INSERT INTO transactions (
                    user_id, account_id, category_id, type, amount, notes, date, kakeibo_type, idempotency_key
                )
                VALUES (%s, %s, %s, 'expense', %s, %s, %s, %s, %s)
                RETURNING id, created_at
                """,
                (
                    user_id,
                    source_id,
                    expense_cat_id,
                    payload.amount,
                    notes_val,
                    tx_date,
                    kakeibo_val,
                    out_key,
                ),
            )
            out_row = cur.fetchone()
            expense_id = str(out_row["id"])

            # 5. Insert inbound income transaction SECOND
            cur.execute(
                """
                INSERT INTO transactions (
                    user_id, account_id, category_id, type, amount, notes, date, kakeibo_type, idempotency_key
                )
                VALUES (%s, %s, %s, 'income', %s, %s, %s, NULL, %s)
                RETURNING id, created_at
                """,
                (
                    user_id,
                    target_id,
                    income_cat_id,
                    payload.amount,
                    notes_val,
                    tx_date,
                    in_key,
                ),
            )
            in_row = cur.fetchone()
            income_id = str(in_row["id"])

            conn.commit()

    return {
        "ok": True,
        "expense_transaction_id": expense_id,
        "income_transaction_id": income_id,
        "message": f"Moved {payload.amount} from {found_accounts[source_id]} to {found_accounts[target_id]}",
    }
