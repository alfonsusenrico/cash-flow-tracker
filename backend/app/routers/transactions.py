from datetime import datetime, timezone
from typing import Any
from uuid import UUID
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field

from app.core.config import settings
from app.db.pool import db_conn
from app.services.auth import get_current_user

router = APIRouter(tags=["Transactions"])


class TransactionCreate(BaseModel):
    account_id: UUID
    type: str = Field(pattern="^(expense|income|transfer)$")
    amount: int = Field(gt=0)
    category_id: UUID | None = None
    transfer_target_account_id: UUID | None = None
    goal_id: UUID | None = None
    obligation_id: UUID | None = None
    notes: str | None = Field(default=None, max_length=500)
    date: datetime | None = None
    receipt_path: str | None = None
    kakeibo_type: str | None = Field(default=None, pattern="^(need|want|saving)$")
    investment_action: str | None = Field(default=None, pattern="^(buy|sell)$")
    units: float | None = Field(default=None, ge=0)
    price_per_unit: float | None = Field(default=None, ge=0)


class TransactionUpdate(BaseModel):
    account_id: UUID | None = None
    category_id: UUID | None = None
    transfer_target_account_id: UUID | None = None
    goal_id: UUID | None = None
    obligation_id: UUID | None = None
    amount: int | None = Field(default=None, gt=0)
    notes: str | None = Field(default=None, max_length=500)
    date: datetime | None = None
    kakeibo_type: str | None = Field(default=None, pattern="^(need|want|saving)$")


@router.get("")
def list_transactions(
    account_id: UUID | None = None,
    category_id: UUID | None = None,
    goal_id: UUID | None = None,
    obligation_id: UUID | None = None,
    type: str | None = Query(default=None, pattern="^(expense|income|transfer)$"),
    kakeibo_type: str | None = Query(default=None, pattern="^(need|want|saving)$"),
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    q: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    conditions = ["t.user_id = %s"]
    params: list[Any] = [user_id]

    if account_id:
        conditions.append("(t.account_id = %s OR t.transfer_target_account_id = %s)")
        params.extend([str(account_id), str(account_id)])
    if category_id:
        conditions.append("t.category_id = %s")
        params.append(str(category_id))
    if goal_id:
        conditions.append("t.goal_id = %s")
        params.append(str(goal_id))
    if obligation_id:
        conditions.append("t.obligation_id = %s")
        params.append(str(obligation_id))
    if type:
        conditions.append("t.type = %s")
        params.append(type)
    if kakeibo_type:
        conditions.append("COALESCE(t.kakeibo_type, c.kakeibo_type, CASE WHEN COALESCE(c.is_primary, TRUE) THEN 'need' ELSE 'want' END) = %s")
        params.append(kakeibo_type)
    if from_date:
        conditions.append("t.date >= %s")
        params.append(from_date)
    if to_date:
        conditions.append("t.date <= %s")
        params.append(to_date)
    if q:
        search_pattern = f"%{q.strip()}%"
        conditions.append("(t.notes ILIKE %s OR c.name ILIKE %s OR g.name ILIKE %s OR o.name ILIKE %s)")
        params.extend([search_pattern, search_pattern, search_pattern, search_pattern])

    where_clause = " AND ".join(conditions)

    with db_conn() as conn:
        with conn.cursor() as cur:
            # Count total matching
            count_query = f"""
                SELECT COUNT(*) AS total
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                LEFT JOIN goals g ON g.id = t.goal_id
                LEFT JOIN obligations o ON o.id = t.obligation_id
                WHERE {where_clause}
            """
            cur.execute(count_query, params)
            total = cur.fetchone()["total"]

            # Select page
            select_query = f"""
                SELECT 
                    t.id,
                    t.account_id,
                    sa.name AS account_name,
                    t.category_id,
                    c.name AS category_name,
                    c.icon AS category_icon,
                    c.color AS category_color,
                    t.goal_id,
                    g.name AS goal_name,
                    t.obligation_id,
                    o.name AS obligation_name,
                    t.type,
                    COALESCE(t.kakeibo_type, c.kakeibo_type, CASE WHEN COALESCE(c.is_primary, TRUE) THEN 'need' ELSE 'want' END) AS kakeibo_type,
                    t.transfer_target_account_id,
                    ta.name AS transfer_target_account_name,
                    t.amount,
                    t.notes,
                    t.date,
                    t.receipt_path,
                    t.created_at
                FROM transactions t
                JOIN accounts sa ON sa.id = t.account_id
                LEFT JOIN accounts ta ON ta.id = t.transfer_target_account_id
                LEFT JOIN categories c ON c.id = t.category_id
                LEFT JOIN goals g ON g.id = t.goal_id
                LEFT JOIN obligations o ON o.id = t.obligation_id
                WHERE {where_clause}
                ORDER BY t.date DESC, t.created_at DESC
                LIMIT %s OFFSET %s
            """
            cur.execute(select_query, params + [limit, offset])
            rows = cur.fetchall()

    return {
        "ok": True,
        "total": total,
        "limit": limit,
        "offset": offset,
        "transactions": [
            {
                "id": str(r["id"]),
                "account_id": str(r["account_id"]),
                "account_name": r["account_name"],
                "category_id": str(r["category_id"]) if r["category_id"] else None,
                "category_name": r["category_name"],
                "category_icon": r["category_icon"],
                "category_color": r["category_color"],
                "goal_id": str(r["goal_id"]) if r["goal_id"] else None,
                "goal_name": r["goal_name"],
                "obligation_id": str(r["obligation_id"]) if r["obligation_id"] else None,
                "obligation_name": r["obligation_name"],
                "type": r["type"],
                "kakeibo_type": r.get("kakeibo_type"),
                "transfer_target_account_id": (
                    str(r["transfer_target_account_id"]) if r["transfer_target_account_id"] else None
                ),
                "transfer_target_account_name": r["transfer_target_account_name"],
                "amount": r["amount"],
                "notes": r["notes"],
                "date": r["date"].isoformat() if r["date"] else None,
                "receipt_path": r["receipt_path"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ],
    }


@router.post("")
def create_transaction(payload: TransactionCreate, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    account_id = str(payload.account_id)
    category_id = str(payload.category_id) if payload.category_id else None
    target_account_id = (
        str(payload.transfer_target_account_id) if payload.transfer_target_account_id else None
    )
    goal_id = str(payload.goal_id) if payload.goal_id else None
    obligation_id = str(payload.obligation_id) if payload.obligation_id else None
    tx_date = payload.date or datetime.now(timezone.utc)

    with db_conn() as conn:
        with conn.cursor() as cur:
            # Validate source account
            cur.execute("SELECT id FROM accounts WHERE user_id = %s AND id = %s", (user_id, account_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Source account not found")

            # Validate transfer target if transfer
            if payload.type == "transfer":
                if not target_account_id:
                    raise HTTPException(
                        status_code=400, detail="Transfer requires a target account"
                    )
                if target_account_id == account_id:
                    raise HTTPException(
                        status_code=400, detail="Cannot transfer to the same account"
                    )
                cur.execute(
                    "SELECT id FROM accounts WHERE user_id = %s AND id = %s",
                    (user_id, target_account_id),
                )
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Target account not found")

            # Validate category if provided
            if category_id:
                cur.execute(
                    "SELECT id FROM categories WHERE user_id = %s AND id = %s",
                    (user_id, category_id),
                )
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Category not found")

            # Validate goal if provided
            if goal_id:
                cur.execute(
                    "SELECT id FROM goals WHERE user_id = %s AND id = %s",
                    (user_id, goal_id),
                )
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Goal not found")

            # Validate obligation if provided
            if obligation_id:
                cur.execute(
                    "SELECT id FROM obligations WHERE user_id = %s AND id = %s",
                    (user_id, obligation_id),
                )
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Obligation not found")

            # Determine kakeibo_type
            kakeibo_val = payload.kakeibo_type
            if payload.investment_action:
                # Investment trade (buy/sell) is capital deployment within portfolio, NOT monthly living budget saving
                kakeibo_val = None
                if not category_id:
                    cur.execute("SELECT id FROM categories WHERE user_id = %s AND name = 'Investasi' LIMIT 1", (user_id,))
                    inv_cat = cur.fetchone()
                    if inv_cat:
                        category_id = inv_cat["id"]
            elif not kakeibo_val and category_id:
                cur.execute(
                    "SELECT kakeibo_type, is_primary FROM categories WHERE id = %s",
                    (category_id,),
                )
                crow = cur.fetchone()
                if crow:
                    kakeibo_val = crow.get("kakeibo_type") or ("need" if crow.get("is_primary", True) else "want")
            elif not kakeibo_val and payload.type == "transfer" and goal_id:
                # Dedicated transfer into a specific financial goal is tracked as saving
                kakeibo_val = "saving"

            # Insert transaction
            cur.execute(
                """
                INSERT INTO transactions (
                    user_id, account_id, category_id, goal_id, obligation_id, type,
                    transfer_target_account_id, amount, notes, date, receipt_path, kakeibo_type
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, created_at
                """,
                (
                    user_id,
                    account_id,
                    category_id,
                    goal_id,
                    obligation_id,
                    payload.type,
                    target_account_id,
                    payload.amount,
                    payload.notes,
                    tx_date,
                    payload.receipt_path,
                    kakeibo_val,
                ),
            )
            created = cur.fetchone()

            # If linked to a goal, update goal's current_amount
            if goal_id:
                if payload.type in ("income", "transfer"):
                    cur.execute(
                        "UPDATE goals SET current_amount = current_amount + %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (payload.amount, goal_id, user_id),
                    )
                elif payload.type == "expense":
                    cur.execute(
                        "UPDATE goals SET current_amount = GREATEST(0, current_amount - %s), updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (payload.amount, goal_id, user_id),
                    )

            # If linked to an obligation, update obligation's remaining_amount
            if obligation_id:
                if payload.type == "expense":
                    cur.execute(
                        "UPDATE obligations SET remaining_amount = GREATEST(0, remaining_amount - %s), updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (payload.amount, obligation_id, user_id),
                    )
                elif payload.type == "income":
                    cur.execute(
                        "UPDATE obligations SET remaining_amount = remaining_amount + %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (payload.amount, obligation_id, user_id),
                    )

            # If investment trade action is specified, update the target instrument's units and avg_buy_price
            if payload.investment_action:
                invest_acc_id = target_account_id if payload.investment_action == "buy" else account_id
                if invest_acc_id:
                    cur.execute(
                        """
                        SELECT id, name, type, instrument_type, instrument_symbol, units, avg_buy_price, last_price
                        FROM accounts
                        WHERE id = %s AND user_id = %s
                        """,
                        (invest_acc_id, user_id),
                    )
                    inv_acc = cur.fetchone()
                    if inv_acc:
                        old_units = float(inv_acc.get("units") or 0)
                        old_avg = float(inv_acc.get("avg_buy_price") or 0)
                        trade_units = float(payload.units or 0)
                        trade_price = float(payload.price_per_unit or 0)

                        if payload.investment_action == "buy":
                            new_units = old_units + trade_units
                            if new_units > 0 and trade_price > 0:
                                new_avg = round(((old_units * old_avg) + (trade_units * trade_price)) / new_units)
                            else:
                                new_avg = old_avg or trade_price

                            cur.execute(
                                """
                                UPDATE accounts
                                SET units = %s,
                                    avg_buy_price = %s,
                                    updated_at = NOW()
                                WHERE id = %s AND user_id = %s
                                """,
                                (new_units, new_avg, invest_acc_id, user_id),
                            )
                        elif payload.investment_action == "sell":
                            new_units = max(0.0, old_units - trade_units)
                            cur.execute(
                                """
                                UPDATE accounts
                                SET units = %s,
                                    updated_at = NOW()
                                WHERE id = %s AND user_id = %s
                                """,
                                (new_units, invest_acc_id, user_id),
                            )

            conn.commit()

    return {
        "ok": True,
        "transaction_id": str(created["id"]),
        "message": "Transaction recorded successfully",
    }


@router.put("/{transaction_id}")
@router.patch("/{transaction_id}")
def update_transaction(
    transaction_id: UUID,
    payload: TransactionUpdate,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    tid = str(transaction_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            # 1. Fetch current transaction
            cur.execute(
                """
                SELECT id, account_id, category_id, goal_id, obligation_id, type,
                       transfer_target_account_id, amount, notes, date, receipt_path, kakeibo_type
                FROM transactions
                WHERE user_id = %s AND id = %s
                """,
                (user_id, tid),
            )
            old_tx = cur.fetchone()
            if not old_tx:
                raise HTTPException(status_code=404, detail="Transaction not found")

            # 2. Reverse previous goal / obligation side-effects
            if old_tx["goal_id"]:
                if old_tx["type"] in ("income", "transfer"):
                    cur.execute(
                        "UPDATE goals SET current_amount = GREATEST(0, current_amount - %s), updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (old_tx["amount"], str(old_tx["goal_id"]), user_id),
                    )
                elif old_tx["type"] == "expense":
                    cur.execute(
                        "UPDATE goals SET current_amount = current_amount + %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (old_tx["amount"], str(old_tx["goal_id"]), user_id),
                    )

            if old_tx["obligation_id"]:
                if old_tx["type"] == "expense":
                    cur.execute(
                        "UPDATE obligations SET remaining_amount = remaining_amount + %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (old_tx["amount"], str(old_tx["obligation_id"]), user_id),
                    )
                elif old_tx["type"] == "income":
                    cur.execute(
                        "UPDATE obligations SET remaining_amount = GREATEST(0, remaining_amount - %s), updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (old_tx["amount"], str(old_tx["obligation_id"]), user_id),
                    )

            # 3. Determine new values (merging payload with old_tx)
            new_amount = payload.amount if payload.amount is not None else old_tx["amount"]
            new_account_id = str(payload.account_id) if payload.account_id is not None else str(old_tx["account_id"])
            new_type = old_tx["type"]

            if "category_id" in payload.model_fields_set:
                new_category_id = str(payload.category_id) if payload.category_id else None
            else:
                new_category_id = str(old_tx["category_id"]) if old_tx["category_id"] else None

            if "transfer_target_account_id" in payload.model_fields_set:
                new_target_account_id = str(payload.transfer_target_account_id) if payload.transfer_target_account_id else None
            else:
                new_target_account_id = str(old_tx["transfer_target_account_id"]) if old_tx["transfer_target_account_id"] else None

            if "goal_id" in payload.model_fields_set:
                new_goal_id = str(payload.goal_id) if payload.goal_id else None
            else:
                new_goal_id = str(old_tx["goal_id"]) if old_tx["goal_id"] else None

            if "obligation_id" in payload.model_fields_set:
                new_obligation_id = str(payload.obligation_id) if payload.obligation_id else None
            else:
                new_obligation_id = str(old_tx["obligation_id"]) if old_tx["obligation_id"] else None

            new_notes = payload.notes if "notes" in payload.model_fields_set else old_tx["notes"]
            new_date = payload.date if payload.date is not None else old_tx["date"]

            if "kakeibo_type" in payload.model_fields_set:
                new_kakeibo_type = payload.kakeibo_type
            elif "category_id" in payload.model_fields_set and new_category_id:
                cur.execute(
                    "SELECT kakeibo_type, is_primary FROM categories WHERE id = %s",
                    (new_category_id,),
                )
                crow = cur.fetchone()
                new_kakeibo_type = (crow.get("kakeibo_type") if crow else None) or ("need" if crow and crow.get("is_primary", True) else "want")
            else:
                new_kakeibo_type = old_tx.get("kakeibo_type")

            # Validate accounts / categories / goals / obligations
            if new_category_id:
                cur.execute("SELECT id FROM categories WHERE user_id = %s AND id = %s", (user_id, new_category_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Category not found")
            if new_goal_id:
                cur.execute("SELECT id FROM goals WHERE user_id = %s AND id = %s", (user_id, new_goal_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Goal not found")
            if new_obligation_id:
                cur.execute("SELECT id FROM obligations WHERE user_id = %s AND id = %s", (user_id, new_obligation_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Obligation not found")

            # 4. Update the transaction row
            cur.execute(
                """
                UPDATE transactions
                SET account_id = %s,
                    category_id = %s,
                    transfer_target_account_id = %s,
                    goal_id = %s,
                    obligation_id = %s,
                    amount = %s,
                    notes = %s,
                    date = %s,
                    kakeibo_type = %s,
                    updated_at = NOW()
                WHERE user_id = %s AND id = %s
                RETURNING id
                """,
                (
                    new_account_id,
                    new_category_id,
                    new_target_account_id,
                    new_goal_id,
                    new_obligation_id,
                    new_amount,
                    new_notes,
                    new_date,
                    new_kakeibo_type,
                    user_id,
                    tid,
                ),
            )

            # 5. Apply new goal / obligation side-effects
            if new_goal_id:
                if new_type in ("income", "transfer"):
                    cur.execute(
                        "UPDATE goals SET current_amount = current_amount + %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (new_amount, new_goal_id, user_id),
                    )
                elif new_type == "expense":
                    cur.execute(
                        "UPDATE goals SET current_amount = GREATEST(0, current_amount - %s), updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (new_amount, new_goal_id, user_id),
                    )

            if new_obligation_id:
                if new_type == "expense":
                    cur.execute(
                        "UPDATE obligations SET remaining_amount = GREATEST(0, remaining_amount - %s), updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (new_amount, new_obligation_id, user_id),
                    )
                elif new_type == "income":
                    cur.execute(
                        "UPDATE obligations SET remaining_amount = remaining_amount + %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (new_amount, new_obligation_id, user_id),
                    )

            conn.commit()

    return {"ok": True, "message": "Transaction updated successfully"}


@router.delete("/{transaction_id}")
def delete_transaction(transaction_id: UUID, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    tid = str(transaction_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Query transaction to reverse any goal/obligation side effects
            cur.execute(
                "SELECT type, amount, goal_id, obligation_id FROM transactions WHERE user_id = %s AND id = %s",
                (user_id, tid),
            )
            tx = cur.fetchone()
            if not tx:
                raise HTTPException(status_code=404, detail="Transaction not found")

            # Reverse goal adjustment
            if tx["goal_id"]:
                if tx["type"] in ("income", "transfer"):
                    cur.execute(
                        "UPDATE goals SET current_amount = GREATEST(0, current_amount - %s), updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (tx["amount"], str(tx["goal_id"]), user_id),
                    )
                elif tx["type"] == "expense":
                    cur.execute(
                        "UPDATE goals SET current_amount = current_amount + %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (tx["amount"], str(tx["goal_id"]), user_id),
                    )

            # Reverse obligation adjustment
            if tx["obligation_id"]:
                if tx["type"] == "expense":
                    cur.execute(
                        "UPDATE obligations SET remaining_amount = remaining_amount + %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (tx["amount"], str(tx["obligation_id"]), user_id),
                    )
                elif tx["type"] == "income":
                    cur.execute(
                        "UPDATE obligations SET remaining_amount = GREATEST(0, remaining_amount - %s), updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (tx["amount"], str(tx["obligation_id"]), user_id),
                    )

            cur.execute(
                "DELETE FROM transactions WHERE user_id = %s AND id = %s",
                (user_id, tid),
            )
            conn.commit()

    return {"ok": True, "message": "Transaction deleted"}


@router.post("/{transaction_id}/receipt")
async def upload_receipt(
    transaction_id: UUID,
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    tid = str(transaction_id)
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Receipt file is empty")

    receipts_dir = Path(settings.receipts_dir).expanduser().resolve()
    user_receipts_dir = receipts_dir / str(user_id)
    user_receipts_dir.mkdir(parents=True, exist_ok=True)

    ext = Path(file.filename or "receipt.jpg").suffix or ".jpg"
    filename = f"{tid}{ext}"
    rel_path = f"{user_id}/{filename}"
    full_path = user_receipts_dir / filename
    full_path.write_bytes(content)

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transactions SET receipt_path = %s WHERE id = %s AND user_id = %s RETURNING id",
                (rel_path, tid, user_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Transaction not found")
            conn.commit()

    return {"ok": True, "receipt_path": rel_path, "message": "Receipt uploaded successfully"}
