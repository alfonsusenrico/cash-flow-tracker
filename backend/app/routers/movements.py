import json
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
from app.services.investment_topups import reject_topup_movement
from app.services.notification_application import account_label, committed_snapshot

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


from app.services.movement_linkage import (
    ensure_internal_movement_categories as _ensure_internal_movement_categories,
    link_transactions_as_movement,
    movement_note,
)


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
    with db_conn() as conn:
        with conn.cursor() as cur:
            expense_id, income_id = str(payload.expense_transaction_id), str(payload.income_transaction_id)
            cur.execute("SELECT movement_id FROM transactions WHERE user_id = %s AND id = ANY(%s)", (current_user["id"], [expense_id, income_id]))
            for row in cur.fetchall():
                reject_topup_movement(cur, current_user["id"], str(row["movement_id"]) if row["movement_id"] else None)
            cur.execute(
                """SELECT 1 FROM notification_events WHERE user_id = %s AND transaction_id = ANY(%s) LIMIT 1""",
                (current_user["id"], [expense_id, income_id]),
            )
            from_notification = cur.fetchone() is not None
            note = None
            if from_notification:
                cur.execute("SELECT account_id FROM transactions WHERE user_id = %s AND id = %s", (current_user["id"], income_id))
                target = cur.fetchone()
                note = movement_note(cur, current_user["id"], str(target["account_id"])) if target else None
            # Notes written by notification processing describe one leg; a user's own notes are kept.
            result = link_transactions_as_movement(cur, current_user["id"], expense_id, income_id, notes=note)
        conn.commit()
    return result


@router.post("/{movement_id}/split")
def split_movement(movement_id: UUID, current_user: dict = Depends(get_current_user)):
    """Undo a movement link, keeping both legs' accounts, amounts, dates, and notes."""
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            reject_topup_movement(cur, user_id, str(movement_id))
            cur.execute(
                """SELECT id, type, account_id, amount, notes FROM transactions
                   WHERE user_id = %s AND movement_id = %s ORDER BY id FOR UPDATE""",
                (user_id, str(movement_id)),
            )
            legs = cur.fetchall()
            if len(legs) != 2:
                raise HTTPException(status_code=404, detail="Movement not found")
            cur.execute(
                """SELECT a.id, a.name, a.type, a.parent_id, a.instrument_type
                   FROM accounts a WHERE a.user_id = %s
                     AND (a.id = ANY(%s) OR a.id IN (SELECT parent_id FROM accounts
                                                     WHERE user_id = %s AND id = ANY(%s)))""",
                (user_id, [leg["account_id"] for leg in legs], user_id, [leg["account_id"] for leg in legs]),
            )
            accounts = cur.fetchall()
            by_id = {str(row["id"]): row for row in accounts}
            ensure_generic_movement_accounts(by_id[str(legs[0]["account_id"])], by_id[str(legs[1]["account_id"])])
            cur.execute(
                """SELECT id, transaction_id, interpretation FROM notification_events
                   WHERE user_id = %s AND movement_id = %s ORDER BY id FOR UPDATE""",
                (user_id, str(movement_id)),
            )
            events = cur.fetchall()
            recorded_by = {str(event["transaction_id"]): event for event in events if event["transaction_id"]}
            for leg in legs:
                category_id, kakeibo = None, None
                evidence = (recorded_by.get(str(leg["id"])) or {}).get("interpretation") or {}
                if evidence.get("category_id"):
                    cur.execute(
                        """SELECT id FROM categories WHERE user_id = %s AND id = %s AND kind = %s
                           AND is_archived = FALSE""",
                        (user_id, evidence["category_id"], leg["type"]),
                    )
                    if cur.fetchone():
                        category_id = evidence["category_id"]
                        kakeibo = evidence.get("kakeibo") if leg["type"] == "expense" else None
                if category_id:
                    cur.execute(
                        """UPDATE transactions SET movement_id = NULL, movement_role = NULL,
                           category_id = %s, kakeibo_type = %s WHERE user_id = %s AND id = %s""",
                        (category_id, kakeibo, user_id, leg["id"]),
                    )
                else:
                    cur.execute(
                        "UPDATE transactions SET movement_id = NULL, movement_role = NULL WHERE user_id = %s AND id = %s",
                        (user_id, leg["id"]),
                    )
            legs_by_id = {str(leg["id"]): leg for leg in legs}
            for event in events:
                leg = legs_by_id.get(str(event["transaction_id"]))
                if not leg:
                    cur.execute(
                        "UPDATE notification_events SET movement_id = NULL, confirmed_role = NULL WHERE id = %s",
                        (event["id"],),
                    )
                    continue
                label = account_label(by_id[str(leg["account_id"])], accounts)
                key = f"notification:{event['id']}"
                snapshot = committed_snapshot(
                    key=key, kind=leg["type"], description=leg["notes"] or "Transaksi", amount=int(leg["amount"]),
                    source=label if leg["type"] == "expense" else None,
                    target=label if leg["type"] == "income" else None,
                )
                cur.execute(
                    """UPDATE notification_events SET movement_id = NULL, confirmed_role = NULL,
                       result_key = %s, result_snapshot = %s, updated_at = NOW() WHERE id = %s""",
                    (key, json.dumps(snapshot), event["id"]),
                )
        conn.commit()
    return {"ok": True, "movement_id": str(movement_id), "transaction_ids": [str(leg["id"]) for leg in legs]}


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
            reject_topup_movement(cur, user_id, str(movement_id))
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
            reject_topup_movement(cur, user_id, str(movement_id))
            cur.execute(
                "DELETE FROM transactions WHERE user_id = %s AND movement_id = %s RETURNING id",
                (user_id, str(movement_id)),
            )
            deleted = cur.fetchall()
            if len(deleted) != 2:
                raise HTTPException(status_code=404, detail="Movement not found")
            conn.commit()
    return {"ok": True, "movement_id": str(movement_id)}
