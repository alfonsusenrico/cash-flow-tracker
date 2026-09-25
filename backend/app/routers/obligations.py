import datetime
import math
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg.errors import ForeignKeyViolation
from pydantic import BaseModel, Field

from app.db.pool import db_conn
from app.services.auth import get_current_user

router = APIRouter(tags=["Obligations"])


class ObligationCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    total_amount: int = Field(gt=0)
    remaining_amount: int = Field(ge=0)
    due_date: str | None = None  # YYYY-MM-DD
    minimum_payment: int | None = Field(default=None, ge=0)
    notes: str | None = None


class ObligationUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    total_amount: int | None = Field(default=None, gt=0)
    remaining_amount: int | None = Field(default=None, ge=0)
    due_date: str | None = None
    minimum_payment: int | None = Field(default=None, ge=0)
    notes: str | None = None
    is_archived: bool | None = None


def format_obligation_row(r: dict[str, Any]) -> dict[str, Any]:
    total = int(r["total_amount"])
    remaining = int(r["remaining_amount"])
    paid = max(0, total - remaining)
    pct = round((paid / total) * 100, 1) if total > 0 else 0

    min_pay = r["minimum_payment"]
    estimated_months = None
    if min_pay and min_pay > 0 and remaining > 0:
        estimated_months = math.ceil(remaining / min_pay)

    return {
        "id": str(r["id"]),
        "name": r["name"],
        "total_amount": total,
        "remaining_amount": remaining,
        "paid_amount": paid,
        "payoff_percentage": min(100.0, pct),
        "due_date": r["due_date"].isoformat() if r["due_date"] else None,
        "minimum_payment": min_pay,
        "estimated_payoff_months": estimated_months,
        "notes": r["notes"],
        "is_archived": r["is_archived"],
        "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
    }


@router.get("")
def list_obligations(
    include_archived: bool = Query(default=False),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            if include_archived:
                cur.execute(
                    """
                    SELECT id, name, total_amount, remaining_amount, due_date, minimum_payment, notes, is_archived, created_at, updated_at
                    FROM obligations
                    WHERE user_id = %s
                    ORDER BY is_archived ASC, due_date ASC NULLS LAST, remaining_amount DESC
                    """,
                    (user_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT id, name, total_amount, remaining_amount, due_date, minimum_payment, notes, is_archived, created_at, updated_at
                    FROM obligations
                    WHERE user_id = %s AND is_archived = false AND remaining_amount > 0
                    ORDER BY due_date ASC NULLS LAST, remaining_amount DESC
                    """,
                    (user_id,),
                )
            rows = cur.fetchall()
            obligations = [format_obligation_row(r) for r in rows]

            active_obligations = [o for o in obligations if not o["is_archived"] and o["remaining_amount"] > 0]
            total_debt = sum(o["total_amount"] for o in active_obligations)
            total_remaining = sum(o["remaining_amount"] for o in active_obligations)
            total_paid = max(0, total_debt - total_remaining)
            total_min_monthly = sum(o["minimum_payment"] or 0 for o in active_obligations)

            return {
                "ok": True,
                "obligations": obligations,
                "summary": {
                    "total_debt": total_debt,
                    "total_remaining": total_remaining,
                    "total_paid": total_paid,
                    "total_minimum_monthly": total_min_monthly,
                    "overall_payoff_percentage": round((total_paid / total_debt) * 100, 1) if total_debt > 0 else 0,
                    "active_count": len(active_obligations),
                },
            }


@router.post("")
def create_obligation(payload: ObligationCreate, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    name = payload.name.strip()
    d_date = None
    if payload.due_date:
        try:
            d_date = datetime.date.fromisoformat(payload.due_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid due_date format, expected YYYY-MM-DD")
    if payload.remaining_amount > payload.total_amount:
        raise HTTPException(status_code=422, detail="Remaining amount cannot exceed total amount")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM obligations WHERE user_id = %s AND name = %s",
                (user_id, name),
            )
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="Obligation with this name already exists")

            is_archived = payload.remaining_amount <= 0
            cur.execute(
                """
                INSERT INTO obligations (user_id, name, total_amount, remaining_amount, due_date, minimum_payment, notes, is_archived)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, name, total_amount, remaining_amount, due_date, minimum_payment, notes, is_archived, created_at, updated_at
                """,
                (user_id, name, payload.total_amount, payload.remaining_amount, d_date, payload.minimum_payment, payload.notes, is_archived),
            )
            row = cur.fetchone()
            conn.commit()
            return {"ok": True, "obligation": format_obligation_row(row)}


@router.put("/{obligation_id}")
@router.patch("/{obligation_id}")
def update_obligation(
    obligation_id: UUID,
    payload: ObligationUpdate,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    if not payload.model_fields_set:
        raise HTTPException(status_code=400, detail="No fields to update")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, total_amount, remaining_amount, due_date, minimum_payment,
                       notes, is_archived, created_at, updated_at
                FROM obligations
                WHERE id = %s AND user_id = %s
                FOR UPDATE
                """,
                (str(obligation_id), user_id),
            )
            old = cur.fetchone()
            if not old:
                raise HTTPException(status_code=404, detail="Obligation not found")

            total = payload.total_amount if payload.total_amount is not None else int(old["total_amount"])
            remaining = (
                payload.remaining_amount
                if payload.remaining_amount is not None
                else int(old["remaining_amount"])
            )
            if remaining > total:
                raise HTTPException(status_code=422, detail="Remaining amount cannot exceed total amount")

            due_date = old["due_date"]
            if "due_date" in payload.model_fields_set:
                if not payload.due_date:
                    due_date = None
                else:
                    try:
                        due_date = datetime.date.fromisoformat(payload.due_date)
                    except ValueError:
                        raise HTTPException(status_code=400, detail="Invalid due_date format")

            archived = bool(old["is_archived"])
            if payload.remaining_amount is not None:
                archived = remaining == 0
            if payload.is_archived is not None:
                archived = payload.is_archived or remaining == 0

            cur.execute(
                """
                UPDATE obligations
                SET name = %s, total_amount = %s, remaining_amount = %s,
                    due_date = %s, minimum_payment = %s, notes = %s,
                    is_archived = %s, updated_at = NOW()
                WHERE id = %s AND user_id = %s
                RETURNING id, name, total_amount, remaining_amount, due_date,
                          minimum_payment, notes, is_archived, created_at, updated_at
                """,
                (
                    payload.name.strip() if payload.name is not None else old["name"],
                    total,
                    remaining,
                    due_date,
                    payload.minimum_payment if "minimum_payment" in payload.model_fields_set else old["minimum_payment"],
                    payload.notes if "notes" in payload.model_fields_set else old["notes"],
                    archived,
                    str(obligation_id),
                    user_id,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return {"ok": True, "obligation": format_obligation_row(row)}


@router.delete("/{obligation_id}")
def delete_obligation(
    obligation_id: UUID,
    hard_delete: bool = Query(default=False),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            if hard_delete:
                try:
                    cur.execute(
                        "DELETE FROM obligations WHERE id = %s AND user_id = %s RETURNING id",
                        (str(obligation_id), user_id),
                    )
                except ForeignKeyViolation as error:
                    raise HTTPException(
                        status_code=409,
                        detail="Hapus atau pindahkan pembayaran yang terkait sebelum menghapus tagihan ini secara permanen.",
                    ) from error
            else:
                cur.execute(
                    """
                    UPDATE obligations
                    SET is_archived = true, updated_at = NOW()
                    WHERE id = %s AND user_id = %s
                    RETURNING id
                    """,
                    (str(obligation_id), user_id),
                )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Obligation not found")
            conn.commit()
            return {"ok": True, "deleted": True}
