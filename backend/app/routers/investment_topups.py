from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from app.db.pool import db_conn
from app.services.auth import get_current_user
from app.services.investment_topups import (
    convert_transaction_to_topup,
    create_topup,
    get_topup,
    mutate_topup,
    topup_response,
)

router = APIRouter(tags=["Investment Top-ups"])


class TopupCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_account_id: UUID
    target_account_id: UUID
    amount: int = Field(gt=0, le=2**63 - 1)
    date: AwareDatetime
    notes: str = Field(default="", max_length=500)
    idempotency_key: str = Field(min_length=1, max_length=128, pattern=r"\S")


class TopupFromTransaction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transaction_id: UUID
    target_account_id: UUID


class TopupUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: int | None = Field(default=None, gt=0, le=2**63 - 1)
    date: AwareDatetime | None = None
    notes: str | None = Field(default=None, max_length=500)


@router.post("")
def record_topup(payload: TopupCreate, current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            result = create_topup(
                cur, user_id=current_user["id"], source_id=str(payload.source_account_id),
                target_id=str(payload.target_account_id), amount=payload.amount,
                tx_date=payload.date, notes=payload.notes.strip(), idempotency_key=payload.idempotency_key,
            )
        conn.commit()
    return result


@router.post("/from-transaction")
def convert_recorded_expense(payload: TopupFromTransaction, current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            result = convert_transaction_to_topup(
                cur, user_id=current_user["id"], transaction_id=str(payload.transaction_id),
                target_id=str(payload.target_account_id),
            )
        conn.commit()
    return result


@router.get("/{topup_id}")
def read_topup(topup_id: UUID, current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            return topup_response(cur, get_topup(cur, current_user["id"], str(topup_id)))


@router.patch("/{topup_id}")
def update_topup(topup_id: UUID, payload: TopupUpdate, current_user: dict = Depends(get_current_user)):
    changes = payload.model_dump(exclude_unset=True)
    if not changes or any(value is None for name, value in changes.items() if name != "notes"):
        raise HTTPException(status_code=422, detail="Supply a non-null amount, date, or notes correction")
    with db_conn() as conn:
        with conn.cursor() as cur:
            result = mutate_topup(cur, current_user["id"], str(topup_id), changes=changes)
        conn.commit()
    return result


@router.delete("/{topup_id}")
def delete_topup(topup_id: UUID, current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            result = mutate_topup(cur, current_user["id"], str(topup_id), delete=True)
        conn.commit()
    return result
