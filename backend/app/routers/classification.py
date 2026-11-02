"""Lengkapi klasifikasi routes: list spending without a pillar and fix it through the owner-edit path."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.db.pool import db_conn
from app.services.auth import get_current_user
from app.services.classification_gaps import (
    FIX_ROW_LIMIT,
    ClassificationError,
    classification_gaps,
    set_category_pillar,
    set_transaction_pillars,
)
from app.services.kakeibo import PILLAR_PATTERN

router = APIRouter(prefix="/classification", tags=["Classification"])


class PillarInput(BaseModel):
    kakeibo_type: str = Field(pattern=PILLAR_PATTERN)


class TransactionPillarsInput(PillarInput):
    transaction_ids: list[UUID] = Field(min_length=1, max_length=FIX_ROW_LIMIT)


def error_response(error: ClassificationError) -> HTTPException:
    return HTTPException(status_code=error.status, detail=error.detail)


@router.get("/gaps")
def get_classification_gaps(
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    current_user: dict = Depends(get_current_user),
):
    """Categories without a pillar and unclassified expenses, in a window or over the whole ledger."""
    if (from_date is None) != (to_date is None):
        raise HTTPException(status_code=422, detail={"code": "incomplete_window", "message": "Isi tanggal awal dan akhir."})
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            gaps = classification_gaps(cur, current_user["id"], from_date, to_date)
    return {"ok": True, **gaps}


@router.post("/categories/{category_id}")
def classify_category(category_id: UUID, payload: PillarInput, current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                result = set_category_pillar(cur, current_user["id"], str(category_id), payload.kakeibo_type)
            except ClassificationError as error:
                raise error_response(error) from None
        conn.commit()
    return {"ok": True, **result}


@router.post("/transactions")
def classify_transactions(payload: TransactionPillarsInput, current_user: dict = Depends(get_current_user)):
    ids = list(dict.fromkeys(str(value) for value in payload.transaction_ids))
    with db_conn() as conn:
        with conn.cursor() as cur:
            try:
                result = set_transaction_pillars(cur, current_user["id"], ids, payload.kakeibo_type)
            except ClassificationError as error:
                raise error_response(error) from None
        conn.commit()
    return {"ok": True, **result}
