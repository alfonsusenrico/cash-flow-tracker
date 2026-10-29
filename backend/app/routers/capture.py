"""Typed transaction capture for the phone companion (text-capture capability)."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.services import text_capture
from app.services.auth import get_current_user
from app.services.kakeibo import Pillar
from app.services.state import rate_limiter

router = APIRouter(prefix="/capture", tags=["Text Capture"])


class CaptureTextIn(BaseModel):
    client_capture_id: UUID
    # Bounded twice: here against oversized bodies, then 1-200 characters after normalisation.
    text: str = Field(max_length=400)
    device_time: datetime


class CaptureAnswerIn(BaseModel):
    question_id: UUID
    reply_id: UUID
    option_id: str | None = Field(default=None, min_length=1, max_length=64)
    text: str | None = Field(default=None, max_length=100)
    account_id: UUID | None = None
    category_id: UUID | None = None
    kakeibo: Pillar | None = None


@router.post("/text")
async def capture_text(payload: CaptureTextIn, request: Request, current_user: dict = Depends(get_current_user)):
    user_id = str(current_user["id"])
    if rate_limiter.exceeded(f"capture:user:{user_id}", text_capture.CAPTURE_RATE_LIMIT,
                             text_capture.CAPTURE_RATE_WINDOW):
        raise HTTPException(status_code=429, detail={"code": "capture_rate_limited"},
                            headers={"Retry-After": str(text_capture.CAPTURE_RATE_WINDOW)})
    capture = await text_capture.submit_capture(
        user_id, payload.model_dump(), getattr(request.app.state, "notification_provider", None),
    )
    return {"ok": True, "capture": capture}


@router.post("/{capture_id}/answer")
def answer_capture(capture_id: UUID, payload: CaptureAnswerIn, current_user: dict = Depends(get_current_user)):
    capture = text_capture.answer_capture(str(current_user["id"]), str(capture_id), payload.model_dump())
    return {"ok": True, "capture": capture}


@router.post("/{capture_id}/undo")
def undo_capture(capture_id: UUID, current_user: dict = Depends(get_current_user)):
    return {"ok": True, "capture": text_capture.undo_capture(str(current_user["id"]), str(capture_id))}


@router.get("/{capture_id}")
def get_capture(capture_id: UUID, current_user: dict = Depends(get_current_user)):
    return {"ok": True, "capture": text_capture.get_capture(str(current_user["id"]), str(capture_id))}
