"""Paired phones: QR pairing codes, the code exchange, and the device list (device-pairing capability)."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field, field_validator

from app.services import device_pairing
from app.services.auth import get_client_ip, get_current_user, get_session_user
from app.services.state import rate_limiter

router = APIRouter(prefix="/devices", tags=["Devices"])


class PairRequest(BaseModel):
    # Bounded here against oversized bodies; the code itself is checked after Crockford folding.
    code: str = Field(max_length=32)
    device_id: str = Field(pattern=r"^[A-Za-z0-9._:-]{1,64}$")
    device_name: str = Field(max_length=200)

    @field_validator("device_name")
    @classmethod
    def normalize_device_name(cls, value: str) -> str:
        name = " ".join(value.split())
        if not 1 <= len(name) <= 80 or not name.isprintable():
            raise ValueError("device_name must be 1-80 printable characters")
        return name


@router.post("/pairing-codes")
def create_pairing_code(current_user: dict = Depends(get_session_user)):
    return {"ok": True, **device_pairing.create_pairing_code(current_user["id"])}


@router.post("/pair")
def pair_device(payload: PairRequest, request: Request):
    if rate_limiter.exceeded(
        f"pair:client:{get_client_ip(request)}",
        device_pairing.PAIR_RATE_LIMIT,
        device_pairing.PAIR_RATE_WINDOW,
    ):
        raise HTTPException(
            status_code=429,
            detail={"code": "pairing_rate_limited"},
            headers={"Retry-After": str(device_pairing.PAIR_RATE_WINDOW)},
        )
    return {"ok": True, **device_pairing.exchange_code(payload.code, payload.device_id, payload.device_name)}


@router.get("")
def list_devices(current_user: dict = Depends(get_current_user)):
    return {"ok": True, "devices": device_pairing.list_devices(current_user["id"])}


@router.delete("/{key_id}")
def revoke_device(key_id: UUID, current_user: dict = Depends(get_current_user)):
    device_pairing.revoke_device(current_user["id"], str(key_id))
    return {"ok": True}
