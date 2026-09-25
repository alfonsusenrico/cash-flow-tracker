from typing import Any, Literal
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel, Field

from app.core.config import settings
from app.db.pool import db_conn
from app.services.auth import (
    authenticate_user,
    generate_api_key,
    get_client_ip,
    get_current_user,
    register_user,
)
from app.services.state import rate_limiter

router = APIRouter(tags=["Auth"])


class RegisterRequest(BaseModel):
    username: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=6)
    invite_code: str
    name: str | None = Field(default=None, max_length=150)


class LoginRequest(BaseModel):
    username: str = Field(min_length=1)
    password: str = Field(min_length=1)


from app.services.market_data import get_usdidr_rate


class SettingsUpdate(BaseModel):
    name: str | None = Field(default=None, max_length=150)
    payday_day: int | None = Field(default=None, ge=1, le=31)
    currency: Literal["IDR", "USD"] | None = None
    emergency_fund_multiplier: int | None = Field(default=None, ge=1, le=36)
    monthly_spending_budget: int | None = Field(default=None, ge=0)


@router.get("/currency/rates")
async def get_currency_rates():
    usd_rate = await get_usdidr_rate()
    return {
        "ok": True,
        "base": "IDR",
        "rates": {
            "IDR": 1.0,
            "USD": round(1.0 / usd_rate, 6) if usd_rate > 0 else 0.00006,
        },
        "usdidr": usd_rate,
    }


@router.post("/register")
def register(payload: RegisterRequest, request: Request, response: Response):
    client_key = f"register:client:{get_client_ip(request)}"
    if rate_limiter.exceeded(client_key, settings.register_rate_limit, settings.register_rate_window):
        raise HTTPException(
            status_code=429,
            detail="Too many attempts. Please try again later.",
            headers={"Retry-After": str(settings.register_rate_window)},
        )
    user = register_user(payload.username, payload.password, payload.invite_code, payload.name)
    # Store session
    request.session["user_id"] = user["id"]
    request.session["username"] = user["username"]
    return {"ok": True, "user": user}


@router.post("/login")
def login(payload: LoginRequest, request: Request):
    client_ip = get_client_ip(request)
    normalized_username = payload.username.strip().lower()
    client_key = f"login:client:{client_ip}"
    user_key = f"login:user:{normalized_username}"
    exceeded = rate_limiter.exceeded(
        client_key,
        settings.login_rate_limit,
        settings.login_rate_window,
    )
    exceeded = rate_limiter.exceeded(
        user_key,
        settings.login_user_rate_limit,
        settings.login_rate_window,
    ) or exceeded
    if exceeded:
        raise HTTPException(
            status_code=429,
            detail="Too many attempts. Please try again later.",
            headers={"Retry-After": str(settings.login_rate_window)},
        )
    user = authenticate_user(payload.username, payload.password)
    rate_limiter.reset(user_key)
    request.session["user_id"] = user["id"]
    request.session["username"] = user["username"]
    return {"ok": True, "user": user}


@router.post("/logout")
def logout(request: Request):
    request.session.clear()
    return {"ok": True, "message": "Logged out successfully"}


@router.get("/me")
def get_me(current_user: dict = Depends(get_current_user)):
    return {"ok": True, "user": current_user}


@router.put("/settings")
@router.patch("/settings")
def update_settings(payload: SettingsUpdate, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    updates = []
    params = []

    if payload.name is not None:
        if not payload.name.strip():
            raise HTTPException(status_code=422, detail="Display name cannot be empty")
        updates.append("name = %s")
        params.append(payload.name.strip())
    if payload.payday_day is not None:
        updates.append("payday_day = %s")
        params.append(payload.payday_day)
    if payload.currency is not None:
        updates.append("currency = %s")
        params.append(payload.currency.strip().upper())
    if payload.emergency_fund_multiplier is not None:
        updates.append("emergency_fund_multiplier = %s")
        params.append(payload.emergency_fund_multiplier)
    if "monthly_spending_budget" in payload.model_fields_set:
        updates.append("monthly_spending_budget = %s")
        params.append(payload.monthly_spending_budget)

    if not updates:
        raise HTTPException(status_code=400, detail="No settings to update")

    updates.append("updated_at = NOW()")
    params.append(user_id)

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE users
                SET {', '.join(updates)}
                WHERE id = %s
                RETURNING id, username, name, payday_day, currency, emergency_fund_multiplier, monthly_spending_budget
                """,
                params,
            )
            updated = cur.fetchone()
            conn.commit()

    return {
        "ok": True,
        "user": {
            "id": str(updated["id"]),
            "username": updated["username"],
            "name": updated.get("name") or updated["username"],
            "payday_day": updated["payday_day"],
            "currency": updated["currency"],
            "emergency_fund_multiplier": updated.get("emergency_fund_multiplier", 6),
            "monthly_spending_budget": updated.get("monthly_spending_budget"),
        },
    }


class PaydayUpdate(BaseModel):
    day: int = Field(ge=1, le=31)


@router.put("/payday")
@router.patch("/payday")
def update_payday(payload: PaydayUpdate, current_user: dict = Depends(get_current_user)):
    return update_settings(SettingsUpdate(payday_day=payload.day), current_user)



@router.get("/api-key")
def get_api_key_metadata(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, key_prefix, created_at, last_used_at
                FROM api_keys
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (user_id,),
            )
            key = cur.fetchone()

    if not key:
        return {"ok": True, "api_key": None}

    return {
        "ok": True,
        "api_key": {
            "id": str(key["id"]),
            "key_prefix": key["key_prefix"],
            "created_at": key["created_at"].isoformat(),
            "last_used_at": key["last_used_at"].isoformat() if key["last_used_at"] else None,
        },
    }


@router.api_route("/api-key/info", methods=["GET", "POST"])
def api_key_info(current_user: dict = Depends(get_current_user)):
    return {
        "ok": True,
        "user_id": current_user["id"],
        "username": current_user["username"],
        "currency": current_user.get("currency", "IDR"),
        "payday_day": current_user.get("payday_day", 25),
    }



@router.post("/api-key/reset")
def reset_api_key(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    plain_key, key_hash, key_prefix = generate_api_key()

    with db_conn() as conn:
        with conn.cursor() as cur:
            # Delete old keys
            cur.execute("DELETE FROM api_keys WHERE user_id = %s", (user_id,))
            cur.execute(
                """
                INSERT INTO api_keys (user_id, key_hash, key_prefix)
                VALUES (%s, %s, %s)
                RETURNING id, created_at
                """,
                (user_id, key_hash, key_prefix),
            )
            conn.commit()

    return {
        "ok": True,
        "api_key": plain_key,
        "key_prefix": key_prefix,
        "message": "Copy this key now. You will not be able to see it again.",
    }
