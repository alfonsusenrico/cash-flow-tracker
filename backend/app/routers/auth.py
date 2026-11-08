from typing import Any, Literal
from fastapi import APIRouter, Depends, Request, Response
from pydantic import BaseModel, Field, field_validator

from app.core.config import settings
from app.errors import ErrorCode, api_error
from app.i18n import current_language, notice, t
from app.db.pool import db_conn
from app.services.auth import (
    authenticate_user,
    generate_api_key,
    get_client_ip,
    get_current_user,
    register_user,
)
from app.services.language import DEFAULT_REGION, LANGUAGES, REGIONS, language_from_accept_language
from app.services.state import rate_limiter

router = APIRouter(tags=["Auth"])


class RegisterRequest(BaseModel):
    username: str = Field(min_length=1, max_length=100)
    password: str = Field(min_length=6)
    invite_code: str
    name: str | None = Field(default=None, max_length=150)
    # The sign-up page's interface language and the chosen region (D11); see `_language` and `_region`.
    language: str | None = Field(default=None, max_length=64)
    region: str | None = Field(default=None, max_length=64)


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
    name_aliases: list[str] | None = Field(default=None, max_length=10)
    language: str | None = Field(default=None, max_length=64)
    region: str | None = Field(default=None, max_length=64)

    @field_validator("name_aliases")
    @classmethod
    def normalize_name_aliases(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        aliases: list[str] = []
        seen: set[str] = set()
        for raw in value:
            alias = " ".join(raw.split())
            if not 3 <= len(alias) <= 150:
                raise ValueError("Each alias must be 3-150 characters")
            if alias.casefold() not in seen:
                seen.add(alias.casefold())
                aliases.append(alias)
        return aliases


def _language(value: str) -> str:
    """A supported interface language, or the coded 422 that saves nothing."""
    language = value.strip().lower()
    if language not in LANGUAGES:
        raise api_error(422, ErrorCode.INVALID_LANGUAGE)
    return language


def _region(value: str) -> str:
    """A supported region, or the coded 422 that saves nothing."""
    region = value.strip().upper()
    if region not in REGIONS:
        raise api_error(422, ErrorCode.INVALID_REGION)
    return region


def _too_many_attempts(retry_after: int):
    return api_error(429, ErrorCode.TOO_MANY_ATTEMPTS, headers={"Retry-After": str(retry_after)})


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
        raise _too_many_attempts(settings.register_rate_window)
    # No account yet, so the browser's preference is the fallback for the language (D2, D13).
    if payload.language is not None:
        language = _language(payload.language)
    else:
        language = language_from_accept_language(request.headers.get("accept-language"))
    region = _region(payload.region) if payload.region is not None else DEFAULT_REGION
    user = register_user(
        payload.username, payload.password, payload.invite_code, payload.name, language=language, region=region,
    )
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
        raise _too_many_attempts(settings.login_rate_window)
    user = authenticate_user(payload.username, payload.password)
    rate_limiter.reset(user_key)
    request.session["user_id"] = user["id"]
    request.session["username"] = user["username"]
    return {"ok": True, "user": user}


@router.post("/logout")
def logout(request: Request):
    request.session.clear()
    return {"ok": True, "message": notice("signed_out")}


@router.get("/me")
def get_me(current_user: dict = Depends(get_current_user)):
    return {"ok": True, "user": current_user}


@router.put("/settings")
@router.patch("/settings")
def update_settings(payload: SettingsUpdate, response: Response, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    updates = []
    params = []

    if payload.name is not None:
        if not payload.name.strip():
            raise api_error(422, ErrorCode.DISPLAY_NAME_EMPTY)
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
    if payload.name_aliases is not None:
        updates.append("name_aliases = %s")
        params.append(payload.name_aliases)
    if payload.language is not None:
        updates.append("language = %s")
        params.append(_language(payload.language))
    if payload.region is not None:
        updates.append("region = %s")
        params.append(_region(payload.region))

    if not updates:
        raise api_error(400, ErrorCode.NO_SETTINGS_TO_UPDATE)

    updates.append("updated_at = NOW()")
    params.append(user_id)

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE users
                SET {', '.join(updates)}
                WHERE id = %s
                RETURNING id, username, name, name_aliases, payday_day, currency, language, region,
                          emergency_fund_multiplier, monthly_spending_budget
                """,
                params,
            )
            updated = cur.fetchone()
            conn.commit()

    result: dict = {
        "ok": True,
        "user": {
            "id": str(updated["id"]),
            "username": updated["username"],
            "name": updated.get("name") or updated["username"],
            "payday_day": updated["payday_day"],
            "currency": updated["currency"],
            "language": updated["language"],
            "region": updated["region"],
            "emergency_fund_multiplier": updated.get("emergency_fund_multiplier", 6),
            "monthly_spending_budget": updated.get("monthly_spending_budget"),
            "name_aliases": list(updated.get("name_aliases") or []),
        },
    }
    if payload.payday_day is not None:
        # Kept for older clients and legacy plan dates only; cycles come from confirmed salary receipts.
        response.headers["Deprecation"] = "true"
        result["deprecations"] = [{
            "field": "payday_day",
            "code": "payday_day_deprecated",
            "message": t(current_language(), "errors.payday_day_deprecated"),
        }]
    return result


class PaydayUpdate(BaseModel):
    day: int = Field(ge=1, le=31)


@router.put("/payday")
@router.patch("/payday")
def update_payday(payload: PaydayUpdate, response: Response, current_user: dict = Depends(get_current_user)):
    return update_settings(SettingsUpdate(payday_day=payload.day), response, current_user)



@router.get("/api-key")
def get_api_key_metadata(current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, key_prefix, created_at, last_used_at
                FROM api_keys
                WHERE user_id = %s AND created_via = 'manual' AND revoked_at IS NULL
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
            # Paired phones keep their own keys; only the manual fallback key is replaced.
            cur.execute("DELETE FROM api_keys WHERE user_id = %s AND created_via = 'manual'", (user_id,))
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
        "message": notice("api_key_shown_once"),
    }
