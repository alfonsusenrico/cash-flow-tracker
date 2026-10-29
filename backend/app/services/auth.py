import hashlib
import ipaddress
import secrets
from datetime import datetime, timezone
from typing import Any

from fastapi import Depends, HTTPException, Request
from passlib.hash import bcrypt

from app.core.config import settings
from app.db.init_db import seed_user_defaults
from app.db.pool import db_conn


def hash_token(plain: str) -> str:
    return hashlib.sha256(plain.encode("utf-8")).hexdigest()


def generate_api_key() -> tuple[str, str, str]:
    """Returns (plain_token, key_hash, key_prefix)"""
    plain = f"cfk_{secrets.token_urlsafe(32)}"
    key_hash = hash_token(plain)
    key_prefix = plain[:8]
    return plain, key_hash, key_prefix


def get_client_ip(req: Request) -> str:
    if req.client:
        return req.client.host
    return "unknown"


def register_user(
    username: str,
    password: str,
    invite_code: str,
    name: str | None = None,
) -> dict[str, Any]:
    username = username.strip().lower()
    if not username:
        raise HTTPException(status_code=400, detail="Username is required")
    if len(password) < settings.password_min_len:
        raise HTTPException(
            status_code=400,
            detail=f"Password must be at least {settings.password_min_len} characters",
        )
    if invite_code != settings.invite_code:
        raise HTTPException(status_code=400, detail="Invalid invite code")

    password_hash = bcrypt.hash(password)
    plain_key, key_hash, key_prefix = generate_api_key()

    with db_conn() as conn:
        with conn.cursor() as cur:
            # Check unique username
            cur.execute("SELECT id FROM users WHERE username = %s", (username,))
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="Username already exists")

            cur.execute(
                """
                INSERT INTO users (username, name, password_hash, invite_code)
                VALUES (%s, %s, %s, %s)
                RETURNING id, username, name, payday_day, currency, created_at
                """,
                (username, name.strip() if name else None, password_hash, invite_code),
            )
            user = cur.fetchone()
            user_id = str(user["id"])

            # Create default API key
            cur.execute(
                """
                INSERT INTO api_keys (user_id, key_hash, key_prefix)
                VALUES (%s, %s, %s)
                """,
                (user_id, key_hash, key_prefix),
            )
            conn.commit()

    # Seed starter categories and default accounts
    seed_user_defaults(user_id)

    return {
        "id": user_id,
        "username": user["username"],
        "name": user.get("name") or user["username"],
        "payday_day": user["payday_day"],
        "currency": user["currency"],
        "api_key": plain_key,
    }


def authenticate_user(username: str, password: str) -> dict[str, Any]:
    username = username.strip().lower()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, username, password_hash, payday_day, currency FROM users WHERE username = %s",
                (username,),
            )
            user = cur.fetchone()

    if not user or not bcrypt.verify(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    return {
        "id": str(user["id"]),
        "username": user["username"],
        "payday_day": user["payday_day"],
        "currency": user["currency"],
    }


def _user_view(user: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(user["id"]),
        "username": user["username"],
        "name": user.get("name") or user["username"],
        "payday_day": user["payday_day"],
        "currency": user["currency"],
        "emergency_fund_multiplier": user.get("emergency_fund_multiplier", 6),
        "monthly_spending_budget": user.get("monthly_spending_budget"),
        "name_aliases": list(user.get("name_aliases") or []),
    }


def _session_user(request: Request) -> dict[str, Any] | None:
    session = getattr(request, "session", None)
    if not session or "user_id" not in session:
        return None
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, username, name, name_aliases, payday_day, currency, emergency_fund_multiplier, monthly_spending_budget FROM users WHERE id = %s",
                (session["user_id"],),
            )
            user = cur.fetchone()
    return _user_view(user) if user else None


def get_current_user(request: Request) -> dict[str, Any]:
    """Unified auth dependency supporting Session Cookie and Bearer API Token."""
    user = _session_user(request)
    if user:
        return user

    auth_header = request.headers.get("Authorization", "").strip()
    if auth_header.startswith("Bearer "):
        token = auth_header[7:].strip()
        if token:
            token_hash = hash_token(token)
            with db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT u.id, u.username, u.name, u.name_aliases, u.payday_day, u.currency, u.emergency_fund_multiplier, u.monthly_spending_budget, k.id AS key_id
                        FROM api_keys k
                        JOIN users u ON u.id = k.user_id
                        WHERE k.key_hash = %s AND k.revoked_at IS NULL
                        """,
                        (token_hash,),
                    )
                    user = cur.fetchone()
                    if user:
                        cur.execute(
                            "UPDATE api_keys SET last_used_at = NOW() WHERE id = %s",
                            (user["key_id"],),
                        )
                        conn.commit()
                        return _user_view(user)

    raise HTTPException(status_code=401, detail="Not authenticated")


def get_session_user(request: Request) -> dict[str, Any]:
    """Browser-session-only auth for routes that mint credentials.

    A request carrying an Authorization header is refused outright: the CSRF origin check
    skips such requests, so accepting one alongside a session cookie would bypass it.
    """
    if request.headers.get("Authorization", "").strip():
        raise HTTPException(status_code=403, detail={"code": "session_required"})
    user = _session_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user
