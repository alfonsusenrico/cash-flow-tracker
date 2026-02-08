import hashlib
import secrets
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException, Request
from passlib.hash import bcrypt

from app.core.config import settings
from app.db.pool import db_conn
from app.services.state import rate_limiter


def get_client_ip(req: Request) -> str:
    forwarded = req.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    real_ip = req.headers.get("x-real-ip", "")
    if real_ip:
        return real_ip.strip()
    if req.client:
        return req.client.host
    return "unknown"


def require_session_user(req: Request) -> str:
    username = (req.session or {}).get("username")
    if not username:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return username


def _new_api_key() -> tuple[str, str]:
    plain = f"cfk_{secrets.token_urlsafe(32)}"
    key_hash = hashlib.sha256(plain.encode("utf-8")).hexdigest()
    return plain, key_hash


def mask_api_key(plain: str) -> str:
    visible = max(6, len(plain) // 2)
    if visible >= len(plain):
        visible = max(1, len(plain) - 1)
    return plain[:visible] + ("*" * (len(plain) - visible))


def create_api_key(cur, username: str, label: str = "default") -> str:
    plain, key_hash = _new_api_key()
    key_masked = mask_api_key(plain)
    # Keep single-key policy: revoke current active key before creating a new one.
    cur.execute(
        """
        UPDATE api_keys
        SET revoked_at=%s
        WHERE username=%s AND revoked_at IS NULL
        """,
        (datetime.now(timezone.utc), username),
    )
    cur.execute(
        """
        INSERT INTO api_keys (username, key_hash, key_prefix, key_masked, label)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (username, key_hash, plain[:12], key_masked, label),
    )
    return plain


def get_active_api_key(cur, username: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT api_key_id::text AS api_key_id,
               key_masked,
               created_at,
               last_used_at
        FROM api_keys
        WHERE username=%s AND revoked_at IS NULL
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (username,),
    )
    return cur.fetchone()


def parse_bearer_token(req: Request) -> str:
    header = req.headers.get("authorization", "")
    parts = header.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise HTTPException(status_code=401, detail="Missing API key")
    return parts[1].strip()


def get_api_user_by_token(token: str) -> str:
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT k.username
            FROM api_keys k
            WHERE k.key_hash=%s AND k.revoked_at IS NULL
            """,
            (token_hash,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Invalid API key")
        cur.execute(
            "UPDATE api_keys SET last_used_at=%s WHERE key_hash=%s",
            (datetime.now(timezone.utc), token_hash),
        )
        conn.commit()
        return row["username"]


def require_api_user(req: Request) -> str:
    token = parse_bearer_token(req)
    return get_api_user_by_token(token)


def enforce_register_rate_limit(req: Request) -> None:
    client_ip = get_client_ip(req)
    if rate_limiter.exceeded(
        f"register:ip:{client_ip}",
        settings.register_rate_limit,
        settings.register_rate_window,
    ):
        raise HTTPException(status_code=429, detail="Too many registration attempts. Try again later.")


def enforce_login_rate_limit(req: Request, username: str) -> None:
    client_ip = get_client_ip(req)
    if rate_limiter.exceeded(f"login:ip:{client_ip}", settings.login_rate_limit, settings.login_rate_window):
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")
    if rate_limiter.exceeded(
        f"login:user:{username}",
        settings.login_user_rate_limit,
        settings.login_rate_window,
    ):
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again later.")


def enforce_public_rate_limit(req: Request, key: str) -> None:
    key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()[:24]
    client_ip = get_client_ip(req)
    if rate_limiter.exceeded(
        f"public:key:{key_hash}",
        settings.public_rate_limit,
        settings.public_rate_window,
    ):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    if rate_limiter.exceeded(
        f"public:ip:{client_ip}",
        settings.public_rate_limit,
        settings.public_rate_window,
    ):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")


def register_user(cur, data: dict[str, Any]) -> tuple[str, str, str]:
    invite_code = (data.get("invite_code") or "").strip()
    if not settings.invite_code:
        raise HTTPException(status_code=403, detail="Registration disabled")
    if invite_code != settings.invite_code:
        raise HTTPException(status_code=403, detail="Invalid invite code")

    username = (data.get("username") or "").strip()
    password = (data.get("password") or "").strip()
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password required")
    if not settings.username_re.fullmatch(username):
        raise HTTPException(
            status_code=400,
            detail="Invalid username. Use 3-32 chars: letters, numbers, dot, underscore, or hyphen.",
        )
    if len(password) < settings.password_min_len:
        raise HTTPException(status_code=400, detail=f"Password too short (min {settings.password_min_len})")
    if len(password.encode("utf-8")) > 72:
        raise HTTPException(status_code=400, detail="Password too long (max 72 bytes)")

    full_name = (data.get("full_name") or "").strip() or username
    pw_hash = bcrypt.hash(password)

    cur.execute(
        "INSERT INTO users (username, password_hash, full_name) VALUES (%s, %s, %s)",
        (username, pw_hash, full_name),
    )
    api_key = create_api_key(cur, username, "initial")
    return username, full_name, api_key
