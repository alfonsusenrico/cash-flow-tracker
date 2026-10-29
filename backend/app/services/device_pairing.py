"""QR device pairing: single-use pairing codes exchanged for one revocable API key per phone.

Codes and keys are credentials: only their SHA-256 hashes are stored, and logs carry neither.
"""

from __future__ import annotations

import logging
import secrets
from typing import Any
from urllib.parse import quote

from fastapi import HTTPException

from app.core.config import settings
from app.db.pool import db_conn
from app.services.auth import generate_api_key, hash_token

logger = logging.getLogger(__name__)

# Crockford base32: digits and capitals without I, L, O and U, so a code read aloud or typed is unambiguous.
CODE_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
CODE_LENGTH = 10
CODE_TTL_SECONDS = 300
MAX_FAILED_ATTEMPTS = 5
PAIR_RATE_LIMIT = 10
PAIR_RATE_WINDOW = 60
MANUAL_KEY_NAME = "API key manual"

# Crockford decoding folds the letters people confuse with digits; separators are ignored.
_CODE_FOLDING = str.maketrans({"O": "0", "I": "1", "L": "1", "-": None, " ": None})

_LIVE_CODE = (
    "used_at IS NULL AND invalidated_at IS NULL AND expires_at > NOW() "
    f"AND failed_attempts < {MAX_FAILED_ATTEMPTS}"
)


def pairing_code_invalid() -> HTTPException:
    # One response for unknown, expired, used, superseded and burned codes, so none can be told apart.
    return HTTPException(status_code=410, detail={"code": "pairing_code_invalid"})


def generate_code() -> str:
    return "".join(secrets.choice(CODE_ALPHABET) for _ in range(CODE_LENGTH))


def normalize_code(raw: str) -> str | None:
    code = raw.strip().upper().translate(_CODE_FOLDING)
    if len(code) != CODE_LENGTH or any(char not in CODE_ALPHABET for char in code):
        return None
    return code


def qr_payload(code: str) -> str:
    server = quote(settings.pairing_server_url, safe="")
    return f"cashflow://pair?v=1&server={server}&code={code}"


def create_pairing_code(user_id: str) -> dict[str, Any]:
    if settings.pairing_configuration_error:
        raise HTTPException(status_code=503, detail={"code": settings.pairing_configuration_error})
    code = generate_code()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE pairing_codes SET invalidated_at = NOW() WHERE user_id = %s AND {_LIVE_CODE}",
                (user_id,),
            )
            replaced = cur.rowcount
            cur.execute(
                """
                INSERT INTO pairing_codes (user_id, code_hash, expires_at)
                VALUES (%s, %s, NOW() + %s * INTERVAL '1 second')
                RETURNING id, expires_at
                """,
                (user_id, hash_token(code), CODE_TTL_SECONDS),
            )
            row = cur.fetchone()
        conn.commit()
    logger.info("Pairing code created id=%s replaced=%s", row["id"], replaced)
    return {
        "code": code,
        "expires_at": row["expires_at"].isoformat(),
        "expires_in_seconds": CODE_TTL_SECONDS,
        "qr_payload": qr_payload(code),
    }


def _record_failed_attempt(cur) -> int:
    # The phone sends only the code, so a wrong guess cannot be tied to one owner: it counts
    # against every live code, and a code reaching the limit is burned.
    # Rows are locked in id order so two concurrent wrong guesses cannot deadlock.
    cur.execute(
        f"""
        UPDATE pairing_codes SET failed_attempts = failed_attempts + 1
        WHERE id IN (SELECT id FROM pairing_codes WHERE {_LIVE_CODE} ORDER BY id FOR UPDATE)
        RETURNING failed_attempts
        """,
    )
    return sum(1 for row in cur.fetchall() if row["failed_attempts"] >= MAX_FAILED_ATTEMPTS)


def exchange_code(raw_code: str, device_id: str, device_name: str) -> dict[str, Any]:
    code = normalize_code(raw_code)
    plain_key, key_hash, key_prefix = generate_api_key()
    with db_conn() as conn:
        with conn.cursor() as cur:
            pairing = None
            if code is not None:
                # The row lock makes a concurrent second exchange wait, then see the code as used.
                cur.execute(
                    f"""
                    SELECT id, user_id, ({_LIVE_CODE}) AS live
                    FROM pairing_codes
                    WHERE code_hash = %s
                    FOR UPDATE
                    """,
                    (hash_token(code),),
                )
                pairing = cur.fetchone()
            if pairing is None or not pairing["live"]:
                burned = _record_failed_attempt(cur)
                conn.commit()
                logger.info("Pairing exchange rejected burned=%s", burned)
                raise pairing_code_invalid()

            user_id = pairing["user_id"]
            cur.execute(
                "DELETE FROM api_keys WHERE user_id = %s AND device_id = %s RETURNING id",
                (user_id, device_id),
            )
            replaced = cur.rowcount
            cur.execute(
                """
                INSERT INTO api_keys (user_id, key_hash, key_prefix, device_name, device_id, created_via)
                VALUES (%s, %s, %s, %s, %s, 'pairing')
                RETURNING id
                """,
                (user_id, key_hash, key_prefix, device_name, device_id),
            )
            key_id = cur.fetchone()["id"]
            cur.execute(
                "UPDATE pairing_codes SET used_at = NOW(), api_key_id = %s WHERE id = %s",
                (key_id, pairing["id"]),
            )
        conn.commit()
    logger.info("Device paired key_id=%s replaced=%s", key_id, replaced)
    return {"api_key": plain_key, "key_prefix": key_prefix, "device_name": device_name}


def _device_view(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "name": row["device_name"] or MANUAL_KEY_NAME,
        "created_via": row["created_via"],
        "key_prefix": row["key_prefix"],
        "created_at": row["created_at"].isoformat(),
        "last_used_at": row["last_used_at"].isoformat() if row["last_used_at"] else None,
    }


def list_devices(user_id: str) -> list[dict[str, Any]]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, device_name, created_via, key_prefix, created_at, last_used_at
                FROM api_keys
                WHERE user_id = %s AND revoked_at IS NULL
                ORDER BY created_at DESC, id
                """,
                (user_id,),
            )
            return [_device_view(row) for row in cur.fetchall()]


def revoke_device(user_id: str, key_id: str) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE api_keys SET revoked_at = NOW()
                WHERE id = %s AND user_id = %s AND revoked_at IS NULL
                RETURNING id
                """,
                (key_id, user_id),
            )
            revoked = cur.fetchone()
        conn.commit()
    if revoked is None:
        raise HTTPException(status_code=404, detail={"code": "device_not_found"})
    logger.info("Device key revoked key_id=%s", key_id)
