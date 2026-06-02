"""Data-access for bot users and pending confirmations (async, pooled)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from .crypto import Crypto
from .db import db_conn


class Store:
    def __init__(self, crypto: Crypto) -> None:
        self._crypto = crypto

    async def link_user(self, telegram_user_id: int, api_key: str, username_hint: str | None) -> None:
        enc = self._crypto.encrypt(api_key)
        async with db_conn() as conn:
            await conn.execute(
                """
                INSERT INTO bot_users (telegram_user_id, api_key_encrypted, username_hint, last_used_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (telegram_user_id)
                DO UPDATE SET api_key_encrypted=EXCLUDED.api_key_encrypted,
                              username_hint=EXCLUDED.username_hint,
                              last_used_at=now()
                """,
                (telegram_user_id, enc, username_hint),
            )

    async def unlink_user(self, telegram_user_id: int) -> bool:
        async with db_conn() as conn:
            cur = await conn.execute(
                "DELETE FROM bot_users WHERE telegram_user_id=%s", (telegram_user_id,)
            )
            return cur.rowcount > 0

    async def get_api_key(self, telegram_user_id: int) -> str | None:
        async with db_conn() as conn:
            cur = await conn.execute(
                "SELECT api_key_encrypted FROM bot_users WHERE telegram_user_id=%s",
                (telegram_user_id,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            await conn.execute(
                "UPDATE bot_users SET last_used_at=now() WHERE telegram_user_id=%s",
                (telegram_user_id,),
            )
            return self._crypto.decrypt(row["api_key_encrypted"])

    async def get_username_hint(self, telegram_user_id: int) -> str | None:
        async with db_conn() as conn:
            cur = await conn.execute(
                "SELECT username_hint FROM bot_users WHERE telegram_user_id=%s",
                (telegram_user_id,),
            )
            row = await cur.fetchone()
            return row["username_hint"] if row else None

    async def create_pending(
        self, telegram_user_id: int, chat_id: int, action: dict[str, Any], ttl_seconds: int = 900
    ) -> str:
        expires = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        async with db_conn() as conn:
            cur = await conn.execute(
                """
                INSERT INTO bot_pending_actions (telegram_user_id, chat_id, action_json, expires_at)
                VALUES (%s, %s, %s::jsonb, %s)
                RETURNING pending_id::text AS pending_id
                """,
                (telegram_user_id, chat_id, json.dumps(action), expires),
            )
            row = await cur.fetchone()
            return row["pending_id"]

    async def take_pending(self, pending_id: str, telegram_user_id: int) -> dict[str, Any] | None:
        """Fetch and delete a non-expired pending action (single-use)."""
        async with db_conn() as conn:
            cur = await conn.execute(
                """
                DELETE FROM bot_pending_actions
                WHERE pending_id=%s::uuid AND telegram_user_id=%s AND expires_at > now()
                RETURNING action_json
                """,
                (pending_id, telegram_user_id),
            )
            row = await cur.fetchone()
            return row["action_json"] if row else None
