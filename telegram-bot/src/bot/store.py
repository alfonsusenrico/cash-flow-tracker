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



    async def add_chat_history(self, telegram_user_id: int, role: str, content: str) -> None:
        async with db_conn() as conn:
            await conn.execute(
                """
                INSERT INTO bot_chat_history (telegram_user_id, role, content)
                VALUES (%s, %s, %s)
                """,
                (telegram_user_id, role, content),
            )

    async def get_chat_history(self, telegram_user_id: int, limit: int = 10) -> list[dict[str, str]]:
        async with db_conn() as conn:
            cur = await conn.execute(
                """
                SELECT role, content
                FROM bot_chat_history
                WHERE telegram_user_id=%s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (telegram_user_id, limit),
            )
            rows = await cur.fetchall()
            return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]

    async def clear_chat_history(self, telegram_user_id: int) -> None:
        async with db_conn() as conn:
            await conn.execute(
                "DELETE FROM bot_chat_history WHERE telegram_user_id=%s",
                (telegram_user_id,),
            )
