"""Async, pooled Postgres access for the bot's own state.

All DB I/O is async and goes through a single shared AsyncConnectionPool so we
never pay per-request connect latency and bound resource usage under load.
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

_pool: AsyncConnectionPool | None = None


def init_pool(conninfo: str, *, min_size: int = 1, max_size: int = 10) -> AsyncConnectionPool:
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(
            conninfo,
            min_size=min_size,
            max_size=max_size,
            open=False,
            kwargs={"row_factory": dict_row},
        )
    return _pool


async def open_pool() -> None:
    assert _pool is not None, "init_pool must be called first"
    await _pool.open()


async def close_pool() -> None:
    if _pool is not None:
        await _pool.close()


@asynccontextmanager
async def db_conn():
    assert _pool is not None, "pool not initialised"
    async with _pool.connection() as conn:
        yield conn
