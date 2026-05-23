"""Cache helpers — thin wrappers around the shared TimedCache singleton."""
from typing import Any

from app.services.state import cache


def cache_get(key: str) -> Any | None:
    return cache.get(key)


def cache_set(key: str, value: Any, ttl: int) -> None:
    cache.set(key, value, ttl)


def invalidate_user_cache(username: str) -> None:
    cache.invalidate_prefix(f"{username}:")
