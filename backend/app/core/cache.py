import threading
import time
from typing import Any


class TimedCache:
    def __init__(self) -> None:
        self._cache: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            payload = self._cache.get(key)
            if not payload:
                return None
            expires_at, value = payload
            if time.time() > expires_at:
                self._cache.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any, ttl: int) -> None:
        with self._lock:
            self._cache[key] = (time.time() + max(1, ttl), value)

    def invalidate_prefix(self, prefix: str) -> None:
        with self._lock:
            for key in list(self._cache.keys()):
                if key.startswith(prefix):
                    self._cache.pop(key, None)
