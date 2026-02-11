import pickle
import threading
import time
from typing import Any

try:
    from redis import Redis
    from redis.exceptions import RedisError
except Exception:  # pragma: no cover - import fallback for minimal envs
    Redis = None

    class RedisError(Exception):
        pass


class TimedCache:
    def __init__(self, redis_url: str | None = None, key_prefix: str = "cashflow") -> None:
        self._cache: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._key_prefix = key_prefix
        self._redis = None
        if redis_url and Redis is not None:
            try:
                client = Redis.from_url(redis_url, decode_responses=False)
                client.ping()
                self._redis = client
            except Exception:
                self._redis = None

    def _redis_key(self, key: str) -> str:
        return f"{self._key_prefix}:cache:{key}"

    def get(self, key: str) -> Any | None:
        if self._redis is not None:
            try:
                raw = self._redis.get(self._redis_key(key))
                if raw is None:
                    return None
                return pickle.loads(raw)
            except (RedisError, pickle.PickleError, ValueError, EOFError):
                pass

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
        ttl = max(1, ttl)
        if self._redis is not None:
            try:
                self._redis.setex(self._redis_key(key), ttl, pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL))
            except (RedisError, pickle.PickleError, TypeError):
                pass

        with self._lock:
            self._cache[key] = (time.time() + ttl, value)

    def invalidate_prefix(self, prefix: str) -> None:
        if self._redis is not None:
            try:
                pattern = self._redis_key(f"{prefix}*")
                cursor = 0
                while True:
                    cursor, keys = self._redis.scan(cursor=cursor, match=pattern, count=200)
                    if keys:
                        self._redis.delete(*keys)
                    if cursor == 0:
                        break
            except RedisError:
                pass

        with self._lock:
            for key in list(self._cache.keys()):
                if key.startswith(prefix):
                    self._cache.pop(key, None)
