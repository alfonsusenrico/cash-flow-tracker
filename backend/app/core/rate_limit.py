import secrets
import threading
import time

try:
    from redis import Redis
    from redis.exceptions import RedisError
except Exception:  # pragma: no cover - import fallback for minimal envs
    Redis = None

    class RedisError(Exception):
        pass


class RateLimiter:
    _REDIS_WINDOW_SCRIPT = """
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]
local ttl = tonumber(ARGV[5])
local cutoff = now_ms - window_ms

redis.call("ZREMRANGEBYSCORE", key, 0, cutoff)
local count = redis.call("ZCARD", key)
if count >= limit then
  redis.call("EXPIRE", key, ttl)
  return 1
end

redis.call("ZADD", key, now_ms, member)
redis.call("EXPIRE", key, ttl)
return 0
"""

    def __init__(self, redis_url: str | None = None, key_prefix: str = "cashflow") -> None:
        self._events: dict[str, list[float]] = {}
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
        return f"{self._key_prefix}:ratelimit:{key}"

    def _exceeded_redis(self, key: str, limit: int, window_seconds: int) -> bool | None:
        if self._redis is None:
            return None
        now_ms = int(time.time() * 1000)
        member = f"{now_ms}-{secrets.token_hex(6)}"
        window_ms = max(1, window_seconds) * 1000
        ttl = max(1, window_seconds + 1)
        try:
            result = self._redis.eval(
                self._REDIS_WINDOW_SCRIPT,
                1,
                self._redis_key(key),
                now_ms,
                window_ms,
                limit,
                member,
                ttl,
            )
            return int(result or 0) == 1
        except RedisError:
            return None

    def exceeded(self, key: str, limit: int, window_seconds: int) -> bool:
        limit = max(1, int(limit))
        window_seconds = max(1, int(window_seconds))

        redis_result = self._exceeded_redis(key, limit, window_seconds)
        if redis_result is not None:
            return redis_result

        now = time.time()
        cutoff = now - window_seconds
        with self._lock:
            events = self._events.get(key, [])
            events = [ts for ts in events if ts >= cutoff]
            if len(events) >= limit:
                self._events[key] = events
                return True
            events.append(now)
            if events:
                self._events[key] = events
            else:
                self._events.pop(key, None)
            return False
