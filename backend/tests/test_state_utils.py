import pathlib
import sys
import time
import unittest

BACKEND_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.core.cache import TimedCache
from app.core.rate_limit import RateLimiter


class StateUtilityTests(unittest.TestCase):
    def test_timed_cache_local_set_get_and_invalidate(self):
        cache = TimedCache(redis_url=None, key_prefix="test")
        cache.set("alice:summary:2026-02", {"ok": True}, ttl=30)

        self.assertEqual(cache.get("alice:summary:2026-02"), {"ok": True})
        cache.invalidate_prefix("alice:")
        self.assertIsNone(cache.get("alice:summary:2026-02"))

    def test_timed_cache_local_expiry(self):
        cache = TimedCache(redis_url=None, key_prefix="test")
        cache.set("k", 123, ttl=1)
        self.assertEqual(cache.get("k"), 123)
        time.sleep(1.05)
        self.assertIsNone(cache.get("k"))

    def test_rate_limiter_local_window_behavior(self):
        limiter = RateLimiter(redis_url=None, key_prefix="test")
        key = "login:ip:127.0.0.1"

        self.assertFalse(limiter.exceeded(key, limit=2, window_seconds=60))
        self.assertFalse(limiter.exceeded(key, limit=2, window_seconds=60))
        self.assertTrue(limiter.exceeded(key, limit=2, window_seconds=60))


if __name__ == "__main__":
    unittest.main()
