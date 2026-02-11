from app.core.cache import TimedCache
from app.core.config import settings
from app.core.rate_limit import RateLimiter

cache = TimedCache(redis_url=settings.redis_url, key_prefix=settings.redis_prefix)
rate_limiter = RateLimiter(redis_url=settings.redis_url, key_prefix=settings.redis_prefix)
