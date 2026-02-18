import os
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    redis_url: str | None
    redis_prefix: str
    session_secret: str
    cookie_secure: bool
    tz: str
    summary_cache_ttl: int
    month_summary_ttl: int
    login_rate_limit: int
    login_rate_window: int
    login_user_rate_limit: int
    register_rate_limit: int
    register_rate_window: int
    password_min_len: int
    username_re: re.Pattern[str]
    db_pool_min: int
    db_pool_max: int
    db_pool_timeout: float
    db_pool_max_waiting: int
    invite_code: str
    public_rate_limit: int
    public_rate_window: int
    receipts_dir: str
    receipt_max_mb: int
    receipt_webp_quality: int


def load_settings() -> Settings:
    database_url = os.getenv("DATABASE_URL", "")
    session_secret = os.getenv("SESSION_SECRET")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    if not session_secret:
        raise RuntimeError("SESSION_SECRET is required")

    username_re = re.compile(r"^[a-zA-Z0-9._-]{3,32}$")

    db_pool_min = max(1, int(os.getenv("DB_POOL_MIN", "1")))
    db_pool_max = max(db_pool_min, int(os.getenv("DB_POOL_MAX", "10")))

    return Settings(
        database_url=database_url,
        redis_url=(os.getenv("REDIS_URL") or "").strip() or None,
        redis_prefix=(os.getenv("REDIS_PREFIX") or "cashflow").strip() or "cashflow",
        session_secret=session_secret,
        cookie_secure=os.getenv("COOKIE_SECURE", "false").lower() == "true",
        tz=os.getenv("TZ", "Asia/Jakarta"),
        summary_cache_ttl=int(os.getenv("SUMMARY_CACHE_TTL", "30")),
        month_summary_ttl=int(os.getenv("MONTH_SUMMARY_TTL", "60")),
        login_rate_limit=int(os.getenv("LOGIN_RATE_LIMIT", "10")),
        login_rate_window=int(os.getenv("LOGIN_RATE_WINDOW", "300")),
        login_user_rate_limit=int(os.getenv("LOGIN_USER_RATE_LIMIT", "5")),
        register_rate_limit=int(os.getenv("REGISTER_RATE_LIMIT", "5")),
        register_rate_window=int(os.getenv("REGISTER_RATE_WINDOW", "900")),
        password_min_len=int(os.getenv("PASSWORD_MIN_LEN", "8")),
        username_re=username_re,
        db_pool_min=db_pool_min,
        db_pool_max=db_pool_max,
        db_pool_timeout=float(os.getenv("DB_POOL_TIMEOUT", "30")),
        db_pool_max_waiting=int(os.getenv("DB_POOL_MAX_WAITING", "100")),
        invite_code=(os.getenv("INVITE_CODE") or "").strip(),
        public_rate_limit=int(os.getenv("PUBLIC_RATE_LIMIT", "120")),
        public_rate_window=int(os.getenv("PUBLIC_RATE_WINDOW", "60")),
        receipts_dir=(os.getenv("RECEIPTS_DIR") or "/app/storage/receipts").strip() or "/app/storage/receipts",
        receipt_max_mb=max(1, int(os.getenv("RECEIPT_MAX_MB", "10"))),
        receipt_webp_quality=max(1, min(100, int(os.getenv("RECEIPT_WEBP_QUALITY", "75")))),
    )


settings = load_settings()
