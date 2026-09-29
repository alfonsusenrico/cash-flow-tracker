import os
import re
import math
from dataclasses import dataclass, field


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
    receipt_max_pixels: int
    app_origins: tuple[str, ...]
    trusted_proxy_cidrs: tuple[str, ...]
    ledger_export_max_rows: int
    notification_ai_enabled: bool = False
    notification_ai_model: str = "gpt-6-luna"
    notification_ai_reasoning_effort: str = "none"
    openai_api_key: str = field(default="", repr=False)
    notification_ai_history_limit: int = 20
    notification_ai_timeout: float = 30
    notification_pairing_window_seconds: int = 900
    notification_ai_configuration_error: str | None = None
    app_env: str = "production"
    notification_ai_dry_run_enabled: bool = False


def _csv_env(name: str) -> tuple[str, ...]:
    raw = os.getenv(name, "")
    return tuple(part.strip().rstrip("/") for part in raw.split(",") if part.strip())


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

    ai_configuration_error = None
    try:
        ai_history_limit = int(os.getenv("NOTIFICATION_AI_HISTORY_LIMIT", "20"))
        ai_timeout = float(os.getenv("NOTIFICATION_AI_TIMEOUT", "30"))
        pairing_window = int(os.getenv("NOTIFICATION_PAIRING_WINDOW_SECONDS", "900"))
        if not 0 <= ai_history_limit <= 50 or not math.isfinite(ai_timeout) or not 1 <= ai_timeout <= 60:
            raise ValueError
        if not 30 <= pairing_window <= 86400:
            raise ValueError
    except ValueError:
        ai_history_limit, ai_timeout, pairing_window = 20, 30, 900
        ai_configuration_error = "processor_configuration_invalid"

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
        receipt_max_pixels=max(1_000_000, int(os.getenv("RECEIPT_MAX_PIXELS", "50000000"))),
        app_origins=_csv_env("APP_ORIGINS") or _csv_env("APP_ORIGIN"),
        trusted_proxy_cidrs=_csv_env("TRUSTED_PROXY_CIDRS"),
        ledger_export_max_rows=max(100, int(os.getenv("LEDGER_EXPORT_MAX_ROWS", "5000"))),
        notification_ai_enabled=os.getenv("NOTIFICATION_AI_ENABLED", "false").lower() == "true",
        notification_ai_model=os.getenv("NOTIFICATION_AI_MODEL", "gpt-6-luna").strip(),
        notification_ai_reasoning_effort=os.getenv("NOTIFICATION_AI_REASONING_EFFORT", "none").strip().lower(),
        openai_api_key=os.getenv("OPENAI_API_KEY", "").strip(),
        notification_ai_history_limit=ai_history_limit,
        notification_ai_timeout=ai_timeout,
        notification_pairing_window_seconds=pairing_window,
        notification_ai_configuration_error=ai_configuration_error,
        app_env=os.getenv("APP_ENV", "production").strip().lower(),
        notification_ai_dry_run_enabled=os.getenv("NOTIFICATION_AI_DRY_RUN_ENABLED", "false").lower() == "true",
    )


settings = load_settings()
