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
    notification_mapping_auto_threshold: float = 0.85
    notification_review_confidence_threshold: float = 0.8
    notification_ai_configuration_error: str | None = None
    app_env: str = "production"
    notification_ai_dry_run_enabled: bool = False
    movement_transfer_fees: tuple[int, ...] = (2500, 6500, 1000, 2900)
    movement_fee_max_difference: int = 10000
    movement_repair_interval_seconds: int = 600
    movement_repair_lookback_hours: int = 48
    movement_repair_max_gap_seconds: int = 21600
    movement_pairing_configuration_error: str | None = None
    history_suggestion_max_rows: int = 300
    history_suggestion_batch_size: int = 20
    history_suggestion_configuration_error: str | None = None


def _csv_env(name: str) -> tuple[str, ...]:
    raw = os.getenv(name, "")
    return tuple(part.strip().rstrip("/") for part in raw.split(",") if part.strip())


def _movement_pairing_settings() -> dict:
    """Fee table and re-pairing schedule; an invalid value disables fee tolerance and re-pairing."""
    try:
        fees = tuple(sorted({int(part) for part in _csv_env("MOVEMENT_TRANSFER_FEES") or ("2500", "6500", "1000", "2900")}))
        max_difference = int(os.getenv("MOVEMENT_FEE_MAX_DIFFERENCE", "10000"))
        interval = int(os.getenv("MOVEMENT_REPAIR_INTERVAL_SECONDS", "600"))
        lookback = int(os.getenv("MOVEMENT_REPAIR_LOOKBACK_HOURS", "48"))
        max_gap = int(os.getenv("MOVEMENT_REPAIR_MAX_GAP_SECONDS", "21600"))
        if not 0 <= max_difference <= 100000 or any(not 0 < fee <= max_difference for fee in fees):
            raise ValueError
        if not 60 <= interval <= 86400 or not 1 <= lookback <= 168 or not 900 <= max_gap <= 172800:
            raise ValueError
    except ValueError:
        return {"movement_transfer_fees": (), "movement_pairing_configuration_error": "movement_pairing_configuration_invalid"}
    return {
        "movement_transfer_fees": fees, "movement_fee_max_difference": max_difference,
        "movement_repair_interval_seconds": interval, "movement_repair_lookback_hours": lookback,
        "movement_repair_max_gap_seconds": max_gap,
    }


def _history_suggestion_settings() -> dict:
    """Cost bounds of a history suggestion run; an invalid value disables runs."""
    try:
        max_rows = int(os.getenv("HISTORY_SUGGESTION_MAX_ROWS", "300"))
        batch_size = int(os.getenv("HISTORY_SUGGESTION_BATCH_SIZE", "20"))
        if not 1 <= max_rows <= 2000 or not 1 <= batch_size <= 50:
            raise ValueError
    except ValueError:
        return {"history_suggestion_configuration_error": "suggestion_configuration_invalid"}
    return {"history_suggestion_max_rows": max_rows, "history_suggestion_batch_size": batch_size}


def load_settings() -> Settings:
    database_url = os.getenv("DATABASE_URL", "")
    session_secret = os.getenv("SESSION_SECRET")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    if not session_secret:
        raise RuntimeError("SESSION_SECRET is required")

    app_env = os.getenv("APP_ENV", "production").strip().lower()
    cookie_secure = os.getenv("COOKIE_SECURE", "true").strip().lower() != "false"
    if app_env in {"production", "prod"} and not cookie_secure:
        raise RuntimeError("COOKIE_SECURE must be true in production")

    username_re = re.compile(r"^[a-zA-Z0-9._-]{3,32}$")

    db_pool_min = max(1, int(os.getenv("DB_POOL_MIN", "1")))
    db_pool_max = max(db_pool_min, int(os.getenv("DB_POOL_MAX", "10")))

    ai_configuration_error = None
    try:
        ai_history_limit = int(os.getenv("NOTIFICATION_AI_HISTORY_LIMIT", "20"))
        ai_timeout = float(os.getenv("NOTIFICATION_AI_TIMEOUT", "30"))
        pairing_window = int(os.getenv("NOTIFICATION_PAIRING_WINDOW_SECONDS", "900"))
        mapping_threshold = float(os.getenv("NOTIFICATION_MAPPING_AUTO_THRESHOLD", "0.85"))
        review_threshold = float(os.getenv("NOTIFICATION_REVIEW_CONFIDENCE_THRESHOLD", "0.8"))
        if not 0 <= ai_history_limit <= 50 or not math.isfinite(ai_timeout) or not 1 <= ai_timeout <= 60:
            raise ValueError
        if not 30 <= pairing_window <= 86400:
            raise ValueError
        if not math.isfinite(mapping_threshold) or not 0.5 <= mapping_threshold <= 1.0:
            raise ValueError
        if not math.isfinite(review_threshold) or not 0.5 <= review_threshold <= 1.0:
            raise ValueError
    except ValueError:
        ai_history_limit, ai_timeout, pairing_window, mapping_threshold = 20, 30, 900, 0.85
        review_threshold = 0.8
        ai_configuration_error = "processor_configuration_invalid"

    return Settings(
        database_url=database_url,
        redis_url=(os.getenv("REDIS_URL") or "").strip() or None,
        redis_prefix=(os.getenv("REDIS_PREFIX") or "cashflow").strip() or "cashflow",
        session_secret=session_secret,
        cookie_secure=cookie_secure,
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
        notification_mapping_auto_threshold=mapping_threshold,
        notification_review_confidence_threshold=review_threshold,
        notification_ai_configuration_error=ai_configuration_error,
        app_env=app_env,
        notification_ai_dry_run_enabled=os.getenv("NOTIFICATION_AI_DRY_RUN_ENABLED", "false").lower() == "true",
        **_movement_pairing_settings(),
        **_history_suggestion_settings(),
    )


settings = load_settings()
