"""Environment-driven configuration for the Telegram bot."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _req(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    telegram_webhook_url: str
    telegram_webhook_secret: str
    webhook_listen: str
    webhook_port: int
    deepseek_api_key: str
    deepseek_base_url: str
    deepseek_model: str
    finance_api_base_url: str
    bot_database_url: str
    bot_secret: str
    confidence_threshold: float
    llm_timeout: int


def load_settings() -> Settings:
    return Settings(
        telegram_bot_token=_req("TELEGRAM_BOT_TOKEN"),
        telegram_webhook_url=_req("TELEGRAM_WEBHOOK_URL"),
        telegram_webhook_secret=_req("TELEGRAM_WEBHOOK_SECRET"),
        webhook_listen=os.environ.get("WEBHOOK_LISTEN", "0.0.0.0"),
        webhook_port=int(os.environ.get("WEBHOOK_PORT", "8081")),
        deepseek_api_key=_req("DEEPSEEK_API_KEY"),
        deepseek_base_url=os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/"),
        deepseek_model=os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash"),
        finance_api_base_url=_req("FINANCE_API_BASE_URL").rstrip("/"),
        bot_database_url=_req("BOT_DATABASE_URL"),
        bot_secret=_req("BOT_SECRET"),
        confidence_threshold=float(os.environ.get("CONFIDENCE_THRESHOLD", "0.75")),
        llm_timeout=int(os.environ.get("LLM_TIMEOUT", "120")),
    )
