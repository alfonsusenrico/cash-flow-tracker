#!/usr/bin/env python3
"""Materialize production runtime.env from environment variables or GitHub Secrets."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ENV_DEFAULTS = {
    "COOKIE_SECURE": "true",
    "APP_ORIGINS": "https://finance.alfonsusenrico.com,http://localhost:8090,http://127.0.0.1:8090",
    "TRUSTED_PROXY_CIDRS": "172.16.0.0/12,127.0.0.1/32,::1/128,100.64.0.0/10",
    "POSTGRES_DB": "ledger",
    "POSTGRES_USER": "ledger",
    "TZ": "Asia/Jakarta",
    "SUMMARY_CACHE_TTL": "30",
    "MONTH_SUMMARY_TTL": "60",
    "LOGIN_RATE_LIMIT": "10",
    "LOGIN_RATE_WINDOW": "300",
    "LOGIN_USER_RATE_LIMIT": "5",
    "REGISTER_RATE_LIMIT": "5",
    "REGISTER_RATE_WINDOW": "900",
    "PASSWORD_MIN_LEN": "8",
    "DB_POOL_MIN": "1",
    "DB_POOL_MAX": "10",
    "DB_POOL_TIMEOUT": "30",
    "DB_POOL_MAX_WAITING": "100",
    "PUBLIC_RATE_LIMIT": "120",
    "PUBLIC_RATE_WINDOW": "60",
    "REDIS_URL": "redis://redis:6379/0",
    "REDIS_PREFIX": "cashflow",
    "RECEIPTS_DIR": "/app/storage/receipts",
    "RECEIPT_MAX_MB": "10",
    "RECEIPT_WEBP_QUALITY": "75",
    "RECEIPT_MAX_PIXELS": "50000000",
    "LEDGER_EXPORT_MAX_ROWS": "5000",
    "WEB_PORT": "8090",
    "WEBHOOK_LISTEN": "0.0.0.0",
    "WEBHOOK_PORT": "8081",
    "FINANCE_API_BASE_URL": "http://api:8000",
    "DEEPSEEK_BASE_URL": "https://openrouter.ai/api/v1",
    "DEEPSEEK_MODEL": "deepseek/deepseek-chat",
    "USE_TWO_STEP_VISION": "true",
    "LLM_TIMEOUT": "300",
    "VISION_BASE_URL": "https://openrouter.ai/api/v1",
    "VISION_MODEL": "nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free",
    "CONFIDENCE_THRESHOLD": "0.75",
}

CRITICAL_VARIABLES = (
    "SESSION_SECRET",
    "POSTGRES_PASSWORD",
    "INVITE_CODE",
)

OPTIONAL_VARIABLES = (
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_WEBHOOK_URL",
    "TELEGRAM_WEBHOOK_SECRET",
    "DEEPSEEK_API_KEY",
    "BOT_SECRET",
)


def materialize(output_path: Path, allow_missing_critical: bool = False) -> None:
    lines: list[str] = ["# Generated Production Runtime Environment\n"]

    # 1. Critical Secrets
    for var_name in CRITICAL_VARIABLES:
        val = os.environ.get(var_name, "").strip()
        if not val and not allow_missing_critical:
            raise ValueError(f"Missing required environment variable or secret: {var_name}")
        lines.append(f"{var_name}={val}\n")

    # 2. Configurable Defaults
    for var_name, default_val in ENV_DEFAULTS.items():
        val = os.environ.get(var_name, default_val).strip()
        lines.append(f"{var_name}={val}\n")

    # 3. Optional Integrations
    for var_name in OPTIONAL_VARIABLES:
        if var_name in os.environ:
            val = os.environ[var_name].strip()
            lines.append(f"{var_name}={val}\n")

    output_path.write_text("".join(lines), encoding="utf-8")
    output_path.chmod(0o600)
    print(f"✔ Materialized runtime environment to {output_path} (mode 0600)")


def main():
    parser = argparse.ArgumentParser(description="Materialize production runtime.env")
    parser.add_argument("--output", "-o", default="runtime.env", type=Path, help="Target env file path")
    parser.add_argument("--env-file", "-e", type=Path, default=None, help="Optional existing env file to seed from")
    parser.add_argument("--allow-missing", action="store_true", help="Allow missing critical secrets (for tests)")
    args = parser.parse_args()

    if args.env_file and args.env_file.exists():
        for line in args.env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                if k.strip() not in os.environ:
                    os.environ[k.strip()] = v.strip()

    try:
        materialize(args.output, allow_missing_critical=args.allow_missing)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
