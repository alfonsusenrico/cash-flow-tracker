"""Run a synthetic local pairing backend from an existing backend checkout. Never production."""
from pathlib import Path
import argparse
import os
import secrets
import sys
from urllib.parse import urlsplit


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend-dir", type=Path, required=True, help="Backend repository root with backend/ and db/migrations/")
    parser.add_argument("--migrate-empty-database", action="store_true")
    args = parser.parse_args()
    root = args.backend_dir.resolve()
    if not (root / "backend/app/services/device_pairing.py").is_file():
        parser.error("backend-dir must contain the QR pairing backend")
    database_url = os.environ.get("PAIRING_TEST_DATABASE_URL", "postgresql://postgres@127.0.0.1:5548/cft_pairing_test")
    parts = urlsplit(database_url)
    if parts.scheme != "postgresql" or parts.hostname not in {"127.0.0.1", "localhost"} or parts.path != "/cft_pairing_test":
        parser.error("Only a loopback database named cft_pairing_test is allowed")
    os.environ.update({
        "DATABASE_URL": database_url, "SESSION_SECRET": secrets.token_urlsafe(32),
        "INVITE_CODE": "SYNTHETIC-PAIRING", "APP_ENV": "development", "COOKIE_SECURE": "false",
        "REDIS_URL": "", "OPENAI_API_KEY": "", "NOTIFICATION_AI_ENABLED": "false",
        "NOTIFICATION_AI_DRY_RUN_ENABLED": "true", "APP_ORIGINS": "http://127.0.0.1:18000",
        "RECEIPTS_DIR": str(root / "pairing-test-receipts"),
    })
    sys.path.insert(0, str(root / "backend"))
    import psycopg
    with psycopg.connect(database_url, autocommit=True) as connection:
        if args.migrate_empty_database:
            count = connection.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'").fetchone()[0]
            if count:
                parser.error("Refusing to migrate a nonempty database")
            migrations = sorted((root / "db/migrations").glob("V*.sql"), key=lambda p: int(p.name.split("__")[0][1:]))
            for path in migrations:
                connection.execute(path.read_text())
            print(f"Applied {len(migrations)} migrations to the empty synthetic database", flush=True)
        elif connection.execute("SELECT to_regclass('public.pairing_codes')").fetchone()[0] is None:
            parser.error("Initialize the empty synthetic database with --migrate-empty-database")
    from dataclasses import replace
    from app.core import config
    # Production requires an HTTPS QR origin. Only this test process injects loopback HTTP.
    config.settings = replace(config.settings, pairing_server_url="http://127.0.0.1:18000")
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=18000, access_log=False, log_level="warning")


if __name__ == "__main__":
    main()
