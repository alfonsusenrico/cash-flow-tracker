"""Run a synthetic local capture backend from an existing backend checkout. Never production.

The database must be a disposable loopback database whose name ends in `_test`, already migrated with
Flyway. Two fictional owners are created: one with a short history of typed and manual parking
payments, so suggestions and merchant memory exist, and one without history. AI stays disabled, so no
model is ever called.
"""
from pathlib import Path
import argparse
import asyncio
import os
import secrets
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit
from uuid import uuid4

HISTORY_KEY = "fictional-capture-key"
EMPTY_KEY = "fictional-empty-key"
PORT = 18093


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend-dir", type=Path, required=True, help="Backend repository root with backend/")
    parser.add_argument("--port", type=int, default=PORT)
    args = parser.parse_args()
    root = args.backend_dir.resolve()
    if not (root / "backend/app/services/text_capture.py").is_file():
        parser.error("backend-dir must contain the text capture backend")
    database_url = os.environ.get("CAPTURE_TEST_DATABASE_URL", "postgresql://postgres@127.0.0.1:55591/cft_capture_test")
    parts = urlsplit(database_url)
    if parts.scheme != "postgresql" or parts.hostname not in {"127.0.0.1", "localhost"} or not parts.path.endswith("_test"):
        parser.error("Only a loopback database whose name ends in _test is allowed")
    os.environ.update({
        "DATABASE_URL": database_url, "SESSION_SECRET": secrets.token_urlsafe(32),
        "INVITE_CODE": "SYNTHETIC-CAPTURE", "APP_ENV": "development", "COOKIE_SECURE": "false",
        "REDIS_URL": "", "OPENAI_API_KEY": "", "NOTIFICATION_AI_ENABLED": "false",
        "NOTIFICATION_AI_DRY_RUN_ENABLED": "true", "APP_ORIGINS": f"http://127.0.0.1:{args.port}",
        "RECEIPTS_DIR": str(root / "capture-test-receipts"),
    })
    sys.path.insert(0, str(root / "backend"))
    import psycopg
    with psycopg.connect(database_url, autocommit=True) as connection:
        if connection.execute("SELECT to_regclass('public.text_captures')").fetchone()[0] is None:
            parser.error("Migrate the empty synthetic database with Flyway first")
        if connection.execute("SELECT COUNT(*) FROM users").fetchone()[0]:
            parser.error("Refusing to seed a database that already has users")

    import uvicorn
    from app.main import app
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def synthetic_lifespan(application):
        async with original_lifespan(application):
            # A configured provider would reach a real model; this fixture refuses to start with one.
            if getattr(application.state, "notification_provider", None) is not None:
                raise RuntimeError("AI provider configured; refusing to run the synthetic fixture")
            await seed()
            print(f"fixture_ready port={args.port} provider=none production_access=false", flush=True)
            yield

    app.router.lifespan_context = synthetic_lifespan
    uvicorn.run(app, host="127.0.0.1", port=args.port, access_log=False, log_level="warning")


def owner(key: str) -> str:
    from app.db.pool import db_conn
    from app.services.auth import hash_token, register_user

    user = register_user("capture_" + uuid4().hex[:12], "fictional_capture_password", "SYNTHETIC-CAPTURE", "Pemilik Fiktif")
    with db_conn() as conn:
        conn.execute("UPDATE api_keys SET key_hash = %s, key_prefix = 'fictional' WHERE user_id = %s", (hash_token(key), user["id"]))
        conn.execute("DELETE FROM accounts WHERE user_id = %s", (user["id"],))
        for name, kind, balance in (("Tunai", "cash", 500_000), ("Dompet Contoh", "ewallet", 300_000), ("Bank Contoh", "bank", 2_000_000)):
            conn.execute("INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, %s, %s, %s)",
                         (user["id"], name, kind, balance))
        conn.execute("""INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type)
                        VALUES (%s, 'Parkir', 'car', '#888888', 'expense', 'need') ON CONFLICT DO NOTHING""", (user["id"],))
        conn.commit()
    return user["id"]


def ids(user_id: str) -> dict:
    from app.db.pool import db_conn

    with db_conn() as conn:
        accounts = {row["name"]: str(row["id"]) for row in conn.execute("SELECT id, name FROM accounts WHERE user_id = %s", (user_id,))}
        categories = {row["name"]: str(row["id"]) for row in conn.execute(
            "SELECT id, name FROM categories WHERE user_id = %s AND kind = 'expense'", (user_id,))}
    return {"accounts": accounts, "categories": categories}


async def seed() -> None:
    from app.db.pool import db_conn
    from app.services import text_capture

    history = owner(HISTORY_KEY)
    owner(EMPTY_KEY)
    known = ids(history)
    now = datetime.now(timezone.utc)
    # Five weekday-style parking payments around this hour, so the suggestion ranks first and the account
    # is clear from history.
    with db_conn() as conn:
        for day in range(1, 6):
            conn.execute(
                """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, merchant, date, source,
                                             review_status, kakeibo_type)
                   VALUES (%s, %s, %s, 'expense', 10000, 'Parkir Contoh', 'Parkir Contoh', %s, 'manual', 'reviewed', 'need')""",
                (history, known["accounts"]["Tunai"], known["categories"]["Parkir"], now - timedelta(days=day, minutes=10)),
            )
        conn.commit()
    # One typed capture answered by the owner teaches merchant memory, so the same text is then recorded at once.
    capture = await text_capture.submit_capture(
        history, {"client_capture_id": uuid4(), "text": "parkir contoh 10rb", "device_time": now - timedelta(days=6)}, None,
    )
    while capture["status"] == "needs_detail":
        question = capture["question"]
        answer = {"question_id": question["question_id"], "reply_id": str(uuid4())}
        if question["type"] == "category":
            answer["category_id"] = known["categories"]["Parkir"]
        elif question["type"] == "account":
            answer["account_id"] = known["accounts"]["Tunai"]
        else:
            raise RuntimeError(f"Unexpected seed question {question['type']}")
        capture = await asyncio.to_thread(text_capture.answer_capture, history, capture["id"], answer)
    if capture["status"] != "recorded":
        raise RuntimeError("The seed capture was not recorded")


if __name__ == "__main__":
    main()
