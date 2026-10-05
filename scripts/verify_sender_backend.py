"""Run a loopback-only fictional backend for companion sender integration tests.

Uses the real auth, ingestion, question and ledger paths with a deterministic fake
provider. Never connects to OpenAI or production. Stop with Ctrl-C to delete only
this run's fictional owner. Requires a disposable, already migrated ledger_test DB.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit
from uuid import UUID, uuid4

url = urlsplit(os.environ.get("DATABASE_URL", ""))
if url.hostname not in {"127.0.0.1", "localhost"} or url.path != "/ledger_test":
    raise SystemExit("Use only a disposable loopback ledger_test database.")
os.environ.update(
    SESSION_SECRET="fictional-verification-session",
    APP_ENV="development",
    REDIS_URL="",
    NOTIFICATION_AI_ENABLED="false",
    COOKIE_SECURE="false",
)
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from fastapi import Depends, HTTPException
from app.main import app
from app.db.pool import close_db_pool, db_conn, open_db_pool
from app.db.init_db import init_db_schema
from app.services.auth import get_current_user, hash_token
from app.services import notification_processing as processing
from app.services.notification_application import deterministic_interpretation
from app.services.notification_evidence import collect_facts
from app.services.notification_interpretation import Interpretation, MappingProposal

owner_id = str(uuid4())
bca_id = str(uuid4())
income_id = str(uuid4())
extra_id = str(uuid4())
processing.settings = replace(
    processing.settings,
    notification_ai_enabled=True,
    notification_ai_model="gpt-4o-mini",
    openai_api_key="fictional-provider-never-used",
)


class FictionalProvider:
    async def interpret(self, context):
        facts = collect_facts({
            "package_name": "com.bca.mybca.omni.android",
            "body_text": context["notification"],
            "post_time": datetime.now(timezone.utc),
        })
        bca_accounts = [row for row in context["accounts"] if row["name"].startswith("BCA")]
        if len(bca_accounts) > 1:
            return Interpretation(
                outcome="record",
                direction="income",
                description="Transfer masuk",
                account_id=UUID(bca_id),
                category_id=UUID(income_id),
                source_account_id=None,
                target_account_id=None,
                kakeibo=None,
                amount_evidence=facts.amount_quotes[0],
                direction_evidence=context["notification"],
                source_evidence="",
                target_evidence="",
                candidate_transaction_id=None,
                confidence=0.99,
                review_reason="none",
                mappings=[MappingProposal(
                    role="observed", account_id=UUID(bca_id), confidence=0.5, alternatives=[],
                )],
            )
        return deterministic_interpretation(facts, context)


@asynccontextmanager
async def lifespan(_app):
    open_db_pool()
    init_db_schema()
    with db_conn() as conn:
        # Recover only this fixture's owner after an interrupted verification process.
        conn.execute(
            """DELETE FROM users WHERE username LIKE 'sender_device_%%' AND id IN
               (SELECT user_id FROM api_keys WHERE key_hash = %s)""",
            (hash_token("fictional-phone-key"),),
        )
        conn.execute(
            """INSERT INTO users (id, username, name, password_hash, invite_code)
               VALUES (%s, %s, 'Raka Purnama', 'fictional-hash', 'fictional')""",
            (owner_id, "sender_device_" + uuid4().hex),
        )
        conn.execute(
            """INSERT INTO accounts (id, user_id, name, type, initial_balance)
               VALUES (%s, %s, 'BCA Harian', 'bank', 1000000)""",
            (bca_id, owner_id),
        )
        conn.execute(
            """INSERT INTO categories (id, user_id, name, kind)
               VALUES (%s, %s, 'Transfer Masuk', 'income')""",
            (income_id, owner_id),
        )
        conn.execute(
            "INSERT INTO api_keys (user_id, key_hash, key_prefix) VALUES (%s, %s, 'fictional')",
            (owner_id, hash_token("fictional-phone-key")),
        )
    worker = asyncio.create_task(processing.notification_worker(FictionalProvider()))
    print("Fictional local sender backend ready; no external provider calls.", flush=True)
    try:
        yield
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (owner_id,))
        close_db_pool()


app.router.lifespan_context = lifespan


def verification_owner(user=Depends(get_current_user)):
    if str(user["id"]) != owner_id:
        raise HTTPException(404)
    return user


@app.get("/verification/sender-state")
def state(_user=Depends(verification_owner)):
    with db_conn() as conn:
        records = conn.execute(
            "SELECT id, amount, notes FROM transactions WHERE user_id = %s", (owner_id,),
        ).fetchall()
        answers = conn.execute(
            "SELECT event_id, question_id, action FROM notification_sender_answers WHERE user_id = %s",
            (owner_id,),
        ).fetchall()
    return {"ok": True, "records": records, "answers": answers}


@app.post("/verification/sender-account/{enabled}")
def ambiguous_account(enabled: bool, _user=Depends(verification_owner)):
    with db_conn() as conn:
        if enabled:
            conn.execute(
                """INSERT INTO accounts (id, user_id, name, type)
                   VALUES (%s, %s, 'BCA Cadangan', 'bank') ON CONFLICT DO NOTHING""",
                (extra_id, owner_id),
            )
        else:
            conn.execute("DELETE FROM accounts WHERE user_id = %s AND id = %s", (owner_id, extra_id))
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=18000, access_log=False)
