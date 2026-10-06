"""Loopback-only fictional ledger for USB companion source-pocket verification."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit
from uuid import UUID, uuid4

url = urlsplit(os.environ.get("DATABASE_URL", ""))
if url.hostname != "127.0.0.1" or url.port != 5547 or url.path != "/ledger_test":
    raise SystemExit("Use only the disposable loopback Jago ledger_test database on port 5547.")
os.environ.update(SESSION_SECRET="fictional-jago-session", APP_ENV="development", REDIS_URL="",
                  NOTIFICATION_AI_ENABLED="false", COOKIE_SECURE="false")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))

from fastapi import Depends, HTTPException
from app.main import app
from app.db.pool import close_db_pool, db_conn, open_db_pool
from app.db.init_db import init_db_schema
from app.services.auth import get_current_user, hash_token
from app.services import notification_processing as processing

owner_id = str(uuid4())
parent_id = str(uuid4())
main_id = str(uuid4())
emergency_id = str(uuid4())
extra_id = str(uuid4())


@asynccontextmanager
async def lifespan(_app):
    open_db_pool()
    init_db_schema()
    with db_conn() as conn:
        conn.execute("DELETE FROM users WHERE username LIKE 'jago_device_%%' AND id IN (SELECT user_id FROM api_keys WHERE key_hash=%s)",
                     (hash_token("fictional-jago-phone-key"),))
        conn.execute("INSERT INTO users (id,username,name,password_hash,invite_code) VALUES (%s,%s,'Raka Purnama','fictional','fictional')",
                     (owner_id, "jago_device_" + uuid4().hex))
        for account_id, label, parent in [(parent_id, "Bank Jago", None), (main_id, "Kantong Utama", parent_id),
                                          (emergency_id, "Dana Darurat", parent_id), (extra_id, "Belanja", parent_id)]:
            conn.execute("INSERT INTO accounts (id,user_id,name,type,parent_id,initial_balance) VALUES (%s,%s,%s,'bank',%s,1000000)",
                         (account_id, owner_id, label, parent))
        conn.execute("UPDATE accounts SET default_pocket_id=%s WHERE id=%s", (main_id, parent_id))
        conn.execute("INSERT INTO categories (user_id,name,kind) VALUES (%s,'Transfer Keluar','expense')", (owner_id,))
        conn.execute("INSERT INTO api_keys (user_id,key_hash,key_prefix) VALUES (%s,%s,'fictional')",
                     (owner_id, hash_token("fictional-jago-phone-key")))
    worker = asyncio.create_task(processing.notification_worker(None))
    print("Fictional local Jago backend ready; no external provider calls.", flush=True)
    try:
        yield
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id=%s", (owner_id,))
        close_db_pool()


app.router.lifespan_context = lifespan


def verification_owner(user=Depends(get_current_user)):
    if str(user["id"]) != owner_id:
        raise HTTPException(404)
    return user


@app.get("/verification/jago-state")
def state(_user=Depends(verification_owner)):
    with db_conn() as conn:
        records = conn.execute("SELECT id,account_id,amount FROM transactions WHERE user_id=%s", (owner_id,)).fetchall()
        answers = conn.execute("SELECT event_id,question_id,reply_id,account_id FROM notification_source_pocket_answers WHERE user_id=%s", (owner_id,)).fetchall()
    return {"ok": True, "records": records, "answers": answers, "main_id": main_id, "emergency_id": emergency_id, "extra_id": extra_id}


@app.post("/verification/jago-pocket/{account_id}/archive")
def archive(account_id: UUID, _user=Depends(verification_owner)):
    if str(account_id) not in {main_id, emergency_id, extra_id}:
        raise HTTPException(404)
    with db_conn() as conn:
        conn.execute("UPDATE accounts SET is_archived=TRUE WHERE user_id=%s AND id=%s", (owner_id, account_id))
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=18000, access_log=False)
