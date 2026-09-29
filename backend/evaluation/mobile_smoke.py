"""Fictional-only local Android integration fixture; never a production entry point."""
import asyncio
import json
import os
from urllib.parse import urlsplit
from contextlib import asynccontextmanager
from dataclasses import replace
from uuid import uuid4
import uvicorn
from app.main import app
from app.db.pool import db_conn, open_db_pool, close_db_pool
from app.db.init_db import init_db_schema
from app.services.auth import register_user, hash_token
from app.services import notification_processing as processing
from app.services.notification_interpretation import Interpretation

ids = {key: str(uuid4()) for key in ('jago', 'main', 'emergency')}

class FictionalProvider:
    async def interpret(self, context):
        return Interpretation.model_validate_json(json.dumps({
            'outcome': 'record', 'direction': 'expense', 'description': 'Kedai Awan',
            'account_id': ids['main'], 'category_id': ids['expense'], 'kakeibo': 'want',
            'source_account_id': None, 'target_account_id': None, 'amount_evidence': 'Rp125.000',
            'direction_evidence': "You've paid", 'source_evidence': '', 'target_evidence': '',
            'candidate_transaction_id': None, 'confidence': .9, 'review_reason': 'none',
        }))

async def complete():
    while True:
        claim = await asyncio.to_thread(processing.claim_event)
        if claim:
            await asyncio.sleep(1)
            try:
                await processing.process_claim(claim, FictionalProvider())
            except Exception as exc:
                print(json.dumps({'fixture_worker_error': type(exc).__name__}), flush=True)
        else:
            await asyncio.sleep(.1)

@asynccontextmanager
async def synthetic_lifespan(application):
    database_name = urlsplit(os.environ.get("DATABASE_URL", "")).path.rstrip("/").rsplit("/", 1)[-1]
    if os.environ.get("ALLOW_FICTIONAL_MOBILE_SMOKE") != "true" or not database_name.endswith("_test"):
        raise RuntimeError("Use an explicitly authorized disposable test database")
    open_db_pool()
    init_db_schema()
    user = register_user('phone_' + uuid4().hex, 'fictional_phone_password', 'TESTCODE', 'Fictional Phone User')
    with db_conn() as conn:
        conn.execute('DELETE FROM api_keys WHERE key_hash=%s', (hash_token('fictional-phone-key'),))
        conn.execute('UPDATE api_keys SET key_hash=%s, key_prefix=%s WHERE user_id=%s',
                     (hash_token('fictional-phone-key'), 'fictional', user['id']))
        for key, name, parent, balance in (
            ('jago', 'Bank Jago', None, 0), ('main', 'Kantong Utama', ids['jago'], 1000000),
            ('emergency', 'Dana Darurat', ids['jago'], 0),
        ):
            conn.execute("INSERT INTO accounts (id,user_id,name,type,parent_id,initial_balance) VALUES (%s,%s,%s,'bank',%s,%s)",
                         (ids[key],user['id'],name,parent,balance))
        conn.execute('UPDATE accounts SET default_pocket_id=%s WHERE id=%s', (ids['main'],ids['jago']))
        ids['expense'] = str(conn.execute("SELECT id FROM categories WHERE user_id=%s AND name='Tagihan & Utilitas' AND kind='expense'", (user['id'],)).fetchone()['id'])
        conn.commit()
    processing.settings = replace(processing.settings, notification_ai_enabled=False, openai_api_key='')
    task = asyncio.create_task(complete())
    print(json.dumps({'fixture_ready': True, 'provider': 'mocked', 'production_access': False}), flush=True)
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        close_db_pool()

app.router.lifespan_context = synthetic_lifespan
if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=18000, access_log=False, log_level='warning')
