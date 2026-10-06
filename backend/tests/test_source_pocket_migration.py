from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import sql


def test_populated_v25_upgrade_and_startup_mirror_preserve_existing_data(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.pool import db_conn

    root = Path(__file__).resolve().parents[2]
    paths = sorted((root / "db/migrations").glob("V*.sql"), key=lambda p: int(p.name.split("__")[0][1:]))
    schema = "fictional_source_upgrade_" + uuid4().hex
    with db_conn() as conn:
        # Everything, including the schema, is rolled back after verification.
        with conn.transaction(force_rollback=True):
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            conn.execute(sql.SQL("SET LOCAL search_path TO {}, public").format(sql.Identifier(schema)))
            for path in paths:
                if path.name.startswith("V26__"):
                    break
                conn.execute(path.read_text())
            user = conn.execute("INSERT INTO users (username,password_hash,invite_code) VALUES ('fictional-upgrade','fictional','fictional') RETURNING id").fetchone()["id"]
            event = conn.execute("""INSERT INTO notification_events
                (user_id,device_id,package_name,post_time,payload_hash,processing_state)
                VALUES (%s,'fictional','com.bca',NOW(),'fictional-upgrade','needs_confirmation') RETURNING id""", (user,)).fetchone()["id"]
            question, reply = uuid4(), uuid4()
            conn.execute("INSERT INTO notification_sender_answers (user_id,event_id,question_id,reply_id,action) VALUES (%s,%s,%s,%s,'unknown')", (user,event,question,reply))
            account = conn.execute("INSERT INTO accounts (user_id,name,type) VALUES (%s,'Fictional Bank','bank') RETURNING id", (user,)).fetchone()["id"]
            category = conn.execute("INSERT INTO categories (user_id,name,kind) VALUES (%s,'Fictional Income','income') RETURNING id", (user,)).fetchone()["id"]
            transaction = conn.execute("INSERT INTO transactions (user_id,type,account_id,category_id,amount) VALUES (%s,'income',%s,%s,125000) RETURNING id", (user,account,category)).fetchone()["id"]
            ledger_before = conn.execute("SELECT row_to_json(t) AS row FROM transactions t WHERE id=%s", (transaction,)).fetchone()["row"]
            before = conn.execute("SELECT row_to_json(n) AS row FROM notification_events n WHERE id=%s", (event,)).fetchone()["row"]
            versioned = root / "db/migrations/V26__notification_source_pocket_confirmation.sql"
            mirror = root / "backend/app/db/notification_source_pocket.sql"
            assert versioned.read_text() == mirror.read_text()
            conn.execute(versioned.read_text())
            conn.execute(mirror.read_text())
            after = conn.execute("SELECT row_to_json(n) AS row FROM notification_events n WHERE id=%s", (event,)).fetchone()["row"]
            assert after.pop("source_pocket_resolution") is None
            assert after == before
            assert conn.execute("SELECT row_to_json(t) AS row FROM transactions t WHERE id=%s", (transaction,)).fetchone()["row"] == ledger_before
            assert conn.execute("SELECT COUNT(*) AS count FROM notification_sender_answers WHERE user_id=%s", (user,)).fetchone()["count"] == 1
            assert conn.execute("SELECT COUNT(*) AS count FROM notification_source_pocket_answers").fetchone()["count"] == 0
