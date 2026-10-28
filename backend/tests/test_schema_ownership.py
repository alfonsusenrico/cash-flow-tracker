from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = ROOT / "db/migrations"
V32 = MIGRATIONS / "V32__adopt_startup_schema_and_retire_merchant_rules.sql"


def migrate_to_v31(conn):
    paths = sorted(MIGRATIONS.glob("V*.sql"), key=lambda p: int(p.name.split("__")[0][1:]))
    for path in paths:
        if path.name.startswith("V32__"):
            break
        conn.execute(path.read_text())


def scratch_schema(conn, prefix):
    schema = f"{prefix}_{uuid4().hex}"
    conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    conn.execute(sql.SQL("SET LOCAL search_path TO {}, public").format(sql.Identifier(schema)))
    return schema


def relation_exists(conn, schema, name):
    return conn.execute("SELECT to_regclass(%s) AS rel", (f"{schema}.{name}",)).fetchone()["rel"] is not None


def test_application_runs_no_startup_schema():
    assert not list((ROOT / "backend/app/db").glob("*.sql"))
    main = (ROOT / "backend/app/main.py").read_text()
    assert "init_db_schema" not in main
    assert "def init_db_schema" not in (ROOT / "backend/app/db/init_db.py").read_text()
    for path in (ROOT / "backend/app").rglob("*.py"):
        assert "merchant_category_rules" not in path.read_text(), path


def test_v32_adopts_startup_schema_archives_paid_debts_and_drops_empty_rules(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.pool import db_conn

    with db_conn() as conn:
        # The scratch schema and everything in it are rolled back after verification.
        with conn.transaction(force_rollback=True):
            schema = scratch_schema(conn, "fictional_v32_upgrade")
            migrate_to_v31(conn)
            assert relation_exists(conn, schema, "merchant_category_rules")
            user = conn.execute(
                "INSERT INTO users (username, password_hash, invite_code) VALUES ('fictional-v32', 'x', 'x') RETURNING id"
            ).fetchone()["id"]
            paid = conn.execute(
                """INSERT INTO obligations (user_id, name, total_amount, remaining_amount)
                   VALUES (%s, 'Cicilan Fiksi', 100000, 0) RETURNING id""", (user,)
            ).fetchone()["id"]
            open_debt = conn.execute(
                """INSERT INTO obligations (user_id, name, total_amount, remaining_amount)
                   VALUES (%s, 'Pinjaman Fiksi', 100000, 40000) RETURNING id""", (user,)
            ).fetchone()["id"]

            conn.execute(V32.read_text())
            conn.execute(V32.read_text())

            assert not relation_exists(conn, schema, "merchant_category_rules")
            assert relation_exists(conn, schema, "uq_tx_user_idempotency")
            column = conn.execute(
                """SELECT is_nullable FROM information_schema.columns
                   WHERE table_schema = %s AND table_name = 'accounts' AND column_name = 'default_funding_account_id'""",
                (schema,),
            ).fetchone()
            assert column == {"is_nullable": "YES"}
            archived = {
                row["id"]: row["is_archived"]
                for row in conn.execute("SELECT id, is_archived FROM obligations WHERE user_id = %s", (user,)).fetchall()
            }
            assert archived == {paid: True, open_debt: False}

            account = conn.execute(
                "INSERT INTO accounts (user_id, name, type) VALUES (%s, 'Bank Fiksi', 'bank') RETURNING id", (user,)
            ).fetchone()["id"]
            insert = """INSERT INTO transactions (user_id, account_id, type, amount, idempotency_key)
                        VALUES (%s, %s, 'expense', 1000, 'fictional-key')"""
            conn.execute(insert, (user, account))
            with pytest.raises(psycopg.errors.UniqueViolation):
                with conn.transaction():
                    conn.execute(insert, (user, account))


def test_v32_refuses_to_drop_merchant_rules_holding_rows(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.pool import db_conn

    with db_conn() as conn:
        with conn.transaction(force_rollback=True):
            schema = scratch_schema(conn, "fictional_v32_rules")
            migrate_to_v31(conn)
            user = conn.execute(
                "INSERT INTO users (username, password_hash, invite_code) VALUES ('fictional-rules', 'x', 'x') RETURNING id"
            ).fetchone()["id"]
            category = conn.execute(
                "INSERT INTO categories (user_id, name, kind) VALUES (%s, 'Makanan Fiksi', 'expense') RETURNING id", (user,)
            ).fetchone()["id"]
            conn.execute(
                "INSERT INTO merchant_category_rules (user_id, merchant_pattern, category_id) VALUES (%s, 'Warung Fiksi', %s)",
                (user, category),
            )

            with pytest.raises(psycopg.errors.RaiseException, match="merchant_category_rules holds 1 row"):
                with conn.transaction():
                    conn.execute(V32.read_text())

            assert relation_exists(conn, schema, "merchant_category_rules")
            assert conn.execute("SELECT COUNT(*) AS count FROM merchant_category_rules").fetchone()["count"] == 1


def test_new_user_movement_categories_are_excluded_without_pillar(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.init_db import seed_user_defaults
    from app.db.pool import db_conn

    with db_conn() as conn:
        user = conn.execute(
            "INSERT INTO users (username, password_hash, invite_code) VALUES (%s, 'x', 'x') RETURNING id",
            (f"fictional_seed_{uuid4().hex[:10]}",),
        ).fetchone()["id"]
        conn.commit()
    try:
        seed_user_defaults(user)
        with db_conn() as conn:
            rows = conn.execute(
                """SELECT name, kind, kakeibo_type, is_excluded_from_budget FROM categories
                   WHERE user_id = %s ORDER BY name, kind""",
                (user,),
            ).fetchall()
    finally:
        with db_conn() as conn:
            conn.execute("DELETE FROM users WHERE id = %s", (user,))
            conn.commit()

    movement = [row for row in rows if row["name"] == "Internal Movement"]
    assert [(row["kind"], row["kakeibo_type"], row["is_excluded_from_budget"]) for row in movement] == [
        ("expense", None, True), ("income", None, True),
    ]
    assert not any(row["is_excluded_from_budget"] for row in rows if row["name"] != "Internal Movement")
