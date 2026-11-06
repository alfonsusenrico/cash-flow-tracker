"""V53 account language and region (add-language-system D13, task 2.1) on a synthetic V52 database.

The migration runs in a throwaway schema inside a forced-rollback transaction. Owners and names are synthetic.
"""
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = sorted((ROOT / "db/migrations").glob("V*.sql"), key=lambda path: int(path.name.split("__")[0][1:]))
V53 = ROOT / "db/migrations/V53__account_language_and_region.sql"


def migrate_schema_to_v52(conn) -> str:
    schema = "synthetic_account_language_" + uuid4().hex
    conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    conn.execute(sql.SQL("SET LOCAL search_path TO {}, public").format(sql.Identifier(schema)))
    for path in MIGRATIONS:
        if path.name.startswith("V53__"):
            break
        conn.execute(path.read_text())
    return schema


def synthetic_owner(conn, label: str, currency: str) -> str:
    owner = str(conn.execute(
        """INSERT INTO users (username, name, password_hash, invite_code, currency)
           VALUES (%s, %s, 'synthetic', 'synthetic', %s) RETURNING id""",
        (f"lang_{label}_{uuid4().hex[:8]}", f"Pemilik {label.title()}", currency),
    ).fetchone()["id"])
    conn.execute("INSERT INTO categories (user_id, name, kind) VALUES (%s, 'Makanan & Minuman', 'expense')", (owner,))
    conn.execute("INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'Rekening Utama', 'bank', 1250000)", (owner,))
    return owner


def test_v53_makes_existing_accounts_indonesian_and_new_ones_english(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.pool import db_conn

    with db_conn() as conn, conn.transaction(force_rollback=True):
        schema = migrate_schema_to_v52(conn)
        owners = [synthetic_owner(conn, label, currency) for label, currency in (
            ("sari", "IDR"), ("raka", "USD"), ("mira", "IDR"),
        )]
        before_users = conn.execute("SELECT * FROM users ORDER BY id").fetchall()
        before_categories = conn.execute("SELECT * FROM categories ORDER BY id").fetchall()
        before_accounts = conn.execute("SELECT * FROM accounts ORDER BY id").fetchall()

        conn.execute(V53.read_text())

        migrated = conn.execute("SELECT * FROM users ORDER BY id").fetchall()
        assert len(migrated) == 3
        assert {str(row["id"]) for row in migrated} == set(owners)
        assert {(row["language"], row["region"]) for row in migrated} == {("id", "ID")}
        # Additive: every earlier column keeps its value, and no other table changed.
        assert [{key: row[key] for key in before_users[0]} for row in migrated] == before_users
        assert conn.execute("SELECT * FROM categories ORDER BY id").fetchall() == before_categories
        assert conn.execute("SELECT * FROM accounts ORDER BY id").fetchall() == before_accounts

        created = conn.execute(
            """INSERT INTO users (username, password_hash, invite_code)
               VALUES (%s, 'synthetic', 'synthetic') RETURNING language, region""",
            (f"lang_new_{uuid4().hex[:8]}",),
        ).fetchone()
        assert (created["language"], created["region"]) == ("en", "ID")

        columns = {
            row["column_name"]: row for row in conn.execute(
                """SELECT column_name, is_nullable, data_type, character_maximum_length FROM information_schema.columns
                   WHERE table_schema = %s AND table_name = 'users' AND column_name IN ('language', 'region')""",
                (schema,),
            ).fetchall()
        }
        assert columns["language"]["is_nullable"] == "NO" and columns["language"]["character_maximum_length"] == 8
        assert columns["region"]["is_nullable"] == "NO" and columns["region"]["data_type"] == "character"

        owner = owners[0]
        with pytest.raises(psycopg.errors.CheckViolation, match="ck_users_language"), conn.transaction():
            conn.execute("UPDATE users SET language = 'fr' WHERE id = %s", (owner,))
        with pytest.raises(psycopg.errors.CheckViolation, match="ck_users_region"), conn.transaction():
            conn.execute("UPDATE users SET region = 'SG' WHERE id = %s", (owner,))
        with pytest.raises(psycopg.errors.NotNullViolation), conn.transaction():
            conn.execute("UPDATE users SET language = NULL WHERE id = %s", (owner,))
        assert conn.execute("SELECT language, region FROM users WHERE id = %s", (owner,)).fetchone() == {
            "language": "id", "region": "ID",
        }
