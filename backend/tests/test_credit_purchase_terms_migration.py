"""V54 credit purchase terms (add-credit-spending-flow tasks 1.2 and 1.4) on a synthetic V53 database.

The migration runs in a throwaway schema inside a forced-rollback transaction. Owners, names and amounts are
synthetic. Figures are computed with the report services on the same connection before and after V54.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

ROOT = Path(__file__).resolve().parents[2]
MIGRATION_DIR = ROOT / "db/migrations"
VERSION = re.compile(r"^V(\d+)__.+\.sql$")
MIGRATIONS = sorted(MIGRATION_DIR.glob("V*.sql"), key=lambda path: int(VERSION.match(path.name).group(1)))
# Found by name, so a rename to the next free version on rebase (design D15) needs no test change.
[V54] = MIGRATION_DIR.glob("V*__credit_purchase_terms.sql")


def migrate_schema_to_v53(conn) -> str:
    schema = "synthetic_credit_terms_" + uuid4().hex
    conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    conn.execute(sql.SQL("SET LOCAL search_path TO {}, public").format(sql.Identifier(schema)))
    for path in MIGRATIONS:
        if path == V54:
            break
        conn.execute(path.read_text())
    return schema


def insert(conn, query: str, params: tuple) -> str:
    return str(conn.execute(query + " RETURNING id", params).fetchone()["id"])


def synthetic_owner(conn, label: str) -> dict[str, str]:
    owner = insert(conn, "INSERT INTO users (username, password_hash, invite_code, language) VALUES (%s, 'synthetic', 'synthetic', 'id')",
                   (f"credit_{label}_{uuid4().hex[:8]}",))
    ids = {"owner": owner}
    ids["bank"] = insert(conn, "INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'Bank Contoh', 'bank', 5000000)", (owner,))
    ids["credit"] = insert(
        conn,
        "INSERT INTO accounts (user_id, name, type, initial_balance, credit_limit, payment_day, role) VALUES (%s, 'PayLater Contoh', 'credit', -300000, 2000000, 11, 'utang')",
        (owner,),
    )
    ids["makan"] = insert(conn, "INSERT INTO categories (user_id, name, kind, kakeibo_type) VALUES (%s, 'Makan Contoh', 'expense', 'need')", (owner,))
    ids["gaji"] = insert(conn, "INSERT INTO categories (user_id, name, kind) VALUES (%s, 'Gaji', 'income')", (owner,))
    return ids


def expense(conn, owner: dict[str, str], account: str, category: str, amount: int, when: datetime, **extra) -> str:
    columns = ["user_id", "account_id", "category_id", "type", "amount", "date", "source", "review_status", *extra]
    values = [owner["owner"], owner[account], owner[category], "expense", amount, when, "manual", "reviewed", *extra.values()]
    return insert(conn, f"INSERT INTO transactions ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})", tuple(values))


def figures(conn, owner_id: str, start: datetime, end: datetime) -> dict:
    """Beranda's Kakeibo and credit figures, the report and analytics category breakdown, the window totals."""
    from app.routers.dashboard import get_kakeibo_breakdown
    from app.services.credit_figures import credit_spending
    from app.services.observed_insights import window_figures
    from app.services.reimbursements import load_spending_offsets
    from app.services.report_figures import category_breakdown

    with conn.cursor() as cur:
        totals = window_figures(cur, owner_id, start, end)
        offsets = load_spending_offsets(cur, owner_id, start, end)
        return {
            "totals": {key: value for key, value in totals.items() if key != "offsets"},
            "offsets": offsets,
            "kakeibo": get_kakeibo_breakdown(cur, owner_id, start, end, totals["inflow"], offsets),
            "categories": category_breakdown(cur, owner_id, start, end, offsets),
            "credit": credit_spending(cur, owner_id, start, end, offsets),
        }


def test_v54_keys_only_the_exact_expense_fee_category_and_moves_no_figure(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.pool import db_conn

    now = datetime.now(timezone.utc)
    start, end = now - timedelta(days=40), now + timedelta(days=1)
    with db_conn() as conn, conn.transaction(force_rollback=True):
        schema = migrate_schema_to_v53(conn)
        first, second = synthetic_owner(conn, "first"), synthetic_owner(conn, "second")
        fee_category = insert(conn, "INSERT INTO categories (user_id, name, kind, kakeibo_type) VALUES (%s, 'Biaya kredit', 'expense', 'unexpected')",
                              (first["owner"],))
        first["fee"] = fee_category
        income_fee = insert(conn, "INSERT INTO categories (user_id, name, kind) VALUES (%s, 'Biaya kredit', 'income')", (first["owner"],))
        lower_case = insert(conn, "INSERT INTO categories (user_id, name, kind) VALUES (%s, 'biaya kredit', 'expense')", (second["owner"],))
        renamed = insert(conn, "INSERT INTO categories (user_id, name, kind, kakeibo_type) VALUES (%s, 'Bunga Contoh', 'expense', 'unexpected')",
                         (second["owner"],))
        second["fee"] = renamed
        for owner in (first, second):
            expense(conn, owner, "credit", "makan", 120_000, now - timedelta(days=2))
            expense(conn, owner, "bank", "makan", 80_000, now - timedelta(days=3))
            expense(conn, owner, "credit", "fee", 10_000, now - timedelta(days=1))
            insert(conn, """INSERT INTO transactions (user_id, account_id, category_id, type, amount, date, source, review_status)
                            VALUES (%s, %s, %s, 'income', 4000000, %s, 'manual', 'reviewed')""",
                   (owner["owner"], owner["bank"], owner["gaji"], now - timedelta(days=5)))
        before = {owner["owner"]: figures(conn, owner["owner"], start, end) for owner in (first, second)}
        categories_before = conn.execute("SELECT id, name, kind, kakeibo_type, is_excluded_from_budget FROM categories ORDER BY id").fetchall()

        conn.execute(V54.read_text())

        keyed = {str(row["id"]): row["system_key"] for row in conn.execute("SELECT id, system_key FROM categories").fetchall()}
        assert keyed[fee_category] == "credit_fee"
        assert keyed[income_fee] is None and keyed[lower_case] is None and keyed[renamed] is None
        assert [key for key in keyed.values() if key == "credit_fee"] == ["credit_fee"]
        assert conn.execute(
            "SELECT id, name, kind, kakeibo_type, is_excluded_from_budget FROM categories ORDER BY id").fetchall() == categories_before
        assert conn.execute("SELECT COUNT(*) AS n FROM accounts WHERE statement_day IS NOT NULL").fetchone()["n"] == 0
        assert conn.execute("SELECT COUNT(*) AS n FROM credit_purchase_terms").fetchone()["n"] == 0
        after = {owner["owner"]: figures(conn, owner["owner"], start, end) for owner in (first, second)}
        assert after == before
        assert before[first["owner"]]["credit"]["spent_on_credit"] == 130_000

        # Running the file again on a V54 schema succeeds and changes nothing.
        snapshot = conn.execute("SELECT id, system_key FROM categories ORDER BY id").fetchall()
        conn.execute(V54.read_text())
        assert conn.execute("SELECT id, system_key FROM categories ORDER BY id").fetchall() == snapshot

        purchase = expense(conn, first, "credit", "makan", 300_000, now)
        for statement in (
            ("UPDATE accounts SET statement_day = 0 WHERE id = %s", (first["credit"],)),
            ("UPDATE accounts SET statement_day = 32 WHERE id = %s", (first["credit"],)),
            ("INSERT INTO credit_purchase_terms (transaction_id, user_id, installment_count) VALUES (%s, %s, 0)", (purchase, first["owner"])),
            ("INSERT INTO credit_purchase_terms (transaction_id, user_id, installment_count) VALUES (%s, %s, 37)", (purchase, first["owner"])),
            ("INSERT INTO credit_purchase_terms (transaction_id, user_id, first_due_date) VALUES (%s, %s, '2026-12-11')",
             (purchase, first["owner"])),
            ("INSERT INTO credit_purchase_terms (transaction_id, user_id, first_due_date_set_by) VALUES (%s, %s, 'owner')",
             (purchase, first["owner"])),
            ("INSERT INTO credit_purchase_terms (transaction_id, user_id, first_due_date, first_due_date_set_by) VALUES (%s, %s, '2026-12-11', 'bank')",
             (purchase, first["owner"])),
            ("UPDATE categories SET system_key = 'credit_fees' WHERE id = %s", (lower_case,)),
        ):
            with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
                conn.execute(*statement)
        conn.execute("UPDATE accounts SET statement_day = 31 WHERE id = %s", (first["credit"],))
        conn.execute(
            "INSERT INTO credit_purchase_terms (transaction_id, user_id, installment_count, first_due_date, first_due_date_set_by) VALUES (%s, %s, 36, '2026-12-11', 'owner')",
            (purchase, first["owner"]),
        )
        conn.execute("DELETE FROM transactions WHERE id = %s", (purchase,))
        assert conn.execute("SELECT COUNT(*) AS n FROM credit_purchase_terms").fetchone()["n"] == 0
        index = conn.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = %s AND indexname = 'idx_credit_purchase_terms_user'", (schema,),
        ).fetchone()
        assert "(user_id)" in index["indexdef"]


def test_v54_keeps_an_owner_with_a_keyed_fee_category_as_is(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.pool import db_conn

    with db_conn() as conn, conn.transaction(force_rollback=True):
        migrate_schema_to_v53(conn)
        owner = synthetic_owner(conn, "keyed")
        conn.execute("ALTER TABLE categories DROP CONSTRAINT categories_system_key_check")
        keyed = insert(conn, "INSERT INTO categories (user_id, name, kind, system_key) VALUES (%s, 'Bunga Contoh', 'expense', 'credit_fee')",
                       (owner["owner"],))
        plain = insert(conn, "INSERT INTO categories (user_id, name, kind) VALUES (%s, 'Biaya kredit', 'expense')", (owner["owner"],))
        conn.execute(V54.read_text())
        rows = {str(row["id"]): row["system_key"] for row in conn.execute(
            "SELECT id, system_key FROM categories WHERE user_id = %s", (owner["owner"],)).fetchall()}
        assert rows[keyed] == "credit_fee" and rows[plain] is None


def test_migration_versions_are_unique_and_v54_follows_the_highest_earlier_one():
    versions = [int(VERSION.match(path.name).group(1)) for path in MIGRATION_DIR.glob("V*.sql")]
    assert len(versions) == len(set(versions)), sorted(version for version in versions if versions.count(version) > 1)
    terms_version = int(VERSION.match(V54.name).group(1))
    assert terms_version == max(version for version in versions if version < terms_version) + 1
    assert V54.exists()
