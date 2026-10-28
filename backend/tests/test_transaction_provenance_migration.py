import json
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

ROOT = Path(__file__).resolve().parents[2]
VERSIONED = ROOT / "db/migrations/V28__transaction_provenance_and_reimbursements.sql"
MIRROR = ROOT / "backend/app/db/transaction_provenance_and_reimbursements.sql"
NEW_COLUMNS = {
    "source", "merchant", "review_status", "classified_by", "classification_confidence",
    "owner_edited_at", "offset_category_id",
}


def migrate_to_v27(conn):
    paths = sorted((ROOT / "db/migrations").glob("V*.sql"), key=lambda p: int(p.name.split("__")[0][1:]))
    for path in paths:
        if path.name.startswith("V28__"):
            break
        conn.execute(path.read_text())


def test_v28_backfills_provenance_once_without_touching_ledger_values(client, db_available):
    if not db_available:
        pytest.skip("Disposable database unavailable")
    from app.db.pool import db_conn

    assert VERSIONED.read_text() == MIRROR.read_text()
    schema = "fictional_provenance_upgrade_" + uuid4().hex
    with db_conn() as conn:
        # The scratch schema and everything in it are rolled back after verification.
        with conn.transaction(force_rollback=True):
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            conn.execute(sql.SQL("SET LOCAL search_path TO {}, public").format(sql.Identifier(schema)))
            migrate_to_v27(conn)

            def one(query, params=()):
                return conn.execute(query, params).fetchone()

            user = one("INSERT INTO users (username, password_hash, invite_code) VALUES ('fictional-owner', 'x', 'x') RETURNING id")["id"]
            other_user = one("INSERT INTO users (username, password_hash, invite_code) VALUES ('fictional-other', 'x', 'x') RETURNING id")["id"]
            bank = one("INSERT INTO accounts (user_id, name, type, initial_balance) VALUES (%s, 'Bank Fiksi', 'bank', 9000000) RETURNING id", (user,))["id"]
            wallet = one("INSERT INTO accounts (user_id, name, type) VALUES (%s, 'Dompet Fiksi', 'ewallet') RETURNING id", (user,))["id"]
            stock = one("INSERT INTO accounts (user_id, name, type, instrument_type) VALUES (%s, 'Saham Fiksi', 'investment', 'stock') RETURNING id", (user,))["id"]
            fund = one("INSERT INTO accounts (user_id, name, type, instrument_type) VALUES (%s, 'Reksa Fiksi', 'investment', 'mutual_fund') RETURNING id", (user,))["id"]
            food = one("INSERT INTO categories (user_id, name, kind) VALUES (%s, 'Makanan Fiksi', 'expense') RETURNING id", (user,))["id"]
            adjustment = one("INSERT INTO categories (user_id, name, kind, is_excluded_from_budget) VALUES (%s, 'Penyesuaian Saldo', 'expense', TRUE) RETURNING id", (user,))["id"]
            one("INSERT INTO categories (user_id, name, kind, kakeibo_type) VALUES (%s, 'Jajan & Nongkrong', 'expense', 'need') RETURNING id", (other_user,))
            rule = one(
                """INSERT INTO recurring_rules (user_id, name, type, amount, source_account_id, next_due_date)
                   VALUES (%s, 'Rutin Fiksi', 'expense', 1000, %s, CURRENT_DATE) RETURNING id""",
                (user, bank),
            )["id"]

            def tx(name, *, tx_type="expense", key=None, category=None, movement=None, role=None,
                   rule_id=None, edited_after="20 seconds", account=bank):
                row = one(
                    """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, date,
                           idempotency_key, movement_id, movement_role, recurring_rule_id, created_at, updated_at)
                       VALUES (%s, %s, %s, %s, 1000, %s, NOW(), %s, %s, %s, %s,
                               TIMESTAMPTZ '2026-10-01 10:00:00+00',
                               TIMESTAMPTZ '2026-10-01 10:00:00+00' + %s::interval)
                       RETURNING id""",
                    (user, account, category, tx_type, name, key, movement, role, rule_id, edited_after),
                )
                return row["id"]

            def event(transaction_id, source):
                event_id = uuid4()
                interpretation = json.dumps({"source": source}) if source else None
                conn.execute(
                    """INSERT INTO notification_events (id, user_id, device_id, package_name, post_time, payload_hash,
                           processing_state, transaction_id, interpretation, result_key, result_snapshot)
                       VALUES (%s, %s, 'fictional', 'com.fiksi', NOW(), %s, 'recorded', %s, %s, 'fiksi', '{}')""",
                    (event_id, user, uuid4().hex, transaction_id, interpretation),
                )
                return event_id

            ids = {}
            event_ai, event_rule, event_manual, event_move = uuid4(), uuid4(), uuid4(), uuid4()
            ids["ai"] = tx("ai", key=f"notification:{event_ai}")
            ids["rule_edited"] = tx("rule edited", key=f"notification:{event_rule}", edited_after="2 days")
            ids["link_only"] = tx("link only")
            ids["manual_resolution"] = tx("owner resolved", key=f"notification:{event_manual}")
            movement_id = uuid4()
            ids["move_out"] = tx("move out", key=f"notification:{event_move}:out", movement=movement_id, role="outbound")
            ids["move_in"] = tx("move in", tx_type="income", key=f"notification:{event_move}:in", movement=movement_id, role="inbound", account=wallet)
            for event_id, tx_key, source in [
                (event_ai, "ai", "ai"), (event_rule, "rule_edited", "deterministic"),
                (event_manual, "manual_resolution", "manual"), (event_move, "move_out", "ai"),
            ]:
                conn.execute(
                    """INSERT INTO notification_events (id, user_id, device_id, package_name, post_time, payload_hash,
                           processing_state, transaction_id, interpretation, result_key, result_snapshot)
                       VALUES (%s, %s, 'fictional', 'com.fiksi', NOW(), %s, 'recorded', %s, %s, 'fiksi', '{}')""",
                    (event_id, user, uuid4().hex, ids[tx_key], json.dumps({"source": source})),
                )
            event(ids["link_only"], None)
            ids["recurring_key"] = tx("recurring key", key=f"recurring:{rule}:2026-10-01")
            ids["payroll_key"] = tx("payroll key", key="payroll:fiksi:2026-10-01")
            ids["recurring_link"] = tx("recurring link", rule_id=rule)
            topup_id = uuid4()
            conn.execute(
                """INSERT INTO investment_topups (id, user_id, source_account_id, target_account_id, amount, date,
                       idempotency_key, request_fingerprint) VALUES (%s, %s, %s, %s, 1000, NOW(), 'fiksi', 'fiksi')""",
                (topup_id, user, bank, fund),
            )
            ids["topup_out"] = tx("topup out", key=f"investment-topup:{topup_id}:out", movement=topup_id, role="outbound")
            ids["topup_in"] = tx("topup in", tx_type="income", key=f"investment-topup:{topup_id}:in", movement=topup_id, role="inbound", account=fund)
            trade = uuid4()
            ids["trade_out"] = tx("trade out", movement=trade, role="outbound")
            ids["trade_in"] = tx("trade in", tx_type="income", movement=trade, role="inbound", account=stock)
            ids["reconcile"] = tx("adjustment", category=adjustment)
            plain_move = uuid4()
            ids["plain_out"] = tx("plain out", movement=plain_move, role="outbound")
            ids["plain_in"] = tx("plain in", tx_type="income", movement=plain_move, role="inbound", account=wallet)
            ids["manual"] = tx("manual", category=food, edited_after="3 days")

            before = {
                key: one("SELECT row_to_json(t) AS row FROM transactions t WHERE id = %s", (value,))["row"]
                for key, value in ids.items()
            }
            conn.execute(VERSIONED.read_text())

            after = {}
            for key, value in ids.items():
                row = one("SELECT row_to_json(t) AS row FROM transactions t WHERE id = %s", (value,))["row"]
                after[key] = {column: row.pop(column) for column in NEW_COLUMNS}
                assert row == before[key], key

            expected = {
                "ai": ("notification", "auto", "ai"),
                "rule_edited": ("notification", "reviewed", "rule"),
                "link_only": ("notification", "auto", "rule"),
                "manual_resolution": ("notification", "reviewed", "owner"),
                "move_out": ("notification", "auto", "ai"),
                "move_in": ("notification", "auto", "ai"),
                "recurring_key": ("recurring", "reviewed", "system"),
                "payroll_key": ("payroll", "reviewed", "system"),
                "recurring_link": ("recurring", "reviewed", "system"),
                "topup_out": ("investment", "reviewed", "system"),
                "topup_in": ("investment", "reviewed", "system"),
                "trade_out": ("investment", "reviewed", "system"),
                "trade_in": ("investment", "reviewed", "system"),
                "reconcile": ("reconcile", "reviewed", "system"),
                "plain_out": ("movement", "reviewed", "system"),
                "plain_in": ("movement", "reviewed", "system"),
                "manual": ("manual", "reviewed", "owner"),
            }
            for key, (source, review_status, classified_by) in expected.items():
                values = after[key]
                assert (values["source"], values["review_status"], values["classified_by"]) == (source, review_status, classified_by), key
                assert values["merchant"] is None and values["classification_confidence"] is None, key
                assert values["owner_edited_at"] is None and values["offset_category_id"] is None, key

            categories = conn.execute(
                "SELECT user_id, name, kind, kakeibo_type, offsets_spending FROM categories WHERE name IN ('Jajan & Nongkrong', 'Patungan & Ganti Uang')"
            ).fetchall()
            by_key = {(row["user_id"], row["name"]): row for row in categories}
            assert by_key[(user, "Jajan & Nongkrong")]["kakeibo_type"] == "want"
            assert by_key[(other_user, "Jajan & Nongkrong")]["kakeibo_type"] == "need"
            for owner in (user, other_user):
                reimbursement = by_key[(owner, "Patungan & Ganti Uang")]
                assert reimbursement["kind"] == "income" and reimbursement["offsets_spending"] is True
            assert one("SELECT COUNT(*) AS count FROM transactions WHERE category_id IN (SELECT id FROM categories WHERE name = 'Jajan & Nongkrong')")["count"] == 0

            # The owner reviews a row and deletes the new category; a restart must change neither.
            conn.execute("UPDATE transactions SET review_status = 'reviewed' WHERE id = %s", (ids["ai"],))
            conn.execute("DELETE FROM categories WHERE user_id = %s AND name = 'Jajan & Nongkrong'", (user,))
            snapshot = conn.execute("SELECT id, source, review_status, classified_by FROM transactions ORDER BY id").fetchall()
            conn.execute(MIRROR.read_text())
            assert conn.execute("SELECT id, source, review_status, classified_by FROM transactions ORDER BY id").fetchall() == snapshot
            assert one("SELECT COUNT(*) AS count FROM categories WHERE user_id = %s AND name = 'Jajan & Nongkrong'", (user,))["count"] == 0

            def rejects(query, params):
                with pytest.raises(psycopg.errors.CheckViolation):
                    with conn.transaction():
                        conn.execute(query, params)

            insert = "INSERT INTO transactions (user_id, account_id, type, amount, source, review_status, classified_by, classification_confidence, offset_category_id) VALUES (%s, %s, %s, 1000, %s, %s, %s, %s, %s)"
            rejects(insert, (user, bank, "expense", "bot", "reviewed", "system", None, None))
            rejects(insert, (user, bank, "expense", "manual", "maybe", "owner", None, None))
            rejects(insert, (user, bank, "expense", "manual", "reviewed", "robot", None, None))
            rejects(insert, (user, bank, "expense", "manual", "reviewed", "owner", 1.5, None))
            rejects(insert, (user, bank, "expense", "manual", "reviewed", "owner", None, food))
            rejects("UPDATE transactions SET merchant = '   ' WHERE id = %s", (ids["manual"],))
            rejects("INSERT INTO categories (user_id, name, kind, offsets_spending) VALUES (%s, 'Salah', 'expense', TRUE)", (user,))
