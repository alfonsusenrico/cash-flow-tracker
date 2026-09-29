from pathlib import Path
from uuid import uuid4

import psycopg
import pytest


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "app" / "db" / "notification_processing.sql"
MIGRATION_PATH = Path(__file__).resolve().parents[2] / "db" / "migrations" / "V20__notification_processing.sql"


def test_notification_startup_schema_matches_versioned_migration():
    assert SCHEMA_PATH.read_text() == MIGRATION_PATH.read_text()


def test_additive_notification_upgrade_preserves_legacy_state(db_url, db_available):
    if not db_available:
        pytest.skip("Isolated test database unavailable")
    user_id, account_id, category_id, transaction_id, event_id = [uuid4() for _ in range(5)]
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (id, username, password_hash, invite_code) VALUES (%s, %s, 'synthetic-hash', 'TEST')", (user_id, f"migration_{user_id.hex}"))
            cur.execute("INSERT INTO accounts (id, user_id, name, type, initial_balance) VALUES (%s, %s, 'BCA Synthetic', 'bank', 123456)", (account_id, user_id))
            cur.execute("INSERT INTO categories (id, user_id, name, kind) VALUES (%s, %s, 'Synthetic expense', 'expense')", (category_id, user_id))
            cur.execute("INSERT INTO transactions (id, user_id, account_id, category_id, type, amount, date) VALUES (%s, %s, %s, %s, 'expense', 1000, NOW())", (transaction_id, user_id, account_id, category_id))
            cur.execute("INSERT INTO notification_events (id, user_id, device_id, package_name, post_time, payload_hash, transaction_id) VALUES (%s, %s, 'synthetic-device', 'com.bca', NOW(), %s, %s)", (event_id, user_id, event_id.hex, transaction_id))
            cur.execute(MIGRATION_PATH.read_text())
            cur.execute(MIGRATION_PATH.read_text())
            cur.execute("SELECT processing_state, attempt_count, transaction_id FROM notification_events WHERE id = %s", (event_id,))
            assert cur.fetchone() == (None, 0, transaction_id)
            cur.execute("SELECT initial_balance FROM accounts WHERE id = %s", (account_id,))
            assert cur.fetchone()[0] == 123456
            cur.execute("SELECT amount FROM transactions WHERE id = %s", (transaction_id,))
            assert cur.fetchone()[0] == 1000
            cur.execute("DELETE FROM users WHERE id = %s", (user_id,))
