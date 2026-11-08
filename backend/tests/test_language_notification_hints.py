"""myBCA category hints for an account seeded in English (add-language-system D15, task 2.5).

The synthetic owner of `notification_owner` is "Raka Purnama"; every text and amount is synthetic.
"""
from uuid import uuid4

import pytest

from test_notification_processing_database import ingest, notification, notification_owner  # noqa: F401 - fixture

MYBCA = "com.bca.mybca.omni.android"


def as_english(owner, categories):
    """The owner as an English account: movement categories under their English seed name, plus `categories`."""
    from app.db.pool import db_conn

    with db_conn() as conn:
        conn.execute("UPDATE users SET language = 'en' WHERE id = %s", (owner["user_id"],))
        conn.execute(
            "UPDATE categories SET name = 'Transfer' WHERE user_id = %s AND system_key = 'internal_movement'",
            (owner["user_id"],),
        )
        conn.execute("DELETE FROM categories WHERE user_id = %s AND name = 'Transfer Masuk'", (owner["user_id"],))
        for key, name, kind in categories:
            owner[key] = str(uuid4())
            conn.execute(
                "INSERT INTO categories (id, user_id, name, kind) VALUES (%s, %s, %s, %s)",
                (owner[key], owner["user_id"], name, kind),
            )
        conn.commit()
    return owner


def recorded_category(owner, event_id):
    from app.db.pool import db_conn

    with db_conn() as conn:
        row = conn.execute(
            """SELECT c.name, c.system_key FROM notification_events e
               JOIN transactions t ON t.id = e.transaction_id LEFT JOIN categories c ON c.id = t.category_id
               WHERE e.id = %s AND e.user_id = %s""",
            (event_id, owner["user_id"]),
        ).fetchone()
    return (row["name"], row["system_key"]) if row else None


@pytest.mark.parametrize("hint_text,name", [
    ("You spent IDR 45,000.00 at Shopping.", "Shopping"),
    ("You spent IDR 32,000.00 at Food & Beverage.", "Food & drinks"),
    ("You spent IDR 21,000.00 at Transportation.", "Transport"),
])
def test_mybca_hint_files_under_the_english_starter_category(client, notification_owner, hint_text, name):
    owner = as_english(notification_owner, [
        ("shopping", "Shopping", "expense"), ("food", "Food & drinks", "expense"), ("transport", "Transport", "expense"),
    ])
    result = ingest(client, notification(hint_text, MYBCA))["results"][0]
    assert result["status"] == "recorded" and result["type"] == "expense", result
    assert recorded_category(owner, result["event_id"]) == (name, None)


def test_indonesian_account_still_files_shopping_under_belanja(client, notification_owner):
    from app.db.pool import db_conn

    with db_conn() as conn:
        conn.execute(
            "INSERT INTO categories (user_id, name, kind) VALUES (%s, 'Belanja', 'expense')", (notification_owner["user_id"],),
        )
        conn.commit()
    result = ingest(client, notification("You spent IDR 45,000.00 at Shopping.", MYBCA))["results"][0]
    assert recorded_category(notification_owner, result["event_id"]) == ("Belanja", None)


@pytest.mark.parametrize("hint_text", ["You spent IDR 300,000.00 at Account Transfer.", "You spent IDR 300,000.00 at Transfer."])
def test_a_transfer_hint_never_reaches_the_english_movement_category(client, notification_owner, hint_text):
    owner = as_english(notification_owner, [])
    result = ingest(client, notification(hint_text, MYBCA))["results"][0]
    assert result["status"] == "recorded" and result["type"] == "expense", result
    # A payment to someone else is spending. The English movement category is named "Transfer", but a name never
    # reaches a system category, so the row stays uncategorised as it does for an Indonesian account.
    assert recorded_category(owner, result["event_id"]) == (None, None)
