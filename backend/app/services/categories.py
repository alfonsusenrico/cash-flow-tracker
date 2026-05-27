"""Default financial categories for each user."""
from __future__ import annotations

from typing import Iterable


DEFAULT_CATEGORIES: tuple[tuple[str, str, str], ...] = (
    ("Salary", "income", "💵"),
    ("Freelance", "income", "💻"),
    ("Business Income", "income", "🏪"),
    ("Investment Income", "income", "📈"),
    ("Gift", "income", "🎁"),
    ("Receivable Collection", "income", "↘"),
    ("Other Income", "income", "💰"),
    ("Food & Drink", "expense", "🍽️"),
    ("Groceries", "expense", "🛒"),
    ("Transport", "expense", "🚗"),
    ("Housing", "expense", "🏠"),
    ("Utilities", "expense", "💡"),
    ("Bills", "expense", "🧾"),
    ("Health", "expense", "❤️"),
    ("Insurance", "expense", "🛡️"),
    ("Education", "expense", "📚"),
    ("Entertainment", "expense", "🎬"),
    ("Shopping", "expense", "🛍️"),
    ("Giving", "expense", "🤲"),
    ("Debt Payment", "expense", "💳"),
    ("Payable Payment", "expense", "↗"),
    ("Tax", "expense", "🏛️"),
    ("Other Expense", "expense", "📋"),
    ("Transfer", "transfer", "⇄"),
    ("Switching", "transfer", "⇄"),
    ("Opening Balance", "adjustment", "⚙️"),
    ("Correction", "adjustment", "✏️"),
)


def seed_default_categories(cur, username: str) -> None:
    """Idempotently ensure a user has the universal category starter set."""
    cur.execute(
        """
        INSERT INTO categories (user_id, name, kind, icon)
        SELECT u.user_id, c.name, c.kind, c.icon
        FROM users u
        CROSS JOIN (VALUES
        """
        + ",\n".join(["(%s, %s, %s)" for _ in DEFAULT_CATEGORIES])
        + """
        ) AS c(name, kind, icon)
        WHERE u.username=%s
        ON CONFLICT (user_id, name) DO NOTHING
        """,
        _flatten(DEFAULT_CATEGORIES) + [username],
    )


def ensure_switching_category(cur, username: str) -> str:
    """Return the user's canonical Switching category, creating it if needed."""
    cur.execute(
        """
        INSERT INTO categories (user_id, name, kind, icon, is_archived)
        SELECT user_id, 'Switching', 'transfer', '⇄', false
        FROM users
        WHERE username=%s
        ON CONFLICT (user_id, name)
        DO UPDATE SET kind='transfer', icon='⇄', is_archived=false
        RETURNING category_id::text
        """,
        (username,),
    )
    row = cur.fetchone()
    return row["category_id"]


def ensure_named_category(cur, username: str, *, name: str, kind: str, icon: str | None = None) -> str:
    """Return a named category for a user, creating/restoring it if needed."""
    cur.execute(
        """
        INSERT INTO categories (user_id, name, kind, icon, is_archived)
        SELECT user_id, %s, %s, %s, false
        FROM users
        WHERE username=%s
        ON CONFLICT (user_id, name)
        DO UPDATE SET kind=EXCLUDED.kind,
                      icon=COALESCE(categories.icon, EXCLUDED.icon),
                      is_archived=false
        RETURNING category_id::text
        """,
        (name, kind, icon, username),
    )
    row = cur.fetchone()
    return row["category_id"]


def _flatten(rows: Iterable[tuple[str, str, str]]) -> list[str]:
    values: list[str] = []
    for row in rows:
        values.extend(row)
    return values
