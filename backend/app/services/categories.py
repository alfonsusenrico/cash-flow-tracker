"""Default financial categories for each user."""
from __future__ import annotations

from typing import Iterable


DEFAULT_CATEGORIES: tuple[tuple[str, str, str], ...] = (
    ("Salary", "income", "💵"),
    ("Freelance", "income", "💻"),
    ("Business Income", "income", "🏪"),
    ("Investment Income", "income", "📈"),
    ("Gift", "income", "🎁"),
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
    ("Tax", "expense", "🏛️"),
    ("Other Expense", "expense", "📋"),
    ("Transfer", "transfer", "⇄"),
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


def _flatten(rows: Iterable[tuple[str, str, str]]) -> list[str]:
    values: list[str] = []
    for row in rows:
        values.extend(row)
    return values
