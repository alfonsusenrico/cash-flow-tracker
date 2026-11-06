import uuid

from app.db.pool import db_conn
from app.services.system_categories import INTERNAL_MOVEMENT, INVESTMENT, SEED_NAMES

# Starter pillars apply only when a new user's categories are seeded; existing users keep their own.
# Investasi and the movement and income categories carry none: they never count as spending.
DEFAULT_CATEGORIES = [
    {"name": "Makanan & Minuman", "icon": "utensils", "color": "#f97316", "kind": "expense", "kakeibo_type": "need"},
    {"name": "Jajan & Nongkrong", "icon": "coffee", "color": "#d97706", "kind": "expense", "kakeibo_type": "want"},
    {"name": "Belanja Harian", "icon": "shopping-cart", "color": "#10b981", "kind": "expense", "kakeibo_type": "need"},
    {"name": "Transportasi", "icon": "car", "color": "#3b82f6", "kind": "expense", "kakeibo_type": "need"},
    {"name": "Tagihan & Utilitas", "icon": "zap", "color": "#eab308", "kind": "expense", "kakeibo_type": "need"},
    {"name": "Belanja", "icon": "shopping-bag", "color": "#ec4899", "kind": "expense", "kakeibo_type": "want"},
    {"name": "Hiburan", "icon": "film", "color": "#8b5cf6", "kind": "expense", "kakeibo_type": "want"},
    {"name": "Kesehatan", "icon": "heart", "color": "#ef4444", "kind": "expense", "kakeibo_type": "need"},
    {"name": SEED_NAMES[INVESTMENT], "icon": "trending-up", "color": "#0ea5e9", "kind": "expense",
     "system_key": INVESTMENT},
    # Movement legs never count as spending, income or a Kakeibo pillar.
    {"name": SEED_NAMES[INTERNAL_MOVEMENT], "icon": "repeat", "color": "#64748b", "kind": "expense",
     "excluded_from_budget": True, "system_key": INTERNAL_MOVEMENT},
    {"name": SEED_NAMES[INTERNAL_MOVEMENT], "icon": "repeat", "color": "#64748b", "kind": "income",
     "excluded_from_budget": True, "system_key": INTERNAL_MOVEMENT},
    {"name": "Gaji", "icon": "dollar-sign", "color": "#22c55e", "kind": "income"},
    {"name": "Pendapatan Lain", "icon": "plus-circle", "color": "#14b8a6", "kind": "income"},
    {"name": "Patungan & Ganti Uang", "icon": "users", "color": "#0d9488", "kind": "income", "offsets_spending": True},
]

DEFAULT_ACCOUNTS = [
    {"name": "Uang Tunai", "type": "cash", "initial_balance": 0},
    {"name": "Rekening Utama", "type": "bank", "initial_balance": 0},
]


def seed_user_defaults(user_id: str | uuid.UUID) -> None:
    """Seeds default categories and starter accounts for a new user if empty."""
    uid = str(user_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Check existing categories
            cur.execute("SELECT COUNT(*) AS c FROM categories WHERE user_id = %s", (uid,))
            row = cur.fetchone()
            if row and row["c"] == 0:
                for cat in DEFAULT_CATEGORIES:
                    cur.execute("""
                        INSERT INTO categories
                            (user_id, name, icon, color, kind, kakeibo_type, offsets_spending, is_excluded_from_budget,
                             system_key)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (
                        uid, cat["name"], cat["icon"], cat["color"], cat["kind"],
                        cat.get("kakeibo_type"), cat.get("offsets_spending", False),
                        cat.get("excluded_from_budget", False), cat.get("system_key"),
                    ))

            # Check existing accounts
            cur.execute("SELECT COUNT(*) AS c FROM accounts WHERE user_id = %s", (uid,))
            row = cur.fetchone()
            if row and row["c"] == 0:
                for acc in DEFAULT_ACCOUNTS:
                    cur.execute("""
                        INSERT INTO accounts (user_id, name, type, initial_balance)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (uid, acc["name"], acc["type"], acc["initial_balance"]))
            conn.commit()
