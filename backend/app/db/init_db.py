import uuid

from app.db.pool import db_conn
from app.services.system_categories import INTERNAL_MOVEMENT, INVESTMENT, seed_names

# Starter names per account language (add-language-system D15, Appendix B), by seed key. Everything else a
# starter row carries is the same in every language, so money rules and pillars never depend on the language.
# Once created they are the owner's names and are never translated. System categories take theirs from
# `system_categories`, keyed by their system key.
_ORDINARY_STARTER_NAMES = {
    "id": {
        "food": "Makanan & Minuman",
        "snacks": "Jajan & Nongkrong",
        "groceries": "Belanja Harian",
        "transport": "Transportasi",
        "bills": "Tagihan & Utilitas",
        "shopping": "Belanja",
        "entertainment": "Hiburan",
        "health": "Kesehatan",
        "salary": "Gaji",
        "other_income": "Pendapatan Lain",
        "reimbursement": "Patungan & Ganti Uang",
        "cash_account": "Uang Tunai",
        "bank_account": "Rekening Utama",
    },
    "en": {
        "food": "Food & drinks",
        "snacks": "Snacks & going out",
        "groceries": "Groceries",
        "transport": "Transport",
        "bills": "Bills & utilities",
        "shopping": "Shopping",
        "entertainment": "Entertainment",
        "health": "Health",
        "salary": "Salary",
        "other_income": "Other income",
        "reimbursement": "Reimbursements",
        "cash_account": "Cash",
        "bank_account": "Main account",
    },
}
STARTER_NAMES = {
    language: {**names, **seed_names(language)} for language, names in _ORDINARY_STARTER_NAMES.items()
}

# Starter pillars apply only when a new user's categories are seeded; existing users keep their own.
# Investasi and the movement and income categories carry none: they never count as spending.
STARTER_CATEGORIES = [
    {"seed": "food", "icon": "utensils", "color": "#f97316", "kind": "expense", "kakeibo_type": "need"},
    {"seed": "snacks", "icon": "coffee", "color": "#d97706", "kind": "expense", "kakeibo_type": "want"},
    {"seed": "groceries", "icon": "shopping-cart", "color": "#10b981", "kind": "expense", "kakeibo_type": "need"},
    {"seed": "transport", "icon": "car", "color": "#3b82f6", "kind": "expense", "kakeibo_type": "need"},
    {"seed": "bills", "icon": "zap", "color": "#eab308", "kind": "expense", "kakeibo_type": "need"},
    {"seed": "shopping", "icon": "shopping-bag", "color": "#ec4899", "kind": "expense", "kakeibo_type": "want"},
    {"seed": "entertainment", "icon": "film", "color": "#8b5cf6", "kind": "expense", "kakeibo_type": "want"},
    {"seed": "health", "icon": "heart", "color": "#ef4444", "kind": "expense", "kakeibo_type": "need"},
    {"seed": INVESTMENT, "icon": "trending-up", "color": "#0ea5e9", "kind": "expense", "system_key": INVESTMENT},
    # Movement legs never count as spending, income or a Kakeibo pillar.
    {"seed": INTERNAL_MOVEMENT, "icon": "repeat", "color": "#64748b", "kind": "expense",
     "excluded_from_budget": True, "system_key": INTERNAL_MOVEMENT},
    {"seed": INTERNAL_MOVEMENT, "icon": "repeat", "color": "#64748b", "kind": "income",
     "excluded_from_budget": True, "system_key": INTERNAL_MOVEMENT},
    {"seed": "salary", "icon": "dollar-sign", "color": "#22c55e", "kind": "income"},
    {"seed": "other_income", "icon": "plus-circle", "color": "#14b8a6", "kind": "income"},
    {"seed": "reimbursement", "icon": "users", "color": "#0d9488", "kind": "income", "offsets_spending": True},
]

STARTER_ACCOUNTS = [
    {"seed": "cash_account", "type": "cash", "initial_balance": 0},
    {"seed": "bank_account", "type": "bank", "initial_balance": 0},
]


def default_categories(language: str = "id") -> list[dict]:
    """The starter categories with their names in `language`."""
    names = STARTER_NAMES[language]
    return [{**category, "name": names[category["seed"]]} for category in STARTER_CATEGORIES]


def default_accounts(language: str = "id") -> list[dict]:
    """The starter accounts with their names in `language`."""
    names = STARTER_NAMES[language]
    return [{**account, "name": names[account["seed"]]} for account in STARTER_ACCOUNTS]


# The Indonesian starter set, as every account had it before languages existed.
DEFAULT_CATEGORIES = default_categories("id")
DEFAULT_ACCOUNTS = default_accounts("id")


def seed_user_defaults(user_id: str | uuid.UUID, language: str = "id") -> None:
    """Seeds default categories and starter accounts for a new user if empty, named in `language`."""
    uid = str(user_id)
    categories = default_categories(language)
    accounts = default_accounts(language)
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Check existing categories
            cur.execute("SELECT COUNT(*) AS c FROM categories WHERE user_id = %s", (uid,))
            row = cur.fetchone()
            if row and row["c"] == 0:
                for cat in categories:
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
                for acc in accounts:
                    cur.execute("""
                        INSERT INTO accounts (user_id, name, type, initial_balance)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (uid, acc["name"], acc["type"], acc["initial_balance"]))
            conn.commit()
