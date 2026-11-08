"""System categories: the investment, movement, balance-adjustment and credit-fee categories rules treat specially.

Rules identify them by `categories.system_key`, never by their display name, so renaming one changes only
its name (add-category-system-keys D1, D5). Ordinary categories carry no key. Every key a rule compares
with, every SQL predicate over the key and the find-or-create of a system category live here.
"""

from __future__ import annotations

INVESTMENT = "investment"
INTERNAL_MOVEMENT = "internal_movement"
BALANCE_ADJUSTMENT = "balance_adjustment"
# Interest or a fee on a credit account (add-credit-spending-flow D8); it is reportable, so fees stay spending.
CREDIT_FEE = "credit_fee"
SYSTEM_KEYS = (INVESTMENT, INTERNAL_MOVEMENT, BALANCE_ADJUSTMENT, CREDIT_FEE)

# Neither spending nor income in any report, whatever the category is named.
NON_REPORTABLE_KEYS = (INTERNAL_MOVEMENT, INVESTMENT)

# The names a system category is created with, in the account's language (add-language-system D15); after
# that the owner may rename it freely, and switching language never renames it.
SEED_NAMES_BY_LANGUAGE = {
    "id": {
        INVESTMENT: "Investasi",
        INTERNAL_MOVEMENT: "Internal Movement",
        BALANCE_ADJUSTMENT: "Penyesuaian Saldo",
        CREDIT_FEE: "Biaya kredit",
    },
    "en": {
        INVESTMENT: "Investments",
        INTERNAL_MOVEMENT: "Transfer",
        BALANCE_ADJUSTMENT: "Balance adjustment",
        CREDIT_FEE: "Credit fee",
    },
}
# The Indonesian names: every account created before languages existed, and the parser's hint vocabulary.
SEED_NAMES = SEED_NAMES_BY_LANGUAGE["id"]


def seed_names(language: str | None) -> dict[str, str]:
    """System category seed names for an account language; Indonesian for an unknown one, as before V53."""
    return SEED_NAMES_BY_LANGUAGE.get(language or "id", SEED_NAMES)


# The parser's `category_hint` values that stand for a system category rather than a category name (D7).
HINT_KEYS = {
    SEED_NAMES[INTERNAL_MOVEMENT]: INTERNAL_MOVEMENT,
    SEED_NAMES[INVESTMENT]: INVESTMENT,
}

# How "ensure" creates a missing system category, and what it repairs on an existing one. The movement and
# adjustment categories are excluded from budget with no pillar, as their helpers always made them, and are
# repaired to that. The credit fee is spending in the unexpected pillar, as "Biaya kredit" always was, and
# the owner may change how it is classified, so nothing is repaired on it.
_ENSURE_SPECS = {
    INTERNAL_MOVEMENT: {"icon": "arrows-right-left", "color": "#3b82f6", "excluded": True, "pillar": None,
                        "repair": True, "unarchive": False},
    BALANCE_ADJUSTMENT: {"icon": "wallet", "color": "#64748b", "excluded": True, "pillar": None,
                         "repair": True, "unarchive": True},
    CREDIT_FEE: {"icon": "receipt", "color": "#64748b", "excluded": False, "pillar": "unexpected",
                 "repair": False, "unarchive": False},
}
_SQL_LIST = ", ".join(f"'{key}'" for key in NON_REPORTABLE_KEYS)


def _require_key(key: str) -> str:
    if key not in SYSTEM_KEYS:
        raise ValueError(f"unknown system category key: {key!r}")
    return key


def is_system_sql(category: str, key: str) -> str:
    """True when the category row exists and carries `key`; FALSE for a missing category or another key."""
    return f"COALESCE({category}.system_key = '{_require_key(key)}', FALSE)"


def non_reportable_sql(category: str = "c") -> str:
    """True when the category is one that reports leave out (movement or investment); never NULL."""
    return f"COALESCE({category}.system_key IN ({_SQL_LIST}), FALSE)"


def reportable_key_sql(category: str = "c") -> str:
    """True for a missing category or one whose key reports count; the negation of `non_reportable_sql`."""
    return f"COALESCE({category}.system_key, '') NOT IN ({_SQL_LIST})"


def is_non_reportable(key: str | None) -> bool:
    return key in NON_REPORTABLE_KEYS


def is_movement(key: str | None) -> bool:
    return key == INTERNAL_MOVEMENT


def is_name_matchable(key: str | None) -> bool:
    """Whether a parser hint may reach the category by its name: an ordinary category, or the credit fee,
    which was matched by name before it had a key (add-credit-spending-flow D8)."""
    return key is None or key == CREDIT_FEE


def hint_key(hint: str | None) -> str | None:
    """The system key a parser hint stands for, or None for a hint that names an ordinary category."""
    return HINT_KEYS.get(" ".join((hint or "").split()))


def ensure_system_category(cur, user_id: str, key: str, kind: str) -> str:
    """The owner's category with `key` and `kind`, created with its seed name when none exists (D5).

    The seed name is the one of the owner's account language at creation (add-language-system D15).

    A user category of that kind that already holds the seed name (created by the owner after the
    migration, when the keyed one had been renamed or deleted) is adopted by setting its key, as the
    adjustment helper always updated a same-name row. The movement and adjustment categories are then
    excluded from budget with no pillar; the adjustment category is also restored from the archive, the
    movement category is not. The credit fee keeps the classification the owner gave it.
    """
    spec = _ENSURE_SPECS.get(_require_key(key))
    if spec is None:
        # The investment category is seeded once and only ever found; nothing creates it on demand.
        raise ValueError(f"system category {key!r} is seeded, not ensured")
    if kind not in ("expense", "income"):
        raise ValueError(f"unknown category kind: {kind!r}")
    unarchive = ", is_archived = FALSE" if spec["unarchive"] else ""
    cur.execute(
        """SELECT id, is_excluded_from_budget, kakeibo_type, is_archived FROM categories
           WHERE user_id = %s AND system_key = %s AND kind = %s""",
        (user_id, key, kind),
    )
    row = cur.fetchone()
    if row:
        needs_repair = spec["repair"] and (
            not row["is_excluded_from_budget"] or row["kakeibo_type"] is not None
            or (spec["unarchive"] and row["is_archived"])
        )
        if needs_repair:
            cur.execute(
                f"""UPDATE categories SET is_excluded_from_budget = TRUE, kakeibo_type = NULL{unarchive}
                    WHERE user_id = %s AND id = %s""",
                (user_id, row["id"]),
            )
        return str(row["id"])
    cur.execute("SELECT language FROM users WHERE id = %s", (user_id,))
    owner = cur.fetchone()
    name = seed_names(owner["language"] if owner else None)[key]
    # The pillar and the budget flag come from the specs above, never from input, so they are SQL literals.
    pillar = "NULL" if spec["pillar"] is None else f"'{spec['pillar']}'"
    excluded = "TRUE" if spec["excluded"] else "FALSE"
    adopt = f"is_excluded_from_budget = TRUE, kakeibo_type = NULL{unarchive}, " if spec["repair"] else ""
    cur.execute(
        f"""INSERT INTO categories (user_id, name, icon, color, kind, kakeibo_type, is_excluded_from_budget, system_key)
            VALUES (%s, %s, %s, %s, %s, {pillar}, {excluded}, %s)
            ON CONFLICT (user_id, name, kind) DO UPDATE
            SET {adopt}system_key = EXCLUDED.system_key
            RETURNING id""",
        (user_id, name, spec["icon"], spec["color"], kind, key),
    )
    return str(cur.fetchone()["id"])
