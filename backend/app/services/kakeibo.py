"""Kakeibo pillar vocabulary and the single rule that resolves an expense's effective pillar."""

from typing import Literal

Pillar = Literal["need", "want", "culture", "unexpected", "saving"]
PILLARS: tuple[str, ...] = ("need", "want", "culture", "unexpected", "saving")
PILLAR_PATTERN = "^(" + "|".join(PILLARS) + ")$"


def effective_pillar_sql(tx: str = "t", category: str = "c") -> str:
    """SQL for an expense's pillar: its own, else its category's, else need; NULL for income.

    A NULL transaction pillar means "follow the category", so a later change to the
    category's pillar applies to every row that never chose one. Pillars describe spending,
    so income never resolves to one.
    """
    return f"CASE WHEN {tx}.type = 'expense' THEN COALESCE({tx}.kakeibo_type, {category}.kakeibo_type, 'need') END"
