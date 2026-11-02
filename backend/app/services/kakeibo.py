"""Kakeibo pillar vocabulary and the single rule that resolves an expense's effective pillar."""

from typing import Literal

Pillar = Literal["need", "want", "culture", "unexpected", "saving"]
PILLARS: tuple[str, ...] = ("need", "want", "culture", "unexpected", "saving")
PILLAR_PATTERN = "^(" + "|".join(PILLARS) + ")$"

# Aggregate key for spending nobody classified ("Belum diklasifikasi"). Rows never carry it: their
# effective pillar is NULL, because clients such as the phone accept only the five pillars or null.
UNCLASSIFIED = "unclassified"
REPORT_BUCKETS: tuple[str, ...] = PILLARS + (UNCLASSIFIED,)
PILLAR_FILTER_PATTERN = "^(" + "|".join(REPORT_BUCKETS) + ")$"


def effective_pillar_sql(tx: str = "t", category: str = "c") -> str:
    """SQL for an expense's pillar: its own, else its category's; NULL when neither has one, and for income.

    A NULL transaction pillar means "follow the category", so a later change to the category's pillar
    applies to every row that never chose one. A missing pillar is never read as `need`.
    """
    return f"CASE WHEN {tx}.type = 'expense' THEN COALESCE({tx}.kakeibo_type, {category}.kakeibo_type) END"


def pillar_bucket_sql(tx: str = "t", category: str = "c") -> str:
    """SQL grouping key of an expense: its effective pillar, or 'unclassified'; NULL for income."""
    return (
        f"CASE WHEN {tx}.type = 'expense' THEN "
        f"COALESCE({tx}.kakeibo_type, {category}.kakeibo_type, '{UNCLASSIFIED}') END"
    )


def effective_pillar(tx_type: str, tx_pillar: str | None, category_pillar: str | None) -> str | None:
    """Python form of `effective_pillar_sql` for figures computed outside SQL."""
    if tx_type != "expense":
        return None
    return tx_pillar or category_pillar or None
