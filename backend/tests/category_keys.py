"""Test helper: the system key a synthetic fixture category carries when it stands for a seeded one.

Fixtures that insert categories by SQL model an owner after V52, where the seeded "Investasi",
"Internal Movement" and "Penyesuaian Saldo" categories carry their keys and every other category none.
"""

from app.services.system_categories import SEED_NAMES

_KEY_OF_SEED_NAME = {name: key for key, name in SEED_NAMES.items()}


def seeded_key(name: str) -> str | None:
    return _KEY_OF_SEED_NAME.get(name)
