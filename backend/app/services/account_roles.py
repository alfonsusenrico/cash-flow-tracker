"""Account roles ("Kegunaan") and investment kinds (organize-accounts-by-use D1, D6).

A role says what an account's money is for. It follows the type where the type already decides: a credit
account is always utang and an investment product always investasi. A cash, bank or wallet account is
harian, tabungan or dana_investasi, as the owner chooses. Only leaves count in figures; a parent's role
matters for its own balance only.
"""
from __future__ import annotations

from typing import Any

from app.services.credit_accounts import CREDIT_TYPE, refuse

HARIAN = "harian"
TABUNGAN = "tabungan"
DANA_INVESTASI = "dana_investasi"
INVESTASI = "investasi"
UTANG = "utang"

# Also the default section order of the "use" layout.
ROLES: tuple[str, ...] = (HARIAN, TABUNGAN, DANA_INVESTASI, INVESTASI, UTANG)
ROLE_PATTERN = "^(" + "|".join(ROLES) + ")$"
CASH_ROLES = frozenset({HARIAN, TABUNGAN, DANA_INVESTASI})

KIND_BY_INSTRUMENT = {"gold": "emas", "mutual_fund": "reksa_dana", "stock": "saham"}
OTHER_KIND = "lainnya"


def is_investment_product(account_type: str | None, instrument_type: str | None) -> bool:
    return account_type == "investment" or bool(instrument_type)


def default_role(account_type: str | None, instrument_type: str | None) -> str:
    """The role V51 backfills from the type, without a goal link: a new account starts here."""
    if account_type == CREDIT_TYPE:
        return UTANG
    if is_investment_product(account_type, instrument_type):
        return INVESTASI
    return HARIAN


def allowed_roles(account_type: str | None, instrument_type: str | None) -> frozenset[str]:
    if account_type == CREDIT_TYPE:
        return frozenset({UTANG})
    if is_investment_product(account_type, instrument_type):
        return frozenset({INVESTASI})
    return CASH_ROLES


def validate_role(account_type: str | None, instrument_type: str | None, role: str) -> None:
    if role not in allowed_roles(account_type, instrument_type):
        raise refuse("account_role_type_mismatch")


def resolve_role_on_update(
    *, current_role: str | None, requested_role: str | None,
    final_type: str | None, final_instrument_type: str | None,
) -> str:
    """The role an edit leaves: the requested one if it fits the final type (else 422), otherwise the
    current one while it still fits, otherwise the type's default. A type change therefore never keeps a
    role that contradicts it, and never silently drops a cash role that still fits (bank to wallet)."""
    if requested_role is not None:
        validate_role(final_type, final_instrument_type, requested_role)
        return requested_role
    if current_role in allowed_roles(final_type, final_instrument_type):
        return current_role
    return default_role(final_type, final_instrument_type)


def account_kind(account: dict[str, Any]) -> str | None:
    """The investment kind shown on Rekening: emas, reksa_dana, saham or lainnya; None for cash and credit."""
    if not is_investment_product(account.get("type"), account.get("instrument_type")):
        return None
    return KIND_BY_INSTRUMENT.get(account.get("instrument_type") or "", OTHER_KIND)
