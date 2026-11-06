"""Credit purchase terms and due dates (add-credit-spending-flow D1, D2, D7).

A purchase on a credit account is paid in full by default, or split into monthly instalments. Its first due
date follows the account's bill day (`statement_day`, 1 when empty) and payment day, unless a first due date
is stored: one the owner set, or one the system kept when the account's days changed. A stored first due
date anchors its whole series, so a later day change never re-dates an instalment of that purchase.

Dates here are local calendar dates (Asia/Jakarta); the caller converts timestamps.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date
from typing import Any

from app.core.config import settings
from app.core.errors import coded_error
from app.services.credit_accounts import CREDIT_TYPE
from app.services.system_categories import CREDIT_FEE

DEFAULT_STATEMENT_DAY = 1
MIN_INSTALLMENTS = 1
MAX_INSTALLMENTS = 36
OWNER = "owner"
SYSTEM = "system"

# English and Indonesian messages of the credit refusals, keyed by error code; `message` is rendered in the
# account's language and the web renders the code itself (add-language-system D8).
MESSAGES = {
    "en": {
        "credit_terms_not_credit_purchase": "Installments can only be set on a purchase made with a credit account.",
        "credit_terms_count_range": "Choose between {min} and {max} installments.",
        "credit_terms_due_before_purchase": "The first due date cannot be before the purchase date, {purchase_date}.",
        "credit_account_required": "This is not a credit account.",
        "credit_period_invalid": "The start of the period must be on or before its end.",
        "account_not_found": "Account not found.",
        "transaction_not_found": "Transaction not found.",
    },
    "id": {
        "credit_terms_not_credit_purchase": "Cicilan hanya bisa diatur untuk pembelian dengan rekening kredit.",
        "credit_terms_count_range": "Pilih {min} sampai {max} kali cicilan.",
        "credit_terms_due_before_purchase": "Jatuh tempo pertama tidak boleh sebelum tanggal pembelian, {purchase_date}.",
        "credit_account_required": "Ini bukan rekening kredit.",
        "credit_period_invalid": "Awal periode harus sama dengan atau sebelum akhirnya.",
        "account_not_found": "Rekening tidak ditemukan.",
        "transaction_not_found": "Transaksi tidak ditemukan.",
    },
}


def credit_error(code: str, language: str | None, *, status_code: int = 422, **params: Any):
    """A refusal with `{code, params, message}`, the message in the account's language (English when unknown)."""
    catalogue = MESSAGES.get(language or "", MESSAGES["en"])
    return coded_error(status_code, code, catalogue[code].format(**params), params)


@dataclass(frozen=True)
class Terms:
    installment_count: int = 1
    first_due_date: date | None = None
    first_due_date_set_by: str | None = None


def day_in_month(year: int, month: int, day: int) -> date:
    """`day` of the month, clamped to the month's last day."""
    return date(year, month, min(day, calendar.monthrange(year, month)[1]))


def shift_month(year: int, month: int, months: int) -> tuple[int, int]:
    index = year * 12 + month - 1 + months
    return index // 12, index % 12 + 1


def first_day_on_or_after(start: date, day: int) -> date:
    """The first date on or after `start` whose day is `day`, clamped in short months."""
    candidate = day_in_month(start.year, start.month, day)
    if candidate >= start:
        return candidate
    year, month = shift_month(start.year, start.month, 1)
    return day_in_month(year, month, day)


def statement_close(purchase_date: date, statement_day: int | None) -> date:
    """The first bill day after the purchase; a purchase on the bill day goes into the next bill (D2.2)."""
    day = statement_day or DEFAULT_STATEMENT_DAY
    candidate = day_in_month(purchase_date.year, purchase_date.month, day)
    if candidate > purchase_date:
        return candidate
    year, month = shift_month(purchase_date.year, purchase_date.month, 1)
    return day_in_month(year, month, day)


def derived_first_due(purchase_date: date, statement_day: int | None, payment_day: int | None) -> date | None:
    """The first payment day on or after the statement close, or None without a payment day (D2.3)."""
    if payment_day is None:
        return None
    return first_day_on_or_after(statement_close(purchase_date, statement_day), payment_day)


def opening_due(created_on: date, payment_day: int | None) -> date | None:
    """Debt owed when the account was added is due on the first payment day from that date (D2.8)."""
    if payment_day is None:
        return None
    return first_day_on_or_after(created_on, payment_day)


def statement_date(due_date: date, statement_day: int | None) -> date:
    """The latest bill day on or before a due date: when that bill was issued."""
    day = statement_day or DEFAULT_STATEMENT_DAY
    candidate = day_in_month(due_date.year, due_date.month, day)
    if candidate <= due_date:
        return candidate
    year, month = shift_month(due_date.year, due_date.month, -1)
    return day_in_month(year, month, day)


def installment_amounts(amount: int, count: int) -> list[int]:
    """Whole-rupiah instalments; the remainder goes on the first, so they always add up (D7)."""
    base = amount // count
    return [base + amount - base * count] + [base] * (count - 1)


def first_due(terms: Terms, purchase_date: date, statement_day: int | None, payment_day: int | None) -> date | None:
    if terms.first_due_date is not None:
        return terms.first_due_date
    return derived_first_due(purchase_date, statement_day, payment_day)


def installment_due_dates(terms: Terms, purchase_date: date, statement_day: int | None, payment_day: int | None) -> list[date] | None:
    """Instalment k is due k months after the first, on the anchor day clamped to the month (D2.5).

    The anchor is the stored first due date's day when there is one, otherwise the payment day.
    """
    first = first_due(terms, purchase_date, statement_day, payment_day)
    if first is None:
        return None
    anchor = first.day if terms.first_due_date is not None else payment_day
    dates = [first]
    for number in range(1, terms.installment_count):
        year, month = shift_month(first.year, first.month, number)
        dates.append(day_in_month(year, month, anchor))
    return dates


def validate_terms(language: str | None, installment_count: int, first_due_date: date | None, purchase_date: date) -> None:
    if not MIN_INSTALLMENTS <= installment_count <= MAX_INSTALLMENTS:
        raise credit_error("credit_terms_count_range", language, min=MIN_INSTALLMENTS, max=MAX_INSTALLMENTS)
    if first_due_date is not None and first_due_date < purchase_date:
        raise credit_error("credit_terms_due_before_purchase", language, purchase_date=purchase_date.isoformat())


def write_terms(cur, user_id: str, transaction_id: str, installment_count: int, first_due_date: date | None) -> None:
    """Store the owner's terms; paid in full with a derived date is the default and needs no row (D1)."""
    if installment_count == 1 and first_due_date is None:
        cur.execute("DELETE FROM credit_purchase_terms WHERE transaction_id = %s AND user_id = %s", (transaction_id, user_id))
        return
    set_by = OWNER if first_due_date is not None else None
    cur.execute(
        """
        INSERT INTO credit_purchase_terms (transaction_id, user_id, installment_count, first_due_date, first_due_date_set_by)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO UPDATE
        SET installment_count = EXCLUDED.installment_count, first_due_date = EXCLUDED.first_due_date,
            first_due_date_set_by = EXCLUDED.first_due_date_set_by, updated_at = NOW()
        """,
        (transaction_id, user_id, installment_count, first_due_date, set_by),
    )


def terms_after_edit(
    cur, user_id: str, transaction_id: str, *, still_credit_purchase: bool, old_date: date, new_date: date,
) -> bool:
    """Keep a purchase's terms valid after an edit, in the edit's transaction (D1); True when an owner due
    date was cleared because the purchase moved past it.

    A purchase moved off credit, or no longer an expense, loses its terms. A date change clears a kept
    (`system`) due date, and an owner due date that now falls before the purchase.
    """
    if not still_credit_purchase:
        cur.execute("DELETE FROM credit_purchase_terms WHERE transaction_id = %s AND user_id = %s", (transaction_id, user_id))
        return False
    if old_date == new_date:
        return False
    cur.execute(
        "SELECT installment_count, first_due_date, first_due_date_set_by FROM credit_purchase_terms WHERE transaction_id = %s AND user_id = %s",
        (transaction_id, user_id),
    )
    row = cur.fetchone()
    if row is None or row["first_due_date"] is None:
        return False
    owner_date_passed = row["first_due_date_set_by"] == OWNER and row["first_due_date"] < new_date
    if row["first_due_date_set_by"] == SYSTEM or owner_date_passed:
        cur.execute(
            """UPDATE credit_purchase_terms SET first_due_date = NULL, first_due_date_set_by = NULL, updated_at = NOW()
               WHERE transaction_id = %s AND user_id = %s""",
            (transaction_id, user_id),
        )
        cur.execute(
            "DELETE FROM credit_purchase_terms WHERE transaction_id = %s AND installment_count = 1 AND first_due_date IS NULL",
            (transaction_id,),
        )
    return owner_date_passed


def lock_purchase(cur, user_id: str, transaction_id: str) -> dict[str, Any] | None:
    """The owner's transaction with what decides whether it may have terms, locked for the terms write."""
    cur.execute(
        """
        SELECT t.id, t.type, t.account_id, t.movement_id, t.fee_for_movement_id, t.amount,
               (t.date AT TIME ZONE %s)::date AS local_date, a.type AS account_type, c.system_key
        FROM transactions t
        JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.user_id = %s AND t.id = %s
        FOR UPDATE OF t
        """,
        (settings.tz, user_id, transaction_id),
    )
    return cur.fetchone()


def is_credit_purchase(row: dict[str, Any]) -> bool:
    """An expense on a credit account that is neither a movement leg nor a fee (D1, D8)."""
    return (
        row["type"] == "expense" and row["account_type"] == CREDIT_TYPE
        and row["movement_id"] is None and row["fee_for_movement_id"] is None and row["system_key"] != CREDIT_FEE
    )


def set_purchase_terms(
    cur, user_id: str, language: str | None, row: dict[str, Any], installment_count: int, first_due_date: date | None,
) -> None:
    if not is_credit_purchase(row):
        raise credit_error("credit_terms_not_credit_purchase", language)
    validate_terms(language, installment_count, first_due_date, row["local_date"])
    write_terms(cur, user_id, str(row["id"]), installment_count, first_due_date)
