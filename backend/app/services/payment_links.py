"""Link recorded transactions to debts and expectation occurrences without creating ledger rows.

A plan or debt never produces a payment by itself: the owner points at a transaction that was
already captured. Candidate lists are suggestions only; nothing here links a transaction unless
the owner names it.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from app.core.config import settings
from app.services.debt_allocations import apply_allocations, lock_debts, validate_debt_capacity

CANDIDATE_WINDOW_DAYS = 7
CANDIDATE_LIMIT = 10
OBLIGATION_CANDIDATE_DAYS = 45


def _conflict(code: str, message: str, **extra: Any) -> HTTPException:
    return HTTPException(status_code=409, detail={"code": code, "message": message, **extra})


def _local_start(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=ZoneInfo(settings.tz)).astimezone(timezone.utc)


def lock_payment(cur, user_id: str, transaction_id: str) -> dict[str, Any]:
    cur.execute(
        """
        SELECT t.id, t.type, t.amount, t.date, t.account_id, t.movement_id, t.fee_for_movement_id,
               t.obligation_id, a.type AS account_type
        FROM transactions t
        JOIN accounts a ON a.id = t.account_id
        WHERE t.user_id = %s AND t.id = %s
        FOR UPDATE OF t
        """,
        (user_id, transaction_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail={"code": "transaction_not_found", "message": "Transaksi tidak ditemukan."})
    return row


def link_payment_to_obligation(cur, user_id: str, transaction: dict[str, Any], obligation_id: str) -> bool:
    """Apply a recorded expense to a debt once. Returns False when it is already applied there."""
    if (
        transaction["type"] != "expense"
        or transaction["movement_id"] is not None
        or transaction["fee_for_movement_id"] is not None
        or transaction["account_type"] == "investment"
    ):
        raise HTTPException(
            status_code=422,
            detail={"code": "invalid_debt_payment", "message": "Hanya pengeluaran biasa yang dapat dikaitkan ke tagihan."},
        )
    transaction_id = str(transaction["id"])
    cur.execute(
        "SELECT obligation_id, amount FROM transaction_obligation_allocations WHERE transaction_id = %s",
        (transaction_id,),
    )
    allocations = {str(row["obligation_id"]): int(row["amount"]) for row in cur.fetchall()}
    legacy_link = str(transaction["obligation_id"]) if transaction["obligation_id"] else None
    if legacy_link == obligation_id or allocations.get(obligation_id) == int(transaction["amount"]):
        return False
    if legacy_link or allocations:
        raise _conflict("transaction_already_linked", "Transaksi ini sudah dikaitkan ke tagihan lain.")
    amount = int(transaction["amount"])
    debts = lock_debts(cur, user_id, [obligation_id])
    validate_debt_capacity(debts, [(obligation_id, amount)])
    cur.execute(
        "INSERT INTO transaction_obligation_allocations (transaction_id, obligation_id, amount) VALUES (%s, %s, %s)",
        (transaction_id, obligation_id, amount),
    )
    apply_allocations(cur, user_id, [(obligation_id, amount)])
    return True


def obligation_payment_candidates(cur, user_id: str, obligation: dict[str, Any]) -> dict[str, Any]:
    """Recent unlinked expenses; an exact minimum-payment amount is flagged but never chosen."""
    since = datetime.now(timezone.utc) - timedelta(days=OBLIGATION_CANDIDATE_DAYS)
    expected = int(obligation["minimum_payment"] or 0)
    cur.execute(
        """
        SELECT t.id, t.amount, t.date, t.notes, t.merchant, t.source, a.name AS account_name
        FROM transactions t
        JOIN accounts a ON a.id = t.account_id
        WHERE t.user_id = %s AND t.type = 'expense' AND t.movement_id IS NULL
          AND t.fee_for_movement_id IS NULL AND a.type <> 'investment'
          AND t.obligation_id IS NULL
          AND NOT EXISTS (SELECT 1 FROM transaction_obligation_allocations x WHERE x.transaction_id = t.id)
          AND t.date >= %s AND t.date <= NOW()
        ORDER BY (t.amount = %s) DESC, t.date DESC, t.id
        LIMIT %s
        """,
        (user_id, since, expected, CANDIDATE_LIMIT),
    )
    rows = [_candidate(row, expected) for row in cur.fetchall()]
    return _with_match_state(rows, window_days=OBLIGATION_CANDIDATE_DAYS)


def occurrence_candidates(cur, user_id: str, rule: dict[str, Any], scheduled_for: date) -> dict[str, Any]:
    """Recorded transactions that might be this occurrence, exact amounts first."""
    expected = int(rule["amount"])
    start = _local_start(scheduled_for - timedelta(days=CANDIDATE_WINDOW_DAYS))
    conditions = [
        "t.user_id = %s", "t.date >= %s", "t.date <= NOW()",
        "NOT EXISTS (SELECT 1 FROM recurring_executions e WHERE e.transaction_id = t.id AND e.status IN ('linked', 'succeeded'))",
    ]
    params: list[Any] = [user_id, start]
    if rule["type"] in {"transfer", "investment_topup"}:
        conditions += ["t.type = 'expense'", "t.account_id = %s", "t.fee_for_movement_id IS NULL"]
        params.append(str(rule["source_account_id"]))
    else:
        conditions += ["t.type = %s", "t.movement_id IS NULL", "t.fee_for_movement_id IS NULL"]
        params.append(rule["type"])
    if rule["type"] == "expense" and rule.get("obligation_id"):
        conditions.append(
            "((t.obligation_id IS NULL AND NOT EXISTS (SELECT 1 FROM transaction_obligation_allocations x WHERE x.transaction_id = t.id))"
            " OR t.obligation_id = %s"
            " OR EXISTS (SELECT 1 FROM transaction_obligation_allocations x WHERE x.transaction_id = t.id AND x.obligation_id = %s))"
        )
        params += [str(rule["obligation_id"]), str(rule["obligation_id"])]
    cur.execute(
        f"""
        SELECT t.id, t.amount, t.date, t.notes, t.merchant, t.source, a.name AS account_name
        FROM transactions t
        JOIN accounts a ON a.id = t.account_id
        WHERE {' AND '.join(conditions)}
        ORDER BY (t.amount = %s) DESC, abs(extract(epoch FROM t.date - %s)), t.id
        LIMIT %s
        """,
        (*params, expected, _local_start(scheduled_for), CANDIDATE_LIMIT),
    )
    rows = [_candidate(row, expected) for row in cur.fetchall()]
    return _with_match_state(rows, window_days=CANDIDATE_WINDOW_DAYS)


def _candidate(row: dict[str, Any], expected: int) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "amount": int(row["amount"]),
        "date": row["date"].isoformat(),
        "notes": row["notes"],
        "merchant": row["merchant"],
        "source": row["source"],
        "account_name": row["account_name"],
        "exact_amount": int(row["amount"]) == expected,
    }


def _with_match_state(rows: list[dict[str, Any]], *, window_days: int) -> dict[str, Any]:
    exact = sum(1 for row in rows if row["exact_amount"])
    if exact > 1:
        state = "ambiguous"
    elif exact == 1:
        state = "single"
    else:
        state = "none"
    return {"match_state": state, "window_days": window_days, "candidates": rows}
