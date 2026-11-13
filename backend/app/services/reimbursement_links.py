"""Reimbursements linked to the exact expenses they repay (link-reimbursements-to-expenses).

A link stores the part of the reimbursement allocated to one expense. Allocation runs per reimbursement in
date order: its expenses by date then id, each taking at most what it cost less what earlier reimbursements
already hold on it, so an expense is never lowered below zero. Any change to either side recomputes every
reimbursement that shares an expense with it.
"""

from __future__ import annotations

from typing import Any

from app.errors import ErrorCode, api_error
from app.services.system_categories import reportable_key_sql

LINK_LIMIT = 20


def _connected_reimbursements(cur, user_id: str, reimbursement_ids: set[str], expense_ids: set[str]) -> set[str]:
    """Every reimbursement reachable through shared expenses from the given ones."""
    reimbursements, expenses = set(reimbursement_ids), set(expense_ids)
    while True:
        cur.execute(
            """SELECT reimbursement_id, expense_id FROM reimbursement_links
               WHERE user_id = %s AND (reimbursement_id = ANY(%s::uuid[]) OR expense_id = ANY(%s::uuid[]))""",
            (user_id, sorted(reimbursements), sorted(expenses)),
        )
        rows = cur.fetchall()
        found_r = {str(row["reimbursement_id"]) for row in rows}
        found_e = {str(row["expense_id"]) for row in rows}
        if found_r <= reimbursements and found_e <= expenses:
            return reimbursements
        reimbursements |= found_r
        expenses |= found_e


def reallocate(cur, user_id: str, *, reimbursement_ids: set[str] = frozenset(), expense_ids: set[str] = frozenset()) -> None:
    """Recompute the stored allocations of every reimbursement connected to these rows (D2, D7)."""
    ids = _connected_reimbursements(cur, user_id, set(reimbursement_ids), set(expense_ids))
    if not ids:
        return
    cur.execute(
        """SELECT l.reimbursement_id, l.expense_id, r.amount AS r_amount, r.date AS r_date,
                  e.amount AS e_amount, e.date AS e_date
           FROM reimbursement_links l
           JOIN transactions r ON r.id = l.reimbursement_id
           JOIN transactions e ON e.id = l.expense_id
           WHERE l.user_id = %s AND l.reimbursement_id = ANY(%s::uuid[])
           ORDER BY r.date, l.reimbursement_id, e.date, l.expense_id
           FOR UPDATE OF l""",
        (user_id, sorted(ids)),
    )
    rows = cur.fetchall()
    held: dict[str, int] = {}
    left: dict[str, int] = {}
    for row in rows:
        reimbursement, expense = str(row["reimbursement_id"]), str(row["expense_id"])
        left.setdefault(reimbursement, int(row["r_amount"]))
        room = max(0, int(row["e_amount"]) - held.get(expense, 0))
        amount = min(left[reimbursement], room)
        left[reimbursement] -= amount
        held[expense] = held.get(expense, 0) + amount
        cur.execute(
            "UPDATE reimbursement_links SET amount = %s WHERE reimbursement_id = %s AND expense_id = %s",
            (amount, reimbursement, expense),
        )


def drop_invalid_links(cur, user_id: str, transaction_ids: list[str]) -> None:
    """Remove links whose expense is no longer an expense or whose reimbursement no longer offsets (D7),
    then recompute what is connected to these rows."""
    if not transaction_ids:
        return
    cur.execute(
        """DELETE FROM reimbursement_links l
           USING transactions e, transactions r
           LEFT JOIN categories rc ON rc.id = r.category_id
           WHERE l.user_id = %s AND e.id = l.expense_id AND r.id = l.reimbursement_id
             AND (l.expense_id = ANY(%s::uuid[]) OR l.reimbursement_id = ANY(%s::uuid[]))
             AND (e.type <> 'expense' OR e.movement_id IS NOT NULL
                  OR r.type <> 'income' OR NOT COALESCE(rc.offsets_spending, FALSE))""",
        (user_id, transaction_ids, transaction_ids),
    )
    reallocate(cur, user_id, reimbursement_ids=set(transaction_ids), expense_ids=set(transaction_ids))


def links_touching(cur, user_id: str, transaction_id: str) -> tuple[set[str], set[str]]:
    """The reimbursements and expenses linked to a row, read before it is deleted."""
    cur.execute(
        """SELECT reimbursement_id, expense_id FROM reimbursement_links
           WHERE user_id = %s AND (reimbursement_id = %s OR expense_id = %s)""",
        (user_id, transaction_id, transaction_id),
    )
    rows = cur.fetchall()
    return {str(row["reimbursement_id"]) for row in rows}, {str(row["expense_id"]) for row in rows}


def _lock_reimbursement(cur, user_id: str, reimbursement_id: str) -> dict[str, Any]:
    cur.execute(
        """SELECT t.id, t.amount, t.account_id, t.date, t.notes, t.merchant, t.type, c.offsets_spending
           FROM transactions t LEFT JOIN categories c ON c.id = t.category_id
           WHERE t.user_id = %s AND t.id = %s FOR UPDATE OF t""",
        (user_id, reimbursement_id),
    )
    row = cur.fetchone()
    if not row:
        raise api_error(404, ErrorCode.TRANSACTION_NOT_FOUND)
    if row["type"] != "income" or not row["offsets_spending"]:
        raise api_error(422, ErrorCode.OFFSET_NOT_ALLOWED)
    return row


def _validate_expenses(cur, user_id: str, expense_ids: list[str]) -> None:
    if not 1 <= len(expense_ids) <= LINK_LIMIT:
        raise api_error(422, ErrorCode.REIMBURSEMENT_LINK_COUNT, {"limit": LINK_LIMIT})
    cur.execute(
        f"""SELECT t.id FROM transactions t JOIN categories c ON c.id = t.category_id
            WHERE t.user_id = %s AND t.id = ANY(%s::uuid[]) AND t.type = 'expense' AND t.movement_id IS NULL
              AND COALESCE(c.is_excluded_from_budget, FALSE) = FALSE AND {reportable_key_sql("c")}
            ORDER BY t.id FOR UPDATE OF t""",
        (user_id, expense_ids),
    )
    if len(cur.fetchall()) != len(expense_ids):
        raise api_error(422, ErrorCode.REIMBURSEMENT_LINK_INVALID)


def _validate_rest_category(cur, user_id: str, category_id: str) -> None:
    cur.execute(
        f"""SELECT 1 FROM categories WHERE user_id = %s AND id = %s AND kind = 'income' AND is_archived = FALSE
              AND NOT COALESCE(offsets_spending, FALSE) AND {reportable_key_sql("categories")}""",
        (user_id, category_id),
    )
    if not cur.fetchone():
        raise api_error(422, ErrorCode.REIMBURSEMENT_REST_CATEGORY)


def set_links(cur, user_id: str, reimbursement_id: str, expense_ids: list[str], rest_category_id: str | None,
              provenance: tuple) -> dict[str, Any]:
    """Link a reimbursement to its expenses (replacing earlier links), allocate, and record the rest as income
    in `rest_category_id` when one is given (D4, D5). Returns the new income's id or None."""
    expense_ids = list(dict.fromkeys(str(expense_id) for expense_id in expense_ids))
    reimbursement = _lock_reimbursement(cur, user_id, reimbursement_id)
    if reimbursement_id in expense_ids:
        raise api_error(422, ErrorCode.REIMBURSEMENT_LINK_INVALID)
    _validate_expenses(cur, user_id, expense_ids)
    if rest_category_id:
        _validate_rest_category(cur, user_id, rest_category_id)
    _, previous_expenses = links_touching(cur, user_id, reimbursement_id)
    cur.execute("DELETE FROM reimbursement_links WHERE user_id = %s AND reimbursement_id = %s", (user_id, reimbursement_id))
    for expense_id in expense_ids:
        cur.execute(
            "INSERT INTO reimbursement_links (reimbursement_id, expense_id, user_id) VALUES (%s, %s, %s)",
            (reimbursement_id, expense_id, user_id),
        )
    # A linked reimbursement does not use its category link any more (D5).
    cur.execute(
        "UPDATE transactions SET offset_category_id = NULL, updated_at = NOW() WHERE user_id = %s AND id = %s",
        (user_id, reimbursement_id),
    )
    reallocate(cur, user_id, reimbursement_ids={reimbursement_id}, expense_ids=previous_expenses | set(expense_ids))
    rest_id = None
    if rest_category_id:
        held = _held(cur, user_id, reimbursement_id)
        rest = int(reimbursement["amount"]) - held
        if rest > 0:
            cur.execute(
                "UPDATE transactions SET amount = %s, updated_at = NOW() WHERE user_id = %s AND id = %s",
                (held, user_id, reimbursement_id),
            )
            cur.execute(
                """INSERT INTO transactions (user_id, account_id, category_id, type, amount, notes, merchant, date,
                       source, review_status, classified_by, classification_confidence)
                   VALUES (%s, %s, %s, 'income', %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
                (user_id, reimbursement["account_id"], rest_category_id, rest, reimbursement["notes"],
                 reimbursement["merchant"], reimbursement["date"], *provenance),
            )
            rest_id = str(cur.fetchone()["id"])
    return {"rest_transaction_id": rest_id}


def remove_links(cur, user_id: str, reimbursement_id: str) -> None:
    _lock_reimbursement(cur, user_id, reimbursement_id)
    _, expenses = links_touching(cur, user_id, reimbursement_id)
    cur.execute("DELETE FROM reimbursement_links WHERE user_id = %s AND reimbursement_id = %s", (user_id, reimbursement_id))
    reallocate(cur, user_id, expense_ids=expenses)


def _held(cur, user_id: str, reimbursement_id: str) -> int:
    cur.execute(
        "SELECT COALESCE(SUM(amount), 0) AS held FROM reimbursement_links WHERE user_id = %s AND reimbursement_id = %s",
        (user_id, reimbursement_id),
    )
    return int(cur.fetchone()["held"])


def link_details(cur, user_id: str, transaction_id: str, amount: int) -> dict[str, Any]:
    """Both sides of a row's links for its detail (D8): what a reimbursement repaid and what is left, and
    what repaid an expense."""
    cur.execute(
        """SELECT l.expense_id, l.amount, e.date, e.amount AS expense_amount, e.notes, e.merchant
           FROM reimbursement_links l JOIN transactions e ON e.id = l.expense_id
           WHERE l.user_id = %s AND l.reimbursement_id = %s ORDER BY e.date, l.expense_id""",
        (user_id, transaction_id),
    )
    repays = [
        {"transaction_id": str(row["expense_id"]), "amount": int(row["amount"]), "date": row["date"].isoformat(),
         "expense_amount": int(row["expense_amount"]), "notes": row["notes"], "merchant": row["merchant"]}
        for row in cur.fetchall()
    ]
    cur.execute(
        """SELECT l.reimbursement_id, l.amount, r.date, r.notes, r.merchant
           FROM reimbursement_links l JOIN transactions r ON r.id = l.reimbursement_id
           WHERE l.user_id = %s AND l.expense_id = %s ORDER BY r.date, l.reimbursement_id""",
        (user_id, transaction_id),
    )
    repaid_by = [
        {"transaction_id": str(row["reimbursement_id"]), "amount": int(row["amount"]), "date": row["date"].isoformat(),
         "notes": row["notes"], "merchant": row["merchant"]}
        for row in cur.fetchall()
    ]
    return {
        "repays": repays,
        "unallocated": amount - sum(line["amount"] for line in repays) if repays else None,
        "repaid_by": repaid_by,
        "repaid_total": sum(line["amount"] for line in repaid_by),
    }
