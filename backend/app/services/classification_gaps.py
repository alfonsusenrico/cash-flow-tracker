"""Lengkapi klasifikasi: list spending without a Kakeibo pillar and let the owner set one (design D7).

Both fixes go through the owner-edit path, so each row whose effective pillar changes writes one
append-only `classification_feedback` row and owner merchant memory, as every other owner edit does.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.kakeibo import UNCLASSIFIED, effective_pillar, pillar_bucket_sql
from app.services.merchant_memory import record_owner_corrections, snapshot_rows
from app.services.provenance import OWNER_EDIT_ASSIGNMENTS
from app.services.report_figures import reportable_category_sql, reportable_expense_sql

GAP_ROW_LIMIT = 200
FIX_ROW_LIMIT = 200


class ClassificationError(Exception):
    def __init__(self, status: int, code: str, message: str, **extra: Any):
        super().__init__(message)
        self.status = status
        self.detail = {"code": code, "message": message, **extra}


def lock_owner(cur, user_id: str) -> None:
    """Serialise with notification processing and bulk review, which write the same rows and memory."""
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{user_id}",))


def _window(alias: str, start: datetime | None, end: datetime | None) -> tuple[str, list[Any]]:
    if start is None or end is None:
        return "", []
    return f" AND {alias}.date >= %s AND {alias}.date <= %s", [start, end]


def classification_gaps(cur, user_id: str, start: datetime | None = None, end: datetime | None = None) -> dict[str, Any]:
    """Expense categories without a pillar and unclassified reportable expenses, optionally in a window."""
    window_sql, window_params = _window("t", start, end)
    cur.execute(
        f"""SELECT c.id, c.name, c.icon, c.color,
                   COUNT(t.id) AS row_count, COALESCE(SUM(t.amount), 0) AS amount
            FROM categories c
            LEFT JOIN transactions t ON t.category_id = c.id AND t.user_id = c.user_id
                 AND t.type = 'expense' AND t.kakeibo_type IS NULL{window_sql}
            WHERE c.user_id = %s AND c.kind = 'expense' AND c.is_archived = FALSE AND c.kakeibo_type IS NULL
              AND {reportable_category_sql("c")}
            GROUP BY c.id, c.name, c.icon, c.color
            ORDER BY amount DESC, c.name""",
        (*window_params, user_id),
    )
    categories = [
        {
            "id": str(row["id"]), "name": row["name"], "icon": row["icon"], "color": row["color"],
            "unclassified_count": int(row["row_count"]), "unclassified_amount": int(row["amount"]),
        }
        for row in cur.fetchall()
    ]
    condition = f"t.user_id = %s AND {reportable_expense_sql()} AND {pillar_bucket_sql()} = %s{window_sql}"
    params = [user_id, UNCLASSIFIED, *window_params]
    cur.execute(
        f"""SELECT COUNT(*) AS count, COALESCE(SUM(t.amount), 0) AS amount
            FROM transactions t LEFT JOIN categories c ON c.id = t.category_id WHERE {condition}""",
        params,
    )
    totals = cur.fetchone()
    cur.execute(
        f"""SELECT t.id, t.date, t.amount, t.merchant, t.notes, t.category_id, c.name AS category_name,
                   a.name AS account_name
            FROM transactions t
            JOIN accounts a ON a.id = t.account_id
            LEFT JOIN categories c ON c.id = t.category_id
            WHERE {condition}
            ORDER BY t.date DESC, t.id LIMIT %s""",
        [*params, GAP_ROW_LIMIT],
    )
    rows = [
        {
            "id": str(row["id"]), "date": row["date"].isoformat(), "amount": int(row["amount"]),
            "merchant": row["merchant"], "notes": row["notes"],
            "category_id": str(row["category_id"]) if row["category_id"] else None,
            "category_name": row["category_name"], "account_name": row["account_name"],
        }
        for row in cur.fetchall()
    ]
    return {
        "window": {"start": start.isoformat(), "end": end.isoformat()} if start and end else None,
        "categories": categories,
        "transactions": rows,
        "transaction_count": int(totals["count"]),
        "transaction_amount": int(totals["amount"]),
        "transaction_limit": GAP_ROW_LIMIT,
    }


def set_category_pillar(cur, user_id: str, category_id: str, pillar: str) -> dict[str, Any]:
    """Give a category its pillar; its expenses without their own pillar follow it.

    The rows themselves are not edited, so their review state and open purpose questions stay as they
    are; feedback is written for every row whose effective pillar changed.
    """
    lock_owner(cur, user_id)
    cur.execute(
        f"""SELECT id, kind, kakeibo_type, {reportable_category_sql("c")} AS reportable
            FROM categories c WHERE user_id = %s AND id = %s FOR UPDATE""",
        (user_id, category_id),
    )
    category = cur.fetchone()
    if not category:
        raise ClassificationError(404, "category_not_found", "Kategori tidak ditemukan.")
    if category["kind"] != "expense":
        raise ClassificationError(422, "pillar_not_allowed", "Pilar Kakeibo hanya untuk kategori pengeluaran.")
    if not category["reportable"]:
        raise ClassificationError(422, "category_not_reportable", "Kategori ini tidak dihitung sebagai pengeluaran.")
    cur.execute(
        """SELECT id FROM transactions
           WHERE user_id = %s AND category_id = %s AND type = 'expense' AND kakeibo_type IS NULL
             AND movement_id IS NULL""",
        (user_id, category_id),
    )
    following = [str(row["id"]) for row in cur.fetchall()]
    before = snapshot_rows(cur, user_id, following) if category["kakeibo_type"] != pillar else {}
    cur.execute(
        "UPDATE categories SET kakeibo_type = %s, updated_at = NOW() WHERE user_id = %s AND id = %s",
        (pillar, user_id, category_id),
    )
    corrections = record_owner_corrections(cur, user_id, before, close_questions=False)
    return {"category_id": category_id, "kakeibo_type": pillar, "updated_transactions": len(following), "corrections": corrections}


def set_transaction_pillars(cur, user_id: str, transaction_ids: list[str], pillar: str) -> dict[str, Any]:
    """Set the pillar of up to 200 of the owner's expenses, all or nothing, as an owner edit."""
    lock_owner(cur, user_id)
    cur.execute(
        """SELECT t.id, t.type, t.movement_id, t.kakeibo_type, c.kakeibo_type AS category_kakeibo_type
           FROM transactions t LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
           WHERE t.user_id = %s AND t.id = ANY(%s::uuid[])
           ORDER BY t.id FOR UPDATE OF t""",
        (user_id, transaction_ids),
    )
    rows = {str(row["id"]): row for row in cur.fetchall()}
    missing = [value for value in transaction_ids if value not in rows]
    if missing:
        raise ClassificationError(404, "transactions_not_found", "Sebagian transaksi tidak ditemukan.", transaction_ids=missing)
    legs = sorted(row_id for row_id, row in rows.items() if row["movement_id"])
    if legs:
        raise ClassificationError(409, "movement_leg_not_editable", "Pindah saldo tidak memakai pilar Kakeibo.", transaction_ids=legs)
    income = sorted(row_id for row_id, row in rows.items() if row["type"] != "expense")
    if income:
        raise ClassificationError(422, "pillar_not_allowed", "Pilar Kakeibo hanya untuk pengeluaran.", transaction_ids=income)
    changed = [
        row_id for row_id, row in rows.items()
        if effective_pillar(row["type"], row["kakeibo_type"], row["category_kakeibo_type"]) != pillar
    ]
    before = snapshot_rows(cur, user_id, changed)
    if changed:
        cur.execute(
            f"""UPDATE transactions
                SET kakeibo_type = %s, review_status = 'reviewed', updated_at = NOW(), {OWNER_EDIT_ASSIGNMENTS}
                WHERE user_id = %s AND id = ANY(%s::uuid[])""",
            (pillar, user_id, changed),
        )
    corrections = record_owner_corrections(cur, user_id, before)
    return {"transaction_count": len(transaction_ids), "corrections": corrections}
