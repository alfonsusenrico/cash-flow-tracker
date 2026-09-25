from collections.abc import Iterable
from uuid import UUID

from fastapi import HTTPException
from pydantic import BaseModel, Field


class DebtAllocation(BaseModel):
    obligation_id: UUID
    amount: int = Field(gt=0)


def validate_allocation_request(
    allocations: list[DebtAllocation] | None,
    *,
    amount: int,
    obligation_id: UUID | str | None,
    transaction_type: str,
    investment_action: str | None = None,
) -> list[tuple[str, int]] | None:
    if allocations is None:
        return None
    if obligation_id is not None:
        raise HTTPException(
            status_code=422,
            detail={"code": "mixed_debt_links", "message": "Pilih satu cara mengaitkan pembayaran ke tagihan."},
        )
    if transaction_type != "expense" or investment_action:
        raise HTTPException(
            status_code=422,
            detail={"code": "invalid_debt_allocation_type", "message": "Pembagian tagihan hanya untuk pengeluaran biasa."},
        )

    normalized = [(str(item.obligation_id), item.amount) for item in allocations]
    if len({obligation_id for obligation_id, _ in normalized}) != len(normalized):
        raise HTTPException(
            status_code=422,
            detail={"code": "duplicate_debt_allocation", "message": "Satu tagihan hanya boleh dipilih sekali."},
        )
    if normalized and sum(value for _, value in normalized) != amount:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "debt_allocation_total_mismatch",
                "message": "Jumlah pembagian tagihan harus sama dengan nominal transaksi.",
                "required_amount": amount,
                "allocated_amount": sum(value for _, value in normalized),
            },
        )
    return normalized


def load_allocation_rows(cur, transaction_id: str) -> list[dict]:
    cur.execute(
        """
        SELECT a.obligation_id, a.amount, o.name AS obligation_name
        FROM transaction_obligation_allocations a
        JOIN obligations o ON o.id = a.obligation_id
        WHERE a.transaction_id = %s
        ORDER BY o.name, a.obligation_id
        """,
        (transaction_id,),
    )
    return cur.fetchall()


def load_allocation_breakdowns(cur, rows: list[dict]) -> dict[str, list[dict]]:
    if not rows:
        return {}
    transaction_ids = [str(row["id"]) for row in rows]
    cur.execute(
        """
        SELECT a.transaction_id, a.obligation_id, a.amount, o.name AS obligation_name
        FROM transaction_obligation_allocations a
        JOIN obligations o ON o.id = a.obligation_id
        WHERE a.transaction_id = ANY(%s)
        ORDER BY a.transaction_id, o.name, a.obligation_id
        """,
        (transaction_ids,),
    )
    result: dict[str, list[dict]] = {transaction_id: [] for transaction_id in transaction_ids}
    for allocation in cur.fetchall():
        result[str(allocation["transaction_id"])].append(
            {
                "obligation_id": str(allocation["obligation_id"]),
                "obligation_name": allocation["obligation_name"],
                "amount": int(allocation["amount"]),
            }
        )
    for row in rows:
        transaction_id = str(row["id"])
        if not result[transaction_id] and row.get("obligation_id"):
            result[transaction_id] = [
                {
                    "obligation_id": str(row["obligation_id"]),
                    "obligation_name": row.get("obligation_name"),
                    "amount": int(row["amount"]),
                }
            ]
    return result


def lock_debts(cur, user_id: str, obligation_ids: Iterable[str]) -> dict[str, dict]:
    ids = sorted(set(obligation_ids))
    if not ids:
        return {}
    cur.execute(
        """
        SELECT id, remaining_amount, is_archived
        FROM obligations
        WHERE user_id = %s AND id = ANY(%s)
        ORDER BY id
        FOR UPDATE
        """,
        (user_id, ids),
    )
    debts = {str(row["id"]): row for row in cur.fetchall()}
    if len(debts) != len(ids):
        raise HTTPException(
            status_code=404,
            detail={"code": "debt_unavailable", "message": "Satu atau lebih tagihan tidak tersedia."},
        )
    return debts


def validate_debt_capacity(
    debts: dict[str, dict],
    allocations: list[tuple[str, int]],
    *,
    previously_linked_ids: set[str] | None = None,
) -> None:
    previously_linked_ids = previously_linked_ids or set()
    for obligation_id, amount in allocations:
        debt = debts[obligation_id]
        available = int(debt["remaining_amount"])
        if debt["is_archived"] and obligation_id not in previously_linked_ids:
            raise HTTPException(
                status_code=409,
                detail={"code": "debt_unavailable", "message": "Tagihan yang diarsipkan tidak dapat menerima pembayaran baru.", "obligation_id": obligation_id},
            )
        if available < amount:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "debt_allocation_exceeds_remaining",
                    "message": "Pembagian melebihi sisa tagihan.",
                    "obligation_id": obligation_id,
                    "required_amount": amount,
                    "available_amount": available,
                },
            )


def apply_allocations(cur, user_id: str, allocations: list[tuple[str, int]]) -> None:
    for obligation_id, amount in allocations:
        cur.execute(
            """
            UPDATE obligations
            SET remaining_amount = remaining_amount - %s,
                is_archived = remaining_amount - %s = 0,
                updated_at = NOW()
            WHERE user_id = %s AND id = %s
            """,
            (amount, amount, user_id, obligation_id),
        )


def reverse_allocations(cur, user_id: str, allocations: list[tuple[str, int]]) -> None:
    for obligation_id, amount in allocations:
        cur.execute(
            """
            UPDATE obligations
            SET remaining_amount = remaining_amount + %s,
                is_archived = FALSE,
                updated_at = NOW()
            WHERE user_id = %s AND id = %s
            """,
            (amount, user_id, obligation_id),
        )
