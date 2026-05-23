"""
Unified resource router for monthly periods (Phase 1).
Mounted at /periods (cookie) and /v1/periods (Bearer).
"""
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from app.db.pool import db_conn
from app.services.ledger.balances import parse_uuid_value
from app.services.ledger.period import parse_month

router = APIRouter(tags=["periods"])


@router.get("")
def list_periods(req: Request, status: str | None = None):
    username = req.state.username
    valid_statuses = ("open", "closed", "reviewed")
    filters = []
    params: list[Any] = [username]
    if status and status in valid_statuses:
        filters.append("p.status=%s")
        params.append(status)
    where = ("AND " + " AND ".join(filters)) if filters else ""
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT p.period_id::text AS period_id, p.month, p.from_date, p.to_date,
                   p.payday_day, p.status, p.notes, p.closed_at, p.created_at
            FROM monthly_periods p
            JOIN users u ON u.user_id=p.user_id
            WHERE u.username=%s {where}
            ORDER BY p.month DESC
            """,
            params,
        )
        return {"periods": cur.fetchall()}


@router.get("/{period_id}")
def get_period(period_id: str, req: Request):
    username = req.state.username
    period_id = parse_uuid_value(period_id, "period_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.period_id::text AS period_id, p.month, p.from_date, p.to_date,
                   p.payday_day, p.status, p.notes, p.closed_at, p.created_at
            FROM monthly_periods p
            JOIN users u ON u.user_id=p.user_id
            WHERE p.period_id=%s::uuid AND u.username=%s
            """,
            (period_id, username),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Period not found")
    return row


@router.post("/{period_id}/close")
async def close_period(period_id: str, req: Request):
    username = req.state.username
    period_id = parse_uuid_value(period_id, "period_id")
    data: dict[str, Any] = {}
    try:
        data = await req.json()
    except Exception:
        pass
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE monthly_periods p SET status='closed', notes=%s, closed_at=now(), updated_at=now()
            FROM users u
            WHERE p.period_id=%s::uuid AND p.user_id=u.user_id AND u.username=%s
              AND p.status='open'
            RETURNING p.period_id
            """,
            (notes, period_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Period not found or already closed")
        conn.commit()
    return {"ok": True}


@router.post("/{period_id}/reopen")
def reopen_period(period_id: str, req: Request):
    username = req.state.username
    period_id = parse_uuid_value(period_id, "period_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE monthly_periods p SET status='open', closed_at=NULL, updated_at=now()
            FROM users u
            WHERE p.period_id=%s::uuid AND p.user_id=u.user_id AND u.username=%s
              AND p.status IN ('closed', 'reviewed')
            RETURNING p.period_id
            """,
            (period_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Period not found or already open")
        conn.commit()
    return {"ok": True}
