"""Allocation Plans + Items CRUD — /allocation-plans and /v1/allocation-plans"""
from typing import Any
from datetime import timezone
from fastapi import APIRouter, HTTPException, Request
from app.db.pool import db_conn
from app.services.ledger.balances import parse_uuid_value
from app.services.ledger.period import parse_month, current_month_local

router = APIRouter(tags=["allocation"])


def _resolve_planned_amount(mode: str, value: float, expected_income: int) -> int:
    if mode == "fixed":
        return int(value)
    if mode == "percent":
        return int(round(expected_income * value / 100))
    return 0


@router.get("")
def list_plans(req: Request):
    username = req.state.username
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.plan_id::text AS plan_id, p.month, p.expected_income,
                   p.status, p.notes, p.created_at, p.updated_at,
                   p.period_id::text AS period_id
            FROM allocation_plans p
            JOIN users u ON u.user_id = p.user_id
            WHERE u.username = %s
            ORDER BY p.month DESC
            """,
            (username,),
        )
        return {"plans": cur.fetchall()}


@router.get("/{plan_id}")
def get_plan(plan_id: str, req: Request):
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.plan_id::text AS plan_id, p.month, p.expected_income,
                   p.status, p.notes, p.created_at, p.updated_at
            FROM allocation_plans p JOIN users u ON u.user_id=p.user_id
            WHERE p.plan_id=%s::uuid AND u.username=%s
            """,
            (plan_id, username),
        )
        plan = cur.fetchone()
        if not plan:
            raise HTTPException(status_code=404, detail="Plan not found")
        cur.execute(
            """
            SELECT i.item_id::text AS item_id, i.bucket_id::text AS bucket_id,
                   i.label, i.mode, i.value, i.priority,
                   i.planned_amount, i.funded_amount, i.status
            FROM allocation_items i WHERE i.plan_id=%s::uuid
            ORDER BY i.priority ASC, i.label ASC
            """,
            (plan_id,),
        )
        return {**plan, "items": cur.fetchall()}


@router.post("")
async def create_plan(req: Request):
    username = req.state.username
    data: dict[str, Any] = await req.json()
    month = str(data.get("month") or current_month_local()).strip()
    parse_month(month)
    expected_income = int(data.get("expected_income") or 0)
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        # Link to period if exists
        cur.execute(
            "SELECT period_id FROM monthly_periods WHERE user_id=(SELECT user_id FROM users WHERE username=%s) AND month=%s",
            (username, month),
        )
        period_row = cur.fetchone()
        period_id = period_row["period_id"] if period_row else None
        try:
            cur.execute(
                """
                INSERT INTO allocation_plans (user_id, period_id, month, expected_income, notes)
                SELECT user_id, %s::uuid, %s, %s, %s FROM users WHERE username=%s
                RETURNING plan_id::text AS plan_id
                """,
                (period_id, month, expected_income, notes, username),
            )
            row = cur.fetchone()
            conn.commit()
        except Exception as e:
            conn.rollback()
            if "unique" in str(e).lower():
                raise HTTPException(status_code=400, detail="Plan for this month already exists")
            raise
    return {"ok": True, "plan_id": row["plan_id"]}


@router.put("/{plan_id}")
async def update_plan(plan_id: str, req: Request):
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    data: dict[str, Any] = await req.json()
    expected_income = int(data.get("expected_income") or 0)
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        # Recompute all item planned_amounts when income changes
        cur.execute(
            """
            UPDATE allocation_plans SET expected_income=%s, notes=%s, updated_at=now()
            WHERE plan_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s)
              AND status='draft'
            RETURNING plan_id
            """,
            (expected_income, notes, plan_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Plan not found or not in draft status")
        # Recompute planned_amounts for percent items
        cur.execute("SELECT item_id, mode, value FROM allocation_items WHERE plan_id=%s::uuid", (plan_id,))
        for item in cur.fetchall():
            if item["mode"] == "percent":
                planned = _resolve_planned_amount("percent", float(item["value"]), expected_income)
                cur.execute("UPDATE allocation_items SET planned_amount=%s WHERE item_id=%s::uuid", (planned, item["item_id"]))
        conn.commit()
    return {"ok": True}


@router.post("/{plan_id}/activate")
def activate_plan(plan_id: str, req: Request):
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE allocation_plans SET status='active', updated_at=now()
            WHERE plan_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s)
              AND status='draft'
            RETURNING plan_id
            """,
            (plan_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Plan not found or already active/closed")
        conn.commit()
    return {"ok": True}


@router.delete("/{plan_id}")
def delete_plan(plan_id: str, req: Request):
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM allocation_plans
            WHERE plan_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s)
              AND status='draft'
            RETURNING plan_id
            """,
            (plan_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Plan not found or not deletable (only drafts can be deleted)")
        conn.commit()
    return {"ok": True}


# --- Items ---

@router.post("/{plan_id}/items")
async def add_item(plan_id: str, req: Request):
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    data: dict[str, Any] = await req.json()
    label = (data.get("label") or "").strip()
    if not label:
        raise HTTPException(status_code=400, detail="label required")
    mode = str(data.get("mode") or "percent").strip().lower()
    if mode not in ("fixed", "percent"):
        raise HTTPException(status_code=400, detail="mode must be fixed or percent")
    value = float(data.get("value") or 0)
    bucket_id = data.get("bucket_id") or None
    priority = int(data.get("priority") or 50)

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT expected_income FROM allocation_plans p JOIN users u ON u.user_id=p.user_id WHERE p.plan_id=%s::uuid AND u.username=%s",
            (plan_id, username),
        )
        plan = cur.fetchone()
        if not plan:
            raise HTTPException(status_code=404, detail="Plan not found")
        planned = _resolve_planned_amount(mode, value, int(plan["expected_income"]))
        cur.execute(
            """
            INSERT INTO allocation_items (plan_id, bucket_id, label, mode, value, priority, planned_amount)
            VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s, %s)
            RETURNING item_id::text AS item_id
            """,
            (plan_id, bucket_id, label, mode, value, priority, planned),
        )
        row = cur.fetchone()
        conn.commit()
    return {"ok": True, "item_id": row["item_id"]}


@router.put("/{plan_id}/items/{item_id}")
async def update_item(plan_id: str, item_id: str, req: Request):
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    item_id = parse_uuid_value(item_id, "item_id")
    data: dict[str, Any] = await req.json()
    label = (data.get("label") or "").strip()
    if not label:
        raise HTTPException(status_code=400, detail="label required")
    mode = str(data.get("mode") or "percent").strip().lower()
    if mode not in ("fixed", "percent"):
        raise HTTPException(status_code=400, detail="mode must be fixed or percent")
    value = float(data.get("value") or 0)
    bucket_id = data.get("bucket_id") or None
    priority = int(data.get("priority") or 50)

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT expected_income FROM allocation_plans p JOIN users u ON u.user_id=p.user_id WHERE p.plan_id=%s::uuid AND u.username=%s",
            (plan_id, username),
        )
        plan = cur.fetchone()
        if not plan:
            raise HTTPException(status_code=404, detail="Plan not found")
        planned = _resolve_planned_amount(mode, value, int(plan["expected_income"]))
        cur.execute(
            """
            UPDATE allocation_items SET label=%s, mode=%s, value=%s, bucket_id=%s::uuid,
              priority=%s, planned_amount=%s
            WHERE item_id=%s::uuid AND plan_id=%s::uuid
            RETURNING item_id
            """,
            (label, mode, value, bucket_id, priority, planned, item_id, plan_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Item not found")
        conn.commit()
    return {"ok": True}


@router.delete("/{plan_id}/items/{item_id}")
def delete_item(plan_id: str, item_id: str, req: Request):
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    item_id = parse_uuid_value(item_id, "item_id")
    with db_conn() as conn, conn.cursor() as cur:
        # Verify plan belongs to user
        cur.execute(
            "SELECT 1 FROM allocation_plans p JOIN users u ON u.user_id=p.user_id WHERE p.plan_id=%s::uuid AND u.username=%s",
            (plan_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Plan not found")
        cur.execute("DELETE FROM allocation_items WHERE item_id=%s::uuid AND plan_id=%s::uuid RETURNING item_id", (item_id, plan_id))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Item not found")
        conn.commit()
    return {"ok": True}


@router.post("/{plan_id}/items/{item_id}/fund")
async def fund_item(plan_id: str, item_id: str, req: Request):
    """Mark an allocation item as funded (manual confirm step)."""
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    item_id = parse_uuid_value(item_id, "item_id")
    data: dict[str, Any] = await req.json()
    amount = int(data.get("amount") or 0)
    if amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be > 0")

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM allocation_plans p JOIN users u ON u.user_id=p.user_id WHERE p.plan_id=%s::uuid AND u.username=%s",
            (plan_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Plan not found")
        cur.execute(
            """
            UPDATE allocation_items
            SET funded_amount = funded_amount + %s,
                status = CASE
                  WHEN funded_amount + %s >= planned_amount THEN 'funded'
                  WHEN funded_amount + %s > 0 THEN 'partial'
                  ELSE status
                END
            WHERE item_id=%s::uuid AND plan_id=%s::uuid
            RETURNING item_id, funded_amount, planned_amount, status
            """,
            (amount, amount, amount, item_id, plan_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Item not found")
        conn.commit()
    return {"ok": True, "funded_amount": row["funded_amount"], "status": row["status"]}
