"""Goals CRUD + projection endpoint — /goals and /v1/goals"""
from datetime import date, timezone
from typing import Any
from fastapi import APIRouter, HTTPException, Request
from app.db.pool import db_conn
from app.services.ledger.balances import parse_uuid_value
from app.services.projection import goal_projection

router = APIRouter(tags=["goals"])


def _parse_date(val: Any) -> date | None:
    if not val:
        return None
    try:
        return date.fromisoformat(str(val))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid target_date, expected YYYY-MM-DD")


@router.get("")
def list_goals(req: Request):
    username = req.state.username
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT g.goal_id::text AS goal_id, g.name, g.target_amount, g.target_date,
                   g.current_amount, g.inflation_rate, g.expected_return,
                   g.linked_bucket_id::text AS linked_bucket_id,
                   b.name AS linked_bucket_name,
                   g.priority, g.status, g.notes, g.created_at
            FROM financial_goals g
            JOIN users u ON u.user_id = g.user_id
            LEFT JOIN buckets b ON b.bucket_id = g.linked_bucket_id
            WHERE u.username = %s AND g.status NOT IN ('cancelled')
            ORDER BY g.priority ASC, g.name ASC
            """,
            (username,),
        )
        goals = cur.fetchall()
    # Attach projection to each goal
    result = []
    for g in goals:
        proj = goal_projection(
            target_amount=int(g["target_amount"]),
            current_amount=int(g["current_amount"]),
            target_date=g["target_date"],
            inflation_rate=float(g["inflation_rate"]),
            expected_return=float(g["expected_return"]),
        )
        result.append({**g, "projection": proj})
    return {"goals": result}


@router.get("/{goal_id}")
def get_goal(goal_id: str, req: Request):
    username = req.state.username
    goal_id = parse_uuid_value(goal_id, "goal_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT g.goal_id::text AS goal_id, g.name, g.target_amount, g.target_date,
                   g.current_amount, g.inflation_rate, g.expected_return,
                   g.linked_bucket_id::text AS linked_bucket_id,
                   g.priority, g.status, g.notes
            FROM financial_goals g JOIN users u ON u.user_id=g.user_id
            WHERE g.goal_id=%s::uuid AND u.username=%s
            """,
            (goal_id, username),
        )
        g = cur.fetchone()
        if not g:
            raise HTTPException(status_code=404, detail="Goal not found")
        cur.execute(
            "SELECT contribution_id::text, amount, date, source, notes FROM goal_contributions WHERE goal_id=%s::uuid ORDER BY date DESC LIMIT 50",
            (goal_id,),
        )
        contributions = cur.fetchall()
    proj = goal_projection(
        target_amount=int(g["target_amount"]),
        current_amount=int(g["current_amount"]),
        target_date=g["target_date"],
        inflation_rate=float(g["inflation_rate"]),
        expected_return=float(g["expected_return"]),
    )
    return {**g, "projection": proj, "contributions": contributions}


@router.post("")
async def create_goal(req: Request):
    username = req.state.username
    data: dict[str, Any] = await req.json()
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    target_amount = int(data.get("target_amount") or 0)
    if target_amount <= 0:
        raise HTTPException(status_code=400, detail="target_amount must be > 0")
    target_date = _parse_date(data.get("target_date"))
    inflation_rate = float(data.get("inflation_rate") or 0.05)
    expected_return = float(data.get("expected_return") or 0.06)
    linked_bucket_id = data.get("linked_bucket_id") or None
    priority = int(data.get("priority") or 50)
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_goals (user_id, name, target_amount, target_date, inflation_rate, expected_return, linked_bucket_id, priority, notes)
            SELECT user_id, %s, %s, %s, %s, %s, %s::uuid, %s, %s FROM users WHERE username=%s
            RETURNING goal_id::text AS goal_id
            """,
            (name, target_amount, target_date, inflation_rate, expected_return, linked_bucket_id, priority, notes, username),
        )
        row = cur.fetchone()
        conn.commit()
    return {"ok": True, "goal_id": row["goal_id"]}


@router.put("/{goal_id}")
async def update_goal(goal_id: str, req: Request):
    username = req.state.username
    goal_id = parse_uuid_value(goal_id, "goal_id")
    data: dict[str, Any] = await req.json()
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    target_amount = int(data.get("target_amount") or 0)
    if target_amount <= 0:
        raise HTTPException(status_code=400, detail="target_amount must be > 0")
    target_date = _parse_date(data.get("target_date"))
    inflation_rate = float(data.get("inflation_rate") or 0.05)
    expected_return = float(data.get("expected_return") or 0.06)
    linked_bucket_id = data.get("linked_bucket_id") or None
    priority = int(data.get("priority") or 50)
    status = str(data.get("status") or "active").strip().lower()
    if status not in ("active", "paused", "completed", "cancelled"):
        raise HTTPException(status_code=400, detail="Invalid status")
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE financial_goals
            SET name=%s, target_amount=%s, target_date=%s, inflation_rate=%s,
                expected_return=%s, linked_bucket_id=%s::uuid, priority=%s,
                status=%s, notes=%s, updated_at=now()
            WHERE goal_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s)
            RETURNING goal_id
            """,
            (name, target_amount, target_date, inflation_rate, expected_return, linked_bucket_id, priority, status, notes, goal_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Goal not found")
        conn.commit()
    return {"ok": True}


@router.delete("/{goal_id}")
def delete_goal(goal_id: str, req: Request):
    username = req.state.username
    goal_id = parse_uuid_value(goal_id, "goal_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE financial_goals SET status='cancelled', updated_at=now() WHERE goal_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s) RETURNING goal_id",
            (goal_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Goal not found")
        conn.commit()
    return {"ok": True}


@router.post("/{goal_id}/contribute")
async def contribute(goal_id: str, req: Request):
    username = req.state.username
    goal_id = parse_uuid_value(goal_id, "goal_id")
    data: dict[str, Any] = await req.json()
    amount = int(data.get("amount") or 0)
    if amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be > 0")
    notes = (data.get("notes") or "").strip() or None
    source = str(data.get("source") or "manual").strip().lower()
    if source not in ("manual", "allocation"):
        source = "manual"

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT goal_id FROM financial_goals g JOIN users u ON u.user_id=g.user_id WHERE g.goal_id=%s::uuid AND u.username=%s AND g.status='active'",
            (goal_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Goal not found or not active")
        cur.execute(
            "INSERT INTO goal_contributions (goal_id, amount, source, notes) VALUES (%s::uuid, %s, %s, %s) RETURNING contribution_id::text AS contribution_id",
            (goal_id, amount, source, notes),
        )
        contribution_id = cur.fetchone()["contribution_id"]
        # Update current_amount
        cur.execute(
            "UPDATE financial_goals SET current_amount = current_amount + %s, updated_at=now() WHERE goal_id=%s::uuid",
            (amount, goal_id),
        )
        conn.commit()
    return {"ok": True, "contribution_id": contribution_id}


@router.get("/{goal_id}/projection")
def get_projection(goal_id: str, req: Request, monthly_contribution: int = 0):
    username = req.state.username
    goal_id = parse_uuid_value(goal_id, "goal_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT target_amount, current_amount, target_date, inflation_rate, expected_return FROM financial_goals g JOIN users u ON u.user_id=g.user_id WHERE g.goal_id=%s::uuid AND u.username=%s",
            (goal_id, username),
        )
        g = cur.fetchone()
    if not g:
        raise HTTPException(status_code=404, detail="Goal not found")
    return goal_projection(
        target_amount=int(g["target_amount"]),
        current_amount=int(g["current_amount"]),
        target_date=g["target_date"],
        inflation_rate=float(g["inflation_rate"]),
        expected_return=float(g["expected_return"]),
        monthly_contribution=monthly_contribution,
    )
