"""Goals CRUD + projection endpoint — /goals and /v1/goals"""
from datetime import date
from typing import Any
from fastapi import APIRouter, HTTPException, Request
from psycopg.errors import UniqueViolation
from app.db.pool import db_conn
from app.services.ledger.balances import get_account_balances, parse_uuid_value
from app.services.ledger.period import now_utc
from app.services.projection import goal_projection

router = APIRouter(tags=["goals"])


def _parse_date(val: Any) -> date | None:
    if not val:
        return None
    try:
        return date.fromisoformat(str(val))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid target_date, expected YYYY-MM-DD")


def _validate_bucket(cur, username: str, bucket_id: str | None) -> None:
    if not bucket_id:
        return
    bucket_id = parse_uuid_value(bucket_id, "linked_bucket_id")
    cur.execute(
        """
        SELECT 1 FROM buckets b
        JOIN users u ON u.user_id = b.user_id
        WHERE u.username=%s AND b.bucket_id=%s::uuid AND b.is_archived=FALSE
        """,
        (username, bucket_id),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="Linked bucket not found")


def _create_or_find_goal_bucket(
    cur,
    username: str,
    *,
    name: str,
    target_amount: int,
    priority: int,
    notes: str | None,
) -> tuple[str, bool]:
    cur.execute(
        """
        SELECT b.bucket_id::text AS bucket_id
        FROM buckets b
        JOIN users u ON u.user_id = b.user_id
        WHERE u.username=%s AND lower(b.name)=lower(%s) AND b.is_archived=FALSE
        LIMIT 1
        """,
        (username, name),
    )
    existing = cur.fetchone()
    if existing:
        return existing["bucket_id"], False

    cur.execute(
        """
        INSERT INTO buckets (user_id, name, kind, target_amount, priority, notes)
        SELECT user_id, %s, 'goal', %s, %s, %s FROM users WHERE username=%s
        RETURNING bucket_id::text AS bucket_id
        """,
        (name, target_amount, priority, notes, username),
    )
    return cur.fetchone()["bucket_id"], True


def _bucket_current_amounts(cur, username: str) -> dict[str, int]:
    balances = get_account_balances(cur, username, now_utc())
    cur.execute(
        """
        SELECT b.bucket_id::text AS bucket_id,
               COALESCE(
                 array_agg(a.account_id::text ORDER BY a.account_name)
                   FILTER (WHERE a.account_id IS NOT NULL),
                 ARRAY[]::text[]
               ) AS linked_account_ids
        FROM buckets b
        JOIN users u ON u.user_id = b.user_id
        LEFT JOIN LATERAL (
            SELECT account_id FROM bucket_accounts WHERE bucket_id = b.bucket_id
            UNION
            SELECT b.linked_account_id WHERE b.linked_account_id IS NOT NULL
        ) ba ON TRUE
        LEFT JOIN accounts a ON a.account_id = ba.account_id AND a.username = u.username
        WHERE u.username=%s AND b.is_archived=FALSE
        GROUP BY b.bucket_id
        """,
        (username,),
    )
    result: dict[str, int] = {}
    for row in cur.fetchall():
        result[row["bucket_id"]] = sum(int(balances.get(account_id, 0)) for account_id in list(row.get("linked_account_ids") or []))
    return result


def _with_projection(goal: dict[str, Any], bucket_amounts: dict[str, int]) -> dict[str, Any]:
    linked_bucket_id = goal.get("linked_bucket_id")
    stored_current = int(goal["current_amount"])
    current_amount = int(bucket_amounts.get(linked_bucket_id, stored_current)) if linked_bucket_id else stored_current
    proj = goal_projection(
        target_amount=int(goal["target_amount"]),
        current_amount=current_amount,
        target_date=goal["target_date"],
        inflation_rate=float(goal["inflation_rate"]),
        expected_return=float(goal["expected_return"]),
    )
    return {
        **goal,
        "stored_current_amount": stored_current,
        "current_amount": current_amount,
        "progress_source": "linked_bucket" if linked_bucket_id else "manual",
        "projection": proj,
    }


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
        bucket_amounts = _bucket_current_amounts(cur, username)
    return {"goals": [_with_projection(g, bucket_amounts) for g in goals]}


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
        bucket_amounts = _bucket_current_amounts(cur, username)
    return {**_with_projection(g, bucket_amounts), "contributions": contributions}


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
    if linked_bucket_id:
        linked_bucket_id = parse_uuid_value(linked_bucket_id, "linked_bucket_id")
    priority = int(data.get("priority") or 50)
    notes = (data.get("notes") or "").strip() or None
    create_linked_bucket = bool(data.get("create_linked_bucket", False))
    bucket_name = (data.get("bucket_name") or name).strip()
    if create_linked_bucket and linked_bucket_id:
        raise HTTPException(status_code=400, detail="Choose an existing linked bucket or create a new one, not both")
    if create_linked_bucket and not bucket_name:
        raise HTTPException(status_code=400, detail="bucket_name required")

    with db_conn() as conn, conn.cursor() as cur:
        try:
            _validate_bucket(cur, username, linked_bucket_id)
            created_bucket_id = None
            created_bucket = False
            if create_linked_bucket:
                linked_bucket_id, created_bucket = _create_or_find_goal_bucket(
                    cur,
                    username,
                    name=bucket_name,
                    target_amount=target_amount,
                    priority=priority,
                    notes=notes,
                )
                created_bucket_id = linked_bucket_id if created_bucket else None
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
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Bucket name already exists")
    return {
        "ok": True,
        "goal_id": row["goal_id"],
        "linked_bucket_id": linked_bucket_id,
        "created_bucket_id": created_bucket_id,
        "created_bucket": created_bucket,
    }


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
    if linked_bucket_id:
        linked_bucket_id = parse_uuid_value(linked_bucket_id, "linked_bucket_id")
    priority = int(data.get("priority") or 50)
    status = str(data.get("status") or "active").strip().lower()
    if status not in ("active", "paused", "completed", "cancelled"):
        raise HTTPException(status_code=400, detail="Invalid status")
    notes = (data.get("notes") or "").strip() or None
    create_linked_bucket = bool(data.get("create_linked_bucket", False))
    bucket_name = (data.get("bucket_name") or name).strip()
    if create_linked_bucket and linked_bucket_id:
        raise HTTPException(status_code=400, detail="Choose an existing linked bucket or create a new one, not both")
    if create_linked_bucket and not bucket_name:
        raise HTTPException(status_code=400, detail="bucket_name required")

    with db_conn() as conn, conn.cursor() as cur:
        try:
            _validate_bucket(cur, username, linked_bucket_id)
            created_bucket_id = None
            created_bucket = False
            if create_linked_bucket:
                linked_bucket_id, created_bucket = _create_or_find_goal_bucket(
                    cur,
                    username,
                    name=bucket_name,
                    target_amount=target_amount,
                    priority=priority,
                    notes=notes,
                )
                created_bucket_id = linked_bucket_id if created_bucket else None
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
                conn.rollback()
                raise HTTPException(status_code=404, detail="Goal not found")
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Bucket name already exists")
    return {"ok": True, "linked_bucket_id": linked_bucket_id, "created_bucket_id": created_bucket_id, "created_bucket": created_bucket}


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
            "SELECT goal_id, linked_bucket_id FROM financial_goals g JOIN users u ON u.user_id=g.user_id WHERE g.goal_id=%s::uuid AND u.username=%s AND g.status='active'",
            (goal_id, username),
        )
        goal = cur.fetchone()
        if not goal:
            raise HTTPException(status_code=404, detail="Goal not found or not active")
        if goal.get("linked_bucket_id") is not None:
            raise HTTPException(status_code=400, detail="Linked goal progress comes from its bucket balance")
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
            "SELECT target_amount, current_amount, target_date, inflation_rate, expected_return, linked_bucket_id::text AS linked_bucket_id FROM financial_goals g JOIN users u ON u.user_id=g.user_id WHERE g.goal_id=%s::uuid AND u.username=%s",
            (goal_id, username),
        )
        g = cur.fetchone()
        if not g:
            raise HTTPException(status_code=404, detail="Goal not found")
        bucket_amounts = _bucket_current_amounts(cur, username) if g.get("linked_bucket_id") else {}
    current_amount = int(bucket_amounts.get(g.get("linked_bucket_id"), int(g["current_amount"])))
    return goal_projection(
        target_amount=int(g["target_amount"]),
        current_amount=current_amount,
        target_date=g["target_date"],
        inflation_rate=float(g["inflation_rate"]),
        expected_return=float(g["expected_return"]),
        monthly_contribution=monthly_contribution,
    )
