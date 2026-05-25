"""Strategy Rules CRUD + preview — /strategy-rules and /v1/strategy-rules"""
from typing import Any
from fastapi import APIRouter, HTTPException, Request
from app.db.pool import db_conn
from app.services.ledger.balances import parse_uuid_value

router = APIRouter(tags=["strategy"])


@router.get("")
def list_rules(req: Request):
    username = req.state.username
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.rule_id::text AS rule_id, r.name, r.trigger, r.mode,
                   r.target_bucket_id::text AS target_bucket_id,
                   b.name AS target_bucket_name,
                   r.value, r.cap, r.floor, r.priority, r.is_active, r.notes
            FROM strategy_rules r
            JOIN users u ON u.user_id = r.user_id
            LEFT JOIN buckets b ON b.bucket_id = r.target_bucket_id
            WHERE u.username = %s
            ORDER BY r.priority ASC, r.name ASC
            """,
            (username,),
        )
        return {"rules": cur.fetchall()}


@router.post("")
async def create_rule(req: Request):
    username = req.state.username
    data: dict[str, Any] = await req.json()
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    trigger = str(data.get("trigger") or "manual").strip().lower()
    if trigger not in ("income_arrival", "manual"):
        raise HTTPException(status_code=400, detail="Invalid trigger")
    mode = str(data.get("mode") or "percent").strip().lower()
    if mode not in ("fixed", "percent", "target_balance", "overflow"):
        raise HTTPException(status_code=400, detail="Invalid mode")
    target_bucket_id = data.get("target_bucket_id") or None
    value = float(data.get("value") or 0)
    cap = data.get("cap")
    if cap is not None:
        cap = int(cap)
    floor = data.get("floor")
    if floor is not None:
        floor = int(floor)
    priority = int(data.get("priority") or 50)
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO strategy_rules (user_id, name, trigger, mode, target_bucket_id, value, cap, floor, priority, notes)
            SELECT user_id, %s, %s, %s, %s::uuid, %s, %s, %s, %s, %s FROM users WHERE username=%s
            RETURNING rule_id::text AS rule_id
            """,
            (name, trigger, mode, target_bucket_id, value, cap, floor, priority, notes, username),
        )
        row = cur.fetchone()
        conn.commit()
    return {"ok": True, "rule_id": row["rule_id"]}


@router.put("/{rule_id}")
async def update_rule(rule_id: str, req: Request):
    username = req.state.username
    rule_id = parse_uuid_value(rule_id, "rule_id")
    data: dict[str, Any] = await req.json()
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    trigger = str(data.get("trigger") or "manual").strip().lower()
    if trigger not in ("income_arrival", "manual"):
        raise HTTPException(status_code=400, detail="Invalid trigger")
    mode = str(data.get("mode") or "percent").strip().lower()
    if mode not in ("fixed", "percent", "target_balance", "overflow"):
        raise HTTPException(status_code=400, detail="Invalid mode")
    target_bucket_id = data.get("target_bucket_id") or None
    value = float(data.get("value") or 0)
    cap = data.get("cap")
    if cap is not None:
        cap = int(cap)
    floor = data.get("floor")
    if floor is not None:
        floor = int(floor)
    priority = int(data.get("priority") or 50)
    is_active = bool(data.get("is_active", True))
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE strategy_rules
            SET name=%s, trigger=%s, mode=%s, target_bucket_id=%s::uuid,
                value=%s, cap=%s, floor=%s, priority=%s, is_active=%s, notes=%s
            WHERE rule_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s)
            RETURNING rule_id
            """,
            (name, trigger, mode, target_bucket_id, value, cap, floor, priority, is_active, notes, rule_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Rule not found")
        conn.commit()
    return {"ok": True}


@router.delete("/{rule_id}")
def delete_rule(rule_id: str, req: Request):
    username = req.state.username
    rule_id = parse_uuid_value(rule_id, "rule_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM strategy_rules WHERE rule_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s) RETURNING rule_id",
            (rule_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Rule not found")
        conn.commit()
    return {"ok": True}


@router.post("/preview")
async def preview_strategy(req: Request):
    """
    Given an income amount, simulate applying all active strategy rules in priority order.
    Returns a list of suggested allocations without writing anything.
    """
    username = req.state.username
    data: dict[str, Any] = await req.json()
    income = int(data.get("income") or 0)
    if income <= 0:
        raise HTTPException(status_code=400, detail="income must be > 0")

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.rule_id::text AS rule_id, r.name, r.mode, r.value,
                   r.cap, r.floor, r.priority,
                   r.target_bucket_id::text AS target_bucket_id,
                   b.name AS target_bucket_name
            FROM strategy_rules r
            JOIN users u ON u.user_id = r.user_id
            LEFT JOIN buckets b ON b.bucket_id = r.target_bucket_id
            WHERE u.username = %s AND r.is_active = TRUE
            ORDER BY r.priority ASC
            """,
            (username,),
        )
        rules = cur.fetchall()

    remaining = income
    allocations = []

    for rule in rules:
        if remaining <= 0:
            break
        mode = rule["mode"]
        value = float(rule["value"])
        cap = rule.get("cap")
        floor_val = rule.get("floor")

        if mode == "overflow":
            amount = remaining
        elif mode == "fixed":
            amount = int(value)
        elif mode == "percent":
            amount = int(round(income * value / 100))
        elif mode == "target_balance":
            # Allocate up to target; simplified — doesn't check current bucket balance
            amount = int(value)
        else:
            amount = 0

        amount = min(amount, remaining)
        if cap is not None:
            amount = min(amount, int(cap))
        if floor_val is not None and amount < int(floor_val):
            amount = 0  # skip if below floor

        if amount > 0:
            remaining -= amount

        allocations.append({
            "rule_id": rule["rule_id"],
            "rule_name": rule["name"],
            "target_bucket_id": rule["target_bucket_id"],
            "target_bucket_name": rule["target_bucket_name"],
            "mode": mode,
            "amount": amount,
        })

    return {
        "income": income,
        "total_allocated": income - remaining,
        "remaining": remaining,
        "allocations": allocations,
    }
