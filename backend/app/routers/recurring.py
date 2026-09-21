import calendar
from datetime import date, datetime, timedelta, timezone
from typing import Any, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.db.pool import db_conn
from app.routers.movements import _ensure_internal_movement_categories
from app.services.auth import get_current_user

router = APIRouter(tags=["Recurring"])


def calculate_next_due_date(
    schedule_type: str,
    schedule_day: Optional[int],
    user_payday_day: int = 25,
    from_date: Optional[date] = None,
    advance: bool = False,
) -> date:
    """
    Computes the next due date for a recurring rule:
    - 'payday': anchored on the user's monthly payday day.
    - 'monthly_day': anchored on a specific day of the month (1-31).
    - 'weekly': anchored on day of week (1=Monday .. 7=Sunday).
    """
    ref = from_date or date.today()

    if schedule_type == "weekly":
        target_weekday = schedule_day if schedule_day and 1 <= schedule_day <= 7 else 1
        current_weekday = ref.isoweekday()  # 1=Mon, 7=Sun
        days_ahead = (target_weekday - current_weekday) % 7
        if days_ahead == 0 and advance:
            days_ahead = 7
        return ref + timedelta(days=days_ahead)

    # Monthly or Payday
    target_day = schedule_day if (schedule_type == "monthly_day" and schedule_day) else user_payday_day
    target_day = max(1, min(31, target_day))

    year = ref.year
    month = ref.month

    # Check if due this month
    max_days_this_month = calendar.monthrange(year, month)[1]
    candidate_day = min(target_day, max_days_this_month)
    candidate_date = date(year, month, candidate_day)

    if not advance and candidate_date >= ref:
        return candidate_date

    # Advance to next month
    if month == 12:
        next_year = year + 1
        next_month = 1
    else:
        next_year = year
        next_month = month + 1

    max_days_next_month = calendar.monthrange(next_year, next_month)[1]
    return date(next_year, next_month, min(target_day, max_days_next_month))


# --- Schemas ---

class RecurringRuleCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    type: str = Field(pattern="^(expense|income|transfer)$")
    amount: int = Field(gt=0)
    source_account_id: UUID
    target_account_id: Optional[UUID] = None
    category_id: Optional[UUID] = None
    obligation_id: Optional[UUID] = None
    schedule_type: str = Field(default="monthly_day", pattern="^(monthly_day|payday|weekly)$")
    schedule_day: Optional[int] = Field(default=None, ge=1, le=31)
    notes: Optional[str] = Field(default=None, max_length=500)
    is_payroll_allocation: bool = False
    auto_post: bool = False
    is_active: bool = True


class RecurringRuleUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=100)
    type: Optional[str] = Field(default=None, pattern="^(expense|income|transfer)$")
    amount: Optional[int] = Field(default=None, gt=0)
    source_account_id: Optional[UUID] = None
    target_account_id: Optional[UUID] = None
    category_id: Optional[UUID] = None
    obligation_id: Optional[UUID] = None
    schedule_type: Optional[str] = Field(default=None, pattern="^(monthly_day|payday|weekly)$")
    schedule_day: Optional[int] = Field(default=None, ge=1, le=31)
    notes: Optional[str] = Field(default=None, max_length=500)
    is_payroll_allocation: Optional[bool] = None
    auto_post: Optional[bool] = None
    is_active: Optional[bool] = None


class ExecuteRulesRequest(BaseModel):
    rule_ids: List[UUID]
    execution_date: Optional[date] = None


class PayrollExecutionItem(BaseModel):
    rule_id: Optional[UUID] = None
    source_account_id: UUID
    target_account_id: UUID
    amount: int = Field(gt=0)
    notes: Optional[str] = None


class PayrollBatchExecuteRequest(BaseModel):
    items: List[PayrollExecutionItem]
    execution_date: Optional[date] = None


def format_recurring_rule_row(r: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(r["id"]),
        "name": r["name"],
        "type": r["type"],
        "amount": int(r["amount"]),
        "source_account_id": str(r["source_account_id"]),
        "source_account_name": r.get("source_account_name"),
        "target_account_id": str(r["target_account_id"]) if r.get("target_account_id") else None,
        "target_account_name": r.get("target_account_name"),
        "category_id": str(r["category_id"]) if r.get("category_id") else None,
        "category_name": r.get("category_name"),
        "category_icon": r.get("category_icon"),
        "category_color": r.get("category_color"),
        "obligation_id": str(r["obligation_id"]) if r.get("obligation_id") else None,
        "obligation_name": r.get("obligation_name"),
        "schedule_type": r["schedule_type"],
        "schedule_day": r["schedule_day"],
        "notes": r["notes"],
        "is_payroll_allocation": bool(r["is_payroll_allocation"]),
        "auto_post": bool(r["auto_post"]),
        "last_executed_at": r["last_executed_at"].isoformat() if r.get("last_executed_at") else None,
        "next_due_date": r["next_due_date"].isoformat() if r.get("next_due_date") else None,
        "is_active": bool(r["is_active"]),
        "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
        "updated_at": r["updated_at"].isoformat() if r.get("updated_at") else None,
    }


# --- Endpoints ---

@router.get("")
def list_recurring_rules(
    is_payroll: Optional[bool] = None,
    include_inactive: bool = Query(default=True),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    conditions = ["r.user_id = %s"]
    params: list[Any] = [user_id]

    if not include_inactive:
        conditions.append("r.is_active = true")

    if is_payroll is not None:
        conditions.append("r.is_payroll_allocation = %s")
        params.append(is_payroll)

    where_clause = " AND ".join(conditions)

    query = f"""
        SELECT 
            r.*,
            sa.name AS source_account_name,
            ta.name AS target_account_name,
            c.name AS category_name,
            c.icon AS category_icon,
            c.color AS category_color,
            o.name AS obligation_name
        FROM recurring_rules r
        JOIN accounts sa ON sa.id = r.source_account_id
        LEFT JOIN accounts ta ON ta.id = r.target_account_id
        LEFT JOIN categories c ON c.id = r.category_id
        LEFT JOIN obligations o ON o.id = r.obligation_id
        WHERE {where_clause}
        ORDER BY r.is_active DESC, r.next_due_date ASC, r.created_at DESC
    """

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            return {
                "ok": True,
                "rules": [format_recurring_rule_row(r) for r in rows],
            }


@router.post("")
def create_recurring_rule(
    payload: RecurringRuleCreate,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    payday_day = int(current_user.get("payday_day") or 25)

    source_id = str(payload.source_account_id)
    target_id = str(payload.target_account_id) if payload.target_account_id else None
    cat_id = str(payload.category_id) if payload.category_id else None
    ob_id = str(payload.obligation_id) if payload.obligation_id else None

    if payload.type == "transfer":
        if not target_id:
            raise HTTPException(status_code=400, detail="Transfer requires target_account_id")
        if target_id == source_id:
            raise HTTPException(status_code=400, detail="Cannot transfer to the same account")

    with db_conn() as conn:
        with conn.cursor() as cur:
            # Validate source account
            cur.execute("SELECT id FROM accounts WHERE user_id = %s AND id = %s", (user_id, source_id))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Source account not found")

            # Validate target account if transfer
            if target_id:
                cur.execute("SELECT id FROM accounts WHERE user_id = %s AND id = %s", (user_id, target_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Target account not found")

            # Validate category if set
            if cat_id:
                cur.execute("SELECT id FROM categories WHERE user_id = %s AND id = %s", (user_id, cat_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Category not found")

            # Validate obligation if set
            if ob_id:
                cur.execute("SELECT id FROM obligations WHERE user_id = %s AND id = %s", (user_id, ob_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Obligation not found")

            next_due = calculate_next_due_date(
                schedule_type=payload.schedule_type,
                schedule_day=payload.schedule_day,
                user_payday_day=payday_day,
            )

            cur.execute(
                """
                INSERT INTO recurring_rules (
                    user_id, name, type, amount, source_account_id, target_account_id,
                    category_id, obligation_id, schedule_type, schedule_day, notes,
                    is_payroll_allocation, auto_post, next_due_date, is_active
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    user_id,
                    payload.name,
                    payload.type,
                    payload.amount,
                    source_id,
                    target_id,
                    cat_id,
                    ob_id,
                    payload.schedule_type,
                    payload.schedule_day,
                    payload.notes,
                    payload.is_payroll_allocation,
                    payload.auto_post,
                    next_due,
                    payload.is_active,
                ),
            )
            rule_id = str(cur.fetchone()["id"])

    return {"ok": True, "id": rule_id, "next_due_date": next_due.isoformat()}


@router.put("/{rule_id}")
def update_recurring_rule(
    rule_id: UUID,
    payload: RecurringRuleUpdate,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    payday_day = int(current_user.get("payday_day") or 25)

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM recurring_rules WHERE id = %s AND user_id = %s",
                (str(rule_id), user_id),
            )
            existing = cur.fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail="Recurring rule not found")

            # Determine updated values
            name = payload.name if payload.name is not None else existing["name"]
            rtype = payload.type if payload.type is not None else existing["type"]
            amount = payload.amount if payload.amount is not None else existing["amount"]
            source_id = str(payload.source_account_id) if payload.source_account_id is not None else str(existing["source_account_id"])
            target_id = str(payload.target_account_id) if payload.target_account_id is not None else (str(existing["target_account_id"]) if existing["target_account_id"] else None)
            cat_id = str(payload.category_id) if payload.category_id is not None else (str(existing["category_id"]) if existing["category_id"] else None)
            ob_id = str(payload.obligation_id) if payload.obligation_id is not None else (str(existing["obligation_id"]) if existing["obligation_id"] else None)
            stype = payload.schedule_type if payload.schedule_type is not None else existing["schedule_type"]
            sday = payload.schedule_day if payload.schedule_day is not None else existing["schedule_day"]
            notes = payload.notes if payload.notes is not None else existing["notes"]
            is_payroll = payload.is_payroll_allocation if payload.is_payroll_allocation is not None else existing["is_payroll_allocation"]
            auto_post = payload.auto_post if payload.auto_post is not None else existing["auto_post"]
            is_active = payload.is_active if payload.is_active is not None else existing["is_active"]

            if rtype == "transfer":
                if not target_id:
                    raise HTTPException(status_code=400, detail="Transfer requires target_account_id")
                if target_id == source_id:
                    raise HTTPException(status_code=400, detail="Cannot transfer to the same account")

            # Recompute next_due_date if schedule changed
            next_due = existing["next_due_date"]
            if payload.schedule_type is not None or payload.schedule_day is not None:
                next_due = calculate_next_due_date(
                    schedule_type=stype,
                    schedule_day=sday,
                    user_payday_day=payday_day,
                )

            cur.execute(
                """
                UPDATE recurring_rules
                SET name = %s, type = %s, amount = %s, source_account_id = %s, target_account_id = %s,
                    category_id = %s, obligation_id = %s, schedule_type = %s, schedule_day = %s,
                    notes = %s, is_payroll_allocation = %s, auto_post = %s, next_due_date = %s,
                    is_active = %s, updated_at = NOW()
                WHERE id = %s AND user_id = %s
                """,
                (
                    name,
                    rtype,
                    amount,
                    source_id,
                    target_id,
                    cat_id,
                    ob_id,
                    stype,
                    sday,
                    notes,
                    is_payroll,
                    auto_post,
                    next_due,
                    is_active,
                    str(rule_id),
                    user_id,
                ),
            )

    return {"ok": True, "id": str(rule_id), "next_due_date": next_due.isoformat() if next_due else None}


@router.delete("/{rule_id}")
def delete_recurring_rule(
    rule_id: UUID,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM recurring_rules WHERE id = %s AND user_id = %s RETURNING id",
                (str(rule_id), user_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Recurring rule not found")
    return {"ok": True, "deleted_id": str(rule_id)}


@router.post("/{rule_id}/toggle")
def toggle_recurring_rule(
    rule_id: UUID,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE recurring_rules
                SET is_active = NOT is_active, updated_at = NOW()
                WHERE id = %s AND user_id = %s
                RETURNING id, is_active
                """,
                (str(rule_id), user_id),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Recurring rule not found")
            return {"ok": True, "id": str(rule_id), "is_active": row["is_active"]}


# --- Engine: Pending & Due Execution ---

@router.get("/pending")
def list_pending_recurring_rules(
    current_user: dict = Depends(get_current_user),
):
    """
    Returns rules that are currently due (next_due_date <= today) and require manual confirmation (auto_post = false).
    """
    user_id = current_user["id"]
    today = date.today()

    query = """
        SELECT 
            r.*,
            sa.name AS source_account_name,
            ta.name AS target_account_name,
            c.name AS category_name,
            c.icon AS category_icon,
            c.color AS category_color,
            o.name AS obligation_name
        FROM recurring_rules r
        JOIN accounts sa ON sa.id = r.source_account_id
        LEFT JOIN accounts ta ON ta.id = r.target_account_id
        LEFT JOIN categories c ON c.id = r.category_id
        LEFT JOIN obligations o ON o.id = r.obligation_id
        WHERE r.user_id = %s 
          AND r.is_active = true 
          AND r.next_due_date <= %s
          AND r.auto_post = false
        ORDER BY r.next_due_date ASC
    """

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (user_id, today))
            rows = cur.fetchall()
            return {
                "ok": True,
                "pending_count": len(rows),
                "rules": [format_recurring_rule_row(r) for r in rows],
            }


@router.post("/process-due")
def process_auto_post_due_rules(
    current_user: dict = Depends(get_current_user),
):
    """
    Scans for active rules with auto_post=true whose next_due_date <= today,
    and executes them automatically into the ledger.
    """
    user_id = current_user["id"]
    payday_day = int(current_user.get("payday_day") or 25)
    today = date.today()
    executed_rules = []

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM recurring_rules
                WHERE user_id = %s AND is_active = true AND auto_post = true AND next_due_date <= %s
                FOR UPDATE
                """,
                (user_id, today),
            )
            due_rules = cur.fetchall()

            for rule in due_rules:
                rule_id = str(rule["id"])
                rule_date = rule["next_due_date"]
                tx_dt = datetime(rule_date.year, rule_date.month, rule_date.day, 10, 0, 0, tzinfo=timezone.utc)

                # Create transaction(s)
                if rule["type"] == "transfer":
                    exp_cat_id, inc_cat_id = _ensure_internal_movement_categories(cur, user_id)
                    cur.execute(
                        """
                        INSERT INTO transactions (
                            user_id, account_id, category_id, obligation_id, type,
                            amount, notes, date, recurring_rule_id, kakeibo_type
                        )
                        VALUES (%s, %s, %s, %s, 'expense', %s, %s, %s, %s, 'saving')
                        """,
                        (
                            user_id,
                            rule["source_account_id"],
                            exp_cat_id,
                            rule["obligation_id"],
                            rule["amount"],
                            rule["notes"] or f"Otomatis: {rule['name']}",
                            tx_dt,
                            rule_id,
                        ),
                    )
                    cur.execute(
                        """
                        INSERT INTO transactions (
                            user_id, account_id, category_id, obligation_id, type,
                            amount, notes, date, recurring_rule_id, kakeibo_type
                        )
                        VALUES (%s, %s, %s, %s, 'income', %s, %s, %s, %s, NULL)
                        """,
                        (
                            user_id,
                            rule["target_account_id"],
                            inc_cat_id,
                            rule["obligation_id"],
                            rule["amount"],
                            rule["notes"] or f"Otomatis: {rule['name']}",
                            tx_dt,
                            rule_id,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO transactions (
                            user_id, account_id, category_id, obligation_id, type,
                            amount, notes, date, recurring_rule_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            user_id,
                            rule["source_account_id"],
                            rule["category_id"],
                            rule["obligation_id"],
                            rule["type"],
                            rule["amount"],
                            rule["notes"] or f"Otomatis: {rule['name']}",
                            tx_dt,
                            rule_id,
                        ),
                    )

                # Decrement obligation if linked
                if rule["obligation_id"]:
                    if rule["type"] == "expense":
                        cur.execute(
                            """
                            UPDATE obligations
                            SET remaining_amount = GREATEST(0, remaining_amount - %s),
                                is_archived = CASE WHEN (remaining_amount - %s) <= 0 THEN true ELSE is_archived END,
                                updated_at = NOW()
                            WHERE id = %s AND user_id = %s
                            """,
                            (rule["amount"], rule["amount"], rule["obligation_id"], user_id),
                        )

                # Advance next_due_date
                next_due = calculate_next_due_date(
                    schedule_type=rule["schedule_type"],
                    schedule_day=rule["schedule_day"],
                    user_payday_day=payday_day,
                    from_date=rule["next_due_date"],
                    advance=True,
                )

                cur.execute(
                    """
                    UPDATE recurring_rules
                    SET last_executed_at = NOW(), next_due_date = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (next_due, rule_id),
                )
                executed_rules.append(rule_id)

    return {
        "ok": True,
        "processed_count": len(executed_rules),
        "executed_rule_ids": executed_rules,
    }


@router.post("/execute")
def execute_selected_rules(
    payload: ExecuteRulesRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Manually executes one or more specific recurring rules on demand (1-tap confirmation).
    """
    user_id = current_user["id"]
    payday_day = int(current_user.get("payday_day") or 25)
    exec_date = payload.execution_date or date.today()
    tx_dt = datetime(exec_date.year, exec_date.month, exec_date.day, 12, 0, 0, tzinfo=timezone.utc)

    executed_count = 0
    with db_conn() as conn:
        with conn.cursor() as cur:
            for r_id in payload.rule_ids:
                cur.execute(
                    "SELECT * FROM recurring_rules WHERE id = %s AND user_id = %s FOR UPDATE",
                    (str(r_id), user_id),
                )
                rule = cur.fetchone()
                if not rule:
                    continue

                if rule["type"] == "transfer":
                    exp_cat_id, inc_cat_id = _ensure_internal_movement_categories(cur, user_id)
                    cur.execute(
                        """
                        INSERT INTO transactions (
                            user_id, account_id, category_id, obligation_id, type,
                            amount, notes, date, recurring_rule_id, kakeibo_type
                        )
                        VALUES (%s, %s, %s, %s, 'expense', %s, %s, %s, %s, 'saving')
                        """,
                        (
                            user_id,
                            rule["source_account_id"],
                            exp_cat_id,
                            rule["obligation_id"],
                            rule["amount"],
                            rule["notes"] or f"Rutin: {rule['name']}",
                            tx_dt,
                            str(rule["id"]),
                        ),
                    )
                    cur.execute(
                        """
                        INSERT INTO transactions (
                            user_id, account_id, category_id, obligation_id, type,
                            amount, notes, date, recurring_rule_id, kakeibo_type
                        )
                        VALUES (%s, %s, %s, %s, 'income', %s, %s, %s, %s, NULL)
                        """,
                        (
                            user_id,
                            rule["target_account_id"],
                            inc_cat_id,
                            rule["obligation_id"],
                            rule["amount"],
                            rule["notes"] or f"Rutin: {rule['name']}",
                            tx_dt,
                            str(rule["id"]),
                        ),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO transactions (
                            user_id, account_id, category_id, obligation_id, type,
                            amount, notes, date, recurring_rule_id
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            user_id,
                            rule["source_account_id"],
                            rule["category_id"],
                            rule["obligation_id"],
                            rule["type"],
                            rule["amount"],
                            rule["notes"] or f"Rutin: {rule['name']}",
                            tx_dt,
                            str(rule["id"]),
                        ),
                    )

                if rule["obligation_id"] and rule["type"] == "expense":
                    cur.execute(
                        """
                        UPDATE obligations
                        SET remaining_amount = GREATEST(0, remaining_amount - %s),
                            is_archived = CASE WHEN (remaining_amount - %s) <= 0 THEN true ELSE is_archived END,
                            updated_at = NOW()
                        WHERE id = %s AND user_id = %s
                        """,
                        (rule["amount"], rule["amount"], rule["obligation_id"], user_id),
                    )

                next_due = calculate_next_due_date(
                    schedule_type=rule["schedule_type"],
                    schedule_day=rule["schedule_day"],
                    user_payday_day=payday_day,
                    from_date=rule["next_due_date"],
                    advance=True,
                )

                cur.execute(
                    """
                    UPDATE recurring_rules
                    SET last_executed_at = NOW(), next_due_date = %s, updated_at = NOW()
                    WHERE id = %s
                    """,
                    (next_due, str(rule["id"])),
                )
                executed_count += 1

    return {"ok": True, "executed_count": executed_count}


@router.post("/payroll/execute")
def execute_payroll_batch(
    payload: PayrollBatchExecuteRequest,
    current_user: dict = Depends(get_current_user),
):
    """
    Atomic 1-tap execution of multiple payroll allocation transfers into accounts and pockets.
    """
    user_id = current_user["id"]
    payday_day = int(current_user.get("payday_day") or 25)
    exec_date = payload.execution_date or date.today()
    tx_dt = datetime(exec_date.year, exec_date.month, exec_date.day, 12, 0, 0, tzinfo=timezone.utc)

    if not payload.items:
        raise HTTPException(status_code=400, detail="No allocation items provided")

    with db_conn() as conn:
        with conn.cursor() as cur:
            # Ensure internal movement categories
            exp_cat_id, inc_cat_id = _ensure_internal_movement_categories(cur, user_id)

            # Validate all accounts belong to user
            for item in payload.items:
                source_id = str(item.source_account_id)
                target_id = str(item.target_account_id)

                cur.execute("SELECT id FROM accounts WHERE user_id = %s AND id = %s", (user_id, source_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail=f"Source account {source_id} not found")

                cur.execute("SELECT id FROM accounts WHERE user_id = %s AND id = %s", (user_id, target_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail=f"Target account {target_id} not found")

                # Insert paired transactions (outbound expense first, then inbound income)
                notes = item.notes or "Alokasi Gaji Bulanan"
                r_id = str(item.rule_id) if item.rule_id else None
                cur.execute(
                    """
                    INSERT INTO transactions (
                        user_id, account_id, category_id, type,
                        amount, notes, date, recurring_rule_id, kakeibo_type
                    )
                    VALUES (%s, %s, %s, 'expense', %s, %s, %s, %s, 'saving')
                    """,
                    (
                        user_id,
                        source_id,
                        exp_cat_id,
                        item.amount,
                        notes,
                        tx_dt,
                        r_id,
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO transactions (
                        user_id, account_id, category_id, type,
                        amount, notes, date, recurring_rule_id, kakeibo_type
                    )
                    VALUES (%s, %s, %s, 'income', %s, %s, %s, %s, NULL)
                    """,
                    (
                        user_id,
                        target_id,
                        inc_cat_id,
                        item.amount,
                        notes,
                        tx_dt,
                        r_id,
                    ),
                )

                # If rule_id provided, advance rule next_due_date
                if item.rule_id:
                    cur.execute(
                        "SELECT * FROM recurring_rules WHERE id = %s AND user_id = %s",
                        (str(item.rule_id), user_id),
                    )
                    rule = cur.fetchone()
                    if rule:
                        next_due = calculate_next_due_date(
                            schedule_type=rule["schedule_type"],
                            schedule_day=rule["schedule_day"],
                            user_payday_day=payday_day,
                            from_date=rule["next_due_date"],
                            advance=True,
                        )
                        cur.execute(
                            """
                            UPDATE recurring_rules
                            SET last_executed_at = NOW(), next_due_date = %s, updated_at = NOW()
                            WHERE id = %s
                            """,
                            (next_due, str(item.rule_id)),
                        )

    return {"ok": True, "allocated_items_count": len(payload.items)}
