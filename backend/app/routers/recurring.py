import calendar
from datetime import date, datetime, timedelta, timezone
from typing import Any, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.db.pool import db_conn
from app.routers.movements import _ensure_internal_movement_categories
from app.services.auth import get_current_user
from app.services.ledger_mutations import (
    create_bilateral_movement,
    ensure_generic_movement_accounts,
    ensure_sufficient_funds,
    lock_owned_accounts,
)

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


def _validate_schedule(schedule_type: str, schedule_day: int | None) -> int | None:
    if schedule_type == "payday":
        return None
    if schedule_day is None:
        raise HTTPException(status_code=422, detail="Schedule day is required")
    upper = 7 if schedule_type == "weekly" else 31
    if not 1 <= schedule_day <= upper:
        raise HTTPException(status_code=422, detail=f"Schedule day must be between 1 and {upper}")
    return schedule_day


def _validate_rule_references(
    cur,
    user_id: str,
    *,
    rule_type: str,
    source_id: str,
    target_id: str | None,
    category_id: str | None,
    obligation_id: str | None,
) -> None:
    if not source_id:
        raise HTTPException(status_code=422, detail="Source account is required")
    cur.execute(
        "SELECT id, type, instrument_type FROM accounts WHERE user_id = %s AND id = %s AND is_archived = FALSE",
        (user_id, source_id),
    )
    source_account = cur.fetchone()
    if not source_account:
        raise HTTPException(status_code=404, detail="Source account not found")
    if rule_type == "transfer":
        if not target_id:
            raise HTTPException(status_code=400, detail="Transfer requires target_account_id")
        if target_id == source_id:
            raise HTTPException(status_code=400, detail="Cannot transfer to the same account")
        if category_id:
            raise HTTPException(status_code=400, detail="Transfer categories are assigned by the movement service")
    elif target_id is not None:
        raise HTTPException(status_code=400, detail="Target account is only valid for transfers")
    if target_id:
        cur.execute(
            "SELECT id, type, instrument_type FROM accounts WHERE user_id = %s AND id = %s AND is_archived = FALSE",
            (user_id, target_id),
        )
        target_account = cur.fetchone()
        if not target_account:
            raise HTTPException(status_code=404, detail="Target account not found")
        if rule_type == "transfer":
            ensure_generic_movement_accounts(source_account, target_account)
    if category_id:
        cur.execute(
            "SELECT id, kind FROM categories WHERE user_id = %s AND id = %s AND is_archived = FALSE",
            (user_id, category_id),
        )
        category = cur.fetchone()
        if not category:
            raise HTTPException(status_code=404, detail="Category not found")
        if rule_type != "transfer" and category["kind"] != rule_type:
            raise HTTPException(status_code=400, detail="Category is incompatible with recurring type")
    elif rule_type != "transfer":
        raise HTTPException(status_code=422, detail="Category is required for income and expense rules")
    if obligation_id:
        if rule_type != "expense":
            raise HTTPException(status_code=400, detail="An obligation can only be linked to an expense rule")
        cur.execute(
            "SELECT id FROM obligations WHERE user_id = %s AND id = %s AND is_archived = FALSE AND remaining_amount > 0",
            (user_id, obligation_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Obligation not found")


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

    if not payload.name.strip():
        raise HTTPException(status_code=422, detail="Rule name is required")
    if payload.is_payroll_allocation and payload.type != "transfer":
        raise HTTPException(status_code=422, detail="Payroll allocation requires a transfer rule")

    source_id = str(payload.source_account_id)
    target_id = str(payload.target_account_id) if payload.target_account_id else None
    cat_id = str(payload.category_id) if payload.category_id else None
    ob_id = str(payload.obligation_id) if payload.obligation_id else None

    schedule_day = _validate_schedule(payload.schedule_type, payload.schedule_day)

    with db_conn() as conn:
        with conn.cursor() as cur:
            _validate_rule_references(
                cur,
                user_id,
                rule_type=payload.type,
                source_id=source_id,
                target_id=target_id,
                category_id=cat_id,
                obligation_id=ob_id,
            )

            next_due = calculate_next_due_date(
                schedule_type=payload.schedule_type,
                schedule_day=schedule_day,
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
                    payload.name.strip(),
                    payload.type,
                    payload.amount,
                    source_id,
                    target_id,
                    cat_id,
                    ob_id,
                    payload.schedule_type,
                    schedule_day,
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
@router.patch("/{rule_id}")
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
            target_id = (
                str(payload.target_account_id) if payload.target_account_id is not None else None
            ) if "target_account_id" in payload.model_fields_set else (
                str(existing["target_account_id"]) if existing["target_account_id"] else None
            )
            cat_id = (
                str(payload.category_id) if payload.category_id is not None else None
            ) if "category_id" in payload.model_fields_set else (
                str(existing["category_id"]) if existing["category_id"] else None
            )
            ob_id = (
                str(payload.obligation_id) if payload.obligation_id is not None else None
            ) if "obligation_id" in payload.model_fields_set else (
                str(existing["obligation_id"]) if existing["obligation_id"] else None
            )
            stype = payload.schedule_type if payload.schedule_type is not None else existing["schedule_type"]
            sday = payload.schedule_day if "schedule_day" in payload.model_fields_set else existing["schedule_day"]
            notes = payload.notes if "notes" in payload.model_fields_set else existing["notes"]
            is_payroll = payload.is_payroll_allocation if payload.is_payroll_allocation is not None else existing["is_payroll_allocation"]
            auto_post = payload.auto_post if payload.auto_post is not None else existing["auto_post"]
            is_active = payload.is_active if payload.is_active is not None else existing["is_active"]

            if not name.strip():
                raise HTTPException(status_code=422, detail="Rule name is required")
            if rtype != "transfer":
                target_id = None
                is_payroll = False
            else:
                if "category_id" in payload.model_fields_set and payload.category_id is not None:
                    raise HTTPException(status_code=400, detail="Transfer categories are assigned by the movement service")
                cat_id = None
            if rtype != "expense":
                ob_id = None
            sday = _validate_schedule(stype, sday)
            _validate_rule_references(
                cur,
                user_id,
                rule_type=rtype,
                source_id=source_id,
                target_id=target_id,
                category_id=cat_id,
                obligation_id=ob_id,
            )

            # Recompute next_due_date if schedule changed
            next_due = existing["next_due_date"]
            if "schedule_type" in payload.model_fields_set or "schedule_day" in payload.model_fields_set:
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


def _execute_rule_occurrence(
    cur,
    rule: dict[str, Any],
    user_id: str,
    payday_day: int,
    scheduled_for: date,
) -> dict[str, Any]:
    rule_id = str(rule["id"])
    cur.execute(
        """
        INSERT INTO recurring_executions (
            recurring_rule_id, user_id, scheduled_for, status
        ) VALUES (%s, %s, %s, 'processing')
        ON CONFLICT (recurring_rule_id, scheduled_for) DO NOTHING
        RETURNING id
        """,
        (rule_id, user_id, scheduled_for),
    )
    claimed = cur.fetchone()
    if not claimed:
        cur.execute(
            """
            SELECT id, status, transaction_id, movement_id
            FROM recurring_executions
            WHERE recurring_rule_id = %s AND scheduled_for = %s
            FOR UPDATE
            """,
            (rule_id, scheduled_for),
        )
        occurrence = cur.fetchone()
        if occurrence and occurrence["status"] in {"processing", "succeeded"}:
            return {
                "rule_id": rule_id,
                "scheduled_for": scheduled_for.isoformat(),
                "status": occurrence["status"],
                "idempotent": True,
            }
        cur.execute(
            """
            UPDATE recurring_executions
            SET status = 'processing', error_code = NULL, error_detail = NULL,
                updated_at = NOW()
            WHERE id = %s
            """,
            (occurrence["id"],),
        )
        occurrence_id = str(occurrence["id"])
    else:
        occurrence_id = str(claimed["id"])

    cur.execute("SAVEPOINT recurring_effect")
    try:
        source_id = str(rule["source_account_id"])
        target_id = str(rule["target_account_id"]) if rule.get("target_account_id") else None
        _validate_rule_references(
            cur,
            user_id,
            rule_type=rule["type"],
            source_id=source_id,
            target_id=target_id,
            category_id=str(rule["category_id"]) if rule.get("category_id") else None,
            obligation_id=str(rule["obligation_id"]) if rule.get("obligation_id") else None,
        )
        tx_dt = datetime(
            scheduled_for.year,
            scheduled_for.month,
            scheduled_for.day,
            12,
            0,
            tzinfo=timezone.utc,
        )
        notes = rule.get("notes") or f"Rutin: {rule['name']}"
        transaction_id = None
        movement_id = None

        if rule["type"] == "transfer":
            locked = lock_owned_accounts(cur, user_id, [source_id, target_id])
            expense_category_id, income_category_id = _ensure_internal_movement_categories(cur, user_id)
            movement = create_bilateral_movement(
                cur,
                user_id=user_id,
                source_id=source_id,
                target_id=target_id,
                amount=int(rule["amount"]),
                notes=notes,
                tx_date=tx_dt,
                expense_category_id=expense_category_id,
                income_category_id=income_category_id,
                source_account=locked[source_id],
                target_account=locked[target_id],
                idempotency_key=f"recurring:{rule_id}:{scheduled_for.isoformat()}",
                recurring_rule_id=rule_id,
                obligation_id=str(rule["obligation_id"]) if rule.get("obligation_id") else None,
            )
            transaction_id = movement["expense_transaction_id"]
            movement_id = movement["movement_id"]
        else:
            locked = lock_owned_accounts(cur, user_id, [source_id])
            if rule["type"] == "expense":
                ensure_sufficient_funds(cur, user_id, locked[source_id], int(rule["amount"]))
            cur.execute(
                """
                INSERT INTO transactions (
                    user_id, account_id, category_id, obligation_id, type,
                    amount, notes, date, recurring_rule_id, idempotency_key
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    user_id,
                    source_id,
                    rule["category_id"],
                    rule["obligation_id"],
                    rule["type"],
                    rule["amount"],
                    notes,
                    tx_dt,
                    rule_id,
                    f"recurring:{rule_id}:{scheduled_for.isoformat()}",
                ),
            )
            transaction_id = str(cur.fetchone()["id"])

        if rule.get("obligation_id") and rule["type"] == "expense":
            cur.execute(
                """
                UPDATE obligations
                SET remaining_amount = GREATEST(0, remaining_amount - %s),
                    is_archived = (remaining_amount - %s) <= 0,
                    updated_at = NOW()
                WHERE id = %s AND user_id = %s
                """,
                (rule["amount"], rule["amount"], rule["obligation_id"], user_id),
            )

        next_due = calculate_next_due_date(
            schedule_type=rule["schedule_type"],
            schedule_day=rule["schedule_day"],
            user_payday_day=payday_day,
            from_date=scheduled_for,
            advance=True,
        )
        cur.execute(
            """
            UPDATE recurring_rules
            SET last_executed_at = NOW(), next_due_date = %s, updated_at = NOW()
            WHERE id = %s AND user_id = %s
            """,
            (next_due, rule_id, user_id),
        )
        cur.execute(
            """
            UPDATE recurring_executions
            SET status = 'succeeded', transaction_id = %s, movement_id = %s,
                error_code = NULL, error_detail = NULL, updated_at = NOW()
            WHERE id = %s
            """,
            (transaction_id, movement_id, occurrence_id),
        )
        cur.execute("RELEASE SAVEPOINT recurring_effect")
        return {
            "rule_id": rule_id,
            "scheduled_for": scheduled_for.isoformat(),
            "status": "succeeded",
            "transaction_id": transaction_id,
            "movement_id": movement_id,
        }
    except HTTPException as exc:
        cur.execute("ROLLBACK TO SAVEPOINT recurring_effect")
        detail = exc.detail if isinstance(exc.detail, str) else exc.detail.get("code", "execution_failed")
        code = exc.detail.get("code", "execution_failed") if isinstance(exc.detail, dict) else "execution_failed"
        cur.execute(
            """
            UPDATE recurring_executions
            SET status = 'failed', error_code = %s, error_detail = %s, updated_at = NOW()
            WHERE id = %s
            """,
            (code, str(detail)[:1000], occurrence_id),
        )
        cur.execute("RELEASE SAVEPOINT recurring_effect")
        return {
            "rule_id": rule_id,
            "scheduled_for": scheduled_for.isoformat(),
            "status": "failed",
            "error_code": code,
        }


def process_due_recurring_rules(*, user_id: str | None = None, limit: int = 20) -> dict[str, Any]:
    today = date.today()
    results = []
    with db_conn() as conn:
        with conn.cursor() as cur:
            conditions = ["r.is_active = TRUE", "r.auto_post = TRUE", "r.next_due_date <= %s"]
            params: list[Any] = [today]
            if user_id:
                conditions.append("r.user_id = %s")
                params.append(user_id)
            cur.execute(
                f"""
                SELECT r.*, u.payday_day
                FROM recurring_rules r
                JOIN users u ON u.id = r.user_id
                WHERE {' AND '.join(conditions)}
                ORDER BY r.next_due_date, r.id
                FOR UPDATE OF r SKIP LOCKED
                LIMIT %s
                """,
                (*params, max(1, min(limit, 100))),
            )
            rules = cur.fetchall()
            for rule in rules:
                results.append(
                    _execute_rule_occurrence(
                        cur,
                        rule,
                        str(rule["user_id"]),
                        int(rule.get("payday_day") or 25),
                        rule["next_due_date"],
                    )
                )
            conn.commit()
    succeeded = [result["rule_id"] for result in results if result["status"] == "succeeded"]
    return {
        "processed_count": len(results),
        "executed_rule_ids": succeeded,
        "results": results,
    }


@router.post("/process-due")
def process_auto_post_due_rules(
    current_user: dict = Depends(get_current_user),
):
    result = process_due_recurring_rules(user_id=current_user["id"], limit=20)
    return {"ok": True, **result}


@router.post("/execute")
def execute_selected_rules(
    payload: ExecuteRulesRequest,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    payday_day = int(current_user.get("payday_day") or 25)
    exec_date = payload.execution_date or date.today()
    results = []
    with db_conn() as conn:
        with conn.cursor() as cur:
            for r_id in payload.rule_ids:
                cur.execute(
                    "SELECT * FROM recurring_rules WHERE id = %s AND user_id = %s FOR UPDATE",
                    (str(r_id), user_id),
                )
                rule = cur.fetchone()
                if not rule:
                    raise HTTPException(status_code=404, detail="Recurring rule not found")
                if not rule["is_active"]:
                    raise HTTPException(status_code=409, detail="Inactive recurring rules cannot be executed")
                results.append(_execute_rule_occurrence(cur, rule, user_id, payday_day, exec_date))
            conn.commit()

    return {
        "ok": True,
        "executed_count": len([result for result in results if result["status"] == "succeeded"]),
        "results": results,
    }


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
            exp_cat_id, inc_cat_id = _ensure_internal_movement_categories(cur, user_id)
            account_ids = [str(item.source_account_id) for item in payload.items]
            account_ids.extend(str(item.target_account_id) for item in payload.items)
            locked = lock_owned_accounts(cur, user_id, account_ids)
            required_by_source: dict[str, int] = {}
            rules_by_id: dict[str, dict[str, Any]] = {}
            for item in payload.items:
                source_id = str(item.source_account_id)
                target_id = str(item.target_account_id)
                if source_id == target_id:
                    raise HTTPException(status_code=400, detail="Payroll source and target must be different")
                ensure_generic_movement_accounts(locked[source_id], locked[target_id])
                required_by_source[source_id] = required_by_source.get(source_id, 0) + item.amount
                if item.rule_id:
                    if str(item.rule_id) in rules_by_id:
                        raise HTTPException(status_code=422, detail="A payroll rule can only be selected once per batch")
                    cur.execute(
                        """
                        SELECT * FROM recurring_rules
                        WHERE id = %s AND user_id = %s AND is_active = TRUE
                          AND is_payroll_allocation = TRUE
                        FOR UPDATE
                        """,
                        (str(item.rule_id), user_id),
                    )
                    rule = cur.fetchone()
                    if not rule:
                        raise HTTPException(status_code=404, detail="Payroll recurring rule not found")
                    if str(rule["source_account_id"]) != source_id or str(rule["target_account_id"]) != target_id:
                        raise HTTPException(status_code=400, detail="Payroll item accounts do not match the rule")
                    rules_by_id[str(item.rule_id)] = rule

            for source_id, required in required_by_source.items():
                ensure_sufficient_funds(cur, user_id, locked[source_id], required)

            results = []
            for item in payload.items:
                source_id = str(item.source_account_id)
                target_id = str(item.target_account_id)
                notes = item.notes or "Alokasi Gaji Bulanan"
                r_id = str(item.rule_id) if item.rule_id else None
                movement = create_bilateral_movement(
                    cur,
                    user_id=user_id,
                    source_id=source_id,
                    target_id=target_id,
                    amount=item.amount,
                    notes=notes,
                    tx_date=tx_dt,
                    expense_category_id=exp_cat_id,
                    income_category_id=inc_cat_id,
                    source_account=locked[source_id],
                    target_account=locked[target_id],
                    idempotency_key=f"payroll:{r_id}:{exec_date.isoformat()}" if r_id else None,
                    recurring_rule_id=r_id,
                )
                results.append({"rule_id": r_id, **movement})

                # If rule_id provided, advance rule next_due_date
                if item.rule_id:
                    rule = rules_by_id[str(item.rule_id)]
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
                        WHERE id = %s AND user_id = %s
                        """,
                        (next_due, str(item.rule_id), user_id),
                    )
            conn.commit()

    return {"ok": True, "allocated_items_count": len(payload.items), "results": results}
