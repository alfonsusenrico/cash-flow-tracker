from datetime import date, datetime, timezone
from typing import Any, List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from app.db.pool import db_conn
from app.errors import ErrorCode, api_error
from app.i18n import account_language, t
from app.routers.movements import _ensure_internal_movement_categories
from app.services.auth import get_current_user
from app.services.investment_topups import create_topup, resolve_topup_source, validate_topup_accounts
from app.services.payment_links import CANDIDATE_LIMIT, lock_payment, occurrence_candidates
from app.services.plan_occurrences import (
    advance_rule,
    calculate_next_due_date,
    link_transaction,
    next_unresolved_due,
    recent_auto_links,
    request_plan_linking,
    unlink_occurrence,
)
from app.services.provenance import system_provenance
from app.services.ledger_mutations import (
    create_bilateral_movement,
    ensure_generic_movement_accounts,
    ensure_sufficient_funds,
    lock_owned_accounts,
)

router = APIRouter(tags=["Recurring"])


# --- Schemas ---

class RecurringRuleCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    type: str = Field(pattern="^(expense|income|transfer|investment_topup)$")
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
    type: Optional[str] = Field(default=None, pattern="^(expense|income|transfer|investment_topup)$")
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


def _reject_retired_options(*, auto_post: bool | None, is_payroll_allocation: bool | None, new_payday_schedule: bool) -> None:
    """Old clients can still send these fields; enabling them must fail rather than be ignored."""
    if auto_post:
        raise api_error(422, ErrorCode.AUTO_POST_RETIRED)
    if is_payroll_allocation:
        raise api_error(422, ErrorCode.PAYROLL_ALLOCATION_RETIRED)
    if new_payday_schedule:
        raise api_error(422, ErrorCode.PAYDAY_SCHEDULE_RETIRED)


def _validate_schedule(schedule_type: str, schedule_day: int | None) -> int | None:
    if schedule_type == "payday":
        return None
    if schedule_day is None:
        raise api_error(422, ErrorCode.SCHEDULE_DAY_REQUIRED)
    upper = 7 if schedule_type == "weekly" else 31
    if not 1 <= schedule_day <= upper:
        raise api_error(422, ErrorCode.SCHEDULE_DAY_OUT_OF_RANGE, {"max": upper})
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
        raise api_error(422, ErrorCode.SOURCE_ACCOUNT_REQUIRED)
    if rule_type == "investment_topup":
        if not target_id or category_id or obligation_id:
            raise api_error(422, ErrorCode.INVALID_INVESTMENT_TOPUP_RULE)
        effective_source = resolve_topup_source(cur, user_id, source_id)
        cur.execute(
            """SELECT a.*, EXISTS (SELECT 1 FROM accounts child WHERE child.parent_id = a.id) AS has_children
               FROM accounts a WHERE user_id = %s AND id = ANY(%s) AND is_archived = FALSE""",
            (user_id, [effective_source, target_id]),
        )
        accounts = {str(row["id"]): row for row in cur.fetchall()}
        if effective_source not in accounts or target_id not in accounts:
            raise api_error(404, ErrorCode.ACCOUNT_NOT_FOUND)
        validate_topup_accounts(accounts[effective_source], accounts[target_id])
        return
    cur.execute(
        "SELECT id, type, instrument_type FROM accounts WHERE user_id = %s AND id = %s AND is_archived = FALSE",
        (user_id, source_id),
    )
    source_account = cur.fetchone()
    if not source_account:
        raise api_error(404, ErrorCode.SOURCE_ACCOUNT_NOT_FOUND)
    if rule_type == "transfer":
        if not target_id:
            raise api_error(400, ErrorCode.TARGET_ACCOUNT_REQUIRED)
        if target_id == source_id:
            raise api_error(400, ErrorCode.SAME_ACCOUNT)
        if category_id:
            raise api_error(400, ErrorCode.TRANSFER_CATEGORY_NOT_ALLOWED)
    elif target_id is not None:
        raise api_error(400, ErrorCode.TARGET_ACCOUNT_TRANSFER_ONLY)
    if target_id:
        cur.execute(
            "SELECT id, type, instrument_type FROM accounts WHERE user_id = %s AND id = %s AND is_archived = FALSE",
            (user_id, target_id),
        )
        target_account = cur.fetchone()
        if not target_account:
            raise api_error(404, ErrorCode.TARGET_ACCOUNT_NOT_FOUND)
        if rule_type == "transfer":
            ensure_generic_movement_accounts(source_account, target_account)
    if category_id:
        cur.execute(
            "SELECT id, kind FROM categories WHERE user_id = %s AND id = %s AND is_archived = FALSE",
            (user_id, category_id),
        )
        category = cur.fetchone()
        if not category:
            raise api_error(404, ErrorCode.CATEGORY_NOT_FOUND)
        if rule_type != "transfer" and category["kind"] != rule_type:
            raise api_error(400, ErrorCode.CATEGORY_TYPE_MISMATCH)
    elif rule_type != "transfer":
        raise api_error(422, ErrorCode.CATEGORY_REQUIRED)
    if obligation_id:
        if rule_type != "expense":
            raise api_error(400, ErrorCode.OBLIGATION_EXPENSE_ONLY)
        cur.execute(
            "SELECT id FROM obligations WHERE user_id = %s AND id = %s AND is_archived = FALSE AND remaining_amount > 0",
            (user_id, obligation_id),
        )
        if not cur.fetchone():
            raise api_error(404, ErrorCode.OBLIGATION_NOT_FOUND)


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
        "legacy_auto_post": r.get("legacy_auto_post"),
        "converted_to_expectation_at": r["converted_to_expectation_at"].isoformat() if r.get("converted_to_expectation_at") else None,
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
        raise api_error(422, ErrorCode.RULE_NAME_REQUIRED)
    _reject_retired_options(
        auto_post=payload.auto_post,
        is_payroll_allocation=payload.is_payroll_allocation,
        new_payday_schedule=payload.schedule_type == "payday",
    )

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
                    False,
                    False,
                    next_due,
                    payload.is_active,
                ),
            )
            rule_id = str(cur.fetchone()["id"])

    request_plan_linking(user_id)
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
                "SELECT * FROM recurring_rules WHERE id = %s AND user_id = %s FOR UPDATE",
                (str(rule_id), user_id),
            )
            existing = cur.fetchone()
            if not existing:
                raise api_error(404, ErrorCode.PLAN_NOT_FOUND)

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
            _reject_retired_options(
                auto_post=payload.auto_post,
                is_payroll_allocation=payload.is_payroll_allocation,
                new_payday_schedule=stype == "payday" and existing["schedule_type"] != "payday",
            )
            # The historical payroll flag can only be cleared.
            is_payroll = existing["is_payroll_allocation"] and payload.is_payroll_allocation is not False
            is_active = payload.is_active if payload.is_active is not None else existing["is_active"]

            if not name.strip():
                raise api_error(422, ErrorCode.RULE_NAME_REQUIRED)
            if rtype == "investment_topup":
                if is_payroll or cat_id or ob_id:
                    raise api_error(422, ErrorCode.INVALID_INVESTMENT_TOPUP_RULE)
            elif rtype != "transfer":
                target_id = None
                is_payroll = False
            else:
                if "category_id" in payload.model_fields_set and payload.category_id is not None:
                    raise api_error(400, ErrorCode.TRANSFER_CATEGORY_NOT_ALLOWED)
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
                    notes = %s, is_payroll_allocation = %s, auto_post = FALSE, next_due_date = %s,
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
                    next_due,
                    is_active,
                    str(rule_id),
                    user_id,
                ),
            )

    request_plan_linking(user_id)
    return {"ok": True, "id": str(rule_id), "next_due_date": next_due.isoformat() if next_due else None}


@router.delete("/{rule_id}")
def delete_recurring_rule(
    rule_id: UUID,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM recurring_rules WHERE id = %s AND user_id = %s FOR UPDATE", (str(rule_id), user_id))
            if not cur.fetchone():
                raise api_error(404, ErrorCode.PLAN_NOT_FOUND)
            cur.execute(
                """
                SELECT EXISTS (SELECT 1 FROM recurring_executions WHERE recurring_rule_id = %s)
                    OR EXISTS (SELECT 1 FROM transactions WHERE recurring_rule_id = %s) AS has_history
                """,
                (str(rule_id), str(rule_id)),
            )
            if cur.fetchone()["has_history"]:
                # Deleting would cascade away occurrence history; deactivating keeps it.
                raise api_error(409, ErrorCode.EXPECTATION_HAS_HISTORY)
            cur.execute("DELETE FROM recurring_rules WHERE id = %s AND user_id = %s", (str(rule_id), user_id))
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
                raise api_error(404, ErrorCode.PLAN_NOT_FOUND)
            return {"ok": True, "id": str(rule_id), "is_active": row["is_active"]}


# --- Engine: Pending & Due Execution ---

@router.get("/pending")
def list_pending_recurring_rules(
    current_user: dict = Depends(get_current_user),
):
    """Due expectations with recorded transactions that might satisfy them; nothing is posted."""
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
        ORDER BY r.next_due_date ASC
    """

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (user_id, today))
            rows = cur.fetchall()
            rules = []
            for row in rows:
                item = format_recurring_rule_row(row)
                item.update(occurrence_candidates(cur, user_id, row, row["next_due_date"]))
                rules.append(item)
            return {
                "ok": True,
                "pending_count": len(rules),
                "rules": rules,
                "recent_auto_links": recent_auto_links(cur, user_id),
            }


def _execute_rule_occurrence(
    cur,
    rule: dict[str, Any],
    user_id: str,
    payday_day: int,
    scheduled_for: date,
    actual_date: date | None = None,
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
        if occurrence and occurrence["status"] in {"processing", "succeeded", "linked", "skipped"}:
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
        ledger_date = actual_date or scheduled_for
        tx_dt = datetime(
            ledger_date.year,
            ledger_date.month,
            ledger_date.day,
            12,
            0,
            tzinfo=timezone.utc,
        )
        # Written in the owner's language when the job records it (D1, D6).
        notes = rule.get("notes") or t(account_language(cur, user_id)[0], "notes.recurring", plan=rule["name"])
        transaction_id = None
        movement_id = None

        if rule["type"] == "investment_topup":
            if rule["auto_post"] or rule["is_payroll_allocation"]:
                raise api_error(422, ErrorCode.INVESTMENT_CONFIRMATION_REQUIRED)
            topup = create_topup(
                cur, user_id=user_id, source_id=source_id, target_id=target_id,
                amount=int(rule["amount"]), tx_date=tx_dt, notes=notes,
                idempotency_key=f"recurring:{rule_id}:{scheduled_for.isoformat()}",
                recurring_rule_id=rule_id, recurring_execution_id=occurrence_id, ledger_source="recurring",
            )["topup"]
            transaction_id = topup["expense_transaction_id"]
            movement_id = topup["movement_id"]
        elif rule["type"] == "transfer":
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
                provenance=system_provenance("recurring"),
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
                    amount, notes, date, recurring_rule_id, idempotency_key,
                    source, review_status, classified_by, classification_confidence
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    *system_provenance("recurring").columns(),
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

        next_due = next_unresolved_due(cur, rule, payday_day, scheduled_for)
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




def _retired(code: str, message_key: str | None = None) -> HTTPException:
    return api_error(410, code, message_key=message_key)


@router.post("/process-due")
def process_auto_post_due_rules(current_user: dict = Depends(get_current_user)):
    raise _retired("scheduled_posting_retired")


@router.post("/execute")
def execute_selected_rules(current_user: dict = Depends(get_current_user)):
    raise _retired("scheduled_posting_retired", "plan_posting_retired")


@router.post("/payroll/execute")
def execute_payroll_batch(current_user: dict = Depends(get_current_user)):
    raise _retired("payroll_execution_retired")


class OccurrenceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    scheduled_for: date


class OccurrenceLinkRequest(OccurrenceRequest):
    transaction_id: UUID


class OccurrenceRecordRequest(OccurrenceRequest):
    execution_date: Optional[date] = None
    acknowledged_candidate_ids: List[UUID] = Field(default_factory=list, max_length=CANDIDATE_LIMIT)


def _lock_rule(cur, user_id: str, rule_id: UUID) -> dict[str, Any]:
    cur.execute("SELECT * FROM recurring_rules WHERE id = %s AND user_id = %s FOR UPDATE", (str(rule_id), user_id))
    rule = cur.fetchone()
    if not rule:
        raise api_error(404, ErrorCode.PLAN_NOT_FOUND)
    return rule


def _occurrence(cur, rule_id: str, scheduled_for: date) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT id, status, transaction_id FROM recurring_executions
        WHERE recurring_rule_id = %s AND scheduled_for = %s FOR UPDATE
        """,
        (rule_id, scheduled_for),
    )
    return cur.fetchone()


def _require_next_occurrence(rule: dict[str, Any], scheduled_for: date) -> None:
    if not rule["is_active"]:
        raise api_error(409, ErrorCode.EXPECTATION_INACTIVE)
    if scheduled_for != rule["next_due_date"]:
        raise api_error(409, ErrorCode.INVALID_SCHEDULED_OCCURRENCE)


@router.get("/{rule_id}/occurrences")
def list_rule_occurrences(rule_id: UUID, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM recurring_rules WHERE id = %s AND user_id = %s", (str(rule_id), user_id))
            if not cur.fetchone():
                raise api_error(404, ErrorCode.PLAN_NOT_FOUND)
            cur.execute(
                """
                SELECT scheduled_for, status, transaction_id, error_code, link_method, updated_at
                FROM recurring_executions WHERE recurring_rule_id = %s AND user_id = %s
                ORDER BY scheduled_for DESC LIMIT 24
                """,
                (str(rule_id), user_id),
            )
            return {
                "ok": True,
                "occurrences": [
                    {
                        "scheduled_for": row["scheduled_for"].isoformat(),
                        "status": row["status"],
                        "transaction_id": str(row["transaction_id"]) if row["transaction_id"] else None,
                        "error_code": row["error_code"],
                        "link_method": row["link_method"],
                        "updated_at": row["updated_at"].isoformat(),
                    }
                    for row in cur.fetchall()
                ],
            }


@router.post("/{rule_id}/occurrences/link")
def link_occurrence(rule_id: UUID, payload: OccurrenceLinkRequest, current_user: dict = Depends(get_current_user)):
    """Mark an occurrence as met by a transaction that was already recorded."""
    user_id = current_user["id"]
    payday_day = int(current_user.get("payday_day") or 25)
    transaction_id = str(payload.transaction_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            rule = _lock_rule(cur, user_id, rule_id)
            existing = _occurrence(cur, str(rule_id), payload.scheduled_for)
            if existing and existing["status"] == "linked" and str(existing["transaction_id"]) == transaction_id:
                return {"ok": True, "changed": False, "next_due_date": rule["next_due_date"].isoformat()}
            relink = bool(existing and existing["status"] == "linked" and existing["transaction_id"] is None)
            if existing and existing["status"] != "failed" and not relink:
                raise api_error(409, ErrorCode.OCCURRENCE_ALREADY_RESOLVED)
            if not relink:
                _require_next_occurrence(rule, payload.scheduled_for)
            transaction = lock_payment(cur, user_id, transaction_id)
            if transaction["date"] > datetime.now(timezone.utc):
                raise api_error(422, ErrorCode.FUTURE_TRANSACTION)
            expected_type = "expense" if rule["type"] in {"transfer", "investment_topup"} else rule["type"]
            if transaction["type"] != expected_type:
                raise api_error(422, ErrorCode.TRANSACTION_TYPE_MISMATCH)
            debt_changed = link_transaction(cur, user_id, rule, payload.scheduled_for, transaction, method="owner")
            next_due = rule["next_due_date"]
            if not relink:
                next_due = advance_rule(cur, rule, user_id, payday_day, payload.scheduled_for)
            conn.commit()
    return {"ok": True, "changed": True, "debt_changed": debt_changed, "next_due_date": next_due.isoformat()}


@router.post("/{rule_id}/occurrences/unlink")
def unlink_rule_occurrence(rule_id: UUID, payload: OccurrenceRequest, current_user: dict = Depends(get_current_user)):
    """Undo a link; the occurrence is due again and this pair is never linked automatically again."""
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            rule = _lock_rule(cur, user_id, rule_id)
            result = unlink_occurrence(cur, user_id, rule, payload.scheduled_for)
            conn.commit()
    return {"ok": True, **result}


@router.post("/{rule_id}/occurrences/skip")
def skip_occurrence(rule_id: UUID, payload: OccurrenceRequest, current_user: dict = Depends(get_current_user)):
    """The owner states that this occurrence did not happen; nothing is recorded."""
    user_id = current_user["id"]
    payday_day = int(current_user.get("payday_day") or 25)
    with db_conn() as conn:
        with conn.cursor() as cur:
            rule = _lock_rule(cur, user_id, rule_id)
            existing = _occurrence(cur, str(rule_id), payload.scheduled_for)
            if existing and existing["status"] == "skipped":
                return {"ok": True, "changed": False, "next_due_date": rule["next_due_date"].isoformat()}
            if existing and existing["status"] != "failed":
                raise api_error(409, ErrorCode.OCCURRENCE_ALREADY_RESOLVED)
            _require_next_occurrence(rule, payload.scheduled_for)
            cur.execute(
                """
                INSERT INTO recurring_executions (recurring_rule_id, user_id, scheduled_for, status)
                VALUES (%s, %s, %s, 'skipped')
                ON CONFLICT (recurring_rule_id, scheduled_for) DO UPDATE
                SET status = 'skipped', error_code = NULL, error_detail = NULL, updated_at = NOW()
                """,
                (str(rule_id), user_id, payload.scheduled_for),
            )
            next_due = advance_rule(cur, rule, user_id, payday_day, payload.scheduled_for)
            conn.commit()
    return {"ok": True, "changed": True, "next_due_date": next_due.isoformat()}


@router.post("/{rule_id}/occurrences/record")
def record_occurrence(rule_id: UUID, payload: OccurrenceRecordRequest, current_user: dict = Depends(get_current_user)):
    """Explicitly record one missed occurrence, after the owner has ruled out captured candidates."""
    user_id = current_user["id"]
    payday_day = int(current_user.get("payday_day") or 25)
    today = date.today()
    execution_date = payload.execution_date or today
    with db_conn() as conn:
        with conn.cursor() as cur:
            rule = _lock_rule(cur, user_id, rule_id)
            existing = _occurrence(cur, str(rule_id), payload.scheduled_for)
            if existing and existing["status"] in {"succeeded", "linked", "skipped", "processing"}:
                return {
                    "ok": True,
                    "result": {"rule_id": str(rule_id), "scheduled_for": payload.scheduled_for.isoformat(), "status": existing["status"], "idempotent": True},
                }
            _require_next_occurrence(rule, payload.scheduled_for)
            if payload.scheduled_for > today or execution_date > today:
                raise api_error(409, ErrorCode.INVALID_SCHEDULED_OCCURRENCE, message_key="occurrence_not_yet_due")
            found = occurrence_candidates(cur, user_id, rule, payload.scheduled_for)["candidates"]
            acknowledged = {str(item) for item in payload.acknowledged_candidate_ids}
            # Same-amount captures are the plausible duplicates; other suggestions do not block.
            unreviewed = [item for item in found if item["exact_amount"] and item["id"] not in acknowledged]
            if unreviewed:
                raise api_error(409, ErrorCode.POSSIBLE_DUPLICATE, extra={"candidates": unreviewed})
            result = _execute_rule_occurrence(cur, rule, user_id, payday_day, payload.scheduled_for, actual_date=execution_date)
            conn.commit()
    if result["status"] == "failed":
        raise api_error(409, result["error_code"], message_key="occurrence_not_recorded")
    return {"ok": True, "result": result}
