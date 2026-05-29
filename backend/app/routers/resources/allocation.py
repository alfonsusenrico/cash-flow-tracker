"""Allocation Plans + Items CRUD — /allocation-plans and /v1/allocation-plans"""
import uuid
import threading
import time
from typing import Any
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Request
from app.db.pool import db_conn
from app.services.ledger.balances import (
    ensure_account_non_negative,
    get_account_balances,
    lock_accounts_for_update,
    parse_uuid_value,
)
from app.services.ledger.cache import invalidate_user_cache
from app.services.ledger.period import (
    clamp_day,
    current_month_local,
    local_day_start_utc,
    now_utc,
    parse_month,
)

router = APIRouter(tags=["allocation"])
_scheduler_started = False


def _resolve_planned_amount(mode: str, value: float, expected_income: int) -> int:
    if mode == "fixed":
        return int(value)
    if mode == "percent":
        return int(round(expected_income * value / 100))
    return 0


def _validate_allocation_value(mode: str, value: float) -> None:
    if value < 0:
        raise HTTPException(status_code=400, detail="value must be >= 0")
    if mode == "percent" and value > 100:
        raise HTTPException(status_code=400, detail="percentage must be between 0 and 100")


TARGET_EMERGENCY_MONTHS = 6


def _allocation_group(bucket_kind: str | None, mode: str) -> str:
    if bucket_kind == "investment":
        return "investment"
    if bucket_kind == "emergency":
        return "emergency_buffer"
    if bucket_kind == "goal":
        return "goals_savings"
    if bucket_kind == "sinking":
        return "cash_buffer"
    if mode == "fixed":
        return "fixed_spending"
    return "dynamic_spending"


def _emergency_bucket_balance(cur, username: str) -> int:
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
        WHERE u.username=%s AND b.kind='emergency' AND b.is_archived=FALSE
        GROUP BY b.bucket_id
        """,
        (username,),
    )
    total = 0
    for row in cur.fetchall():
        for account_id in row.get("linked_account_ids") or []:
            total += int(balances.get(account_id, 0))
    return total


def _allocation_health(cur, username: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    emergency_base_items = [
        {
            "item_id": item.get("item_id"),
            "label": item.get("label"),
            "planned_amount": int(item.get("planned_amount") or 0),
            "include_in_emergency_base": bool(item.get("include_in_emergency_base", False)),
            "bucket_kind": item.get("bucket_kind"),
            "bucket_name": item.get("bucket_name"),
            "group": item.get("group") or _allocation_group(item.get("bucket_kind"), item.get("mode", "fixed")),
        }
        for item in items
    ]
    monthly_need = sum(
        item["planned_amount"]
        for item in emergency_base_items
        if item["include_in_emergency_base"]
    )
    current_amount = _emergency_bucket_balance(cur, username)
    target_amount = monthly_need * TARGET_EMERGENCY_MONTHS
    gap = max(target_amount - current_amount, 0)
    coverage_months = round(current_amount / monthly_need, 1) if monthly_need > 0 else None
    if monthly_need <= 0 or current_amount >= target_amount:
        status = "ok"
    elif coverage_months is not None and coverage_months >= 3:
        status = "warn"
    else:
        status = "critical"
    return {
        "emergency_fund": {
            "target_months": TARGET_EMERGENCY_MONTHS,
            "monthly_need": monthly_need,
            "target_amount": target_amount,
            "current_amount": current_amount,
            "gap": gap,
            "coverage_months": coverage_months,
            "status": status,
            "baseline_items": emergency_base_items,
        }
    }


def _validate_bucket(cur, username: str, bucket_id: str | None) -> str | None:
    if not bucket_id:
        return None
    bucket_id = parse_uuid_value(bucket_id, "bucket_id")
    cur.execute(
        """
        SELECT 1
        FROM buckets b
        JOIN users u ON u.user_id = b.user_id
        WHERE u.username=%s AND b.bucket_id=%s::uuid AND b.is_archived=FALSE
        """,
        (username, bucket_id),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="Bucket not found")
    return bucket_id


def _bucket_kind(cur, username: str, bucket_id: str | None) -> str | None:
    if not bucket_id:
        return None
    cur.execute(
        """
        SELECT b.kind
        FROM buckets b
        JOIN users u ON u.user_id = b.user_id
        WHERE u.username=%s AND b.bucket_id=%s::uuid AND b.is_archived=FALSE
        """,
        (username, bucket_id),
    )
    row = cur.fetchone()
    return row["kind"] if row else None


def _default_include_in_emergency_base(bucket_kind: str | None, mode: str) -> bool:
    return _allocation_group(bucket_kind, mode) in {"fixed_spending", "dynamic_spending"}


def _validate_account(cur, username: str, account_id: str | None, field_name: str) -> str | None:
    if not account_id:
        return None
    account_id = parse_uuid_value(account_id, field_name)
    cur.execute(
        "SELECT account_id::text FROM accounts WHERE username=%s AND account_id=%s::uuid",
        (username, account_id),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail=f"{field_name} not found")
    return account_id


def _default_bucket_account(cur, username: str, bucket_id: str | None) -> str | None:
    if not bucket_id:
        return None
    cur.execute(
        """
        SELECT account_id::text AS account_id
        FROM (
            SELECT ba.account_id
            FROM bucket_accounts ba
            JOIN buckets b ON b.bucket_id = ba.bucket_id
            JOIN users u ON u.user_id = b.user_id
            WHERE u.username=%s AND b.bucket_id=%s::uuid
            UNION
            SELECT b.linked_account_id
            FROM buckets b
            JOIN users u ON u.user_id = b.user_id
            WHERE u.username=%s AND b.bucket_id=%s::uuid AND b.linked_account_id IS NOT NULL
        ) linked
        LIMIT 2
        """,
        (username, bucket_id, username, bucket_id),
    )
    rows = cur.fetchall()
    return rows[0]["account_id"] if len(rows) == 1 else None


def _funding_window_for_plan(cur, username: str, month: str) -> tuple[datetime, datetime]:
    year, month_num = parse_month(month)
    prev_month = month_num - 1
    prev_year = year
    if prev_month == 0:
        prev_month = 12
        prev_year -= 1
    prev_month_key = f"{prev_year:04d}-{prev_month:02d}"
    cur.execute(
        "SELECT payday_day FROM payday_overrides WHERE username=%s AND month=%s",
        (username, prev_month_key),
    )
    override = cur.fetchone()
    if override:
        payday_day = int(override["payday_day"])
    else:
        cur.execute("SELECT default_payday_day FROM users WHERE username=%s", (username,))
        user = cur.fetchone()
        payday_day = int(user["default_payday_day"] or 25) if user else 25
    payroll_day = datetime(prev_year, prev_month, clamp_day(prev_year, prev_month, payday_day)).date()
    start = local_day_start_utc(payroll_day - timedelta(days=7))
    end = local_day_start_utc(payroll_day + timedelta(days=8))
    return start, end


def _has_payroll_received(cur, username: str, source_account_id: str, month: str) -> bool:
    start, end = _funding_window_for_plan(cur, username, month)
    cur.execute(
        """
        SELECT 1
        FROM transactions t
        JOIN accounts a ON a.account_id=t.account_id
        WHERE a.username=%s
          AND t.account_id=%s::uuid
          AND t.deleted_at IS NULL
          AND t.is_transfer=FALSE
          AND t.is_cycle_topup=TRUE
          AND t.transaction_type='debit'
          AND t.date >= %s AND t.date < %s
        LIMIT 1
        """,
        (username, source_account_id, start, end),
    )
    return bool(cur.fetchone())


def _planned_total(cur, plan_id: str) -> int:
    cur.execute(
        "SELECT COALESCE(SUM(planned_amount),0)::bigint AS total FROM allocation_items WHERE plan_id=%s::uuid",
        (plan_id,),
    )
    return int(cur.fetchone()["total"] or 0)


def _next_month_str(month: str) -> str:
    year, month_num = parse_month(month)
    if month_num == 12:
        return f"{year + 1:04d}-01"
    return f"{year:04d}-{month_num + 1:02d}"


def _create_next_active_plan(cur, username: str, plan_id: str) -> str | None:
    cur.execute(
        """
        SELECT p.plan_id::text AS plan_id,
               p.user_id::text AS user_id,
               p.month,
               p.expected_income,
               p.notes,
               p.funding_source_account_id::text AS funding_source_account_id,
               p.auto_fund_enabled
        FROM allocation_plans p
        JOIN users u ON u.user_id=p.user_id
        WHERE p.plan_id=%s::uuid AND u.username=%s
        """,
        (plan_id, username),
    )
    plan = cur.fetchone()
    if not plan:
        return None
    next_month = _next_month_str(plan["month"])
    cur.execute(
        """
        SELECT plan_id::text AS plan_id
        FROM allocation_plans
        WHERE user_id=%s::uuid AND month=%s
        """,
        (plan["user_id"], next_month),
    )
    if cur.fetchone():
        return None
    cur.execute(
        """
        INSERT INTO allocation_plans (
            user_id,
            month,
            expected_income,
            status,
            notes,
            funding_source_account_id,
            auto_fund_enabled,
            activated_at
        )
        VALUES (%s::uuid, %s, %s, 'active', %s, %s::uuid, %s, now())
        RETURNING plan_id::text AS plan_id
        """,
        (
            plan["user_id"],
            next_month,
            int(plan["expected_income"] or 0),
            plan.get("notes"),
            plan.get("funding_source_account_id"),
            bool(plan.get("auto_fund_enabled")),
        ),
    )
    next_plan_id = cur.fetchone()["plan_id"]
    cur.execute(
        """
        INSERT INTO allocation_items (
            plan_id,
            bucket_id,
            target_account_id,
            include_in_emergency_base,
            label,
            mode,
            value,
            priority,
            planned_amount,
            funded_amount,
            status
        )
        SELECT %s::uuid,
               bucket_id,
               target_account_id,
               include_in_emergency_base,
               label,
               mode,
               value,
               priority,
               planned_amount,
               0,
               'pending'
        FROM allocation_items
        WHERE plan_id=%s::uuid
        ORDER BY priority, label
        """,
        (next_plan_id, plan_id),
    )
    return next_plan_id


def _budgeted_spending_accounts(cur, plan_id: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT i.target_account_id::text AS account_id,
               COALESCE(SUM(i.planned_amount), 0)::bigint AS amount
        FROM allocation_items i
        JOIN accounts a ON a.account_id = i.target_account_id
        WHERE i.plan_id=%s::uuid
          AND i.target_account_id IS NOT NULL
          AND a.profile_type IN ('fixed_spending','dynamic_spending')
          AND a.is_no_limit = FALSE
        GROUP BY i.target_account_id
        HAVING COALESCE(SUM(i.planned_amount), 0) > 0
        """,
        (plan_id,),
    )
    return cur.fetchall()


def _upsert_allocation_budgets(cur, username: str, plan_id: str, run_id: str, month: str) -> None:
    for row in _budgeted_spending_accounts(cur, plan_id):
        cur.execute(
            """
            INSERT INTO budgets (
                user_id,
                username,
                account_id,
                month,
                amount,
                source,
                allocation_plan_id,
                allocation_run_id
            )
            SELECT user_id, username, %s::uuid, %s, %s, 'allocation', %s::uuid, %s::uuid
            FROM users
            WHERE username=%s
            ON CONFLICT (username, account_id, month)
            DO UPDATE SET
                amount=EXCLUDED.amount,
                source='allocation',
                allocation_plan_id=EXCLUDED.allocation_plan_id,
                allocation_run_id=EXCLUDED.allocation_run_id
            WHERE budgets.source IS DISTINCT FROM 'manual'
            """,
            (row["account_id"], month, int(row["amount"]), plan_id, run_id, username),
        )


def _fund_plan(cur, username: str, plan_id: str, *, source_account_id: str | None, trigger_type: str) -> dict[str, Any]:
    cur.execute(
        """
        SELECT p.plan_id::text AS plan_id,
               p.user_id::text AS user_id,
               p.month,
               p.expected_income,
               p.status,
               p.funding_source_account_id::text AS funding_source_account_id,
               p.auto_fund_enabled
        FROM allocation_plans p
        JOIN users u ON u.user_id=p.user_id
        WHERE p.plan_id=%s::uuid AND u.username=%s
        FOR UPDATE OF p
        """,
        (plan_id, username),
    )
    plan = cur.fetchone()
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    if plan["status"] != "active":
        raise HTTPException(status_code=400, detail="Only active plans can allocate funds")

    source_account_id = source_account_id or plan.get("funding_source_account_id")
    if not source_account_id:
        cur.execute(
            """
            SELECT account_id::text AS account_id
            FROM accounts
            WHERE username=%s AND is_payroll_source=TRUE
            ORDER BY account_name
            """,
            (username,),
        )
        payroll_accounts = cur.fetchall()
        if len(payroll_accounts) == 1:
            source_account_id = payroll_accounts[0]["account_id"]
        elif len(payroll_accounts) > 1:
            raise HTTPException(status_code=400, detail="Choose a payroll source account")
        else:
            raise HTTPException(status_code=400, detail="Set a payroll source account before allocating funds")
    source_account_id = _validate_account(cur, username, source_account_id, "source_account_id")

    total_planned = _planned_total(cur, plan_id)
    if total_planned > int(plan["expected_income"] or 0):
        raise HTTPException(status_code=400, detail="Planned allocation exceeds expected income")

    cur.execute(
        """
        SELECT i.item_id::text AS item_id,
               i.label,
               i.planned_amount,
               i.funded_amount,
               i.target_account_id::text AS target_account_id,
               a.account_name AS target_account_name
        FROM allocation_items i
        LEFT JOIN accounts a ON a.account_id=i.target_account_id
        WHERE i.plan_id=%s::uuid
        ORDER BY i.priority ASC, i.label ASC
        FOR UPDATE OF i
        """,
        (plan_id,),
    )
    items = cur.fetchall()
    if not items:
        raise HTTPException(status_code=400, detail="Plan has no allocation items")
    missing_targets = [item["label"] for item in items if not item.get("target_account_id")]
    if missing_targets:
        raise HTTPException(status_code=400, detail=f"Set target accounts for: {', '.join(missing_targets)}")

    if not _has_payroll_received(cur, username, source_account_id, plan["month"]):
        raise HTTPException(status_code=400, detail="Payroll transaction has not been recorded for this plan cycle")

    target_ids = [item["target_account_id"] for item in items if item.get("target_account_id")]
    lock_accounts_for_update(cur, username, [source_account_id, *target_ids])

    remaining_items = [
        {
            **item,
            "remaining": max(int(item["planned_amount"] or 0) - int(item["funded_amount"] or 0), 0),
        }
        for item in items
    ]
    amount_to_move = sum(
        int(item["remaining"])
        for item in remaining_items
        if item["remaining"] > 0 and item["target_account_id"] != source_account_id
    )
    amount_to_mark = sum(int(item["remaining"]) for item in remaining_items if item["remaining"] > 0)
    if amount_to_mark <= 0:
        run_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO allocation_funding_runs (run_id, plan_id, user_id, source_account_id, trigger_type, status, amount, completed_at)
            VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, %s, 'skipped', 0, now())
            """,
            (run_id, plan_id, plan["user_id"], source_account_id, trigger_type),
        )
        _upsert_allocation_budgets(cur, username, plan_id, run_id, plan["month"])
        return {"ok": True, "run_id": run_id, "status": "skipped", "amount": 0, "transfer_ids": []}

    if amount_to_move > 0:
        temp_id = str(uuid.uuid4())
        ensure_account_non_negative(
            cur,
            source_account_id,
            now_utc(),
            [
                {
                    "transaction_id": temp_id,
                    "date": now_utc(),
                    "transaction_type": "credit",
                    "amount": amount_to_move,
                }
            ],
        )

    run_id = str(uuid.uuid4())
    transfer_ids: list[str] = []
    cur.execute(
        """
        INSERT INTO allocation_funding_runs (run_id, plan_id, user_id, source_account_id, trigger_type, status)
        VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, %s, 'pending')
        """,
        (run_id, plan_id, plan["user_id"], source_account_id, trigger_type),
    )

    for item in remaining_items:
        amount = int(item["remaining"])
        if amount <= 0:
            continue
        target_account_id = item["target_account_id"]
        if target_account_id != source_account_id:
            transfer_id = str(uuid.uuid4())
            transfer_ids.append(transfer_id)
            cur.execute(
                """
                INSERT INTO transactions (
                    account_id,
                    transaction_type,
                    is_cycle_topup,
                    transaction_name,
                    amount,
                    date,
                    is_transfer,
                    transfer_id,
                    notes,
                    is_reviewed
                )
                VALUES
                  (%s::uuid, 'credit', false, %s, %s, %s, true, %s::uuid, %s, true),
                  (%s::uuid, 'debit', false, %s, %s, %s, true, %s::uuid, %s, true)
                """,
                (
                    source_account_id,
                    f"Allocation to {item['target_account_name'] or item['label']}",
                    amount,
                    now_utc(),
                    transfer_id,
                    f"Allocation plan {plan['month']} · {item['label']}",
                    target_account_id,
                    "Allocation funding",
                    amount,
                    now_utc(),
                    transfer_id,
                    f"Allocation plan {plan['month']} · {item['label']}",
                ),
            )
        cur.execute(
            """
            UPDATE allocation_items
            SET funded_amount = planned_amount,
                status = CASE
                  WHEN planned_amount = 0 THEN 'pending'
                  ELSE 'funded'
                END
            WHERE item_id=%s::uuid
            """,
            (item["item_id"],),
        )

    _upsert_allocation_budgets(cur, username, plan_id, run_id, plan["month"])
    cur.execute(
        """
        UPDATE allocation_funding_runs
        SET status='succeeded',
            amount=%s,
            transfer_ids=%s::uuid[],
            completed_at=now()
        WHERE run_id=%s::uuid
        """,
        (amount_to_mark, transfer_ids, run_id),
    )
    cur.execute(
        "UPDATE allocation_plans SET funding_source_account_id=%s::uuid, updated_at=now() WHERE plan_id=%s::uuid",
        (source_account_id, plan_id),
    )
    return {"ok": True, "run_id": run_id, "status": "succeeded", "amount": amount_to_mark, "transfer_ids": transfer_ids}


@router.get("")
def list_plans(req: Request):
    username = req.state.username
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.plan_id::text AS plan_id, p.month, p.expected_income,
                   p.status, p.notes, p.created_at, p.updated_at,
                   p.period_id::text AS period_id,
                   p.funding_source_account_id::text AS funding_source_account_id,
                   p.auto_fund_enabled,
                   p.activated_at
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
                   p.status, p.notes, p.created_at, p.updated_at,
                   p.funding_source_account_id::text AS funding_source_account_id,
                   fs.account_name AS funding_source_account_name,
                   p.auto_fund_enabled,
                   p.activated_at
            FROM allocation_plans p JOIN users u ON u.user_id=p.user_id
            LEFT JOIN accounts fs ON fs.account_id=p.funding_source_account_id
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
                   b.name AS bucket_name, b.kind AS bucket_kind,
                   i.target_account_id::text AS target_account_id,
                   ta.account_name AS target_account_name,
                   i.include_in_emergency_base,
                   i.label, i.mode, i.value, i.priority,
                   i.planned_amount, i.funded_amount, i.status
            FROM allocation_items i
            LEFT JOIN buckets b ON b.bucket_id = i.bucket_id
            LEFT JOIN accounts ta ON ta.account_id = i.target_account_id
            WHERE i.plan_id=%s::uuid
            ORDER BY i.priority ASC, i.label ASC
            """,
            (plan_id,),
        )
        items = cur.fetchall()
        for item in items:
            item["group"] = _allocation_group(item.get("bucket_kind"), item["mode"])
        cur.execute(
            """
            SELECT run_id::text AS run_id,
                   source_account_id::text AS source_account_id,
                   trigger_type,
                   status,
                   amount,
                   failure_reason,
                   transfer_ids::text[] AS transfer_ids,
                   created_at,
                   completed_at
            FROM allocation_funding_runs
            WHERE plan_id=%s::uuid
            ORDER BY created_at DESC
            LIMIT 10
            """,
            (plan_id,),
        )
        return {**plan, "items": items, "health": _allocation_health(cur, username, items), "funding_runs": cur.fetchall()}


@router.post("")
async def create_plan(req: Request):
    username = req.state.username
    data: dict[str, Any] = await req.json()
    month = str(data.get("month") or current_month_local()).strip()
    parse_month(month)
    expected_income = int(data.get("expected_income") or 0)
    notes = (data.get("notes") or "").strip() or None
    funding_source_account_id = data.get("funding_source_account_id") or None
    auto_fund_enabled = bool(data.get("auto_fund_enabled", True))

    with db_conn() as conn, conn.cursor() as cur:
        funding_source_account_id = _validate_account(cur, username, funding_source_account_id, "funding_source_account_id")
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
                INSERT INTO allocation_plans (
                    user_id,
                    period_id,
                    month,
                    expected_income,
                    notes,
                    funding_source_account_id,
                    auto_fund_enabled
                )
                SELECT user_id, %s::uuid, %s, %s, %s, %s::uuid, %s FROM users WHERE username=%s
                RETURNING plan_id::text AS plan_id
                """,
                (period_id, month, expected_income, notes, funding_source_account_id, auto_fund_enabled, username),
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
    funding_source_account_id = data.get("funding_source_account_id") or None
    auto_fund_enabled = bool(data.get("auto_fund_enabled", True))

    with db_conn() as conn, conn.cursor() as cur:
        funding_source_account_id = _validate_account(cur, username, funding_source_account_id, "funding_source_account_id")
        # Allow updates on draft and active plans; closed plans remain immutable.
        cur.execute(
            """
            UPDATE allocation_plans
            SET expected_income=%s,
                notes=%s,
                funding_source_account_id=%s::uuid,
                auto_fund_enabled=%s,
                updated_at=now()
            WHERE plan_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s)
              AND status<>'closed'
            RETURNING plan_id
            """,
            (expected_income, notes, funding_source_account_id, auto_fund_enabled, plan_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Plan not found or already closed")
        # Recompute planned_amount for percent items and reconcile per-item status
        # in case funded_amount no longer matches the new planned_amount.
        cur.execute(
            """
            UPDATE allocation_items
            SET planned_amount = ROUND(%s * value / 100.0)::bigint,
                status = CASE
                  WHEN ROUND(%s * value / 100.0)::bigint = 0 AND funded_amount > 0 THEN 'overflowed'
                  WHEN ROUND(%s * value / 100.0)::bigint = 0 THEN 'pending'
                  WHEN funded_amount = 0 THEN 'pending'
                  WHEN funded_amount > ROUND(%s * value / 100.0)::bigint THEN 'overflowed'
                  WHEN funded_amount = ROUND(%s * value / 100.0)::bigint THEN 'funded'
                  ELSE 'partial'
                END
            WHERE plan_id=%s::uuid AND mode='percent'
            """,
            (expected_income, expected_income, expected_income, expected_income, expected_income, plan_id),
        )
        conn.commit()
    invalidate_user_cache(username)
    return {"ok": True}


@router.post("/{plan_id}/activate")
def activate_plan(plan_id: str, req: Request):
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT expected_income
            FROM allocation_plans
            WHERE plan_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s)
              AND status='draft'
            """,
            (plan_id, username),
        )
        plan = cur.fetchone()
        if not plan:
            raise HTTPException(status_code=404, detail="Plan not found or already active/closed")
        total_planned = _planned_total(cur, plan_id)
        if total_planned > int(plan["expected_income"] or 0):
            raise HTTPException(status_code=400, detail="Planned allocation exceeds expected income")
        cur.execute(
            """
            UPDATE allocation_plans SET status='active', activated_at=COALESCE(activated_at, now()), updated_at=now()
            WHERE plan_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s)
              AND status='draft'
            RETURNING plan_id
            """,
            (plan_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Plan not found or already active/closed")
        next_plan_id = _create_next_active_plan(cur, username, plan_id)
        conn.commit()
    return {"ok": True, "next_plan_id": next_plan_id}


@router.delete("/{plan_id}")
def delete_plan(plan_id: str, req: Request):
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    with db_conn() as conn, conn.cursor() as cur:
        # Confirm ownership and that the plan is not closed (closed plans stay
        # immutable for reporting integrity). Active plans can be deleted —
        # already-funded transfers stay in place because they are real money
        # movements; deletion only removes the plan/items/runs records.
        cur.execute(
            """
            SELECT p.plan_id::text AS plan_id
            FROM allocation_plans p
            JOIN users u ON u.user_id = p.user_id
            WHERE p.plan_id=%s::uuid AND u.username=%s AND p.status<>'closed'
            """,
            (plan_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Plan not found or already closed")
        # Remove allocation-derived budgets explicitly so we don't leave
        # orphaned 'allocation' rows once the FK SET NULL fires on delete.
        cur.execute(
            "DELETE FROM budgets WHERE allocation_plan_id=%s::uuid",
            (plan_id,),
        )
        cur.execute(
            "DELETE FROM allocation_plans WHERE plan_id=%s::uuid",
            (plan_id,),
        )
        conn.commit()
    invalidate_user_cache(username)
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
    _validate_allocation_value(mode, value)
    bucket_id = data.get("bucket_id") or None
    target_account_id = data.get("target_account_id") or None
    include_override = data.get("include_in_emergency_base") if "include_in_emergency_base" in data else None
    priority = int(data.get("priority") or 50)

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT expected_income FROM allocation_plans p JOIN users u ON u.user_id=p.user_id WHERE p.plan_id=%s::uuid AND u.username=%s",
            (plan_id, username),
        )
        plan = cur.fetchone()
        if not plan:
            raise HTTPException(status_code=404, detail="Plan not found")
        bucket_id = _validate_bucket(cur, username, bucket_id)
        bucket_kind = _bucket_kind(cur, username, bucket_id)
        include_in_emergency_base = (
            bool(include_override)
            if include_override is not None
            else _default_include_in_emergency_base(bucket_kind, mode)
        )
        target_account_id = _validate_account(cur, username, target_account_id, "target_account_id")
        if not target_account_id:
            target_account_id = _default_bucket_account(cur, username, bucket_id)
        planned = _resolve_planned_amount(mode, value, int(plan["expected_income"]))
        cur.execute(
            """
            INSERT INTO allocation_items (
                plan_id,
                bucket_id,
                target_account_id,
                include_in_emergency_base,
                label,
                mode,
                value,
                priority,
                planned_amount
            )
            VALUES (%s::uuid, %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s)
            RETURNING item_id::text AS item_id
            """,
            (plan_id, bucket_id, target_account_id, include_in_emergency_base, label, mode, value, priority, planned),
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
    _validate_allocation_value(mode, value)
    bucket_id = data.get("bucket_id") or None
    target_account_id = data.get("target_account_id") or None
    include_override = data.get("include_in_emergency_base") if "include_in_emergency_base" in data else None
    priority = int(data.get("priority") or 50)

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT expected_income FROM allocation_plans p JOIN users u ON u.user_id=p.user_id WHERE p.plan_id=%s::uuid AND u.username=%s",
            (plan_id, username),
        )
        plan = cur.fetchone()
        if not plan:
            raise HTTPException(status_code=404, detail="Plan not found")
        bucket_id = _validate_bucket(cur, username, bucket_id)
        bucket_kind = _bucket_kind(cur, username, bucket_id)
        if include_override is None:
            cur.execute(
                "SELECT include_in_emergency_base FROM allocation_items WHERE item_id=%s::uuid AND plan_id=%s::uuid",
                (item_id, plan_id),
            )
            existing_item = cur.fetchone()
            if not existing_item:
                raise HTTPException(status_code=404, detail="Item not found")
            include_in_emergency_base = bool(existing_item["include_in_emergency_base"])
        else:
            include_in_emergency_base = bool(include_override)
        target_account_id = _validate_account(cur, username, target_account_id, "target_account_id")
        if not target_account_id:
            target_account_id = _default_bucket_account(cur, username, bucket_id)
        planned = _resolve_planned_amount(mode, value, int(plan["expected_income"]))
        cur.execute(
            """
            UPDATE allocation_items SET label=%s, mode=%s, value=%s, bucket_id=%s::uuid,
              target_account_id=%s::uuid, include_in_emergency_base=%s,
              priority=%s, planned_amount=%s
            WHERE item_id=%s::uuid AND plan_id=%s::uuid
            RETURNING item_id
            """,
            (label, mode, value, bucket_id, target_account_id, include_in_emergency_base, priority, planned, item_id, plan_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Item not found")
        conn.commit()
    return {"ok": True}


@router.put("/{plan_id}/items/{item_id}/emergency-base")
async def update_item_emergency_base(plan_id: str, item_id: str, req: Request):
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    item_id = parse_uuid_value(item_id, "item_id")
    data: dict[str, Any] = await req.json()
    include = bool(data.get("include_in_emergency_base", False))
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE allocation_items i
            SET include_in_emergency_base=%s
            FROM allocation_plans p
            JOIN users u ON u.user_id=p.user_id
            WHERE i.plan_id=p.plan_id
              AND p.plan_id=%s::uuid
              AND i.item_id=%s::uuid
              AND u.username=%s
            RETURNING i.item_id
            """,
            (include, plan_id, item_id, username),
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


@router.get("/{plan_id}/funding-status")
def funding_status(plan_id: str, req: Request):
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.plan_id::text AS plan_id,
                   p.month,
                   p.status,
                   p.expected_income,
                   p.funding_source_account_id::text AS funding_source_account_id,
                   fs.account_name AS funding_source_account_name,
                   p.auto_fund_enabled
            FROM allocation_plans p
            JOIN users u ON u.user_id=p.user_id
            LEFT JOIN accounts fs ON fs.account_id=p.funding_source_account_id
            WHERE p.plan_id=%s::uuid AND u.username=%s
            """,
            (plan_id, username),
        )
        plan = cur.fetchone()
        if not plan:
            raise HTTPException(status_code=404, detail="Plan not found")
        total_planned = _planned_total(cur, plan_id)
        cur.execute(
            "SELECT COALESCE(SUM(planned_amount - funded_amount),0)::bigint AS remaining FROM allocation_items WHERE plan_id=%s::uuid",
            (plan_id,),
        )
        remaining = int(cur.fetchone()["remaining"] or 0)
        source_id = plan.get("funding_source_account_id")
        source_options = []
        if not source_id:
            cur.execute(
                """
                SELECT account_id::text AS account_id, account_name
                FROM accounts
                WHERE username=%s AND is_payroll_source=TRUE
                ORDER BY account_name
                """,
                (username,),
            )
            source_options = cur.fetchall()
            if len(source_options) == 1:
                source_id = source_options[0]["account_id"]
        payroll_received = bool(source_id and _has_payroll_received(cur, username, source_id, plan["month"]))
        source_balance = None
        if source_id:
            source_balance = int(get_account_balances(cur, username, now_utc()).get(source_id, 0))
        missing_source = not source_id
        missing_payroll = source_id is not None and not payroll_received
        insufficient_balance = source_balance is not None and source_balance < remaining
        missing_targets = []
        cur.execute(
            """
            SELECT label FROM allocation_items
            WHERE plan_id=%s::uuid AND target_account_id IS NULL
            ORDER BY priority, label
            """,
            (plan_id,),
        )
        missing_targets = [r["label"] for r in cur.fetchall()]
        over_allocated = total_planned > int(plan["expected_income"] or 0)
        can_allocate = (
            plan["status"] == "active"
            and remaining > 0
            and not missing_source
            and not missing_payroll
            and not insufficient_balance
            and not missing_targets
            and not over_allocated
        )
        reasons = []
        if plan["status"] != "active":
            reasons.append("Activate the plan first.")
        if remaining <= 0:
            reasons.append("This plan is already fully funded.")
        if missing_source:
            reasons.append("Choose a payroll source account.")
        if missing_payroll:
            reasons.append("Record payroll income for this cycle first.")
        if insufficient_balance:
            reasons.append("Payroll source balance is not sufficient.")
        if missing_targets:
            reasons.append(f"Set target accounts for: {', '.join(missing_targets)}.")
        if over_allocated:
            reasons.append("Planned allocation exceeds expected income.")
    return {
        "plan_id": plan_id,
        "month": plan["month"],
        "remaining_amount": remaining,
        "total_planned": total_planned,
        "source_account_id": source_id,
        "source_account_name": plan.get("funding_source_account_name"),
        "source_options": source_options,
        "source_balance": source_balance,
        "payroll_received": payroll_received,
        "can_allocate": can_allocate,
        "reasons": reasons,
    }


@router.post("/{plan_id}/allocate-funds")
async def allocate_plan_funds(plan_id: str, req: Request):
    username = req.state.username
    plan_id = parse_uuid_value(plan_id, "plan_id")
    data: dict[str, Any] = await req.json()
    source_account_id = data.get("source_account_id") or None
    trigger_type = str(data.get("trigger_type") or "manual").strip().lower()
    if trigger_type not in ("manual", "automatic"):
        raise HTTPException(status_code=400, detail="Invalid trigger_type")
    with db_conn() as conn, conn.cursor() as cur:
        try:
            result = _fund_plan(cur, username, plan_id, source_account_id=source_account_id, trigger_type=trigger_type)
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:
            conn.rollback()
            raise
    invalidate_user_cache(username)
    return result


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


def run_due_auto_funding_once() -> int:
    """Best-effort auto allocator. Skips plans that are not ready."""
    processed = 0
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.plan_id::text AS plan_id,
                   u.username
            FROM allocation_plans p
            JOIN users u ON u.user_id=p.user_id
            WHERE p.status='active'
              AND p.auto_fund_enabled=TRUE
              AND EXISTS (
                SELECT 1 FROM allocation_items i
                WHERE i.plan_id=p.plan_id AND i.funded_amount < i.planned_amount
              )
            ORDER BY p.month ASC, p.created_at ASC
            LIMIT 25
            """
        )
        candidates = cur.fetchall()
        for candidate in candidates:
            try:
                _fund_plan(
                    cur,
                    candidate["username"],
                    candidate["plan_id"],
                    source_account_id=None,
                    trigger_type="automatic",
                )
                conn.commit()
                processed += 1
            except HTTPException:
                conn.rollback()
                continue
            except Exception:
                conn.rollback()
                continue
    return processed


def start_allocation_scheduler(interval_seconds: int = 1800) -> None:
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True

    def loop() -> None:
        while True:
            try:
                run_due_auto_funding_once()
            except Exception:
                pass
            time.sleep(interval_seconds)

    thread = threading.Thread(target=loop, name="allocation-auto-funding", daemon=True)
    thread.start()
