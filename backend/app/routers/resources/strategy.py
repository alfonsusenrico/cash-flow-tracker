"""Strategy Rules CRUD + preview — /strategy-rules and /v1/strategy-rules"""
import re
from typing import Any
from fastapi import APIRouter, HTTPException, Request
from psycopg.errors import UniqueViolation
from app.db.pool import db_conn
from app.services.ledger.balances import get_account_balances, parse_uuid_value
from app.services.ledger.period import current_month_local, parse_month, now_utc

router = APIRouter(tags=["strategy"])


def _parse_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _validate_rule_value(mode: str, value: float) -> None:
    if value < 0:
        raise HTTPException(status_code=400, detail="value must be >= 0")
    if mode == "percent" and value > 100:
        raise HTTPException(status_code=400, detail="percentage must be between 0 and 100")


def _validate_target_bucket(cur, username: str, bucket_id: str | None) -> None:
    if not bucket_id:
        return
    bucket_id = parse_uuid_value(bucket_id, "target_bucket_id")
    cur.execute(
        """
        SELECT 1 FROM buckets b
        JOIN users u ON u.user_id = b.user_id
        WHERE u.username=%s AND b.bucket_id=%s::uuid AND b.is_archived=FALSE
        """,
        (username, bucket_id),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="Target bucket not found")


def _normalize_name(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _strategy_group(bucket_kind: str | None, mode: str) -> str:
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


GROUP_LABELS = {
    "fixed_spending": "Fixed spending",
    "dynamic_spending": "Dynamic spending",
    "cash_buffer": "Cash and buffers",
    "emergency_buffer": "Emergency fund",
    "goals_savings": "Goals and savings",
    "investment": "Investment",
}


def _summarize_allocations(income: int, allocations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    totals = {key: 0 for key in GROUP_LABELS}
    for allocation in allocations:
        if allocation.get("skipped"):
            continue
        totals[allocation["group"]] = totals.get(allocation["group"], 0) + int(allocation.get("amount") or 0)
    return [
        {
            "group": key,
            "label": label,
            "amount": amount,
            "percent": round((amount / income) * 100) if income > 0 else 0,
        }
        for key, label in GROUP_LABELS.items()
        if (amount := totals.get(key, 0)) > 0
    ]


def _load_bucket_balances(cur, username: str) -> dict[str, dict[str, Any]]:
    balances = get_account_balances(cur, username, now_utc())
    cur.execute(
        """
        SELECT b.bucket_id::text AS bucket_id, b.name, b.kind, b.target_amount,
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
    result: dict[str, dict[str, Any]] = {}
    for bucket in cur.fetchall():
        account_ids = list(bucket.get("linked_account_ids") or [])
        current_amount = sum(int(balances.get(account_id, 0)) for account_id in account_ids)
        result[bucket["bucket_id"]] = {
            **bucket,
            "current_amount": current_amount,
        }
    return result


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


def _load_active_rules(cur, username: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT r.rule_id::text AS rule_id, r.name, r.mode, r.value,
               r.cap, r.floor, r.priority,
               r.target_bucket_id::text AS target_bucket_id,
               b.name AS target_bucket_name,
               b.kind AS target_bucket_kind
        FROM strategy_rules r
        JOIN users u ON u.user_id = r.user_id
        LEFT JOIN buckets b ON b.bucket_id = r.target_bucket_id
        WHERE u.username = %s AND r.is_active = TRUE
        ORDER BY
          CASE r.mode
            WHEN 'fixed' THEN 0
            WHEN 'target_balance' THEN 1
            WHEN 'percent' THEN 2
            WHEN 'overflow' THEN 3
            ELSE 9
          END ASC,
          r.priority ASC,
          r.name ASC
        """,
        (username,),
    )
    return cur.fetchall()


def _calculate_strategy(
    *,
    income: int,
    rules: list[dict[str, Any]],
    buckets: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    remaining = income
    allocations = []

    for rule in rules:
        mode = rule["mode"]
        value = float(rule["value"])
        cap = rule.get("cap")
        floor_val = rule.get("floor")
        target_bucket_id = rule.get("target_bucket_id")
        bucket = buckets.get(target_bucket_id) if target_bucket_id else None
        base_amount = 0
        reason = ""

        if remaining <= 0:
            reason = "Skipped because no income remains."
        elif mode == "overflow":
            base_amount = remaining
            reason = "Allocated all remaining income."
        elif mode == "fixed":
            base_amount = int(value)
            reason = f"Fixed amount Rp {base_amount:,}."
        elif mode == "percent":
            base_amount = int(round(income * value / 100))
            reason = f"{value:g}% of total income."
        elif mode == "target_balance":
            target_amount = int(bucket.get("target_amount") or 0) if bucket else 0
            current_amount = int(bucket.get("current_amount") or 0) if bucket else 0
            shortfall = max(target_amount - current_amount, 0)
            fallback_amount = int(value)
            base_amount = shortfall if target_amount > 0 else fallback_amount
            reason = (
                f"Bucket needs Rp {shortfall:,} to reach target."
                if target_amount > 0
                else f"No bucket target set; using fallback amount Rp {fallback_amount:,}."
            )

        amount = min(base_amount, max(remaining, 0))
        applied: list[str] = []
        skipped = False

        if cap is not None and amount > int(cap):
            amount = int(cap)
            applied.append(f"Capped at Rp {amount:,}.")
        if floor_val is not None and amount < int(floor_val):
            amount = 0
            skipped = True
            applied.append(f"Skipped because amount is below floor Rp {int(floor_val):,}.")
        if amount <= 0 and not skipped:
            skipped = True
            if mode == "target_balance" and base_amount <= 0:
                applied.append("Skipped because target is already met.")
            elif remaining <= 0:
                applied.append("Skipped because no income remains.")
            else:
                applied.append("Skipped because calculated amount is zero.")

        if amount > 0:
            remaining -= amount

        allocations.append({
            "rule_id": rule["rule_id"],
            "rule_name": rule["name"],
            "target_bucket_id": target_bucket_id,
            "target_bucket_name": rule.get("target_bucket_name"),
            "target_bucket_kind": rule.get("target_bucket_kind") or (bucket.get("kind") if bucket else None),
            "group": _strategy_group(rule.get("target_bucket_kind") or (bucket.get("kind") if bucket else None), mode),
            "mode": mode,
            "priority": int(rule["priority"]),
            "value": float(rule["value"]),
            "amount": amount,
            "base_amount": base_amount,
            "remaining_after": remaining,
            "skipped": skipped,
            "reason": " ".join([reason, *applied]).strip(),
            "bucket_current_amount": int(bucket.get("current_amount") or 0) if bucket else None,
            "bucket_target_amount": int(bucket.get("target_amount") or 0) if bucket else None,
        })

    summary = _summarize_allocations(income, allocations)
    return {
        "income": income,
        "total_allocated": income - remaining,
        "remaining": remaining,
        "allocations": allocations,
        "summary": summary,
    }


@router.get("")
def list_rules(req: Request):
    username = req.state.username
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.rule_id::text AS rule_id, r.name, r.trigger, r.mode,
                   r.target_bucket_id::text AS target_bucket_id,
                   b.name AS target_bucket_name,
                   b.kind AS target_bucket_kind,
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
    if target_bucket_id:
        target_bucket_id = parse_uuid_value(target_bucket_id, "target_bucket_id")
    value = float(data.get("value") or 0)
    _validate_rule_value(mode, value)
    cap = _parse_optional_int(data.get("cap"))
    floor = _parse_optional_int(data.get("floor"))
    priority = int(data.get("priority") or 50)
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        _validate_target_bucket(cur, username, target_bucket_id)
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
    if target_bucket_id:
        target_bucket_id = parse_uuid_value(target_bucket_id, "target_bucket_id")
    value = float(data.get("value") or 0)
    _validate_rule_value(mode, value)
    cap = _parse_optional_int(data.get("cap"))
    floor = _parse_optional_int(data.get("floor"))
    priority = int(data.get("priority") or 50)
    is_active = bool(data.get("is_active", True))
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        _validate_target_bucket(cur, username, target_bucket_id)
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
        rules = _load_active_rules(cur, username)
        buckets = _load_bucket_balances(cur, username)

    return _calculate_strategy(income=income, rules=rules, buckets=buckets)


def _load_all_rules_for_matching(cur, username: str) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT r.rule_id::text AS rule_id, r.name, r.trigger, r.mode,
               r.target_bucket_id::text AS target_bucket_id,
               b.name AS target_bucket_name,
               b.kind AS target_bucket_kind,
               r.value, r.cap, r.floor, r.priority, r.is_active, r.notes
        FROM strategy_rules r
        JOIN users u ON u.user_id = r.user_id
        LEFT JOIN buckets b ON b.bucket_id = r.target_bucket_id
        WHERE u.username = %s
        ORDER BY r.priority ASC, r.name ASC
        """,
        (username,),
    )
    return cur.fetchall()


def _load_allocation_plan_for_strategy(cur, username: str, plan_id: str) -> dict[str, Any]:
    cur.execute(
        """
        SELECT p.plan_id::text AS plan_id, p.month, p.expected_income, p.status
        FROM allocation_plans p
        JOIN users u ON u.user_id = p.user_id
        WHERE u.username=%s AND p.plan_id=%s::uuid
        """,
        (username, plan_id),
    )
    plan = cur.fetchone()
    if not plan:
        raise HTTPException(status_code=404, detail="Allocation plan not found")
    if int(plan["expected_income"] or 0) <= 0:
        raise HTTPException(status_code=400, detail="Allocation plan expected_income must be > 0")
    cur.execute(
        """
        SELECT i.item_id::text AS item_id, i.bucket_id::text AS bucket_id,
               b.name AS bucket_name, b.kind AS bucket_kind,
               i.label, i.mode, i.value, i.priority, i.planned_amount
        FROM allocation_items i
        LEFT JOIN buckets b ON b.bucket_id = i.bucket_id
        WHERE i.plan_id=%s::uuid
        ORDER BY i.priority ASC, i.label ASC
        """,
        (plan_id,),
    )
    return {**plan, "items": cur.fetchall()}


def _find_matching_rule(item: dict[str, Any], rules: list[dict[str, Any]]) -> dict[str, Any] | None:
    bucket_id = item.get("bucket_id")
    if bucket_id:
        match = next((rule for rule in rules if rule.get("target_bucket_id") == bucket_id), None)
        if match:
            return match
    label = _normalize_name(item.get("bucket_name") or item.get("label"))
    return next((rule for rule in rules if _normalize_name(rule.get("name")) == label), None)


def _suggest_rules_from_allocation(plan: dict[str, Any], rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expected_income = int(plan["expected_income"] or 0)
    suggestions: list[dict[str, Any]] = []
    for item in plan["items"]:
        planned_amount = int(item.get("planned_amount") or 0)
        if planned_amount <= 0:
            continue
        percent = round((planned_amount / expected_income) * 100)
        name = item.get("bucket_name") or item.get("label")
        bucket_kind = item.get("bucket_kind")
        existing = _find_matching_rule(item, rules)
        action = "create"
        if existing:
            same_value = int(round(float(existing.get("value") or 0))) == int(percent)
            same_mode = existing.get("mode") == "percent"
            same_bucket = (existing.get("target_bucket_id") or None) == (item.get("bucket_id") or None)
            action = "noop" if same_value and same_mode and same_bucket and existing.get("is_active") else "update"
        suggestions.append({
            "action": action,
            "existing_rule_id": existing.get("rule_id") if existing else None,
            "name": name,
            "trigger": "manual",
            "mode": "percent",
            "value": percent,
            "source_mode": item.get("mode"),
            "source_amount": planned_amount,
            "target_bucket_id": item.get("bucket_id"),
            "target_bucket_name": item.get("bucket_name"),
            "target_bucket_kind": bucket_kind,
            "priority": int(item.get("priority") or 50),
            "group": _strategy_group(bucket_kind, "percent"),
            "notes": f"Generated from allocation plan {plan['month']}.",
        })
    return suggestions


@router.post("/from-allocation/preview")
async def preview_from_allocation(req: Request):
    username = req.state.username
    data: dict[str, Any] = await req.json()
    plan_id = parse_uuid_value(data.get("plan_id"), "plan_id")
    with db_conn() as conn, conn.cursor() as cur:
        plan = _load_allocation_plan_for_strategy(cur, username, plan_id)
        rules = _load_all_rules_for_matching(cur, username)
        suggestions = _suggest_rules_from_allocation(plan, rules)
    summary = _summarize_allocations(
        int(plan["expected_income"]),
        [
            {
                "group": suggestion["group"],
                "amount": suggestion["source_amount"],
                "skipped": False,
            }
            for suggestion in suggestions
        ],
    )
    return {
        "plan_id": plan["plan_id"],
        "month": plan["month"],
        "expected_income": int(plan["expected_income"]),
        "suggestions": suggestions,
        "summary": summary,
    }


@router.post("/from-allocation/apply")
async def apply_from_allocation(req: Request):
    username = req.state.username
    data: dict[str, Any] = await req.json()
    plan_id = parse_uuid_value(data.get("plan_id"), "plan_id")
    created = 0
    updated = 0
    skipped = 0
    changed_rules: list[dict[str, Any]] = []

    with db_conn() as conn, conn.cursor() as cur:
        plan = _load_allocation_plan_for_strategy(cur, username, plan_id)
        rules = _load_all_rules_for_matching(cur, username)
        suggestions = _suggest_rules_from_allocation(plan, rules)
        for suggestion in suggestions:
            if suggestion["action"] == "noop":
                skipped += 1
                continue
            target_bucket_id = suggestion.get("target_bucket_id")
            if target_bucket_id:
                _validate_target_bucket(cur, username, target_bucket_id)
            if suggestion["action"] == "update" and suggestion.get("existing_rule_id"):
                cur.execute(
                    """
                    UPDATE strategy_rules
                    SET name=%s, trigger='manual', mode='percent', target_bucket_id=%s::uuid,
                        value=%s, cap=NULL, floor=NULL, priority=%s, is_active=TRUE, notes=%s
                    WHERE rule_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s)
                    RETURNING rule_id::text AS rule_id
                    """,
                    (
                        suggestion["name"],
                        target_bucket_id,
                        suggestion["value"],
                        suggestion["priority"],
                        suggestion["notes"],
                        suggestion["existing_rule_id"],
                        username,
                    ),
                )
                row = cur.fetchone()
                if row:
                    updated += 1
                    changed_rules.append({**suggestion, "rule_id": row["rule_id"]})
                continue
            cur.execute(
                """
                INSERT INTO strategy_rules (user_id, name, trigger, mode, target_bucket_id, value, priority, notes)
                SELECT user_id, %s, 'manual', 'percent', %s::uuid, %s, %s, %s
                FROM users WHERE username=%s
                RETURNING rule_id::text AS rule_id
                """,
                (
                    suggestion["name"],
                    target_bucket_id,
                    suggestion["value"],
                    suggestion["priority"],
                    suggestion["notes"],
                    username,
                ),
            )
            row = cur.fetchone()
            created += 1
            changed_rules.append({**suggestion, "rule_id": row["rule_id"]})
        conn.commit()

    return {
        "ok": True,
        "plan_id": plan["plan_id"],
        "month": plan["month"],
        "created": created,
        "updated": updated,
        "skipped": skipped,
        "rules": changed_rules,
    }


@router.post("/apply")
async def apply_strategy(req: Request):
    """Create an allocation plan and items from active strategy rules."""
    username = req.state.username
    data: dict[str, Any] = await req.json()
    month = str(data.get("month") or current_month_local()).strip()
    parse_month(month)
    expected_income = int(data.get("expected_income") or data.get("income") or 0)
    if expected_income <= 0:
        raise HTTPException(status_code=400, detail="expected_income must be > 0")
    notes = (data.get("notes") or "Generated from strategy rules").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        rules = _load_active_rules(cur, username)
        if not rules:
            raise HTTPException(status_code=400, detail="No active strategy rules to apply")
        buckets = _load_bucket_balances(cur, username)
        preview = _calculate_strategy(income=expected_income, rules=rules, buckets=buckets)
        generated_items = [a for a in preview["allocations"] if int(a["amount"]) > 0]
        if not generated_items:
            raise HTTPException(status_code=400, detail="Strategy did not generate any allocation items")

        cur.execute(
            "SELECT period_id FROM monthly_periods WHERE user_id=(SELECT user_id FROM users WHERE username=%s) AND month=%s",
            (username, month),
        )
        period_row = cur.fetchone()
        period_id = period_row["period_id"] if period_row else None

        # Overwrite an existing plan for this month when it is safe to do so.
        cur.execute(
            """
            SELECT p.plan_id::text AS plan_id, p.status,
                   COALESCE((SELECT SUM(funded_amount) FROM allocation_items WHERE plan_id=p.plan_id), 0) AS funded
            FROM allocation_plans p JOIN users u ON u.user_id=p.user_id
            WHERE u.username=%s AND p.month=%s
            """,
            (username, month),
        )
        existing = cur.fetchone()
        try:
            if existing:
                if existing["status"] == "closed":
                    raise HTTPException(status_code=400, detail="Allocation plan for this month is closed")
                if int(existing["funded"] or 0) > 0:
                    raise HTTPException(status_code=400, detail="Plan already has funded items; delete it before regenerating from strategy")
                plan_id = existing["plan_id"]
                # Replace the plan's items and refresh its expected income / notes.
                cur.execute("DELETE FROM allocation_items WHERE plan_id=%s::uuid", (plan_id,))
                cur.execute(
                    "UPDATE allocation_plans SET expected_income=%s, notes=%s, updated_at=now() WHERE plan_id=%s::uuid",
                    (expected_income, notes, plan_id),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO allocation_plans (user_id, period_id, month, expected_income, notes)
                    SELECT user_id, %s::uuid, %s, %s, %s FROM users WHERE username=%s
                    RETURNING plan_id::text AS plan_id
                    """,
                    (period_id, month, expected_income, notes, username),
                )
                plan_id = cur.fetchone()["plan_id"]
            for item in generated_items:
                item_mode = "percent" if item["mode"] == "percent" else "fixed"
                item_value = item["value"] if item["mode"] == "percent" else item["amount"]
                label = item["target_bucket_name"] or item["rule_name"]
                target_account_id = _default_bucket_account(cur, username, item["target_bucket_id"])
                cur.execute(
                    """
                    INSERT INTO allocation_items
                      (plan_id, bucket_id, target_account_id, label, mode, value, priority, planned_amount)
                    VALUES (%s::uuid, %s::uuid, %s::uuid, %s, %s, %s, %s, %s)
                    RETURNING item_id::text AS item_id
                    """,
                    (
                        plan_id,
                        item["target_bucket_id"],
                        target_account_id,
                        label,
                        item_mode,
                        item_value,
                        item["priority"],
                        item["amount"],
                    ),
                )
                item["item_id"] = cur.fetchone()["item_id"]
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Allocation plan for this month already exists")

    return {
        "ok": True,
        "plan_id": plan_id,
        "month": month,
        "expected_income": expected_income,
        "items": generated_items,
        "remaining": preview["remaining"],
        "total_allocated": preview["total_allocated"],
        "overwritten": bool(existing),
    }
