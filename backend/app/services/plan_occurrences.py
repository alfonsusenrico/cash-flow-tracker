"""Plan occurrences: due dates, links to recorded transactions, and automatic linking of a certain match.

A plan never creates a transaction. An occurrence is linked to a transaction that was already
recorded, either by the owner or automatically when exactly one unlinked transaction of the same
type, account and amount was recorded near the due date and no other due plan could claim it. The
owner can undo any link; the pair is then never linked automatically again.
"""

from __future__ import annotations

import calendar
import logging
import threading
from collections.abc import Callable
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from fastapi import HTTPException
from psycopg import errors as pg_errors

from app.core.config import settings
from app.db.pool import db_conn
from app.services.debt_allocations import reverse_allocations
from app.services.payment_links import link_payment_to_obligation, lock_payment

logger = logging.getLogger(__name__)

RESOLVED_STATUSES = ("succeeded", "linked", "skipped", "processing")
AUTO_LINK_TYPES = ("expense", "income")
AUTO_LINK_DAYS_BEFORE = 3
AUTO_LINK_DAYS_AFTER = 7
RECENT_AUTO_LINK_DAYS = 7
# A schedule cannot need more steps than this to pass resolved occurrences; it guards a broken rule.
MAX_ADVANCE_STEPS = 400


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


def next_unresolved_due(cur, rule: dict[str, Any], payday_day: int, after: date) -> date:
    """The first due date after `after` that is not already resolved.

    An undone link moves a plan back to an earlier occurrence; resolving that one must not land on
    a later occurrence that was already met.
    """
    due = after
    for _ in range(MAX_ADVANCE_STEPS):
        due = calculate_next_due_date(
            schedule_type=rule["schedule_type"], schedule_day=rule["schedule_day"],
            user_payday_day=payday_day, from_date=due, advance=True,
        )
        cur.execute(
            "SELECT 1 FROM recurring_executions WHERE recurring_rule_id = %s AND scheduled_for = %s AND status = ANY(%s)",
            (str(rule["id"]), due, list(RESOLVED_STATUSES)),
        )
        if not cur.fetchone():
            return due
    raise RuntimeError("plan schedule did not reach an unresolved occurrence")


def advance_rule(cur, rule: dict[str, Any], user_id: str, payday_day: int, scheduled_for: date) -> date:
    next_due = next_unresolved_due(cur, rule, payday_day, scheduled_for)
    cur.execute(
        "UPDATE recurring_rules SET next_due_date = %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
        (next_due, str(rule["id"]), user_id),
    )
    return next_due


def record_link_event(
    cur, user_id: str, rule_id: str, scheduled_for: date, transaction_id: str | None, *, action: str, method: str,
) -> None:
    cur.execute(
        """INSERT INTO recurring_link_events (user_id, recurring_rule_id, scheduled_for, transaction_id, action, method)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (user_id, rule_id, scheduled_for, transaction_id, action, method),
    )


def link_transaction(
    cur, user_id: str, rule: dict[str, Any], scheduled_for: date, transaction: dict[str, Any], *, method: str,
) -> bool:
    """Mark the occurrence as met by a recorded transaction; returns whether debt progress changed.

    The caller holds the rule and occurrence locks. The partial unique index on linked transactions
    refuses a second occurrence for the same transaction even under concurrent requests.
    """
    transaction_id = str(transaction["id"])
    cur.execute(
        "SELECT 1 FROM recurring_executions WHERE transaction_id = %s AND status IN ('linked', 'succeeded')",
        (transaction_id,),
    )
    if cur.fetchone():
        raise HTTPException(
            status_code=409,
            detail={"code": "transaction_already_matched", "message": "Transaksi ini sudah dipakai untuk kejadian lain."},
        )
    debt_changed = False
    if rule["type"] == "expense" and rule.get("obligation_id"):
        debt_changed = link_payment_to_obligation(cur, user_id, transaction, str(rule["obligation_id"]))
    cur.execute(
        """
        INSERT INTO recurring_executions (
            recurring_rule_id, user_id, scheduled_for, status, transaction_id, movement_id, link_method, obligation_applied
        )
        VALUES (%s, %s, %s, 'linked', %s, %s, %s, %s)
        ON CONFLICT (recurring_rule_id, scheduled_for) DO UPDATE
        SET status = 'linked', transaction_id = EXCLUDED.transaction_id, movement_id = EXCLUDED.movement_id,
            link_method = EXCLUDED.link_method, obligation_applied = EXCLUDED.obligation_applied,
            error_code = NULL, error_detail = NULL, updated_at = NOW()
        """,
        (str(rule["id"]), user_id, scheduled_for, transaction_id, transaction["movement_id"], method, debt_changed),
    )
    record_link_event(cur, user_id, str(rule["id"]), scheduled_for, transaction_id, action="link", method=method)
    return debt_changed


def unlink_occurrence(cur, user_id: str, rule: dict[str, Any], scheduled_for: date) -> dict[str, Any]:
    """Undo a link: the occurrence is due again and debt progress that the link applied is reversed once."""
    rule_id = str(rule["id"])
    cur.execute(
        """
        SELECT id, status, transaction_id, link_method, obligation_applied FROM recurring_executions
        WHERE recurring_rule_id = %s AND scheduled_for = %s FOR UPDATE
        """,
        (rule_id, scheduled_for),
    )
    occurrence = cur.fetchone()
    if not occurrence or occurrence["status"] != "linked":
        return {"changed": False, "debt_changed": False, "next_due_date": rule["next_due_date"].isoformat()}
    transaction_id = str(occurrence["transaction_id"]) if occurrence["transaction_id"] else None
    debt_changed = False
    if occurrence["obligation_applied"] and transaction_id and rule.get("obligation_id"):
        cur.execute(
            """DELETE FROM transaction_obligation_allocations WHERE transaction_id = %s AND obligation_id = %s
               RETURNING amount""",
            (transaction_id, str(rule["obligation_id"])),
        )
        removed = cur.fetchone()
        if removed:
            reverse_allocations(cur, user_id, [(str(rule["obligation_id"]), int(removed["amount"]))])
            debt_changed = True
    if transaction_id:
        cur.execute(
            """INSERT INTO recurring_link_rejections (user_id, recurring_rule_id, transaction_id, scheduled_for)
               VALUES (%s, %s, %s, %s) ON CONFLICT (recurring_rule_id, transaction_id) DO NOTHING""",
            (user_id, rule_id, transaction_id, scheduled_for),
        )
    cur.execute("DELETE FROM recurring_executions WHERE id = %s", (occurrence["id"],))
    cur.execute(
        """UPDATE recurring_rules SET next_due_date = LEAST(next_due_date, %s), updated_at = NOW()
           WHERE id = %s AND user_id = %s RETURNING next_due_date""",
        (scheduled_for, rule_id, user_id),
    )
    next_due = cur.fetchone()["next_due_date"]
    record_link_event(cur, user_id, rule_id, scheduled_for, transaction_id, action="unlink", method="owner")
    return {"changed": True, "debt_changed": debt_changed, "next_due_date": next_due.isoformat()}


def _local_start(day: date) -> datetime:
    return datetime.combine(day, time.min, tzinfo=ZoneInfo(settings.tz)).astimezone(timezone.utc)


def exact_candidates(cur, user_id: str, rule: dict[str, Any], scheduled_for: date, *, limit: int = 2) -> list[str]:
    """Unlinked transactions that meet every automatic-linking condition for this occurrence."""
    conditions = [
        "t.user_id = %s", "t.type = %s", "t.account_id = %s", "t.amount = %s",
        "t.movement_id IS NULL", "t.fee_for_movement_id IS NULL",
        "t.date >= %s", "t.date < %s", "t.date <= NOW()",
        "NOT EXISTS (SELECT 1 FROM recurring_executions e WHERE e.transaction_id = t.id AND e.status IN ('linked', 'succeeded'))",
        "NOT EXISTS (SELECT 1 FROM recurring_link_rejections r WHERE r.recurring_rule_id = %s AND r.transaction_id = t.id)",
    ]
    params: list[Any] = [
        user_id, rule["type"], str(rule["source_account_id"]), int(rule["amount"]),
        _local_start(scheduled_for - timedelta(days=AUTO_LINK_DAYS_BEFORE)),
        _local_start(scheduled_for + timedelta(days=AUTO_LINK_DAYS_AFTER + 1)),
        str(rule["id"]),
    ]
    if rule["type"] == "expense" and rule.get("obligation_id"):
        conditions.append("a.type <> 'investment'")
        conditions.append(
            "((t.obligation_id IS NULL AND NOT EXISTS (SELECT 1 FROM transaction_obligation_allocations x WHERE x.transaction_id = t.id))"
            " OR t.obligation_id = %s"
            " OR EXISTS (SELECT 1 FROM transaction_obligation_allocations x WHERE x.transaction_id = t.id AND x.obligation_id = %s))"
        )
        params += [str(rule["obligation_id"]), str(rule["obligation_id"])]
    cur.execute(
        f"""SELECT t.id FROM transactions t JOIN accounts a ON a.id = t.account_id
            WHERE {' AND '.join(conditions)} ORDER BY t.id LIMIT %s""",
        (*params, limit),
    )
    return [str(row["id"]) for row in cur.fetchall()]


def _other_claimants(cur, user_id: str, rule_id: str, transaction_id: str, today: date) -> bool:
    """Whether another due, unresolved plan could claim the same transaction."""
    cur.execute(
        """
        SELECT 1
        FROM recurring_rules r
        JOIN transactions t ON t.id = %s AND t.user_id = r.user_id
        WHERE r.user_id = %s AND r.id <> %s AND r.is_active AND r.next_due_date <= %s
          AND r.type = t.type AND r.source_account_id = t.account_id AND r.amount = t.amount
          AND t.date >= ((r.next_due_date - %s)::timestamp AT TIME ZONE %s)
          AND t.date < ((r.next_due_date + %s)::timestamp AT TIME ZONE %s)
          AND NOT EXISTS (
              SELECT 1 FROM recurring_executions e
              WHERE e.recurring_rule_id = r.id AND e.scheduled_for = r.next_due_date AND e.status = ANY(%s))
          AND NOT EXISTS (
              SELECT 1 FROM recurring_link_rejections j WHERE j.recurring_rule_id = r.id AND j.transaction_id = t.id)
        LIMIT 1
        """,
        (
            transaction_id, user_id, rule_id, today,
            AUTO_LINK_DAYS_BEFORE, settings.tz, AUTO_LINK_DAYS_AFTER + 1, settings.tz,
            list(RESOLVED_STATUSES),
        ),
    )
    return cur.fetchone() is not None


def _due_rules(cur, user_id: str, today: date) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT r.* FROM recurring_rules r
        WHERE r.user_id = %s AND r.is_active AND r.type = ANY(%s) AND r.next_due_date <= %s
          AND NOT EXISTS (
              SELECT 1 FROM recurring_executions e
              WHERE e.recurring_rule_id = r.id AND e.scheduled_for = r.next_due_date AND e.status = ANY(%s))
        ORDER BY r.next_due_date, r.id
        """,
        (user_id, list(AUTO_LINK_TYPES), today, list(RESOLVED_STATUSES)),
    )
    return cur.fetchall()


def _auto_link_one(conn, user_id: str, rule_id: str, scheduled_for: date, today: date, payday_day: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM recurring_rules WHERE id = %s AND user_id = %s FOR UPDATE", (rule_id, user_id))
        rule = cur.fetchone()
        if (
            not rule or not rule["is_active"] or rule["type"] not in AUTO_LINK_TYPES
            or rule["next_due_date"] != scheduled_for or scheduled_for > today
        ):
            conn.rollback()
            return False
        cur.execute(
            "SELECT status FROM recurring_executions WHERE recurring_rule_id = %s AND scheduled_for = %s FOR UPDATE",
            (rule_id, scheduled_for),
        )
        existing = cur.fetchone()
        if existing and existing["status"] != "failed":
            conn.rollback()
            return False
        candidates = exact_candidates(cur, user_id, rule, scheduled_for)
        if len(candidates) != 1 or _other_claimants(cur, user_id, rule_id, candidates[0], today):
            conn.rollback()
            return False
        try:
            transaction = lock_payment(cur, user_id, candidates[0])
            link_transaction(cur, user_id, rule, scheduled_for, transaction, method="auto")
            advance_rule(cur, rule, user_id, payday_day, scheduled_for)
        except (HTTPException, pg_errors.UniqueViolation):
            # Not linkable now (debt capacity, a concurrent link): the occurrence stays with the owner.
            conn.rollback()
            return False
    conn.commit()
    return True


def link_due_plans_for_user(conn, user_id: str, *, now: datetime | None = None) -> dict[str, int]:
    """Link every due occurrence of this owner that has exactly one certain transaction."""
    now = now or datetime.now(timezone.utc)
    today = now.astimezone(ZoneInfo(settings.tz)).date()
    with conn.cursor() as cur:
        rules = _due_rules(cur, user_id, today)
        cur.execute("SELECT payday_day FROM users WHERE id = %s", (user_id,))
        owner = cur.fetchone()
    conn.commit()
    payday_day = int((owner or {}).get("payday_day") or 25)
    linked = 0
    for rule in rules:
        linked += int(_auto_link_one(conn, user_id, str(rule["id"]), rule["next_due_date"], today, payday_day))
    return {"due": len(rules), "linked": linked}


def recent_auto_links(cur, user_id: str, *, now: datetime | None = None) -> list[dict[str, Any]]:
    since = (now or datetime.now(timezone.utc)) - timedelta(days=RECENT_AUTO_LINK_DAYS)
    cur.execute(
        """
        SELECT e.recurring_rule_id, e.scheduled_for, e.transaction_id, e.updated_at,
               r.name, r.amount, r.type, o.name AS obligation_name,
               t.date AS transaction_date, t.merchant, t.notes, a.name AS account_name
        FROM recurring_executions e
        JOIN recurring_rules r ON r.id = e.recurring_rule_id
        LEFT JOIN obligations o ON o.id = r.obligation_id
        LEFT JOIN transactions t ON t.id = e.transaction_id
        LEFT JOIN accounts a ON a.id = t.account_id
        WHERE e.user_id = %s AND e.status = 'linked' AND e.link_method = 'auto' AND e.updated_at >= %s
        ORDER BY e.updated_at DESC
        LIMIT 20
        """,
        (user_id, since),
    )
    return [
        {
            "rule_id": str(row["recurring_rule_id"]),
            "scheduled_for": row["scheduled_for"].isoformat(),
            "linked_at": row["updated_at"].isoformat(),
            "name": row["name"],
            "amount": int(row["amount"]),
            "type": row["type"],
            "obligation_name": row["obligation_name"],
            "transaction_id": str(row["transaction_id"]) if row["transaction_id"] else None,
            "transaction_date": row["transaction_date"].isoformat() if row["transaction_date"] else None,
            "merchant": row["merchant"],
            "notes": row["notes"],
            "account_name": row["account_name"],
        }
        for row in cur.fetchall()
    ]


def process_due_plan_links(after: str | None = None, *, limit: int = 20) -> str | None:
    """Sweep one bounded page of owners with due plans; return the cursor for the next page."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT DISTINCT user_id FROM recurring_rules
                   WHERE is_active AND type = ANY(%s) AND next_due_date <= CURRENT_DATE + 1
                     AND (%s::uuid IS NULL OR user_id > %s::uuid)
                   ORDER BY user_id LIMIT %s""",
                (list(AUTO_LINK_TYPES), after, after, limit),
            )
            owners = [str(row["user_id"]) for row in cur.fetchall()]
    for user_id in owners:
        _link_for_owner(user_id)
    return owners[-1] if len(owners) == limit else None


def _link_for_owner(user_id: str) -> None:
    try:
        with db_conn() as conn:
            link_due_plans_for_user(conn, user_id)
    except Exception as error:
        # One owner's failure must not stop the others; log the reason class only.
        logger.warning("Plan auto-linking failed for an owner: %s", type(error).__name__)


_requests: set[str] = set()
_requests_lock = threading.Lock()
_waker: Callable[[], None] | None = None


def set_plan_link_waker(waker: Callable[[], None] | None) -> None:
    global _waker
    _waker = waker


def request_plan_linking(user_id: str) -> None:
    """Ask the background loop to check this owner's due plans; cheap enough for the ingest path."""
    if not settings.plan_auto_link_enabled:
        return
    with _requests_lock:
        _requests.add(str(user_id))
    waker = _waker
    if waker is not None:
        waker()


def take_plan_link_requests() -> list[str]:
    with _requests_lock:
        owners = sorted(_requests)
        _requests.clear()
    return owners


def link_requested_plans() -> int:
    owners = take_plan_link_requests()
    for user_id in owners:
        _link_for_owner(user_id)
    return len(owners)
