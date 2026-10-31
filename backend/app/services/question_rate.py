"""How often the owner is asked, per week, for automatically recorded payments (learn-habits-to-ask-less D9).

Counts only what was recorded: notification and typed-capture expenses and incomes that are not movement
legs or transfer fees, grouped by the local week of the transaction date. Nothing is estimated.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

MAX_WEEKS = 26
# Interpretation sources of a notification row recorded without a question.
NOTIFICATION_OUTCOMES = {"memory": "memory", "habit": "habit", "ai": "ai"}
# Captures store the classifier of the recorded row; "rule" there means the owner's merchant memory.
CAPTURE_OUTCOMES = {"rule": "memory", "ai": "ai"}


def week_start(day: date) -> date:
    return day - timedelta(days=day.isoweekday() - 1)


def outcome(row: dict) -> tuple[str, bool]:
    """The recording outcome of a row and whether the owner answered its question."""
    if row["source"] == "capture":
        if row["capture_answers"]:
            return "asked", True
        return CAPTURE_OUTCOMES.get(row["capture_classifier"], "other"), False
    interpretation = row["interpretation"] or {}
    source = interpretation.get("source")
    if source == "manual":
        # The owner recorded an event the automation could not: that was a question, answered.
        return "asked", True
    if "purpose_question" in (interpretation.get("review_reasons") or []):
        answered = bool(interpretation.get("purpose_answer")) or row["classified_by"] == "owner"
        return "asked", answered
    return NOTIFICATION_OUTCOMES.get(source, "other"), False


def question_rate(cur, user_id: str, *, weeks: int, today: date, tz: str) -> dict:
    """The last `weeks` local weeks, oldest first; the current week is marked as in progress."""
    zone = ZoneInfo(tz)
    current = week_start(today)
    first = current - timedelta(weeks=weeks - 1)
    start = datetime.combine(first, time.min, zone)
    end = datetime.combine(current + timedelta(days=7), time.min, zone)
    buckets = {
        first + timedelta(weeks=index): {
            "week_start": (first + timedelta(weeks=index)).isoformat(),
            "week_end": (first + timedelta(weeks=index, days=6)).isoformat(),
            "in_progress": index == weeks - 1, "recorded": 0, "asked": 0, "answered": 0,
            "by_memory": 0, "by_habit": 0, "by_ai": 0, "by_other": 0, "corrections": 0, "habit_corrections": 0,
        }
        for index in range(weeks)
    }
    cur.execute(
        """
        SELECT t.date, t.source, t.classified_by, event.interpretation,
               jsonb_array_length(COALESCE(capture.answers, '[]'::jsonb)) > 0 AS capture_answers,
               capture.draft -> 'recorded' ->> 'classified_by' AS capture_classifier
        FROM transactions t
        LEFT JOIN LATERAL (
            SELECT n.interpretation FROM notification_events n
            WHERE t.source = 'notification' AND n.user_id = t.user_id AND n.transaction_id = t.id
            ORDER BY n.post_time, n.id LIMIT 1
        ) event ON TRUE
        LEFT JOIN text_captures capture
            ON t.source = 'capture' AND capture.user_id = t.user_id AND capture.transaction_id = t.id
        WHERE t.user_id = %s AND t.source IN ('notification', 'capture') AND t.type IN ('expense', 'income')
          AND t.movement_id IS NULL AND t.fee_for_movement_id IS NULL AND t.date >= %s AND t.date < %s
        """,
        (user_id, start, end),
    )
    for row in cur.fetchall():
        bucket = buckets[week_start(row["date"].astimezone(zone).date())]
        kind, answered = outcome(row)
        bucket["recorded"] += 1
        if kind == "asked":
            bucket["asked"] += 1
            bucket["answered"] += int(answered)
        else:
            bucket[f"by_{kind}"] += 1
    cur.execute(
        """SELECT created_at, before_classified_by FROM classification_feedback
           WHERE user_id = %s AND kind = 'correction' AND transaction_source IN ('notification', 'capture')
             AND created_at >= %s AND created_at < %s""",
        (user_id, start, end),
    )
    for row in cur.fetchall():
        bucket = buckets[week_start(row["created_at"].astimezone(zone).date())]
        bucket["corrections"] += 1
        bucket["habit_corrections"] += int(row["before_classified_by"] == "habit")
    ordered = [buckets[key] for key in sorted(buckets)]
    return {"weeks": ordered, "has_data": any(week["recorded"] or week["corrections"] for week in ordered)}
