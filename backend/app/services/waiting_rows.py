"""Rows the Tinjau badge counts, with the Tinjau section each one is answered in.

Beranda, the Analisis calendar and the money flow read the same rows with the same SQL as the badge
(`REVIEWABLE_ROW_SQL AND ATTENTION_SQL`), so a waiting row is counted alike everywhere and never moved
silently into a total it is not part of.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from app.routers.review import review_reasons
from app.services.kakeibo import pillar_bucket_sql
from app.services.monthly_report import ATTENTION_SQL, OPEN_QUESTION_JOIN_SQL, REVIEWABLE_ROW_SQL
from app.services.report_figures import reportable_expense_sql

# Tinjau sections in the order the review page shows them.
WAITING_SECTIONS: tuple[str, ...] = ("questions", "ambiguous", "awaiting", "groups")
# Sections whose rows wait for a movement counterpart rather than for a category answer.
COUNTERPART_SECTIONS: frozenset[str] = frozenset({"ambiguous", "awaiting"})


def waiting_section(row: dict[str, Any]) -> str:
    """The Tinjau section a waiting row is answered in, without the per-row candidate lookups.

    A leg the pairing flagged as ambiguous is listed under "ambiguous" even when its candidates have
    since been taken; the review page then shows it with the awaiting legs or the groups.
    """
    if row["has_open_question"]:
        return "questions"
    reasons = review_reasons(row)
    if row["review_status"] == "needs_review" and "ambiguous_movement" in reasons:
        return "ambiguous"
    if row["movement_category"] and "awaiting_counterpart" in reasons:
        return "awaiting"
    return "groups"


def fetch_waiting_rows(cur, user_id: str, start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Every waiting row of the window in one statement, newest first, each with its `section`."""
    cur.execute(
        f"""
        SELECT t.id, t.type, t.amount, t.date, t.review_status, t.movement_id, t.merchant, t.notes,
               t.account_id, a.name AS account_name, c.name AS category_name,
               COALESCE(c.name = 'Internal Movement', FALSE) AS movement_category,
               {reportable_expense_sql()} AS in_keluar,
               {pillar_bucket_sql()} AS bucket,
               ev.interpretation -> 'review_reasons' AS review_reasons,
               question.id IS NOT NULL AS has_open_question
        FROM transactions t
        LEFT JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
        LEFT JOIN categories c ON c.id = t.category_id
        LEFT JOIN LATERAL (
            SELECT e.interpretation FROM notification_events e
            WHERE e.user_id = t.user_id AND e.transaction_id = t.id
            ORDER BY e.post_time, e.id LIMIT 1
        ) ev ON TRUE
        {OPEN_QUESTION_JOIN_SQL}
        WHERE t.user_id = %s AND {REVIEWABLE_ROW_SQL} AND {ATTENTION_SQL}
          AND t.date >= %s AND t.date <= %s
        ORDER BY t.date DESC, t.id
        """,
        (user_id, start, end),
    )
    rows = []
    for row in cur.fetchall():
        rows.append({**row, "section": waiting_section(row)})
    return rows


def waiting_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Beranda's "waiting" figure: the rows by section and whether Keluar already holds each one."""
    lines = []
    for row in rows:
        in_keluar = bool(row["in_keluar"])
        lines.append({
            "section": row["section"],
            "transaction_id": str(row["id"]),
            "label": row["merchant"] or row["notes"] or row["category_name"] or "Tanpa keterangan",
            "account_name": row["account_name"],
            "type": row["type"],
            "date": row["date"].isoformat(),
            "amount": int(row["amount"]),
            "in_keluar": in_keluar,
            "in_keluar_as": row["bucket"] if in_keluar else None,
        })
    lines.sort(key=lambda line: WAITING_SECTIONS.index(line["section"]))
    in_keluar_total = sum(line["amount"] for line in lines if line["in_keluar"])
    total = sum(line["amount"] for line in lines)
    return {
        "count": len(lines),
        "total": total,
        "in_keluar": in_keluar_total,
        "not_in_keluar": total - in_keluar_total,
        "lines": lines,
    }
