"""Capture-only notification test sessions ("Sesi uji", notification-test-session D1-D6).

A session is a time window per owner. A notification whose `post_time` falls inside a window is stored raw and
settled as `ignored` with `error_code = 'test_session'`; nothing books, pairs, asks or learns from it (D3). The
report reads each event through the same deterministic parser as ingest, with no AI.
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException

from app.errors import ErrorCode, api_error
from app.services.notification_resolution import INSTITUTION_LABELS

logger = logging.getLogger(__name__)

MIN_DURATION_MINUTES = 15
MAX_DURATION_MINUTES = 360
CASE_MAX_LENGTH = 8
NOTE_MAX_LENGTH = 400
PREVIEW_LENGTH = 140
TEST_SESSION_CODE = "test_session"
# A cell starting with one of these is read as a formula by spreadsheet apps (CSV injection).
FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")
CSV_COLUMNS = (
    "nomor_kasus", "waktu_notifikasi", "waktu_diterima", "selisih_detik", "aplikasi", "arah", "nominal",
    "teks", "catatan",
)


def not_found() -> HTTPException:
    return api_error(404, ErrorCode.TEST_SESSION_NOT_FOUND)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def session_for_post_time(cur, user_id: str, post_time: datetime) -> dict | None:
    """The owner's session whose window holds `post_time`, open or ended (D2).

    `data_deleted` tells the caller not to store the notification: "Hapus data sesi" removed that session's data,
    and its window still keeps a late upload from booking.
    """
    cur.execute(
        """SELECT id, data_deleted_at IS NOT NULL AS data_deleted FROM notification_test_sessions
           WHERE user_id = %s AND started_at <= %s AND %s < COALESCE(ended_at, planned_end_at)
           ORDER BY started_at DESC LIMIT 1""",
        (user_id, post_time, post_time),
    )
    row = cur.fetchone()
    return {"id": str(row["id"]), "data_deleted": row["data_deleted"]} if row else None


def _status(row: dict, now: datetime) -> str:
    return "open" if row["ended_at"] is None and row["planned_end_at"] > now else "ended"


def _view(row: dict, now: datetime) -> dict[str, Any]:
    status = _status(row, now)
    ends_at = row["ended_at"] or row["planned_end_at"]
    return {
        "id": str(row["id"]),
        "status": status,
        "started_at": row["started_at"].isoformat(),
        "planned_end_at": row["planned_end_at"].isoformat(),
        "ended_at": ends_at.isoformat() if status == "ended" else None,
        "event_count": int(row.get("event_count") or 0),
        "labelled_count": int(row.get("labelled_count") or 0),
    }


SESSION_SELECT = """
    SELECT s.*, COUNT(n.id) AS event_count, COUNT(n.test_case) AS labelled_count
    FROM notification_test_sessions s
    LEFT JOIN notification_events n ON n.user_id = s.user_id AND n.test_session_id = s.id
    WHERE s.user_id = %s AND s.data_deleted_at IS NULL {extra}
    GROUP BY s.id
"""


def _owned(cur, user_id: str, session_id: str, *, for_update: bool = False) -> dict:
    cur.execute(
        "SELECT * FROM notification_test_sessions WHERE user_id = %s AND id = %s AND data_deleted_at IS NULL"
        + (" FOR UPDATE" if for_update else ""),
        (user_id, session_id),
    )
    row = cur.fetchone()
    if not row:
        raise not_found()
    return row


def _session(cur, user_id: str, session_id: str) -> dict[str, Any]:
    cur.execute(SESSION_SELECT.format(extra="AND s.id = %s"), (user_id, session_id))
    row = cur.fetchone()
    if not row:
        raise not_found()
    return _view(row, _now())


def close_expired(cur, user_id: str, now: datetime) -> None:
    """An open session past its planned end is closed at that end, so a new one can start (D1)."""
    cur.execute(
        """UPDATE notification_test_sessions SET ended_at = planned_end_at
           WHERE user_id = %s AND ended_at IS NULL AND planned_end_at <= %s""",
        (user_id, now),
    )


def start_session(cur, user_id: str, duration_minutes: int) -> dict[str, Any]:
    now = _now()
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"test-session:{user_id}",))
    close_expired(cur, user_id, now)
    cur.execute("SELECT 1 FROM notification_test_sessions WHERE user_id = %s AND ended_at IS NULL", (user_id,))
    if cur.fetchone():
        raise api_error(409, ErrorCode.TEST_SESSION_OPEN)
    cur.execute(
        """INSERT INTO notification_test_sessions (user_id, started_at, planned_end_at)
           VALUES (%s, %s, %s) RETURNING id""",
        (user_id, now, now + timedelta(minutes=duration_minutes)),
    )
    session_id = str(cur.fetchone()["id"])
    logger.info("Test session started minutes=%s", duration_minutes)
    return _session(cur, user_id, session_id)


def end_session(cur, user_id: str, session_id: str) -> dict[str, Any]:
    """Ends an open session now; ending an ended session again changes nothing."""
    row = _owned(cur, user_id, session_id, for_update=True)
    now = _now()
    if row["ended_at"] is None:
        cur.execute(
            "UPDATE notification_test_sessions SET ended_at = LEAST(%s, planned_end_at) WHERE id = %s",
            (now, session_id),
        )
        logger.info("Test session ended")
    return _session(cur, user_id, session_id)


def current_session(cur, user_id: str) -> dict[str, Any] | None:
    now = _now()
    cur.execute(
        SESSION_SELECT.format(extra="AND s.ended_at IS NULL AND s.planned_end_at > %s"), (user_id, now),
    )
    row = cur.fetchone()
    return _view(row, now) if row else None


def list_sessions(cur, user_id: str) -> list[dict[str, Any]]:
    now = _now()
    cur.execute(SESSION_SELECT.format(extra="") + " ORDER BY s.started_at DESC", (user_id,))
    return [_view(row, now) for row in cur.fetchall()]


def _event_view(event: dict) -> dict[str, Any]:
    from app.services.notification_evidence import collect_facts
    from app.services.notification_processing import source_event

    facts = collect_facts(source_event(event))
    arrived = event["created_at"]
    posted = event["post_time"]
    return {
        "event_id": str(event["id"]),
        "test_case": event.get("test_case"),
        "label_notes": event.get("label_notes"),
        "post_time": posted.isoformat(),
        "received_at": arrived.isoformat(),
        "gap_seconds": round((arrived - posted).total_seconds(), 1),
        "package_name": event["package_name"],
        "app_label": INSTITUTION_LABELS.get(facts.institution or "", event.get("app_label") or event["package_name"]),
        "direction": facts.direction,
        "amount": facts.amount,
        "parse_status": facts.status,
        "preview": (facts.text or "")[:PREVIEW_LENGTH],
    }


def session_events(cur, user_id: str, session_id: str) -> list[dict[str, Any]]:
    _owned(cur, user_id, session_id)
    cur.execute(
        """SELECT * FROM notification_events WHERE user_id = %s AND test_session_id = %s
           ORDER BY post_time, id""",
        (user_id, session_id),
    )
    return [_event_view(event) for event in cur.fetchall()]


def label_event(cur, user_id: str, session_id: str, event_id: str, changes: dict[str, Any]) -> dict[str, Any]:
    """Sets the case number and/or note; an empty value clears it."""
    _owned(cur, user_id, session_id)
    assignments, values = [], []
    for column in ("test_case", "label_notes"):
        if column in changes:
            value = (changes[column] or "").strip() or None
            assignments.append(f"{column} = %s")
            values.append(value)
    if assignments:
        assignments.append("labelled_at = NOW()")
        assignments.append("updated_at = NOW()")
    cur.execute(
        f"""UPDATE notification_events SET {", ".join(assignments) or "updated_at = updated_at"}
            WHERE user_id = %s AND test_session_id = %s AND id = %s RETURNING *""",
        (*values, user_id, session_id, event_id),
    )
    event = cur.fetchone()
    if not event:
        raise not_found()
    return _event_view(event)


def _cell(value: Any) -> str:
    text = "" if value is None else str(value)
    return f"'{text}" if text.startswith(FORMULA_PREFIXES) else text


def export_csv(cur, user_id: str, session_id: str) -> str:
    rows = session_events(cur, user_id, session_id)
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(CSV_COLUMNS)
    for row in rows:
        writer.writerow([_cell(value) for value in (
            row["test_case"], row["post_time"], row["received_at"], row["gap_seconds"], row["app_label"],
            row["direction"], row["amount"], row["preview"], row["label_notes"],
        )])
    return buffer.getvalue()


def delete_session_data(cur, user_id: str, session_id: str) -> None:
    """"Hapus data sesi": the captured notifications go; the window stays so a late upload never books (D6)."""
    row = _owned(cur, user_id, session_id, for_update=True)
    now = _now()
    cur.execute("DELETE FROM notification_events WHERE user_id = %s AND test_session_id = %s", (user_id, session_id))
    deleted = cur.rowcount
    cur.execute(
        """UPDATE notification_test_sessions SET data_deleted_at = %s,
           ended_at = COALESCE(ended_at, LEAST(%s, planned_end_at)) WHERE id = %s""",
        (now, now, row["id"]),
    )
    logger.info("Test session data deleted events=%s", deleted)
