"""Export one Sesi uji session for a local replay (notification-test-session D7).

Read-only: everything runs in a `SET TRANSACTION READ ONLY` transaction, and nothing is changed. The
export holds the session's notifications (with their texts) and the owner's account structure, so it is
personal data: write it outside the repository, replay it locally, then delete it.

    docker exec -i -e PGOPTIONS="-c default_transaction_read_only=on" ledger_api \\
        python - --user-id <uuid> --session <uuid> < scripts/verify/export_test_session.py > <scratch>/export.json
    PYTHONPATH=backend python3 scripts/verify/export_test_session.py --user-id <uuid> --session <uuid> --out <scratch>/export.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

# From the repository the backend sits two levels up; piped into a container (`python -`) it is already
# importable and `__file__` is "<stdin>".
_SCRIPT = Path(globals().get("__file__", "<stdin>")).resolve()
REPOSITORY = _SCRIPT.parents[2] if len(_SCRIPT.parents) > 2 and (_SCRIPT.parents[2] / "backend").is_dir() else None
if REPOSITORY:
    sys.path.insert(0, str(REPOSITORY / "backend"))

EXPORT_VERSION = 1
EVENT_COLUMNS = (
    "id", "device_id", "package_name", "app_label", "notification_key", "notification_id", "channel_id", "category",
    "title", "body_text", "big_text", "sub_text", "summary_text", "post_time", "payload_hash", "raw_extras",
    "source_version", "is_financial", "event_class", "expected_amount", "expected_direction", "expected_counterparty",
    "test_case", "label_notes", "created_at",
)
# The owner's structure the pipeline reads, in insert order. Each table has a `user_id` column.
STRUCTURE_TABLES = (
    "accounts", "categories", "notification_account_aliases", "notification_sender_aliases",
    "account_report_links", "merchant_memory",
)
USER_COLUMNS = ("name", "name_aliases", "currency")


def refuse_repository_path(path: Path, repository: Path | None = REPOSITORY) -> None:
    """Personal data never lands in the repository, where it could be committed."""
    if repository is None:
        return
    resolved = path.resolve()
    if resolved == repository or repository in resolved.parents:
        raise SystemExit(f"Refusing to write an export inside the repository: {resolved}")


def _plain(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (UUID, Decimal)):
        return str(value)
    return value


def _rows(cur, query: str, params: tuple) -> list[dict]:
    cur.execute(query, params)
    return [{key: _plain(value) for key, value in row.items()} for row in cur.fetchall()]


def export(cur, user_id: str, session_id: str) -> dict:
    cur.execute(
        "SELECT * FROM notification_test_sessions WHERE user_id = %s AND id = %s AND data_deleted_at IS NULL",
        (user_id, session_id),
    )
    session = cur.fetchone()
    if not session:
        raise SystemExit("Session not found for this owner, or its data was deleted")
    events = _rows(
        cur,
        f"SELECT {', '.join(EVENT_COLUMNS)} FROM notification_events WHERE user_id = %s AND test_session_id = %s "
        "ORDER BY post_time, id",
        (user_id, session_id),
    )
    cur.execute(f"SELECT {', '.join(USER_COLUMNS)} FROM users WHERE id = %s", (user_id,))
    structure = {"user": {key: _plain(value) for key, value in (cur.fetchone() or {}).items()}}
    for table in STRUCTURE_TABLES:
        structure[table] = _rows(cur, f"SELECT * FROM {table} WHERE user_id = %s", (user_id,))
    return {
        "export_version": EXPORT_VERSION,
        "session": {key: _plain(session[key]) for key in ("id", "started_at", "planned_end_at", "ended_at")},
        "events": events,
        "structure": structure,
        "counts": {"events": len(events), **{table: len(structure[table]) for table in STRUCTURE_TABLES}},
    }


def run(user_id: str, session_id: str) -> dict:
    from app.db.pool import DB_POOL, db_conn

    DB_POOL.open(wait=True)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            data = export(cur, user_id, session_id)
        conn.rollback()
    return data


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only export of one Sesi uji session for a local replay")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--session", required=True)
    parser.add_argument("--out", type=Path, help="write here instead of standard output; never inside the repository")
    arguments = parser.parse_args(argv)
    if arguments.out:
        refuse_repository_path(arguments.out)
    body = json.dumps(run(arguments.user_id, arguments.session), ensure_ascii=False)
    if arguments.out:
        arguments.out.write_text(body, encoding="utf-8")
        print(f"Wrote {arguments.out}", file=sys.stderr)
    else:
        print(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
