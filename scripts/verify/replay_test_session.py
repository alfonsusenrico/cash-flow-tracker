"""Replay a Sesi uji export on a local database through the real pipeline (notification-test-session D7).

The export (from `export_test_session.py`) is loaded for a fresh synthetic owner on a loopback database that
already has the migrations of the revision under test. Its notifications go through `accept_event` in
`post_time` order, without their session tags, then the deferred pairing pass runs. The result lists what
each notification became and a summary per case number, for `score_test_session.py`.

    PYTHONPATH=<worktree>/backend python3 scripts/verify/replay_test_session.py \\
        --export <scratch>/export.json --database-url postgresql://ledger:...@127.0.0.1:55642/replay \\
        [--backend <worktree>/backend] [--drop-app com.gojek.gopay] [--ai] --out <scratch>/replay.json

`--drop-app` removes one app's notifications, which turns a two-sided movement into one-sided income or
expense. `--ai` uses the model and needs `OPENAI_API_KEY`; without it every notification is read
deterministically. Never point this at production: only loopback hosts are accepted.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

_SCRIPT = Path(__file__).resolve()
REPOSITORY = _SCRIPT.parents[2]
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
PAYLOAD_FIELDS = (
    "device_id", "package_name", "app_label", "notification_key", "notification_id", "channel_id", "category",
    "title", "body_text", "big_text", "sub_text", "summary_text", "post_time", "payload_hash", "raw_extras",
    "source_version", "is_financial", "event_class", "expected_amount", "expected_direction", "expected_counterparty",
)
# Insert order follows the foreign keys; account self-references are filled in a second pass.
ACCOUNT_SELF_REFERENCES = ("default_pocket_id", "default_funding_account_id")
STRUCTURE_ORDER = (
    "categories", "notification_account_aliases", "notification_sender_aliases", "account_report_links",
    "merchant_memory",
)
NULLED_REFERENCES = {"account_report_links": ("converted_from_pattern_id",)}


def refuse_remote(database_url: str) -> None:
    host = urlparse(database_url).hostname
    if host not in LOOPBACK_HOSTS:
        raise SystemExit(f"Refusing a non-loopback database host: {host!r}. Replays run on local databases only.")


def refuse_repository_path(path: Path) -> None:
    resolved = path.resolve()
    if resolved == REPOSITORY or REPOSITORY in resolved.parents:
        raise SystemExit(f"Refusing to write replay output inside the repository: {resolved}")


def configure(database_url: str, backend: Path | None) -> None:
    """Points the app at the local database before any app module is imported."""
    refuse_remote(database_url)
    os.environ["DATABASE_URL"] = database_url
    for name, value in (("SESSION_SECRET", "replay-only-secret"), ("INVITE_CODE", "REPLAY"), ("APP_ENV", "test"),
                        ("REDIS_URL", ""), ("BACKGROUND_JOBS_ENABLED", "false"), ("PLAN_AUTO_LINK_ENABLED", "false")):
        os.environ.setdefault(name, value)
    sys.path.insert(0, str(backend or REPOSITORY / "backend"))


def _columns(cur, table: str) -> dict[str, str]:
    """Column name to data type, so JSON values are sent as JSON and lists as arrays."""
    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table,))
    return {row["column_name"]: row["data_type"] for row in cur.fetchall()}


def _insert(cur, table: str, row: dict, columns: dict[str, str]) -> None:
    from psycopg.types.json import Jsonb

    names = [name for name in row if name in columns]
    values = [Jsonb(row[name]) if columns[name] in {"json", "jsonb"} and row[name] is not None else row[name]
              for name in names]
    cur.execute(
        f"INSERT INTO {table} ({', '.join(names)}) VALUES ({', '.join(['%s'] * len(names))})",
        values,
    )


def remap_ids(structure: dict) -> dict:
    """Gives every exported row a new id and rewrites each reference to it.

    The copy may share a database with the exported owner (a test, or a second replay), so ids must not
    collide. Ids are UUIDs, so any string equal to an exported id is a reference to that row.
    """
    mapping = {row["id"]: str(uuid4()) for rows in structure.values() if isinstance(rows, list)
               for row in rows if isinstance(row, dict) and row.get("id")}

    def rewrite(value):
        if isinstance(value, str):
            return mapping.get(value, value)
        if isinstance(value, list):
            return [rewrite(item) for item in value]
        if isinstance(value, dict):
            return {key: rewrite(item) for key, item in value.items()}
        return value

    return {key: rewrite(rows) if isinstance(rows, list) else rows for key, rows in structure.items()}


def load_structure(cur, structure: dict) -> str:
    """Creates a synthetic owner holding the exported structure under new ids."""
    structure = remap_ids(structure)
    owner = str(uuid4())
    user = structure.get("user") or {}
    user_columns = _columns(cur, "users")
    _insert(cur, "users", {
        "id": owner, "username": f"replay_{uuid4().hex[:12]}", "password_hash": "replay-only", "invite_code": "REPLAY",
        **{key: value for key, value in user.items() if key in {"name", "name_aliases", "currency"}},
    }, user_columns)

    accounts = [{**row, "user_id": owner} for row in structure.get("accounts", [])]
    account_columns = _columns(cur, "accounts")
    pending = sorted(accounts, key=lambda row: row.get("parent_id") is not None)
    for row in pending:
        _insert(cur, "accounts", {**row, **{name: None for name in ACCOUNT_SELF_REFERENCES}}, account_columns)
    for row in accounts:
        for name in ACCOUNT_SELF_REFERENCES:
            if row.get(name) and name in account_columns:
                cur.execute(f"UPDATE accounts SET {name} = %s WHERE user_id = %s AND id = %s", (row[name], owner, row["id"]))

    for table in STRUCTURE_ORDER:
        columns = _columns(cur, table)
        if not columns:
            continue
        for row in structure.get(table, []):
            copy = {**row, "user_id": owner, **{name: None for name in NULLED_REFERENCES.get(table, ())}}
            _insert(cur, table, copy, columns)
    return owner


def payloads(events: list[dict], drop_apps: set[str]) -> list[tuple[dict, str | None]]:
    selected = []
    for event in sorted(events, key=lambda row: (row["post_time"], row["id"])):
        if event["package_name"].lower() in drop_apps:
            continue
        payload = {name: event.get(name) for name in PAYLOAD_FIELDS}
        payload["post_time"] = datetime.fromisoformat(event["post_time"])
        if payload.get("expected_amount") is not None:
            payload["expected_amount"] = Decimal(str(payload["expected_amount"]))
        selected.append((payload, event.get("test_case")))
    return selected


def _ai_worker(owner: str) -> int:
    """Processes the owner's queued events with the model until none are left; returns how many ran."""
    import asyncio

    import httpx

    from app.services import notification_processing as processing
    from app.services.openai_notification_provider import OpenAINotificationProvider

    settings = processing.settings
    if not settings.openai_api_key:
        raise SystemExit("--ai needs OPENAI_API_KEY")

    async def drain() -> int:
        processed = 0
        async with httpx.AsyncClient(follow_redirects=False, trust_env=False) as client:
            provider = OpenAINotificationProvider(
                client, api_key=settings.openai_api_key, model=settings.notification_ai_model,
                reasoning_effort=settings.notification_ai_reasoning_effort, timeout=settings.notification_ai_timeout,
            )
            while (claim := processing.claim_event(include_ai=True)) is not None:
                if str(claim["user_id"]) != owner:
                    raise SystemExit("The local database holds other owners' queued events; replay on an empty one")
                await processing.process_claim(claim, provider)
                processed += 1
        return processed

    return asyncio.run(drain())


OUTCOME_SQL = """
    SELECT n.payload_hash, n.package_name, n.post_time, n.processing_state, n.error_code, n.provenance_kind,
           n.reviewed_at IS NOT NULL AS reviewed, n.active_question ->> 'type' AS question,
           n.duplicate_of_transaction_id, n.reporting_link_id IS NOT NULL AS link_skipped,
           COALESCE(n.movement_id, t.movement_id) AS movement_id,
           t.type AS transaction_type, t.amount, a.name AS account_name, p.name AS parent_name, c.name AS category_name
    FROM notification_events n
    LEFT JOIN transactions t ON t.id = n.transaction_id AND t.user_id = n.user_id
    LEFT JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
    LEFT JOIN accounts p ON p.id = a.parent_id AND p.user_id = a.user_id
    LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
    WHERE n.user_id = %s ORDER BY n.post_time, n.id
"""


def _outcome(row: dict) -> str:
    if row["processing_state"] == "ignored":
        return "ignored"
    if row["provenance_kind"] == "duplicate_observation":
        return "skipped" if row["reviewed"] or row["link_skipped"] else "held"
    if row["processing_state"] in {"needs_review", "needs_confirmation", "failed", "queued", "processing"}:
        return "review"
    if row["movement_id"]:
        return "movement"
    return row["transaction_type"] or "unbooked"


def summarise(rows: list[dict], cases: dict[str, str | None]) -> dict:
    per_case: dict[str, dict] = {}
    for row in rows:
        case = cases.get(row["payload_hash"]) or "tanpa-nomor"
        summary = per_case.setdefault(case, {"events": 0, "movements": set(), "income": 0, "expense": 0, "held": 0,
                                             "skipped": 0, "review": 0, "ignored": 0, "questions": 0})
        summary["events"] += 1
        outcome = _outcome(row)
        if outcome == "movement":
            summary["movements"].add(str(row["movement_id"]))
        elif outcome in summary:
            summary[outcome] += 1
        if row["question"]:
            summary["questions"] += 1
    return {case: {**summary, "movements": len(summary["movements"])} for case, summary in sorted(per_case.items())}


def replay(data: dict, *, drop_apps: set[str], ai: bool, keep: bool = False) -> dict:
    from dataclasses import replace

    from app.db.pool import DB_POOL, db_conn
    from app.services import notification_processing as processing
    from app.services.movement_pairing import repair_owner

    DB_POOL.open(wait=True)
    with db_conn() as conn:
        with conn.cursor() as cur:
            owner = load_structure(cur, data["structure"])
        conn.commit()
    processing.settings = replace(processing.settings, notification_ai_enabled=ai)
    selected = payloads(data["events"], {app.lower() for app in drop_apps})
    cases = {payload["payload_hash"]: case for payload, case in selected}
    failures = []
    try:
        for payload, _case in selected:
            try:
                processing.accept_event(payload, owner)
            except Exception as exc:  # the replay reports a failing notification and continues with the rest
                failures.append({"payload_hash": payload["payload_hash"], "error": type(exc).__name__})
        processed_by_ai = _ai_worker(owner) if ai else 0
        if selected:
            repair_owner(owner, min(payload["post_time"] for payload, _ in selected) - timedelta(hours=1))
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(OUTCOME_SQL, (owner,))
                rows = cur.fetchall()
        events = [{
            "test_case": cases.get(row["payload_hash"]), "package_name": row["package_name"],
            "post_time": row["post_time"].isoformat(), "outcome": _outcome(row), "state": row["processing_state"],
            "error_code": row["error_code"], "question": row["question"],
            "booking": None if not row["transaction_type"] else {
                "type": row["transaction_type"], "amount": int(row["amount"]),
                "account": f"{row['parent_name']} · {row['account_name']}" if row["parent_name"] else row["account_name"],
                "category": row["category_name"], "movement": bool(row["movement_id"]),
            },
        } for row in rows]
        return {
            "dropped_apps": sorted(drop_apps), "ai": ai, "processed_by_ai": processed_by_ai,
            "failures": failures, "events": events, "cases": summarise(rows, cases),
        }
    finally:
        if not keep:
            with db_conn() as conn:
                conn.execute("DELETE FROM users WHERE id = %s", (owner,))
                conn.commit()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay a Sesi uji export on a local database")
    parser.add_argument("--export", type=Path, required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--backend", type=Path, help="the backend source tree of the revision under test")
    parser.add_argument("--drop-app", action="append", default=[], help="package name to leave out; repeatable")
    parser.add_argument("--ai", action="store_true")
    parser.add_argument("--keep", action="store_true", help="keep the synthetic owner for inspection")
    parser.add_argument("--out", type=Path)
    arguments = parser.parse_args(argv)
    configure(arguments.database_url, arguments.backend)
    if arguments.out:
        refuse_repository_path(arguments.out)
    data = json.loads(arguments.export.read_text(encoding="utf-8"))
    result = replay(data, drop_apps=set(arguments.drop_app), ai=arguments.ai, keep=arguments.keep)
    body = json.dumps(result, indent=2, ensure_ascii=False)
    if arguments.out:
        arguments.out.write_text(body, encoding="utf-8")
    else:
        print(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
