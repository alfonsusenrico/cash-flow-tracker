"""Tinjau's "Terlihat dua kali": second observations of one money movement, and held possible ones.

A second observation books nothing (recognise-one-arrival-seen-twice D1–D3); a held event is one the
model called a second observation without the backend rule confirming it (D5, OD116). Both wait here
for 30 days until the owner confirms them ("Benar, satu transaksi") or records them on their own
("Catat terpisah", undoable). They count as automatic, never as needing attention (OD117).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import HTTPException

from app.db.pool import db_conn
from app.services.notification_evidence import EvidenceError, collect_facts
from app.services.notification_resolution import INSTITUTION_LABELS

SEEN_TWICE_DAYS = 30
SEEN_TWICE_LIMIT = 200

# An event Tinjau lists under "Terlihat dua kali"; `n` is notification_events.
SEEN_TWICE_SQL = """(n.provenance_kind = 'duplicate_observation'
    OR (n.processing_state = 'needs_review' AND n.error_code = 'possible_duplicate_observation'))"""

# The facts a "Catat terpisah" undo puts back; the event's result fields are restored by review undo.
RESTORED_EVENT_FIELDS = (
    "processing_state", "provenance_kind", "error_code", "duplicate_of_transaction_id", "reviewed_at",
    "interpretation",
)


def seen_twice_since(now: datetime | None = None) -> datetime:
    return (now or datetime.now(timezone.utc)) - timedelta(days=SEEN_TWICE_DAYS)


def seen_twice_count(cur, user_id: str, start: datetime | None = None, end: datetime | None = None) -> int:
    query = f"""SELECT COUNT(*) AS count FROM notification_events n
                WHERE n.user_id = %s AND {SEEN_TWICE_SQL} AND n.reviewed_at IS NULL AND n.post_time >= %s"""
    params: list[Any] = [user_id, seen_twice_since()]
    if start is not None and end is not None:
        query += " AND n.post_time >= %s AND n.post_time <= %s"
        params += [start, end]
    cur.execute(query, params)
    return int(cur.fetchone()["count"])


def _account_label(row: dict, prefix: str) -> str | None:
    name = row.get(f"{prefix}_name")
    if not name:
        return None
    parent = row.get(f"{prefix}_parent")
    return f"{parent} · {name}" if parent else name


def _kept_leg(cur, user_id: str, transaction_id: str | None) -> dict | None:
    """The recorded leg an item repeats, with its movement's route when it is one."""
    if not transaction_id:
        return None
    cur.execute(
        """SELECT t.id, t.type, t.amount, t.date, t.movement_id,
                  a.name AS account_name, pa.name AS account_parent,
                  o.type AS other_type, oa.name AS other_name, opa.name AS other_parent
           FROM transactions t
           JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
           LEFT JOIN accounts pa ON pa.id = a.parent_id AND pa.user_id = a.user_id
           LEFT JOIN transactions o ON o.user_id = t.user_id AND o.movement_id = t.movement_id AND o.id <> t.id
           LEFT JOIN accounts oa ON oa.id = o.account_id AND oa.user_id = o.user_id
           LEFT JOIN accounts opa ON opa.id = oa.parent_id AND opa.user_id = oa.user_id
           WHERE t.user_id = %s AND t.id = %s
           LIMIT 1""",
        (user_id, transaction_id),
    )
    row = cur.fetchone()
    if not row:
        return None
    account = _account_label(row, "account")
    other = _account_label(row, "other")
    route = None
    if row["movement_id"] and other:
        source, target = (account, other) if row["type"] == "expense" else (other, account)
        route = {"source": source, "target": target}
    return {
        "transaction_id": str(row["id"]), "type": row["type"], "amount": int(row["amount"]),
        "date": row["date"].isoformat() if row["date"] else None, "account_name": account, "movement": route,
    }


def _held_original(cur, user_id: str, event: dict) -> str | None:
    """The transaction of the neighbour the model named for a held event."""
    neighbour = (event.get("interpretation") or {}).get("same_money_as_event_id")
    if not neighbour:
        return None
    cur.execute(
        """SELECT COALESCE(transaction_id, duplicate_of_transaction_id) AS transaction_id
           FROM notification_events WHERE user_id = %s AND id = %s""",
        (user_id, neighbour),
    )
    row = cur.fetchone()
    return str(row["transaction_id"]) if row and row["transaction_id"] else None


def seen_twice_items(cur, user_id: str) -> list[dict[str, Any]]:
    cur.execute(
        f"""SELECT n.* FROM notification_events n
            WHERE n.user_id = %s AND {SEEN_TWICE_SQL} AND n.reviewed_at IS NULL AND n.post_time >= %s
            ORDER BY n.post_time DESC, n.id LIMIT %s""",
        (user_id, seen_twice_since(), SEEN_TWICE_LIMIT),
    )
    events = cur.fetchall()
    cur.execute(
        """SELECT a.id, a.name, p.name AS parent FROM accounts a
           LEFT JOIN accounts p ON p.id = a.parent_id AND p.user_id = a.user_id WHERE a.user_id = %s""",
        (user_id,),
    )
    labels = {str(row["id"]): f"{row['parent']} · {row['name']}" if row["parent"] else row["name"]
              for row in cur.fetchall()}
    items = []
    for event in events:
        from app.services.notification_processing import source_event

        facts = collect_facts(source_event(event))
        duplicate = event["provenance_kind"] == "duplicate_observation"
        kept_id = (str(event["duplicate_of_transaction_id"]) if event.get("duplicate_of_transaction_id")
                   else None) if duplicate else _held_original(cur, user_id, event)
        account_id = (event.get("interpretation") or {}).get("account_id")
        items.append({
            "event_id": str(event["id"]), "kind": "duplicate" if duplicate else "held",
            "app": INSTITUTION_LABELS.get(facts.institution or "", event.get("app_label") or "Aplikasi"),
            "type": facts.direction if facts.direction in {"expense", "income"} else None,
            "amount": facts.amount, "date": event["post_time"].isoformat(),
            "account_name": labels.get(str(account_id)) if account_id else None,
            "kept": _kept_leg(cur, user_id, kept_id),
        })
    return items


def _locked_item(cur, user_id: str, event_id: str) -> dict:
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{user_id}",))
    cur.execute(
        f"SELECT n.* FROM notification_events n WHERE n.user_id = %s AND n.id = %s AND {SEEN_TWICE_SQL} FOR UPDATE",
        (user_id, event_id),
    )
    event = cur.fetchone()
    if not event:
        raise HTTPException(status_code=404, detail={"code": "seen_twice_not_found",
                                                     "message": "Item ini tidak ada di Terlihat dua kali."})
    return event


def acknowledge(user_id: str, event_id: str) -> dict:
    """"Benar, satu transaksi": the owner confirms one money movement; idempotent."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            event = _locked_item(cur, user_id, event_id)
            cur.execute(
                """UPDATE notification_events SET reviewed_at = COALESCE(reviewed_at, NOW()), updated_at = NOW()
                   WHERE user_id = %s AND id = %s""",
                (user_id, str(event["id"])),
            )
        conn.commit()
    return {"ok": True, "event_id": str(event_id)}


def record_separately(user_id: str, event_id: str) -> dict:
    """"Catat terpisah": record the event as its own leg, unpaired, with a review undo token."""
    from app.core.config import settings
    from app.services.notification_application import classify_without_model, record_observed_leg, resolve_record
    from app.services.notification_context import load_context
    from app.services.notification_processing import source_event
    from app.services.notification_source_pocket import resolved_facts
    from app.services.provenance import notification_provenance
    from app.services.review_undo import ActionRecorder

    with db_conn() as conn:
        with conn.cursor() as cur:
            event = _locked_item(cur, user_id, event_id)
            recorder = ActionRecorder(cur, user_id, "seen_twice_separate")
            recorder.capture([], event_ids=[str(event["id"])])
            before = json.loads(json.dumps({field: event.get(field) for field in RESTORED_EVENT_FIELDS}, default=str))
            facts = resolved_facts(cur, event, collect_facts(source_event(event)))
            context = load_context(cur, user_id, facts, history_limit=settings.notification_ai_history_limit,
                                   event_id=str(event["id"]))
            try:
                if context["incomplete"]:
                    raise EvidenceError("incomplete_context")
                proposal, source = classify_without_model(facts, context)
                if proposal.direction not in {"expense", "income"}:
                    raise EvidenceError("unsupported_seen_twice")
                resolved = resolve_record(facts, proposal, context)
                record_observed_leg(
                    cur, event, facts, context, observed=resolved["observed"], category_id=resolved["category_id"],
                    kakeibo=resolved["kakeibo"], description=resolved["description"],
                    signature={**resolved["signature"], "source": source},
                    provenance=notification_provenance(source, proposal.confidence),
                    merchant=proposal.merchant, arrival_guard=False, pair_counterpart=False,
                )
            except EvidenceError as exc:
                raise HTTPException(status_code=422, detail={
                    "code": exc.code, "message": "Notifikasi ini tidak dapat dicatat terpisah.",
                }) from None
            cur.execute(
                """UPDATE notification_events SET duplicate_of_transaction_id = NULL, reviewed_at = NOW(),
                       updated_at = NOW()
                   WHERE user_id = %s AND id = %s RETURNING transaction_id""",
                (user_id, str(event["id"])),
            )
            transaction_id = cur.fetchone()["transaction_id"]
            created = [str(transaction_id)] if transaction_id else []
            undo = recorder.finish(created_transaction_ids=created, extra={"seen_twice": {
                "event_id": str(event["id"]), "before": before,
            }})
        conn.commit()
    return {"ok": True, "transaction_id": created[0] if created else None, **undo}


def restore_seen_twice(cur, user_id: str, record: dict) -> None:
    """Undo of "Catat terpisah": the event is a second observation (or a hold) again."""
    before = record["before"]
    kept = before.get("duplicate_of_transaction_id")
    if kept:
        cur.execute("SELECT 1 FROM transactions WHERE user_id = %s AND id = %s", (user_id, kept))
        if not cur.fetchone():
            kept = None
    cur.execute(
        """UPDATE notification_events SET processing_state = %s, provenance_kind = %s, error_code = %s,
               transaction_id = NULL, duplicate_of_transaction_id = %s, reviewed_at = %s, interpretation = %s,
               updated_at = NOW()
           WHERE user_id = %s AND id = %s""",
        (before["processing_state"], before["provenance_kind"], before["error_code"], kept, before["reviewed_at"],
         json.dumps(before["interpretation"]) if before.get("interpretation") is not None else None,
         user_id, record["event_id"]),
    )
