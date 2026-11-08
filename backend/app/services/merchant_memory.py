"""Match keys, owner merchant memory and append-only classification feedback."""

from __future__ import annotations

import json
import unicodedata
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from app.services.kakeibo import effective_pillar
from app.services.notification_evidence import SUPPORTED_PACKAGES, NotificationFacts, collect_facts
from app.services.notification_resolution import names_owner, owner_aliases
from app.services.provenance import MERCHANT_MAX_LENGTH
from app.services.system_categories import INTERNAL_MOVEMENT, is_movement, is_system_sql


# Parser fallbacks that stand in for a merchant the notification did not name.
PLACEHOLDER_COUNTERPARTIES = frozenset({
    "bca payment", "bca", "qris merchant", "debit", "bank jago", "bank jago payment", "transaksi bank",
})
MATCH_KEY_MAX_LENGTH = 220
SIGNATURE_MAX_LENGTH = 200
MEMORY_CONTEXT_LIMIT = 30
FEEDBACK_FIELDS: tuple[str, ...] = (
    "merchant", "notes", "category_id", "kakeibo_type", "type", "offset_category_id",
)


def normalize_key_text(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def merchant_match_key(merchant: str | None) -> str | None:
    normalized = normalize_key_text(merchant or "")
    return f"merchant:{normalized}"[:MATCH_KEY_MAX_LENGTH] if normalized else None


def is_masked(value: str) -> bool:
    return "*" in value


def notification_merchant(facts: NotificationFacts, identity: list[str]) -> str | None:
    """The merchant or counterparty the notification names, or None when it names none."""
    parsed = facts.parsed
    if parsed.own_account_transfer or parsed.rdn_deposit or facts.direction not in {"expense", "income"}:
        return None
    counterparty = " ".join((parsed.counterparty or "").split())
    if not counterparty or normalize_key_text(counterparty) in PLACEHOLDER_COUNTERPARTIES:
        return None
    if is_masked(counterparty) or names_owner(counterparty, identity):
        return None
    return counterparty[:MERCHANT_MAX_LENGTH]


def signature_kind(facts: NotificationFacts) -> str:
    counterparty = normalize_key_text(facts.parsed.counterparty or "")
    if counterparty == "qris merchant":
        return "qris"
    if counterparty == "debit":
        return "debit"
    hint = normalize_key_text(facts.parsed.category_hint or "")
    if facts.institution == "bca" and hint:
        return hint
    if counterparty in {"bank jago", "bank jago payment"}:
        return "payment"
    return hint or "unknown"


def text_signature(facts: NotificationFacts) -> str:
    parts = [facts.institution or "unknown", facts.direction or "unknown", signature_kind(facts)]
    counterparty = (facts.parsed.counterparty or "").strip()
    if counterparty and is_masked(counterparty):
        parts.append(f"from:{normalize_key_text(counterparty)}")
    return "|".join(parts)[:SIGNATURE_MAX_LENGTH]


def classification_key(facts: NotificationFacts, identity: list[str]) -> dict[str, Any] | None:
    """Match key of an expense or income notification (design D2); None for movements and own transfers."""
    if facts.status != "candidate" or facts.direction not in {"expense", "income"}:
        return None
    if facts.parsed.own_account_transfer or facts.parsed.rdn_deposit:
        return None
    if names_owner(facts.parsed.counterparty, identity):
        return None
    signature = text_signature(facts)
    merchant = notification_merchant(facts, identity)
    if merchant:
        return {"match_key": merchant_match_key(merchant), "key_kind": "merchant", "merchant": merchant,
                "text_signature": signature, "merchant_less": False, "institution": facts.institution}
    return {"match_key": f"signature:{signature}"[:MATCH_KEY_MAX_LENGTH], "key_kind": "signature",
            "merchant": None, "text_signature": signature, "merchant_less": True,
            "institution": facts.institution}


def institution_packages(institution: str | None) -> list[str]:
    return sorted(package for package, name in SUPPORTED_PACKAGES.items() if name == institution)


def local_time_features(moment: datetime | None) -> tuple[int | None, int | None]:
    from app.core.config import settings

    if moment is None:
        return None, None
    local = moment.astimezone(ZoneInfo(settings.tz))
    return local.isoweekday(), local.hour


def load_memory(cur, user_id: str, match_key: str | None) -> list[dict]:
    """Usable memory entries, exact key first; entries with an inactive category are left out.

    Entries in the movement category are left out too: they were learned before transfers stopped teaching
    memory, and applying one would file the next report of that wording as a transfer
    (recognise-one-arrival-seen-twice D4, OD121). The rows stay; nothing here deletes owner data.
    """
    cur.execute(
        f"""SELECT m.id, m.match_key, m.key_kind, m.display_merchant, m.notes, m.category_id,
                  m.kakeibo_type, m.source, m.hit_count, m.account_id, c.kind AS category_kind,
                  c.offsets_spending AS category_offsets_spending,
                  CASE WHEN oc.id IS NOT NULL AND oc.is_archived = FALSE AND oc.kind = 'expense'
                       THEN m.offset_category_id END AS offset_category_id
           FROM merchant_memory m
           JOIN categories c ON c.id = m.category_id AND c.user_id = m.user_id AND c.is_archived = FALSE
           LEFT JOIN categories oc ON oc.id = m.offset_category_id AND oc.user_id = m.user_id
           WHERE m.user_id = %s AND NOT {is_system_sql("c", INTERNAL_MOVEMENT)}
           ORDER BY (m.match_key = %s) DESC, (m.source = 'owner') DESC, m.hit_count DESC,
                    m.last_used_at DESC NULLS LAST, m.updated_at DESC, m.id
           LIMIT %s""",
        (user_id, match_key or "", MEMORY_CONTEXT_LIMIT),
    )
    return cur.fetchall()


def owner_memory_hit(entries: list[dict], key: dict | None, direction: str | None) -> dict | None:
    """The owner entry that classifies this notification without the model (design D5)."""
    if not key or key["key_kind"] != "merchant" or not entries:
        return None
    entry = entries[0]
    if (entry["match_key"] != key["match_key"] or entry["key_kind"] != "merchant"
            or entry["source"] != "owner" or entry["category_kind"] != direction):
        return None
    offset = entry["offset_category_id"] if direction == "income" and entry["category_offsets_spending"] else None
    return {
        "memory_id": str(entry["id"]), "merchant": entry["display_merchant"], "notes": entry["notes"],
        "category_id": str(entry["category_id"]),
        "kakeibo": entry["kakeibo_type"] if direction == "expense" else None,
        "offset_category_id": str(offset) if offset else None,
    }


def count_memory_hit(cur, user_id: str, memory_id: str) -> None:
    cur.execute(
        """UPDATE merchant_memory SET hit_count = hit_count + 1, last_used_at = NOW()
           WHERE user_id = %s AND id = %s""",
        (user_id, memory_id),
    )


def upsert_memory(cur, user_id: str, match_key: str, values: dict, source: str) -> None:
    """Remember a classification for a key; an AI value never replaces an owner value.

    Only captures name an account; a write without one keeps the remembered account.
    """
    cur.execute(
        """INSERT INTO merchant_memory (user_id, match_key, key_kind, display_merchant, notes, category_id,
               kakeibo_type, offset_category_id, account_id, source)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (user_id, match_key) DO UPDATE
           SET key_kind = EXCLUDED.key_kind, display_merchant = EXCLUDED.display_merchant,
               notes = EXCLUDED.notes, category_id = EXCLUDED.category_id,
               kakeibo_type = EXCLUDED.kakeibo_type, offset_category_id = EXCLUDED.offset_category_id,
               account_id = COALESCE(EXCLUDED.account_id, merchant_memory.account_id),
               source = EXCLUDED.source, updated_at = NOW()
           WHERE merchant_memory.source = 'ai' OR EXCLUDED.source = 'owner'""",
        (user_id, match_key, "merchant" if match_key.startswith("merchant:") else "signature",
         values.get("merchant"), (values.get("notes") or None) and values["notes"][:500],
         values["category_id"], values.get("kakeibo_type"), values.get("offset_category_id"),
         values.get("account_id"), source),
    )


def jsonable(value):
    return json.loads(json.dumps(value, default=str))


def memory_row(cur, user_id: str, match_key: str) -> dict | None:
    cur.execute(
        """SELECT match_key, key_kind, display_merchant, notes, category_id, kakeibo_type, offset_category_id,
                  account_id, source, hit_count, last_used_at, updated_at
           FROM merchant_memory WHERE user_id = %s AND match_key = %s FOR UPDATE""",
        (user_id, match_key),
    )
    row = cur.fetchone()
    return jsonable(row) if row else None


def restore_memory(cur, user_id: str, learned: dict | None) -> None:
    """Undo one memory write unless something changed the entry since (capture and review undo)."""
    if not learned:
        return
    current = memory_row(cur, user_id, learned["match_key"])
    if current is None or current["updated_at"] != learned["after_updated_at"]:
        return
    before = learned["before"]
    if before is None:
        cur.execute("DELETE FROM merchant_memory WHERE user_id = %s AND match_key = %s",
                    (user_id, learned["match_key"]))
        return
    cur.execute(
        """UPDATE merchant_memory SET display_merchant = %s, notes = %s, category_id = %s, kakeibo_type = %s,
               offset_category_id = %s, account_id = %s, source = %s, hit_count = %s, last_used_at = %s,
               updated_at = %s
           WHERE user_id = %s AND match_key = %s""",
        (before["display_merchant"], before["notes"], before["category_id"], before["kakeibo_type"],
         before["offset_category_id"], before["account_id"], before["source"], before["hit_count"],
         before["last_used_at"], before["updated_at"], user_id, learned["match_key"]),
    )


def snapshot_rows(cur, user_id: str, transaction_ids: list[str]) -> dict[str, dict]:
    """Classification state of rows, read before or after an owner edit."""
    if not transaction_ids:
        return {}
    cur.execute(
        """SELECT t.id, t.type, t.amount, t.date, t.account_id, t.merchant, t.notes, t.category_id,
                  c.name AS category_name, c.system_key AS category_system_key, t.kakeibo_type,
                  c.kakeibo_type AS category_kakeibo_type,
                  t.offset_category_id, t.movement_id, t.source, t.review_status, t.classified_by
           FROM transactions t LEFT JOIN categories c ON c.id = t.category_id AND c.user_id = t.user_id
           WHERE t.user_id = %s AND t.id = ANY(%s::uuid[])""",
        (user_id, transaction_ids),
    )
    snapshots = {}
    for row in cur.fetchall():
        snapshots[str(row["id"])] = {
            **row,
            "effective_kakeibo": effective_pillar(row["type"], row["kakeibo_type"], row["category_kakeibo_type"]),
        }
    return snapshots


def classification_values(row: dict) -> dict:
    return {
        "merchant": row.get("merchant"),
        "notes": (row.get("notes") or "").strip(),
        "category_id": str(row["category_id"]) if row.get("category_id") else None,
        "kakeibo_type": row.get("effective_kakeibo"),
        "type": row["type"],
        "offset_category_id": str(row["offset_category_id"]) if row.get("offset_category_id") else None,
    }


def owner_identity(cur, user_id: str) -> list[str]:
    cur.execute("SELECT name, name_aliases FROM users WHERE id = %s", (user_id,))
    return owner_aliases(cur.fetchone() or {})


def transaction_event_key(cur, user_id: str, transaction_id: str) -> dict:
    """Event id and match key of the notification that recorded a row, if any."""
    cur.execute(
        """SELECT * FROM notification_events WHERE user_id = %s AND transaction_id = %s
           ORDER BY post_time, id LIMIT 1""",
        (user_id, transaction_id),
    )
    event = cur.fetchone()
    if not event:
        return {}
    stored = event.get("interpretation") or {}
    if stored.get("match_key"):
        key = {"match_key": stored["match_key"], "text_signature": stored.get("text_signature"),
               "merchant_less": bool(stored.get("merchant_less")),
               "institution": SUPPORTED_PACKAGES.get(str(event["package_name"]).strip().lower())}
    else:
        key = classification_key(collect_facts(event), owner_identity(cur, user_id)) or {
            "institution": SUPPORTED_PACKAGES.get(str(event["package_name"]).strip().lower()),
        }
    return {**key, "event_id": str(event["id"]), "event": event}


def insert_feedback(
    cur, user_id: str, *, kind: str, before: dict | None, after: dict, key: dict,
    answer: dict | None = None, history_suggestion_id: str | None = None,
) -> str:
    """Append one labelled example and return its id; feedback rows are never updated (V29 trigger)."""
    weekday, hour = local_time_features(after.get("date"))
    answer = answer or {}
    before = before or {}
    cur.execute(
        """INSERT INTO classification_feedback (
               user_id, kind, transaction_id, event_id, question_id, reply_id, suggestion_id, answer_text,
               before_merchant, before_notes, before_category_id, before_category_name, before_kakeibo_type,
               before_type, before_offset_category_id,
               after_merchant, after_notes, after_category_id, after_category_name, after_kakeibo_type,
               after_type, after_offset_category_id,
               local_weekday, local_hour, amount, account_id, institution, text_signature, match_key,
               transaction_source, history_suggestion_id, before_classified_by, after_classified_by,
               after_review_status)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING id""",
        (
            user_id, kind, str(after["id"]), key.get("event_id"), answer.get("question_id"),
            answer.get("reply_id"), answer.get("suggestion_id"), answer.get("text"),
            before.get("merchant"), (before.get("notes") or None) and before["notes"][:500],
            before.get("category_id"), before.get("category_name"), before.get("effective_kakeibo"),
            before.get("type"), before.get("offset_category_id"),
            after.get("merchant"), (after.get("notes") or None) and after["notes"][:500],
            after.get("category_id"), after.get("category_name"), after.get("effective_kakeibo"),
            after.get("type"), after.get("offset_category_id"),
            weekday, hour, after.get("amount"), after.get("account_id"), key.get("institution"),
            key.get("text_signature"), key.get("match_key"), after.get("source"), history_suggestion_id,
            before.get("classified_by"), after.get("classified_by"), after.get("review_status"),
        ),
    )
    return str(cur.fetchone()["id"])


def live_feedback_sql(alias: str) -> str:
    """SQL condition: the feedback row is a label that still counts (neither an undo row nor reverted).

    Undo appends an `undo` row naming the reverted label instead of changing it (redesign-review-triage
    D13), so every reader of labels filters through this one condition.
    """
    return (
        f"({alias}.kind <> 'undo' AND NOT EXISTS (SELECT 1 FROM classification_feedback reverting "
        f"WHERE reverting.user_id = {alias}.user_id AND reverting.reverts_feedback_id = {alias}.id))"
    )


def is_movement_category(system_key: str | None) -> bool:
    return is_movement(system_key)


def learn_from_owner(cur, user_id: str, after: dict, key: dict) -> None:
    """Upsert owner memory for a plain expense or income with a category (design D4).

    A row filed in the movement category is one side of a transfer, not a classification to repeat: the
    next notification with the same wording may be a different transfer or a second report of one
    (recognise-one-arrival-seen-twice D4).
    """
    if (after["type"] not in {"expense", "income"} or after.get("movement_id") or not after.get("category_id")
            or is_movement_category(after["category_system_key"])):
        return
    values = {
        "merchant": after.get("merchant"), "notes": after.get("notes"), "category_id": str(after["category_id"]),
        "kakeibo_type": after.get("kakeibo_type") if after["type"] == "expense" else None,
        "offset_category_id": str(after["offset_category_id"]) if after.get("offset_category_id") else None,
    }
    keys = [key.get("match_key"), merchant_match_key(after.get("merchant"))]
    for match_key in dict.fromkeys(item for item in keys if item):
        upsert_memory(cur, user_id, match_key, values, "owner")


def close_purpose_question(cur, user_id: str, transaction_id: str) -> None:
    cur.execute(
        """UPDATE notification_events SET active_question = NULL, updated_at = NOW()
           WHERE user_id = %s AND transaction_id = %s AND active_question ->> 'type' = 'purpose'""",
        (user_id, transaction_id),
    )


def record_owner_corrections(
    cur, user_id: str, before: dict[str, dict], *, kind: str = "correction",
    history_suggestions: dict[str, str] | None = None, close_questions: bool = True,
) -> int:
    """Write feedback and memory for rows whose classification an owner edit changed.

    An accepted history suggestion is an owner edit too; its feedback row carries its own kind and the
    suggestion id (one per transaction in `history_suggestions`). `close_questions` is False when the
    owner classified a whole category: no row's purpose was answered, so its open question stays.
    """
    after_rows = snapshot_rows(cur, user_id, list(before))
    written = 0
    for transaction_id, after in after_rows.items():
        previous = before[transaction_id]
        if classification_values(previous) == classification_values(after):
            continue
        key = transaction_event_key(cur, user_id, transaction_id)
        insert_feedback(cur, user_id, kind=kind, before=previous, after=after, key=key,
                        history_suggestion_id=(history_suggestions or {}).get(transaction_id))
        learn_from_owner(cur, user_id, after, key)
        if close_questions:
            close_purpose_question(cur, user_id, transaction_id)
        written += 1
    return written


def retire_automatic_memory(cur, user_id: str, transaction_ids: list[str]) -> int:
    """Delete the model's memory for legs that became a movement; owner entries stay (design D4).

    A model guess such as "Tabungan by Jago is Pendapatan Lain" was learned while the leg looked like
    income. Once the leg is a movement that guess is wrong for the next report of the same wording.
    """
    keys: set[str] = set()
    for transaction_id in transaction_ids:
        if match_key := transaction_event_key(cur, user_id, str(transaction_id)).get("match_key"):
            keys.add(match_key)
    cur.execute("SELECT merchant FROM transactions WHERE user_id = %s AND id = ANY(%s::uuid[])",
                (user_id, [str(value) for value in transaction_ids]))
    keys.update(key for row in cur.fetchall() if (key := merchant_match_key(row["merchant"])))
    if not keys:
        return 0
    cur.execute(
        """DELETE FROM merchant_memory m
           WHERE m.user_id = %s AND m.match_key = ANY(%s) AND m.source = 'ai'
             AND NOT EXISTS (SELECT 1 FROM categories c WHERE c.id = m.category_id AND c.system_key = %s)
           RETURNING m.match_key""",
        (user_id, sorted(keys), INTERNAL_MOVEMENT),
    )
    return len(cur.fetchall())


def learn_from_model(cur, user_id: str, key: dict | None, values: dict) -> None:
    """Remember a confident model classification of a named merchant, below any owner entry."""
    if not key or key["key_kind"] != "merchant" or not values.get("category_id"):
        return
    upsert_memory(cur, user_id, key["match_key"], values, "ai")


def stored_key_fields(key: dict | None) -> dict:
    if not key:
        return {}
    return {"match_key": key["match_key"], "text_signature": key["text_signature"],
            "merchant_less": key["merchant_less"]}
