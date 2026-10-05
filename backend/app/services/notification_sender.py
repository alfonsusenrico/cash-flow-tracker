"""Owner-confirmed sender names, separate from owned-account mapping and identity."""

from __future__ import annotations

import re
import unicodedata
from uuid import uuid4

from fastapi import HTTPException
from psycopg.types.json import Jsonb

from app.services.notification_mapping import version_tuple


SENDER_MIN_VERSION = (1, 5, 0)


def supports_sender_confirmation(event: dict) -> bool:
    version = version_tuple(event.get("source_version"))
    return version is not None and version >= SENDER_MIN_VERSION


def normalize_mask(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def validate_sender_name(value: str | None) -> str:
    from app.services.notification_context import sanitize_text

    if not isinstance(value, str) or any(unicodedata.category(char).startswith("C") for char in value):
        raise HTTPException(status_code=422, detail={"code": "invalid_sender_name"})
    name = unicodedata.normalize("NFKC", value).strip()
    if not 1 <= len(name) <= 80 or sanitize_text(name) != name:
        raise HTTPException(status_code=422, detail={"code": "invalid_sender_name"})
    return name


def masked_transfer_sender(facts) -> str | None:
    if (facts.status != "candidate" or facts.institution != "bca" or facts.direction != "income"
            or facts.parsed.own_account_transfer or facts.parsed.rdn_deposit
            or (facts.parsed.category_hint or "").casefold() != "transfer masuk"):
        return None
    sender = (facts.parsed.counterparty or "").strip()
    if not 1 <= len(sender) <= 150 or "*" not in sender:
        return None
    # Require a real sender token, not a generic missing-sender fallback or a money value.
    return sender if re.search(r"[\w*]*\*[\w*]*", sender) else None


def sender_scope(facts, account_id: str) -> dict | None:
    mask = masked_transfer_sender(facts)
    if mask is None:
        return None
    return {"institution": facts.institution, "receiving_account_id": account_id,
            "mask_normalized": normalize_mask(mask), "mask_display": mask}


def matching_resolution(cur, event: dict, scope: dict) -> dict | None:
    resolution = event.get("sender_resolution")
    if resolution and all(resolution.get(key) == scope[key] for key in (
        "institution", "receiving_account_id", "mask_normalized"
    )):
        return resolution
    cur.execute(
        """SELECT sender_name, state FROM notification_sender_aliases
           WHERE user_id = %s AND institution = %s AND receiving_account_id = %s
             AND mask_normalized = %s""",
        (event["user_id"], scope["institution"], scope["receiving_account_id"], scope["mask_normalized"]),
    )
    alias = cur.fetchone()
    if alias and alias["state"] == "active":
        return {**scope, "action": "name", "name": alias["sender_name"], "confirmed_by": "owner"}
    return None


def attach_sender_context(cur, event: dict, facts, context: dict) -> None:
    account_id = context.get("facts", {}).get("effective_account_id")
    if not account_id:
        return
    scope = sender_scope(facts, str(account_id))
    if scope and (resolution := matching_resolution(cur, event, scope)):
        context["confirmed_sender"] = {
            "name": resolution.get("name"), "action": resolution["action"], "confirmed_by": "owner",
        }


def persist_question(cur, event: dict, question: dict) -> dict:
    previous = event.get("active_question") or {}
    meaning = {key: value for key, value in previous.items() if key != "question_id"}
    question = {**question, "question_id": previous["question_id"] if meaning == question else str(uuid4())}
    cur.execute(
        "UPDATE notification_events SET active_question = %s WHERE user_id = %s AND id = %s",
        (Jsonb(question), event["user_id"], event["id"]),
    )
    event["active_question"] = question
    return question


def sender_gate(
    cur, event: dict, facts, context: dict, observed: dict, signature: dict, *, proven_movement: bool = False,
) -> tuple[bool, str | None]:
    """Return a hold or the confirmed description before the shared ledger write path."""
    scope = sender_scope(facts, str(observed["id"]))
    if scope is None:
        return False, None
    # Existing owner evidence is independent of learned third-party sender names.
    if (proven_movement or signature.get("self_identity")
            or context.get("facts", {}).get("counterparty_is_owner")):
        return False, None
    if resolution := matching_resolution(cur, event, scope):
        # Persist the decision used by this event so later memory edits cannot change it.
        cur.execute(
            "UPDATE notification_events SET sender_resolution = %s WHERE user_id = %s AND id = %s",
            (Jsonb(resolution), event["user_id"], event["id"]),
        )
        event["sender_resolution"] = resolution
        name = resolution.get("name")
        return False, f"Transfer masuk dari {name}" if resolution["action"] == "name" else "Transfer masuk"
    if supports_sender_confirmation(event):
        from app.services.notification_application import account_label

        persist_question(cur, event, {
            "type": "sender", "masked_sender": scope["mask_display"], "institution": facts.institution,
            "receiving_account": {"id": str(observed["id"]), "label": account_label(observed, context["accounts"])},
            "amount": facts.amount, "currency": facts.currency,
        })
        state = "needs_confirmation"
    else:
        state = "needs_review"
    cur.execute(
        """UPDATE notification_events SET processing_state = %s, error_code = 'sender_confirmation_required',
           lease_token = NULL, lease_expires_at = NULL, updated_at = NOW()
           WHERE user_id = %s AND id = %s""",
        (state, event["user_id"], event["id"]),
    )
    return True, None


def accept_sender_answer(cur, event: dict, answer: dict) -> bool:
    question_id, reply_id = answer["question_id"], answer["reply_id"]
    action = answer["action"]
    name = validate_sender_name(answer.get("name")) if action == "name" else None
    if action == "unknown" and answer.get("name") is not None:
        raise HTTPException(status_code=422, detail={"code": "invalid_sender_answer"})
    cur.execute(
        """SELECT event_id, question_id, reply_id, action, sender_name FROM notification_sender_answers
           WHERE user_id = %s AND ((event_id = %s AND question_id = %s) OR reply_id = %s)""",
        (event["user_id"], event["id"], question_id, reply_id),
    )
    receipts = cur.fetchall()
    if receipts:
        if (len(receipts) == 1 and str(receipts[0]["event_id"]) == str(event["id"])
                and str(receipts[0]["question_id"]) == question_id
                and receipts[0]["action"] == action and receipts[0]["sender_name"] == name):
            return False
        raise HTTPException(status_code=409, detail={"code": "sender_answer_conflict"})
    question = event.get("active_question") or {}
    if (event["processing_state"] != "needs_confirmation" or question.get("type") != "sender"
            or question.get("question_id") != question_id):
        raise HTTPException(status_code=409, detail={"code": "stale_confirmation"})
    resolution = {
        "institution": question["institution"], "receiving_account_id": question["receiving_account"]["id"],
        "mask_normalized": normalize_mask(question["masked_sender"]), "mask_display": question["masked_sender"],
        "action": action, "name": name, "confirmed_by": "owner", "question_id": question_id,
    }
    cur.execute(
        """INSERT INTO notification_sender_answers (user_id, event_id, question_id, reply_id, action, sender_name)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (event["user_id"], event["id"], question_id, reply_id, action, name),
    )
    if name:
        cur.execute(
            """INSERT INTO notification_sender_aliases
                   (user_id, institution, receiving_account_id, mask_normalized, mask_display, sender_name)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (user_id, institution, receiving_account_id, mask_normalized) DO UPDATE
               SET state = CASE WHEN notification_sender_aliases.sender_name = EXCLUDED.sender_name
                                     THEN notification_sender_aliases.state ELSE 'ambiguous' END,
                   updated_at = NOW()""",
            (event["user_id"], resolution["institution"], resolution["receiving_account_id"],
             resolution["mask_normalized"], resolution["mask_display"], name),
        )
    cur.execute(
        """UPDATE notification_events SET sender_resolution = %s, active_question = NULL
           WHERE user_id = %s AND id = %s""",
        (Jsonb(resolution), event["user_id"], event["id"]),
    )
    return True
