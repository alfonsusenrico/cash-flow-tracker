"""Event-scoped owner choices for Jago transfers without source-pocket evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace

from fastapi import HTTPException
from psycopg.types.json import Jsonb

from app.services.notification_evidence import EvidenceError, requires_source_pocket
from app.services.notification_mapping import version_tuple
from app.services.notification_resolution import MAPPABLE_ACCOUNT_TYPES, institution_parent
from app.services.notification_sender import persist_question


SOURCE_POCKET_MIN_VERSION = (1, 6, 0)


def eligible_sources(cur, user_id: str) -> tuple[dict, list[dict]]:
    cur.execute(
        """SELECT id, name, type, parent_id, instrument_type FROM accounts
           WHERE user_id = %s AND is_archived = FALSE ORDER BY name, id""",
        (user_id,),
    )
    accounts = cur.fetchall()
    cur.execute(
        """SELECT account_id FROM notification_account_aliases
           WHERE user_id = %s AND institution = 'jago' AND name_normalized = ''""", (user_id,),
    )
    alias = cur.fetchone()
    if alias:
        for account in accounts:
            if str(account["id"]) == str(alias["account_id"]):
                account["learned_notification_names"] = [{"key": "jago|"}]
    parent = institution_parent(accounts, "jago")
    if parent.get("parent_id") or parent["type"] not in MAPPABLE_ACCOUNT_TYPES or parent.get("instrument_type"):
        raise EvidenceError("uncertain_institution_mapping")
    children = [row for row in accounts if str(row.get("parent_id")) == str(parent["id"])]
    options = [row for row in children if row["type"] in MAPPABLE_ACCOUNT_TYPES and not row.get("instrument_type")]
    if not children:
        options = [parent]
    if not options:
        raise EvidenceError("source_pocket_unconfigured")
    return parent, options


def evidence_revision(facts) -> str:
    meaning = [facts.text, facts.amount, facts.currency, facts.direction, facts.timestamp.isoformat()]
    return hashlib.sha256(json.dumps(meaning, ensure_ascii=False).encode()).hexdigest()


def resolved_facts(cur, event: dict, facts):
    if not requires_source_pocket(facts):
        return facts
    parent, options = eligible_sources(cur, str(event["user_id"]))
    decision = event.get("source_pocket_resolution") or {}
    account_id = decision.get("account_id")
    if (decision.get("jago_account_id") != str(parent["id"])
            or decision.get("evidence_revision") != evidence_revision(facts)
            or account_id not in {str(row["id"]) for row in options}):
        raise EvidenceError("source_pocket_confirmation_required")
    # Hold references stable until application commits; account mutation cannot race the choice.
    cur.execute(
        """SELECT id FROM accounts WHERE user_id = %s AND id = ANY(%s::uuid[])
           AND is_archived = FALSE ORDER BY id FOR SHARE""",
        (event["user_id"], sorted({str(parent["id"]), account_id})),
    )
    if len(cur.fetchall()) != len({str(parent["id"]), account_id}):
        raise EvidenceError("source_pocket_confirmation_required")
    locked_parent, locked_options = eligible_sources(cur, str(event["user_id"]))
    if str(locked_parent["id"]) != str(parent["id"]) or account_id not in {str(row["id"]) for row in locked_options}:
        raise EvidenceError("source_pocket_confirmation_required")
    return replace(facts, confirmed_source_account_id=account_id)


def source_pocket_gate(cur, event: dict, facts) -> bool:
    if not requires_source_pocket(facts):
        return False
    try:
        resolved_facts(cur, event, facts)
        return False
    except EvidenceError:
        pass
    state, code = "needs_review", "source_pocket_confirmation_required"
    version = version_tuple(event.get("source_version"))
    try:
        parent, _ = eligible_sources(cur, str(event["user_id"]))
        if version is not None and version >= SOURCE_POCKET_MIN_VERSION:
            from app.services.notification_context import sanitize_text

            persist_question(cur, event, {
                "type": "source_pocket", "institution": "jago",
                "jago_account": {"id": str(parent["id"]), "label": sanitize_text(parent["name"])[:150]},
                "amount": facts.amount, "currency": facts.currency,
                "destination_label": sanitize_text(facts.parsed.counterparty)[:150] or None,
                "evidence_revision": evidence_revision(facts),
            })
            state = "needs_confirmation"
    except EvidenceError as exc:
        code = exc.code
    cur.execute(
        """UPDATE notification_events SET processing_state = %s, error_code = %s,
           lease_token = NULL, lease_expires_at = NULL, updated_at = NOW()
           WHERE user_id = %s AND id = %s""", (state, code, event["user_id"], event["id"]),
    )
    event.update(processing_state=state, error_code=code)
    return True


def source_options(cur, event: dict) -> dict:
    from app.services.notification_context import sanitize_text

    question = event.get("active_question") or {}
    if event["processing_state"] != "needs_confirmation" or question.get("type") != "source_pocket":
        raise HTTPException(status_code=409, detail={"code": "stale_confirmation"})
    try:
        parent, options = eligible_sources(cur, str(event["user_id"]))
    except EvidenceError:
        raise HTTPException(status_code=409, detail={"code": "source_pocket_unconfigured"}) from None
    if str(parent["id"]) != question["jago_account"]["id"]:
        raise HTTPException(status_code=409, detail={"code": "stale_confirmation"})
    return {"event_id": str(event["id"]), "question_id": question["question_id"],
            "options": [{"id": str(row["id"]), "label": sanitize_text(
                f"{parent['name']} · {row['name']}" if row.get("parent_id") else row["name"]
            )[:300]} for row in options]}


def accept_source_answer(cur, event: dict, facts, answer: dict) -> bool:
    cur.execute(
        """SELECT event_id, question_id, reply_id, account_id FROM notification_source_pocket_answers
           WHERE user_id = %s AND ((event_id = %s AND question_id = %s) OR reply_id = %s)""",
        (event["user_id"], event["id"], answer["question_id"], answer["reply_id"]),
    )
    receipts = cur.fetchall()
    if receipts:
        if (len(receipts) == 1 and str(receipts[0]["event_id"]) == str(event["id"])
                and str(receipts[0]["question_id"]) == answer["question_id"]
                and str(receipts[0]["reply_id"]) == answer["reply_id"]
                and str(receipts[0]["account_id"]) == answer["account_id"]):
            return False
        raise HTTPException(status_code=409, detail={"code": "source_answer_conflict"})
    question = event.get("active_question") or {}
    options = source_options(cur, event)
    if (not requires_source_pocket(facts) or question.get("question_id") != answer["question_id"]
            or question.get("evidence_revision") != evidence_revision(facts)):
        raise HTTPException(status_code=409, detail={"code": "stale_confirmation"})
    if answer["account_id"] not in {row["id"] for row in options["options"]}:
        raise HTTPException(status_code=409, detail={"code": "invalid_source_pocket"})
    decision = {"account_id": answer["account_id"], "question_id": answer["question_id"],
                "jago_account_id": question["jago_account"]["id"],
                "evidence_revision": question["evidence_revision"], "confirmed_by": "owner"}
    checked = {**event, "source_pocket_resolution": decision}
    try:
        resolved_facts(cur, checked, facts)
    except EvidenceError:
        raise HTTPException(status_code=409, detail={"code": "invalid_source_pocket"}) from None
    cur.execute(
        """INSERT INTO notification_source_pocket_answers
           (user_id, event_id, question_id, reply_id, account_id) VALUES (%s, %s, %s, %s, %s)""",
        (event["user_id"], event["id"], answer["question_id"], answer["reply_id"], answer["account_id"]),
    )
    cur.execute(
        """UPDATE notification_events SET source_pocket_resolution = %s, active_question = NULL
           WHERE user_id = %s AND id = %s""", (Jsonb(decision), event["user_id"], event["id"]),
    )
    return True
