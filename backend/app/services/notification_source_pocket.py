"""Event-scoped owner choices for Jago transfers without source-pocket evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace

from psycopg.types.json import Jsonb

from app.errors import ErrorCode, api_error
from app.services.notification_evidence import EvidenceError, requires_source_pocket, unnamed_source_move
from app.services.notification_mapping import version_tuple
from app.services.notification_resolution import (
    MAPPABLE_ACCOUNT_TYPES, alias_key, institution_parent, pocket_endpoint,
)
from app.services.notification_sender import persist_question


SOURCE_POCKET_MIN_VERSION = (1, 6, 0)


def eligible_sources(cur, user_id: str) -> tuple[dict, list[dict]]:
    cur.execute(
        """SELECT id, name, type, parent_id, instrument_type, default_pocket_id FROM accounts
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


def jago_accounts(cur, user_id: str) -> list[dict]:
    """Active accounts with their learned notification names, as pocket resolution expects them."""
    cur.execute(
        """SELECT id, name, type, parent_id, default_pocket_id, instrument_type FROM accounts
           WHERE user_id = %s AND is_archived = FALSE ORDER BY name, id""",
        (user_id,),
    )
    accounts = cur.fetchall()
    cur.execute(
        """SELECT account_id, institution, name_normalized FROM notification_account_aliases
           WHERE user_id = %s""", (user_id,),
    )
    learned: dict[str, list[dict]] = {}
    for alias in cur.fetchall():
        learned.setdefault(str(alias["account_id"]), []).append(
            {"key": alias_key(alias["institution"], alias["name_normalized"])})
    for account in accounts:
        account["learned_notification_names"] = learned.get(str(account["id"]), [])
    return accounts


def resolve_move_target(accounts: list[dict], facts) -> str | None:
    """The registered pocket a model-read move goes into, or None while its name still needs mapping."""
    if not unnamed_source_move(facts):
        return None
    try:
        parent = institution_parent(accounts, "jago")
        return str(pocket_endpoint(facts.parsed.target_pocket, parent, accounts, institution="jago")["id"])
    except EvidenceError:
        return None


def move_target_id(cur, user_id: str, facts) -> str | None:
    return resolve_move_target(jago_accounts(cur, user_id), facts)


def owned_move_sources(cur, user_id: str, target_id: str) -> list[dict]:
    """Every account money can come from for a move into the target: the owner's own liquid accounts that hold
    a balance themselves (no account with pockets), the target excepted, whichever institution they are at."""
    cur.execute(
        """SELECT a.id, a.name, a.type, a.parent_id, a.instrument_type FROM accounts a
           WHERE a.user_id = %s AND a.is_archived = FALSE AND a.id <> %s
             AND a.type = ANY(%s) AND a.instrument_type IS NULL
             AND NOT EXISTS (SELECT 1 FROM accounts c WHERE c.user_id = a.user_id AND c.parent_id = a.id
                             AND c.is_archived = FALSE)
           ORDER BY a.parent_id NULLS FIRST, a.name, a.id""",
        (user_id, target_id, sorted(MAPPABLE_ACCOUNT_TYPES)),
    )
    return cur.fetchall()


def eligible_move_sources(cur, user_id: str, facts) -> tuple[dict, list[dict], str | None]:
    """Eligible sources: Jago pockets for an unnamed outbound transfer; for a move with an unnamed source, every
    owned account except the target, because the money may come from another bank or wallet."""
    parent, options = eligible_sources(cur, user_id)
    target_id = move_target_id(cur, user_id, facts) if unnamed_source_move(facts) else None
    if target_id:
        options = owned_move_sources(cur, user_id, target_id)
    return parent, options, target_id


def learned_decision(cur, event: dict, facts, options: list[dict], target_id: str | None):
    from app.core.config import settings
    from app.services.pocket_learning import learned_pocket

    return learned_pocket(cur, event, facts, eligible={str(row["id"]) for row in options}, target_id=target_id,
                          auto_enabled=settings.pocket_learning_auto_record_enabled)


def resolved_facts(cur, event: dict, facts):
    if not requires_source_pocket(facts) or facts.confirmed_source_account_id:
        return facts
    decision = event.get("source_pocket_resolution") or {}
    parent, options, target_id = eligible_move_sources(cur, str(event["user_id"]), facts)
    if unnamed_source_move(facts) and target_id is None and not decision:
        # The target's name is mapped first; recording still refuses a move without a confirmed source.
        return facts
    account_id = decision.get("account_id")
    basis = None
    if (decision.get("jago_account_id") != str(parent["id"])
            or decision.get("evidence_revision") != evidence_revision(facts)
            or account_id not in {str(row["id"]) for row in options}):
        # No valid answer: only the owner's agreeing earlier choices may decide (learn-payment-pockets D5).
        learned = learned_decision(cur, event, facts, options, target_id)
        if not learned.auto:
            raise EvidenceError("source_pocket_confirmation_required")
        account_id = learned.account_id
        basis = {"agreeing": learned.agreeing, "support": learned.support}
    # Hold references stable until application commits; account mutation cannot race the choice.
    cur.execute(
        """SELECT id FROM accounts WHERE user_id = %s AND id = ANY(%s::uuid[])
           AND is_archived = FALSE ORDER BY id FOR SHARE""",
        (event["user_id"], sorted({str(parent["id"]), account_id})),
    )
    if len(cur.fetchall()) != len({str(parent["id"]), account_id}):
        raise EvidenceError("source_pocket_confirmation_required")
    locked_parent, locked_options, _target = eligible_move_sources(cur, str(event["user_id"]), facts)
    if str(locked_parent["id"]) != str(parent["id"]) or account_id not in {str(row["id"]) for row in locked_options}:
        raise EvidenceError("source_pocket_confirmation_required")
    return replace(facts, confirmed_source_account_id=account_id, pocket_basis=basis)


def default_pocket_id(parent: dict, options: list[dict]) -> str | None:
    """Only a preselection: the configured default pocket, else the pocket named like the main one."""
    from app.services.notification_resolution import normalize_name

    if parent.get("default_pocket_id"):
        return str(parent["default_pocket_id"])
    main = [row for row in options if normalize_name(row["name"]) == "utama"]
    return str(main[0]["id"]) if len(main) == 1 else None


def source_pocket_gate(cur, event: dict, facts) -> bool:
    if not requires_source_pocket(facts):
        return False
    try:
        resolved_facts(cur, event, facts)
        return False
    except EvidenceError:
        pass
    if unnamed_source_move(facts):
        from app.services.notification_application import confirm_hand_entered_move

        if confirm_hand_entered_move(cur, event, facts, source_id=None):
            return True
    state, code = "needs_review", "source_pocket_confirmation_required"
    version = version_tuple(event.get("source_version"))
    try:
        parent, options, target_id = eligible_move_sources(cur, str(event["user_id"]), facts)
        if version is not None and version >= SOURCE_POCKET_MIN_VERSION:
            from app.services.notification_context import sanitize_text

            destination = facts.parsed.counterparty
            question = {
                "type": "source_pocket", "institution": "jago",
                "jago_account": {"id": str(parent["id"]), "label": sanitize_text(parent["name"])[:150]},
                "amount": facts.amount, "currency": facts.currency,
                "evidence_revision": evidence_revision(facts),
            }
            if target_id:
                cur.execute("SELECT name FROM accounts WHERE user_id = %s AND id = %s", (event["user_id"], target_id))
                destination = cur.fetchone()["name"]
                question["excluded_account_id"] = target_id
            # The owner's own favourite first, then the model's guess for a move, then the default pocket.
            option_ids = {str(row["id"]) for row in options}
            candidates = [learned_decision(cur, event, facts, options, target_id).account_id,
                          (facts.model_reading or {}).get("suggested_source_account_id") if target_id else None,
                          None if target_id else default_pocket_id(parent, options)]
            suggested = next((candidate for candidate in candidates if candidate in option_ids), None)
            if suggested:
                question["suggested_account_id"] = suggested
            question["destination_label"] = sanitize_text(destination)[:150] or None
            persist_question(cur, event, question)
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
        raise api_error(409, ErrorCode.STALE_CONFIRMATION)
    try:
        parent, options = eligible_sources(cur, str(event["user_id"]))
    except EvidenceError:
        raise api_error(409, ErrorCode.SOURCE_POCKET_UNCONFIGURED) from None
    if str(parent["id"]) != question["jago_account"]["id"]:
        raise api_error(409, ErrorCode.STALE_CONFIRMATION)
    if question.get("excluded_account_id"):
        options = owned_move_sources(cur, str(event["user_id"]), question["excluded_account_id"])
    cur.execute("SELECT id, name FROM accounts WHERE user_id = %s", (event["user_id"],))
    names = {str(row["id"]): row["name"] for row in cur.fetchall()}
    result = {"event_id": str(event["id"]), "question_id": question["question_id"],
              "options": [{"id": str(row["id"]), "label": sanitize_text(
                  f"{names[str(row['parent_id'])]} · {row['name']}" if row.get("parent_id") else row["name"]
              )[:300]} for row in options]}
    if question.get("suggested_account_id") in {option["id"] for option in result["options"]}:
        result["suggested_account_id"] = question["suggested_account_id"]
    return result


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
        raise api_error(409, ErrorCode.SOURCE_ANSWER_CONFLICT)
    question = event.get("active_question") or {}
    options = source_options(cur, event)
    if (not requires_source_pocket(facts) or question.get("question_id") != answer["question_id"]
            or question.get("evidence_revision") != evidence_revision(facts)):
        raise api_error(409, ErrorCode.STALE_CONFIRMATION)
    if answer["account_id"] not in {row["id"] for row in options["options"]}:
        raise api_error(409, ErrorCode.INVALID_SOURCE_POCKET)
    decision = {"account_id": answer["account_id"], "question_id": answer["question_id"],
                "jago_account_id": question["jago_account"]["id"],
                "evidence_revision": question["evidence_revision"], "confirmed_by": "owner"}
    checked = {**event, "source_pocket_resolution": decision}
    try:
        resolved_facts(cur, checked, facts)
    except EvidenceError:
        raise api_error(409, ErrorCode.INVALID_SOURCE_POCKET) from None
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
