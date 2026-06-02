"""Deterministic resolution of an LLM proposal into an executable action.

Pure, synchronous logic (fuzzy matching + policy) — fast and unit-testable.
Maps the model's free-text account/category names to real IDs, recomputes a
final confidence, and decides: execute, confirm, ask, query, or clarify.
"""
from __future__ import annotations

import difflib
from typing import Any

_TX_INTENTS = {"create_transaction", "update_transaction", "delete_transaction"}
_MV_INTENTS = {"create_movement", "update_movement", "delete_movement"}
_QUERY_INTENTS = {"query", "query_balance", "query_transactions"}
_ALWAYS_CONFIRM = {
    "update_transaction", "delete_transaction", "update_movement", "delete_movement",
}


def _norm(s: str | None) -> str:
    return (s or "").strip().lower()


def match_one(name: str | None, options: list[dict[str, Any]], key: str) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Return (single match, []) or (None, candidates) when ambiguous/none."""
    if not name:
        return None, []
    n = _norm(name)
    exact = [o for o in options if _norm(o.get(key)) == n]
    if len(exact) == 1:
        return exact[0], []
    if len(exact) > 1:
        return None, exact
    contains = [o for o in options if n and (n in _norm(o.get(key)) or _norm(o.get(key)) in n)]
    if len(contains) == 1:
        return contains[0], []
    if len(contains) > 1:
        return None, contains
    names = [o.get(key, "") for o in options]
    close = difflib.get_close_matches(name, names, n=3, cutoff=0.6)
    matched = [o for o in options if o.get(key) in close]
    if len(matched) == 1:
        return matched[0], []
    return None, matched


def resolve(
    proposal: dict[str, Any],
    accounts: list[dict[str, Any]],
    categories: list[dict[str, Any]],
    threshold: float,
) -> dict[str, Any]:
    intent = proposal.get("intent") or "none"
    confidence = float(proposal.get("confidence") or 0.0)
    questions: list[dict[str, Any]] = []
    fields: dict[str, Any] = {
        "amount": proposal.get("amount"),
        "transaction_name": proposal.get("transaction_name"),
        "transaction_type": proposal.get("transaction_type"),
        "is_cycle_topup": bool(proposal.get("is_cycle_topup") or False),
        "date": proposal.get("date"),
        "query": proposal.get("query"),
    }

    if intent == "none":
        return {"intent": intent, "decision": "clarify", "fields": fields, "questions": [],
                "confidence": confidence, "assistant_message": proposal.get("assistant_message") or "Bisa diperjelas?"}
    
    # Handle query intents - pass through without validation
    if intent in _QUERY_INTENTS:
        return {
            "intent": intent,
            "decision": "query",
            "fields": fields,
            "questions": [],
            "confidence": confidence,
            "assistant_message": proposal.get("assistant_message") or "",
            "query_accounts": proposal.get("query_accounts") or [],
            "time_range": proposal.get("time_range"),
        }

    # Resolve account / target / category names to IDs.
    acc, acc_cands = match_one(proposal.get("account_name"), accounts, "account_name")
    if acc:
        fields["account_id"], fields["account_name"] = acc["account_id"], acc["account_name"]
    elif acc_cands:
        questions.append({"field": "account", "candidates": [a["account_name"] for a in acc_cands]})

    if intent in _MV_INTENTS:
        tgt, tgt_cands = match_one(proposal.get("target_account_name"), accounts, "account_name")
        if tgt:
            fields["target_account_id"], fields["target_account_name"] = tgt["account_id"], tgt["account_name"]
        elif tgt_cands:
            questions.append({"field": "target_account", "candidates": [a["account_name"] for a in tgt_cands]})

    if intent in _TX_INTENTS and proposal.get("category_name"):
        cat, cat_cands = match_one(proposal.get("category_name"), categories, "name")
        if cat:
            fields["category_id"], fields["category_name"] = cat["category_id"], cat["name"]
        elif cat_cands:
            questions.append({"field": "category", "candidates": [c["name"] for c in cat_cands]})

    # Required-field gaps.
    missing: list[str] = []
    if intent == "create_transaction":
        if not fields.get("amount"):
            missing.append("amount")
        if fields.get("transaction_type") not in ("debit", "credit"):
            missing.append("transaction_type")
        if not fields.get("transaction_name"):
            missing.append("transaction_name")
        if not fields.get("account_id") and not any(q["field"] == "account" for q in questions):
            missing.append("account")
    elif intent == "create_movement":
        if not fields.get("amount"):
            missing.append("amount")
        if not fields.get("account_id") and not any(q["field"] == "account" for q in questions):
            missing.append("account")
        if not fields.get("target_account_id") and not any(q["field"] == "target_account" for q in questions):
            missing.append("target_account")
    else:  # update_* / delete_*
        if not fields.get("query"):
            missing.append("target")
    for m in missing:
        questions.append({"field": m, "candidates": []})

    if questions:
        decision = "ask"
    elif intent in _ALWAYS_CONFIRM:
        decision = "confirm"
    elif confidence >= threshold:
        decision = "execute"
    else:
        decision = "confirm"

    return {
        "intent": intent,
        "decision": decision,
        "fields": fields,
        "questions": questions,
        "confidence": confidence,
        "assistant_message": proposal.get("assistant_message") or "",
    }
