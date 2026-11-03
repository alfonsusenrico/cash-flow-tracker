"""Parse-only preview of a typed capture for the web Catat dialog (redesign-transactions-workspace D11).

The preview reads what the deterministic parser, the owner's accounts and the owner's own merchant memory
say about the text. It runs inside a read-only transaction, so it can write nothing, and it never calls a
model: the category comes only from memory the owner taught. Recording stays with `/capture/text`.
"""

from __future__ import annotations

import logging
from datetime import datetime

from fastapi import HTTPException

from app.db.pool import db_conn
from app.services.capture_resolution import account_label, capture_facts, liquid_leaves, memory_state
from app.services.text_capture import TEXT_MAX_LENGTH, clean_text

logger = logging.getLogger(__name__)

PARSE_RATE_LIMIT = 120
PARSE_RATE_WINDOW = 60
AMOUNT_PROBLEMS = {"missing": "missing_amount", "several": "several_amounts", "invalid": "missing_amount"}


def account_hint(facts: dict, accounts: list[dict]) -> dict | None:
    """The one owner account the text names, only when exactly one candidate remains."""
    leaves = liquid_leaves(accounts)
    candidates = {candidate for mention in facts["mentions"] for candidate in mention["candidates"] if candidate in leaves}
    if len(candidates) != 1:
        return None
    (account_id,) = candidates
    return {"account_id": account_id, "label": account_label(leaves[account_id], accounts)}


def category_hint(cur, user_id: str, memory: dict) -> dict | None:
    hit = memory["hit"]
    if not hit:
        return None
    cur.execute(
        "SELECT name, kakeibo_type FROM categories WHERE user_id = %s AND id = %s AND is_archived = FALSE",
        (user_id, hit["category_id"]),
    )
    category = cur.fetchone()
    if category is None:
        return None
    return {"category_id": hit["category_id"], "label": category["name"], "pillar": hit["kakeibo"] or category["kakeibo_type"]}


def time_preview(facts: dict) -> dict:
    reading = facts["time"]
    placed = reading["status"] in {"none", "resolved"}
    return {
        "occurred_at": facts["date"] if placed else None,
        "time_phrase": reading["evidence"],
        "time_options": reading["options"] if reading["status"] == "ambiguous" else [],
    }


def preview_capture(user_id: str, text: str, device_time: datetime) -> dict:
    if device_time.tzinfo is None:
        raise HTTPException(status_code=422, detail={"code": "invalid_device_time"})
    clean = clean_text(text, TEXT_MAX_LENGTH, "invalid_capture_text")
    problems: list[str] = []
    with db_conn() as conn:
        with conn.cursor() as cur:
            # The preview's whole contract is that it writes nothing; the database enforces it.
            cur.execute("SET TRANSACTION READ ONLY")
            try:
                facts, accounts, _ = capture_facts(cur, user_id, {"text": clean, "device_time": device_time})
            except HTTPException as error:
                if (error.detail or {}).get("code") != "invalid_capture_date":
                    raise
                logger.info("Capture preview user=%s outcome=problem problems=unusable_date", user_id)
                return {"amount": None, "problems": ["unusable_date"], "kind": None, "description": None,
                        "occurred_at": None, "time_phrase": None, "time_options": [], "account_hint": None,
                        "category_hint": None}
            memory = memory_state(cur, user_id, facts)
            category = category_hint(cur, user_id, memory)
    if facts["amount_issue"]:
        problems.append(AMOUNT_PROBLEMS[facts["amount_issue"]])
    preview = {
        "amount": facts["amount"] if not facts["amount_issue"] else None,
        "problems": problems,
        "kind": facts["kind"] or facts["kind_guess"],
        "description": facts["description"] or None,
        **time_preview(facts),
        "account_hint": account_hint(facts, accounts),
        "category_hint": category,
    }
    logger.info("Capture preview user=%s outcome=%s problems=%s", user_id, "problem" if problems else "read",
                ",".join(problems) or "none")
    return preview
