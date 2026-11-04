from __future__ import annotations

from app.core.config import settings
from app.db.pool import db_conn
from app.services.notification_context import load_context, sanitize_text
from app.services.notification_application import account_label, resolve_record
from app.services.notification_evidence import EvidenceError, collect_facts
from app.services.notification_interpretation import validate_interpretation
from app.services.notification_processing import database_operation, source_event
from app.services.openai_notification_provider import ProviderError, dry_run_configuration_error


def dry_run_available() -> bool:
    return dry_run_configuration_error(settings) is None


def stored_event(event_id: str, user_id: str) -> dict | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            cur.execute("SELECT * FROM notification_events WHERE id = %s AND user_id = %s", (event_id, user_id))
            return cur.fetchone()


def context_for(user_id: str, event: dict):
    facts = collect_facts(source_event(event))
    if facts.status != "candidate" or facts.institution == "stockbit":
        return facts, None
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
            return facts, load_context(cur, user_id, facts, history_limit=settings.notification_ai_history_limit)


def compact_proposal(proposal, context: dict, resolved: dict) -> dict:
    accounts = context["accounts"]
    categories = {str(row["id"]): row["name"] for row in context["categories"]}
    return {
        "description": sanitize_text(proposal.description)[:160],
        "type": proposal.direction,
        "source": account_label(resolved["source"], accounts) if resolved["source"] else None,
        "target": account_label(resolved["target"], accounts) if resolved["target"] else None,
        "account": account_label(resolved["observed"], accounts) if resolved["observed"] else None,
        "category": categories.get(resolved["category_id"]),
        "kakeibo": resolved["kakeibo"],
    }


async def run_dry_run(provider, user_id: str, event: dict) -> dict:
    facts, context = await database_operation(context_for, user_id, event)
    if facts.status == "ignored":
        return {"status": "ignored"}
    if facts.institution == "stockbit":
        return {"status": "not_ai_eligible"}
    if facts.status != "candidate" or context is None or context["incomplete"]:
        return {"status": "needs_review", "error_code": facts.error_code or "incomplete_context"}
    try:
        proposal = await provider.interpret(context)
        validate_interpretation(proposal, facts, context)
        if proposal.outcome != "record":
            result = {"status": proposal.outcome}
            if proposal.outcome == "needs_review":
                result["error_code"] = proposal.review_reason if proposal.review_reason != "none" else "model_uncertain"
            return result
        resolved = resolve_record(facts, proposal, context)
    except EvidenceError as exc:
        return {"status": "needs_review", "error_code": exc.code}
    except ProviderError as exc:
        status = "needs_review" if exc.code in {"provider_invalid_output", "incomplete_context"} else "failed"
        return {"status": status, "error_code": exc.code}
    result = compact_proposal(proposal, context, resolved)
    result.update(status="would_record", amount=facts.amount, currency=facts.currency, timestamp=facts.timestamp.isoformat())
    return result
