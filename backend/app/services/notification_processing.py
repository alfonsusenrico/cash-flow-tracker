from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import datetime, timezone
from uuid import uuid4

import psycopg
from fastapi import HTTPException
from psycopg.types.json import Jsonb

from app.core.config import settings
from app.db.pool import db_conn
from app.services.notification_application import (
    apply_broker_trade, apply_interpretation, committed_snapshot, deterministic_interpretation,
)
from app.services.notification_context import load_context
from app.services.notification_evidence import EvidenceError, collect_facts
from app.services.notification_interpretation import PROMPT_VERSION
from app.services.openai_notification_provider import ProviderError, configuration_error


logger = logging.getLogger(__name__)
MAX_ATTEMPTS = 3


def source_event(event: dict) -> dict:
    original = dict(event)
    captured = event.get("captured_facts") or {}
    if captured:
        original["expected_amount"] = captured.get("expected_amount")
        original["expected_direction"] = captured.get("expected_direction")
        original["post_time"] = datetime.fromisoformat(captured["post_time"])
    return original


def set_outcome(cur, event_id: str, status: str, code: str | None):
    cur.execute(
        """UPDATE notification_events SET processing_state = %s, error_code = %s,
           lease_token = NULL, lease_expires_at = NULL, updated_at = NOW() WHERE id = %s""",
        (status, code, event_id),
    )


def compact_result(cur, event: dict) -> dict:
    if event.get("processing_state") is None:
        materialize_legacy_result(cur, event)
    status = event["processing_state"]
    result = {"payload_hash": event["payload_hash"], "event_id": str(event["id"]), "status": status}
    if status == "recorded":
        result.update(event["result_snapshot"])
        if event.get("transaction_id") and result.get("type") in {"expense", "income"} and not event.get("movement_id"):
            cur.execute(
                """SELECT id FROM transactions WHERE user_id = %s AND id = %s
                   AND type IN ('expense', 'income') AND movement_id IS NULL AND movement_role IS NULL""",
                (event["user_id"], event["transaction_id"]),
            )
            if transaction := cur.fetchone():
                result["transaction_id"] = str(transaction["id"])
    elif event.get("error_code"):
        result["error_code"] = event["error_code"]
    return result


def materialize_legacy_result(cur, event: dict):
    if not event.get("transaction_id"):
        set_outcome(cur, str(event["id"]), "needs_review", "legacy_review_required")
        event.update(processing_state="needs_review", error_code="legacy_review_required")
        return
    cur.execute(
        """SELECT t.*, a.name AS account_name, p.name AS parent_name
           FROM transactions t JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
           LEFT JOIN accounts p ON p.id = a.parent_id AND p.user_id = a.user_id
           WHERE t.user_id = %s AND (t.id = %s OR t.movement_id = (
               SELECT movement_id FROM transactions WHERE user_id = %s AND id = %s)) ORDER BY t.id""",
        (event["user_id"], event["transaction_id"], event["user_id"], event["transaction_id"]),
    )
    transactions = cur.fetchall()
    if not transactions:
        set_outcome(cur, str(event["id"]), "needs_review", "legacy_review_required")
        event.update(processing_state="needs_review", error_code="legacy_review_required")
        return
    anchor = next(row for row in transactions if str(row["id"]) == str(event["transaction_id"]))

    def label(row):
        return f"{row['parent_name']} · {row['account_name']}" if row.get("parent_name") else row["account_name"]

    movement_id = str(anchor["movement_id"]) if anchor.get("movement_id") else None
    key = f"movement:{movement_id}" if movement_id else f"notification:{event['id']}"
    kind = anchor["type"]
    source = label(anchor) if kind == "expense" else None
    target = label(anchor) if kind == "income" else None
    if movement_id:
        outgoing = next((row for row in transactions if row.get("movement_role") == "outbound"), None)
        incoming = next((row for row in transactions if row.get("movement_role") == "inbound"), None)
        if outgoing and incoming:
            kind, source, target = "internal_movement", label(outgoing), label(incoming)
    snapshot = committed_snapshot(key=key, kind=kind, description=anchor.get("notes") or "Transaksi",
                                  amount=int(anchor["amount"]), source=source, target=target)
    cur.execute(
        """UPDATE notification_events SET processing_state = 'recorded', result_key = %s,
           result_snapshot = %s, movement_id = %s WHERE user_id = %s AND id = %s""",
        (key, Jsonb(snapshot), movement_id, event["user_id"], event["id"]),
    )
    event.update(processing_state="recorded", result_snapshot=snapshot, result_key=key)


def accept_event(payload: dict, user_id: str) -> tuple[dict, bool, int]:
    facts = collect_facts(payload)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM notification_events WHERE user_id = %s AND payload_hash = %s FOR UPDATE", (user_id, payload["payload_hash"]))
            existing = cur.fetchone()
            if existing:
                result = compact_result(cur, existing)
                conn.commit()
                return result, False, 0
            if facts.status == "ignored":
                return {"payload_hash": payload["payload_hash"], "status": "ignored"}, False, 0
            mode = "ai" if settings.notification_ai_enabled and facts.institution != "stockbit" else "deterministic"
            status = "queued" if mode == "ai" else "processing"
            error_code = facts.error_code
            if facts.status == "needs_review":
                status = "needs_review"
            elif mode == "ai" and configuration_error(settings):
                status = "failed"
                error_code = configuration_error(settings)
            fields = (
                "device_id", "package_name", "app_label", "notification_key", "notification_id", "channel_id",
                "category", "title", "body_text", "big_text", "sub_text", "summary_text", "post_time", "payload_hash",
                "raw_extras", "source_version", "is_financial", "event_class", "expected_amount", "expected_direction",
                "expected_counterparty", "label_notes",
            )
            values = [Jsonb(payload.get(name)) if name == "raw_extras" and payload.get(name) is not None else payload.get(name) for name in fields]
            captured = {"expected_amount": str(payload["expected_amount"]) if payload.get("expected_amount") is not None else None,
                        "expected_direction": payload.get("expected_direction"), "post_time": payload["post_time"].isoformat()}
            cur.execute(
                "INSERT INTO notification_events (user_id, " + ", ".join(fields)
                + ", processing_state, processing_mode, captured_facts, error_code, next_attempt_at) VALUES ("
                + ", ".join(["%s"] * (len(fields) + 5)) + ", NOW()"
                + ") ON CONFLICT (user_id, payload_hash) DO NOTHING RETURNING *",
                [user_id, *values, status, mode, Jsonb(captured), error_code],
            )
            event = cur.fetchone()
            if not event:
                cur.execute("SELECT * FROM notification_events WHERE user_id = %s AND payload_hash = %s FOR UPDATE", (user_id, payload["payload_hash"]))
                result = compact_result(cur, cur.fetchone())
                conn.commit()
                return result, False, 0
            created = 0
            if status == "processing":
                try:
                    with conn.transaction():
                        cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{user_id}",))
                        context = load_context(cur, user_id, facts, history_limit=settings.notification_ai_history_limit)
                        if context["incomplete"]:
                            raise EvidenceError("incomplete_context")
                        if facts.institution == "stockbit":
                            created = apply_broker_trade(cur, event, facts, context)
                        else:
                            proposal = deterministic_interpretation(facts, context)
                            created = apply_interpretation(cur, event, facts, proposal, context)
                except (EvidenceError, HTTPException) as exc:
                    set_outcome(cur, str(event["id"]), "needs_review", exc.code if isinstance(exc, EvidenceError) else "invalid_current_reference")
                cur.execute("SELECT * FROM notification_events WHERE user_id = %s AND id = %s", (user_id, event["id"]))
                event = cur.fetchone()
            result = compact_result(cur, event)
        conn.commit()
    return result, True, created


def claim_event(*, include_ai: bool = True) -> dict | None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE notification_events SET processing_state = 'failed', error_code = 'worker_attempts_exhausted',
                   lease_token = NULL, lease_expires_at = NULL
                   WHERE processing_state = 'processing' AND lease_expires_at < NOW() AND attempt_count >= %s""",
                (MAX_ATTEMPTS,),
            )
            cur.execute(
                """SELECT * FROM notification_events WHERE (processing_mode = 'deterministic'
                       OR (processing_mode = 'ai' AND %s)) AND attempt_count < %s
                   AND ((processing_state = 'queued' AND next_attempt_at <= NOW())
                        OR (processing_state = 'processing' AND lease_expires_at < NOW()))
                   ORDER BY next_attempt_at, created_at FOR UPDATE SKIP LOCKED LIMIT 1""",
                (include_ai, MAX_ATTEMPTS),
            )
            event = cur.fetchone()
            if not event:
                return None
            token = str(uuid4())
            cur.execute(
                """UPDATE notification_events SET processing_state = 'processing', attempt_count = attempt_count + 1,
                   processing_generation = processing_generation + 1, lease_token = %s,
                   lease_expires_at = NOW() + INTERVAL '90 seconds', provider = %s, model = %s,
                   prompt_version = %s, updated_at = NOW() WHERE id = %s RETURNING *""",
                (token, "openai" if event["processing_mode"] == "ai" else None,
                 settings.notification_ai_model if event["processing_mode"] == "ai" else None,
                 PROMPT_VERSION, event["id"]),
            )
            claimed = cur.fetchone()
        conn.commit()
    return claimed


def read_context(event: dict) -> tuple:
    facts = collect_facts(source_event(event))
    with db_conn() as conn:
        with conn.cursor() as cur:
            context = load_context(cur, str(event["user_id"]), facts, history_limit=settings.notification_ai_history_limit)
    return facts, context


def apply_claim(event: dict, proposal) -> bool:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{event['user_id']}",))
            cur.execute(
                """SELECT * FROM notification_events WHERE id = %s AND user_id = %s
                   AND processing_state = 'processing' AND lease_token = %s AND processing_generation = %s
                   AND lease_expires_at > clock_timestamp() FOR UPDATE""",
                (event["id"], event["user_id"], event["lease_token"], event["processing_generation"]),
            )
            owned = cur.fetchone()
            if not owned:
                return False
            facts = collect_facts(source_event(owned))
            context = load_context(cur, str(owned["user_id"]), facts, history_limit=settings.notification_ai_history_limit)
            if owned["processing_mode"] == "deterministic":
                if facts.status != "candidate" or context["incomplete"]:
                    raise EvidenceError(facts.error_code or "incomplete_context")
                if facts.institution == "stockbit":
                    apply_broker_trade(cur, owned, facts, context)
                else:
                    apply_interpretation(cur, owned, facts, deterministic_interpretation(facts, context), context)
            else:
                apply_interpretation(cur, owned, facts, proposal, context)
        conn.commit()
    return True


def fail_claim(event: dict, error: ProviderError):
    retry = error.transient and event["attempt_count"] < MAX_ATTEMPTS
    delay = max(error.retry_after, 2 ** event["attempt_count"] + random.uniform(0, 1))
    status = "queued" if retry else "failed"
    if error.code in {"provider_invalid_output", "incomplete_context", "model_uncertain", "invalid_current_reference", "invalid_interpretation"}:
        status = "needs_review"
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE notification_events SET processing_state = %s, error_code = %s,
                   next_attempt_at = NOW() + %s * INTERVAL '1 second', lease_token = NULL, lease_expires_at = NULL,
                   updated_at = NOW() WHERE id = %s AND processing_state = 'processing'
                   AND lease_token = %s AND processing_generation = %s AND lease_expires_at > clock_timestamp()""",
                (status, error.code, delay, event["id"], event["lease_token"], event["processing_generation"]),
            )
        conn.commit()


async def database_operation(function, *args):
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


async def process_claim(event: dict, provider):
    try:
        if event["processing_mode"] == "deterministic":
            await database_operation(apply_claim, event, None)
            return
        facts, context = await database_operation(read_context, event)
        if facts.status != "candidate":
            raise ProviderError("invalid_interpretation")
        if context["incomplete"]:
            raise ProviderError("incomplete_context")
        proposal = await provider.interpret(context)
        await database_operation(apply_claim, event, proposal)
    except ProviderError as exc:
        await database_operation(fail_claim, event, exc)
    except (EvidenceError, HTTPException):
        await database_operation(fail_claim, event, ProviderError("invalid_current_reference"))
    except psycopg.Error:
        await database_operation(fail_claim, event, ProviderError("database_unavailable", transient=True))


async def notification_worker(provider):
    while True:
        try:
            event = await database_operation(lambda: claim_event(include_ai=provider is not None))
            if event:
                await process_claim(event, provider)
                continue
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("Notification worker unavailable; accepted events retain durable recovery state")
        await asyncio.sleep(1)
