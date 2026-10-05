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
    apply_broker_trade, apply_interpretation, apply_manual_resolution, committed_snapshot,
    deterministic_interpretation,
)
from app.services.notification_context import load_context, sanitize_text
from app.services.notification_evidence import EvidenceError, collect_facts
from app.services.notification_interpretation import PROMPT_VERSION
from app.services.notification_mapping import (
    mapping_decision, mark_automatic_mapping, proposal_view, request_confirmation, store_aliases,
    supports_confirmation,
)
from app.services.openai_notification_provider import ProviderError, configuration_error
from app.services.notification_sender import (
    accept_sender_answer, attach_sender_context, supports_sender_confirmation,
)


logger = logging.getLogger(__name__)
# Short in-place retries for transient blips before an event is marked failed.
MAX_ATTEMPTS = 3
# Failed events are retried automatically on an escalating schedule within a bounded horizon.
MAX_AUTOMATIC_ATTEMPTS = 8
AUTOMATIC_RETRY_HORIZON_HOURS = 48
FAILED_RETRY_DELAYS_SECONDS = (30, 120, 600, 1800, 3600, 10800)
# Failures caused by the provider, configuration, or infrastructure rather than by the
# notification itself; the owner cannot fix them by reviewing the event.
RETRYABLE_FAILURE_CODES = frozenset({
    "provider_timeout", "provider_unavailable", "database_unavailable", "provider_access_denied",
    "provider_credential_missing", "provider_model_not_permitted", "provider_reasoning_not_permitted",
    "provider_request_rejected", "ai_disabled", "processor_configuration_invalid",
    "worker_attempts_exhausted",
})


def failed_retry_delay(attempt_count: int) -> int:
    index = max(0, min(len(FAILED_RETRY_DELAYS_SECONDS) - 1, attempt_count - 1))
    return FAILED_RETRY_DELAYS_SECONDS[index]


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
    if status == "needs_confirmation":
        question = event.get("active_question") or {}
        if question.get("type") == "sender":
            result["confirmation"] = question
        else:
            result["mapping_proposal"] = proposal_view(cur, event)
            if supports_sender_confirmation(event) and question:
                result["confirmation"] = {"type": "account", "question_id": question["question_id"]}
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
                        if facts.institution == "stockbit" and facts.parsed.investment_action:
                            created = apply_broker_trade(cur, event, facts, context)
                        else:
                            proposal = deterministic_interpretation(facts, context)
                            created = apply_interpretation(cur, event, facts, proposal, context, source="deterministic")
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
                   lease_token = NULL, lease_expires_at = NULL, next_attempt_at = NOW() + INTERVAL '1 hour'
                   WHERE processing_state = 'processing' AND lease_expires_at < NOW() AND attempt_count >= %s""",
                (MAX_ATTEMPTS,),
            )
            cur.execute(
                """SELECT * FROM notification_events WHERE (processing_mode = 'deterministic'
                       OR (processing_mode = 'ai' AND %s))
                   AND ((processing_state = 'queued' AND next_attempt_at <= NOW() AND attempt_count < %s)
                        OR (processing_state = 'processing' AND lease_expires_at < NOW() AND attempt_count < %s)
                        OR (processing_state = 'failed' AND next_attempt_at <= NOW() AND attempt_count < %s
                            AND created_at > NOW() - %s * INTERVAL '1 hour'))
                   ORDER BY next_attempt_at, created_at FOR UPDATE SKIP LOCKED LIMIT 1""",
                (include_ai, MAX_ATTEMPTS, MAX_ATTEMPTS, MAX_AUTOMATIC_ATTEMPTS, AUTOMATIC_RETRY_HORIZON_HOURS),
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
            attach_sender_context(cur, event, facts, context)
    return facts, context


def apply_deterministic_fallback(conn, cur, event: dict, facts, context: dict, reason_code: str) -> None:
    """Record through the deterministic interpretation when the model could not.

    Raises EvidenceError with the backend-proven reason when the deterministic path
    cannot record either, so the event lands in needs_review with an actionable code.
    """
    if facts.status != "candidate" or context["incomplete"] or (facts.institution == "stockbit" and facts.parsed.investment_action):
        raise EvidenceError(facts.error_code or reason_code)
    try:
        with conn.transaction():
            proposal = deterministic_interpretation(facts, context)
            apply_interpretation(cur, event, facts, proposal, context, source="deterministic_fallback")
    except EvidenceError as exc:
        raise EvidenceError(exc.code) from None
    except HTTPException:
        raise EvidenceError(reason_code) from None


def apply_mapped_proposal(conn, cur, event: dict, facts, context: dict, proposal) -> None:
    """Apply the model's account choices for unresolved names per the confidence threshold."""
    decision = mapping_decision(proposal, context)
    user_id = str(event["user_id"])
    if decision["confidence"] >= settings.notification_mapping_auto_threshold:
        with conn.transaction():
            store_aliases(cur, user_id, decision["entries"], "ai")
            refreshed = load_context(cur, user_id, facts, history_limit=settings.notification_ai_history_limit)
            if refreshed["facts"].get("mapping_error"):
                raise EvidenceError("invalid_mapping_proposal")
            apply_interpretation(cur, event, facts, proposal, refreshed, source="ai")
            mark_automatic_mapping(cur, event, decision)
    elif supports_confirmation(event):
        request_confirmation(cur, event, decision)
    else:
        raise EvidenceError(context["facts"]["mapping_error"])


def apply_claim(event: dict, proposal, provider_error: str | None = None) -> bool:
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
                if facts.institution == "stockbit" and facts.parsed.investment_action:
                    apply_broker_trade(cur, owned, facts, context)
                else:
                    apply_interpretation(cur, owned, facts, deterministic_interpretation(facts, context), context,
                                         source="deterministic")
            elif (context["facts"].get("unresolved_names") and proposal is not None
                  and proposal.outcome == "record"):
                apply_mapped_proposal(conn, cur, owned, facts, context, proposal)
            elif proposal is not None and proposal.outcome == "ignored":
                apply_interpretation(cur, owned, facts, proposal, context)
            else:
                try:
                    if proposal is None or proposal.outcome != "record":
                        raise EvidenceError(provider_error or "model_uncertain")
                    with conn.transaction():
                        apply_interpretation(cur, owned, facts, proposal, context, source="ai")
                except (EvidenceError, HTTPException) as exc:
                    reason = exc.code if isinstance(exc, EvidenceError) else "invalid_current_reference"
                    apply_deterministic_fallback(conn, cur, owned, facts, context, reason)
        conn.commit()
    return True


def fail_claim(event: dict, error: ProviderError):
    attempts = event["attempt_count"]
    if error.transient and attempts < MAX_ATTEMPTS:
        status = "queued"
        delay = max(error.retry_after, 2 ** attempts + random.uniform(0, 1))
    elif error.transient or error.code in RETRYABLE_FAILURE_CODES:
        status = "failed"
        delay = max(error.retry_after, failed_retry_delay(attempts))
    else:
        status = "needs_review"
        delay = 0
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
            raise ProviderError(facts.error_code or "invalid_interpretation")
        if context["incomplete"]:
            raise ProviderError("incomplete_context")
        proposal, provider_error = None, None
        # An unusable model answer is occasional and usually not repeated, so ask once more first.
        for _ in range(2):
            try:
                proposal = await provider.interpret(context)
                provider_error = None
                break
            except ProviderError as exc:
                if exc.transient or exc.code in RETRYABLE_FAILURE_CODES:
                    raise
                provider_error = exc.code
                if exc.code != "provider_invalid_output":
                    break
        await database_operation(apply_claim, event, proposal, provider_error)
    except ProviderError as exc:
        await database_operation(fail_claim, event, exc)
    except (EvidenceError, HTTPException) as exc:
        code = exc.code if isinstance(exc, EvidenceError) else "invalid_current_reference"
        await database_operation(fail_claim, event, ProviderError(code))
    except psycopg.Error:
        await database_operation(fail_claim, event, ProviderError("database_unavailable", transient=True))


def resolve_event(event_id: str, user_id: str, resolution: dict) -> dict:
    """Record an owner-chosen interpretation of an unresolved event (idempotent per event)."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{user_id}",))
            cur.execute("SELECT * FROM notification_events WHERE user_id = %s AND id = %s FOR UPDATE", (user_id, event_id))
            event = cur.fetchone()
            if not event:
                raise HTTPException(status_code=404, detail="Notification event not found")
            current = compact_result(cur, event)
            if current["status"] == "recorded" and (event.get("interpretation") or {}).get("source") == "manual":
                conn.commit()
                return current
            if current["status"] not in {"needs_review", "failed", "needs_confirmation"}:
                raise HTTPException(status_code=409, detail="Only unresolved notifications can be recorded manually")
            facts = collect_facts(source_event(event))
            amount = resolution.get("amount")
            if facts.amount is not None:
                if amount is not None and amount != facts.amount:
                    raise HTTPException(status_code=422, detail={
                        "code": "conflicting_amount",
                        "message": "Nominal harus sama dengan nominal pada notifikasi.",
                    })
                amount = facts.amount
            elif amount is None:
                raise HTTPException(status_code=422, detail={
                    "code": "amount_required",
                    "message": "Notifikasi tidak memuat satu nominal yang pasti; isi nominal secara manual.",
                })
            context = load_context(cur, user_id, facts, history_limit=settings.notification_ai_history_limit)
            if context["incomplete"]:
                raise HTTPException(status_code=409, detail={"code": "incomplete_context", "message": "Data akun terlalu besar untuk diproses."})
            description = resolution.get("notes") or sanitize_text(
                facts.parsed.counterparty or event.get("title") or "Transaksi"
            )[:160]
            try:
                apply_manual_resolution(
                    cur, event, facts, context, direction=resolution["type"], amount=amount,
                    account_id=str(resolution["account_id"]), category_id=str(resolution["category_id"]),
                    description=description, kakeibo=resolution.get("kakeibo"),
                )
            except EvidenceError as exc:
                raise HTTPException(status_code=422, detail={"code": exc.code, "message": "Pilihan akun atau kategori tidak valid."}) from None
            cur.execute("SELECT * FROM notification_events WHERE user_id = %s AND id = %s", (user_id, event_id))
            result = compact_result(cur, cur.fetchone())
        conn.commit()
    return result


def confirm_mapping(event_id: str, user_id: str, account_id: str, question_id: str | None = None) -> dict:
    """Owner confirms or corrects a proposed account; the event is reprocessed with the new alias."""
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{user_id}",))
            cur.execute("SELECT * FROM notification_events WHERE user_id = %s AND id = %s FOR UPDATE", (user_id, event_id))
            event = cur.fetchone()
            if not event:
                raise HTTPException(status_code=404, detail="Notification event not found")
            proposal = (event.get("interpretation") or {}).get("mapping_proposal")
            if event["processing_state"] != "needs_confirmation" or not proposal:
                raise HTTPException(status_code=409, detail="Notifikasi ini tidak menunggu konfirmasi rekening")
            question = event.get("active_question") or {}
            if question_id is not None and question.get("question_id") != question_id:
                raise HTTPException(status_code=409, detail={"code": "stale_confirmation"})
            cur.execute(
                """SELECT id, type, instrument_type FROM accounts
                   WHERE user_id = %s AND id = %s AND is_archived = FALSE""",
                (user_id, account_id),
            )
            account = cur.fetchone()
            if not account or account["type"] not in {"bank", "cash", "wallet", "ewallet"} or account.get("instrument_type"):
                raise HTTPException(status_code=422, detail={
                    "code": "invalid_account_reference", "message": "Pilih rekening atau kantong yang aktif.",
                })
            store_aliases(cur, user_id, [{**proposal, "account_id": account_id}], "owner")
            mode = event["processing_mode"]
            if mode == "ai" and configuration_error(settings):
                mode = "deterministic"
            cur.execute(
                """UPDATE notification_events SET processing_state = 'queued', processing_mode = %s,
                       attempt_count = 0, processing_generation = processing_generation + 1,
                       next_attempt_at = NOW(), error_code = NULL, interpretation = NULL, active_question = NULL,
                       lease_token = NULL, lease_expires_at = NULL, updated_at = NOW()
                   WHERE user_id = %s AND id = %s RETURNING *""",
                (mode, user_id, event_id),
            )
            result = compact_result(cur, cur.fetchone())
        conn.commit()
    return result


def confirm_sender(event_id: str, user_id: str, answer: dict) -> dict:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{user_id}",))
            cur.execute("SELECT * FROM notification_events WHERE user_id = %s AND id = %s FOR UPDATE", (user_id, event_id))
            event = cur.fetchone()
            if not event:
                raise HTTPException(status_code=404, detail="Notification event not found")
            if accept_sender_answer(cur, event, answer):
                mode = event["processing_mode"]
                if mode == "ai" and configuration_error(settings):
                    mode = "deterministic"
                cur.execute(
                    """UPDATE notification_events SET processing_state = 'queued', processing_mode = %s,
                           attempt_count = 0, processing_generation = processing_generation + 1,
                           next_attempt_at = NOW(), error_code = NULL, interpretation = NULL,
                           lease_token = NULL, lease_expires_at = NULL, updated_at = NOW()
                       WHERE user_id = %s AND id = %s""",
                    (mode, user_id, event_id),
                )
            cur.execute("SELECT * FROM notification_events WHERE user_id = %s AND id = %s", (user_id, event_id))
            result = compact_result(cur, cur.fetchone())
        conn.commit()
    return result


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
