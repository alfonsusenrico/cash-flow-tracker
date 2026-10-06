from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.db.pool import db_conn
from app.services.auth import get_current_user

router = APIRouter(tags=["Notification Ingestion"])


class NotificationEventIn(BaseModel):
    device_id: str = Field(..., max_length=64)
    package_name: str = Field(..., max_length=255)
    app_label: str | None = Field(default=None, max_length=100)
    notification_key: str | None = None
    notification_id: int | None = None
    channel_id: str | None = Field(default=None, max_length=100)
    category: str | None = Field(default=None, max_length=50)
    title: str | None = None
    body_text: str | None = None
    big_text: str | None = None
    sub_text: str | None = None
    summary_text: str | None = None
    post_time: datetime
    payload_hash: str = Field(..., max_length=64)
    raw_extras: dict[str, Any] | None = None
    source_version: str | None = Field(default=None, max_length=50)
    is_financial: bool | None = None
    event_class: str | None = Field(default=None, max_length=50)
    expected_amount: Decimal | None = None
    expected_direction: str | None = Field(default=None, max_length=20)
    expected_counterparty: str | None = Field(default=None, max_length=255)
    label_notes: str | None = None


class BatchNotificationIngest(BaseModel):
    events: list[NotificationEventIn] = Field(..., min_length=1, max_length=200)


class DryRunNotificationIn(BaseModel):
    event: NotificationEventIn


class NotificationResolution(BaseModel):
    type: Literal["expense", "income"]
    account_id: UUID
    category_id: UUID
    amount: int | None = Field(default=None, gt=0, le=9_223_372_036_854_775_807)
    notes: str | None = Field(default=None, max_length=160)
    kakeibo: Literal["need", "want", "culture", "unexpected", "saving"] | None = None


class MappingConfirmation(BaseModel):
    account_id: UUID
    question_id: UUID | None = None


class SenderConfirmation(BaseModel):
    question_id: UUID
    reply_id: UUID
    action: Literal["name", "unknown"]
    name: str | None = Field(default=None, max_length=80)


class SourcePocketConfirmation(BaseModel):
    question_id: UUID
    reply_id: UUID
    account_id: UUID


class SenderAliasUpdate(BaseModel):
    name: str = Field(max_length=80)


class NotificationLabelUpdate(BaseModel):
    is_financial: bool | None = None
    event_class: str | None = Field(default=None, max_length=50)
    expected_amount: Decimal | None = None
    expected_direction: str | None = Field(default=None, max_length=20)
    expected_counterparty: str | None = Field(default=None, max_length=255)
    label_notes: str | None = None


@router.post("/notifications")
def ingest_notifications(
    payload: BatchNotificationIngest,
    current_user: dict = Depends(get_current_user),
):
    from app.services.notification_processing import accept_event
    import psycopg

    inserted = updated = created = 0
    results = []
    for event in payload.events:
        try:
            result, accepted, transaction_count = accept_event(event.model_dump(), str(current_user["id"]))
        except psycopg.Error:
            result = {"payload_hash": event.payload_hash, "status": "failed", "error_code": "acceptance_failed"}
            accepted, transaction_count = False, 0
        results.append(result)
        inserted += int(accepted)
        updated += int(not accepted and "event_id" in result)
        created += transaction_count
    return {
        "ok": not any(result.get("error_code") == "acceptance_failed" for result in results),
        "received": len(payload.events), "inserted": inserted, "updated": updated,
        "created_transactions": created, "results": results,
    }


def dry_run_guard(request: Request):
    from app.services.notification_dry_run import dry_run_available
    if not dry_run_available() or not getattr(request.app.state, "notification_provider", None):
        raise HTTPException(status_code=404, detail="Not found")
    return request.app.state.notification_provider


@router.post("/notifications/dry-run")
async def dry_run_notification(payload: DryRunNotificationIn, request: Request, current_user: dict = Depends(get_current_user)):
    from app.services.notification_dry_run import run_dry_run
    provider = dry_run_guard(request)
    return {"ok": True, "result": await run_dry_run(provider, str(current_user["id"]), payload.event.model_dump())}


@router.post("/notifications/{event_id}/dry-run")
async def dry_run_stored_notification(event_id: UUID, request: Request, current_user: dict = Depends(get_current_user)):
    from app.services.notification_dry_run import run_dry_run, stored_event
    from app.services.notification_processing import database_operation
    provider = dry_run_guard(request)
    event = await database_operation(stored_event, str(event_id), str(current_user["id"]))
    if not event:
        raise HTTPException(status_code=404, detail="Notification event not found")
    return {"ok": True, "result": await run_dry_run(provider, str(current_user["id"]), event)}


@router.get("/notifications/{event_id}/result")
def notification_result(event_id: UUID, current_user: dict = Depends(get_current_user)):
    from app.services.notification_processing import compact_result

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM notification_events WHERE user_id = %s AND id = %s FOR UPDATE", (current_user["id"], str(event_id)))
            event = cur.fetchone()
            if not event:
                raise HTTPException(status_code=404, detail="Notification event not found")
            result = compact_result(cur, event)
        conn.commit()
    return {"ok": True, "result": result}


@router.post("/notifications/{event_id}/retry")
def retry_notification(event_id: UUID, request: Request, current_user: dict = Depends(get_current_user)):
    from app.services import notification_processing
    from app.services.notification_mapping import newer_companion_version
    from app.services.openai_notification_provider import configuration_error

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM notification_events WHERE user_id = %s AND id = %s FOR UPDATE", (current_user["id"], str(event_id)))
            event = cur.fetchone()
            if not event:
                raise HTTPException(status_code=404, detail="Notification event not found")
            result = notification_processing.compact_result(cur, event)
            if result["status"] not in {"needs_review", "failed", "needs_confirmation"}:
                raise HTTPException(status_code=409, detail="Only unresolved notifications can be retried")
            mode = event["processing_mode"]
            if mode not in {"ai", "deterministic"}:
                mode = "deterministic" if event["package_name"].lower() == "com.stockbit.android" else "ai"
            if mode == "ai" and configuration_error(notification_processing.settings):
                raise HTTPException(status_code=409, detail="AI processing unavailable")
            # An event captured by an older app build can use features of the build that retries it.
            companion = newer_companion_version(event.get("source_version"), request.headers.get("X-Companion-Version"))
            cur.execute(
                """UPDATE notification_events SET processing_state = 'queued', processing_mode = %s,
                   attempt_count = 0, processing_generation = processing_generation + 1,
                   next_attempt_at = NOW(), error_code = NULL, lease_token = NULL, lease_expires_at = NULL,
                   source_version = %s
                   WHERE user_id = %s AND id = %s RETURNING *""",
                (mode, companion, current_user["id"], str(event_id)),
            )
            result = notification_processing.compact_result(cur, cur.fetchone())
        conn.commit()
    return {"ok": True, "result": result}


@router.post("/notifications/{event_id}/resolve")
def resolve_notification(
    event_id: UUID, payload: NotificationResolution, current_user: dict = Depends(get_current_user)
):
    from app.services.notification_processing import resolve_event

    resolution = payload.model_dump()
    if resolution["notes"] is not None:
        resolution["notes"] = resolution["notes"].strip() or None
    return {"ok": True, "result": resolve_event(str(event_id), str(current_user["id"]), resolution)}


@router.post("/notifications/{event_id}/confirm-mapping")
def confirm_notification_mapping(
    event_id: UUID, payload: MappingConfirmation, current_user: dict = Depends(get_current_user)
):
    from app.services.notification_processing import confirm_mapping

    question_id = str(payload.question_id) if payload.question_id is not None else None
    return {"ok": True, "result": confirm_mapping(str(event_id), str(current_user["id"]), str(payload.account_id), question_id)}


@router.post("/notifications/{event_id}/confirm-sender")
def confirm_notification_sender(
    event_id: UUID, payload: SenderConfirmation, current_user: dict = Depends(get_current_user)
):
    from app.services.notification_processing import confirm_sender

    answer = payload.model_dump(mode="json")
    return {"ok": True, "result": confirm_sender(str(event_id), str(current_user["id"]), answer)}


@router.get("/notifications/{event_id}/source-pockets")
def get_source_pockets(event_id: UUID, current_user: dict = Depends(get_current_user)):
    from app.services.notification_source_pocket import source_options

    with db_conn() as conn:
        event = conn.execute("SELECT * FROM notification_events WHERE user_id = %s AND id = %s",
                             (current_user["id"], event_id)).fetchone()
        if not event:
            raise HTTPException(status_code=404, detail="Notification event not found")
        with conn.cursor() as cur:
            result = source_options(cur, event)
    return {"ok": True, "result": result}


@router.post("/notifications/{event_id}/confirm-source-pocket")
def confirm_notification_source_pocket(
    event_id: UUID, payload: SourcePocketConfirmation, current_user: dict = Depends(get_current_user),
):
    from app.services.notification_processing import confirm_source_pocket

    return {"ok": True, "result": confirm_source_pocket(
        str(event_id), str(current_user["id"]), payload.model_dump(mode="json"),
    )}


@router.get("/sender-aliases")
def list_sender_aliases(current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        rows = conn.execute(
            """SELECT al.*, a.name AS account_name, p.name AS parent_name
               FROM notification_sender_aliases al
               JOIN accounts a ON a.id = al.receiving_account_id AND a.user_id = al.user_id
               LEFT JOIN accounts p ON p.id = a.parent_id AND p.user_id = a.user_id
               WHERE al.user_id = %s ORDER BY al.created_at DESC, al.id""",
            (current_user["id"],),
        ).fetchall()
    return {"ok": True, "aliases": [{
        "id": str(row["id"]), "institution": row["institution"], "mask": row["mask_display"],
        "name": row["sender_name"], "state": row["state"], "account_id": str(row["receiving_account_id"]),
        "account": f"{row['parent_name']} · {row['account_name']}" if row["parent_name"] else row["account_name"],
    } for row in rows]}


@router.patch("/sender-aliases/{alias_id}")
def update_sender_alias(alias_id: UUID, payload: SenderAliasUpdate, current_user: dict = Depends(get_current_user)):
    from app.services.notification_sender import validate_sender_name

    name = validate_sender_name(payload.name)
    with db_conn() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{current_user['id']}",))
        updated = conn.execute(
            """UPDATE notification_sender_aliases SET sender_name = %s, state = 'active', updated_at = NOW()
               WHERE user_id = %s AND id = %s RETURNING id""",
            (name, current_user["id"], str(alias_id)),
        ).fetchone()
        if not updated:
            raise HTTPException(status_code=404, detail="Sender alias not found")
        conn.commit()
    return {"ok": True}


@router.delete("/sender-aliases/{alias_id}")
def delete_sender_alias(alias_id: UUID, current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"notification:{current_user['id']}",))
        deleted = conn.execute(
            "DELETE FROM notification_sender_aliases WHERE user_id = %s AND id = %s RETURNING id",
            (current_user["id"], str(alias_id)),
        ).fetchone()
        if not deleted:
            raise HTTPException(status_code=404, detail="Sender alias not found")
        conn.commit()
    return {"ok": True}


@router.get("/aliases")
def list_notification_aliases(current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT al.id, al.institution, al.name_display, al.account_id, al.source, al.created_at,
                          a.name AS account_name, p.name AS parent_name
                   FROM notification_account_aliases al
                   JOIN accounts a ON a.id = al.account_id AND a.user_id = al.user_id
                   LEFT JOIN accounts p ON p.id = a.parent_id AND p.user_id = a.user_id
                   WHERE al.user_id = %s ORDER BY al.created_at DESC""",
                (current_user["id"],),
            )
            rows = cur.fetchall()
    return {"ok": True, "aliases": [{
        "id": str(row["id"]), "institution": row["institution"], "name": row["name_display"],
        "account_id": str(row["account_id"]),
        "account": f"{row['parent_name']} · {row['account_name']}" if row["parent_name"] else row["account_name"],
        "source": row["source"], "created_at": row["created_at"].isoformat(),
    } for row in rows]}


@router.delete("/aliases/{alias_id}")
def delete_notification_alias(alias_id: UUID, current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM notification_account_aliases WHERE user_id = %s AND id = %s RETURNING id",
                (current_user["id"], str(alias_id)),
            )
            deleted = cur.fetchone()
        conn.commit()
    if not deleted:
        raise HTTPException(status_code=404, detail="Alias not found")
    return {"ok": True}


@router.get("/notifications")
def list_ingested_notifications(
    package_name: str | None = None,
    is_financial: bool | None = None,
    event_class: str | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    """
    List captured notification events for inspection, labeling, and parser testing.
    """
    user_id = current_user["id"]
    conditions = ["user_id = %s"]
    params: list[Any] = [user_id]

    if package_name:
        conditions.append("package_name = %s")
        params.append(package_name)
    if is_financial is not None:
        conditions.append("is_financial = %s")
        params.append(is_financial)
    if event_class:
        conditions.append("event_class = %s")
        params.append(event_class)

    where_clause = " AND ".join(conditions)

    count_query = f"SELECT COUNT(*) AS total FROM notification_events WHERE {where_clause}"
    select_query = f"""
        SELECT 
            id, device_id, package_name, app_label,
            notification_id, channel_id, category,
            title, body_text, big_text, sub_text, summary_text,
            post_time, captured_at, payload_hash, raw_extras, source_version,
            is_financial, event_class, expected_amount, expected_direction,
            expected_counterparty, label_notes, labelled_at, processing_state, error_code,
            attempt_count, provider, model, prompt_version
        FROM notification_events
        WHERE {where_clause}
        ORDER BY post_time DESC
        LIMIT %s OFFSET %s
    """

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(count_query, tuple(params))
            total = cur.fetchone()["total"]

            cur.execute(select_query, tuple(params + [limit, offset]))
            rows = cur.fetchall()

    events = []
    for r in rows:
        events.append({
            "id": str(r["id"]),
            "device_id": r["device_id"],
            "package_name": r["package_name"],
            "app_label": r["app_label"],
            "notification_id": r["notification_id"],
            "channel_id": r["channel_id"],
            "category": r["category"],
            "title": r["title"],
            "body_text": r["body_text"],
            "big_text": r["big_text"],
            "sub_text": r["sub_text"],
            "summary_text": r["summary_text"],
            "post_time": r["post_time"].isoformat(),
            "captured_at": r["captured_at"].isoformat(),
            "payload_hash": r["payload_hash"],
            "raw_extras": r["raw_extras"],
            "source_version": r["source_version"],
            "is_financial": r["is_financial"],
            "event_class": r["event_class"],
            "expected_amount": float(r["expected_amount"]) if r["expected_amount"] is not None else None,
            "expected_direction": r["expected_direction"],
            "expected_counterparty": r["expected_counterparty"],
            "label_notes": r["label_notes"],
            "labelled_at": r["labelled_at"].isoformat() if r["labelled_at"] else None,
            "processing_state": r.get("processing_state"),
            "error_code": r.get("error_code"),
            "attempt_count": r.get("attempt_count", 0),
            "provider": r.get("provider"),
            "model": r.get("model"),
            "prompt_version": r.get("prompt_version"),
        })

    return {
        "ok": True,
        "total": total,
        "limit": limit,
        "offset": offset,
        "events": events,
    }


@router.patch("/notifications/{event_id}/label")
def update_notification_label(
    event_id: UUID,
    label: NotificationLabelUpdate,
    current_user: dict = Depends(get_current_user),
):
    """
    Update label classification for a specific notification event.
    """
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE notification_events
                SET is_financial = COALESCE(%s, is_financial),
                    event_class = COALESCE(%s, event_class),
                    expected_amount = COALESCE(%s, expected_amount),
                    expected_direction = COALESCE(%s, expected_direction),
                    expected_counterparty = COALESCE(%s, expected_counterparty),
                    label_notes = COALESCE(%s, label_notes),
                    labelled_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s AND user_id = %s
                RETURNING id
                """,
                (
                    label.is_financial, label.event_class, label.expected_amount,
                    label.expected_direction, label.expected_counterparty, label.label_notes,
                    str(event_id), user_id
                ),
            )
            updated = cur.fetchone()
            if not updated:
                raise HTTPException(status_code=404, detail="Notification event not found")
        conn.commit()

    return {"ok": True, "event_id": str(event_id)}
