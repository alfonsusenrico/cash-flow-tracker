"""Companion health reports: the latest self-reported capture readiness of each paired phone.

Only the newest report per device key is kept. Logs carry the key id and codes, never names or keys.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Literal

from fastapi import HTTPException, Request
from pydantic import AwareDatetime, BaseModel, Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator

from app.db.pool import db_conn
from app.errors import ErrorCode, api_error
from app.services.auth import hash_token

logger = logging.getLogger(__name__)

HEALTH_RATE_LIMIT = 30
HEALTH_RATE_WINDOW = 60

SelfTestStatus = Literal["passed", "failed", "not_run"]
FailedStep = Literal["post", "capture", "server"]


def _iso_string_only(value: Any) -> Any:
    # Pydantic would also read numbers as Unix times; the contract allows only ISO-8601 strings.
    if value is not None and not isinstance(value, str):
        raise ValueError("timestamp must be an ISO-8601 string")
    return value


class SelfTest(BaseModel):
    status: SelfTestStatus
    failed_step: FailedStep | None = None
    at: AwareDatetime | None = None

    _at_is_string = field_validator("at", mode="before")(_iso_string_only)

    @model_validator(mode="after")
    def consistent_result(self) -> "SelfTest":
        if (self.status == "failed") != (self.failed_step is not None):
            raise ValueError("failed_step is required for a failed self-test and must be null otherwise")
        if self.status != "not_run" and self.at is None:
            raise ValueError("at is required when the self-test ran")
        return self


class HealthReport(BaseModel):
    """Unknown fields are ignored so a newer phone can add fields without breaking this server."""

    app_version: StrictStr = Field(pattern=r"^[0-9A-Za-z.+_-]{1,32}$")
    listener_connected: StrictBool
    notification_access: StrictBool
    post_notifications: StrictBool
    battery_unrestricted: StrictBool
    self_test: SelfTest
    last_capture_at: AwareDatetime | None
    queue_pending: StrictInt = Field(ge=0, le=2_147_483_647)

    _capture_is_string = field_validator("last_capture_at", mode="before")(_iso_string_only)


def device_key_invalid() -> HTTPException:
    return api_error(401, ErrorCode.DEVICE_KEY_INVALID)


def get_bearer_device(request: Request) -> dict[str, str]:
    """Resolve the calling device from its own Bearer key; a browser session is never enough."""
    header = request.headers.get("Authorization", "").strip()
    if not header.startswith("Bearer "):
        raise device_key_invalid()
    token = header[7:].strip()
    if not token:
        raise device_key_invalid()
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE api_keys SET last_used_at = NOW()
                WHERE key_hash = %s AND revoked_at IS NULL
                RETURNING id, user_id
                """,
                (hash_token(token),),
            )
            key = cur.fetchone()
        conn.commit()
    if key is None:
        raise device_key_invalid()
    return {"key_id": str(key["id"]), "user_id": str(key["user_id"])}


def is_ready(notification_access: bool, listener_connected: bool, self_test_status: str) -> bool:
    # Battery and post-notification permission are advisory: they never block readiness.
    return notification_access and listener_connected and self_test_status == "passed"


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def health_view(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "reported_at": row["reported_at"].isoformat(),
        "ready": is_ready(row["notification_access"], row["listener_connected"], row["self_test_status"]),
        "app_version": row["app_version"],
        "listener_connected": row["listener_connected"],
        "notification_access": row["notification_access"],
        "post_notifications": row["post_notifications"],
        "battery_unrestricted": row["battery_unrestricted"],
        "self_test": {
            "status": row["self_test_status"],
            "failed_step": row["self_test_failed_step"],
            "at": _iso(row["self_test_at"]),
        },
        "last_capture_at": _iso(row["last_capture_at"]),
        "queue_pending": row["queue_pending"],
    }


def record_report(key_id: str, report: HealthReport) -> dict[str, Any]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            # The share lock waits for a concurrent revoke, so a revoked key never leaves a report behind.
            cur.execute("SELECT 1 FROM api_keys WHERE id = %s AND revoked_at IS NULL FOR SHARE", (key_id,))
            if cur.fetchone() is None:
                raise device_key_invalid()
            cur.execute(
                """
                INSERT INTO device_health (
                    api_key_id, app_version, listener_connected, notification_access, post_notifications,
                    battery_unrestricted, self_test_status, self_test_failed_step, self_test_at,
                    last_capture_at, queue_pending, reported_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (api_key_id) DO UPDATE SET
                    app_version = EXCLUDED.app_version,
                    listener_connected = EXCLUDED.listener_connected,
                    notification_access = EXCLUDED.notification_access,
                    post_notifications = EXCLUDED.post_notifications,
                    battery_unrestricted = EXCLUDED.battery_unrestricted,
                    self_test_status = EXCLUDED.self_test_status,
                    self_test_failed_step = EXCLUDED.self_test_failed_step,
                    self_test_at = EXCLUDED.self_test_at,
                    last_capture_at = EXCLUDED.last_capture_at,
                    queue_pending = EXCLUDED.queue_pending,
                    reported_at = EXCLUDED.reported_at
                RETURNING *
                """,
                (
                    key_id,
                    report.app_version,
                    report.listener_connected,
                    report.notification_access,
                    report.post_notifications,
                    report.battery_unrestricted,
                    report.self_test.status,
                    report.self_test.failed_step,
                    report.self_test.at,
                    report.last_capture_at,
                    report.queue_pending,
                ),
            )
            row = cur.fetchone()
        conn.commit()
    health = health_view(row)
    logger.info(
        "Device health reported key_id=%s ready=%s self_test=%s failed_step=%s",
        key_id, health["ready"], report.self_test.status, report.self_test.failed_step,
    )
    return health
