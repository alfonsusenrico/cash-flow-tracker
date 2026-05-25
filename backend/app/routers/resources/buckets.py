"""Buckets CRUD — /buckets and /v1/buckets"""
from typing import Any
from fastapi import APIRouter, HTTPException, Request
from psycopg.errors import UniqueViolation
from app.db.pool import db_conn
from app.services.ledger.balances import parse_uuid_value

router = APIRouter(tags=["buckets"])


def _row(r: dict) -> dict:
    return {
        "bucket_id": r["bucket_id"],
        "name": r["name"],
        "kind": r["kind"],
        "target_amount": r.get("target_amount"),
        "linked_account_id": r.get("linked_account_id"),
        "priority": r["priority"],
        "is_archived": r["is_archived"],
        "notes": r.get("notes"),
        "created_at": r["created_at"].isoformat(),
    }


@router.get("")
def list_buckets(req: Request, include_archived: bool = False):
    username = req.state.username
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.bucket_id::text AS bucket_id, b.name, b.kind, b.target_amount,
                   b.linked_account_id::text AS linked_account_id,
                   b.priority, b.is_archived, b.notes, b.created_at
            FROM buckets b
            JOIN users u ON u.user_id = b.user_id
            WHERE u.username = %s AND (%s OR b.is_archived = FALSE)
            ORDER BY b.priority ASC, b.name ASC
            """,
            (username, include_archived),
        )
        return {"buckets": cur.fetchall()}


@router.post("")
async def create_bucket(req: Request):
    username = req.state.username
    data: dict[str, Any] = await req.json()
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    kind = str(data.get("kind") or "spending").strip().lower()
    if kind not in ("spending", "sinking", "emergency", "goal", "investment"):
        raise HTTPException(status_code=400, detail="Invalid kind")
    target_amount = data.get("target_amount")
    if target_amount is not None:
        target_amount = int(target_amount)
    linked_account_id = data.get("linked_account_id") or None
    priority = int(data.get("priority") or 50)
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO buckets (user_id, name, kind, target_amount, linked_account_id, priority, notes)
                SELECT user_id, %s, %s, %s, %s::uuid, %s, %s FROM users WHERE username=%s
                RETURNING bucket_id::text AS bucket_id
                """,
                (name, kind, target_amount, linked_account_id, priority, notes, username),
            )
            row = cur.fetchone()
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Bucket name already exists")
    return {"ok": True, "bucket_id": row["bucket_id"]}


@router.put("/{bucket_id}")
async def update_bucket(bucket_id: str, req: Request):
    username = req.state.username
    bucket_id = parse_uuid_value(bucket_id, "bucket_id")
    data: dict[str, Any] = await req.json()
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    kind = str(data.get("kind") or "spending").strip().lower()
    if kind not in ("spending", "sinking", "emergency", "goal", "investment"):
        raise HTTPException(status_code=400, detail="Invalid kind")
    target_amount = data.get("target_amount")
    if target_amount is not None:
        target_amount = int(target_amount)
    linked_account_id = data.get("linked_account_id") or None
    priority = int(data.get("priority") or 50)
    notes = (data.get("notes") or "").strip() or None
    is_archived = bool(data.get("is_archived", False))

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                UPDATE buckets SET name=%s, kind=%s, target_amount=%s,
                  linked_account_id=%s::uuid, priority=%s, notes=%s, is_archived=%s
                WHERE bucket_id=%s::uuid
                  AND user_id=(SELECT user_id FROM users WHERE username=%s)
                RETURNING bucket_id
                """,
                (name, kind, target_amount, linked_account_id, priority, notes, is_archived, bucket_id, username),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Bucket not found")
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Bucket name already exists")
    return {"ok": True}


@router.delete("/{bucket_id}")
def delete_bucket(bucket_id: str, req: Request):
    username = req.state.username
    bucket_id = parse_uuid_value(bucket_id, "bucket_id")
    with db_conn() as conn, conn.cursor() as cur:
        # Soft-archive instead of hard delete to preserve allocation item links
        cur.execute(
            """
            UPDATE buckets SET is_archived=TRUE
            WHERE bucket_id=%s::uuid
              AND user_id=(SELECT user_id FROM users WHERE username=%s)
            RETURNING bucket_id
            """,
            (bucket_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Bucket not found")
        conn.commit()
    return {"ok": True}
