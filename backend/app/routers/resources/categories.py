"""
Unified resource router for categories (Phase 1).
Mounted at /categories (cookie) and /v1/categories (Bearer).
"""
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from psycopg.errors import UniqueViolation

from app.db.pool import db_conn
from app.services.categories import seed_default_categories
from app.services.ledger.balances import parse_uuid_value

router = APIRouter(tags=["categories"])


@router.get("")
def list_categories(req: Request):
    username = req.state.username
    with db_conn() as conn, conn.cursor() as cur:
        seed_default_categories(cur, username)
        cur.execute(
            """
            SELECT category_id::text AS category_id, name, kind,
                   parent_category_id::text AS parent_category_id,
                   color, icon, is_archived, created_at
            FROM categories
            WHERE user_id = (SELECT user_id FROM users WHERE username=%s)
            ORDER BY kind, name
            """,
            (username,),
        )
        rows = cur.fetchall()
        conn.commit()
        return {"categories": rows}


@router.post("")
async def create_category(req: Request):
    username = req.state.username
    data: dict[str, Any] = await req.json()
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    kind = str(data.get("kind") or "expense").strip().lower()
    if kind not in ("income", "expense", "transfer", "adjustment"):
        raise HTTPException(status_code=400, detail="Invalid kind")
    parent_id = data.get("parent_category_id")
    color = (data.get("color") or "").strip() or None
    icon = (data.get("icon") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO categories (user_id, name, kind, parent_category_id, color, icon)
                SELECT user_id, %s, %s, %s::uuid, %s, %s FROM users WHERE username=%s
                RETURNING category_id::text AS category_id
                """,
                (name, kind, parent_id, color, icon, username),
            )
            row = cur.fetchone()
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Category name already exists")
    return {"ok": True, "category_id": row["category_id"]}


@router.put("/{category_id}")
async def update_category(category_id: str, req: Request):
    username = req.state.username
    category_id = parse_uuid_value(category_id, "category_id")
    data: dict[str, Any] = await req.json()
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    kind = str(data.get("kind") or "expense").strip().lower()
    if kind not in ("income", "expense", "transfer", "adjustment"):
        raise HTTPException(status_code=400, detail="Invalid kind")
    color = (data.get("color") or "").strip() or None
    icon = (data.get("icon") or "").strip() or None
    is_archived = bool(data.get("is_archived", False))

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                UPDATE categories SET name=%s, kind=%s, color=%s, icon=%s, is_archived=%s
                WHERE category_id=%s::uuid
                  AND user_id=(SELECT user_id FROM users WHERE username=%s)
                RETURNING category_id
                """,
                (name, kind, color, icon, is_archived, category_id, username),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Category not found")
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Category name already exists")
    return {"ok": True}


@router.delete("/{category_id}")
def delete_category(category_id: str, req: Request):
    username = req.state.username
    category_id = parse_uuid_value(category_id, "category_id")
    with db_conn() as conn, conn.cursor() as cur:
        # Soft-archive instead of hard delete to preserve transaction links
        cur.execute(
            """
            UPDATE categories SET is_archived=TRUE
            WHERE category_id=%s::uuid
              AND user_id=(SELECT user_id FROM users WHERE username=%s)
            RETURNING category_id
            """,
            (category_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Category not found")
        conn.commit()
    return {"ok": True}
