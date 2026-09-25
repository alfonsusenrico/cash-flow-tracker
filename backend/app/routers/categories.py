from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.db.pool import db_conn
from app.services.auth import get_current_user

router = APIRouter(tags=["Categories"])


class CategoryCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    icon: str = Field(default="tag", max_length=50)
    color: str = Field(default="#3b82f6", max_length=30)
    kind: str = Field(default="expense", pattern="^(expense|income)$")
    monthly_budget: int | None = Field(default=None, ge=0)
    is_primary: bool = True
    kakeibo_type: str | None = Field(default=None, pattern="^(need|want|saving)$")


class CategoryUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    icon: str | None = Field(default=None, max_length=50)
    color: str | None = Field(default=None, max_length=30)
    kind: str | None = Field(default=None, pattern="^(expense|income)$")
    monthly_budget: int | None = Field(default=None, ge=0)
    is_primary: bool | None = None
    kakeibo_type: str | None = Field(default=None, pattern="^(need|want|saving)$")
    is_archived: bool | None = None


@router.get("")
def list_categories(
    kind: str | None = None,
    include_archived: bool = False,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    query = """
        SELECT id, name, icon, color, kind, monthly_budget, is_primary, kakeibo_type, is_archived, created_at
        FROM categories
        WHERE user_id = %s
    """
    params = [user_id]

    if not include_archived:
        query += " AND is_archived = FALSE"
    if kind in ("expense", "income"):
        query += " AND kind = %s"
        params.append(kind)

    query += " ORDER BY kind DESC, name ASC"

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

    return {
        "ok": True,
        "categories": [
            {
                "id": str(r["id"]),
                "category_id": str(r["id"]),
                "name": r["name"],
                "category_name": r["name"],
                "icon": r["icon"],
                "color": r["color"],
                "kind": r["kind"],
                "monthly_budget": r["monthly_budget"],
                "is_primary": bool(r.get("is_primary", True)),
                "kakeibo_type": (
                    r.get("kakeibo_type") or ("need" if r.get("is_primary", True) else "want")
                ) if r["kind"] == "expense" else None,
                "is_archived": r["is_archived"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ],
    }


@router.post("")
def create_category(payload: CategoryCreate, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    name = payload.name.strip()
    if payload.kind == "income" and (payload.monthly_budget is not None or payload.kakeibo_type is not None):
        raise HTTPException(status_code=422, detail="Income categories cannot have an expense budget or Kakeibo pillar")
    kakeibo_type = (
        payload.kakeibo_type or ("need" if payload.is_primary else "want")
    ) if payload.kind == "expense" else None

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM categories WHERE user_id = %s AND name = %s AND kind = %s",
                (user_id, name, payload.kind),
            )
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="Category already exists for this type")

            cur.execute(
                """
                INSERT INTO categories (user_id, name, icon, color, kind, monthly_budget, is_primary, kakeibo_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, name, icon, color, kind, monthly_budget, is_primary, kakeibo_type, is_archived, created_at
                """,
                (
                    user_id,
                    name,
                    payload.icon,
                    payload.color,
                    payload.kind,
                    payload.monthly_budget,
                    payload.is_primary,
                    kakeibo_type,
                ),
            )
            cat = cur.fetchone()
            conn.commit()

    return {
        "ok": True,
        "category": {
            "id": str(cat["id"]),
            "name": cat["name"],
            "icon": cat["icon"],
            "color": cat["color"],
            "kind": cat["kind"],
            "monthly_budget": cat["monthly_budget"],
            "is_primary": bool(cat.get("is_primary", True)),
            "kakeibo_type": (
                cat.get("kakeibo_type") or ("need" if cat.get("is_primary", True) else "want")
            ) if cat["kind"] == "expense" else None,
            "is_archived": cat["is_archived"],
            "created_at": cat["created_at"].isoformat(),
        },
    }


@router.put("/{category_id}")
@router.patch("/{category_id}")
def update_category(
    category_id: UUID, payload: CategoryUpdate, current_user: dict = Depends(get_current_user)
):
    user_id = current_user["id"]
    cid = str(category_id)
    updates = []
    params = []

    if payload.name is not None:
        updates.append("name = %s")
        params.append(payload.name.strip())
    if payload.icon is not None:
        updates.append("icon = %s")
        params.append(payload.icon)
    if payload.color is not None:
        updates.append("color = %s")
        params.append(payload.color)
    if payload.kind is not None:
        updates.append("kind = %s")
        params.append(payload.kind)
    if "monthly_budget" in payload.model_fields_set:
        updates.append("monthly_budget = %s")
        params.append(payload.monthly_budget)
    if payload.is_primary is not None:
        updates.append("is_primary = %s")
        params.append(payload.is_primary)
    if payload.kakeibo_type is not None:
        updates.append("kakeibo_type = %s")
        params.append(payload.kakeibo_type)
    if payload.is_archived is not None:
        updates.append("is_archived = %s")
        params.append(payload.is_archived)

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    updates.append("updated_at = NOW()")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT kind FROM categories WHERE user_id = %s AND id = %s FOR UPDATE",
                (user_id, cid),
            )
            current_category = cur.fetchone()
            if not current_category:
                raise HTTPException(status_code=404, detail="Category not found")
            final_kind = payload.kind or current_category["kind"]
            if final_kind == "income":
                if payload.monthly_budget is not None or payload.kakeibo_type is not None:
                    raise HTTPException(status_code=422, detail="Income categories cannot have an expense budget or Kakeibo pillar")
                if payload.kind == "income":
                    updates.extend(["monthly_budget = NULL", "kakeibo_type = NULL"])
            elif payload.is_primary is not None and payload.kakeibo_type is None:
                updates.append("kakeibo_type = %s")
                params.append("need" if payload.is_primary else "want")
            if payload.kind and payload.kind != current_category["kind"]:
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM transactions WHERE category_id = %s AND type != %s) OR EXISTS (SELECT 1 FROM recurring_rules WHERE category_id = %s AND type != %s) AS has_incompatible_usage",
                    (cid, payload.kind, cid, payload.kind),
                )
                if cur.fetchone()["has_incompatible_usage"]:
                    raise HTTPException(status_code=409, detail="Category type cannot change while incompatible transactions or rules use it")
            params.extend([user_id, cid])
            cur.execute(
                f"""
                UPDATE categories
                SET {', '.join(updates)}
                WHERE user_id = %s AND id = %s
                RETURNING id, name, icon, color, kind, monthly_budget, is_primary, kakeibo_type, is_archived, created_at
                """,
                params,
            )
            cat = cur.fetchone()
            if not cat:
                raise HTTPException(status_code=404, detail="Category not found")
            conn.commit()

    return {
        "ok": True,
        "category": {
            "id": str(cat["id"]),
            "name": cat["name"],
            "icon": cat["icon"],
            "color": cat["color"],
            "kind": cat["kind"],
            "monthly_budget": cat["monthly_budget"],
            "is_primary": bool(cat.get("is_primary", True)),
            "kakeibo_type": (
                cat.get("kakeibo_type") or ("need" if cat.get("is_primary", True) else "want")
            ) if cat["kind"] == "expense" else None,
            "is_archived": cat["is_archived"],
            "created_at": cat["created_at"].isoformat() if cat["created_at"] else None,
        },
    }


@router.delete("/{category_id}")
def delete_category(category_id: UUID, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    cid = str(category_id)

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (SELECT 1 FROM transactions WHERE category_id = %s)
                    OR EXISTS (SELECT 1 FROM recurring_rules WHERE category_id = %s) AS is_referenced
                """,
                (cid, cid),
            )
            row = cur.fetchone()
            if row and row["is_referenced"]:
                cur.execute(
                    "UPDATE categories SET is_archived = TRUE, updated_at = NOW() WHERE user_id = %s AND id = %s",
                    (user_id, cid),
                )
                conn.commit()
                return {"ok": True, "message": "Category archived because it is referenced by ledger or recurring activity"}

            cur.execute(
                "DELETE FROM categories WHERE user_id = %s AND id = %s RETURNING id",
                (user_id, cid),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Category not found")
            conn.commit()

    return {"ok": True, "message": "Category deleted"}
