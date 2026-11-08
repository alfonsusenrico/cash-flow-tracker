from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.db.pool import db_conn
from app.errors import ErrorCode, api_error
from app.i18n import notice
from app.services.auth import get_current_user
from app.services.classification_gaps import category_followers, lock_owner
from app.services.kakeibo import PILLAR_PATTERN
from app.services.merchant_memory import record_owner_corrections, snapshot_rows
from app.services.report_figures import reportable_category_sql

router = APIRouter(tags=["Categories"])


class CategoryCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    icon: str = Field(default="tag", max_length=50)
    color: str = Field(default="#3b82f6", max_length=30)
    kind: str = Field(default="expense", pattern="^(expense|income)$")
    monthly_budget: int | None = Field(default=None, ge=0)
    is_primary: bool = True
    kakeibo_type: str | None = Field(default=None, pattern=PILLAR_PATTERN)


class CategoryUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    icon: str | None = Field(default=None, max_length=50)
    color: str | None = Field(default=None, max_length=30)
    kind: str | None = Field(default=None, pattern="^(expense|income)$")
    monthly_budget: int | None = Field(default=None, ge=0)
    is_primary: bool | None = None
    kakeibo_type: str | None = Field(default=None, pattern=PILLAR_PATTERN)
    is_archived: bool | None = None


@router.get("")
def list_categories(
    kind: str | None = None,
    include_archived: bool = False,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    query = """
        SELECT id, name, icon, color, kind, monthly_budget, is_primary, kakeibo_type, is_archived, created_at,
               is_excluded_from_budget, offsets_spending, system_key
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
                # The stored pillar; a category without one is unclassified, never derived from is_primary.
                "kakeibo_type": r.get("kakeibo_type") if r["kind"] == "expense" else None,
                "is_archived": r["is_archived"],
                "is_excluded_from_budget": bool(r.get("is_excluded_from_budget", False)),
                "offsets_spending": bool(r.get("offsets_spending", False)),
                # Read-only: set by seeding and the system "ensure" helpers, never by a client.
                "system_key": r["system_key"],
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
        raise api_error(422, ErrorCode.INCOME_CATEGORY_BUDGET)
    # A category created without a pillar stays unclassified; is_primary never derives one.
    kakeibo_type = payload.kakeibo_type if payload.kind == "expense" else None

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM categories WHERE user_id = %s AND name = %s AND kind = %s",
                (user_id, name, payload.kind),
            )
            if cur.fetchone():
                raise api_error(409, ErrorCode.CATEGORY_NAME_TAKEN)

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
            "kakeibo_type": cat.get("kakeibo_type") if cat["kind"] == "expense" else None,
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
    sent = payload.model_fields_set
    # Column -> value; one entry per column, so the income reset below cannot assign a column twice.
    assignments: dict[str, Any] = {}

    if payload.name is not None:
        assignments["name"] = payload.name.strip()
    if payload.icon is not None:
        assignments["icon"] = payload.icon
    if payload.color is not None:
        assignments["color"] = payload.color
    if payload.kind is not None:
        assignments["kind"] = payload.kind
    if "monthly_budget" in sent:
        assignments["monthly_budget"] = payload.monthly_budget
    if payload.is_primary is not None:
        assignments["is_primary"] = payload.is_primary
    # An explicit null clears the pillar; leaving the field out keeps the stored one.
    pillar_sent = "kakeibo_type" in sent
    if pillar_sent:
        assignments["kakeibo_type"] = payload.kakeibo_type
    if payload.is_archived is not None:
        assignments["is_archived"] = payload.is_archived

    if not assignments:
        raise api_error(400, ErrorCode.NO_FIELDS_TO_UPDATE)

    # is_primary never derives a pillar: an update without kakeibo_type keeps the stored one.
    pillar_may_change = pillar_sent or payload.kind is not None

    with db_conn() as conn:
        with conn.cursor() as cur:
            if pillar_may_change:
                # Same lock order as "Lengkapi klasifikasi": owner lock first, then the category row.
                lock_owner(cur, user_id)
            cur.execute(
                f"""SELECT kind, offsets_spending, system_key, {reportable_category_sql("c")} AS reportable
                    FROM categories c WHERE user_id = %s AND id = %s FOR UPDATE""",
                (user_id, cid),
            )
            current_category = cur.fetchone()
            if not current_category:
                raise api_error(404, ErrorCode.CATEGORY_NOT_FOUND)
            if payload.kind and payload.kind != current_category["kind"]:
                if current_category["offsets_spending"]:
                    raise api_error(409, ErrorCode.REIMBURSEMENT_CATEGORY_KIND)
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM transactions WHERE user_id = %s AND offset_category_id = %s) AS is_offset",
                    (user_id, cid),
                )
                if cur.fetchone()["is_offset"]:
                    raise api_error(409, ErrorCode.CATEGORY_KIND_HAS_OFFSETS)
                if current_category["system_key"]:
                    # One system category per key and kind (uq_category_user_system_key_kind).
                    cur.execute(
                        "SELECT EXISTS (SELECT 1 FROM categories WHERE user_id = %s AND system_key = %s AND kind = %s) AS taken",
                        (user_id, current_category["system_key"], payload.kind),
                    )
                    if cur.fetchone()["taken"]:
                        raise api_error(409, ErrorCode.SYSTEM_CATEGORY_EXISTS)
            final_kind = payload.kind or current_category["kind"]
            if final_kind == "income":
                if payload.monthly_budget is not None or payload.kakeibo_type is not None:
                    raise api_error(422, ErrorCode.INCOME_CATEGORY_BUDGET)
                if payload.kind == "income":
                    assignments["monthly_budget"] = None
                    assignments["kakeibo_type"] = None
            if payload.kind and payload.kind != current_category["kind"]:
                cur.execute(
                    "SELECT EXISTS (SELECT 1 FROM transactions WHERE category_id = %s AND type != %s) OR EXISTS (SELECT 1 FROM recurring_rules WHERE category_id = %s AND type != %s) AS has_incompatible_usage",
                    (cid, payload.kind, cid, payload.kind),
                )
                if cur.fetchone()["has_incompatible_usage"]:
                    raise api_error(409, ErrorCode.CATEGORY_KIND_IN_USE)
            # A pillar change teaches habit learning exactly as "Lengkapi klasifikasi" does: one
            # feedback row per following expense whose effective pillar changed, purpose questions kept.
            before = {}
            if pillar_may_change and final_kind == "expense" and current_category["reportable"]:
                before = snapshot_rows(cur, user_id, category_followers(cur, user_id, cid))
            set_clause = ", ".join([f"{column} = %s" for column in assignments] + ["updated_at = NOW()"])
            cur.execute(
                f"""
                UPDATE categories
                SET {set_clause}
                WHERE user_id = %s AND id = %s
                RETURNING id, name, icon, color, kind, monthly_budget, is_primary, kakeibo_type, is_archived, created_at
                """,
                [*assignments.values(), user_id, cid],
            )
            cat = cur.fetchone()
            if not cat:
                raise api_error(404, ErrorCode.CATEGORY_NOT_FOUND)
            corrections = record_owner_corrections(cur, user_id, before, close_questions=False)
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
            "kakeibo_type": cat.get("kakeibo_type") if cat["kind"] == "expense" else None,
            "is_archived": cat["is_archived"],
            "created_at": cat["created_at"].isoformat() if cat["created_at"] else None,
        },
        "corrections": corrections,
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
                    OR EXISTS (SELECT 1 FROM transactions WHERE offset_category_id = %s)
                    OR EXISTS (SELECT 1 FROM recurring_rules WHERE category_id = %s) AS is_referenced
                """,
                (cid, cid, cid),
            )
            row = cur.fetchone()
            if row and row["is_referenced"]:
                cur.execute(
                    "UPDATE categories SET is_archived = TRUE, updated_at = NOW() WHERE user_id = %s AND id = %s",
                    (user_id, cid),
                )
                conn.commit()
                return {"ok": True, "message": notice("category_archived")}

            cur.execute(
                "DELETE FROM categories WHERE user_id = %s AND id = %s RETURNING id",
                (user_id, cid),
            )
            if not cur.fetchone():
                raise api_error(404, ErrorCode.CATEGORY_NOT_FOUND)
            conn.commit()

    return {"ok": True, "message": notice("category_deleted")}
