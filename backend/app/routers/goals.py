import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.db.pool import db_conn
from app.routers.accounts import get_accounts_with_balances
from app.services.auth import get_current_user

router = APIRouter(tags=["Goals"])


class GoalCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    target_amount: int = Field(gt=0)
    current_amount: int = Field(default=0, ge=0)
    target_date: str | None = None  # YYYY-MM-DD
    color: str = Field(default="#10b981", max_length=30)
    icon: str = Field(default="target", max_length=50)
    is_emergency: bool = False
    account_ids: list[UUID] = Field(default_factory=list)


class GoalUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    target_amount: int | None = Field(default=None, gt=0)
    current_amount: int | None = Field(default=None, ge=0)
    target_date: str | None = None
    color: str | None = Field(default=None, max_length=30)
    icon: str | None = Field(default=None, max_length=50)
    is_emergency: bool | None = None
    is_archived: bool | None = None
    account_ids: list[UUID] | None = None


def get_goal_accounts_map(
    conn,
    user_id: str,
    goal_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    if not goal_ids:
        return {}

    all_accounts = get_accounts_with_balances(user_id, include_archived=True)
    accounts_by_id = {str(a["id"]): a for a in all_accounts}

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT goal_id, account_id
            FROM goal_accounts
            WHERE goal_id = ANY(%s)
            """,
            (goal_ids,),
        )
        rows = cur.fetchall()

    goal_map: dict[str, list[dict[str, Any]]] = {gid: [] for gid in goal_ids}
    for row in rows:
        gid = str(row["goal_id"])
        aid = str(row["account_id"])
        if aid in accounts_by_id:
            goal_map.setdefault(gid, []).append(accounts_by_id[aid])

    return goal_map


def format_goal_row(
    r: dict[str, Any],
    linked_accounts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    target = int(r["target_amount"])
    linked_list = linked_accounts or []
    linked_ids = {str(a["id"]) for a in linked_list}

    # An account contributes to the balance if its parent is NOT also linked.
    # If the parent is linked, the parent's balance already includes the child's balance.
    contributing_accounts = [
        a for a in linked_list
        if not a.get("parent_id") or str(a.get("parent_id")) not in linked_ids
    ]

    if linked_accounts:
        current = sum(int(a["balance"]) for a in contributing_accounts)
    else:
        current = int(r["current_amount"])

    pct = round((current / target) * 100, 1) if target > 0 else 0
    remaining = max(0, target - current)

    monthly_pace: int | None = None
    target_date_val = r["target_date"]
    if target_date_val:
        today = datetime.date.today()
        if isinstance(target_date_val, datetime.date):
            t_date = target_date_val
        else:
            t_date = datetime.date.fromisoformat(str(target_date_val))

        months_diff = (t_date.year - today.year) * 12 + (t_date.month - today.month)
        if months_diff > 0:
            monthly_pace = round(remaining / months_diff)
        else:
            monthly_pace = remaining

    accounts_payload = [
        {
            "id": str(a["id"]),
            "name": a["name"],
            "type": a["type"],
            "balance": int(a["balance"]),
            "parent_id": str(a["parent_id"]) if a.get("parent_id") else None,
        }
        for a in contributing_accounts
    ]

    return {
        "id": str(r["id"]),
        "name": r["name"],
        "target_amount": target,
        "current_amount": current,
        "remaining_amount": remaining,
        "percentage_completed": min(100.0, pct),
        "target_date": r["target_date"].isoformat() if r["target_date"] else None,
        "monthly_target_pace": monthly_pace,
        "color": r["color"],
        "icon": r["icon"],
        "is_emergency": bool(r.get("is_emergency", False)),
        "is_archived": r["is_archived"],
        "account_ids": [str(a["id"]) for a in (linked_accounts or [])],
        "linked_accounts": accounts_payload,
        "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
    }


@router.get("")
def list_goals(
    include_archived: bool = Query(default=False),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, target_amount, current_amount, target_date, color, icon, is_emergency, is_archived, created_at, updated_at
                FROM goals
                WHERE user_id = %s AND (is_archived = false OR %s = true)
                ORDER BY is_archived ASC, target_date ASC NULLS LAST, created_at ASC
                """,
                (user_id, include_archived),
            )
            rows = cur.fetchall()

        goal_ids = [str(r["id"]) for r in rows]
        goal_accounts_map = get_goal_accounts_map(conn, user_id, goal_ids)
        goals = [format_goal_row(r, goal_accounts_map.get(str(r["id"]), [])) for r in rows]

        total_target = sum(g["target_amount"] for g in goals if not g["is_archived"])
        total_current = sum(g["current_amount"] for g in goals if not g["is_archived"])

        return {
            "ok": True,
            "goals": goals,
            "summary": {
                "total_target": total_target,
                "total_saved": total_current,
                "total_remaining": max(0, total_target - total_current),
                "overall_percentage": round((total_current / total_target) * 100, 1) if total_target > 0 else 0,
                "active_goals_count": len([g for g in goals if not g["is_archived"]]),
            },
        }


@router.get("/{goal_id}")
def get_goal(goal_id: UUID, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, target_amount, current_amount, target_date, color, icon, is_emergency, is_archived, created_at, updated_at
                FROM goals
                WHERE id = %s AND user_id = %s
                """,
                (str(goal_id), user_id),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Goal not found")

        goal_accounts_map = get_goal_accounts_map(conn, user_id, [str(goal_id)])
        linked_accounts = goal_accounts_map.get(str(goal_id), [])
        return {"ok": True, "goal": format_goal_row(row, linked_accounts)}


@router.post("")
def create_goal(payload: GoalCreate, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    name = payload.name.strip()
    t_date = None
    if payload.target_date:
        try:
            t_date = datetime.date.fromisoformat(payload.target_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid target_date format, expected YYYY-MM-DD")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM goals WHERE user_id = %s AND name = %s",
                (user_id, name),
            )
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="Goal with this name already exists")

            valid_account_ids: list[str] = []
            if payload.account_ids:
                cur.execute(
                    "SELECT id FROM accounts WHERE user_id = %s AND id = ANY(%s)",
                    (user_id, [str(aid) for aid in payload.account_ids]),
                )
                valid_account_ids = [str(r["id"]) for r in cur.fetchall()]

            # If is_emergency is False but name contains "darurat" or "emergency", default to True
            is_emergency = payload.is_emergency
            if not is_emergency and any(k in name.lower() for k in ["darurat", "emergency"]):
                is_emergency = True

            cur.execute(
                """
                INSERT INTO goals (user_id, name, target_amount, current_amount, target_date, color, icon, is_emergency)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, name, target_amount, current_amount, target_date, color, icon, is_emergency, is_archived, created_at, updated_at
                """,
                (user_id, name, payload.target_amount, payload.current_amount, t_date, payload.color, payload.icon, is_emergency),
            )
            row = cur.fetchone()
            goal_id = str(row["id"])

            for acc_id in valid_account_ids:
                cur.execute(
                    """
                    INSERT INTO goal_accounts (goal_id, account_id)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (goal_id, acc_id),
                )
            conn.commit()

        goal_accounts_map = get_goal_accounts_map(conn, user_id, [goal_id])
        linked_accounts = goal_accounts_map.get(goal_id, [])
        return {"ok": True, "goal": format_goal_row(row, linked_accounts)}


@router.put("/{goal_id}")
@router.patch("/{goal_id}")
def update_goal(
    goal_id: UUID,
    payload: GoalUpdate,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    updates = []
    params = []

    if payload.name is not None:
        name = payload.name.strip()
        updates.append("name = %s")
        params.append(name)

    if payload.target_amount is not None:
        updates.append("target_amount = %s")
        params.append(payload.target_amount)

    if payload.current_amount is not None:
        updates.append("current_amount = %s")
        params.append(payload.current_amount)

    if payload.target_date is not None:
        if payload.target_date == "":
            updates.append("target_date = NULL")
        else:
            try:
                t_date = datetime.date.fromisoformat(payload.target_date)
                updates.append("target_date = %s")
                params.append(t_date)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid target_date format")

    if payload.color is not None:
        updates.append("color = %s")
        params.append(payload.color)

    if payload.icon is not None:
        updates.append("icon = %s")
        params.append(payload.icon)

    if payload.is_emergency is not None:
        updates.append("is_emergency = %s")
        params.append(payload.is_emergency)

    if payload.is_archived is not None:
        updates.append("is_archived = %s")
        params.append(payload.is_archived)

    if not updates and payload.account_ids is None:
        raise HTTPException(status_code=400, detail="No fields to update")

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM goals WHERE id = %s AND user_id = %s",
                (str(goal_id), user_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Goal not found")

            if updates:
                updates.append("updated_at = NOW()")
                cur.execute(
                    f"""
                    UPDATE goals
                    SET {", ".join(updates)}
                    WHERE id = %s AND user_id = %s
                    RETURNING id, name, target_amount, current_amount, target_date, color, icon, is_emergency, is_archived, created_at, updated_at
                    """,
                    (*params, str(goal_id), user_id),
                )
                row = cur.fetchone()
            else:
                cur.execute(
                    """
                    SELECT id, name, target_amount, current_amount, target_date, color, icon, is_emergency, is_archived, created_at, updated_at
                    FROM goals
                    WHERE id = %s AND user_id = %s
                    """,
                    (str(goal_id), user_id),
                )
                row = cur.fetchone()

            if payload.account_ids is not None:
                cur.execute(
                    "DELETE FROM goal_accounts WHERE goal_id = %s",
                    (str(goal_id),),
                )
                if payload.account_ids:
                    cur.execute(
                        "SELECT id FROM accounts WHERE user_id = %s AND id = ANY(%s)",
                        (user_id, [str(aid) for aid in payload.account_ids]),
                    )
                    valid_account_ids = [str(r["id"]) for r in cur.fetchall()]
                    for acc_id in valid_account_ids:
                        cur.execute(
                            """
                            INSERT INTO goal_accounts (goal_id, account_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING
                            """,
                            (str(goal_id), acc_id),
                        )
            conn.commit()

        goal_accounts_map = get_goal_accounts_map(conn, user_id, [str(goal_id)])
        linked_accounts = goal_accounts_map.get(str(goal_id), [])
        return {"ok": True, "goal": format_goal_row(row, linked_accounts)}


@router.delete("/{goal_id}")
def delete_goal(
    goal_id: UUID,
    hard_delete: bool = Query(default=False),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            if hard_delete:
                cur.execute(
                    "DELETE FROM goals WHERE id = %s AND user_id = %s RETURNING id",
                    (str(goal_id), user_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE goals
                    SET is_archived = true, updated_at = NOW()
                    WHERE id = %s AND user_id = %s
                    RETURNING id
                    """,
                    (str(goal_id), user_id),
                )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Goal not found")
            conn.commit()
            return {"ok": True, "deleted": True}
