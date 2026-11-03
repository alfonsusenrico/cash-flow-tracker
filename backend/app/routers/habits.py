"""GET /api/habits: the owner's learned habits and top-up patterns, read only (redesign-money-and-settings D7)."""

from fastapi import APIRouter, Depends

from app.db.pool import db_conn
from app.services.auth import get_current_user
from app.services.habit_list import habit_list

router = APIRouter()


@router.get("")
def get_habits(current_user: dict = Depends(get_current_user)):
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            listing = habit_list(cur, current_user["id"])
    return {"ok": True, **listing}
