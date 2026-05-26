"""Buckets CRUD — /buckets and /v1/buckets"""
from typing import Any
from fastapi import APIRouter, HTTPException, Request
from psycopg.errors import UniqueViolation
from app.db.pool import db_conn
from app.services.ledger.balances import get_account_balances, parse_uuid_value
from app.services.ledger.period import now_utc

router = APIRouter(tags=["buckets"])
GOAL_BACKED_BUCKET_KINDS = {"sinking", "emergency", "goal", "investment"}


def _parse_target_amount(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    amount = int(raw)
    if amount < 0:
        raise HTTPException(status_code=400, detail="target_amount must be >= 0")
    return amount


def _parse_bool(raw: Any, default: bool = False) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _should_sync_linked_goal(data: dict[str, Any], kind: str, target_amount: int | None) -> bool:
    if kind not in GOAL_BACKED_BUCKET_KINDS or not target_amount:
        return False
    return _parse_bool(data.get("create_linked_goal"), default=True)


def _parse_account_ids(data: dict[str, Any]) -> list[str]:
    raw_ids = data.get("linked_account_ids")
    if raw_ids is None:
        raw_ids = [data.get("linked_account_id")] if data.get("linked_account_id") else []
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="linked_account_ids must be a list")

    account_ids: list[str] = []
    seen: set[str] = set()
    for raw in raw_ids:
        if not raw:
            continue
        account_id = parse_uuid_value(raw, "account_id")
        if account_id not in seen:
            seen.add(account_id)
            account_ids.append(account_id)
    return account_ids


def _ensure_accounts(cur, username: str, account_ids: list[str]) -> None:
    if not account_ids:
        return
    cur.execute(
        """
        SELECT account_id::text AS account_id
        FROM accounts
        WHERE username=%s AND account_id = ANY(%s::uuid[])
        """,
        (username, account_ids),
    )
    found = {r["account_id"] for r in cur.fetchall()}
    if found != set(account_ids):
        raise HTTPException(status_code=404, detail="Linked account not found")


def _replace_bucket_accounts(cur, bucket_id: str, account_ids: list[str]) -> None:
    cur.execute("DELETE FROM bucket_accounts WHERE bucket_id=%s::uuid", (bucket_id,))
    if account_ids:
        cur.executemany(
            "INSERT INTO bucket_accounts (bucket_id, account_id) VALUES (%s::uuid, %s::uuid)",
            [(bucket_id, account_id) for account_id in account_ids],
        )


def _find_archived_bucket_by_name(cur, username: str, name: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT b.bucket_id::text AS bucket_id, b.name
        FROM buckets b
        JOIN users u ON u.user_id = b.user_id
        WHERE u.username=%s
          AND lower(b.name)=lower(%s)
          AND b.is_archived=TRUE
        ORDER BY b.created_at DESC
        LIMIT 1
        """,
        (username, name),
    )
    return cur.fetchone()


def _sync_linked_goal_for_bucket(
    cur,
    username: str,
    *,
    bucket_id: str,
    name: str,
    target_amount: int,
    priority: int,
    notes: str | None,
    previous_name: str | None = None,
) -> tuple[str, bool]:
    cur.execute(
        """
        SELECT g.goal_id::text AS goal_id, g.name
        FROM financial_goals g
        JOIN users u ON u.user_id = g.user_id
        WHERE u.username=%s
          AND g.linked_bucket_id=%s::uuid
          AND g.status IN ('active', 'paused')
        ORDER BY g.created_at ASC
        LIMIT 1
        """,
        (username, bucket_id),
    )
    existing = cur.fetchone()
    if existing:
        next_name = name if previous_name is None or existing["name"] == previous_name else existing["name"]
        cur.execute(
            """
            UPDATE financial_goals
            SET name=%s,
                target_amount=%s,
                priority=%s,
                notes=COALESCE(notes, %s),
                updated_at=now()
            WHERE goal_id=%s::uuid
            RETURNING goal_id::text AS goal_id
            """,
            (next_name, target_amount, priority, notes, existing["goal_id"]),
        )
        return cur.fetchone()["goal_id"], False

    cur.execute(
        """
        INSERT INTO financial_goals (user_id, name, target_amount, linked_bucket_id, priority, notes)
        SELECT user_id, %s, %s, %s::uuid, %s, %s
        FROM users
        WHERE username=%s
        RETURNING goal_id::text AS goal_id
        """,
        (name, target_amount, bucket_id, priority, notes, username),
    )
    return cur.fetchone()["goal_id"], True


@router.get("")
def list_buckets(req: Request, include_archived: bool = False):
    username = req.state.username
    with db_conn() as conn, conn.cursor() as cur:
        balances = get_account_balances(cur, username, now_utc())
        cur.execute(
            """
            SELECT b.bucket_id::text AS bucket_id, b.name, b.kind, b.target_amount,
                   b.linked_account_id::text AS linked_account_id,
                   b.priority, b.is_archived, b.notes, b.created_at,
                   COALESCE(
                     array_agg(a.account_id::text ORDER BY a.account_name)
                       FILTER (WHERE a.account_id IS NOT NULL),
                     ARRAY[]::text[]
                   ) AS linked_account_ids,
                   COALESCE(
                     array_agg(a.account_name ORDER BY a.account_name)
                       FILTER (WHERE a.account_id IS NOT NULL),
                     ARRAY[]::text[]
                   ) AS linked_account_names
            FROM buckets b
            JOIN users u ON u.user_id = b.user_id
            LEFT JOIN LATERAL (
                SELECT account_id FROM bucket_accounts WHERE bucket_id = b.bucket_id
                UNION
                SELECT b.linked_account_id WHERE b.linked_account_id IS NOT NULL
            ) ba ON TRUE
            LEFT JOIN accounts a ON a.account_id = ba.account_id AND a.username = u.username
            WHERE u.username = %s AND (%s OR b.is_archived = FALSE)
            GROUP BY b.bucket_id
            ORDER BY b.priority ASC, b.name ASC
            """,
            (username, include_archived),
        )
        bucket_rows = cur.fetchall()
        cur.execute(
            """
            SELECT g.linked_bucket_id::text AS bucket_id,
                   array_agg(g.name ORDER BY g.priority, g.name) AS goal_names
            FROM financial_goals g
            JOIN users u ON u.user_id = g.user_id
            WHERE u.username=%s
              AND g.status IN ('active', 'paused')
              AND g.linked_bucket_id IS NOT NULL
            GROUP BY g.linked_bucket_id
            """,
            (username,),
        )
        linked_goals = {
            row["bucket_id"]: list(row.get("goal_names") or [])
            for row in cur.fetchall()
        }
        buckets = []
        for row in bucket_rows:
            account_ids = list(row.get("linked_account_ids") or [])
            account_names = list(row.get("linked_account_names") or [])
            legacy_id = row.get("linked_account_id")
            linked_accounts = []
            for account_id, account_name in zip(account_ids, account_names, strict=False):
                if not account_name:
                    continue
                linked_accounts.append({
                    "account_id": account_id,
                    "account_name": account_name,
                    "balance": int(balances.get(account_id, 0)),
                })
            row["linked_account_ids"] = [a["account_id"] for a in linked_accounts]
            row["linked_accounts"] = linked_accounts
            row["linked_account_id"] = row["linked_account_ids"][0] if row["linked_account_ids"] else legacy_id
            row["current_amount"] = sum(int(a["balance"]) for a in linked_accounts)
            row["linked_goals"] = linked_goals.get(row["bucket_id"], [])
            buckets.append(row)
        return {"buckets": buckets}


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
    target_amount = _parse_target_amount(data.get("target_amount"))
    linked_account_ids = _parse_account_ids(data)
    linked_account_id = linked_account_ids[0] if linked_account_ids else None
    priority = int(data.get("priority") or 50)
    notes = (data.get("notes") or "").strip() or None
    sync_goal = _should_sync_linked_goal(data, kind, target_amount)

    with db_conn() as conn, conn.cursor() as cur:
        try:
            _ensure_accounts(cur, username, linked_account_ids)
            archived = _find_archived_bucket_by_name(cur, username, name)
            if archived:
                cur.execute(
                    """
                    UPDATE buckets
                    SET name=%s,
                        kind=%s,
                        target_amount=%s,
                        linked_account_id=%s::uuid,
                        priority=%s,
                        notes=%s,
                        is_archived=FALSE
                    WHERE bucket_id=%s::uuid
                      AND user_id=(SELECT user_id FROM users WHERE username=%s)
                    RETURNING bucket_id::text AS bucket_id
                    """,
                    (name, kind, target_amount, linked_account_id, priority, notes, archived["bucket_id"], username),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO buckets (user_id, name, kind, target_amount, linked_account_id, priority, notes)
                    SELECT user_id, %s, %s, %s, %s::uuid, %s, %s FROM users WHERE username=%s
                    RETURNING bucket_id::text AS bucket_id
                    """,
                    (name, kind, target_amount, linked_account_id, priority, notes, username),
                )
            row = cur.fetchone()
            _replace_bucket_accounts(cur, row["bucket_id"], linked_account_ids)
            linked_goal_id = None
            linked_goal_created = False
            if sync_goal and target_amount:
                linked_goal_id, linked_goal_created = _sync_linked_goal_for_bucket(
                    cur,
                    username,
                    bucket_id=row["bucket_id"],
                    name=name,
                    target_amount=target_amount,
                    priority=priority,
                    notes=notes,
                )
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Bucket name already exists")
    return {
        "ok": True,
        "bucket_id": row["bucket_id"],
        "linked_goal_id": linked_goal_id,
        "linked_goal_created": linked_goal_created,
    }


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
    target_amount = _parse_target_amount(data.get("target_amount"))
    linked_account_ids = _parse_account_ids(data)
    linked_account_id = linked_account_ids[0] if linked_account_ids else None
    priority = int(data.get("priority") or 50)
    notes = (data.get("notes") or "").strip() or None
    is_archived = bool(data.get("is_archived", False))
    sync_goal = _should_sync_linked_goal(data, kind, target_amount)

    with db_conn() as conn, conn.cursor() as cur:
        try:
            _ensure_accounts(cur, username, linked_account_ids)
            cur.execute(
                """
                SELECT name
                FROM buckets
                WHERE bucket_id=%s::uuid
                  AND user_id=(SELECT user_id FROM users WHERE username=%s)
                """,
                (bucket_id, username),
            )
            previous = cur.fetchone()
            if not previous:
                raise HTTPException(status_code=404, detail="Bucket not found")
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
            _replace_bucket_accounts(cur, bucket_id, linked_account_ids)
            linked_goal_id = None
            linked_goal_created = False
            if sync_goal and target_amount and not is_archived:
                linked_goal_id, linked_goal_created = _sync_linked_goal_for_bucket(
                    cur,
                    username,
                    bucket_id=bucket_id,
                    name=name,
                    target_amount=target_amount,
                    priority=priority,
                    notes=notes,
                    previous_name=previous["name"],
                )
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Bucket name already exists")
    return {"ok": True, "linked_goal_id": linked_goal_id, "linked_goal_created": linked_goal_created}


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
