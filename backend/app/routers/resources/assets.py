"""Assets CRUD + holdings + snapshots + net worth — /assets and /v1/assets"""
from datetime import date, timezone
from typing import Any
from fastapi import APIRouter, HTTPException, Request
from psycopg.errors import UniqueViolation
from app.db.pool import db_conn
from app.services.ledger.balances import parse_uuid_value, get_account_balances
from app.services.ledger.period import now_utc

router = APIRouter(tags=["assets"])

ASSET_CLASSES = ("stock", "etf", "mutual_fund", "bond", "crypto", "metal", "property", "other")


# ── Assets ──────────────────────────────────────────────────────────────────

@router.get("")
def list_assets(req: Request):
    username = req.state.username
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.asset_id::text AS asset_id, a.name, a.class, a.currency,
                   a.ticker, a.is_active, a.notes,
                   s.unit_price AS latest_price, s.as_of_date AS price_date,
                   COALESCE(SUM(h.quantity), 0) AS total_quantity,
                   COALESCE(SUM(h.cost_basis), 0) AS total_cost_basis
            FROM assets a
            JOIN users u ON u.user_id = a.user_id
            LEFT JOIN asset_holdings h ON h.asset_id = a.asset_id
            LEFT JOIN LATERAL (
                SELECT unit_price, as_of_date FROM asset_snapshots
                WHERE asset_id = a.asset_id ORDER BY as_of_date DESC LIMIT 1
            ) s ON TRUE
            WHERE u.username = %s AND a.is_active = TRUE
            GROUP BY a.asset_id, a.name, a.class, a.currency, a.ticker, a.is_active, a.notes, s.unit_price, s.as_of_date
            ORDER BY a.name
            """,
            (username,),
        )
        rows = cur.fetchall()
    result = []
    for r in rows:
        qty = float(r["total_quantity"] or 0)
        price = int(r["latest_price"] or 0)
        current_value = int(qty * price)
        cost_basis = int(r["total_cost_basis"] or 0)
        result.append({
            **r,
            "total_quantity": qty,
            "current_value": current_value,
            "unrealized_gain": current_value - cost_basis,
        })
    return {"assets": result}


@router.post("")
async def create_asset(req: Request):
    username = req.state.username
    data: dict[str, Any] = await req.json()
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    asset_class = str(data.get("class") or "other").strip().lower()
    if asset_class not in ASSET_CLASSES:
        raise HTTPException(status_code=400, detail="Invalid class")
    currency = str(data.get("currency") or "IDR").upper()
    ticker = (data.get("ticker") or "").strip() or None
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO assets (user_id, name, class, currency, ticker, notes)
                SELECT user_id, %s, %s, %s, %s, %s FROM users WHERE username=%s
                RETURNING asset_id::text AS asset_id
                """,
                (name, asset_class, currency, ticker, notes, username),
            )
            row = cur.fetchone()
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Asset name already exists")
    return {"ok": True, "asset_id": row["asset_id"]}


@router.put("/{asset_id}")
async def update_asset(asset_id: str, req: Request):
    username = req.state.username
    asset_id = parse_uuid_value(asset_id, "asset_id")
    data: dict[str, Any] = await req.json()
    name = (data.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    asset_class = str(data.get("class") or "other").strip().lower()
    if asset_class not in ASSET_CLASSES:
        raise HTTPException(status_code=400, detail="Invalid class")
    ticker = (data.get("ticker") or "").strip() or None
    is_active = bool(data.get("is_active", True))
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        try:
            cur.execute(
                """
                UPDATE assets SET name=%s, class=%s, ticker=%s, is_active=%s, notes=%s
                WHERE asset_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s)
                RETURNING asset_id
                """,
                (name, asset_class, ticker, is_active, notes, asset_id, username),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Asset not found")
            conn.commit()
        except UniqueViolation:
            conn.rollback()
            raise HTTPException(status_code=400, detail="Asset name already exists")
    return {"ok": True}


@router.delete("/{asset_id}")
def delete_asset(asset_id: str, req: Request):
    username = req.state.username
    asset_id = parse_uuid_value(asset_id, "asset_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE assets SET is_active=FALSE WHERE asset_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s) RETURNING asset_id",
            (asset_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Asset not found")
        conn.commit()
    return {"ok": True}


# ── Holdings ─────────────────────────────────────────────────────────────────

@router.get("/{asset_id}/holdings")
def list_holdings(asset_id: str, req: Request):
    username = req.state.username
    asset_id = parse_uuid_value(asset_id, "asset_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT h.holding_id::text AS holding_id, h.quantity, h.cost_basis,
                   h.acquired_at, h.notes, h.account_id::text AS account_id,
                   a.account_name
            FROM asset_holdings h
            JOIN assets ast ON ast.asset_id = h.asset_id
            JOIN users u ON u.user_id = h.user_id
            LEFT JOIN accounts a ON a.account_id = h.account_id
            WHERE h.asset_id=%s::uuid AND u.username=%s
            ORDER BY h.acquired_at DESC
            """,
            (asset_id, username),
        )
        return {"holdings": cur.fetchall()}


@router.post("/{asset_id}/holdings")
async def add_holding(asset_id: str, req: Request):
    username = req.state.username
    asset_id = parse_uuid_value(asset_id, "asset_id")
    data: dict[str, Any] = await req.json()
    quantity = float(data.get("quantity") or 0)
    if quantity <= 0:
        raise HTTPException(status_code=400, detail="quantity must be > 0")
    cost_basis = int(data.get("cost_basis") or 0)
    acquired_at = data.get("acquired_at") or date.today().isoformat()
    account_id = data.get("account_id") or None
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM assets a JOIN users u ON u.user_id=a.user_id WHERE a.asset_id=%s::uuid AND u.username=%s",
            (asset_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Asset not found")
        cur.execute(
            """
            INSERT INTO asset_holdings (user_id, asset_id, quantity, cost_basis, acquired_at, account_id, notes)
            SELECT user_id, %s::uuid, %s, %s, %s, %s::uuid, %s FROM users WHERE username=%s
            RETURNING holding_id::text AS holding_id
            """,
            (asset_id, quantity, cost_basis, acquired_at, account_id, notes, username),
        )
        row = cur.fetchone()
        conn.commit()
    return {"ok": True, "holding_id": row["holding_id"]}


@router.delete("/{asset_id}/holdings/{holding_id}")
def delete_holding(asset_id: str, holding_id: str, req: Request):
    username = req.state.username
    asset_id = parse_uuid_value(asset_id, "asset_id")
    holding_id = parse_uuid_value(holding_id, "holding_id")
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM asset_holdings WHERE holding_id=%s::uuid AND asset_id=%s::uuid AND user_id=(SELECT user_id FROM users WHERE username=%s) RETURNING holding_id",
            (holding_id, asset_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Holding not found")
        conn.commit()
    return {"ok": True}


# ── Snapshots ────────────────────────────────────────────────────────────────

@router.post("/{asset_id}/snapshots")
async def add_snapshot(asset_id: str, req: Request):
    username = req.state.username
    asset_id = parse_uuid_value(asset_id, "asset_id")
    data: dict[str, Any] = await req.json()
    unit_price = int(data.get("unit_price") or 0)
    if unit_price < 0:
        raise HTTPException(status_code=400, detail="unit_price must be >= 0")
    as_of_date = data.get("as_of_date") or date.today().isoformat()

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM assets a JOIN users u ON u.user_id=a.user_id WHERE a.asset_id=%s::uuid AND u.username=%s",
            (asset_id, username),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Asset not found")
        cur.execute(
            """
            INSERT INTO asset_snapshots (asset_id, as_of_date, unit_price)
            VALUES (%s::uuid, %s, %s)
            ON CONFLICT (asset_id, as_of_date) DO UPDATE SET unit_price=EXCLUDED.unit_price
            RETURNING snapshot_id::text AS snapshot_id
            """,
            (asset_id, as_of_date, unit_price),
        )
        row = cur.fetchone()
        conn.commit()
    return {"ok": True, "snapshot_id": row["snapshot_id"]}


# ── Net Worth ────────────────────────────────────────────────────────────────

@router.get("/net-worth")
def get_net_worth(req: Request):
    username = req.state.username
    now = now_utc()
    with db_conn() as conn, conn.cursor() as cur:
        # Liquid: sum of all account balances
        balances = get_account_balances(cur, username, now)
        liquid = sum(balances.values())

        # Invested: sum of (quantity * latest_price) per holding
        cur.execute(
            """
            SELECT h.quantity,
                   COALESCE(s.unit_price, 0) AS unit_price,
                   h.cost_basis
            FROM asset_holdings h
            JOIN assets a ON a.asset_id = h.asset_id
            JOIN users u ON u.user_id = h.user_id
            LEFT JOIN LATERAL (
                SELECT unit_price FROM asset_snapshots
                WHERE asset_id = h.asset_id ORDER BY as_of_date DESC LIMIT 1
            ) s ON TRUE
            WHERE u.username = %s AND a.is_active = TRUE
            """,
            (username,),
        )
        holdings = cur.fetchall()
        invested = sum(int(float(h["quantity"]) * int(h["unit_price"])) for h in holdings)
        total_cost = sum(int(h["cost_basis"]) for h in holdings)
        unrealized_gain = invested - total_cost

        # History (last 90 days)
        cur.execute(
            """
            SELECT as_of_date, liquid_assets, invested_assets, net_worth
            FROM net_worth_snapshots
            WHERE user_id=(SELECT user_id FROM users WHERE username=%s)
            ORDER BY as_of_date DESC LIMIT 90
            """,
            (username,),
        )
        history = cur.fetchall()

    net_worth = liquid + invested
    return {
        "as_of": now.date().isoformat(),
        "liquid_assets": liquid,
        "invested_assets": invested,
        "total_cost_basis": total_cost,
        "unrealized_gain": unrealized_gain,
        "net_worth": net_worth,
        "history": history,
    }


@router.post("/net-worth/snapshot")
def record_net_worth_snapshot(req: Request):
    """Compute and store today's net worth snapshot. Call daily (or manually)."""
    username = req.state.username
    now = now_utc()
    today = now.date()

    with db_conn() as conn, conn.cursor() as cur:
        balances = get_account_balances(cur, username, now)
        liquid = sum(balances.values())

        cur.execute(
            """
            SELECT h.quantity, COALESCE(s.unit_price, 0) AS unit_price
            FROM asset_holdings h
            JOIN assets a ON a.asset_id = h.asset_id
            JOIN users u ON u.user_id = h.user_id
            LEFT JOIN LATERAL (
                SELECT unit_price FROM asset_snapshots WHERE asset_id=h.asset_id ORDER BY as_of_date DESC LIMIT 1
            ) s ON TRUE
            WHERE u.username=%s AND a.is_active=TRUE
            """,
            (username,),
        )
        invested = sum(int(float(h["quantity"]) * int(h["unit_price"])) for h in cur.fetchall())
        net_worth = liquid + invested

        cur.execute(
            """
            INSERT INTO net_worth_snapshots (user_id, as_of_date, liquid_assets, invested_assets, net_worth)
            SELECT user_id, %s, %s, %s, %s FROM users WHERE username=%s
            ON CONFLICT (user_id, as_of_date) DO UPDATE
              SET liquid_assets=EXCLUDED.liquid_assets, invested_assets=EXCLUDED.invested_assets, net_worth=EXCLUDED.net_worth
            RETURNING snapshot_id::text AS snapshot_id
            """,
            (today, liquid, invested, net_worth, username),
        )
        row = cur.fetchone()
        conn.commit()
    return {"ok": True, "snapshot_id": row["snapshot_id"], "net_worth": net_worth}
