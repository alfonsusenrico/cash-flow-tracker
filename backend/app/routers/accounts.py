from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.db.pool import db_conn
from app.services.auth import get_current_user
from app.services.market_data import (
    get_instrument_quote,
    search_instruments,
    sync_all_tracked_prices,
)

router = APIRouter(tags=["Accounts"])


class AccountCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    type: str = Field(default="bank", pattern="^(cash|bank|wallet|investment)$")
    initial_balance: int = Field(default=0, ge=0)
    parent_id: UUID | None = None
    default_funding_account_id: UUID | None = None
    default_pocket_id: UUID | None = None
    instrument_type: str | None = Field(default=None, pattern="^(stock|mutual_fund|gold|crypto|deposit|other)$")
    instrument_symbol: str | None = Field(default=None, max_length=30)
    units: float | None = Field(default=None, ge=0)
    avg_buy_price: float | None = Field(default=None, ge=0)
    color: str | None = Field(default="#3b82f6", max_length=30)
    display_order: int | None = Field(default=0)


class AccountUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    type: str | None = Field(default=None, pattern="^(cash|bank|wallet|investment)$")
    is_archived: bool | None = None
    parent_id: UUID | None = None
    default_funding_account_id: UUID | None = None
    default_pocket_id: UUID | None = None
    instrument_type: str | None = Field(default=None, pattern="^(stock|mutual_fund|gold|crypto|deposit|other)$")
    instrument_symbol: str | None = Field(default=None, max_length=30)
    units: float | None = Field(default=None, ge=0)
    avg_buy_price: float | None = Field(default=None, ge=0)
    last_price: float | None = Field(default=None, ge=0)
    color: str | None = Field(default=None, max_length=30)
    display_order: int | None = Field(default=None)


class AccountReorder(BaseModel):
    account_ids: list[UUID]


class AccountReconcile(BaseModel):
    actual_balance: int = Field(ge=0)
    notes: str | None = Field(default=None, max_length=500)


class AccountValuationUpdate(BaseModel):
    current_balance: int = Field(ge=0)
    cost_basis: int | None = Field(default=None, ge=0)
    notes: str | None = Field(default=None, max_length=500)


def get_accounts_with_balances(user_id: str, include_archived: bool = False) -> list[dict[str, Any]]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    a.id, 
                    a.parent_id,
                    a.default_funding_account_id,
                    funding_acc.name AS default_funding_account_name,
                    a.default_pocket_id,
                    default_pocket.name AS default_pocket_name,
                    a.name, 
                    a.type, 
                    a.initial_balance, 
                    a.instrument_type,
                    a.instrument_symbol,
                    a.units,
                    a.avg_buy_price,
                    a.last_price,
                    a.last_price_at,
                    a.color,
                    a.display_order,
                    a.is_archived,
                    a.created_at,
                    (
                        a.initial_balance 
                        + COALESCE(SUM(CASE WHEN t.account_id = a.id AND t.type = 'income' THEN t.amount ELSE 0 END), 0)
                        - COALESCE(SUM(CASE WHEN t.account_id = a.id AND t.type = 'expense' THEN t.amount ELSE 0 END), 0)
                    ) AS ledger_balance
                FROM accounts a
                LEFT JOIN accounts funding_acc ON funding_acc.id = a.default_funding_account_id
                LEFT JOIN accounts default_pocket ON default_pocket.id = a.default_pocket_id
                LEFT JOIN transactions t ON t.account_id = a.id
                WHERE a.user_id = %s AND (a.is_archived = false OR %s = true)
                GROUP BY 
                    a.id, a.parent_id, a.default_funding_account_id, funding_acc.name,
                    a.default_pocket_id, default_pocket.name,
                    a.name, a.type, a.initial_balance,
                    a.instrument_type, a.instrument_symbol, a.units, a.avg_buy_price,
                    a.last_price, a.last_price_at, a.color, a.display_order, a.is_archived, a.created_at
                ORDER BY a.display_order ASC, a.created_at ASC, a.name ASC
                """,
                (user_id, include_archived),
            )
            rows = cur.fetchall()

    raw_accounts = []
    for r in rows:
        units = float(r["units"]) if r.get("units") is not None else None
        avg_buy_price = float(r["avg_buy_price"]) if r.get("avg_buy_price") is not None else None
        last_price = float(r["last_price"]) if r.get("last_price") is not None else None
        last_price_at = r["last_price_at"].isoformat() if r.get("last_price_at") else None
        ledger_balance = int(r.get("ledger_balance", r.get("balance", 0)))

        # Capital Gain & Balance calculation
        capital_gain = None
        capital_gain_pct = None
        balance = ledger_balance

        # 1. Total Cost Basis calculation
        total_cost_basis = None
        if units is not None and units > 0 and avg_buy_price is not None and avg_buy_price > 0:
            total_cost_basis = int(round(units * avg_buy_price))
        elif r.get("initial_balance", 0) > 0:
            total_cost_basis = int(r["initial_balance"])
        elif avg_buy_price is not None and avg_buy_price > 0:
            total_cost_basis = avg_buy_price

        # 2. Market value & Capital Gain calculation
        if units is not None and units > 0 and last_price is not None and last_price > 0:
            market_val = int(round(units * last_price))
            balance = market_val
            if total_cost_basis is not None and total_cost_basis > 0:
                capital_gain = market_val - total_cost_basis
                capital_gain_pct = round(((market_val - total_cost_basis) / total_cost_basis) * 100, 2)
        elif r.get("type") == "investment" or r.get("instrument_type") is not None:
            # Mode B: manual / untracked valuation
            if last_price is not None and last_price > 0:
                balance = int(last_price)
            if total_cost_basis is not None and total_cost_basis > 0 and balance != total_cost_basis:
                capital_gain = balance - total_cost_basis
                capital_gain_pct = round(((balance - total_cost_basis) / total_cost_basis) * 100, 2)

        raw_accounts.append({
            "id": str(r["id"]),
            "account_id": str(r["id"]),
            "parent_id": str(r["parent_id"]) if r.get("parent_id") else None,
            "default_funding_account_id": str(r["default_funding_account_id"]) if r.get("default_funding_account_id") else None,
            "default_funding_account_name": r.get("default_funding_account_name"),
            "default_pocket_id": str(r["default_pocket_id"]) if r.get("default_pocket_id") else None,
            "default_pocket_name": r.get("default_pocket_name"),
            "name": r.get("name"),
            "account_name": r.get("name"),
            "type": r.get("type"),
            "initial_balance": r.get("initial_balance", 0),
            "balance": balance,
            "current_balance": balance,
            "instrument_type": r.get("instrument_type"),
            "instrument_symbol": r.get("instrument_symbol"),
            "units": units,
            "avg_buy_price": avg_buy_price,
            "last_price": last_price,
            "last_price_at": last_price_at,
            "capital_gain": capital_gain,
            "capital_gain_pct": capital_gain_pct,
            "cost_basis": total_cost_basis,
            "color": r.get("color") or "#3b82f6",
            "display_order": int(r.get("display_order") or 0),
            "is_archived": r.get("is_archived", False),
            "is_parent": False,
            "children": [],
            "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
        })

    accounts_by_id = {a["id"]: a for a in raw_accounts}

    # Attach children to parents
    for a in raw_accounts:
        pid = a["parent_id"]
        if pid and pid in accounts_by_id:
            parent = accounts_by_id[pid]
            parent["children"].append(a)
            parent["is_parent"] = True

    # Aggregate child balances and child capital gains into parent
    for a in raw_accounts:
        if a["is_parent"]:
            child_sum = sum(c["balance"] for c in a["children"])
            a["balance"] = a["balance"] + child_sum
            a["current_balance"] = a["balance"]

            # Aggregate capital gains if children have them
            child_gains = [c["capital_gain"] for c in a["children"] if c.get("capital_gain") is not None]
            if child_gains:
                parent_gain = sum(child_gains)
                a["capital_gain"] = parent_gain
                total_child_cost = sum(
                    c["balance"] - c["capital_gain"]
                    for c in a["children"]
                    if c.get("capital_gain") is not None and (c["balance"] - c["capital_gain"]) > 0
                )
                if total_child_cost > 0:
                    a["capital_gain_pct"] = round((parent_gain / total_child_cost) * 100, 2)

    return raw_accounts


@router.get("/instruments/search")
async def search_account_instruments(
    q: str = "",
    limit: int = 10,
    _: dict = Depends(get_current_user),
):
    results = await search_instruments(query=q, limit=limit)
    return {"ok": True, "results": results}


@router.get("/instruments/quote")
async def get_instrument_quote_endpoint(
    symbol: str,
    _: dict = Depends(get_current_user),
):
    quote = await get_instrument_quote(symbol)
    if not quote:
        raise HTTPException(status_code=404, detail=f"No quote found for symbol {symbol}")
    return {"ok": True, "quote": quote}


@router.post("/sync-prices")
async def sync_prices_endpoint(_: dict = Depends(get_current_user)):
    with db_conn() as conn:
        result = await sync_all_tracked_prices(conn)
    return {"ok": True, **result}


@router.get("")
def list_accounts(include_archived: bool = False, current_user: dict = Depends(get_current_user)):
    accounts = get_accounts_with_balances(current_user["id"], include_archived)
    # Total balance sums only top-level accounts (parent_id IS NULL) to prevent duplicate counting
    total_balance = sum(a["balance"] for a in accounts if not a["is_archived"] and a["parent_id"] is None)
    return {
        "ok": True,
        "accounts": accounts,
        "total_balance": total_balance,
    }


@router.post("/list")
def list_accounts_post(current_user: dict = Depends(get_current_user)):
    return list_accounts(include_archived=False, current_user=current_user)


@router.post("/reorder")
def reorder_accounts(
    payload: AccountReorder,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    if not payload.account_ids:
        return {"ok": True, "message": "No accounts to reorder"}

    with db_conn() as conn:
        with conn.cursor() as cur:
            for order, acc_id in enumerate(payload.account_ids):
                cur.execute(
                    """
                    UPDATE accounts
                    SET display_order = %s, updated_at = NOW()
                    WHERE user_id = %s AND id = %s
                    """,
                    (order, user_id, str(acc_id)),
                )
            conn.commit()

    return {"ok": True, "message": "Accounts reordered successfully"}


@router.post("")
async def create_account(payload: AccountCreate, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    name = payload.name.strip()
    parent_id = str(payload.parent_id) if payload.parent_id else None
    default_funding_account_id = str(payload.default_funding_account_id) if payload.default_funding_account_id else None
    default_pocket_id = str(payload.default_pocket_id) if payload.default_pocket_id else None

    # Determine initial values
    initial_bal = payload.initial_balance
    if payload.units and payload.avg_buy_price and initial_bal == 0:
        initial_bal = int(round(payload.units * payload.avg_buy_price))

    last_price = None
    last_price_at = None

    # If instrument_symbol is provided, attempt to fetch live quote
    if payload.instrument_symbol:
        quote = await get_instrument_quote(payload.instrument_symbol)
        if quote and quote.get("price") is not None:
            last_price = quote["price"]
            last_price_at = datetime.now(timezone.utc)

    with db_conn() as conn:
        with conn.cursor() as cur:
            # Check unique name
            cur.execute(
                "SELECT id FROM accounts WHERE user_id = %s AND name = %s",
                (user_id, name),
            )
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="Account with this name already exists")

            # 2-level depth validation
            if parent_id:
                cur.execute(
                    "SELECT id, parent_id FROM accounts WHERE user_id = %s AND id = %s",
                    (user_id, parent_id),
                )
                parent_row = cur.fetchone()
                if not parent_row:
                    raise HTTPException(status_code=404, detail="Parent account not found")
                if parent_row["parent_id"] is not None:
                    raise HTTPException(
                        status_code=400,
                        detail="Child pockets cannot have sub-pockets (maximum 2 levels allowed)",
                    )

            if default_funding_account_id:
                cur.execute(
                    "SELECT id FROM accounts WHERE user_id = %s AND id = %s AND is_archived = false",
                    (user_id, default_funding_account_id),
                )
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Default funding account not found")

            if default_pocket_id:
                cur.execute(
                    "SELECT id FROM accounts WHERE user_id = %s AND id = %s AND is_archived = false",
                    (user_id, default_pocket_id),
                )
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Default pocket account not found")

            cur.execute(
                """
                INSERT INTO accounts (
                    user_id, parent_id, default_funding_account_id, default_pocket_id, name, type, initial_balance,
                    instrument_type, instrument_symbol, units, avg_buy_price,
                    last_price, last_price_at, color, display_order
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, parent_id, default_funding_account_id, default_pocket_id, name, type, initial_balance,
                          instrument_type, instrument_symbol, units, avg_buy_price,
                          last_price, last_price_at, color, display_order, is_archived, created_at
                """,
                (
                    user_id,
                    parent_id,
                    default_funding_account_id,
                    default_pocket_id,
                    name,
                    payload.type,
                    initial_bal,
                    payload.instrument_type,
                    payload.instrument_symbol.upper() if payload.instrument_symbol else None,
                    payload.units,
                    payload.avg_buy_price,
                    last_price,
                    last_price_at,
                    payload.color or "#3b82f6",
                    payload.display_order or 0,
                ),
            )
            row = cur.fetchone()
            conn.commit()

    acc_id = str(row["id"])
    pid = str(row["parent_id"]) if row.get("parent_id") else None
    dfid = str(row["default_funding_account_id"]) if row.get("default_funding_account_id") else None
    dpid = str(row["default_pocket_id"]) if row.get("default_pocket_id") else None
    init_b = int(row.get("initial_balance", 0))
    u = float(row["units"]) if row.get("units") is not None else None
    abp = float(row["avg_buy_price"]) if row.get("avg_buy_price") is not None else None
    lp = float(row["last_price"]) if row.get("last_price") is not None else None
    lpa = row["last_price_at"].isoformat() if row.get("last_price_at") else None

    bal = init_b
    cap_gain = None
    cap_gain_pct = None

    total_cost = None
    if u is not None and u > 0 and abp is not None and abp > 0:
        total_cost = int(round(u * abp))
    elif init_b > 0:
        total_cost = init_b
    elif abp is not None and abp > 0:
        total_cost = abp

    if u is not None and u > 0 and lp is not None and lp > 0:
        bal = int(round(u * lp))
        if total_cost is not None and total_cost > 0:
            cap_gain = bal - total_cost
            cap_gain_pct = round(((bal - total_cost) / total_cost) * 100, 2)
    elif row.get("type") == "investment" or row.get("instrument_type") is not None:
        if total_cost is not None and total_cost > 0 and bal != total_cost:
            cap_gain = bal - total_cost
            cap_gain_pct = round(((bal - total_cost) / total_cost) * 100, 2)

    return {
        "ok": True,
        "account": {
            "id": acc_id,
            "account_id": acc_id,
            "parent_id": pid,
            "default_funding_account_id": dfid,
            "default_pocket_id": dpid,
            "name": row.get("name"),
            "account_name": row.get("name"),
            "type": row.get("type"),
            "initial_balance": init_b,
            "balance": bal,
            "current_balance": bal,
            "instrument_type": row.get("instrument_type"),
            "instrument_symbol": row.get("instrument_symbol"),
            "units": u,
            "avg_buy_price": abp,
            "last_price": lp,
            "last_price_at": lpa,
            "capital_gain": cap_gain,
            "capital_gain_pct": cap_gain_pct,
            "cost_basis": total_cost,
            "color": row.get("color") or "#3b82f6",
            "display_order": int(row.get("display_order") or 0),
            "is_archived": row.get("is_archived", False),
            "is_parent": False,
            "children": [],
            "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        },
    }


@router.put("/{account_id}")
@router.patch("/{account_id}")
async def update_account(
    account_id: UUID,
    payload: AccountUpdate,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    aid = str(account_id)
    updates = []
    params = []

    if payload.name is not None:
        updates.append("name = %s")
        params.append(payload.name.strip())
    if payload.type is not None:
        updates.append("type = %s")
        params.append(payload.type)
    if payload.is_archived is not None:
        updates.append("is_archived = %s")
        params.append(payload.is_archived)
    if "parent_id" in payload.model_fields_set:
        updates.append("parent_id = %s")
        params.append(str(payload.parent_id) if payload.parent_id else None)
    if "default_funding_account_id" in payload.model_fields_set:
        dfid = str(payload.default_funding_account_id) if payload.default_funding_account_id else None
        if dfid and dfid == aid:
            raise HTTPException(status_code=400, detail="Account cannot be its own funding account")
        updates.append("default_funding_account_id = %s")
        params.append(dfid)
    if "instrument_type" in payload.model_fields_set:
        updates.append("instrument_type = %s")
        params.append(payload.instrument_type)
    if "instrument_symbol" in payload.model_fields_set:
        sym = payload.instrument_symbol.upper().strip() if payload.instrument_symbol else None
        updates.append("instrument_symbol = %s")
        params.append(sym)
        if sym:
            quote = await get_instrument_quote(sym)
            if quote and quote.get("price") is not None:
                updates.append("last_price = %s")
                params.append(quote["price"])
                updates.append("last_price_at = NOW()")
    if "units" in payload.model_fields_set:
        updates.append("units = %s")
        params.append(payload.units)
    if "avg_buy_price" in payload.model_fields_set:
        updates.append("avg_buy_price = %s")
        params.append(payload.avg_buy_price)
    if "last_price" in payload.model_fields_set and payload.last_price is not None:
        updates.append("last_price = %s")
        params.append(payload.last_price)
        updates.append("last_price_at = NOW()")
    if "color" in payload.model_fields_set and payload.color is not None:
        updates.append("color = %s")
        params.append(payload.color.strip())
    if "display_order" in payload.model_fields_set and payload.display_order is not None:
        updates.append("display_order = %s")
        params.append(payload.display_order)

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    with db_conn() as conn:
        with conn.cursor() as cur:
            # Verify account exists
            cur.execute("SELECT id, parent_id FROM accounts WHERE user_id = %s AND id = %s", (user_id, aid))
            curr_acc = cur.fetchone()
            if not curr_acc:
                raise HTTPException(status_code=404, detail="Account not found")

            # Validate default_funding_account_id if provided
            if "default_funding_account_id" in payload.model_fields_set and payload.default_funding_account_id is not None:
                dfid = str(payload.default_funding_account_id)
                cur.execute(
                    "SELECT id FROM accounts WHERE user_id = %s AND id = %s AND is_archived = false",
                    (user_id, dfid),
                )
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Default funding account not found")

            # Validate and update default_pocket_id if provided
            if "default_pocket_id" in payload.model_fields_set:
                if payload.default_pocket_id is not None:
                    dpid = str(payload.default_pocket_id)
                    cur.execute(
                        "SELECT id FROM accounts WHERE user_id = %s AND id = %s AND parent_id = %s AND is_archived = false",
                        (user_id, dpid, aid),
                    )
                    if not cur.fetchone():
                        raise HTTPException(status_code=400, detail="Default pocket must be an active child pocket of this account")
                    updates.append("default_pocket_id = %s")
                    params.append(dpid)
                else:
                    updates.append("default_pocket_id = NULL")

            # Validate parent_id if being updated
            if "parent_id" in payload.model_fields_set and payload.parent_id is not None:
                pid = str(payload.parent_id)
                if pid == aid:
                    raise HTTPException(status_code=400, detail="Account cannot be its own parent")

                cur.execute(
                    "SELECT COUNT(*) AS c FROM accounts WHERE user_id = %s AND parent_id = %s AND id != %s",
                    (user_id, aid, aid),
                )
                if cur.fetchone()["c"] > 0:
                    raise HTTPException(
                        status_code=400,
                        detail="An account with child pockets cannot become a child pocket",
                    )

                cur.execute(
                    "SELECT id, parent_id FROM accounts WHERE user_id = %s AND id = %s",
                    (user_id, pid),
                )
                parent_row = cur.fetchone()
                if not parent_row:
                    raise HTTPException(status_code=404, detail="Parent account not found")
                if parent_row["parent_id"] is not None:
                    raise HTTPException(
                        status_code=400,
                        detail="Child pockets cannot have sub-pockets (maximum 2 levels allowed)",
                    )

            if payload.is_archived is True:
                cur.execute(
                    "UPDATE accounts SET is_archived = TRUE, updated_at = NOW() WHERE user_id = %s AND parent_id = %s",
                    (user_id, aid),
                )
                cur.execute(
                    "UPDATE accounts SET default_pocket_id = NULL, updated_at = NOW() WHERE user_id = %s AND default_pocket_id = %s",
                    (user_id, aid),
                )

            updates.append("updated_at = NOW()")
            params.extend([user_id, aid])

            cur.execute(
                f"""
                UPDATE accounts
                SET {', '.join(updates)}
                WHERE user_id = %s AND id = %s
                RETURNING id
                """,
                params,
            )
            conn.commit()

    accounts = get_accounts_with_balances(user_id, include_archived=True)
    updated = next((a for a in accounts if a["id"] == aid), None)
    return {"ok": True, "account": updated}


@router.post("/{account_id}/valuation")
def update_account_valuation(
    account_id: UUID,
    payload: AccountValuationUpdate,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    aid = str(account_id)

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, type, instrument_type, instrument_symbol, units, avg_buy_price, initial_balance
                FROM accounts
                WHERE user_id = %s AND id = %s
                """,
                (user_id, aid),
            )
            acc = cur.fetchone()
            if not acc:
                raise HTTPException(status_code=404, detail="Account not found")

            updates = []
            params = []
            if payload.cost_basis is not None and payload.cost_basis > 0:
                if acc["units"] and float(acc["units"]) > 0:
                    new_unit_avg = round(float(payload.cost_basis) / float(acc["units"]), 4)
                    updates.append("avg_buy_price = %s")
                    params.append(new_unit_avg)
                else:
                    updates.append("avg_buy_price = %s")
                    params.append(payload.cost_basis)
                updates.append("initial_balance = %s")
                params.append(payload.cost_basis)

            # If units are tracked, update last_price = current_balance / units
            if acc["units"] and float(acc["units"]) > 0:
                new_unit_price = round(float(payload.current_balance) / float(acc["units"]), 4)
                updates.append("last_price = %s")
                params.append(new_unit_price)
                updates.append("last_price_at = NOW()")
            else:
                updates.append("last_price = %s")
                params.append(payload.current_balance)
                updates.append("last_price_at = NOW()")

            if updates:
                updates.append("updated_at = NOW()")
                params.extend([user_id, aid])
                cur.execute(
                    f"UPDATE accounts SET {', '.join(updates)} WHERE user_id = %s AND id = %s",
                    params,
                )
            conn.commit()

    accounts = get_accounts_with_balances(user_id, include_archived=False)
    updated = next((a for a in accounts if a["id"] == aid), None)
    return {"ok": True, "account": updated}


@router.post("/{account_id}/reconcile")
def reconcile_account(
    account_id: UUID,
    payload: AccountReconcile,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    aid = str(account_id)

    # 1. Fetch current calculated balance
    accounts = get_accounts_with_balances(user_id, include_archived=False)
    account = next((a for a in accounts if a["id"] == aid), None)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    current_balance = account["balance"]
    diff = payload.actual_balance - current_balance

    if diff == 0:
        return {
            "ok": True,
            "diff": 0,
            "message": "Balance is already in sync",
            "account": account,
        }

    with db_conn() as conn:
        with conn.cursor() as cur:
            tx_type = "income" if diff > 0 else "expense"
            cur.execute(
                "SELECT id FROM categories WHERE user_id = %s AND kind = %s AND (name ILIKE '%%penyesuaian%%' OR name ILIKE '%%adjustment%%') AND is_archived = false LIMIT 1",
                (user_id, tx_type),
            )
            cat_row = cur.fetchone()
            category_id = str(cat_row["id"]) if cat_row else None

            tx_amount = abs(diff)
            tx_notes = payload.notes or "Balance Adjustment (Reconciliation)"

            cur.execute(
                """
                INSERT INTO transactions (
                    user_id, account_id, category_id, type, amount, notes, date
                )
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                RETURNING id
                """,
                (user_id, aid, category_id, tx_type, tx_amount, tx_notes),
            )
            conn.commit()

    # Refetch updated account
    updated_accounts = get_accounts_with_balances(user_id, include_archived=False)
    updated_account = next((a for a in updated_accounts if a["id"] == aid), None)

    return {
        "ok": True,
        "diff": diff,
        "message": f"Account reconciled with {tx_type} adjustment of {tx_amount}",
        "account": updated_account,
    }


@router.delete("/{account_id}")
def delete_account(account_id: UUID, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    aid = str(account_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Check if account or its child pockets have transactions
            cur.execute(
                """
                SELECT COUNT(*) AS c 
                FROM transactions t
                WHERE t.account_id = %s
                   OR t.account_id IN (SELECT id FROM accounts WHERE parent_id = %s)
                """,
                (aid, aid),
            )
            row = cur.fetchone()
            if row and row["c"] > 0:
                # Soft-archive account and child pockets to preserve transaction history
                cur.execute(
                    "UPDATE accounts SET is_archived = TRUE, updated_at = NOW() WHERE user_id = %s AND (id = %s OR parent_id = %s)",
                    (user_id, aid, aid),
                )
                conn.commit()
                return {"ok": True, "message": "Account and pockets archived because of existing transactions"}

            cur.execute(
                "DELETE FROM accounts WHERE user_id = %s AND id = %s RETURNING id",
                (user_id, aid),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Account not found")
            conn.commit()

    return {"ok": True, "message": "Account deleted"}
