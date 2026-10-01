from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any
from uuid import uuid4

from fastapi import HTTPException

from app.services.ledger_mutations import (
    LIQUID_ACCOUNT_TYPES,
    create_bilateral_movement,
    ensure_sufficient_funds,
    get_locked_ledger_balance,
    lock_owned_accounts,
)
from app.services.movement_linkage import ensure_internal_movement_categories


def validate_topup_accounts(source: dict[str, Any], target: dict[str, Any]) -> None:
    if source["type"] not in LIQUID_ACCOUNT_TYPES or source.get("instrument_type"):
        raise HTTPException(status_code=422, detail={"code": "liquid_funding_required"})
    if (
        target.get("instrument_type") != "mutual_fund"
        or target.get("units") is not None
        or target.get("has_children")
    ):
        raise HTTPException(
            status_code=422,
            detail={
                "code": "amount_product_required",
                "message": "Top up requires a mutual-fund product tracked by total amount, without units.",
            },
        )
    if str(source["id"]) == str(target["id"]):
        raise HTTPException(status_code=422, detail={"code": "distinct_accounts_required"})


def initialize_amount_tracking(cur, user_id: str, account: dict[str, Any]) -> dict[str, Any]:
    if account.get("investment_tracking_mode") == "amount":
        return account
    ledger_balance = get_locked_ledger_balance(cur, user_id, str(account["id"]))
    value = int(account["last_price"]) if account.get("last_price") is not None else ledger_balance
    basis = None
    if int(account.get("initial_balance") or 0) > 0:
        basis = int(account["initial_balance"])
    elif account.get("avg_buy_price") is not None and account["avg_buy_price"] > 0:
        basis = int(round(account["avg_buy_price"]))
    elif value == 0 and ledger_balance == 0:
        basis = 0
    cur.execute(
        """UPDATE accounts SET investment_tracking_mode = 'amount',
               investment_cost_basis = %s, last_price = %s, updated_at = NOW()
           WHERE id = %s AND user_id = %s""",
        (basis, value, account["id"], user_id),
    )
    return {**account, "investment_tracking_mode": "amount", "investment_cost_basis": basis, "last_price": value}


def apply_contribution_delta(cur, user_id: str, account: dict[str, Any], delta: int) -> None:
    value = int(account["last_price"]) + delta
    old_basis = account.get("investment_cost_basis")
    basis = int(old_basis) + delta if old_basis is not None else None
    if value < 0 or (basis is not None and basis < 0):
        raise HTTPException(status_code=409, detail={"code": "invalid_investment_reversal"})
    if value > 2**63 - 1 or (basis is not None and basis > 2**63 - 1):
        raise HTTPException(status_code=422, detail={"code": "investment_amount_overflow"})
    cur.execute(
        """UPDATE accounts SET last_price = %s, investment_cost_basis = %s,
               investment_value_estimated = TRUE, updated_at = NOW()
           WHERE id = %s AND user_id = %s""",
        (value, basis, account["id"], user_id),
    )


def reject_topup_movement(cur, user_id: str, movement_id: str | None) -> None:
    if not movement_id:
        return
    cur.execute("SELECT id FROM investment_topups WHERE user_id = %s AND id = %s", (user_id, movement_id))
    if cur.fetchone():
        raise HTTPException(
            status_code=409,
            detail={"code": "investment_topup_lifecycle_required", "investment_topup_id": movement_id},
        )


def get_topup(cur, user_id: str, topup_id: str, *, for_update: bool = False) -> dict[str, Any]:
    lock = " FOR UPDATE" if for_update else ""
    cur.execute(
        "SELECT * FROM investment_topups WHERE user_id = %s AND id = %s" + lock,
        (user_id, topup_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Investment top-up not found")
    return row


def topup_response(cur, row: dict[str, Any], *, idempotent: bool = False) -> dict[str, Any]:
    cur.execute(
        "SELECT id, movement_role FROM transactions WHERE user_id = %s AND movement_id = %s",
        (row["user_id"], row["id"]),
    )
    legs = {leg["movement_role"]: str(leg["id"]) for leg in cur.fetchall()}
    return {
        "ok": True,
        "idempotent": idempotent,
        "topup": {
            "id": str(row["id"]),
            "movement_id": str(row["id"]),
            "movement_kind": "investment_topup",
            "source_account_id": str(row["source_account_id"]),
            "target_account_id": str(row["target_account_id"]),
            "amount": int(row["amount"]),
            "date": row["date"].isoformat(),
            "notes": row["notes"],
            "is_deleted": row["is_deleted"],
            "expense_transaction_id": legs.get("outbound"),
            "income_transaction_id": legs.get("inbound"),
        },
    }


def resolve_topup_source(cur, user_id: str, source_id: str) -> str:
    from app.routers.transactions import resolve_effective_account

    cur.execute(
        "SELECT * FROM accounts WHERE user_id = %s AND is_archived = FALSE ORDER BY display_order, created_at, name",
        (user_id,),
    )
    accounts = cur.fetchall()
    source = next((account for account in accounts if str(account["id"]) == source_id), None)
    if not source:
        raise HTTPException(status_code=404, detail="Funding account not found")
    # Resolution does not write the parent's default while financial account locks are pending.
    effective = resolve_effective_account(None, user_id, source, accounts)
    return str(effective["id"])


def create_topup(
    cur,
    *,
    user_id: str,
    source_id: str,
    target_id: str,
    amount: int,
    tx_date: datetime,
    notes: str,
    idempotency_key: str,
    recurring_rule_id: str | None = None,
    recurring_execution_id: str | None = None,
) -> dict[str, Any]:
    fingerprint = sha256(json.dumps({
        "source": source_id,
        "target": target_id,
        "amount": amount,
        "date": tx_date.astimezone(timezone.utc).isoformat(),
        "notes": notes,
    }, sort_keys=True).encode()).hexdigest()
    # A retry identifier is locked before account state, including when the first request is in flight.
    cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"investment-topup:{user_id}:{idempotency_key}",))
    cur.execute(
        "SELECT * FROM investment_topups WHERE user_id = %s AND idempotency_key = %s",
        (user_id, idempotency_key),
    )
    existing = cur.fetchone()
    if existing:
        if existing["request_fingerprint"] != fingerprint:
            raise HTTPException(status_code=409, detail={"code": "idempotency_mismatch"})
        return topup_response(cur, existing, idempotent=True)

    effective_source_id = resolve_topup_source(cur, user_id, source_id)
    locked = lock_owned_accounts(cur, user_id, [source_id, effective_source_id, target_id])
    if resolve_topup_source(cur, user_id, source_id) != effective_source_id:
        raise HTTPException(status_code=409, detail={"code": "funding_selection_changed"})
    source, target = locked[effective_source_id], locked[target_id]
    validate_topup_accounts(source, target)
    ensure_sufficient_funds(cur, user_id, source, amount)
    target = initialize_amount_tracking(cur, user_id, target)
    topup_id = str(uuid4())
    cur.execute(
        """INSERT INTO investment_topups (
               id, user_id, source_account_id, target_account_id, amount, date, notes,
               idempotency_key, request_fingerprint, recurring_execution_id
           ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (topup_id, user_id, effective_source_id, target_id, amount, tx_date, notes,
         idempotency_key, fingerprint, recurring_execution_id),
    )
    expense_category, income_category = ensure_internal_movement_categories(cur, user_id)
    create_bilateral_movement(
        cur, user_id=user_id, source_id=effective_source_id, target_id=target_id,
        amount=amount, notes=notes or "Top up Investasi", tx_date=tx_date,
        expense_category_id=expense_category, income_category_id=income_category,
        source_account=source, target_account=target,
        idempotency_key=f"investment-topup:{topup_id}", recurring_rule_id=recurring_rule_id,
        is_investment_topup=True, movement_id=topup_id,
    )
    apply_contribution_delta(cur, user_id, target, amount)
    return topup_response(cur, get_topup(cur, user_id, topup_id))


def mutate_topup(
    cur, user_id: str, topup_id: str, *, changes: dict[str, Any] | None = None, delete: bool = False
) -> dict[str, Any]:
    row = get_topup(cur, user_id, topup_id, for_update=True)
    if row["is_deleted"]:
        if delete:
            return topup_response(cur, row, idempotent=True)
        raise HTTPException(status_code=409, detail={"code": "investment_topup_deleted"})
    source_id, target_id = str(row["source_account_id"]), str(row["target_account_id"])
    locked = lock_owned_accounts(cur, user_id, [source_id, target_id], include_archived=True)
    updates = changes or {}
    amount = int(updates.get("amount", row["amount"]))
    delta = -int(row["amount"]) if delete else amount - int(row["amount"])
    if delta > 0:
        if locked[source_id]["is_archived"] or locked[target_id]["is_archived"]:
            raise HTTPException(status_code=409, detail={"code": "archived_topup_account"})
        validate_topup_accounts(locked[source_id], locked[target_id])
        ensure_sufficient_funds(cur, user_id, locked[source_id], delta)
    cur.execute(
        "SELECT id FROM transactions WHERE user_id = %s AND movement_id = %s ORDER BY id FOR UPDATE",
        (user_id, topup_id),
    )
    if len(cur.fetchall()) != 2:
        raise HTTPException(status_code=409, detail={"code": "incomplete_investment_topup"})
    if delta:
        apply_contribution_delta(cur, user_id, locked[target_id], delta)
    if delete:
        cur.execute("DELETE FROM transactions WHERE user_id = %s AND movement_id = %s", (user_id, topup_id))
        cur.execute("UPDATE investment_topups SET is_deleted = TRUE, updated_at = NOW() WHERE id = %s", (topup_id,))
    else:
        tx_date = updates.get("date", row["date"])
        notes = updates.get("notes", row["notes"]) or ""
        cur.execute(
            "UPDATE transactions SET amount = %s, date = %s, notes = %s, updated_at = NOW() WHERE user_id = %s AND movement_id = %s",
            (amount, tx_date, notes or "Top up Investasi", user_id, topup_id),
        )
        cur.execute(
            "UPDATE investment_topups SET amount = %s, date = %s, notes = %s, updated_at = NOW() WHERE id = %s",
            (amount, tx_date, notes, topup_id),
        )
    return topup_response(cur, get_topup(cur, user_id, topup_id))
