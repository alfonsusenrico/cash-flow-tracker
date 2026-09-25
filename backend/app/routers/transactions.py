import re
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field

from app.core.config import settings
from app.db.pool import db_conn
from app.services.auth import get_current_user
from app.services.debt_allocations import (
    DebtAllocation,
    apply_allocations,
    load_allocation_breakdowns,
    load_allocation_rows,
    lock_debts,
    reverse_allocations,
    validate_allocation_request,
    validate_debt_capacity,
)
from app.services.ledger_mutations import ensure_sufficient_funds, get_locked_ledger_balance, lock_owned_accounts
from app.services.receipts import (
    build_receipt_relative_path,
    prepare_receipt_payload,
    remove_receipt_file,
    store_receipt,
)

router = APIRouter(tags=["Transactions"])


class TransactionCreate(BaseModel):
    account_id: UUID | None = None
    account_name: str | None = Field(default=None, max_length=100)
    type: str = Field(pattern="^(expense|income)$")
    amount: int = Field(gt=0)
    category_id: UUID | None = None
    category_name: str | None = Field(default=None, max_length=100)
    target_account_id: UUID | None = None
    target_account_name: str | None = Field(default=None, max_length=100)
    goal_id: UUID | None = None
    obligation_id: UUID | None = None
    obligation_allocations: list[DebtAllocation] | None = None
    notes: str | None = Field(default=None, max_length=500)
    date: datetime | None = None
    receipt_path: str | None = None
    idempotency_key: str | None = Field(default=None, max_length=64)
    kakeibo_type: str | None = Field(default=None, pattern="^(need|want|saving)$")
    investment_action: str | None = Field(default=None, pattern="^(buy|sell)$")
    units: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    price_per_unit: float | None = Field(default=None, gt=0, allow_inf_nan=False)


class TransactionUpdate(BaseModel):
    account_id: UUID | None = None
    category_id: UUID | None = None
    type: str | None = Field(default=None, pattern="^(expense|income)$")
    goal_id: UUID | None = None
    obligation_id: UUID | None = None
    obligation_allocations: list[DebtAllocation] | None = None
    amount: int | None = Field(default=None, gt=0)
    notes: str | None = Field(default=None, max_length=500)
    date: datetime | None = None
    kakeibo_type: str | None = Field(default=None, pattern="^(need|want|saving)$")


@router.get("")
def list_transactions(
    account_id: UUID | None = None,
    category_id: UUID | None = None,
    goal_id: UUID | None = None,
    obligation_id: UUID | None = None,
    type: str | None = Query(default=None, pattern="^(expense|income|transfer)$"),
    kakeibo_type: str | None = Query(default=None, pattern="^(need|want|saving)$"),
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    q: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            conditions = ["t.user_id = %s"]
            params: list[Any] = [user_id]

            if account_id:
                aid = str(account_id)
                cur.execute(
                    "SELECT id FROM accounts WHERE user_id = %s AND (id = %s OR parent_id = %s) AND is_archived = FALSE",
                    (user_id, aid, aid),
                )
                child_rows = cur.fetchall()
                matching_acc_ids = [str(r["id"]) for r in child_rows] or [aid]
                placeholders = ", ".join(["%s"] * len(matching_acc_ids))
                conditions.append(f"t.account_id IN ({placeholders})")
                params.extend(matching_acc_ids)

            if category_id:
                conditions.append("t.category_id = %s")
                params.append(str(category_id))
            if goal_id:
                conditions.append("t.goal_id = %s")
                params.append(str(goal_id))
            if obligation_id:
                conditions.append("(t.obligation_id = %s OR EXISTS (SELECT 1 FROM transaction_obligation_allocations a WHERE a.transaction_id = t.id AND a.obligation_id = %s))")
                params.extend([str(obligation_id), str(obligation_id)])
            if type:
                if type == "transfer":
                    conditions.append("(t.type = 'transfer' OR c.name IN ('Internal Movement', 'Investasi') OR c.is_excluded_from_budget = true)")
                elif type == "expense":
                    conditions.append("t.type = 'expense' AND COALESCE(c.name, '') NOT IN ('Internal Movement', 'Investasi') AND COALESCE(c.is_excluded_from_budget, false) = false")
                elif type == "income":
                    conditions.append("t.type = 'income' AND COALESCE(c.name, '') NOT IN ('Internal Movement', 'Investasi') AND COALESCE(c.is_excluded_from_budget, false) = false")
            if kakeibo_type:
                conditions.append("COALESCE(t.kakeibo_type, c.kakeibo_type, CASE WHEN COALESCE(c.is_primary, TRUE) THEN 'need' ELSE 'want' END) = %s")
                params.append(kakeibo_type)
            if from_date:
                conditions.append("t.date >= %s")
                params.append(from_date)
            if to_date:
                conditions.append("t.date <= %s")
                params.append(to_date)
            if q:
                search_pattern = f"%{q.strip()}%"
                conditions.append("(t.notes ILIKE %s OR c.name ILIKE %s OR g.name ILIKE %s OR o.name ILIKE %s OR EXISTS (SELECT 1 FROM transaction_obligation_allocations a JOIN obligations debt ON debt.id = a.obligation_id WHERE a.transaction_id = t.id AND debt.name ILIKE %s))")
                params.extend([search_pattern] * 5)

            where_clause = " AND ".join(conditions)

            # Count total matching
            count_query = f"""
                SELECT COUNT(*) AS total
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                LEFT JOIN goals g ON g.id = t.goal_id
                LEFT JOIN obligations o ON o.id = t.obligation_id
                WHERE {where_clause}
            """
            cur.execute(count_query, params)
            total = cur.fetchone()["total"]

            # Select page
            select_query = f"""
                SELECT
                    t.id,
                    t.account_id,
                    sa.name AS account_name,
                    t.category_id,
                    c.name AS category_name,
                    c.icon AS category_icon,
                    c.color AS category_color,
                    COALESCE(c.is_excluded_from_budget, false) AS is_excluded_from_budget,
                    t.goal_id,
                    g.name AS goal_name,
                    t.obligation_id,
                    o.name AS obligation_name,
                    t.type,
                    COALESCE(t.kakeibo_type, c.kakeibo_type, CASE WHEN COALESCE(c.is_primary, TRUE) THEN 'need' ELSE 'want' END) AS kakeibo_type,
                    t.amount,
                    t.notes,
                    t.date,
                    t.receipt_path,
                    t.movement_id,
                    t.movement_role,
                    partner.id AS partner_id,
                    partner.account_id AS movement_target_account_id,
                    partner_account.name AS movement_target_account_name,
                    t.created_at
                FROM transactions t
                JOIN accounts sa ON sa.id = t.account_id
                LEFT JOIN transactions partner
                    ON partner.movement_id = t.movement_id
                   AND partner.id != t.id
                LEFT JOIN accounts partner_account ON partner_account.id = partner.account_id
                LEFT JOIN categories c ON c.id = t.category_id
                LEFT JOIN goals g ON g.id = t.goal_id
                LEFT JOIN obligations o ON o.id = t.obligation_id
                WHERE {where_clause}
                ORDER BY t.date DESC, t.created_at DESC
                LIMIT %s OFFSET %s
            """
            cur.execute(select_query, params + [limit, offset])
            rows = cur.fetchall()
            allocation_breakdowns = load_allocation_breakdowns(cur, rows)

    return {
        "ok": True,
        "total": total,
        "limit": limit,
        "offset": offset,
        "transactions": [
            {
                "id": str(r["id"]),
                "account_id": str(r["account_id"]),
                "account_name": r["account_name"],
                "category_id": str(r["category_id"]) if r["category_id"] else None,
                "category_name": r["category_name"],
                "category_icon": r["category_icon"],
                "category_color": r["category_color"],
                "is_excluded_from_budget": bool(r.get("is_excluded_from_budget", False)),
                "goal_id": str(r["goal_id"]) if r["goal_id"] else None,
                "goal_name": r["goal_name"],
                "obligation_id": str(r["obligation_id"]) if r["obligation_id"] else None,
                "obligation_name": r["obligation_name"],
                "obligation_allocations": allocation_breakdowns[str(r["id"])],
                "type": r["type"],
                "kakeibo_type": r.get("kakeibo_type"),
                "movement_id": str(r["movement_id"]) if r.get("movement_id") else None,
                "movement_role": r.get("movement_role"),
                "partner_id": str(r["partner_id"]) if r.get("partner_id") else None,
                "transfer_target_account_id": str(r["movement_target_account_id"]) if r.get("movement_target_account_id") else None,
                "transfer_target_account_name": r.get("movement_target_account_name"),
                "amount": r["amount"],
                "notes": r["notes"],
                "date": r["date"].isoformat() if r["date"] else None,
                "receipt_path": r["receipt_path"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ],
    }


def _match_account_by_name(
    cur,
    user_id: str,
    raw_name: str,
    accounts: list[dict[str, Any]],
    parent_account: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not raw_name:
        return None
    raw = raw_name.strip()
    raw_lower = raw.lower()

    # 0. Match by instrument symbol (e.g. "BBRI" matching "BBRI" or "BBRI.JK")
    for acc in accounts:
        sym = (acc.get("instrument_symbol") or "").strip().upper()
        if sym:
            sym_clean = re.sub(r"\.(JK|IDX)$", "", sym, flags=re.IGNORECASE)
            if raw.upper() == sym or raw.upper() == sym_clean:
                return acc

    # 1. Exact match
    for acc in accounts:
        if (acc.get("name") or "").strip().lower() == raw_lower:
            return acc

    # 2. Match without "pocket" or "kantong"
    norm = re.sub(r"\bpocket\b|\bkantong\b", "", raw_lower, flags=re.IGNORECASE).strip()
    if norm:
        for acc in accounts:
            acc_name = (acc.get("name") or "").strip().lower()
            acc_norm = re.sub(r"\bpocket\b|\bkantong\b", "", acc_name, flags=re.IGNORECASE).strip()
            if acc_name == norm or acc_norm == norm:
                return acc

    # 3. Synonym dictionary
    synonyms = {
        "emergency fund": "dana darurat",
        "emergency": "dana darurat",
        "darurat": "dana darurat",
        "savings": "tabungan",
        "saving": "tabungan",
        "rdn bca": "rdn",
        "rdn": "rdn",
        "rekening dana nasabah": "rdn",
        "dana nasabah": "rdn",
    }
    for syn_key, syn_val in synonyms.items():
        if syn_key in norm or syn_key in raw_lower:
            for acc in accounts:
                if syn_val in (acc.get("name") or "").lower():
                    return acc

    # 4. Main/Utama fallback to parent or designated main account
    if norm in ("main", "utama", "kantong utama", "") or raw_lower in ("main", "utama", "kantong utama"):
        if parent_account:
            return parent_account
        jago_acc = next((a for a in accounts if "jago" in (a.get("name") or "").lower() and not a.get("parent_id")), None)
        if jago_acc:
            return jago_acc
        for acc in accounts:
            if any(k in (acc.get("name") or "").lower() for k in ("utama", "main")):
                return acc
        return next((a for a in accounts if not a.get("parent_id")), None)

    # 5. Substring match
    for acc in accounts:
        acc_name = (acc.get("name") or "").strip().lower()
        if norm and (norm in acc_name or acc_name in norm):
            return acc
        if raw_lower in acc_name or acc_name in raw_lower:
            return acc

    # 6. Auto-provision child pocket under parent bank if applicable
    if norm and norm not in ("main", "utama", "kantong utama"):
        target_parent = parent_account
        if not target_parent:
            target_parent = next((a for a in accounts if "jago" in (a.get("name") or "").lower() and not a.get("parent_id")), None)
        if target_parent and "id" in target_parent:
            cur.execute(
                """
                INSERT INTO accounts (user_id, parent_id, name, type, initial_balance)
                VALUES (%s, %s, %s, 'bank', 0)
                RETURNING id, name, parent_id, type
                """,
                (user_id, target_parent["id"], raw),
            )
            created = cur.fetchone()
            if created:
                accounts.append(created)
                return created

    return None


def _resolve_category_by_name(
    cur,
    user_id: str,
    raw_name: str,
    categories: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not raw_name:
        return None
    raw = raw_name.strip()
    raw_lower = raw.lower()

    for cat in categories:
        if (cat.get("name") or "").strip().lower() == raw_lower:
            return cat

    # Common English/Indonesian synonyms
    synonyms = {
        "food & beverage": "makanan & minuman",
        "food and beverage": "makanan & minuman",
        "f&b": "makanan & minuman",
        "food": "makanan & minuman",
        "beverage": "makanan & minuman",
        "groceries": "belanja",
        "shopping": "belanja",
        "transport": "transportasi",
        "transportation": "transportasi",
        "salary": "gaji",
        "health": "kesehatan",
        "medical": "kesehatan",
        "bills & utilities": "tagihan & utilitas",
        "utilities": "tagihan & utilitas",
        "payment": "tagihan & utilitas",
        "account transfer": "pendapatan lain",
        "transfer masuk": "pendapatan lain",
        "transfer": "internal movement",
    }
    target_syn = synonyms.get(raw_lower)
    if target_syn:
        for cat in categories:
            if (cat.get("name") or "").strip().lower() == target_syn:
                return cat

    # Auto-seed standard categories if missing
    icon = "tag"
    color = "#3b82f6"
    kind = "expense"
    if raw_lower == "internal movement":
        icon = "repeat"
        color = "#64748b"
    elif raw_lower == "investasi":
        icon = "trending-up"
        color = "#0ea5e9"
    elif raw_lower in ("gaji", "pendapatan lain"):
        icon = "dollar-sign"
        color = "#22c55e"
        kind = "income"
    elif raw_lower == "makanan & minuman":
        icon = "utensils"
        color = "#f97316"

    cur.execute(
        """
        INSERT INTO categories (user_id, name, icon, color, kind)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING id, name, kind, icon, color
        """,
        (user_id, raw, icon, color, kind),
    )
    created = cur.fetchone()
    if created:
        categories.append(created)
        return created

    cur.execute(
        "SELECT id, name, kind, icon, color FROM categories WHERE user_id = %s AND LOWER(name) = %s LIMIT 1",
        (user_id, raw_lower),
    )
    found = cur.fetchone()
    if found:
        categories.append(found)
        return found
    return None


def resolve_effective_account(
    cur,
    user_id: str,
    account: dict[str, Any] | None,
    accounts: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """If the account has child pockets, the parent account cannot directly hold transactions or balance.
    Resolves the account down to:
    1. Its configured default_pocket_id (if set and active)
    2. Fallback: Child pocket matching common main pocket names ('ATM', 'Main Pocket', 'Kantong Utama', 'Utama', 'Tabungan', 'Checking')
    3. Fallback: First active child pocket ordered by display_order, created_at, name.
    If resolved to a fallback, automatically updates parent's default_pocket_id in DB.
    """
    if not account:
        return None

    acc_id = str(account["id"])
    child_pockets = [
        a for a in accounts
        if a.get("parent_id") and str(a["parent_id"]) == acc_id and not a.get("is_archived")
    ]

    # Standalone account or already a child pocket
    if not child_pockets:
        return account

    # 1. Configured default pocket
    dpid = str(account["default_pocket_id"]) if account.get("default_pocket_id") else None
    if dpid:
        chosen = next((c for c in child_pockets if str(c["id"]) == dpid), None)
        if chosen:
            return chosen

    # 2. Fallback: Look for standard default pocket names
    preferred_names = ["atm", "kantong utama", "main pocket", "utama", "tabungan", "checking"]
    chosen = None
    for pref in preferred_names:
        chosen = next((c for c in child_pockets if pref in (c.get("name") or "").lower()), None)
        if chosen:
            break

    # 3. Fallback: First active child pocket (sorted by display_order, created_at, name)
    if not chosen:
        sorted_pockets = sorted(
            child_pockets,
            key=lambda x: (
                x.get("display_order", 0),
                str(x.get("created_at") or ""),
                (x.get("name") or "").lower(),
            )
        )
        chosen = sorted_pockets[0]

    # Persist the default_pocket_id back to parent account so it's formally saved
    if chosen and cur:
        try:
            chosen_id = str(chosen["id"])
            cur.execute(
                "UPDATE accounts SET default_pocket_id = %s, updated_at = NOW() WHERE user_id = %s AND id = %s",
                (chosen_id, user_id, acc_id),
            )
            account["default_pocket_id"] = chosen_id
        except Exception:
            pass

    return chosen


@router.post("")
def create_transaction(payload: TransactionCreate, current_user: dict = Depends(get_current_user)):
    if payload.receipt_path is not None:
        raise HTTPException(status_code=422, detail="Upload receipts through the owned transaction receipt endpoint")
    user_id = current_user["id"]
    tx_date = payload.date or datetime.now(timezone.utc)

    with db_conn() as conn:
        with conn.cursor() as cur:
            # 1. Idempotency Check
            if payload.idempotency_key:
                if payload.obligation_allocations is not None:
                    cur.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                        (f"debt-payment:{user_id}:{payload.idempotency_key}",),
                    )
                cur.execute(
                    """
                    SELECT id, created_at, amount, type
                    FROM transactions
                    WHERE user_id = %s AND idempotency_key = %s
                    """,
                    (user_id, payload.idempotency_key),
                )
                existing_tx = cur.fetchone()
                if existing_tx:
                    return {
                        "ok": True,
                        "transaction_id": str(existing_tx["id"]),
                        "created_at": existing_tx["created_at"].isoformat() if existing_tx.get("created_at") else None,
                        "amount": existing_tx["amount"],
                        "type": existing_tx["type"],
                        "idempotent": True,
                        "message": "Transaction already recorded (idempotent)",
                    }

            # Pre-fetch user accounts & categories for name resolution
            cur.execute(
                "SELECT id, parent_id, name, type, instrument_type, instrument_symbol, default_funding_account_id, default_pocket_id FROM accounts WHERE user_id = %s AND is_archived = FALSE",
                (user_id,),
            )
            accounts = cur.fetchall()

            cur.execute(
                "SELECT id, name, kind, kakeibo_type, is_primary FROM categories WHERE user_id = %s AND is_archived = FALSE",
                (user_id,),
            )
            categories = cur.fetchall()

            # Resolve source account
            account_id = None
            matched_source = None
            if payload.account_id:
                account_id = str(payload.account_id)
                matched_source = next((a for a in accounts if str(a["id"]) == account_id), None)
                if not matched_source:
                    cur.execute(
                        "SELECT id, name, type, default_pocket_id, parent_id, is_archived FROM accounts WHERE user_id = %s AND id = %s",
                        (user_id, account_id),
                    )
                    inactive_source = cur.fetchone()
                    if inactive_source and not inactive_source["is_archived"]:
                        matched_source = inactive_source
                if not matched_source:
                    raise HTTPException(status_code=404, detail="Source account not found")
            elif payload.account_name:
                matched_source = _match_account_by_name(cur, user_id, payload.account_name, accounts)
                if not matched_source:
                    raise HTTPException(status_code=400, detail=f"Source account '{payload.account_name}' could not be resolved")
                account_id = str(matched_source["id"])
            else:
                raise HTTPException(status_code=422, detail="Either account_id or account_name must be provided")

            # Route to effective pocket if source is a parent account with child pockets
            effective_source = resolve_effective_account(cur, user_id, matched_source, accounts)
            if effective_source:
                matched_source = effective_source
                account_id = str(effective_source["id"])

            # Resolve target account for investment tracking if provided
            target_account_id = None
            if payload.target_account_id:
                target_account_id = str(payload.target_account_id)
                matched_target = next((a for a in accounts if str(a["id"]) == target_account_id), None)
                if not matched_target:
                    raise HTTPException(status_code=404, detail="Target account not found")
                effective_target = resolve_effective_account(cur, user_id, matched_target, accounts)
                if effective_target:
                    matched_target = effective_target
                    target_account_id = str(effective_target["id"])
            elif payload.target_account_name:
                matched_target = _match_account_by_name(cur, user_id, payload.target_account_name, accounts)
                if not matched_target:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Target account '{payload.target_account_name}' could not be resolved",
                    )
                effective_target = resolve_effective_account(cur, user_id, matched_target, accounts)
                if effective_target:
                    matched_target = effective_target
                target_account_id = str(matched_target["id"])

            # Resolve category
            category_id = None
            if payload.category_id:
                category_id = str(payload.category_id)
                cur.execute(
                    "SELECT id, kind FROM categories WHERE user_id = %s AND id = %s AND is_archived = FALSE",
                    (user_id, category_id),
                )
                category = cur.fetchone()
                if not category:
                    raise HTTPException(status_code=404, detail="Category not found")
                if not payload.investment_action and category["kind"] != payload.type:
                    raise HTTPException(status_code=400, detail="Category is incompatible with transaction type")
            elif payload.category_name:
                matched_cat = _resolve_category_by_name(cur, user_id, payload.category_name, categories)
                if matched_cat:
                    category_id = str(matched_cat["id"])
                else:
                    raise HTTPException(status_code=400, detail="Category could not be resolved")

            if not payload.investment_action and (payload.target_account_id or payload.target_account_name):
                raise HTTPException(status_code=400, detail="A target account is only valid for an investment trade")
            if not payload.investment_action and (
                matched_source.get("type") == "investment" or matched_source.get("instrument_type")
            ):
                raise HTTPException(status_code=422, detail="Investment positions can only be changed through a Beli/Jual trade")

            # Validate goal if provided
            goal_id = str(payload.goal_id) if payload.goal_id else None
            if goal_id:
                cur.execute("SELECT id FROM goals WHERE user_id = %s AND id = %s AND is_archived = FALSE", (user_id, goal_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Goal not found")

            # Validate obligation if provided
            obligation_id = str(payload.obligation_id) if payload.obligation_id else None
            allocation_values = validate_allocation_request(
                payload.obligation_allocations,
                amount=payload.amount,
                obligation_id=payload.obligation_id,
                transaction_type=payload.type,
                investment_action=payload.investment_action,
            )
            if obligation_id:
                cur.execute("SELECT id FROM obligations WHERE user_id = %s AND id = %s AND is_archived = FALSE AND remaining_amount > 0", (user_id, obligation_id))
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Obligation not found")

            # Determine kakeibo_type
            kakeibo_val = payload.kakeibo_type
            if payload.investment_action:
                # Investment trade (buy/sell) is capital deployment within portfolio, NOT monthly living budget saving
                kakeibo_val = None
                if not category_id:
                    cur.execute("SELECT id FROM categories WHERE user_id = %s AND name = 'Investasi' LIMIT 1", (user_id,))
                    inv_cat = cur.fetchone()
                    if inv_cat:
                        category_id = str(inv_cat["id"])
            elif not kakeibo_val and category_id:
                cur.execute(
                    "SELECT kakeibo_type, is_primary FROM categories WHERE id = %s",
                    (category_id,),
                )
                crow = cur.fetchone()
                if crow:
                    kakeibo_val = crow.get("kakeibo_type") or ("need" if crow.get("is_primary", True) else "want")

            if payload.investment_action:
                if target_account_id is None:
                    raise HTTPException(status_code=400, detail="Investment trades require a target account")
                if account_id == target_account_id:
                    raise HTTPException(status_code=400, detail="Funding and investment accounts must be different")

                trade_units = float(payload.units or 0)
                trade_price = float(payload.price_per_unit or 0)
                expected_amount = int(round(trade_units * trade_price))
                if expected_amount <= 0 or expected_amount != payload.amount:
                    raise HTTPException(status_code=400, detail="Trade amount must equal units multiplied by price per unit")

                if payload.investment_action == "buy":
                    funding_id = account_id
                    position_id = target_account_id
                else:
                    position_id = account_id
                    funding_id = target_account_id

                locked = lock_owned_accounts(cur, user_id, [funding_id, position_id])
                funding = locked[funding_id]
                position = locked[position_id]
                if funding.get("type") not in {"cash", "bank", "wallet", "ewallet"} or funding.get("instrument_type"):
                    raise HTTPException(status_code=400, detail="Funding account must be a liquid account")
                if position.get("type") != "investment" and not position.get("instrument_type"):
                    raise HTTPException(status_code=400, detail="Position account must be an investment account")

                old_units = float(position.get("units") or 0)
                old_avg = float(position.get("avg_buy_price") or 0)
                if payload.investment_action == "sell" and trade_units > old_units:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "code": "insufficient_units",
                            "required_units": trade_units,
                            "available_units": old_units,
                            "account_id": position_id,
                        },
                    )

                from app.routers.movements import _ensure_internal_movement_categories
                from app.services.ledger_mutations import create_bilateral_movement

                expense_category_id, income_category_id = _ensure_internal_movement_categories(cur, user_id)
                source_id = funding_id if payload.investment_action == "buy" else position_id
                destination_id = position_id if payload.investment_action == "buy" else funding_id
                result = create_bilateral_movement(
                    cur,
                    user_id=user_id,
                    source_id=source_id,
                    target_id=destination_id,
                    amount=payload.amount,
                    notes=payload.notes or f"Investment {payload.investment_action}",
                    tx_date=tx_date,
                    expense_category_id=expense_category_id,
                    income_category_id=income_category_id,
                    source_account=locked[source_id],
                    target_account=locked[destination_id],
                    idempotency_key=payload.idempotency_key,
                    is_trade=True,
                )

                if payload.investment_action == "buy":
                    new_units = old_units + trade_units
                    new_avg = round(((old_units * old_avg) + (trade_units * trade_price)) / new_units)
                    cur.execute(
                        "UPDATE accounts SET units = %s, avg_buy_price = %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (new_units, new_avg, position_id, user_id),
                    )
                else:
                    cur.execute(
                        "UPDATE accounts SET units = %s, updated_at = NOW() WHERE id = %s AND user_id = %s",
                        (old_units - trade_units, position_id, user_id),
                    )
                conn.commit()
                return {
                    "ok": True,
                    "transaction_id": result["expense_transaction_id"],
                    **result,
                    "message": "Investment trade recorded successfully",
                }

            if payload.target_account_id or payload.target_account_name:
                raise HTTPException(status_code=400, detail="Use the movement endpoint for account transfers")

            if payload.type == "expense":
                locked = lock_owned_accounts(cur, user_id, [account_id])
                ensure_sufficient_funds(cur, user_id, locked[account_id], payload.amount)
            if allocation_values:
                debts = lock_debts(cur, user_id, [debt_id for debt_id, _ in allocation_values])
                validate_debt_capacity(debts, allocation_values)

            # Insert transaction
            cur.execute(
                """
                INSERT INTO transactions (
                    user_id, account_id, category_id, goal_id, obligation_id, type,
                    amount, notes, date, receipt_path, kakeibo_type, idempotency_key
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, created_at
                """,
                (
                    user_id,
                    account_id,
                    category_id,
                    goal_id,
                    obligation_id,
                    payload.type,
                    payload.amount,
                    payload.notes,
                    tx_date,
                    None,
                    kakeibo_val,
                    payload.idempotency_key,
                ),
            )
            created = cur.fetchone()

            if allocation_values:
                for debt_id, allocated_amount in allocation_values:
                    cur.execute(
                        """
                        INSERT INTO transaction_obligation_allocations (transaction_id, obligation_id, amount)
                        VALUES (%s, %s, %s)
                        """,
                        (str(created["id"]), debt_id, allocated_amount),
                    )
                apply_allocations(cur, user_id, allocation_values)

            # If linked to an obligation, update obligation's remaining_amount
            if obligation_id:
                if payload.type == "expense":
                    cur.execute(
                        """
                        UPDATE obligations
                        SET remaining_amount = GREATEST(0, remaining_amount - %s),
                            is_archived = CASE WHEN (remaining_amount - %s) <= 0 THEN true ELSE is_archived END,
                            updated_at = NOW()
                        WHERE id = %s AND user_id = %s
                        """,
                        (payload.amount, payload.amount, obligation_id, user_id),
                    )
                elif payload.type == "income":
                    cur.execute(
                        """
                        UPDATE obligations
                        SET remaining_amount = remaining_amount + %s,
                            is_archived = CASE WHEN (remaining_amount + %s) > 0 THEN false ELSE is_archived END,
                            updated_at = NOW()
                        WHERE id = %s AND user_id = %s
                        """,
                        (payload.amount, payload.amount, obligation_id, user_id),
                    )

            conn.commit()

    return {
        "ok": True,
        "transaction_id": str(created["id"]),
        "message": "Transaction recorded successfully",
    }


@router.get("/{transaction_id}")
def get_transaction(transaction_id: UUID, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    tid = str(transaction_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    t.id, t.account_id, sa.name AS account_name,
                    t.category_id, c.name AS category_name, c.icon AS category_icon, c.color AS category_color,
                    t.goal_id, g.name AS goal_name,
                    t.obligation_id, o.name AS obligation_name,
                    t.type,
                    COALESCE(t.kakeibo_type, c.kakeibo_type, CASE WHEN COALESCE(c.is_primary, TRUE) THEN 'need' ELSE 'want' END) AS kakeibo_type,
                    t.amount, t.notes, t.date, t.receipt_path,
                    t.movement_id, t.movement_role,
                    partner.id AS partner_id,
                    partner.account_id AS movement_target_account_id,
                    partner_account.name AS movement_target_account_name,
                    t.created_at
                FROM transactions t
                JOIN accounts sa ON sa.id = t.account_id
                LEFT JOIN transactions partner
                    ON partner.movement_id = t.movement_id
                   AND partner.id != t.id
                LEFT JOIN accounts partner_account ON partner_account.id = partner.account_id
                LEFT JOIN categories c ON c.id = t.category_id
                LEFT JOIN goals g ON g.id = t.goal_id
                LEFT JOIN obligations o ON o.id = t.obligation_id
                WHERE t.user_id = %s AND t.id = %s
                """,
                (user_id, tid),
            )
            r = cur.fetchone()
            if not r:
                raise HTTPException(status_code=404, detail="Transaction not found")
            allocation_breakdowns = load_allocation_breakdowns(cur, [r])

    return {
        "ok": True,
        "transaction": {
            "id": str(r["id"]),
            "account_id": str(r["account_id"]),
            "account_name": r["account_name"],
            "category_id": str(r["category_id"]) if r["category_id"] else None,
            "category_name": r["category_name"],
            "category_icon": r["category_icon"],
            "category_color": r["category_color"],
            "goal_id": str(r["goal_id"]) if r["goal_id"] else None,
            "goal_name": r["goal_name"],
            "obligation_id": str(r["obligation_id"]) if r["obligation_id"] else None,
            "obligation_name": r["obligation_name"],
            "obligation_allocations": allocation_breakdowns[tid],
            "type": r["type"],
            "kakeibo_type": r.get("kakeibo_type"),
            "movement_id": str(r["movement_id"]) if r.get("movement_id") else None,
            "movement_role": r.get("movement_role"),
            "partner_id": str(r["partner_id"]) if r.get("partner_id") else None,
            "transfer_target_account_id": str(r["movement_target_account_id"]) if r.get("movement_target_account_id") else None,
            "transfer_target_account_name": r.get("movement_target_account_name"),
            "amount": r["amount"],
            "notes": r["notes"],
            "date": r["date"].isoformat() if r["date"] else None,
            "receipt_path": r["receipt_path"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        },
    }


@router.put("/{transaction_id}")
@router.patch("/{transaction_id}")
def update_transaction(
    transaction_id: UUID,
    payload: TransactionUpdate,
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    tid = str(transaction_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            # 1. Fetch current transaction
            cur.execute(
                """
                SELECT id, account_id, category_id, goal_id, obligation_id, type,
                       amount, notes, date, receipt_path, kakeibo_type, movement_id
                FROM transactions
                WHERE user_id = %s AND id = %s
                FOR UPDATE
                """,
                (user_id, tid),
            )
            old_tx = cur.fetchone()
            if not old_tx:
                raise HTTPException(status_code=404, detail="Transaction not found")
            if old_tx.get("movement_id"):
                raise HTTPException(status_code=409, detail="Linked movements must be edited through the movement endpoint")
            old_allocation_rows = load_allocation_rows(cur, tid)
            old_allocations = [
                (str(row["obligation_id"]), int(row["amount"]))
                for row in old_allocation_rows
            ]

            # 3. Determine new values (merging payload with old_tx)
            new_amount = payload.amount if payload.amount is not None else old_tx["amount"]
            if payload.account_id is not None:
                new_acc_id_str = str(payload.account_id)
                cur.execute(
                    "SELECT id, parent_id, name, type, default_pocket_id FROM accounts WHERE user_id = %s AND is_archived = FALSE",
                    (user_id,),
                )
                user_accounts = cur.fetchall()
                target_acc = next((a for a in user_accounts if str(a["id"]) == new_acc_id_str), None)
                if not target_acc:
                    raise HTTPException(status_code=404, detail="Account not found")
                effective_acc = resolve_effective_account(cur, user_id, target_acc, user_accounts)
                new_account_id = str(effective_acc["id"]) if effective_acc else new_acc_id_str
            else:
                new_account_id = str(old_tx["account_id"])
            new_type = payload.type if payload.type is not None else old_tx["type"]

            if "category_id" in payload.model_fields_set:
                new_category_id = str(payload.category_id) if payload.category_id else None
            else:
                new_category_id = str(old_tx["category_id"]) if old_tx["category_id"] else None

            if "goal_id" in payload.model_fields_set:
                new_goal_id = str(payload.goal_id) if payload.goal_id else None
            else:
                new_goal_id = str(old_tx["goal_id"]) if old_tx["goal_id"] else None

            allocation_field_set = "obligation_allocations" in payload.model_fields_set
            legacy_field_set = "obligation_id" in payload.model_fields_set
            if legacy_field_set:
                new_obligation_id = str(payload.obligation_id) if payload.obligation_id else None
            elif allocation_field_set:
                new_obligation_id = None
            else:
                new_obligation_id = str(old_tx["obligation_id"]) if old_tx["obligation_id"] else None

            if allocation_field_set and payload.obligation_allocations is None:
                raise HTTPException(
                    status_code=422,
                    detail={"code": "invalid_debt_allocations", "message": "Kirim daftar pembagian tagihan atau daftar kosong untuk menghapusnya."},
                )
            if allocation_field_set:
                requested_allocations = payload.obligation_allocations
            elif legacy_field_set:
                requested_allocations = None
            elif old_allocations:
                requested_allocations = [
                    DebtAllocation(obligation_id=UUID(debt_id), amount=allocated_amount)
                    for debt_id, allocated_amount in old_allocations
                ]
            else:
                requested_allocations = None

            new_allocations = validate_allocation_request(
                requested_allocations,
                amount=new_amount,
                obligation_id=new_obligation_id,
                transaction_type=new_type,
            )

            new_notes = payload.notes if "notes" in payload.model_fields_set else old_tx["notes"]
            new_date = payload.date if payload.date is not None else old_tx["date"]

            if "kakeibo_type" in payload.model_fields_set:
                new_kakeibo_type = payload.kakeibo_type
            elif "category_id" in payload.model_fields_set and new_category_id:
                cur.execute(
                    "SELECT kakeibo_type, is_primary FROM categories WHERE id = %s",
                    (new_category_id,),
                )
                crow = cur.fetchone()
                new_kakeibo_type = (crow.get("kakeibo_type") if crow else None) or ("need" if crow and crow.get("is_primary", True) else "want")
            else:
                new_kakeibo_type = old_tx.get("kakeibo_type")

            locked_account = None
            if new_type == "expense":
                locked_account = lock_owned_accounts(cur, user_id, [new_account_id])[new_account_id]

            affected_debt_ids = [debt_id for debt_id, _ in old_allocations]
            affected_debt_ids.extend(debt_id for debt_id, _ in new_allocations or [])
            if old_tx["obligation_id"]:
                affected_debt_ids.append(str(old_tx["obligation_id"]))
            if new_obligation_id:
                affected_debt_ids.append(new_obligation_id)
            locked_debts = lock_debts(cur, user_id, affected_debt_ids)

            if old_allocations:
                reverse_allocations(cur, user_id, old_allocations)
                for debt_id, allocated_amount in old_allocations:
                    locked_debts[debt_id]["remaining_amount"] += allocated_amount
                    locked_debts[debt_id]["is_archived"] = False
            elif old_tx["obligation_id"]:
                old_debt_id = str(old_tx["obligation_id"])
                if old_tx["type"] == "expense":
                    reverse_allocations(cur, user_id, [(old_debt_id, int(old_tx["amount"]))])
                    locked_debts[old_debt_id]["remaining_amount"] += int(old_tx["amount"])
                    locked_debts[old_debt_id]["is_archived"] = False
                elif old_tx["type"] == "income":
                    cur.execute(
                        """
                        UPDATE obligations
                        SET remaining_amount = GREATEST(0, remaining_amount - %s),
                            is_archived = CASE WHEN (remaining_amount - %s) <= 0 THEN true ELSE is_archived END,
                            updated_at = NOW()
                        WHERE id = %s AND user_id = %s
                        """,
                        (old_tx["amount"], old_tx["amount"], old_debt_id, user_id),
                    )
                    locked_debts[old_debt_id]["remaining_amount"] = max(
                        0,
                        int(locked_debts[old_debt_id]["remaining_amount"]) - int(old_tx["amount"]),
                    )
                    locked_debts[old_debt_id]["is_archived"] = locked_debts[old_debt_id]["remaining_amount"] == 0

            if new_allocations:
                validate_debt_capacity(
                    locked_debts,
                    new_allocations,
                    previously_linked_ids=set(debt_id for debt_id, _ in old_allocations),
                )

            # Validate accounts / categories / goals / obligations
            if new_category_id:
                cur.execute(
                    "SELECT id, kind FROM categories WHERE user_id = %s AND id = %s AND is_archived = FALSE",
                    (user_id, new_category_id),
                )
                category = cur.fetchone()
                if not category:
                    raise HTTPException(status_code=404, detail="Category not found")
                if category["kind"] != new_type:
                    raise HTTPException(status_code=400, detail="Category is incompatible with transaction type")
            if new_goal_id:
                cur.execute("SELECT id FROM goals WHERE user_id = %s AND id = %s AND is_archived = FALSE", (user_id, new_goal_id))
                if not cur.fetchone() and new_goal_id != (str(old_tx["goal_id"]) if old_tx.get("goal_id") else None):
                    raise HTTPException(status_code=404, detail="Goal not found")
            if new_obligation_id:
                cur.execute("SELECT id FROM obligations WHERE user_id = %s AND id = %s AND is_archived = FALSE AND remaining_amount > 0", (user_id, new_obligation_id))
                if not cur.fetchone() and new_obligation_id != (str(old_tx["obligation_id"]) if old_tx.get("obligation_id") else None):
                    raise HTTPException(status_code=404, detail="Obligation not found")

            if new_type == "expense":
                if locked_account.get("type") in {"cash", "bank", "wallet", "ewallet"} and not locked_account.get("instrument_type"):
                    available = get_locked_ledger_balance(cur, user_id, new_account_id)
                    if old_tx["type"] == "expense" and str(old_tx["account_id"]) == new_account_id:
                        available += int(old_tx["amount"])
                    if available < new_amount:
                        raise HTTPException(
                            status_code=409,
                            detail={
                                "code": "insufficient_funds",
                                "required_amount": new_amount,
                                "available_amount": available,
                                "account_id": new_account_id,
                            },
                        )

            # 4. Update the transaction row
            cur.execute(
                """
                UPDATE transactions
                SET account_id = %s,
                    category_id = %s,
                    type = %s,
                    goal_id = %s,
                    obligation_id = %s,
                    amount = %s,
                    notes = %s,
                    date = %s,
                    kakeibo_type = %s,
                    updated_at = NOW()
                WHERE user_id = %s AND id = %s
                RETURNING id
                """,
                (
                    new_account_id,
                    new_category_id,
                    new_type,
                    new_goal_id,
                    new_obligation_id,
                    new_amount,
                    new_notes,
                    new_date,
                    new_kakeibo_type,
                    user_id,
                    tid,
                ),
            )

            if old_allocations or new_allocations:
                cur.execute(
                    "DELETE FROM transaction_obligation_allocations WHERE transaction_id = %s",
                    (tid,),
                )
                for debt_id, allocated_amount in new_allocations or []:
                    cur.execute(
                        """
                        INSERT INTO transaction_obligation_allocations (transaction_id, obligation_id, amount)
                        VALUES (%s, %s, %s)
                        """,
                        (tid, debt_id, allocated_amount),
                    )
                if new_allocations:
                    apply_allocations(cur, user_id, new_allocations)

            # 5. Apply new obligation side-effect. Goal links are metadata only.
            if new_obligation_id:
                if new_type == "expense":
                    cur.execute(
                        """
                        UPDATE obligations
                        SET remaining_amount = GREATEST(0, remaining_amount - %s),
                            is_archived = CASE WHEN (remaining_amount - %s) <= 0 THEN true ELSE is_archived END,
                            updated_at = NOW()
                        WHERE id = %s AND user_id = %s
                        """,
                        (new_amount, new_amount, new_obligation_id, user_id),
                    )
                elif new_type == "income":
                    cur.execute(
                        """
                        UPDATE obligations
                        SET remaining_amount = remaining_amount + %s,
                            is_archived = CASE WHEN (remaining_amount + %s) > 0 THEN false ELSE is_archived END,
                            updated_at = NOW()
                        WHERE id = %s AND user_id = %s
                        """,
                        (new_amount, new_amount, new_obligation_id, user_id),
                    )

            conn.commit()

    return {"ok": True, "message": "Transaction updated successfully"}


@router.delete("/{transaction_id}")
def delete_transaction(transaction_id: UUID, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    tid = str(transaction_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            # Query transaction to reverse any goal/obligation side effects
            cur.execute(
                "SELECT type, amount, goal_id, obligation_id, movement_id, receipt_path FROM transactions WHERE user_id = %s AND id = %s FOR UPDATE",
                (user_id, tid),
            )
            tx = cur.fetchone()
            if not tx:
                raise HTTPException(status_code=404, detail="Transaction not found")
            if tx.get("movement_id"):
                raise HTTPException(status_code=409, detail="Linked movements must be deleted through the movement endpoint")

            allocation_rows = load_allocation_rows(cur, tid)
            allocations = [
                (str(row["obligation_id"]), int(row["amount"]))
                for row in allocation_rows
            ]
            if allocations:
                lock_debts(cur, user_id, [debt_id for debt_id, _ in allocations])
                reverse_allocations(cur, user_id, allocations)

            # Reverse obligation adjustment
            if tx["obligation_id"]:
                if tx["type"] == "expense":
                    cur.execute(
                        """
                        UPDATE obligations
                        SET remaining_amount = remaining_amount + %s,
                            is_archived = CASE WHEN (remaining_amount + %s) > 0 THEN false ELSE is_archived END,
                            updated_at = NOW()
                        WHERE id = %s AND user_id = %s
                        """,
                        (tx["amount"], tx["amount"], str(tx["obligation_id"]), user_id),
                    )
                elif tx["type"] == "income":
                    cur.execute(
                        """
                        UPDATE obligations
                        SET remaining_amount = GREATEST(0, remaining_amount - %s),
                            is_archived = CASE WHEN (remaining_amount - %s) <= 0 THEN true ELSE is_archived END,
                            updated_at = NOW()
                        WHERE id = %s AND user_id = %s
                        """,
                        (tx["amount"], tx["amount"], str(tx["obligation_id"]), user_id),
                    )

            cur.execute(
                "DELETE FROM transactions WHERE user_id = %s AND id = %s",
                (user_id, tid),
            )
            conn.commit()

    remove_receipt_file(tx.get("receipt_path"))

    return {"ok": True, "message": "Transaction deleted"}


@router.post("/{transaction_id}/receipt")
async def upload_receipt(
    transaction_id: UUID,
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    tid = str(transaction_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM transactions WHERE id = %s AND user_id = %s",
                (tid, user_id),
            )
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Transaction not found")

    max_bytes = max(1, settings.receipt_max_mb) * 1024 * 1024
    chunks = []
    total_bytes = 0
    while total_bytes <= max_bytes:
        chunk = await file.read(min(64 * 1024, max_bytes + 1 - total_bytes))
        if not chunk:
            break
        chunks.append(chunk)
        total_bytes += len(chunk)
    if total_bytes > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"Receipt file too large (max {settings.receipt_max_mb}MB)",
        )
    content = b"".join(chunks)
    prepared = prepare_receipt_payload(
        raw=content,
        filename=file.filename,
        content_type=file.content_type,
        category="general",
    )
    rel_path = build_receipt_relative_path(
        str(user_id),
        tid,
        prepared.category,
        prepared.stored_ext,
    )
    try:
        with db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT receipt_path FROM transactions WHERE id = %s AND user_id = %s FOR UPDATE",
                    (tid, user_id),
                )
                owned_transaction = cur.fetchone()
                if not owned_transaction:
                    raise HTTPException(status_code=404, detail="Transaction not found")
                store_receipt(rel_path, prepared.content)
                cur.execute(
                    "UPDATE transactions SET receipt_path = %s, updated_at = NOW() WHERE id = %s AND user_id = %s RETURNING id",
                    (rel_path, tid, user_id),
                )
                if not cur.fetchone():
                    raise HTTPException(status_code=404, detail="Transaction not found")
                conn.commit()
    except Exception:
        remove_receipt_file(rel_path)
        raise

    remove_receipt_file(owned_transaction.get("receipt_path"))

    return {"ok": True, "receipt_path": rel_path, "message": "Receipt uploaded successfully"}
