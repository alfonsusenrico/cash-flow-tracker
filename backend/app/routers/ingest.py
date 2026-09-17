import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.db.pool import db_conn
from app.services.auth import get_current_user
from app.services.category_rules import resolve_category_for_notification
from app.services.market_data import get_instrument_quote
from app.services.notification_parser import parse_notification

router = APIRouter(tags=["Notification Ingestion"])


def _match_pocket_account(
    pocket_name: str | None,
    child_accounts: list[dict],
    parent_account: dict | None = None,
) -> dict | None:
    if not pocket_name:
        return None
    raw = pocket_name.strip()
    raw_lower = raw.lower()
    norm = re.sub(r"\bpocket\b|\bkantong\b", "", raw_lower, flags=re.IGNORECASE).strip()

    # 1. Exact match on child account name
    for acc in child_accounts:
        acc_name = (acc.get("name") or "").strip().lower()
        if acc_name == raw_lower or (norm and acc_name == norm):
            return acc

    # 2. Substring match against child accounts
    for acc in child_accounts:
        acc_name = (acc.get("name") or "").strip().lower()
        if norm and (norm in acc_name or acc_name in norm):
            return acc
        if raw_lower in acc_name or acc_name in raw_lower:
            return acc

    # 3. Known synonyms (English <-> Indonesian)
    # e.g. "Emergency Fund" or "My Emergency Fund" -> "Dana Darurat"
    synonyms = {
        "emergency fund": "dana darurat",
        "emergency": "dana darurat",
        "darurat": "dana darurat",
        "savings": "tabungan",
        "saving": "tabungan",
        "main": "utama",
    }
    for syn_key, syn_val in synonyms.items():
        if syn_key in norm or syn_key in raw_lower:
            for acc in child_accounts:
                acc_name = (acc.get("name") or "").strip().lower()
                if syn_val in acc_name:
                    return acc

    # 4. Main/Utama fallback to parent or designated main child pocket
    if norm in ("main", "utama", "kantong utama", "") or raw_lower in ("main", "utama", "kantong utama"):
        for acc in child_accounts:
            acc_name = (acc.get("name") or "").strip().lower()
            if any(k in acc_name for k in ("utama", "main")):
                return acc
        return parent_account

    return None


class NotificationEventIn(BaseModel):
    device_id: str = Field(..., max_length=64)
    package_name: str = Field(..., max_length=255)
    app_label: str | None = Field(default=None, max_length=100)
    notification_key: str | None = None
    notification_id: int | None = None
    channel_id: str | None = Field(default=None, max_length=100)
    category: str | None = Field(default=None, max_length=50)
    title: str | None = None
    body_text: str | None = None
    big_text: str | None = None
    sub_text: str | None = None
    summary_text: str | None = None
    post_time: datetime
    payload_hash: str = Field(..., max_length=64)
    raw_extras: dict[str, Any] | None = None
    source_version: str | None = Field(default=None, max_length=50)
    is_financial: bool | None = None
    event_class: str | None = Field(default=None, max_length=50)
    expected_amount: Decimal | None = None
    expected_direction: str | None = Field(default=None, max_length=20)
    expected_counterparty: str | None = Field(default=None, max_length=255)
    label_notes: str | None = None


class BatchNotificationIngest(BaseModel):
    events: list[NotificationEventIn] = Field(..., min_length=1, max_length=200)


class NotificationLabelUpdate(BaseModel):
    is_financial: bool | None = None
    event_class: str | None = Field(default=None, max_length=50)
    expected_amount: Decimal | None = None
    expected_direction: str | None = Field(default=None, max_length=20)
    expected_counterparty: str | None = Field(default=None, max_length=255)
    label_notes: str | None = None


@router.post("/notifications")
async def ingest_notifications(
    payload: BatchNotificationIngest,
    current_user: dict = Depends(get_current_user),
):
    """
    Ingest a batch of raw/sanitized notification events from the mobile companion app.
    Idempotent via payload_hash; parses, auto-categorizes, and records transactions.
    """
    user_id = current_user["id"]
    inserted = 0
    updated = 0
    created_txs = 0

    with db_conn() as conn:
        with conn.cursor() as cur:
            # Pre-fetch user's categories, rules, and accounts for fast batch processing
            cur.execute("SELECT id, name, kind, kakeibo_type, is_excluded_from_budget FROM categories WHERE user_id = %s", (user_id,))
            categories = cur.fetchall()

            cur.execute("SELECT merchant_pattern, category_id FROM merchant_category_rules WHERE user_id = %s", (user_id,))
            user_rules = cur.fetchall()

            cur.execute("SELECT id, name, type, default_funding_account_id, parent_id FROM accounts WHERE user_id = %s AND is_archived = FALSE", (user_id,))
            accounts = cur.fetchall()

            for ev in payload.events:
                # Discard empty group summary / foreground notifications
                if not (ev.title and ev.title.strip()) and not (ev.body_text and ev.body_text.strip()) and not (ev.big_text and ev.big_text.strip()):
                    continue

                extras_json = json.dumps(ev.raw_extras) if ev.raw_extras else None

                # 1. Deterministic Notification Parsing
                parsed = parse_notification(
                    package_name=ev.package_name,
                    title=ev.title,
                    body_text=ev.body_text,
                    big_text=ev.big_text,
                )
                parsed_summary = parsed.to_dict()

                # Strict Whitelist Gate: Discard non-financial noise before DB insertion or ledger creation
                final_amount = parsed.amount if (parsed.amount is not None and parsed.amount > 0) else (
                    int(ev.expected_amount) if ev.expected_amount else None
                )
                if not parsed.is_financial or parsed.event_class == "noise" or not final_amount or final_amount <= 0:
                    continue

                # 2. Automated Categorization
                cat_res = resolve_category_for_notification(parsed, categories, user_rules)
                parsed_summary["resolved_category"] = cat_res

                # 3. Check existing event & transaction linkage
                cur.execute(
                    "SELECT id, transaction_id FROM notification_events WHERE user_id = %s AND payload_hash = %s",
                    (user_id, ev.payload_hash),
                )
                existing_ev = cur.fetchone()
                tx_id = existing_ev.get("transaction_id") if (isinstance(existing_ev, dict)) else None

                # 4. Auto-create ledger transaction if it is a settled transaction with a valid amount
                if tx_id is None and parsed.is_financial and parsed.event_class != "noise" and final_amount and final_amount > 0:
                    pkg_lower = ev.package_name.lower()
                    matched_account = None
                    if "jago" in pkg_lower:
                        matched_account = next((a for a in accounts if "jago" in a.get("name", "").lower()), None)
                    elif "bca" in pkg_lower:
                        matched_account = next((a for a in accounts if "bca" in a.get("name", "").lower() and "rdn" not in a.get("name", "").lower()), None)
                    elif "gopay" in pkg_lower or "gojek" in pkg_lower:
                        matched_account = next((a for a in accounts if "gopay" in a.get("name", "").lower()), None)
                    elif "shopee" in pkg_lower:
                        matched_account = next((a for a in accounts if "shopee" in a.get("name", "").lower()), None)
                    elif "stockbit" in pkg_lower:
                        matched_account = next((a for a in accounts if "stockbit" in a.get("name", "").lower()), None)

                    if not matched_account and accounts:
                        matched_account = accounts[0]

                    if matched_account and "id" in matched_account:
                        source_account_id = matched_account["id"]
                        transfer_target_id = None
                        tx_type = "expense"

                        # Child accounts belonging to the matched parent institution
                        child_accounts = [
                            a for a in accounts
                            if a.get("parent_id") and str(a.get("parent_id")) == str(matched_account["id"])
                        ]

                        # If this is an investment account with a linked default funding account (e.g. Stockbit linked to RDN BCA)
                        is_investment = (
                            matched_account.get("type") == "investment"
                            or "stockbit" in pkg_lower
                            or cat_res.get("category_name") == "Investasi"
                        )
                        if is_investment and matched_account.get("default_funding_account_id"):
                            funding_acc = next(
                                (a for a in accounts if str(a["id"]) == str(matched_account["default_funding_account_id"])),
                                None,
                            )
                            if funding_acc:
                                stock_pocket_id = None
                                if parsed.symbol or parsed.instrument_symbol:
                                    sym = (parsed.symbol or "").upper().strip()
                                    inst_sym = (parsed.instrument_symbol or f"{sym}.JK").upper().strip()
                                    trade_units = float(parsed.units or ((parsed.lots or 0) * 100))
                                    trade_price = float(parsed.price_per_unit or 0)

                                    try:
                                        live_quote = await get_instrument_quote(inst_sym)
                                        cur_price = live_quote["price"] if (live_quote and live_quote.get("price")) else trade_price
                                    except Exception:
                                        cur_price = trade_price

                                    cur.execute(
                                        """
                                        SELECT id, units, avg_buy_price, last_price
                                        FROM accounts
                                        WHERE user_id = %s
                                          AND parent_id = %s
                                          AND (instrument_symbol = %s OR UPPER(name) = %s)
                                          AND is_archived = FALSE
                                        LIMIT 1
                                        """,
                                        (user_id, matched_account["id"], inst_sym, sym),
                                    )
                                    existing_pocket = cur.fetchone()

                                    if existing_pocket and isinstance(existing_pocket, dict):
                                        stock_pocket_id = existing_pocket.get("id")
                                        old_units = float(existing_pocket.get("units") or 0)
                                        old_avg = float(existing_pocket.get("avg_buy_price") or 0)

                                        if parsed.investment_action == "buy" or parsed.event_class == "expense" or parsed.direction == "out":
                                            new_units = old_units + trade_units
                                            if new_units > 0 and trade_price > 0:
                                                new_avg = round(((old_units * old_avg) + (trade_units * trade_price)) / new_units)
                                            else:
                                                new_avg = old_avg or trade_price

                                            cur.execute(
                                                """
                                                UPDATE accounts
                                                SET units = %s,
                                                    avg_buy_price = %s,
                                                    last_price = %s,
                                                    last_price_at = NOW(),
                                                    instrument_symbol = %s,
                                                    updated_at = NOW()
                                                WHERE id = %s
                                                """,
                                                (new_units, new_avg, cur_price, inst_sym, stock_pocket_id),
                                            )
                                        else:
                                            new_units = max(0.0, old_units - trade_units)
                                            cur.execute(
                                                """
                                                UPDATE accounts
                                                SET units = %s,
                                                    last_price = %s,
                                                    last_price_at = NOW(),
                                                    updated_at = NOW()
                                                WHERE id = %s
                                                """,
                                                (new_units, cur_price, stock_pocket_id),
                                            )
                                    else:
                                        if parsed.investment_action == "buy" or parsed.event_class == "expense" or parsed.direction == "out":
                                            cur.execute(
                                                """
                                                INSERT INTO accounts (
                                                    user_id, parent_id, name, type, initial_balance,
                                                    instrument_type, instrument_symbol, units, avg_buy_price,
                                                    last_price, last_price_at
                                                ) VALUES (
                                                    %s, %s, %s, 'investment', 0,
                                                    'stock', %s, %s, %s,
                                                    %s, NOW()
                                                ) RETURNING id
                                                """,
                                                (
                                                    user_id,
                                                    matched_account["id"],
                                                    sym,
                                                    inst_sym,
                                                    trade_units,
                                                    trade_price,
                                                    cur_price,
                                                ),
                                            )
                                            created_pocket = cur.fetchone()
                                            if created_pocket and isinstance(created_pocket, dict):
                                                stock_pocket_id = created_pocket.get("id")

                                target_investment_dest_id = stock_pocket_id or matched_account["id"]

                                # When buying: cash leaves funding_acc (RDN BCA) and enters investment account/pocket
                                if parsed.event_class == "expense" or parsed.direction == "out":
                                    source_account_id = funding_acc["id"]
                                    transfer_target_id = target_investment_dest_id
                                    tx_type = "transfer"
                                else:  # selling: proceeds leave investment pocket/account and enter funding_acc (RDN BCA)
                                    source_account_id = target_investment_dest_id
                                    transfer_target_id = funding_acc["id"]
                                    tx_type = "transfer"
                        elif "jago" in pkg_lower and (parsed.source_pocket or parsed.target_pocket):
                            src_acc = _match_pocket_account(parsed.source_pocket, child_accounts, matched_account)
                            tgt_acc = _match_pocket_account(parsed.target_pocket, child_accounts, matched_account)

                            # Auto-create source pocket if not found and not a variation of main/utama
                            if parsed.source_pocket and not src_acc:
                                norm_s = re.sub(r"\bpocket\b|\bkantong\b", "", parsed.source_pocket, flags=re.I).strip()
                                if norm_s and norm_s.lower() not in ("main", "utama", "kantong utama"):
                                    cur.execute(
                                        """
                                        INSERT INTO accounts (user_id, parent_id, name, type, initial_balance)
                                        VALUES (%s, %s, %s, 'bank', 0)
                                        RETURNING id, name, parent_id, type
                                        """,
                                        (user_id, matched_account["id"], parsed.source_pocket.strip()),
                                    )
                                    src_acc = cur.fetchone()
                                    if src_acc:
                                        accounts.append(src_acc)
                                        child_accounts.append(src_acc)

                            # Auto-create target pocket if not found and not a variation of main/utama
                            if parsed.target_pocket and not tgt_acc:
                                norm_t = re.sub(r"\bpocket\b|\bkantong\b", "", parsed.target_pocket, flags=re.I).strip()
                                if norm_t and norm_t.lower() not in ("main", "utama", "kantong utama"):
                                    cur.execute(
                                        """
                                        INSERT INTO accounts (user_id, parent_id, name, type, initial_balance)
                                        VALUES (%s, %s, %s, 'bank', 0)
                                        RETURNING id, name, parent_id, type
                                        """,
                                        (user_id, matched_account["id"], parsed.target_pocket.strip()),
                                    )
                                    tgt_acc = cur.fetchone()
                                    if tgt_acc:
                                        accounts.append(tgt_acc)
                                        child_accounts.append(tgt_acc)

                            # Fallback: moving out of pocket -> destination defaults to parent account
                            if src_acc and not tgt_acc:
                                tgt_acc = matched_account
                            # Fallback: moving into pocket -> source defaults to parent account
                            elif tgt_acc and not src_acc:
                                src_acc = matched_account

                            if src_acc and "id" in src_acc:
                                source_account_id = src_acc["id"]
                            if tgt_acc and "id" in tgt_acc:
                                transfer_target_id = tgt_acc["id"]

                            tx_type = "transfer"
                        else:
                            is_internal_movement = (
                                parsed.direction == "internal"
                                or cat_res.get("category_name") == "Internal Movement"
                                or (parsed.counterparty and any(kw in parsed.counterparty.lower() for kw in ["alfonsus", "enrico", "tabungan by jago"]))
                            )
                            if is_internal_movement:
                                tx_type = "transfer"
                                if not transfer_target_id and parsed.counterparty:
                                    cp_lower = parsed.counterparty.lower()
                                    if "jago" in cp_lower and "jago" not in matched_account.get("name", "").lower():
                                        target_cand = next((a for a in accounts if "jago" in a.get("name", "").lower() and not a.get("parent_id")), None)
                                        if target_cand:
                                            transfer_target_id = target_cand["id"]
                                    elif "bca" in cp_lower and "bca" not in matched_account.get("name", "").lower():
                                        target_cand = next((a for a in accounts if "bca" in a.get("name", "").lower() and "rdn" not in a.get("name", "").lower() and not a.get("parent_id")), None)
                                        if target_cand:
                                            transfer_target_id = target_cand["id"]
                                    elif "gopay" in cp_lower and "gopay" not in matched_account.get("name", "").lower():
                                        target_cand = next((a for a in accounts if "gopay" in a.get("name", "").lower() and not a.get("parent_id")), None)
                                        if target_cand:
                                            transfer_target_id = target_cand["id"]
                            elif parsed.event_class == "income":
                                tx_type = "income"
                            else:
                                tx_type = "expense"

                        notes_content = f"{ev.app_label or ev.package_name}: {parsed.counterparty or ev.title or ''}".strip()
                        kakeibo_val = (
                            None
                            if (tx_type == "transfer" or is_investment or parsed.investment_action)
                            else cat_res.get("kakeibo_type", "need")
                        )
                        if tx_type == "transfer":
                            expense_cat = next((c for c in categories if c.get("name") == "Internal Movement" and c.get("kind") == "expense"), None)
                            income_cat = next((c for c in categories if c.get("name") == "Internal Movement" and c.get("kind") == "income"), None)
                            expense_cat_id = (expense_cat.get("id") if expense_cat else None) or cat_res.get("category_id")
                            income_cat_id = (income_cat.get("id") if income_cat else None) or cat_res.get("category_id")

                            if not expense_cat_id or not income_cat_id:
                                from app.routers.movements import _ensure_internal_movement_categories
                                exp_id, inc_id = _ensure_internal_movement_categories(cur, user_id)
                                expense_cat_id = expense_cat_id or exp_id
                                income_cat_id = income_cat_id or inc_id
                            cur.execute(
                                """
                                INSERT INTO transactions (
                                    user_id, account_id, category_id, type, amount, notes, date, kakeibo_type
                                ) VALUES (
                                    %s, %s, %s, 'expense', %s, %s, %s, %s
                                ) RETURNING id
                                """,
                                (
                                    user_id,
                                    source_account_id,
                                    expense_cat_id,
                                    final_amount,
                                    notes_content,
                                    ev.post_time,
                                    None,
                                ),
                            )
                            new_tx = cur.fetchone()
                            if new_tx and isinstance(new_tx, dict):
                                tx_id = new_tx.get("id")
                                if tx_id:
                                    created_txs += 1
                            if transfer_target_id:
                                cur.execute(
                                    """
                                    INSERT INTO transactions (
                                        user_id, account_id, category_id, type, amount, notes, date, kakeibo_type
                                    ) VALUES (
                                        %s, %s, %s, 'income', %s, %s, %s, %s
                                    ) RETURNING id
                                    """,
                                    (
                                        user_id,
                                        transfer_target_id,
                                        income_cat_id,
                                        final_amount,
                                        notes_content,
                                        ev.post_time,
                                        None,
                                    ),
                                )
                                if cur.fetchone():
                                    created_txs += 1
                        else:
                            cur.execute(
                                """
                                INSERT INTO transactions (
                                    user_id, account_id, category_id, type, amount, notes, date, kakeibo_type
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s
                                ) RETURNING id
                                """,
                                (
                                    user_id,
                                    source_account_id,
                                    cat_res.get("category_id"),
                                    tx_type,
                                    final_amount,
                                    notes_content,
                                    ev.post_time,
                                    kakeibo_val,
                                ),
                            )
                            new_tx = cur.fetchone()
                            if new_tx and isinstance(new_tx, dict):
                                tx_id = new_tx.get("id")
                                if tx_id:
                                    created_txs += 1

                # 5. Upsert notification event in lake
                labelled_at = datetime.now(timezone.utc) if (ev.is_financial is not None or ev.event_class is not None) else None
                upsert_sql = """
                    INSERT INTO notification_events (
                        user_id, device_id, package_name, app_label,
                        notification_key, notification_id, channel_id, category,
                        title, body_text, big_text, sub_text, summary_text,
                        post_time, payload_hash, raw_extras, source_version,
                        is_financial, event_class, expected_amount, expected_direction,
                        expected_counterparty, label_notes, labelled_at,
                        parsed_summary, transaction_id
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s
                    )
                    ON CONFLICT (user_id, payload_hash) DO UPDATE SET
                        is_financial = COALESCE(EXCLUDED.is_financial, notification_events.is_financial),
                        event_class = COALESCE(EXCLUDED.event_class, notification_events.event_class),
                        expected_amount = COALESCE(EXCLUDED.expected_amount, notification_events.expected_amount),
                        expected_direction = COALESCE(EXCLUDED.expected_direction, notification_events.expected_direction),
                        expected_counterparty = COALESCE(EXCLUDED.expected_counterparty, notification_events.expected_counterparty),
                        label_notes = COALESCE(EXCLUDED.label_notes, notification_events.label_notes),
                        parsed_summary = COALESCE(EXCLUDED.parsed_summary, notification_events.parsed_summary),
                        transaction_id = COALESCE(notification_events.transaction_id, EXCLUDED.transaction_id),
                        updated_at = NOW()
                    RETURNING (xmax = 0) AS was_inserted;
                """
                cur.execute(
                    upsert_sql,
                    (
                        user_id, ev.device_id, ev.package_name, ev.app_label,
                        ev.notification_key, ev.notification_id, ev.channel_id, ev.category,
                        ev.title, ev.body_text, ev.big_text, ev.sub_text, ev.summary_text,
                        ev.post_time, ev.payload_hash, extras_json, ev.source_version,
                        parsed.is_financial if ev.is_financial is None else ev.is_financial,
                        parsed.event_class if ev.event_class is None else ev.event_class,
                        final_amount if ev.expected_amount is None else ev.expected_amount,
                        parsed.direction if ev.expected_direction is None else ev.expected_direction,
                        parsed.counterparty if ev.expected_counterparty is None else ev.expected_counterparty,
                        cat_res.get("category_name") if ev.label_notes is None else ev.label_notes,
                        labelled_at,
                        json.dumps(parsed_summary, default=str),
                        tx_id,
                    ),
                )
                res = cur.fetchone()
                if res and res["was_inserted"]:
                    inserted += 1
                else:
                    updated += 1

        conn.commit()

    return {
        "ok": True,
        "received": len(payload.events),
        "inserted": inserted,
        "updated": updated,
        "created_transactions": created_txs,
    }


@router.get("/notifications")
def list_ingested_notifications(
    package_name: str | None = None,
    is_financial: bool | None = None,
    event_class: str | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    """
    List captured notification events for inspection, labeling, and parser testing.
    """
    user_id = current_user["id"]
    conditions = ["user_id = %s"]
    params: list[Any] = [user_id]

    if package_name:
        conditions.append("package_name = %s")
        params.append(package_name)
    if is_financial is not None:
        conditions.append("is_financial = %s")
        params.append(is_financial)
    if event_class:
        conditions.append("event_class = %s")
        params.append(event_class)

    where_clause = " AND ".join(conditions)

    count_query = f"SELECT COUNT(*) AS total FROM notification_events WHERE {where_clause}"
    select_query = f"""
        SELECT 
            id, device_id, package_name, app_label,
            notification_id, channel_id, category,
            title, body_text, big_text, sub_text, summary_text,
            post_time, captured_at, payload_hash, raw_extras, source_version,
            is_financial, event_class, expected_amount, expected_direction,
            expected_counterparty, label_notes, labelled_at
        FROM notification_events
        WHERE {where_clause}
        ORDER BY post_time DESC
        LIMIT %s OFFSET %s
    """

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(count_query, tuple(params))
            total = cur.fetchone()["total"]

            cur.execute(select_query, tuple(params + [limit, offset]))
            rows = cur.fetchall()

    events = []
    for r in rows:
        events.append({
            "id": str(r["id"]),
            "device_id": r["device_id"],
            "package_name": r["package_name"],
            "app_label": r["app_label"],
            "notification_id": r["notification_id"],
            "channel_id": r["channel_id"],
            "category": r["category"],
            "title": r["title"],
            "body_text": r["body_text"],
            "big_text": r["big_text"],
            "sub_text": r["sub_text"],
            "summary_text": r["summary_text"],
            "post_time": r["post_time"].isoformat(),
            "captured_at": r["captured_at"].isoformat(),
            "payload_hash": r["payload_hash"],
            "raw_extras": r["raw_extras"],
            "source_version": r["source_version"],
            "is_financial": r["is_financial"],
            "event_class": r["event_class"],
            "expected_amount": float(r["expected_amount"]) if r["expected_amount"] is not None else None,
            "expected_direction": r["expected_direction"],
            "expected_counterparty": r["expected_counterparty"],
            "label_notes": r["label_notes"],
            "labelled_at": r["labelled_at"].isoformat() if r["labelled_at"] else None,
        })

    return {
        "ok": True,
        "total": total,
        "limit": limit,
        "offset": offset,
        "events": events,
    }


@router.patch("/notifications/{event_id}/label")
def update_notification_label(
    event_id: UUID,
    label: NotificationLabelUpdate,
    current_user: dict = Depends(get_current_user),
):
    """
    Update label classification for a specific notification event.
    """
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE notification_events
                SET is_financial = COALESCE(%s, is_financial),
                    event_class = COALESCE(%s, event_class),
                    expected_amount = COALESCE(%s, expected_amount),
                    expected_direction = COALESCE(%s, expected_direction),
                    expected_counterparty = COALESCE(%s, expected_counterparty),
                    label_notes = COALESCE(%s, label_notes),
                    labelled_at = NOW(),
                    updated_at = NOW()
                WHERE id = %s AND user_id = %s
                RETURNING id
                """,
                (
                    label.is_financial, label.event_class, label.expected_amount,
                    label.expected_direction, label.expected_counterparty, label.label_notes,
                    str(event_id), user_id
                ),
            )
            updated = cur.fetchone()
            if not updated:
                raise HTTPException(status_code=404, detail="Notification event not found")
        conn.commit()

    return {"ok": True, "event_id": str(event_id)}
