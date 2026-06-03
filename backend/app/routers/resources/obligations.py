"""External payables and receivables — /obligations and /v1/obligations."""
from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from psycopg.errors import UniqueViolation

from app.db.pool import db_conn
from app.services.categories import ensure_named_category, seed_default_categories
from app.services.ledger import invalidate_user_cache
from app.services.ledger.balances import (
    ensure_account_non_negative,
    lock_accounts_for_update,
    parse_tx_datetime,
    parse_uuid_value,
)
from app.services.ledger.period import now_local, now_utc, parse_date_utc
from app.services.ledger.reports import write_transaction_audit

router = APIRouter(tags=["obligations"])

OPEN_STATUSES = ("open", "partial")


def _parse_int(value: Any, field_name: str, default: int | None = None) -> int:
    if value is None or (isinstance(value, str) and not value.strip()):
        if default is not None:
            return default
        raise HTTPException(status_code=400, detail=f"{field_name} required")
    if isinstance(value, bool):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")
    try:
        if isinstance(value, str):
            value = value.strip().replace(".", "").replace(",", "")
        return int(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}")


def _parse_optional_date(value: Any, field_name: str) -> date | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {field_name}, expected YYYY-MM-DD")


def _parse_settlement_datetime(value: Any):
    raw = str(value or "").strip()
    if raw and len(raw) == 10:
        if raw == now_local().date().isoformat():
            return now_utc()
        return parse_date_utc(raw, end_of_day=True)
    return parse_tx_datetime(raw or None)


def _parse_kind(raw: Any) -> str:
    kind = str(raw or "").strip().lower()
    if kind not in ("receivable", "payable"):
        raise HTTPException(status_code=400, detail="Invalid kind")
    return kind


def _parse_counterparty_type(raw: Any) -> str:
    value = str(raw or "person").strip().lower()
    if value not in ("person", "client", "vendor", "institution", "other"):
        raise HTTPException(status_code=400, detail="Invalid counterparty type")
    return value


def _serialize_obligation(row: dict[str, Any]) -> dict[str, Any]:
    row["principal_amount"] = int(row.get("principal_amount") or 0)
    row["outstanding_amount"] = int(row.get("outstanding_amount") or 0)
    row["settled_amount"] = int(row.get("settled_amount") or 0)
    return row


def _ensure_account(cur, username: str, account_id: str | None) -> str | None:
    if not account_id:
        return None
    account_id = parse_uuid_value(account_id, "account_id")
    cur.execute(
        "SELECT account_id::text AS account_id FROM accounts WHERE username=%s AND account_id=%s::uuid",
        (username, account_id),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="Account not found")
    return account_id


def _ensure_category(cur, username: str, category_id: str | None) -> str | None:
    if not category_id:
        return None
    category_id = parse_uuid_value(category_id, "category_id")
    cur.execute(
        """
        SELECT category_id::text AS category_id
        FROM categories
        WHERE user_id=(SELECT user_id FROM users WHERE username=%s)
          AND category_id=%s::uuid
        """,
        (username, category_id),
    )
    if not cur.fetchone():
        raise HTTPException(status_code=404, detail="Category not found")
    return category_id


def _ensure_counterparty(cur, username: str, data: dict[str, Any]) -> str | None:
    counterparty_id = data.get("counterparty_id")
    if counterparty_id:
        counterparty_id = parse_uuid_value(counterparty_id, "counterparty_id")
        cur.execute(
            """
            SELECT c.counterparty_id::text AS counterparty_id
            FROM counterparties c
            JOIN users u ON u.user_id=c.user_id
            WHERE u.username=%s AND c.counterparty_id=%s::uuid
            """,
            (username, counterparty_id),
        )
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="Counterparty not found")
        return counterparty_id

    name = (data.get("counterparty_name") or "").strip()
    if not name:
        return None
    cp_type = _parse_counterparty_type(data.get("counterparty_type"))
    notes = (data.get("counterparty_notes") or "").strip() or None
    try:
        cur.execute(
            """
            INSERT INTO counterparties (user_id, name, type, notes)
            SELECT user_id, %s, %s, %s
            FROM users
            WHERE username=%s
            ON CONFLICT (user_id, name)
            DO UPDATE SET type=EXCLUDED.type,
                          notes=COALESCE(counterparties.notes, EXCLUDED.notes),
                          updated_at=now()
            RETURNING counterparty_id::text AS counterparty_id
            """,
            (name, cp_type, notes, username),
        )
    except UniqueViolation:
        cur.execute(
            """
            SELECT c.counterparty_id::text AS counterparty_id
            FROM counterparties c
            JOIN users u ON u.user_id=c.user_id
            WHERE u.username=%s AND lower(c.name)=lower(%s)
            """,
            (username, name),
        )
    row = cur.fetchone()
    return row["counterparty_id"] if row else None


def _obligation_select(where: str) -> str:
    return f"""
        SELECT o.obligation_id::text AS obligation_id,
               o.kind,
               o.title,
               o.description,
               o.principal_amount,
               o.outstanding_amount,
               o.currency,
               o.status,
               o.issue_date::text AS issue_date,
               o.due_date::text AS due_date,
               o.default_account_id::text AS default_account_id,
               a.account_name AS default_account_name,
               o.category_id::text AS category_id,
               cat.name AS category_name,
               o.counterparty_id::text AS counterparty_id,
               c.name AS counterparty_name,
               c.type AS counterparty_type,
               o.notes,
               o.recurrence_frequency,
               o.auto_post_enabled,
               o.auto_post_day,
               o.created_at,
               o.updated_at,
               o.settled_at,
               o.initial_transaction_id::text AS initial_transaction_id,
               COALESCE(SUM(s.amount) FILTER (WHERE s.reversed_at IS NULL), 0) AS settled_amount
        FROM obligations o
        JOIN users u ON u.user_id=o.user_id
        LEFT JOIN counterparties c ON c.counterparty_id=o.counterparty_id
        LEFT JOIN accounts a ON a.account_id=o.default_account_id
        LEFT JOIN categories cat ON cat.category_id=o.category_id
        LEFT JOIN obligation_settlements s ON s.obligation_id=o.obligation_id
        WHERE {where}
        GROUP BY o.obligation_id, a.account_name, cat.name, c.name, c.type
    """


def _fetch_obligation(cur, username: str, obligation_id: str) -> dict[str, Any]:
    cur.execute(
        _obligation_select("u.username=%s AND o.obligation_id=%s::uuid"),
        (username, obligation_id),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Obligation not found")
    return _serialize_obligation(row)


def _recompute_status(cur, obligation_id: str) -> None:
    cur.execute(
        """
        SELECT o.principal_amount,
               COALESCE(SUM(s.amount) FILTER (WHERE s.reversed_at IS NULL), 0) AS settled
        FROM obligations o
        LEFT JOIN obligation_settlements s ON s.obligation_id=o.obligation_id
        WHERE o.obligation_id=%s::uuid
        GROUP BY o.obligation_id
        """,
        (obligation_id,),
    )
    row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Obligation not found")
    principal = int(row["principal_amount"])
    settled = int(row["settled"] or 0)
    outstanding = max(0, principal - settled)
    status = "settled" if outstanding == 0 else "partial" if settled > 0 else "open"
    cur.execute(
        """
        UPDATE obligations
        SET outstanding_amount=%s,
            status=%s,
            settled_at=CASE WHEN %s='settled' THEN COALESCE(settled_at, now()) ELSE NULL END,
            updated_at=now()
        WHERE obligation_id=%s::uuid
        """,
        (outstanding, status, status, obligation_id),
    )


@router.get("/summary")
def obligation_summary(req: Request):
    username = req.state.username
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              COALESCE(SUM(outstanding_amount) FILTER (WHERE kind='receivable' AND status IN ('open','partial')), 0) AS receivable_outstanding,
              COALESCE(SUM(outstanding_amount) FILTER (WHERE kind='payable' AND status IN ('open','partial')), 0) AS payable_outstanding,
              COALESCE(SUM(outstanding_amount) FILTER (WHERE kind='receivable' AND status IN ('open','partial') AND due_date < CURRENT_DATE), 0) AS receivable_overdue,
              COALESCE(SUM(outstanding_amount) FILTER (WHERE kind='payable' AND status IN ('open','partial') AND due_date < CURRENT_DATE), 0) AS payable_overdue,
              COALESCE(SUM(outstanding_amount) FILTER (WHERE status IN ('open','partial') AND due_date >= CURRENT_DATE AND due_date <= CURRENT_DATE + INTERVAL '30 days'), 0) AS due_soon,
              COUNT(*) FILTER (WHERE status IN ('open','partial')) AS open_count
            FROM obligations
            WHERE user_id=(SELECT user_id FROM users WHERE username=%s)
            """,
            (username,),
        )
        row = cur.fetchone() or {}
    receivable = int(row.get("receivable_outstanding") or 0)
    payable = int(row.get("payable_outstanding") or 0)
    return {
        "receivable_outstanding": receivable,
        "payable_outstanding": payable,
        "receivable_overdue": int(row.get("receivable_overdue") or 0),
        "payable_overdue": int(row.get("payable_overdue") or 0),
        "due_soon": int(row.get("due_soon") or 0),
        "open_count": int(row.get("open_count") or 0),
        "net_expected": receivable - payable,
    }


@router.get("/counterparties")
def list_counterparties(req: Request):
    username = req.state.username
    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.counterparty_id::text AS counterparty_id,
                   c.name,
                   c.type,
                   c.notes,
                   c.created_at,
                   c.updated_at
            FROM counterparties c
            JOIN users u ON u.user_id=c.user_id
            WHERE u.username=%s
            ORDER BY c.name
            """,
            (username,),
        )
        return {"counterparties": cur.fetchall()}


@router.get("")
def list_obligations(req: Request, kind: str | None = None, status: str | None = None):
    username = req.state.username
    filters = ["u.username=%s"]
    params: list[Any] = [username]
    if kind:
        filters.append("o.kind=%s")
        params.append(_parse_kind(kind))
    if status and status != "all":
        statuses = [s.strip().lower() for s in status.split(",") if s.strip()]
        invalid = [s for s in statuses if s not in ("open", "partial", "settled", "cancelled", "written_off")]
        if invalid:
            raise HTTPException(status_code=400, detail="Invalid status filter")
        filters.append("o.status = ANY(%s::text[])")
        params.append(statuses or list(OPEN_STATUSES))
    else:
        filters.append("o.status <> 'cancelled'")

    with db_conn() as conn, conn.cursor() as cur:
        cur.execute(
            _obligation_select(" AND ".join(filters)) + " ORDER BY o.status, o.due_date NULLS LAST, o.created_at DESC",
            params,
        )
        obligations = [_serialize_obligation(row) for row in cur.fetchall()]
    return {"obligations": obligations}


@router.post("")
async def create_obligation(req: Request):
    username = req.state.username
    data: dict[str, Any] = await req.json()
    kind = _parse_kind(data.get("kind"))
    title = (data.get("title") or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="title required")
    amount = _parse_int(data.get("principal_amount") or data.get("amount"), "principal_amount")
    if amount <= 0:
        raise HTTPException(status_code=400, detail="principal_amount must be > 0")
    issue_date = _parse_optional_date(data.get("issue_date"), "issue_date") or date.today()
    due_date = _parse_optional_date(data.get("due_date"), "due_date")
    description = (data.get("description") or "").strip() or None
    notes = (data.get("notes") or "").strip() or None
    recurrence = str(data.get("recurrence_frequency") or "none").strip().lower()
    if recurrence not in ("none", "weekly", "monthly", "quarterly", "yearly"):
        raise HTTPException(status_code=400, detail="Invalid recurrence_frequency")
    auto_post = bool(data.get("auto_post_enabled", False))
    auto_day_raw = data.get("auto_post_day")
    auto_day = _parse_int(auto_day_raw, "auto_post_day", default=0) if auto_day_raw not in (None, "") else None
    if auto_day is not None and (auto_day < 1 or auto_day > 31):
        raise HTTPException(status_code=400, detail="auto_post_day must be between 1 and 31")

    with db_conn() as conn, conn.cursor() as cur:
        seed_default_categories(cur, username)
        default_account_id = _ensure_account(cur, username, data.get("default_account_id"))
        category_id = _ensure_category(cur, username, data.get("category_id"))
        counterparty_id = _ensure_counterparty(cur, username, data)
        if not category_id:
            category_id = ensure_named_category(
                cur,
                username,
                name="Receivable Collection" if kind == "receivable" else "Payable Payment",
                kind="income" if kind == "receivable" else "expense",
                icon="in" if kind == "receivable" else "out",
            )

        initial_tx_id = None
        if default_account_id:
            lock_accounts_for_update(cur, username, [default_account_id])
            if issue_date == now_local().date():
                tx_date = now_utc()
            else:
                tx_date = parse_date_utc(issue_date.isoformat(), end_of_day=True)
            tx_type = "credit" if kind == "receivable" else "debit"

            if tx_type == "credit":
                ensure_account_non_negative(
                    cur,
                    default_account_id,
                    tx_date,
                    [{
                        "transaction_id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
                        "date": tx_date,
                        "transaction_type": "credit",
                        "amount": amount,
                    }]
                )

            cp_name = "counterparty"
            if counterparty_id:
                cur.execute("SELECT name FROM counterparties WHERE counterparty_id=%s::uuid", (counterparty_id,))
                row = cur.fetchone()
                if row:
                    cp_name = row["name"]

            tx_name = (
                f"Lent to {cp_name}: {title}"
                if kind == "receivable"
                else f"Borrowed from {cp_name}: {title}"
            )

            initial_cat_id = ensure_named_category(
                cur,
                username,
                name="Receivable Disbursement" if kind == "receivable" else "Payable Receipt",
                kind="expense" if kind == "receivable" else "income",
                icon="out" if kind == "receivable" else "in",
            )

            cur.execute(
                """
                INSERT INTO transactions (
                    account_id, transaction_type, is_cycle_topup, transaction_name,
                    amount, date, is_transfer, category_id, notes, currency, is_reviewed
                )
                VALUES (%s::uuid, %s, false, %s, %s, %s, false, %s::uuid, %s, 'IDR', true)
                RETURNING transaction_id::text AS transaction_id
                """,
                (default_account_id, tx_type, tx_name, amount, tx_date, initial_cat_id, notes),
            )
            initial_tx_id = cur.fetchone()["transaction_id"]

        cur.execute(
            """
            INSERT INTO obligations (
                user_id, kind, counterparty_id, title, description,
                principal_amount, outstanding_amount, currency, issue_date,
                due_date, default_account_id, category_id, notes,
                recurrence_frequency, auto_post_enabled, auto_post_day,
                initial_transaction_id
            )
            SELECT user_id, %s, %s::uuid, %s, %s, %s, %s, 'IDR', %s,
                   %s, %s::uuid, %s::uuid, %s, %s, %s, %s, %s::uuid
            FROM users
            WHERE username=%s
            RETURNING obligation_id::text AS obligation_id
            """,
            (
                kind,
                counterparty_id,
                title,
                description,
                amount,
                amount,
                issue_date,
                due_date,
                default_account_id,
                category_id,
                notes,
                recurrence,
                auto_post,
                auto_day,
                initial_tx_id,
                username,
            ),
        )
        obligation_id = cur.fetchone()["obligation_id"]
        conn.commit()
    return {"ok": True, "obligation_id": obligation_id}


@router.get("/{obligation_id}")
def get_obligation(obligation_id: str, req: Request):
    username = req.state.username
    obligation_id = parse_uuid_value(obligation_id, "obligation_id")
    with db_conn() as conn, conn.cursor() as cur:
        obligation = _fetch_obligation(cur, username, obligation_id)
        cur.execute(
            """
            SELECT s.settlement_id::text AS settlement_id,
                   s.transaction_id::text AS transaction_id,
                   s.account_id::text AS account_id,
                   a.account_name,
                   s.amount,
                   s.settled_at,
                   s.notes,
                   s.reversed_at,
                   s.reversed_by
            FROM obligation_settlements s
            JOIN accounts a ON a.account_id=s.account_id
            WHERE s.obligation_id=%s::uuid
            ORDER BY s.created_at DESC
            """,
            (obligation_id,),
        )
        settlements = cur.fetchall()
        for row in settlements:
            row["amount"] = int(row.get("amount") or 0)
    return {"obligation": obligation, "settlements": settlements}


@router.put("/{obligation_id}")
async def update_obligation(obligation_id: str, req: Request):
    username = req.state.username
    obligation_id = parse_uuid_value(obligation_id, "obligation_id")
    data: dict[str, Any] = await req.json()
    with db_conn() as conn, conn.cursor() as cur:
        current = _fetch_obligation(cur, username, obligation_id)
        if current["status"] in ("cancelled", "written_off"):
            raise HTTPException(status_code=400, detail="Closed obligation cannot be edited")
        kind = _parse_kind(data.get("kind", current["kind"]))
        title = (data.get("title", current["title"]) or "").strip()
        if not title:
            raise HTTPException(status_code=400, detail="title required")
        principal = _parse_int(data.get("principal_amount", current["principal_amount"]), "principal_amount")
        if principal <= 0:
            raise HTTPException(status_code=400, detail="principal_amount must be > 0")
        settled = int(current["settled_amount"])
        if principal < settled:
            raise HTTPException(status_code=400, detail="principal_amount cannot be lower than settled amount")
        outstanding = principal - settled
        status_value = "settled" if outstanding == 0 else "partial" if settled > 0 else "open"
        issue_date = _parse_optional_date(data.get("issue_date", current["issue_date"]), "issue_date") or date.today()
        due_date = _parse_optional_date(data.get("due_date", current["due_date"]), "due_date")
        default_account_id = _ensure_account(cur, username, data.get("default_account_id", current["default_account_id"]))
        category_id = _ensure_category(cur, username, data.get("category_id", current["category_id"]))
        counterparty_id = _ensure_counterparty(cur, username, data)
        if counterparty_id is None and data.get("counterparty_name") is None:
            counterparty_id = current.get("counterparty_id")
        recurrence = str(data.get("recurrence_frequency", current["recurrence_frequency"]) or "none").strip().lower()
        if recurrence not in ("none", "weekly", "monthly", "quarterly", "yearly"):
            raise HTTPException(status_code=400, detail="Invalid recurrence_frequency")
        auto_post = bool(data.get("auto_post_enabled", current["auto_post_enabled"]))
        auto_day_raw = data.get("auto_post_day", current["auto_post_day"])
        auto_day = _parse_int(auto_day_raw, "auto_post_day", default=0) if auto_day_raw not in (None, "") else None
        if auto_day is not None and (auto_day < 1 or auto_day > 31):
            raise HTTPException(status_code=400, detail="auto_post_day must be between 1 and 31")

        # Initial transaction management
        initial_tx_id = current.get("initial_transaction_id")

        if initial_tx_id:
            if not default_account_id:
                old_acc_id = current["default_account_id"]
                if old_acc_id:
                    lock_accounts_for_update(cur, username, [old_acc_id])
                    if current["kind"] == "payable":
                        ensure_account_non_negative(
                            cur,
                            old_acc_id,
                            parse_tx_datetime(current["issue_date"]),
                            exclude_tx_ids=[initial_tx_id]
                        )

                cur.execute(
                    """
                    UPDATE transactions
                    SET deleted_at=%s,
                        deleted_by=%s,
                        delete_reason=%s
                    WHERE transaction_id=%s::uuid AND deleted_at IS NULL
                    RETURNING transaction_id::text AS transaction_id,
                              account_id::text AS account_id,
                              transaction_type,
                              transaction_name,
                              amount,
                              date,
                              is_transfer,
                              is_cycle_topup,
                              transfer_id::text AS transfer_id,
                              deleted_at,
                              deleted_by,
                              delete_reason
                    """,
                    (now_utc(), username, "obligation_initial_tx_removed", initial_tx_id),
                )
                tx_row = cur.fetchone()
                if tx_row:
                    write_transaction_audit(cur, username=username, performed_by=username, action="soft_delete", tx_row=tx_row)

                initial_tx_id = None
            else:
                old_acc_id = current["default_account_id"]
                lock_accounts_for_update(cur, username, [old_acc_id, default_account_id])

                if issue_date == now_local().date():
                    tx_date = now_utc()
                else:
                    tx_date = parse_date_utc(issue_date.isoformat(), end_of_day=True)
                tx_type = "credit" if kind == "receivable" else "debit"

                cp_name = "counterparty"
                if counterparty_id:
                    cur.execute("SELECT name FROM counterparties WHERE counterparty_id=%s::uuid", (counterparty_id,))
                    row = cur.fetchone()
                    if row:
                        cp_name = row["name"]

                tx_name = (
                    f"Lent to {cp_name}: {title}"
                    if kind == "receivable"
                    else f"Borrowed from {cp_name}: {title}"
                )

                initial_cat_id = ensure_named_category(
                    cur,
                    username,
                    name="Receivable Disbursement" if kind == "receivable" else "Payable Receipt",
                    kind="expense" if kind == "receivable" else "income",
                    icon="out" if kind == "receivable" else "in",
                )

                if old_acc_id != default_account_id:
                    if old_acc_id and current["kind"] == "payable":
                        ensure_account_non_negative(
                            cur,
                            old_acc_id,
                            parse_tx_datetime(current["issue_date"]),
                            exclude_tx_ids=[initial_tx_id]
                        )
                    if tx_type == "credit":
                        ensure_account_non_negative(
                            cur,
                            default_account_id,
                            tx_date,
                            new_rows=[{
                                "transaction_id": initial_tx_id,
                                "date": tx_date,
                                "transaction_type": tx_type,
                                "amount": principal,
                            }],
                            exclude_tx_ids=[initial_tx_id]
                        )
                else:
                    ensure_account_non_negative(
                        cur,
                        default_account_id,
                        min(parse_tx_datetime(current["issue_date"]), tx_date),
                        new_rows=[{
                            "transaction_id": initial_tx_id,
                            "date": tx_date,
                            "transaction_type": tx_type,
                            "amount": principal,
                        }],
                        exclude_tx_ids=[initial_tx_id]
                    )

                cur.execute(
                    """
                    UPDATE transactions
                    SET account_id=%s::uuid,
                        transaction_type=%s,
                        transaction_name=%s,
                        amount=%s,
                        date=%s,
                        category_id=%s::uuid,
                        notes=%s,
                        updated_at=now()
                    WHERE transaction_id=%s::uuid
                    """,
                    (
                        default_account_id,
                        tx_type,
                        tx_name,
                        principal,
                        tx_date,
                        initial_cat_id,
                        (data.get("notes", current["notes"]) or "").strip() or None,
                        initial_tx_id,
                    ),
                )
        elif default_account_id:
            lock_accounts_for_update(cur, username, [default_account_id])
            if issue_date == now_local().date():
                tx_date = now_utc()
            else:
                tx_date = parse_date_utc(issue_date.isoformat(), end_of_day=True)
            tx_type = "credit" if kind == "receivable" else "debit"

            if tx_type == "credit":
                ensure_account_non_negative(
                    cur,
                    default_account_id,
                    tx_date,
                    [{
                        "transaction_id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
                        "date": tx_date,
                        "transaction_type": "credit",
                        "amount": principal,
                    }]
                )

            cp_name = "counterparty"
            if counterparty_id:
                cur.execute("SELECT name FROM counterparties WHERE counterparty_id=%s::uuid", (counterparty_id,))
                row = cur.fetchone()
                if row:
                    cp_name = row["name"]

            tx_name = (
                f"Lent to {cp_name}: {title}"
                if kind == "receivable"
                else f"Borrowed from {cp_name}: {title}"
            )

            initial_cat_id = ensure_named_category(
                cur,
                username,
                name="Receivable Disbursement" if kind == "receivable" else "Payable Receipt",
                kind="expense" if kind == "receivable" else "income",
                icon="out" if kind == "receivable" else "in",
            )

            cur.execute(
                """
                INSERT INTO transactions (
                    account_id, transaction_type, is_cycle_topup, transaction_name,
                    amount, date, is_transfer, category_id, notes, currency, is_reviewed
                )
                VALUES (%s::uuid, %s, false, %s, %s, %s, false, %s::uuid, %s, 'IDR', true)
                RETURNING transaction_id::text AS transaction_id
                """,
                (
                    default_account_id,
                    tx_type,
                    tx_name,
                    principal,
                    tx_date,
                    initial_cat_id,
                    (data.get("notes", current["notes"]) or "").strip() or None,
                ),
            )
            initial_tx_id = cur.fetchone()["transaction_id"]

        cur.execute(
            """
            UPDATE obligations
            SET kind=%s,
                counterparty_id=%s::uuid,
                title=%s,
                description=%s,
                principal_amount=%s,
                outstanding_amount=%s,
                status=%s,
                issue_date=%s,
                due_date=%s,
                default_account_id=%s::uuid,
                category_id=%s::uuid,
                notes=%s,
                recurrence_frequency=%s,
                auto_post_enabled=%s,
                auto_post_day=%s,
                settled_at=CASE WHEN %s='settled' THEN COALESCE(settled_at, now()) ELSE NULL END,
                initial_transaction_id=%s::uuid,
                updated_at=now()
            WHERE obligation_id=%s::uuid
            """,
            (
                kind,
                counterparty_id,
                title,
                (data.get("description", current["description"]) or "").strip() or None,
                principal,
                outstanding,
                status_value,
                issue_date,
                due_date,
                default_account_id,
                category_id,
                (data.get("notes", current["notes"]) or "").strip() or None,
                recurrence,
                auto_post,
                auto_day,
                status_value,
                initial_tx_id,
                obligation_id,
            ),
        )
        updated = _fetch_obligation(cur, username, obligation_id)
        conn.commit()
    invalidate_user_cache(username)
    return {"ok": True, "obligation": updated}


@router.post("/{obligation_id}/settlements")
async def create_settlement(obligation_id: str, req: Request):
    username = req.state.username
    obligation_id = parse_uuid_value(obligation_id, "obligation_id")
    data: dict[str, Any] = await req.json()
    amount = _parse_int(data.get("amount"), "amount")
    if amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be > 0")
    settled_at = _parse_settlement_datetime(data.get("settled_at") or data.get("date"))
    notes = (data.get("notes") or "").strip() or None

    with db_conn() as conn, conn.cursor() as cur:
        obligation = _fetch_obligation(cur, username, obligation_id)
        if obligation["status"] not in OPEN_STATUSES:
            raise HTTPException(status_code=400, detail="Obligation is not open")
        if amount > int(obligation["outstanding_amount"]):
            raise HTTPException(status_code=400, detail="amount exceeds outstanding amount")
        account_id = _ensure_account(cur, username, data.get("account_id") or obligation.get("default_account_id"))
        if not account_id:
            raise HTTPException(status_code=400, detail="account_id required")
        lock_accounts_for_update(cur, username, [account_id])
        if obligation["kind"] == "payable":
            ensure_account_non_negative(
                cur,
                account_id,
                settled_at,
                [{
                    "transaction_id": "ffffffff-ffff-ffff-ffff-ffffffffffff",
                    "date": settled_at,
                    "transaction_type": "credit",
                    "amount": amount,
                }],
            )

        cp = obligation.get("counterparty_name") or "counterparty"
        tx_type = "debit" if obligation["kind"] == "receivable" else "credit"
        tx_name = (
            f"Receivable from {cp}: {obligation['title']}"
            if obligation["kind"] == "receivable"
            else f"Payable to {cp}: {obligation['title']}"
        )
        category_id = obligation.get("category_id") or ensure_named_category(
            cur,
            username,
            name="Receivable Collection" if obligation["kind"] == "receivable" else "Payable Payment",
            kind="income" if obligation["kind"] == "receivable" else "expense",
            icon="in" if obligation["kind"] == "receivable" else "out",
        )
        cur.execute(
            """
            INSERT INTO transactions (
                account_id, transaction_type, is_cycle_topup, transaction_name,
                amount, date, is_transfer, category_id, notes, currency, is_reviewed
            )
            VALUES (%s::uuid, %s, false, %s, %s, %s, false, %s::uuid, %s, 'IDR', true)
            RETURNING transaction_id::text AS transaction_id
            """,
            (account_id, tx_type, tx_name, amount, settled_at, category_id, notes),
        )
        tx_id = cur.fetchone()["transaction_id"]
        cur.execute(
            """
            INSERT INTO obligation_settlements (
                obligation_id, user_id, transaction_id, account_id, amount, settled_at, notes
            )
            SELECT %s::uuid, user_id, %s::uuid, %s::uuid, %s, %s, %s
            FROM users
            WHERE username=%s
            RETURNING settlement_id::text AS settlement_id
            """,
            (obligation_id, tx_id, account_id, amount, settled_at, notes, username),
        )
        settlement_id = cur.fetchone()["settlement_id"]
        _recompute_status(cur, obligation_id)
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True, "settlement_id": settlement_id, "transaction_id": tx_id}


@router.delete("/{obligation_id}/settlements/{settlement_id}")
def reverse_settlement(obligation_id: str, settlement_id: str, req: Request):
    username = req.state.username
    obligation_id = parse_uuid_value(obligation_id, "obligation_id")
    settlement_id = parse_uuid_value(settlement_id, "settlement_id")
    with db_conn() as conn, conn.cursor() as cur:
        _fetch_obligation(cur, username, obligation_id)
        cur.execute(
            """
            SELECT s.settlement_id::text AS settlement_id,
                   s.transaction_id::text AS transaction_id,
                   s.account_id::text AS account_id,
                   s.amount,
                   s.reversed_at
            FROM obligation_settlements s
            JOIN users u ON u.user_id=s.user_id
            WHERE u.username=%s
              AND s.obligation_id=%s::uuid
              AND s.settlement_id=%s::uuid
            FOR UPDATE
            """,
            (username, obligation_id, settlement_id),
        )
        settlement = cur.fetchone()
        if not settlement:
            raise HTTPException(status_code=404, detail="Settlement not found")
        if settlement.get("reversed_at"):
            raise HTTPException(status_code=400, detail="Settlement already reversed")
        tx_id = settlement.get("transaction_id")
        if tx_id:
            cur.execute(
                """
                UPDATE transactions t
                SET deleted_at=%s,
                    deleted_by=%s,
                    delete_reason=%s
                FROM accounts a
                WHERE a.account_id=t.account_id
                  AND a.username=%s
                  AND t.transaction_id=%s::uuid
                  AND t.deleted_at IS NULL
                RETURNING t.transaction_id::text AS transaction_id,
                          t.account_id::text AS account_id,
                          t.transaction_type,
                          t.transaction_name,
                          t.amount,
                          t.date,
                          t.is_transfer,
                          t.is_cycle_topup,
                          t.transfer_id::text AS transfer_id,
                          t.deleted_at,
                          t.deleted_by,
                          t.delete_reason
                """,
                (now_utc(), username, "obligation_settlement_reversed", username, tx_id),
            )
            tx_row = cur.fetchone()
            if tx_row:
                write_transaction_audit(
                    cur,
                    username=username,
                    performed_by=username,
                    action="soft_delete",
                    tx_row=tx_row,
                )
        cur.execute(
            """
            UPDATE obligation_settlements
            SET reversed_at=now(),
                reversed_by=%s
            WHERE settlement_id=%s::uuid
            """,
            (username, settlement_id),
        )
        _recompute_status(cur, obligation_id)
        conn.commit()

    invalidate_user_cache(username)
    return {"ok": True}


@router.post("/{obligation_id}/cancel")
def cancel_obligation(obligation_id: str, req: Request):
    username = req.state.username
    obligation_id = parse_uuid_value(obligation_id, "obligation_id")
    with db_conn() as conn, conn.cursor() as cur:
        _fetch_obligation(cur, username, obligation_id)
        cur.execute(
            """
            UPDATE obligations
            SET status='cancelled',
                outstanding_amount=0,
                updated_at=now()
            WHERE obligation_id=%s::uuid
            """,
            (obligation_id,),
        )
        conn.commit()
    invalidate_user_cache(username)
    return {"ok": True}


@router.post("/{obligation_id}/write-off")
def write_off_obligation(obligation_id: str, req: Request):
    username = req.state.username
    obligation_id = parse_uuid_value(obligation_id, "obligation_id")
    with db_conn() as conn, conn.cursor() as cur:
        _fetch_obligation(cur, username, obligation_id)
        cur.execute(
            """
            UPDATE obligations
            SET status='written_off',
                outstanding_amount=0,
                settled_at=now(),
                updated_at=now()
            WHERE obligation_id=%s::uuid
            """,
            (obligation_id,),
        )
        conn.commit()
    invalidate_user_cache(username)
    return {"ok": True}
