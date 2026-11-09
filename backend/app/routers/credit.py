"""Credit bills, the credit account overview and purchase terms (add-credit-spending-flow D1, D4, D12).

Every read computes the account's schedule from its ledger (`services/credit_bills.py`); nothing is stored
but the purchase terms. Every endpoint is owner-scoped: another owner's account or transaction is a 404.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from app.core.config import settings
from app.db.pool import db_conn
from app.errors import ErrorCode, api_error
from app.i18n import language_for_user
from app.services import credit_bills
from app.services.auth import get_current_user
from app.services.credit_figures import paid_to_credit_by_account, spent_on_credit_by_account
from app.services.credit_terms import Terms, is_credit_purchase, lock_purchase, set_purchase_terms
from app.services.reimbursements import load_spending_offsets
from app.services.report_windows import PERIOD_PATTERN, get_timeframe_window

router = APIRouter(tags=["Credit"])

STATUS_PATTERN = "^({0})(,({0}))*$".format("|".join(credit_bills.BILL_STATUSES))


class CreditTermsRequest(BaseModel):
    # The range is checked by the service so the refusal carries `credit_terms_count_range`.
    installment_count: int = 1
    first_due_date: date | None = None


def _period(user: dict[str, Any], start: date | None, end: date | None) -> tuple[datetime, datetime, date, date]:
    """The requested local days as an inclusive instant window, or the owner's current period."""
    if start is None and end is None:
        start_utc, end_utc, start_day, end_day, _ = get_timeframe_window(
            int(user.get("payday_day") or 25), "cycle", user_id=str(user["id"]), language=language_for_user(user),
        )
        return start_utc, end_utc, start_day, end_day
    zone = ZoneInfo(settings.tz)
    today = credit_bills.local_today()
    start = start or end
    end = end or today
    if start > end:
        raise api_error(422, ErrorCode.CREDIT_PERIOD_INVALID)
    end_instant = datetime.combine(end + timedelta(days=1), time.min, zone) - timedelta(microseconds=1)
    return datetime.combine(start, time.min, zone), end_instant, start, end


def _schedule_or_refuse(cur, user: dict[str, Any], account_id: str) -> credit_bills.Schedule:
    cur.execute("SELECT type FROM accounts WHERE user_id = %s AND id = %s", (str(user["id"]), account_id))
    account = cur.fetchone()
    if account is None:
        raise api_error(404, ErrorCode.ACCOUNT_NOT_FOUND)
    if account["type"] != credit_bills.CREDIT_TYPE:
        raise api_error(422, ErrorCode.CREDIT_ACCOUNT_REQUIRED)
    schedules = credit_bills.load_schedules(cur, str(user["id"]), credit_bills.local_today(), [account_id], include_archived=True)
    return schedules[account_id]


def _list_window(
    user: dict[str, Any], period: str | None, period_offset: int, from_instant: datetime | None, to_instant: datetime | None,
) -> tuple[datetime, datetime, date, date] | None:
    """The ledger list's own window (apply-markas-cleanup D9): a period resolved as `GET /api/transactions` resolves
    it, or the exact instants the list sends. None when the request names neither form."""
    zone = ZoneInfo(settings.tz)
    if period:
        start, end, start_day, end_day, _ = get_timeframe_window(
            int(user.get("payday_day") or 25), period, cycle_offset=period_offset, user_id=str(user["id"]),
            language=language_for_user(user),
        )
        return start, end, start_day, end_day
    if from_instant is None and to_instant is None:
        return None
    if from_instant is None or to_instant is None or from_instant > to_instant:
        raise api_error(422, ErrorCode.CREDIT_PERIOD_INVALID)
    return from_instant, to_instant, from_instant.astimezone(zone).date(), to_instant.astimezone(zone).date()


@router.get("/credit/accounts/{account_id}")
def credit_account_overview(
    account_id: UUID,
    from_date: date | None = Query(default=None, alias="from"),
    to_date: date | None = Query(default=None, alias="to"),
    period: str | None = Query(default=None, pattern=PERIOD_PATTERN),
    period_offset: int = Query(default=0, ge=-36, le=12),
    from_instant: datetime | None = Query(default=None, alias="from_date"),
    to_instant: datetime | None = Query(default=None, alias="to_date"),
    current_user: dict = Depends(get_current_user),
):
    """What one credit account owes, its bills with their items, and its totals for a period (H5, H8).

    The period is local days (`from`, `to`), or the ledger list's window (`period` with `period_offset`, or the
    instants `from_date` and `to_date`) so the Kredit header counts exactly the rows the list holds (D9).
    """
    user_id = str(current_user["id"])
    aid = str(account_id)
    if (period and (from_date or to_date or from_instant or to_instant)) or ((from_date or to_date) and (from_instant or to_instant)):
        raise api_error(422, ErrorCode.PERIOD_WITH_DATES)
    window = _list_window(current_user, period, period_offset, from_instant, to_instant)
    start, end, start_day, end_day = window or _period(current_user, from_date, to_date)
    with db_conn() as conn, conn.cursor() as cur:
        schedule = _schedule_or_refuse(cur, current_user, aid)
        offsets = load_spending_offsets(cur, user_id, start, end)
        spent = spent_on_credit_by_account(cur, user_id, start, end, offsets).get(aid, 0)
        paid = paid_to_credit_by_account(cur, user_id, start, end).get(aid, 0)

    def in_period(day: date | None) -> bool:
        return day is not None and start_day <= day <= end_day

    settled = [item for item in schedule.items if in_period(item.settled_on)]
    totals = {
        "spent_on_credit": spent,
        "fees": sum(item.amount for item in schedule.items if item.kind == credit_bills.FEE and in_period(item.date)),
        "refunds": sum(credit.amount for credit in schedule.credits
                       if credit.kind == credit_bills.REFUND and in_period(credit.date)),
        "paid_to_credit": paid,
        "items_settled": len(settled),
        "items_settled_on_time": sum(1 for item in settled if item.settled_on_time),
    }
    return {
        "ok": True,
        "account": credit_bills.account_summary(schedule),
        "due_dates_unavailable": not schedule.due_dates_available,
        "next_due": credit_bills.next_due_summary(schedule),
        "bills": [bill.as_dict() for bill in schedule.bills],
        "period": {"from": start_day.isoformat(), "to": end_day.isoformat()},
        "totals": totals,
    }


@router.get("/credit/bills")
def list_credit_bills(
    from_date: date | None = Query(default=None, alias="from"),
    to_date: date | None = Query(default=None, alias="to"),
    status: str | None = Query(default=None, pattern=STATUS_PATTERN),
    current_user: dict = Depends(get_current_user),
):
    """Bills across the owner's active credit accounts by due date, without items (H9)."""
    if from_date and to_date and from_date > to_date:
        raise api_error(422, ErrorCode.CREDIT_PERIOD_INVALID)
    wanted = set(status.split(",")) if status else None
    with db_conn() as conn, conn.cursor() as cur:
        schedules = credit_bills.load_schedules(cur, str(current_user["id"]), credit_bills.local_today())
    bills = []
    for account_id, schedule in schedules.items():
        for bill in schedule.bills:
            if from_date and bill.due_date < from_date or to_date and bill.due_date > to_date:
                continue
            if wanted is not None and bill.status not in wanted:
                continue
            bills.append({"account_id": account_id, "account_name": schedule.account["name"], **bill.as_dict(with_items=False)})
    bills.sort(key=lambda bill: (bill["due_date"], bill["account_name"], bill["account_id"]))
    unavailable = [
        {"account_id": account_id, "account_name": schedule.account["name"]}
        for account_id, schedule in schedules.items() if not schedule.due_dates_available
    ]
    return {"ok": True, "bills": bills, "due_dates_unavailable": unavailable}


def _terms_response(cur, user: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    user_id = str(user["id"])
    transaction_id = str(row["id"])
    cur.execute(
        "SELECT installment_count, first_due_date, first_due_date_set_by FROM credit_purchase_terms WHERE transaction_id = %s",
        (transaction_id,),
    )
    stored = cur.fetchone()
    terms = Terms(int(stored["installment_count"]), stored["first_due_date"], stored["first_due_date_set_by"]) if stored else Terms()
    account_id = str(row["account_id"])
    schedule = credit_bills.load_schedules(cur, user_id, credit_bills.local_today(), [account_id], include_archived=True)[account_id]
    return {
        "ok": True,
        "transaction_id": transaction_id,
        "credit_terms": {
            "installment_count": terms.installment_count,
            "first_due_date": terms.first_due_date.isoformat() if terms.first_due_date else None,
            "first_due_date_set_by": terms.first_due_date_set_by,
        },
        "due_dates_unavailable": not schedule.due_dates_available,
        "installments": credit_bills.purchase_installments(schedule, transaction_id),
        **{key: value for key, value in credit_bills.purchase_facts(schedule, transaction_id, terms.installment_count).items()
           if key != "due_dates_unavailable"},
    }


def _locked_purchase(cur, user: dict[str, Any], transaction_id: str) -> dict[str, Any]:
    row = lock_purchase(cur, str(user["id"]), transaction_id)
    if row is None:
        raise api_error(404, ErrorCode.TRANSACTION_NOT_FOUND)
    return row


@router.put("/transactions/{transaction_id}/credit-terms")
def put_credit_terms(transaction_id: UUID, payload: CreditTermsRequest, current_user: dict = Depends(get_current_user)):
    """Set or change a credit purchase's instalments and first due date; no figure changes (D3)."""
    with db_conn() as conn, conn.cursor() as cur:
        row = _locked_purchase(cur, current_user, str(transaction_id))
        set_purchase_terms(cur, str(current_user["id"]), row, payload.installment_count, payload.first_due_date)
        response = _terms_response(cur, current_user, row)
        conn.commit()
    return response


@router.delete("/transactions/{transaction_id}/credit-terms")
def delete_credit_terms(transaction_id: UUID, current_user: dict = Depends(get_current_user)):
    """Return a credit purchase to paid in full with its derived due date."""
    with db_conn() as conn, conn.cursor() as cur:
        row = _locked_purchase(cur, current_user, str(transaction_id))
        if not is_credit_purchase(row):
            raise api_error(422, ErrorCode.CREDIT_TERMS_NOT_CREDIT_PURCHASE)
        cur.execute("DELETE FROM credit_purchase_terms WHERE transaction_id = %s AND user_id = %s",
                    (str(transaction_id), str(current_user["id"])))
        response = _terms_response(cur, current_user, row)
        conn.commit()
    return response
