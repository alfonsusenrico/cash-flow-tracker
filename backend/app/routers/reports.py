"""Laporan Bulanan routes: the report, its period list, the readiness API and the reflection note."""

from __future__ import annotations

from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from app.core.config import settings
from app.db.pool import db_conn
from app.routers.accounts import get_accounts_with_balances, summarize_account_balances
from app.routers.dashboard import calculate_ketahanan_dana, get_kakeibo_breakdown
from app.services.auth import get_current_user
from app.services.essential_cost import DEFAULT_TARGET_MONTHS, estimate_essential_cost
from app.services.monthly_comparison import build_comparison
from app.services.monthly_report import (
    BASIS_PATTERN,
    ReportError,
    build_report,
    delete_reflection,
    describe_period,
    latest_readiness,
    list_periods,
    local_start,
    recurring_in_period,
    reflection_dates,
    resolve_period,
    save_reflection,
)

router = APIRouter(prefix="/reports/monthly", tags=["Reports"])

REFLECTION_MAX_LENGTH = 1000


class ReflectionInput(BaseModel):
    basis: str = Field(pattern=BASIS_PATTERN)
    start_date: date
    note: str = Field(max_length=REFLECTION_MAX_LENGTH)

    @field_validator("note")
    @classmethod
    def note_has_text(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("Catatan tidak boleh kosong.")
        return stripped


def report_error(error: ReportError) -> HTTPException:
    return HTTPException(status_code=error.status, detail={"code": error.code, "message": error.message})


def _essential_now(cur, user: dict) -> dict:
    """The current essential cost and coverage, as Beranda shows them; labelled "saat ini" in the report."""
    accounts = get_accounts_with_balances(user["id"], include_archived=False)
    liquid = summarize_account_balances(accounts)["liquid_balance"]
    essential = estimate_essential_cost(cur, user["id"])
    multiplier = int(user.get("emergency_fund_multiplier") or DEFAULT_TARGET_MONTHS)
    coverage = calculate_ketahanan_dana(cur, user["id"], liquid, accounts, target_multiplier=multiplier, essential=essential)
    return {
        "as_of": "now",
        "amount": essential.amount,
        "living_amount": essential.living_amount,
        "debt_minimum": essential.debt_minimum,
        "method": essential.method,
        "basis": essential.describe()["basis"],
        "period_count": len(essential.periods),
        "limited_history": essential.limited_history,
        "coverage_months": coverage["coverage_months"],
        "target_months": coverage["target_months"],
        "status": coverage["status"],
        "balance_basis": coverage["balance_basis"],
        "coverage_unavailable_reason": coverage["coverage_unavailable_reason"],
    }


@router.get("/periods")
def get_report_periods(
    basis: str = Query(default="month", pattern=BASIS_PATTERN),
    current_user: dict = Depends(get_current_user),
):
    """The period in progress and up to 24 closed periods of the basis, newest first."""
    now = datetime.now(timezone.utc)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            listing = list_periods(cur, current_user["id"], basis, now=now)
            noted = reflection_dates(cur, current_user["id"], basis)
    return {
        "ok": True,
        "timezone": settings.tz,
        "basis": basis,
        "cycle_available": listing["cycle_available"],
        "cycle_status": listing["cycle_status"],
        "first_recorded_date": listing["first_day"].isoformat() if listing["first_day"] else None,
        "periods": [
            {**describe_period(period, basis, listing["first_day"]), "has_reflection": local_start(period) in noted}
            for period in listing["periods"]
        ],
    }


@router.get("/latest")
def get_latest_report(
    basis: str | None = Query(default=None, pattern=BASIS_PATTERN),
    current_user: dict = Depends(get_current_user),
):
    """Whether the latest closed period's report is ready, for the phone's monthly reminder."""
    now = datetime.now(timezone.utc)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            readiness = latest_readiness(cur, current_user["id"], basis, now=now)
    return {"ok": True, "timezone": settings.tz, **readiness}


@router.get("")
def get_monthly_report(
    basis: str = Query(default="month", pattern=BASIS_PATTERN),
    start_date: date | None = None,
    current_user: dict = Depends(get_current_user),
):
    """One period's report; without `start_date` the latest closed period, else the one in progress."""
    now = datetime.now(timezone.utc)
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            listing = list_periods(cur, user_id, basis, now=now)
            if listing["first_day"] is None:
                return {"ok": True, "timezone": settings.tz, "basis": basis, "empty": True,
                        "cycle_available": listing["cycle_available"]}
            try:
                period = resolve_period(listing, basis, start_date)
            except ReportError as error:
                raise report_error(error) from None
            report = build_report(
                cur, user_id, basis=basis, period=period, listing=listing,
                kakeibo_breakdown=get_kakeibo_breakdown,
                overall_budget=current_user.get("monthly_spending_budget"),
                essential=_essential_now(cur, current_user), now=now,
            )
    return {"ok": True, "timezone": settings.tz, "empty": False, "cycle_available": listing["cycle_available"], **report}


@router.get("/comparison")
def get_monthly_comparison(
    basis: str = Query(default="month", pattern=BASIS_PATTERN),
    start_date: date | None = None,
    current_user: dict = Depends(get_current_user),
):
    """The whole-period comparison of one period, the same object as the report's `comparison`."""
    now = datetime.now(timezone.utc)
    user_id = current_user["id"]
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            listing = list_periods(cur, user_id, basis, now=now)
            if listing["first_day"] is None:
                return {"ok": True, "timezone": settings.tz, "basis": basis, "empty": True,
                        "cycle_available": listing["cycle_available"]}
            try:
                period = resolve_period(listing, basis, start_date)
            except ReportError as error:
                raise report_error(error) from None
            comparison = build_comparison(
                cur, user_id, basis=basis, period=period, listing=listing, now=now,
                kakeibo_breakdown=get_kakeibo_breakdown, recurring_in_period=recurring_in_period,
            )
    return {"ok": True, "timezone": settings.tz, "empty": False, "cycle_available": listing["cycle_available"],
            "comparison": comparison}


@router.put("/reflection")
def put_reflection(payload: ReflectionInput, current_user: dict = Depends(get_current_user)):
    """Save the owner's note for a listed period ("Apa yang mau diperbaiki bulan depan?")."""
    now = datetime.now(timezone.utc)
    with db_conn() as conn:
        with conn.cursor() as cur:
            listing = list_periods(cur, current_user["id"], payload.basis, now=now)
            try:
                period = resolve_period(listing, payload.basis, payload.start_date)
            except ReportError as error:
                raise report_error(error) from None
            reflection = save_reflection(cur, current_user["id"], payload.basis, period, payload.note)
        conn.commit()
    return {"ok": True, "reflection": reflection}


@router.delete("/reflection")
def remove_reflection(
    basis: str = Query(pattern=BASIS_PATTERN),
    start_date: date = Query(),
    current_user: dict = Depends(get_current_user),
):
    with db_conn() as conn:
        with conn.cursor() as cur:
            removed = delete_reflection(cur, current_user["id"], basis, start_date)
        conn.commit()
    if not removed:
        raise HTTPException(status_code=404, detail={"code": "reflection_not_found", "message": "Catatan tidak ditemukan."})
    return {"ok": True}
