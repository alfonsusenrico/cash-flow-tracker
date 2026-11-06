"""The Beranda cycle summary: every figure of the current period with the lines that make it up.

The period is the open salary cycle, else the current calendar month. Every figure reuses the
dashboard's report rules (report window, reportable SQL, Kakeibo breakdown, reimbursement offsets,
the Tinjau badge rules), so a Beranda number always equals the same number on Analisis and Transaksi.
Lines add up exactly to their figure, amounts are integer rupiah, and nothing is fabricated: a figure
that cannot be computed is reported unavailable with a reason.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from app.core.config import settings
from app.routers.accounts import summarize_account_balances
from app.services.credit_accounts import credit_owed, is_credit
from app.services.kakeibo import UNCLASSIFIED
from app.services.observed_insights import comparison_windows, period_comparison
from app.services.plan_occurrences import unpaid_plan_lines
from app.services.reimbursements import load_spending_offsets
from app.services.report_figures import reportable_income_sql
from app.services.report_windows import (
    MONTH_NAMES_ID,
    get_previous_timeframe_window,
    get_timeframe_window,
    short_local_date,
)
from app.services.salary_cycles import ExpectedSalary, expected_next_salary, resolve_cycle
from app.services.salary_receipts import get_source, receipts
from app.services.waiting_rows import fetch_waiting_rows, waiting_summary

SAFE_TO_SPEND_ROUNDING = 1000
# Same horizon as the overview's upcoming commitments when no salary date can be expected.
SCHEDULE_FALLBACK_DAYS = 30
# Fewer compared days than this make a comparison chip misleading (finance-dashboard-cockpit).
MIN_COMPARED_DAYS = 3

KELUAR_BUCKETS: tuple[tuple[str, str, str], ...] = (
    ("need", "Kebutuhan", "need_spent"),
    ("want", "Keinginan", "want_spent"),
    ("culture", "Budaya", "culture_spent"),
    ("unexpected", "Tak Terduga", "unexpected_spent"),
    ("saving", "Tabungan", "saving_spent"),
    (UNCLASSIFIED, "Belum diklasifikasi", "unclassified_spent"),
)

KakeiboBreakdown = Callable[..., dict[str, Any]]

# Where a line's label comes from (add-language-system D7): a name the owner gave, shown as stored, or fixed
# wording the web renders from the line's key in the interface language. `label` itself stays Indonesian.
USER_LABEL = "user"
SYSTEM_LABEL = "system"


def rupiah(amount: int) -> str:
    return "Rp " + f"{amount:,}".replace(",", ".")


def ledger_href(period: str, **filters: str) -> str:
    query = "&".join(f"{key}={value}" for key, value in {"period": period, **filters}.items())
    return f"/ledger?{query}"


def expected_salary_for_cycle(conn, user_id: str, cycle_open: bool, receipt_rows: list[dict], now: datetime) -> ExpectedSalary:
    if not cycle_open:
        return ExpectedSalary("unknown", reason="no_cycle")
    source = get_source(conn, user_id)
    if source is None:
        return ExpectedSalary("unknown", reason="no_salary_source")
    # Same normalisation as the source matcher in salary candidates: collapsed whitespace, lower case.
    wanted = " ".join(source["value"].split()).lower()
    from_source = [
        row["date"] for row in receipt_rows
        if " ".join((row[source["field"]] or "").split()).lower() == wanted
    ]
    return expected_next_salary(from_source, tz_name=settings.tz, as_of=now)


def _masuk(cur, user_id: str, start: datetime, end: datetime, period: str) -> dict[str, Any]:
    """Reportable income by category: the overview's `total_inflow`, reimbursements excluded."""
    cur.execute(
        f"""
        SELECT c.id, c.name, COALESCE(SUM(t.amount), 0) AS amount
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        WHERE t.user_id = %s AND {reportable_income_sql()} AND t.date >= %s AND t.date <= %s
        GROUP BY c.id, c.name
        ORDER BY amount DESC, c.name NULLS LAST
        """,
        (user_id, start, end),
    )
    lines = []
    for row in cur.fetchall():
        key = str(row["id"]) if row["id"] else "uncategorized"
        lines.append({
            "key": key,
            "label": row["name"] or "Tanpa kategori",
            "label_source": USER_LABEL if row["id"] else SYSTEM_LABEL,
            "amount": int(row["amount"]),
            "href": ledger_href(period, type="income", category=key),
        })
    return {"total": sum(line["amount"] for line in lines), "lines": lines}


def _keluar(kakeibo: dict[str, Any], offsets, period: str) -> dict[str, Any]:
    lines = []
    for key, label, field in KELUAR_BUCKETS:
        # `note_codes` carries the same facts as `note`, in the same order, for the web to render (D7).
        notes = []
        note_codes = []
        if key == "saving":
            notes.append("Termasuk uang yang dipindah ke kantong tabungan atau diinvestasikan")
            note_codes.append({"code": "includes_savings_moves", "params": {}})
        offset = offsets.for_pillar(key)
        if offset > 0:
            notes.append(f"Sudah dikurangi ganti uang {rupiah(offset)}")
            note_codes.append({"code": "reduced_by_reimbursements", "params": {"amount": int(offset)}})
        lines.append({
            "key": key,
            "label": label,
            "label_source": SYSTEM_LABEL,
            "amount": int(kakeibo[field]),
            "note": ". ".join(notes) if notes else None,
            "note_codes": note_codes,
            "href": ledger_href(period, kakeibo=key),
        })
    total = int(kakeibo["total_allocated"])
    if sum(line["amount"] for line in lines) != total:
        raise RuntimeError("Kakeibo buckets do not add up to their total")
    return {"total": total, "lines": lines, "offsets_total": offsets.applied}


def spending_split(keluar: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Keluar split into Pengeluaran (every bucket but saving) and Ditabung & investasi (the saving bucket).

    The saving bucket may be negative when more left savings than came in (organize-accounts-by-use D4).
    The two add up to `keluar.total` exactly, so Beranda's segments and the Kakeibo card stay one identity
    (refine-workspace-layout D4). Analisis uses the same split for its Pengeluaran explanation.
    """
    spending_lines = [line for line in keluar["lines"] if line["key"] != "saving"]
    saving_lines = [line for line in keluar["lines"] if line["key"] == "saving"]
    spending = {"amount": sum(line["amount"] for line in spending_lines), "lines": spending_lines}
    fresh_saving = {"amount": sum(line["amount"] for line in saving_lines), "lines": saving_lines}
    if spending["amount"] + fresh_saving["amount"] != keluar["total"]:
        raise RuntimeError("Pengeluaran and saving do not add up to Keluar")
    return spending, fresh_saving


def savings_change(kakeibo: dict[str, Any]) -> dict[str, int]:
    """The parts of the savings change ("Tabungan bertambah/berkurang"), for Beranda's lines (D4).

    `total` is the Kakeibo saving bucket: `masuk - diambil - belanja + pilar_tabungan`, where `masuk` is
    moved into savings, `diambil` moved out of savings, `belanja` spending paid from a savings account and
    `pilar_tabungan` expenses whose pillar is saving. `diambil` and `belanja` are positive amounts, and
    `total` may be negative.
    """
    parts = {
        "total": int(kakeibo["saving_spent"]),
        "masuk": int(kakeibo["saving_into"]),
        "diambil": int(kakeibo["saving_out"]),
        "belanja": int(kakeibo["saving_paid_from_savings"]),
        "pilar_tabungan": int(kakeibo["saving_expenses"]),
    }
    if parts["masuk"] - parts["diambil"] - parts["belanja"] + parts["pilar_tabungan"] != parts["total"]:
        raise RuntimeError("Savings change parts do not add up to the saving bucket")
    return parts


def _months_left(start: date, target: date) -> int:
    return max(1, (target.year - start.year) * 12 + (target.month - start.month))


def _goal_lines(
    cur, user_id: str, accounts: list[dict[str, Any]], start: datetime, end: datetime,
    period_start: date, today: date,
) -> list[dict]:
    """The unfunded part of each dated goal's even monthly set-aside for this period.

    `funded` is this period's money into the goal: inbound movement legs into its accounts (or their
    pockets) from outside them, plus goal-linked expenses that are not movement legs. The set-aside is
    paced on the amount still missing at the period start, so funding it does not shrink it.
    """
    cur.execute(
        """
        SELECT g.id, g.name, g.target_amount, g.current_amount, g.target_date,
               COALESCE(array_agg(ga.account_id) FILTER (WHERE ga.account_id IS NOT NULL), '{}') AS account_ids
        FROM goals g
        LEFT JOIN goal_accounts ga ON ga.goal_id = g.id
        WHERE g.user_id = %s AND g.is_archived = FALSE AND g.target_date > %s
        GROUP BY g.id
        ORDER BY g.target_date, g.name
        """,
        (user_id, today),
    )
    goals = cur.fetchall()
    balances = {str(account["id"]): int(account["balance"]) for account in accounts}
    parents = {str(account["id"]): account.get("parent_id") for account in accounts}
    goal_accounts: dict[str, set[str]] = {}
    for goal in goals:
        linked = {str(account_id) for account_id in goal["account_ids"]}
        pockets = {account_id for account_id, parent in parents.items() if parent and parent in linked}
        goal_accounts[str(goal["id"])] = linked | pockets
    watched = sorted(set().union(*goal_accounts.values())) if goal_accounts else []

    cur.execute(
        """
        SELECT inbound.account_id AS target_account_id, outbound.account_id AS source_account_id, inbound.amount
        FROM transactions inbound
        JOIN transactions outbound
          ON outbound.user_id = inbound.user_id AND outbound.movement_id = inbound.movement_id
         AND outbound.movement_role = 'outbound'
        WHERE inbound.user_id = %s AND inbound.movement_role = 'inbound'
          AND inbound.date >= %s AND inbound.date <= %s AND inbound.account_id = ANY(%s::uuid[])
        """,
        (user_id, start, end, watched),
    )
    inbound = cur.fetchall()
    cur.execute(
        """
        SELECT t.goal_id, COALESCE(SUM(t.amount), 0) AS amount FROM transactions t
        WHERE t.user_id = %s AND t.goal_id IS NOT NULL AND t.type = 'expense' AND t.movement_id IS NULL
          AND t.date >= %s AND t.date <= %s
        GROUP BY t.goal_id
        """,
        (user_id, start, end),
    )
    linked_spending = {str(row["goal_id"]): int(row["amount"]) for row in cur.fetchall()}

    lines = []
    for goal in goals:
        goal_id = str(goal["id"])
        members = goal_accounts[goal_id]
        linked = {str(account_id) for account_id in goal["account_ids"]}
        funded = linked_spending.get(goal_id, 0) + sum(
            int(row["amount"]) for row in inbound
            if str(row["target_account_id"]) in members and str(row["source_account_id"]) not in members
        )
        if linked:
            # A linked parent already contains its pockets, so a linked pocket under it is not added again.
            current = sum(balances.get(account_id, 0) for account_id in linked if parents.get(account_id) not in linked)
        else:
            current = int(goal["current_amount"])
        remaining_at_start = max(0, int(goal["target_amount"]) - max(0, current - funded))
        if remaining_at_start <= 0:
            continue
        months_left = _months_left(period_start, goal["target_date"])
        set_aside = -(-remaining_at_start // months_left)
        lines.append({
            "kind": "goal",
            "goal_id": goal_id,
            "label": goal["name"],
            "label_source": USER_LABEL,
            "target_date": goal["target_date"].isoformat(),
            "months_left": months_left,
            "set_aside": set_aside,
            "funded": funded,
            "amount": max(0, set_aside - funded),
        })
    return lines


def net_worth_breakdown(cur, user_id: str, accounts: list[dict[str, Any]]) -> dict[str, Any]:
    """Assets less active debts, each account counted once (a parent's balance already holds its pockets).

    Assets are Dana likuid, Dana investasi and Investasi; Dana investasi is left out of Dana likuid, so
    Saldo tersedia and Ketahanan Dana's fallback never count it.

    The one net worth rule: Beranda (`/dashboard/cycle-summary`) and Rekening (`/dashboard/net-worth`) both use it.
    """
    balances = summarize_account_balances(accounts)
    cur.execute(
        """
        SELECT id, name, remaining_amount FROM obligations
        WHERE user_id = %s AND is_archived = FALSE AND remaining_amount > 0
        ORDER BY remaining_amount DESC, name
        """,
        (user_id,),
    )
    debts = [{"id": str(row["id"]), "name": row["name"], "amount": int(row["remaining_amount"])} for row in cur.fetchall()]
    lines = [{
        "key": "liquid", "label": "Dana likuid", "label_source": SYSTEM_LABEL, "amount": balances["liquid_balance"],
    }]
    if balances["dana_investasi"]:
        # Cash kept for buying investments is an asset of its own, outside Dana likuid (D2).
        lines.append({
            "key": "dana_investasi", "label": "Dana investasi", "label_source": SYSTEM_LABEL,
            "amount": balances["dana_investasi"],
        })
    lines.append({
        "key": "investment", "label": "Investasi", "label_source": SYSTEM_LABEL,
        "amount": balances["investment_balance"],
    })
    if balances["reconciliation_pending"]:
        # Negative balances waiting for reconciliation are kept out of liquid funds but still owed.
        lines.append({
            "key": "reconciliation_pending", "label": "Saldo minus perlu dicocokkan", "label_source": SYSTEM_LABEL,
            "amount": -balances["reconciliation_pending"],
        })
    lines += [
        {"key": f"debt:{debt['id']}", "label": debt["name"], "label_source": USER_LABEL, "amount": -debt["amount"]}
        for debt in debts
    ]
    # What credit accounts owe is utang next to the debts (add-credit-accounts D4).
    credit_lines = [
        {
            "key": f"credit:{account['id']}", "label": account["name"], "label_source": USER_LABEL,
            "amount": -credit_owed(account["own_balance"]),
        }
        for account in accounts
        if is_credit(account) and credit_owed(account["own_balance"]) > 0
    ]
    lines += credit_lines
    return {
        "total": sum(line["amount"] for line in lines),
        "liquid": balances["liquid_balance"],
        "dana_investasi": balances["dana_investasi"],
        "investment": balances["investment_balance"],
        "reconciliation_pending": balances["reconciliation_pending"],
        "debts": sum(debt["amount"] for debt in debts),
        "credit_owed": balances["credit_owed"],
        "lines": lines,
    }


def _comparison(
    cur, user_id: str, kakeibo_breakdown: KakeiboBreakdown,
    current: tuple[datetime, datetime, str], previous: tuple[datetime, datetime, str] | None,
) -> dict[str, Any]:
    """The fair period comparison of `/dashboard/period-comparison`, with Keluar as the Kakeibo total.

    Keluar includes net fresh saving like `keluar.total`; "belanja" is the comparison's outflow, which
    leaves saving moves out, for the insight sentence.
    """
    compared = period_comparison(cur, user_id, current=current, previous=previous)
    current_window, previous_window = comparison_windows(current, previous)

    def keluar(window: tuple[datetime, datetime], inflow: int) -> int:
        offsets = load_spending_offsets(cur, user_id, *window)
        return int(kakeibo_breakdown(cur, user_id, *window, inflow, offsets)["total_allocated"])

    now_figures = compared["current"]
    figures = {
        "keluar": keluar(current_window, now_figures["inflow"]),
        "masuk": now_figures["inflow"],
        "belanja": now_figures["outflow"],
    }
    if compared["compared_days"] < MIN_COMPARED_DAYS:
        status = "too_short"
    elif previous_window is None or not compared["previous_has_data"]:
        status = "no_previous"
    else:
        status = "available"
    result: dict[str, Any] = {
        "status": status,
        "compared_days": compared["compared_days"],
        "label": compared["label"],
        "previous_label": compared["previous_label"],
    }
    if status != "available":
        return {**result, **{name: {"current": value, "previous": None, "change": None} for name, value in figures.items()}}
    before = compared["previous"]
    previous_figures = {
        "keluar": keluar(previous_window, before["inflow"]),
        "masuk": before["inflow"],
        "belanja": before["outflow"],
    }
    return {
        **result,
        **{
            name: {"current": value, "previous": previous_figures[name], "change": value - previous_figures[name]}
            for name, value in figures.items()
        },
    }


def _safe_to_spend(basis: str, expected: ExpectedSalary, remaining: int, today: date) -> dict[str, Any]:
    over_by = -remaining if remaining < 0 else None
    unavailable = {"status": "unavailable", "per_day": None, "days_left": None,
                   "rounding": SAFE_TO_SPEND_ROUNDING, "over_by": over_by}
    if basis != "cycle":
        return {**unavailable, "reason": "no_cycle", "detail": None}
    if expected.date is None:
        return {**unavailable, "reason": "no_expected_salary", "detail": expected.reason}
    # Today counts; the payday itself does not.
    days_left = (expected.date - today).days
    if expected.status == "overdue" or days_left < 1:
        return {**unavailable, "reason": "salary_overdue", "detail": None}
    per_day = max(0, remaining) // days_left // SAFE_TO_SPEND_ROUNDING * SAFE_TO_SPEND_ROUNDING
    return {
        "status": "available", "reason": None, "detail": None, "per_day": per_day, "days_left": days_left,
        "rounding": SAFE_TO_SPEND_ROUNDING, "over_by": over_by,
    }


def build_cycle_summary(
    conn, user: dict[str, Any], *, accounts: list[dict[str, Any]], kakeibo_breakdown: KakeiboBreakdown,
    now: datetime,
) -> dict[str, Any]:
    """Every Beranda figure for the owner's current period, read through one connection.

    `accounts` is the owner's non-archived accounts with balances, and `kakeibo_breakdown` the
    dashboard's breakdown, passed in so this service does not import the dashboard router.
    """
    user_id = str(user["id"])
    payday_day = int(user.get("payday_day") or 25)
    zone = ZoneInfo(settings.tz)
    today = now.astimezone(zone).date()

    receipt_rows = receipts(conn, user_id)
    cycle = resolve_cycle([row["date"] for row in receipt_rows], tz_name=settings.tz, as_of=now)
    basis = "cycle" if cycle.status == "open" else "month"
    start, end, start_date, end_date, window_label = get_timeframe_window(
        payday_day, basis, user_id=user_id, cycle=cycle,
    )
    previous_start, previous_end, _, _, previous_label = get_previous_timeframe_window(
        payday_day, basis, start_date, end_date, 0, user_id=user_id,
    )
    previous = None if previous_end < previous_start else (previous_start, previous_end, previous_label)
    expected = expected_salary_for_cycle(conn, user_id, basis == "cycle", receipt_rows, now)

    with conn.cursor() as cur:
        masuk = _masuk(cur, user_id, start, end, basis)
        offsets = load_spending_offsets(cur, user_id, start, end)
        kakeibo = kakeibo_breakdown(cur, user_id, start, end, masuk["total"], offsets)
        keluar = _keluar(kakeibo, offsets, basis)
        waiting = waiting_summary(fetch_waiting_rows(cur, user_id, start, end))

        if expected.status == "estimated" and expected.date > today:
            window_end, window_basis = expected.date - timedelta(days=1), "expected_salary"
        else:
            window_end, window_basis = today + timedelta(days=SCHEDULE_FALLBACK_DAYS), "next_30_days"
        plan_lines = [
            {**line, "label_source": USER_LABEL}
            for line in unpaid_plan_lines(cur, user_id, payday_day, start_date, window_end, today)
        ]
        scheduled_lines = plan_lines + _goal_lines(cur, user_id, accounts, start, end, start_date, today)
        net_worth = net_worth_breakdown(cur, user_id, accounts)
        comparison = _comparison(cur, user_id, kakeibo_breakdown, (start, end, window_label), previous)

    spending, fresh_saving = spending_split(keluar)
    tabungan = savings_change(kakeibo)
    if tabungan["total"] != fresh_saving["amount"]:
        raise RuntimeError("Savings change does not match the saving bucket of Keluar")
    scheduled_total = sum(line["amount"] for line in scheduled_lines)
    remaining_lines = [
        {"key": "masuk", "label": "Masuk", "label_source": SYSTEM_LABEL, "amount": masuk["total"]},
        {"key": "keluar", "label": "Keluar", "label_source": SYSTEM_LABEL, "amount": -keluar["total"]},
        {"key": "scheduled", "label": "Terjadwal", "label_source": SYSTEM_LABEL, "amount": -scheduled_total},
    ]
    remaining = sum(line["amount"] for line in remaining_lines)

    if basis == "cycle":
        label = f"Siklus sejak {short_local_date(start_date)}"
    else:
        label = f"{MONTH_NAMES_ID[start_date.month - 1]} {start_date.year}"
    return {
        "as_of": now.astimezone(zone).isoformat(),
        "period": {
            "basis": basis,
            "status": "open",
            "cycle_status": cycle.status,
            "start_date": start_date.isoformat(),
            "today": today.isoformat(),
            "day_index": (today - start_date).days + 1,
            "label": label,
            "cycle_offset_basis": basis,
        },
        "expected_salary": {
            "status": expected.status,
            "date": expected.date.isoformat() if expected.date else None,
            "basis": "regular_receipts" if expected.date else None,
            "receipt_dates": [day.isoformat() for day in expected.receipt_dates],
            "reason": expected.reason,
        },
        "keluar": keluar,
        "spending": spending,
        "fresh_saving": fresh_saving,
        "tabungan": tabungan,
        "masuk": masuk,
        "waiting": waiting,
        "scheduled": {
            "total": scheduled_total,
            # The bills part alone, for Target & Tagihan's list (redesign-money-and-settings D4).
            "plans_total": sum(line["amount"] for line in scheduled_lines if line["kind"] == "plan"),
            "window_start": start_date.isoformat(),
            "window_end": window_end.isoformat(),
            "window_basis": window_basis,
            "lines": scheduled_lines,
        },
        "remaining": {"total": remaining, "lines": remaining_lines},
        "safe_to_spend": _safe_to_spend(basis, expected, remaining, today),
        "net_worth": net_worth,
        "comparison": comparison,
    }
