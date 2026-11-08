from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, File, Query, UploadFile
from pydantic import BaseModel, Field, field_validator

from app.core.config import settings
from app.db.pool import db_conn
from app.errors import ErrorCode, api_error
from app.i18n import language_for_user, notice, t
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
from app.services import credit_bills
from app.services.credit_accounts import ensure_not_credit, is_credit, over_limit, refuse
from app.services.credit_terms import is_credit_purchase, lock_purchase, set_purchase_terms, terms_after_edit
from app.services.entity_names import resolve_account_name, resolve_category_name
from app.services.kakeibo import PILLAR_FILTER_PATTERN, PILLAR_PATTERN, effective_pillar, effective_pillar_sql, pillar_bucket_sql
from app.services.report_figures import cash_flow_totals
from app.services.ledger_mutations import (
    delete_transaction_rows,
    ensure_sufficient_funds,
    get_locked_ledger_balance,
    lock_owned_accounts,
)
from app.services.merchant_memory import record_owner_corrections, snapshot_rows
from app.services.monthly_report import waiting_row_sql
from app.services.plan_occurrences import request_plan_linking
from app.services.provenance import (
    CLASSIFICATION_FIELDS,
    OWNER_EDIT_ASSIGNMENTS,
    OWNER_REVIEW_STATUS_PATTERN,
    REVIEW_STATUS_PATTERN,
    SOURCE_PATTERN,
    normalize_merchant,
    owner_provenance,
    system_provenance,
)
from app.services.receipts import (
    build_receipt_relative_path,
    prepare_receipt_payload,
    remove_receipt_file,
    store_receipt,
)
from app.services.reimbursements import (
    category_offsets_spending,
    load_spending_offsets,
    offset_not_allowed,
    validate_offset_category,
)
from app.services.report_figures import possible_duplicate_sql, reportable_expense_sql, reportable_income_sql
from app.services.report_windows import PERIOD_PATTERN, get_timeframe_window
from app.services.saving_flow import counts_as_saving_sql, credit_row_sql, row_saving_flow_sql, saving_flow_joins
from app.services.system_categories import (
    INTERNAL_MOVEMENT, INVESTMENT, is_system_sql, non_reportable_sql, reportable_key_sql,
)

router = APIRouter(tags=["Transactions"])


class TransactionClassificationInput(BaseModel):
    merchant: str | None = Field(default=None, max_length=500)
    review_status: str | None = Field(default=None, pattern=OWNER_REVIEW_STATUS_PATTERN)
    offset_category_id: UUID | None = None

    @field_validator("merchant")
    @classmethod
    def normalize_merchant_field(cls, value: str | None) -> str | None:
        return normalize_merchant(value)


def provenance_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Provenance and reimbursement fields shared by the ledger list and detail responses."""
    confidence = row.get("classification_confidence")
    return {
        "source": row["source"],
        "merchant": row.get("merchant"),
        "review_status": row["review_status"],
        "classified_by": row.get("classified_by"),
        "classification_confidence": float(confidence) if confidence is not None else None,
        "owner_edited_at": row["owner_edited_at"].isoformat() if row.get("owner_edited_at") else None,
        "is_reimbursement": bool(row.get("is_reimbursement")),
        "offset_category_id": str(row["offset_category_id"]) if row.get("offset_category_id") else None,
        "offset_category_name": row.get("offset_category_name"),
        "habit_basis": habit_basis_fields(row.get("habit_basis")),
    }


def habit_basis_fields(basis: dict | None) -> dict[str, int] | None:
    """How many earlier owner answers a habit-recorded row followed (learn-habits-to-ask-less D10)."""
    if not basis:
        return None
    return {"agreeing": int(basis["agreeing"]), "support": int(basis["support"])}


# A movement's fee row and, for a fee row, the movement it was charged on (movement pairing D5).
FEE_LINK_JOINS = """
    LEFT JOIN transactions fee
        ON t.movement_id IS NOT NULL AND fee.user_id = t.user_id AND fee.fee_for_movement_id = t.movement_id
    LEFT JOIN LATERAL (
        SELECT MAX(CASE WHEN leg.type = 'expense' THEN leg_account.name END) AS source_account_name,
               MAX(CASE WHEN leg.type = 'income' THEN leg_account.name END) AS target_account_name,
               MAX(leg.amount) AS amount
        FROM transactions leg JOIN accounts leg_account ON leg_account.id = leg.account_id
        WHERE t.fee_for_movement_id IS NOT NULL AND leg.user_id = t.user_id
          AND leg.movement_id = t.fee_for_movement_id
        HAVING COUNT(*) > 0
    ) fee_movement ON TRUE
"""


def fee_link_fields(row: dict[str, Any]) -> dict[str, Any]:
    movement_fee = None
    if row.get("movement_fee_id"):
        movement_fee = {"transaction_id": str(row["movement_fee_id"]), "amount": int(row["movement_fee_amount"])}
    fee_movement = None
    if row.get("fee_for_movement_id") and row.get("fee_movement_amount") is not None:
        fee_movement = {
            "movement_id": str(row["fee_for_movement_id"]),
            "source_account_name": row.get("fee_source_account_name"),
            "target_account_name": row.get("fee_target_account_name"),
            "amount": int(row["fee_movement_amount"]),
        }
    return {"movement_fee": movement_fee, "fee_movement": fee_movement}


EARLIEST = datetime(1970, 1, 1, tzinfo=timezone.utc)
LATEST = datetime(9999, 12, 31, tzinfo=timezone.utc)


def aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def later_bound(bound: datetime, other: datetime | None) -> datetime:
    return max(bound, aware(other)) if other else bound


def earlier_bound(bound: datetime, other: datetime | None) -> datetime:
    return min(bound, aware(other)) if other else bound


def ledger_cash_flow(
    cur, user_id: str, inflow: int, gross_outflow: int, gross_saving: int,
    start: datetime | None, end: datetime | None, offset_scope: dict[str, str | None] | None,
) -> dict[str, int]:
    """Ledger summary figures; outflow is net of reimbursements the way report figures are.

    `offset_scope` is None when other filters make the rows differ from any report figure; the
    outflow then stays gross and the offset is reported as 0.

    Outflow is split into `spending` (Pengeluaran) and `saving` (saving-pillar expenses), which add up
    to it exactly (refine-workspace-layout D4).
    """
    offset = 0
    saving_offset = 0
    start, end = aware(start) if start else EARLIEST, aware(end) if end else LATEST
    if offset_scope is not None and start <= end:
        offsets = load_spending_offsets(cur, user_id, start, end)
        if offset_scope["category_id"] and offset_scope["pillar"]:
            offset = offsets.for_category_pillar(offset_scope["category_id"], offset_scope["pillar"])
            saving_offset = offset if offset_scope["pillar"] == "saving" else 0
        elif offset_scope["category_id"]:
            offset = offsets.for_category(offset_scope["category_id"])
            # A category's rows are all saving or none; per-row pillar overrides leave the offset with spending.
            saving_offset = offset if gross_saving == gross_outflow else 0
        elif offset_scope["pillar"]:
            offset = offsets.for_pillar(offset_scope["pillar"])
            saving_offset = offset if offset_scope["pillar"] == "saving" else 0
        else:
            offset = offsets.applied
            saving_offset = offsets.for_pillar("saving")
    totals = cash_flow_totals(inflow, gross_outflow, offset)
    saving = min(max(0, gross_saving - saving_offset), totals["outflow"])
    spending = totals["outflow"] - saving
    # `spending_net` is Transaksi's "Bersih": money in less Pengeluaran, so saving is not counted as spent.
    return {**totals, "spending": spending, "saving": saving, "spending_net": inflow - spending}


PROVENANCE_SELECT = """
    t.source, t.merchant, t.review_status, t.classified_by, t.classification_confidence,
    t.owner_edited_at, t.offset_category_id, offset_category.name AS offset_category_name,
    COALESCE(c.offsets_spending, FALSE) AND t.type = 'income' AS is_reimbursement,
    CASE WHEN t.classified_by = 'habit' THEN (
        SELECT habit_event.interpretation -> 'habit' FROM notification_events habit_event
        WHERE habit_event.user_id = t.user_id AND habit_event.transaction_id = t.id
        ORDER BY habit_event.post_time, habit_event.id LIMIT 1
    ) END AS habit_basis
"""


LIST_INCLUDES = ("daily_totals", "type_counts")

# Movement legs of an investment top-up belong to the Investasi tab, never to Pindah saldo.
TOPUP_LEG_SQL = "EXISTS (SELECT 1 FROM investment_topups it WHERE it.id = t.movement_id AND it.user_id = t.user_id)"
TYPE_FILTER_SQL = {
    "transfer": f"(t.type = 'transfer' OR {non_reportable_sql('c')} OR c.is_excluded_from_budget = true)",
    "expense": f"t.type = 'expense' AND {reportable_key_sql('c')} AND COALESCE(c.is_excluded_from_budget, false) = false",
    "income": f"t.type = 'income' AND {reportable_key_sql('c')} AND COALESCE(c.is_excluded_from_budget, false) = false",
    "movement": (
        f"((t.type = 'transfer' OR {is_system_sql('c', INTERNAL_MOVEMENT)} OR c.is_excluded_from_budget = true)"
        f" AND NOT {is_system_sql('c', INVESTMENT)} AND NOT {TOPUP_LEG_SQL})"
    ),
    "investment": f"({TOPUP_LEG_SQL} OR {is_system_sql('c', INVESTMENT)})",
    # Rows whose saving flow changes Beranda's "Ditabung & investasi" (apply-markas-transaksi D28).
    "saving": counts_as_saving_sql("t", "c"),
    "credit": credit_row_sql(),
}
TAB_KEYS = ("expense", "income", "movement", "investment", "saving", "credit")
# Tabs that list one row per movement under `logical_movements`, as Pindah saldo does.
LOGICAL_MOVEMENT_TYPES = (None, "movement", "investment", "saving", "credit")
# Tabs whose filter needs `saving_flow_joins` in the FROM clause.
FLOW_JOIN_TYPES = ("saving", "credit")


def ledger_saving_net(cur, user_id: str, start: datetime | None, end: datetime | None) -> int:
    """Beranda's "Ditabung & investasi" over the window, from the function its figure comes from (D28).

    Without a period or range the window spans every row. The Kakeibo inflow argument only scales its
    percentages, so the saving total does not depend on it.
    """
    # Imported here: the dashboard router and the cycle summary import this router through the review queue.
    from app.routers.dashboard import get_kakeibo_breakdown
    from app.services.cycle_summary import savings_change

    start, end = aware(start) if start else EARLIEST, aware(end) if end else LATEST
    offsets = load_spending_offsets(cur, user_id, start, end)
    return savings_change(get_kakeibo_breakdown(cur, user_id, start, end, 0, offsets))["total"]


def parse_includes(value: str | None) -> set[str]:
    """`include=daily_totals,type_counts`; an unknown part is a 422 rather than silently ignored."""
    if not value:
        return set()
    parts = {part.strip() for part in value.split(",") if part.strip()}
    unknown = sorted(parts - set(LIST_INCLUDES))
    if unknown:
        raise api_error(422, ErrorCode.UNKNOWN_INCLUDE, extra={"values": unknown})
    return parts


def local_day_bounds(days: list[date]) -> tuple[datetime, datetime]:
    """The instants that contain the given local days, so the per-day aggregate can use the date index."""
    zone = ZoneInfo(settings.tz)
    return datetime.combine(min(days), time.min, zone), datetime.combine(max(days) + timedelta(days=1), time.min, zone)


class CreditTermsInput(BaseModel):
    # The range is checked by the service so the refusal carries `credit_terms_count_range`.
    installment_count: int = 1
    first_due_date: date | None = None


class TransactionCreate(TransactionClassificationInput):
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
    kakeibo_type: str | None = Field(default=None, pattern=PILLAR_PATTERN)
    investment_action: str | None = Field(default=None, pattern="^(buy|sell)$")
    units: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    price_per_unit: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    # Terms of a purchase on a credit account (add-credit-spending-flow D1); omitted means paid in full.
    credit_terms: CreditTermsInput | None = None


class TransactionUpdate(TransactionClassificationInput):
    account_id: UUID | None = None
    category_id: UUID | None = None
    type: str | None = Field(default=None, pattern="^(expense|income)$")
    goal_id: UUID | None = None
    obligation_id: UUID | None = None
    obligation_allocations: list[DebtAllocation] | None = None
    amount: int | None = Field(default=None, gt=0)
    notes: str | None = Field(default=None, max_length=500)
    date: datetime | None = None
    kakeibo_type: str | None = Field(default=None, pattern=PILLAR_PATTERN)


@router.get("")
def list_transactions(
    account_id: UUID | None = None,
    category_id: UUID | None = None,
    goal_id: UUID | None = None,
    obligation_id: UUID | None = None,
    type: str | None = Query(default=None, pattern="^(expense|income|transfer|movement|investment|saving|credit)$"),
    kakeibo_type: str | None = Query(default=None, pattern=PILLAR_FILTER_PATTERN),
    source: str | None = Query(default=None, pattern=SOURCE_PATTERN),
    review_status: str | None = Query(default=None, pattern=REVIEW_STATUS_PATTERN),
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    period: str | None = Query(default=None, pattern=PERIOD_PATTERN),
    period_offset: int = Query(default=0, ge=-36, le=12),
    uncategorized: bool = False,
    possible_duplicates: bool = False,
    q: str | None = None,
    logical_movements: bool = False,
    waiting: bool = False,
    include: str | None = Query(default=None, max_length=100),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    current_user: dict = Depends(get_current_user),
):
    user_id = current_user["id"]
    includes = parse_includes(include)
    payday_day = current_user.get("payday_day") or 25
    if period and (from_date or to_date):
        raise api_error(422, ErrorCode.PERIOD_WITH_DATES)
    if uncategorized and category_id:
        raise api_error(422, ErrorCode.UNCATEGORIZED_WITH_CATEGORY)
    period_label = None
    if period:
        from_date, to_date, _, _, period_label = get_timeframe_window(payday_day, period, cycle_offset=period_offset, user_id=user_id, language=language_for_user(current_user))
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
            if uncategorized:
                conditions.append("t.category_id IS NULL")
            if goal_id:
                conditions.append("t.goal_id = %s")
                params.append(str(goal_id))
            if obligation_id:
                conditions.append("(t.obligation_id = %s OR EXISTS (SELECT 1 FROM transaction_obligation_allocations a WHERE a.transaction_id = t.id AND a.obligation_id = %s))")
                params.extend([str(obligation_id), str(obligation_id)])
            if waiting:
                conditions.append(waiting_row_sql("t"))
            if kakeibo_type and isinstance(kakeibo_type, str):
                # "unclassified" selects expenses with neither a row nor a category pillar.
                conditions.append(f"{pillar_bucket_sql()} = %s")
                params.append(kakeibo_type)
            if possible_duplicates:
                conditions.append(possible_duplicate_sql("t"))
            if source:
                conditions.append("t.source = %s")
                params.append(source)
            if review_status:
                conditions.append("t.review_status = %s")
                params.append(review_status)
            if from_date:
                conditions.append("t.date >= %s")
                params.append(from_date)
            if to_date:
                conditions.append("t.date <= %s")
                params.append(to_date)
            if q:
                search_pattern = f"%{q.strip()}%"
                conditions.append("(t.notes ILIKE %s OR t.merchant ILIKE %s OR c.name ILIKE %s OR g.name ILIKE %s OR o.name ILIKE %s OR EXISTS (SELECT 1 FROM transaction_obligation_allocations a JOIN obligations debt ON debt.id = a.obligation_id WHERE a.transaction_id = t.id AND debt.name ILIKE %s))")
                params.extend([search_pattern] * 6)

            # Filtered views retain individual legs: either side may match independently. A period or date
            # window and the Pindah saldo, Investasi, Tabungan & investasi and Kredit tabs keep one row per
            # movement (its outbound leg, or the only leg), so Transaksi's tab and day counts match its rows.
            if logical_movements and not any((
                account_id, category_id, goal_id, obligation_id, type not in LOGICAL_MOVEMENT_TYPES,
                kakeibo_type, source, review_status, q, uncategorized, possible_duplicates, waiting,
            )):
                conditions.append(
                    """(
                        t.movement_role IS DISTINCT FROM 'inbound'
                        OR NOT EXISTS (
                            SELECT 1 FROM transactions outbound
                            WHERE outbound.user_id = t.user_id
                              AND outbound.movement_id = t.movement_id
                              AND outbound.movement_role = 'outbound'
                        )
                    )"""
                )

            # Every filter but the type tab: what the tab counts are computed over.
            tab_where_clause = " AND ".join(conditions)
            if type:
                conditions.append(TYPE_FILTER_SQL[type])
            where_clause = " AND ".join(conditions)
            # The saving and credit filters read both legs of a movement; other filters need no extra joins.
            flow_joins = saving_flow_joins("t") if type in FLOW_JOIN_TYPES else ""

            # Combined total count and cash flow summary query
            cycle_start, cycle_end, _, _, cycle_label = get_timeframe_window(payday_day, "cycle", user_id=user_id, language=language_for_user(current_user))

            summary_query = f"""
                SELECT
                    COUNT(*) AS total,
                    COALESCE(SUM(CASE WHEN {reportable_income_sql()} THEN t.amount ELSE 0 END), 0) AS cumulative_inflow,
                    COALESCE(SUM(CASE WHEN {reportable_expense_sql()} THEN t.amount ELSE 0 END), 0) AS cumulative_outflow,
                    COALESCE(SUM(CASE WHEN {reportable_income_sql()} AND t.date >= %s AND t.date <= %s
                        THEN t.amount ELSE 0 END), 0) AS cycle_inflow,
                    COALESCE(SUM(CASE WHEN {reportable_expense_sql()} AND t.date >= %s AND t.date <= %s
                        THEN t.amount ELSE 0 END), 0) AS cycle_outflow,
                    COALESCE(SUM(CASE WHEN {reportable_expense_sql()} AND {effective_pillar_sql()} = 'saving'
                        THEN t.amount ELSE 0 END), 0) AS cumulative_saving,
                    COALESCE(SUM(CASE WHEN {reportable_expense_sql()} AND {effective_pillar_sql()} = 'saving'
                        AND t.date >= %s AND t.date <= %s THEN t.amount ELSE 0 END), 0) AS cycle_saving
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                LEFT JOIN goals g ON g.id = t.goal_id
                LEFT JOIN obligations o ON o.id = t.obligation_id
                {flow_joins}
                WHERE {where_clause}
            """
            cur.execute(summary_query, [cycle_start, cycle_end, cycle_start, cycle_end, cycle_start, cycle_end] + params)
            agg = cur.fetchone()
            total = agg["total"]
            # Offsets apply where the filtered rows are exactly what a report figure covers.
            offset_scope = None
            if not any((account_id, goal_id, obligation_id, source, review_status, q, uncategorized, possible_duplicates, waiting)) \
                    and type in (None, "expense"):
                offset_scope = {"category_id": str(category_id) if category_id else None, "pillar": kakeibo_type}
            cumulative = ledger_cash_flow(
                cur, user_id, int(agg["cumulative_inflow"] or 0), int(agg["cumulative_outflow"] or 0),
                int(agg["cumulative_saving"] or 0), from_date, to_date, offset_scope,
            )
            cycle = ledger_cash_flow(
                cur, user_id, int(agg["cycle_inflow"] or 0), int(agg["cycle_outflow"] or 0),
                int(agg["cycle_saving"] or 0), later_bound(cycle_start, from_date), earlier_bound(cycle_end, to_date),
                offset_scope,
            )
            # Beranda's figure has no per-filter form, so it is reported only for the whole window (D28).
            saving_net = None
            if type == "saving" and not any((
                account_id, category_id, goal_id, obligation_id, kakeibo_type, source, review_status, q,
                uncategorized, possible_duplicates, waiting,
            )):
                saving_net = ledger_saving_net(cur, user_id, from_date, to_date)
            cumulative = {**cumulative, "saving_net": saving_net}

            # Select page
            select_query = f"""
                SELECT
                    t.id,
                    t.account_id,
                    sa.name AS account_name,
                    sa.type AS account_type,
                    sa_parent.name AS account_parent_name,
                    t.category_id,
                    c.name AS category_name,
                    c.system_key AS category_system_key,
                    c.icon AS category_icon,
                    c.color AS category_color,
                    COALESCE(c.is_excluded_from_budget, false) AS is_excluded_from_budget,
                    t.goal_id,
                    g.name AS goal_name,
                    t.obligation_id,
                    o.name AS obligation_name,
                    t.recurring_rule_id,
                    t.type,
                    {effective_pillar_sql()} AS kakeibo_type,
                    t.amount,
                    t.notes,
                    t.date,
                    t.receipt_path,
                    t.movement_id,
                    t.movement_role,
                    t.fee_for_movement_id,
                    fee.id AS movement_fee_id,
                    fee.amount AS movement_fee_amount,
                    fee_movement.source_account_name AS fee_source_account_name,
                    fee_movement.target_account_name AS fee_target_account_name,
                    fee_movement.amount AS fee_movement_amount,
                    topup.id AS investment_topup_id,
                    partner.id AS partner_id,
                    partner.account_id AS movement_target_account_id,
                    partner_account.name AS movement_target_account_name,
                    partner_account.type AS movement_target_account_type,
                    partner_parent.name AS movement_target_account_parent_name,
                    credit_terms.installment_count AS credit_installment_count,
                    {row_saving_flow_sql("t")} AS saving_flow,
                    t.created_at,
                    (t.date AT TIME ZONE %s)::date AS local_date,
                    {PROVENANCE_SELECT}
                FROM transactions t
                JOIN accounts sa ON sa.id = t.account_id
                LEFT JOIN categories offset_category ON offset_category.id = t.offset_category_id
                LEFT JOIN transactions partner
                    ON partner.movement_id = t.movement_id
                   AND partner.user_id = t.user_id
                   AND partner.id != t.id
                LEFT JOIN accounts partner_account ON partner_account.id = partner.account_id
                LEFT JOIN accounts sa_parent ON sa_parent.id = sa.parent_id
                LEFT JOIN accounts partner_parent ON partner_parent.id = partner_account.parent_id
                {FEE_LINK_JOINS}
                LEFT JOIN investment_topups topup ON topup.id = t.movement_id AND topup.user_id = t.user_id
                LEFT JOIN credit_purchase_terms credit_terms ON credit_terms.transaction_id = t.id
                LEFT JOIN categories c ON c.id = t.category_id
                LEFT JOIN goals g ON g.id = t.goal_id
                LEFT JOIN obligations o ON o.id = t.obligation_id
                {flow_joins}
                WHERE {where_clause}
                ORDER BY t.date DESC, t.created_at DESC, t.id DESC
                LIMIT %s OFFSET %s
            """
            cur.execute(select_query, [settings.tz] + params + [limit, offset])
            rows = cur.fetchall()
            allocation_breakdowns = load_allocation_breakdowns(cur, rows)
            credit_blocks = load_credit_blocks(cur, user_id, rows)
            daily_totals = None
            if "daily_totals" in includes:
                daily_totals = load_daily_totals(
                    cur, where_clause, params, sorted({r["local_date"] for r in rows}), flow_joins,
                )
            type_counts = None
            if "type_counts" in includes:
                type_counts = load_type_counts(cur, tab_where_clause, params)

    extras: dict[str, Any] = {}
    if daily_totals is not None:
        extras["daily_totals"] = daily_totals
    if type_counts is not None:
        extras["type_counts"] = type_counts
    return {
        "ok": True,
        "total": total,
        "limit": limit,
        "offset": offset,
        **extras,
        "summary": {
            "cycle": {"start": cycle_start.isoformat(), "end": cycle_end.isoformat(), "label": cycle_label, **cycle},
            "cumulative": cumulative,
            "period": {
                "basis": period,
                "offset": period_offset if period else None,
                "label": period_label,
                "start": from_date.isoformat() if from_date else None,
                "end": to_date.isoformat() if to_date else None,
                **cumulative,
            } if (period or from_date or to_date) else None,
        },
        "transactions": [
            {
                "id": str(r["id"]),
                "account_id": str(r["account_id"]),
                "account_name": r["account_name"],
                "account_parent_name": r.get("account_parent_name"),
                "category_id": str(r["category_id"]) if r["category_id"] else None,
                "category_name": r["category_name"],
                "category_system_key": r["category_system_key"],
                "category_icon": r["category_icon"],
                "category_color": r["category_color"],
                "is_excluded_from_budget": bool(r.get("is_excluded_from_budget", False)),
                "goal_id": str(r["goal_id"]) if r["goal_id"] else None,
                "goal_name": r["goal_name"],
                "obligation_id": str(r["obligation_id"]) if r["obligation_id"] else None,
                "obligation_name": r["obligation_name"],
                "obligation_allocations": allocation_breakdowns[str(r["id"])],
                "recurring_rule_id": str(r["recurring_rule_id"]) if r["recurring_rule_id"] else None,
                "type": r["type"],
                "kakeibo_type": r.get("kakeibo_type"),
                "movement_id": str(r["movement_id"]) if r.get("movement_id") else None,
                "investment_topup_id": str(r["investment_topup_id"]) if r.get("investment_topup_id") else None,
                "movement_kind": "investment_topup" if r.get("investment_topup_id") else None,
                "movement_role": r.get("movement_role"),
                "fee_for_movement_id": str(r["fee_for_movement_id"]) if r.get("fee_for_movement_id") else None,
                **fee_link_fields(r),
                "partner_id": str(r["partner_id"]) if r.get("partner_id") else None,
                "transfer_target_account_id": str(r["movement_target_account_id"]) if r.get("movement_target_account_id") else None,
                "transfer_target_account_name": r.get("movement_target_account_name"),
                "transfer_target_account_parent_name": r.get("movement_target_account_parent_name"),
                "amount": r["amount"],
                "notes": r["notes"],
                "date": r["date"].isoformat() if r["date"] else None,
                "local_date": r["local_date"].isoformat(),
                "receipt_path": r["receipt_path"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                **provenance_fields(r),
                "credit": credit_blocks[str(r["id"])],
                "saving_flow": r["saving_flow"],
            }
            for r in rows
        ],
    }


def credit_side(row: dict[str, Any]) -> tuple[str, str, str] | None:
    """The row's role toward a credit account and that account's id and name, or None (D12, H5)."""
    if row["account_type"] == "credit":
        role = credit_bills.row_role({
            "type": row["type"], "movement_id": row["movement_id"],
            "fee_for_movement_id": row["fee_for_movement_id"], "system_key": row["category_system_key"],
        })
        return role, str(row["account_id"]), row["account_name"]
    if row["movement_id"] and row.get("movement_target_account_type") == "credit":
        # The cash leg of a credit movement: paying into it, or a cash advance out of it.
        role = credit_bills.PAYMENT if row["movement_role"] == "outbound" else credit_bills.CASH_ADVANCE
        return role, str(row["movement_target_account_id"]), row["movement_target_account_name"]
    return None


def load_credit_blocks(
    cur, user_id: str, rows: list[dict[str, Any]], *, with_installments: bool = False,
) -> dict[str, dict[str, Any] | None]:
    """Each row's `credit` block; schedules are built once per credit account on the page."""
    sides = {str(row["id"]): credit_side(row) for row in rows}
    purchase_accounts = sorted({side[1] for side in sides.values() if side and side[0] == credit_bills.PURCHASE})
    schedules = credit_bills.load_schedules(
        cur, user_id, credit_bills.local_today(), purchase_accounts, include_archived=True,
    ) if purchase_accounts else {}
    blocks: dict[str, dict[str, Any] | None] = {}
    for row in rows:
        side = sides[str(row["id"])]
        if side is None:
            blocks[str(row["id"])] = None
            continue
        role, account_id, account_name = side
        block: dict[str, Any] = {"role": role, "account_id": account_id, "account_name": account_name}
        if role == credit_bills.PURCHASE:
            schedule = schedules.get(account_id)
            block.update(credit_bills.purchase_facts(schedule, str(row["id"]), int(row["credit_installment_count"] or 1)))
            if with_installments:
                block["installments"] = credit_bills.purchase_installments(schedule, str(row["id"]))
        blocks[str(row["id"])] = block
    return blocks


LIST_FROM_SQL = """
    FROM transactions t
    LEFT JOIN categories c ON c.id = t.category_id
    LEFT JOIN goals g ON g.id = t.goal_id
    LEFT JOIN obligations o ON o.id = t.obligation_id
"""


def load_daily_totals(
    cur, where_clause: str, params: list[Any], days: list[date], flow_joins: str = "",
) -> list[dict[str, Any]]:
    """Figures for each local day the page touches, over every row of that day that matches the filters.

    Inflow and outflow use the summary's gross rules, so over all pages they add up to the summary's
    inflow and gross outflow; `spending` is the outflow without saving-pillar expenses. A movement counts once (its legs share the movement id), and waiting rows
    are counted like any row and also reported on their own.
    """
    if not days:
        return []
    start, end = local_day_bounds(days)
    cur.execute(
        f"""
        WITH matching AS (
            SELECT t.id, t.amount, t.movement_id, (t.date AT TIME ZONE %s)::date AS day,
                   {reportable_income_sql()} AS is_inflow, {reportable_expense_sql()} AS is_outflow,
                   {reportable_expense_sql()} AND {effective_pillar_sql()} IS DISTINCT FROM 'saving' AS is_spending,
                   {waiting_row_sql("t")} AS is_waiting
            {LIST_FROM_SQL}
            {flow_joins}
            WHERE {where_clause} AND t.date >= %s AND t.date < %s
        ), movements AS (
            SELECT day, movement_id, MAX(amount) AS amount FROM matching
            WHERE movement_id IS NOT NULL GROUP BY day, movement_id
        )
        SELECT m.day, COUNT(*) AS count,
               COALESCE(SUM(m.amount) FILTER (WHERE m.is_inflow), 0) AS inflow,
               COALESCE(SUM(m.amount) FILTER (WHERE m.is_outflow), 0) AS outflow,
               COALESCE(SUM(m.amount) FILTER (WHERE m.is_spending), 0) AS spending,
               COALESCE((SELECT SUM(mv.amount) FROM movements mv WHERE mv.day = m.day), 0) AS movements,
               COUNT(*) FILTER (WHERE m.is_waiting) AS waiting_count,
               COALESCE(SUM(m.amount) FILTER (WHERE m.is_waiting), 0) AS waiting_amount
        FROM matching m
        WHERE m.day = ANY(%s)
        GROUP BY m.day
        ORDER BY m.day DESC
        """,
        [settings.tz] + params + [start, end, days],
    )
    return [
        {
            "date": row["day"].isoformat(),
            "count": int(row["count"]),
            "inflow": int(row["inflow"]),
            "outflow": int(row["outflow"]),
            "spending": int(row["spending"]),
            "movements": int(row["movements"]),
            "waiting_count": int(row["waiting_count"]),
            "waiting_amount": int(row["waiting_amount"]),
        }
        for row in cur.fetchall()
    ]


def load_type_counts(cur, tab_where_clause: str, params: list[Any]) -> dict[str, int]:
    """Rows behind each type tab under every filter except the type itself."""
    tabs = ", ".join(f"COUNT(*) FILTER (WHERE {TYPE_FILTER_SQL[key]}) AS {key}" for key in TAB_KEYS)
    cur.execute(
        f"SELECT COUNT(*) AS all_rows, {tabs} {LIST_FROM_SQL} {saving_flow_joins('t')} WHERE {tab_where_clause}", params,
    )
    row = cur.fetchone()
    return {"all": int(row["all_rows"]), **{key: int(row[key]) for key in TAB_KEYS}}


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
        raise api_error(422, ErrorCode.RECEIPT_UPLOAD_ENDPOINT)
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
                        "message": notice("transaction_duplicate"),
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
                    raise api_error(404, ErrorCode.SOURCE_ACCOUNT_NOT_FOUND)
            elif payload.account_name:
                matched_source = resolve_account_name(payload.account_name, accounts, field="account_name")
                account_id = str(matched_source["id"])
            else:
                raise api_error(422, ErrorCode.ACCOUNT_REQUIRED)

            # Route to effective pocket if source is a parent account with child pockets
            effective_source = resolve_effective_account(cur, user_id, matched_source, accounts)
            if effective_source:
                matched_source = effective_source
                account_id = str(effective_source["id"])

            if payload.credit_terms is not None and (
                payload.type != "expense" or not is_credit(matched_source) or payload.investment_action
                or payload.target_account_id or payload.target_account_name
            ):
                raise api_error(422, ErrorCode.CREDIT_TERMS_NOT_CREDIT_PURCHASE)

            # Resolve target account for investment tracking if provided
            target_account_id = None
            if payload.target_account_id:
                target_account_id = str(payload.target_account_id)
                matched_target = next((a for a in accounts if str(a["id"]) == target_account_id), None)
                if not matched_target:
                    raise api_error(404, ErrorCode.TARGET_ACCOUNT_NOT_FOUND)
                effective_target = resolve_effective_account(cur, user_id, matched_target, accounts)
                if effective_target:
                    matched_target = effective_target
                    target_account_id = str(effective_target["id"])
            elif payload.target_account_name:
                matched_target = resolve_account_name(payload.target_account_name, accounts, field="target_account_name")
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
                    raise api_error(404, ErrorCode.CATEGORY_NOT_FOUND)
                if not payload.investment_action and category["kind"] != payload.type:
                    raise api_error(400, ErrorCode.CATEGORY_TYPE_MISMATCH)
            elif payload.category_name:
                matched_cat = resolve_category_name(payload.category_name, categories, transaction_type=payload.type)
                if not payload.investment_action and matched_cat["kind"] != payload.type:
                    raise api_error(400, ErrorCode.CATEGORY_TYPE_MISMATCH)
                category_id = str(matched_cat["id"])

            if not payload.investment_action and (payload.target_account_id or payload.target_account_name):
                raise api_error(400, ErrorCode.TARGET_ACCOUNT_TRADE_ONLY)
            if not payload.investment_action and (
                matched_source.get("type") == "investment" or matched_source.get("instrument_type")
            ):
                raise api_error(422, ErrorCode.INVESTMENT_TRADE_REQUIRED)

            # Validate goal if provided
            goal_id = str(payload.goal_id) if payload.goal_id else None
            if goal_id:
                cur.execute("SELECT id FROM goals WHERE user_id = %s AND id = %s AND is_archived = FALSE", (user_id, goal_id))
                if not cur.fetchone():
                    raise api_error(404, ErrorCode.GOAL_NOT_FOUND)

            # Validate obligation if provided
            obligation_id = str(payload.obligation_id) if payload.obligation_id else None
            allocation_values = validate_allocation_request(
                payload.obligation_allocations,
                amount=payload.amount,
                obligation_id=payload.obligation_id,
                transaction_type=payload.type,
                investment_action=payload.investment_action,
            )
            if (obligation_id or allocation_values) and is_credit(matched_source):
                # A debt is paid from cash; a credit purchase that repays another debt is not supported (D7).
                raise refuse("credit_account_not_allowed")
            if obligation_id:
                cur.execute("SELECT id FROM obligations WHERE user_id = %s AND id = %s AND is_archived = FALSE AND remaining_amount > 0", (user_id, obligation_id))
                if not cur.fetchone():
                    raise api_error(404, ErrorCode.OBLIGATION_NOT_FOUND)

            # Determine kakeibo_type
            kakeibo_val = payload.kakeibo_type
            if payload.investment_action:
                # Investment trade (buy/sell) is capital deployment within portfolio, NOT monthly living budget saving
                kakeibo_val = None
                if not category_id:
                    cur.execute(
                        "SELECT id FROM categories WHERE user_id = %s AND system_key = %s LIMIT 1", (user_id, INVESTMENT),
                    )
                    inv_cat = cur.fetchone()
                    if inv_cat:
                        category_id = str(inv_cat["id"])
            elif not kakeibo_val and category_id:
                cur.execute(
                    "SELECT kakeibo_type, is_primary FROM categories WHERE id = %s",
                    (category_id,),
                )
                crow = cur.fetchone()
                # A category without a pillar leaves the row unclassified, following the category.
                if crow:
                    kakeibo_val = crow.get("kakeibo_type")

            if payload.investment_action:
                if payload.merchant or payload.review_status or payload.offset_category_id:
                    raise api_error(422, ErrorCode.TRADE_FIELDS_NOT_ALLOWED)
                if target_account_id is None:
                    raise api_error(400, ErrorCode.TRADE_TARGET_REQUIRED)
                if account_id == target_account_id:
                    raise api_error(400, ErrorCode.DISTINCT_ACCOUNTS_REQUIRED)

                trade_units = float(payload.units or 0)
                trade_price = float(payload.price_per_unit or 0)
                expected_amount = int(round(trade_units * trade_price))
                if expected_amount <= 0 or expected_amount != payload.amount:
                    raise api_error(400, ErrorCode.TRADE_AMOUNT_MISMATCH)

                if payload.investment_action == "buy":
                    funding_id = account_id
                    position_id = target_account_id
                else:
                    position_id = account_id
                    funding_id = target_account_id

                locked = lock_owned_accounts(cur, user_id, [funding_id, position_id])
                funding = locked[funding_id]
                position = locked[position_id]
                if position.get("investment_tracking_mode") == "amount":
                    raise api_error(409, ErrorCode.AMOUNT_PRODUCT_TRADE_NOT_SUPPORTED)
                ensure_not_credit(funding)
                if funding.get("type") not in {"cash", "bank", "wallet", "ewallet"} or funding.get("instrument_type"):
                    raise api_error(400, ErrorCode.FUNDING_NOT_LIQUID)
                if position.get("type") != "investment" and not position.get("instrument_type"):
                    raise api_error(400, ErrorCode.POSITION_NOT_INVESTMENT)

                old_units = float(position.get("units") or 0)
                old_avg = float(position.get("avg_buy_price") or 0)
                if payload.investment_action == "sell" and trade_units > old_units:
                    raise api_error(409, ErrorCode.INSUFFICIENT_UNITS, {"required_units": trade_units, "available_units": old_units}, extra={"required_units": trade_units, "available_units": old_units, "account_id": position_id})

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
                    notes=payload.notes or t(language_for_user(current_user), "notes.investment_trade", action=payload.investment_action),
                    tx_date=tx_date,
                    expense_category_id=expense_category_id,
                    income_category_id=income_category_id,
                    source_account=locked[source_id],
                    target_account=locked[destination_id],
                    idempotency_key=payload.idempotency_key,
                    is_trade=True,
                    provenance=system_provenance("investment"),
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
                    "message": notice("trade_recorded"),
                }

            if payload.target_account_id or payload.target_account_name:
                raise api_error(400, ErrorCode.MOVEMENT_ENDPOINT_REQUIRED)

            offset_category_id = str(payload.offset_category_id) if payload.offset_category_id else None
            if offset_category_id:
                if payload.type != "income" or not category_offsets_spending(cur, user_id, category_id):
                    raise offset_not_allowed()
                validate_offset_category(cur, user_id, offset_category_id)

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
                    amount, notes, date, receipt_path, kakeibo_type, idempotency_key,
                    merchant, offset_category_id,
                    source, review_status, classified_by, classification_confidence
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    payload.merchant,
                    offset_category_id,
                    *owner_provenance(payload.review_status or "reviewed").columns(),
                ),
            )
            created = cur.fetchone()
            if payload.credit_terms is not None:
                set_purchase_terms(
                    cur, user_id, lock_purchase(cur, user_id, str(created["id"])),
                    payload.credit_terms.installment_count, payload.credit_terms.first_due_date,
                )

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

            # A purchase over a credit account's limit is saved and only flagged (add-credit-accounts D5).
            credit_over_limit = False
            if is_credit(matched_source):
                cur.execute("SELECT credit_limit FROM accounts WHERE user_id = %s AND id = %s", (user_id, account_id))
                limit = cur.fetchone()["credit_limit"]
                credit_over_limit = over_limit(get_locked_ledger_balance(cur, user_id, account_id), limit)

            conn.commit()

    request_plan_linking(user_id)
    return {
        "ok": True,
        "transaction_id": str(created["id"]),
        "over_limit": credit_over_limit,
        "message": notice("transaction_recorded"),
    }


@router.get("/{transaction_id}")
def get_transaction(transaction_id: UUID, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    tid = str(transaction_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    t.id, t.account_id, sa.name AS account_name, sa.type AS account_type,
                    sa_parent.name AS account_parent_name,
                    t.category_id, c.name AS category_name, c.system_key AS category_system_key,
                    c.icon AS category_icon, c.color AS category_color,
                    t.goal_id, g.name AS goal_name,
                    t.obligation_id, o.name AS obligation_name,
                    t.type,
                    {effective_pillar_sql()} AS kakeibo_type,
                    t.amount, t.notes, t.date, t.receipt_path,
                    t.movement_id, t.movement_role, t.fee_for_movement_id, topup.id AS investment_topup_id,
                    partner.id AS partner_id,
                    partner.account_id AS movement_target_account_id,
                    partner_account.name AS movement_target_account_name,
                    partner_account.type AS movement_target_account_type,
                    partner_parent.name AS movement_target_account_parent_name,
                    credit_terms.installment_count AS credit_installment_count,
                    {row_saving_flow_sql("t")} AS saving_flow,
                    t.created_at,
                    (t.date AT TIME ZONE %s)::date AS local_date,
                    {waiting_row_sql("t")} AS waiting,
                    {PROVENANCE_SELECT}
                FROM transactions t
                JOIN accounts sa ON sa.id = t.account_id
                LEFT JOIN categories offset_category ON offset_category.id = t.offset_category_id
                LEFT JOIN transactions partner
                    ON partner.movement_id = t.movement_id
                   AND partner.id != t.id
                LEFT JOIN accounts partner_account ON partner_account.id = partner.account_id
                LEFT JOIN accounts sa_parent ON sa_parent.id = sa.parent_id
                LEFT JOIN accounts partner_parent ON partner_parent.id = partner_account.parent_id
                LEFT JOIN investment_topups topup ON topup.id = t.movement_id AND topup.user_id = t.user_id
                LEFT JOIN credit_purchase_terms credit_terms ON credit_terms.transaction_id = t.id
                LEFT JOIN categories c ON c.id = t.category_id
                LEFT JOIN goals g ON g.id = t.goal_id
                LEFT JOIN obligations o ON o.id = t.obligation_id
                WHERE t.user_id = %s AND t.id = %s
                """,
                (settings.tz, user_id, tid),
            )
            r = cur.fetchone()
            if not r:
                raise api_error(404, ErrorCode.TRANSACTION_NOT_FOUND)
            allocation_breakdowns = load_allocation_breakdowns(cur, [r])
            origin = load_origin(cur, user_id, tid, r.get("movement_id"))
            credit_block = load_credit_blocks(cur, user_id, [r], with_installments=True)[tid]

    return {
        "ok": True,
        "transaction": {
            "id": str(r["id"]),
            "account_id": str(r["account_id"]),
            "account_name": r["account_name"],
            "account_parent_name": r.get("account_parent_name"),
            "category_id": str(r["category_id"]) if r["category_id"] else None,
            "category_name": r["category_name"],
            "category_system_key": r["category_system_key"],
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
            "investment_topup_id": str(r["investment_topup_id"]) if r.get("investment_topup_id") else None,
            "movement_kind": "investment_topup" if r.get("investment_topup_id") else None,
            "movement_role": r.get("movement_role"),
            "fee_for_movement_id": str(r["fee_for_movement_id"]) if r.get("fee_for_movement_id") else None,
            "partner_id": str(r["partner_id"]) if r.get("partner_id") else None,
            "transfer_target_account_id": str(r["movement_target_account_id"]) if r.get("movement_target_account_id") else None,
            "transfer_target_account_name": r.get("movement_target_account_name"),
            "transfer_target_account_parent_name": r.get("movement_target_account_parent_name"),
            "amount": r["amount"],
            "notes": r["notes"],
            "date": r["date"].isoformat() if r["date"] else None,
            "receipt_path": r["receipt_path"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "local_date": r["local_date"].isoformat(),
            "waiting": bool(r["waiting"]),
            "origin": origin,
            **provenance_fields(r),
            "credit": credit_block,
            "saving_flow": r["saving_flow"],
        },
    }


def load_origin(cur, user_id: str, transaction_id: str, movement_id) -> dict[str, Any] | None:
    """The owner's own source text of a row: the phone notification it came from, or the typed capture."""
    cur.execute(
        """SELECT app_label, title, COALESCE(NULLIF(big_text, ''), body_text) AS text FROM notification_events
           WHERE user_id = %s AND transaction_id = %s ORDER BY post_time, id LIMIT 1""",
        (user_id, transaction_id),
    )
    event = cur.fetchone()
    if event and event["text"]:
        return {"kind": "notification", "app": event["app_label"], "title": event["title"], "text": event["text"]}
    cur.execute(
        """SELECT text FROM text_captures WHERE user_id = %s AND undone_at IS NULL
             AND (transaction_id = %s OR (%s::uuid IS NOT NULL AND movement_id = %s::uuid))
           ORDER BY created_at LIMIT 1""",
        (user_id, transaction_id, movement_id, movement_id),
    )
    capture = cur.fetchone()
    return {"kind": "capture", "text": capture["text"]} if capture else None


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
                SELECT t.id, t.account_id, t.category_id, t.goal_id, t.obligation_id, t.type,
                       t.amount, t.notes, t.date, t.receipt_path, t.kakeibo_type, t.movement_id,
                       t.merchant, t.offset_category_id, t.review_status,
                       c.kakeibo_type AS category_kakeibo_type
                FROM transactions t
                LEFT JOIN categories c ON c.id = t.category_id
                WHERE t.user_id = %s AND t.id = %s
                FOR UPDATE OF t
                """,
                (user_id, tid),
            )
            old_tx = cur.fetchone()
            if not old_tx:
                raise api_error(404, ErrorCode.TRANSACTION_NOT_FOUND)
            if payload.model_fields_set == {"review_status"}:
                return mark_review_status(conn, cur, user_id, tid, payload.review_status)
            if old_tx.get("movement_id"):
                raise api_error(409, ErrorCode.LINKED_MOVEMENT_ENDPOINT)
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
                    raise api_error(404, ErrorCode.ACCOUNT_NOT_FOUND)
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
                raise api_error(422, ErrorCode.INVALID_DEBT_ALLOCATIONS)
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
                new_kakeibo_type = crow.get("kakeibo_type") if crow else None
            else:
                new_kakeibo_type = old_tx.get("kakeibo_type")

            new_merchant = payload.merchant if "merchant" in payload.model_fields_set else old_tx.get("merchant")
            old_offset_id = str(old_tx["offset_category_id"]) if old_tx.get("offset_category_id") else None
            offset_allowed = new_type == "income" and category_offsets_spending(cur, user_id, new_category_id)
            if "offset_category_id" in payload.model_fields_set:
                new_offset_id = str(payload.offset_category_id) if payload.offset_category_id else None
                if new_offset_id and not offset_allowed:
                    raise offset_not_allowed()
                if new_offset_id and new_offset_id != old_offset_id:
                    validate_offset_category(cur, user_id, new_offset_id)
            else:
                # The link only means something on a reimbursement; recategorising away drops it.
                new_offset_id = old_offset_id if offset_allowed else None

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
                    raise api_error(404, ErrorCode.CATEGORY_NOT_FOUND)
                if category["kind"] != new_type:
                    raise api_error(400, ErrorCode.CATEGORY_TYPE_MISMATCH)
            if new_goal_id:
                cur.execute("SELECT id FROM goals WHERE user_id = %s AND id = %s AND is_archived = FALSE", (user_id, new_goal_id))
                if not cur.fetchone() and new_goal_id != (str(old_tx["goal_id"]) if old_tx.get("goal_id") else None):
                    raise api_error(404, ErrorCode.GOAL_NOT_FOUND)
            if new_obligation_id:
                cur.execute("SELECT id FROM obligations WHERE user_id = %s AND id = %s AND is_archived = FALSE AND remaining_amount > 0", (user_id, new_obligation_id))
                if not cur.fetchone() and new_obligation_id != (str(old_tx["obligation_id"]) if old_tx.get("obligation_id") else None):
                    raise api_error(404, ErrorCode.OBLIGATION_NOT_FOUND)
                if is_credit(locked_account):
                    raise refuse("credit_account_not_allowed")

            if new_type == "expense":
                if locked_account.get("type") in {"cash", "bank", "wallet", "ewallet"} and not locked_account.get("instrument_type"):
                    available = get_locked_ledger_balance(cur, user_id, new_account_id)
                    if old_tx["type"] == "expense" and str(old_tx["account_id"]) == new_account_id:
                        available += int(old_tx["amount"])
                    if available < new_amount:
                        raise api_error(409, ErrorCode.INSUFFICIENT_FUNDS, {"required_amount": new_amount, "available_amount": available}, extra={"required_amount": new_amount, "available_amount": available, "account_id": new_account_id})

            new_category_pillar = old_tx.get("category_kakeibo_type")
            if new_category_id != (str(old_tx["category_id"]) if old_tx["category_id"] else None):
                new_category_pillar = None
                if new_category_id:
                    cur.execute("SELECT kakeibo_type FROM categories WHERE id = %s", (new_category_id,))
                    category_row = cur.fetchone()
                    new_category_pillar = category_row.get("kakeibo_type") if category_row else None
            classification_changed = classification_change(
                old_tx,
                {
                    "category_id": new_category_id,
                    "kakeibo_type": effective_pillar(new_type, new_kakeibo_type, new_category_pillar),
                    "notes": new_notes,
                    "merchant": new_merchant,
                    "type": new_type,
                    "offset_category_id": new_offset_id,
                },
            )
            if "review_status" in payload.model_fields_set and payload.review_status:
                new_review_status = payload.review_status
            elif classification_changed:
                new_review_status = "reviewed"
            else:
                new_review_status = old_tx["review_status"]
            owner_assignments = f", {OWNER_EDIT_ASSIGNMENTS}" if classification_changed else ""
            before_classification = snapshot_rows(cur, user_id, [tid]) if classification_changed else {}

            # 4. Update the transaction row
            cur.execute(
                f"""
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
                    merchant = %s,
                    offset_category_id = %s,
                    review_status = %s,
                    -- A typed capture's time the owner corrects is the owner's from then on.
                    time_basis = CASE WHEN time_basis IS NOT NULL AND date IS DISTINCT FROM %s
                                      THEN 'owner' ELSE time_basis END,
                    time_evidence = CASE WHEN date IS DISTINCT FROM %s THEN NULL ELSE time_evidence END,
                    updated_at = NOW(){owner_assignments}
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
                    new_merchant,
                    new_offset_id,
                    new_review_status,
                    new_date,
                    new_date,
                    user_id,
                    tid,
                ),
            )
            if classification_changed:
                record_owner_corrections(cur, user_id, before_classification)
            edited = lock_purchase(cur, user_id, tid)
            credit_terms_reset = terms_after_edit(
                cur, user_id, tid, still_credit_purchase=is_credit_purchase(edited),
                old_date=credit_bills.local_day(old_tx["date"]), new_date=edited["local_date"],
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

    request_plan_linking(user_id)
    return {"ok": True, "message": notice("transaction_updated"), "credit_terms_reset": credit_terms_reset}


def classification_change(old_tx: dict[str, Any], new_values: dict[str, Any]) -> bool:
    """True when the owner changed what the row is, not only how much, when or where from.

    Pillars are compared as effective pillars, because the edit form sends the pillar it shows even
    when the row follows its category.
    """
    old_values = {
        "category_id": str(old_tx["category_id"]) if old_tx["category_id"] else None,
        "kakeibo_type": effective_pillar(old_tx["type"], old_tx.get("kakeibo_type"), old_tx.get("category_kakeibo_type")),
        "notes": (old_tx.get("notes") or "").strip(),
        "merchant": old_tx.get("merchant"),
        "type": old_tx["type"],
        "offset_category_id": str(old_tx["offset_category_id"]) if old_tx.get("offset_category_id") else None,
    }
    compared = {**new_values, "notes": (new_values.get("notes") or "").strip()}
    return any(old_values[field] != compared[field] for field in CLASSIFICATION_FIELDS)


def mark_review_status(conn, cur, user_id: str, transaction_id: str, review_status: str | None) -> dict[str, Any]:
    """Review action: confirms or flags the current classification without changing it."""
    if review_status is None:
        raise api_error(422, ErrorCode.INVALID_REVIEW_STATUS)
    cur.execute(
        "UPDATE transactions SET review_status = %s, updated_at = NOW() WHERE user_id = %s AND id = %s",
        (review_status, user_id, transaction_id),
    )
    conn.commit()
    return {"ok": True, "message": notice("review_status_updated"), "review_status": review_status}


@router.delete("/{transaction_id}")
def delete_transaction(transaction_id: UUID, current_user: dict = Depends(get_current_user)):
    user_id = current_user["id"]
    tid = str(transaction_id)
    with db_conn() as conn:
        with conn.cursor() as cur:
            tx = delete_transaction_rows(cur, user_id, tid)
            conn.commit()

    remove_receipt_file(tx.get("receipt_path"))

    return {"ok": True, "message": notice("transaction_deleted")}


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
                raise api_error(404, ErrorCode.TRANSACTION_NOT_FOUND)

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
        raise api_error(413, ErrorCode.RECEIPT_TOO_LARGE, {"max_mb": settings.receipt_max_mb})
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
                    raise api_error(404, ErrorCode.TRANSACTION_NOT_FOUND)
                store_receipt(rel_path, prepared.content)
                cur.execute(
                    "UPDATE transactions SET receipt_path = %s, updated_at = NOW() WHERE id = %s AND user_id = %s RETURNING id",
                    (rel_path, tid, user_id),
                )
                if not cur.fetchone():
                    raise api_error(404, ErrorCode.TRANSACTION_NOT_FOUND)
                conn.commit()
    except Exception:
        remove_receipt_file(rel_path)
        raise

    remove_receipt_file(owned_transaction.get("receipt_path"))

    return {"ok": True, "receipt_path": rel_path, "message": notice("receipt_uploaded")}
