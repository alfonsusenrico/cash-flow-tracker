"""What Beranda's "Ditabung & investasi" counts, as one definition for the Kakeibo aggregate and the ledger rows.

`get_kakeibo_breakdown` sums these predicates over a window; the ledger reads the same predicates per row as
`saving_flow` (apply-markas-transaksi D27) and filters on them for `type=saving` (D28). Roles and goal links
are read as they are now, so a later role change also applies to earlier rows.
"""
from __future__ import annotations

from app.services.credit_accounts import CREDIT_TYPE
from app.services.kakeibo import effective_pillar_sql
from app.services.report_figures import reportable_expense_sql

# A savings destination (organize-accounts-by-use D3): role tabungan, a goal link or an investment product.
# The role test is NULL-safe so a row read before V51's backfill counts as it did before V51.
PRODUCT_SQL = "({a}.type = 'investment' OR {a}.instrument_type IS NOT NULL)"
SAVINGS_DESTINATION_SQL = (
    "({a}.role IS NOT DISTINCT FROM 'tabungan' OR {a}.type = 'investment' OR {a}.instrument_type IS NOT NULL"
    " OR EXISTS (SELECT 1 FROM goal_accounts ga_{a} WHERE ga_{a}.account_id = {a}.id))"
)
# An account whose spending also lowers the savings change (D4): role tabungan or a goal link, not a product.
SAVINGS_ACCOUNT_SQL = (
    "({a}.role IS NOT DISTINCT FROM 'tabungan'"
    " OR EXISTS (SELECT 1 FROM goal_accounts ga_{a} WHERE ga_{a}.account_id = {a}.id))"
)

SAVED = "saved"
INVESTED = "invested"
TAKEN = "taken"
PAID_FROM_SAVINGS = "paid_from_savings"
SAVING_PILLAR = "saving_pillar"
DEBT_PAYMENT = "debt_payment"
# The values that change "Ditabung & investasi"; DEBT_PAYMENT is informational only.
COUNTED_FLOWS = (SAVED, INVESTED, TAKEN, PAID_FROM_SAVINGS, SAVING_PILLAR)


def saving_pillar_sql(tx: str = "t", category: str = "c") -> str:
    """True for an expense whose pillar, its own or its category's, is Tabungan.

    Such an expense counts in "Ditabung & investasi", not in Pengeluaran (`spending_basis`, apply-markas-analisis
    D31), and its row's saving direction is `saving_pillar`. NULL for income, so callers wrap it in COALESCE.
    """
    return f"({effective_pillar_sql(tx, category)} = 'saving')"


def counted_movement_sql(outbound: str, source: str, target: str) -> str:
    """True when a durably linked movement can count as saving.

    A Beli/Jual trade stays outside Kakeibo: a movement touching an investment product counts only as an
    investment top-up, or as an older row stored as saving before top-ups existed.
    """
    return f"""NOT (
                  ({PRODUCT_SQL.format(a=source)} OR {PRODUCT_SQL.format(a=target)})
                  AND {outbound}.kakeibo_type IS DISTINCT FROM 'saving'
                  AND NOT EXISTS (
                      SELECT 1 FROM investment_topups it
                      WHERE it.user_id = {outbound}.user_id AND it.id = {outbound}.movement_id
                  )
              )"""


def saving_flow_joins(tx: str = "t") -> str:
    """Both legs of the row's movement and the accounts of the legs and of the row, for `saving_flow_sql`.

    Joined on the unique (movement_id, movement_role), so a row never multiplies; a row outside a movement
    joins no legs. Joins rather than per-row subqueries keep the whole-window counts cheap.
    """
    return f"""
    LEFT JOIN transactions sf_out
        ON sf_out.user_id = {tx}.user_id AND sf_out.movement_id = {tx}.movement_id AND sf_out.movement_role = 'outbound'
    LEFT JOIN transactions sf_in
        ON sf_in.user_id = {tx}.user_id AND sf_in.movement_id = {tx}.movement_id AND sf_in.movement_role = 'inbound'
    LEFT JOIN accounts sf_sa ON sf_sa.id = sf_out.account_id AND sf_sa.user_id = {tx}.user_id
    LEFT JOIN accounts sf_da ON sf_da.id = sf_in.account_id AND sf_da.user_id = {tx}.user_id
    LEFT JOIN accounts sf_ra ON sf_ra.id = {tx}.account_id AND sf_ra.user_id = {tx}.user_id
"""


def saving_flow_sql(tx: str = "t", category: str = "c") -> str:
    """A row's saving direction, with `saving_flow_joins(tx)` and the row's category in scope.

    Both legs of a movement read the same legs, so they carry the same value. The counted classes (saved,
    invested, taken) come first, then debt_payment; a linked movement with none of them falls through to the
    expense rule like any row. An expense is paid_from_savings before saving_pillar, as the aggregate counts
    it in both parts and they cancel. Every other row is NULL. The cases nest so that each predicate is
    evaluated at most twice per row.
    """
    counted = counted_movement_sql("sf_out", "sf_sa", "sf_da")
    source_saving = SAVINGS_DESTINATION_SQL.format(a="sf_sa")
    target_saving = SAVINGS_DESTINATION_SQL.format(a="sf_da")
    target_product = PRODUCT_SQL.format(a="sf_da")
    movement_flow = f"""CASE WHEN sf_sa.id IS NOT NULL AND sf_da.id IS NOT NULL THEN
            CASE
                WHEN {source_saving} THEN
                    CASE WHEN NOT {target_saving} AND {counted} THEN '{TAKEN}' END
                WHEN {target_saving} THEN
                    CASE WHEN {counted} THEN CASE WHEN {target_product} THEN '{INVESTED}' ELSE '{SAVED}' END END
                WHEN sf_da.type = '{CREDIT_TYPE}' THEN '{DEBT_PAYMENT}'
            END
        END"""
    expense_flow = f"""CASE WHEN {reportable_expense_sql(tx, category)} THEN
            CASE
                WHEN {SAVINGS_ACCOUNT_SQL.format(a="sf_ra")} THEN '{PAID_FROM_SAVINGS}'
                WHEN {saving_pillar_sql(tx, category)} THEN '{SAVING_PILLAR}'
            END
        END"""
    return f"COALESCE({movement_flow}, {expense_flow})"


def row_saving_flow_sql(tx: str = "t") -> str:
    """`saving_flow_sql` for one listed row, as a select-list subquery that needs no joins in the outer query.

    PostgreSQL evaluates such a column after the sort and limit, so a page computes it for its own rows only.
    """
    return f"""(
        SELECT {saving_flow_sql("sf_row", "sf_cat")}
        FROM transactions sf_row
        LEFT JOIN categories sf_cat ON sf_cat.id = sf_row.category_id
        {saving_flow_joins("sf_row")}
        WHERE sf_row.id = {tx}.id
    )"""


def counts_as_saving_sql(tx: str = "t", category: str = "c") -> str:
    """Rows listed under "Tabungan & investasi": the parts of Beranda's figure (D28)."""
    values = ", ".join(f"'{value}'" for value in COUNTED_FLOWS)
    return f"({saving_flow_sql(tx, category)} IN ({values}))"


def credit_row_sql() -> str:
    """Rows on a credit account, and the cash legs of movements into or out of one (D30).

    Needs `saving_flow_joins`: a row is listed when it or either leg of its movement is on a credit account.
    """
    return f"(sf_ra.type = '{CREDIT_TYPE}' OR sf_sa.type = '{CREDIT_TYPE}' OR sf_da.type = '{CREDIT_TYPE}')"
