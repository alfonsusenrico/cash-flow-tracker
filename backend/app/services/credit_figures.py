"""The two informational credit figures of a period (add-credit-spending-flow D11) and Terjadwal's credit-bill
plans (D9).

"Pengeluaran pakai kredit" is the part of Pengeluaran paid with credit accounts: reportable expenses in every
pillar but saving, net of the reimbursements Pengeluaran applies. "Tagihan kredit dibayar" is the cash moved
into credit accounts from accounts that are not credit. Neither changes any other figure.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.apportionment import apportion
from app.services.credit_bills import Schedule
from app.services.kakeibo import pillar_bucket_sql
from app.services.reimbursements import SpendingOffsets
from app.services.report_figures import reportable_expense_sql

# The saving bucket is "Ditabung & investasi", not Pengeluaran (cycle_summary.spending_split).
SAVING = "saving"
OTHER = "__not_credit__"


def spent_on_credit_by_account(
    cur, user_id: str, start: datetime, end: datetime, offsets: SpendingOffsets,
) -> dict[str, int]:
    """Each credit account's share of Pengeluaran in the window.

    A reimbursement offset of a category and pillar is shared between the accounts that hold that spending
    in proportion to it, so the credit part never exceeds its pillar's net figure.
    """
    cur.execute(
        f"""
        SELECT t.account_id, a.type = 'credit' AS is_credit, t.category_id, {pillar_bucket_sql()} AS pillar,
               SUM(t.amount) AS amount
        FROM transactions t
        LEFT JOIN categories c ON c.id = t.category_id
        JOIN accounts a ON a.id = t.account_id AND a.user_id = t.user_id
        WHERE t.user_id = %s AND {reportable_expense_sql()} AND t.date >= %s AND t.date <= %s
        GROUP BY t.account_id, a.type, t.category_id, pillar
        """,
        (user_id, start, end),
    )
    groups: dict[tuple[str | None, str], dict[str, int]] = {}
    for row in cur.fetchall():
        if row["pillar"] == SAVING:
            continue
        category = str(row["category_id"]) if row["category_id"] else None
        weights = groups.setdefault((category, row["pillar"]), {})
        key = str(row["account_id"]) if row["is_credit"] else OTHER
        weights[key] = weights.get(key, 0) + int(row["amount"])
    totals: dict[str, int] = {}
    for (category, pillar), weights in groups.items():
        offset = offsets.for_category_pillar(category, pillar) if category else 0
        shares = apportion(offset, dict(sorted(weights.items())))
        for account_id, gross in weights.items():
            if account_id == OTHER:
                continue
            totals[account_id] = totals.get(account_id, 0) + gross - shares.get(account_id, 0)
    return {account_id: amount for account_id, amount in totals.items() if amount}


def paid_to_credit_by_account(cur, user_id: str, start: datetime, end: datetime) -> dict[str, int]:
    """Cash that left a non-credit account for a credit account in the window, by the credit account."""
    cur.execute(
        """
        SELECT inbound.account_id, SUM(outbound.amount) AS amount
        FROM transactions inbound
        JOIN transactions outbound
          ON outbound.user_id = inbound.user_id AND outbound.movement_id = inbound.movement_id
         AND outbound.movement_role = 'outbound'
        JOIN accounts target ON target.id = inbound.account_id AND target.user_id = inbound.user_id
        JOIN accounts source ON source.id = outbound.account_id AND source.user_id = outbound.user_id
        WHERE inbound.user_id = %s AND inbound.movement_role = 'inbound'
          AND target.type = 'credit' AND source.type <> 'credit'
          AND outbound.date >= %s AND outbound.date <= %s
        GROUP BY inbound.account_id
        """,
        (user_id, start, end),
    )
    return {str(row["account_id"]): int(row["amount"]) for row in cur.fetchall() if int(row["amount"])}


def credit_spending(cur, user_id: str, start: datetime, end: datetime, offsets: SpendingOffsets) -> dict[str, Any]:
    """`spending.credit` of the cycle summary: both figures, with one line per credit account that has one."""
    spent = spent_on_credit_by_account(cur, user_id, start, end, offsets)
    paid = paid_to_credit_by_account(cur, user_id, start, end)
    account_ids = sorted(set(spent) | set(paid))
    lines = []
    if account_ids:
        cur.execute(
            """SELECT id, name FROM accounts WHERE user_id = %s AND id = ANY(%s::uuid[])
               ORDER BY display_order, created_at, name""",
            (user_id, account_ids),
        )
        for row in cur.fetchall():
            account_id = str(row["id"])
            lines.append({
                "key": f"credit:{account_id}",
                "account_id": account_id,
                "label": row["name"],
                "label_source": "user",
                "spent_on_credit": spent.get(account_id, 0),
                "paid_to_credit": paid.get(account_id, 0),
            })
    return {
        "spent_on_credit": sum(spent.values()),
        "paid_to_credit": sum(paid.values()),
        "lines": lines,
    }


def credit_bill_plan_targets(cur, user_id: str) -> dict[str, str]:
    """Active transfer plans into a credit account: rule id to the credit account id (D9)."""
    cur.execute(
        """
        SELECT r.id, r.target_account_id FROM recurring_rules r
        JOIN accounts a ON a.id = r.target_account_id AND a.user_id = r.user_id
        WHERE r.user_id = %s AND r.is_active AND r.type = 'transfer' AND a.type = 'credit'
        """,
        (user_id,),
    )
    return {str(row["id"]): str(row["target_account_id"]) for row in cur.fetchall()}


def count_credit_bill_plans(
    lines: list[dict[str, Any]], targets: dict[str, str], schedules: dict[str, Schedule],
) -> list[dict[str, Any]]:
    """F1 (D9): an unpaid transfer plan into a credit account counts only up to that account's opening debt
    still left to pay, shared in due-date order; the rest is already in Pengeluaran as purchases, fees or
    spent cash advances. Other plan lines are returned unchanged.

    `lines` is in due-date order, as `unpaid_plan_lines` returns it.
    """
    opening_left = {account_id: schedule.opening_left() for account_id, schedule in schedules.items()}
    counted_lines = []
    for line in lines:
        account_id = targets.get(line.get("rule_id"))
        if line.get("kind") != "plan" or account_id is None:
            counted_lines.append(line)
            continue
        plan_amount = int(line["amount"])
        counted = min(plan_amount, opening_left.get(account_id, 0))
        opening_left[account_id] = opening_left.get(account_id, 0) - counted
        counted_lines.append({
            **line,
            "amount": counted,
            "plan_amount": plan_amount,
            "counted_elsewhere": plan_amount - counted,
            "credit_account_id": account_id,
        })
    return counted_lines
