import type { PlanLine } from "@/components/beranda/types";
import type { CreditBill } from "@/lib/creditBills";
import type { ExpensePlanDraft } from "@/types/recurring";

/** A charge the server saw recur with a steady amount (`GET /dashboard/recurring-charges`); only the fields read here. */
export interface DetectedCharge {
  key: string;
  merchant: string;
  cadence: "weekly" | "monthly";
  typical_amount: number;
  next_expected_date: string | null;
  existing_plan: { id: string } | null;
  plan_draft?: ExpensePlanDraft;
}

export type BillStatus = "overdue" | "dueToday" | "scheduled";

/** One row of "Tagihan 30 hari ke depan" (apply-markas-target D5, D14). */
export type BillRow =
  | {
      kind: "plan";
      key: string;
      ruleId: string;
      label: string;
      date: string;
      /** What the row shows: a credit-bill plan's full `plan_amount`, else the line's amount. */
      shown: number;
      /** What the section's total adds: the line's `amount`, the part not yet in Pengeluaran. */
      counted: number;
      /** The part of a credit-bill plan already in Pengeluaran; 0 for every other plan. */
      countedElsewhere: number;
      status: BillStatus;
    }
  | {
      kind: "charge";
      key: string;
      label: string;
      date: string;
      shown: number;
      counted: number;
      cadence: DetectedCharge["cadence"];
      draft: ExpensePlanDraft | null;
    }
  | {
      kind: "credit";
      key: string;
      accountId: string;
      label: string;
      date: string;
      shown: number;
      status: BillStatus;
    };

function statusOf(due: string, overdue: boolean, today: string): BillStatus {
  if (overdue || due < today) return "overdue";
  return due === today ? "dueToday" : "scheduled";
}

/**
 * Planned bills of the window, then charges seen recurring that no plan covers yet, from today to the window end,
 * marked as estimates (OD97), by date. The section counts and adds these rows only, each plan with its `amount`, as
 * Terjadwal does.
 */
export function billRows(lines: PlanLine[], charges: DetectedCharge[], today: string, windowEnd: string): BillRow[] {
  const planned: BillRow[] = lines.map((line) => ({
    kind: "plan",
    key: `plan-${line.rule_id}-${line.due_date}`,
    ruleId: line.rule_id,
    label: line.label,
    date: line.due_date,
    shown: line.plan_amount ?? line.amount,
    counted: line.amount,
    countedElsewhere: line.counted_elsewhere ?? 0,
    status: statusOf(line.due_date, line.overdue, today),
  }));
  const detected: BillRow[] = charges
    .filter((charge) => !charge.existing_plan && charge.next_expected_date != null)
    .filter((charge) => charge.next_expected_date! >= today && charge.next_expected_date! <= windowEnd)
    .map((charge) => ({
      kind: "charge",
      key: `seen-${charge.key}`,
      label: charge.merchant,
      date: charge.next_expected_date!,
      shown: charge.typical_amount,
      counted: charge.typical_amount,
      cadence: charge.cadence,
      draft: charge.plan_draft ?? null,
    }));
  return [...planned, ...detected].sort((a, b) => a.date.localeCompare(b.date));
}

/**
 * Unpaid credit bills beside the plans (H9, D14): each bill with something left to pay, except one a credit-bill
 * plan line already stands for (same account and due date), so the bill and its plan are listed once. These rows
 * are already in Pengeluaran and never join the section's count, total or Terjadwal.
 */
export function creditBillRows(bills: CreditBill[], lines: PlanLine[], today: string): BillRow[] {
  return bills
    .filter((bill) => bill.left_to_pay > 0 && bill.status !== "paid" && bill.status !== "paid_late")
    .filter((bill) => !lines.some((line) => line.credit_account_id === bill.account_id && line.due_date === bill.due_date))
    .map((bill) => ({
      kind: "credit",
      key: `credit-${bill.account_id}-${bill.due_date}`,
      accountId: bill.account_id,
      label: bill.account_name,
      date: bill.due_date,
      shown: bill.left_to_pay,
      status: bill.status === "overdue" ? "overdue" : statusOf(bill.due_date, false, today),
    }));
}

/** Every row by date; a plan or charge comes before a credit bill of the same day. */
export function mergeBillRows(rows: BillRow[], creditRows: BillRow[]): BillRow[] {
  return [...rows, ...creditRows].sort((a, b) => a.date.localeCompare(b.date));
}

/** The section's count and total: plan and charge rows only (D5, D14). */
export function billTotals(rows: BillRow[]): { count: number; total: number } {
  const counted = rows.filter((row): row is Extract<BillRow, { counted: number }> => row.kind !== "credit");
  return { count: counted.length, total: counted.reduce((sum, row) => sum + row.counted, 0) };
}
