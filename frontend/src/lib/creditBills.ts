import type { CreditBillStatus } from "@/components/accounts/types";
import type { RecurringRule } from "@/types/recurring";

/** One bill of `GET /api/credit/bills` (add-credit-spending-flow H9); every amount is the server's. */
export interface CreditBill {
  account_id: string;
  account_name: string;
  due_date: string;
  statement_date: string;
  amount: number;
  paid: number;
  left_to_pay: number;
  status: CreditBillStatus;
  settled_on: string | null;
}

export interface CreditBillsResponse {
  ok: boolean;
  bills: CreditBill[];
  due_dates_unavailable: { account_id: string; account_name: string }[];
}

/** The statuses of a bill that still asks for money. */
export const UNPAID_BILL_STATUSES = "open,upcoming,due_today,overdue";

/** `GET /credit/bills` for unpaid bills due up to `to`, and from `from` when given (apply-markas-target D14, D16). */
export function creditBillsPath(to: string, from?: string): string {
  const params = new URLSearchParams();
  if (from) params.set("from", from);
  params.set("to", to);
  params.set("status", UNPAID_BILL_STATUSES);
  return `/credit/bills?${params.toString()}`;
}

/** A transfer plan paying this bill: into the bill's credit account on its due date (D14, D16). */
export function billPaidByPlan(bill: Pick<CreditBill, "account_id" | "due_date">, rule: Pick<RecurringRule, "type" | "target_account_id" | "next_due_date">): boolean {
  return rule.type === "transfer" && rule.target_account_id === bill.account_id && rule.next_due_date === bill.due_date;
}

/**
 * The credit bills that need the owner (D16): something is still left to pay, and no listed plan already stands
 * for the same account and due date, so a bill and its plan are listed once, as the plan.
 */
export function dueCreditBills(bills: CreditBill[], listedPlans: RecurringRule[]): CreditBill[] {
  return bills
    .filter((bill) => bill.left_to_pay > 0 && bill.status !== "paid" && bill.status !== "paid_late")
    .filter((bill) => !listedPlans.some((rule) => billPaidByPlan(bill, rule)))
    .sort((a, b) => a.due_date.localeCompare(b.due_date) || a.account_name.localeCompare(b.account_name));
}
