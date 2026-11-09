import { owedOn } from "./accountFacts";
import type { AccountItem, CreditBillStatus, CreditNextDue } from "./types";

/**
 * What a credit account's tile and detail show (add-credit-spending-flow H8, apply-markas-rekening D7), read from
 * its `GET /api/accounts` row. Every value is the server's; nothing here computes a bill.
 */
export interface CreditFacts {
  /** What the account owes, as a positive amount; 0 when it is paid off or overpaid. */
  owed: number;
  /** Money paid beyond what was owed ("Lebih bayar"), as a positive amount; 0 otherwise. */
  overpaid: number;
  limit: number | null;
  /** The limit minus what is owed; negative over the limit, null without a limit. */
  limitLeft: number | null;
  overLimit: boolean;
  /** The next unpaid bill, or null when no bill is open. */
  nextDue: CreditNextDue | null;
  /** The day of the month a bill is due; without it the bills cannot be worked out. */
  paymentDay: number | null;
  statementDay: number | null;
}

export function creditFacts(account: AccountItem): CreditFacts {
  const limit = account.credit_limit ?? null;
  const owed = account.balance > 0 ? 0 : owedOn(account);
  return {
    owed,
    overpaid: account.balance > 0 ? account.balance : 0,
    limit,
    limitLeft: limit === null ? null : account.limit_left ?? limit - owed,
    overLimit: Boolean(account.over_limit),
    nextDue: account.next_due ?? null,
    paymentDay: account.payment_day ?? null,
    statementDay: account.statement_day ?? null,
  };
}

/** The `terms.billStatus` key of a bill status (Terbuka, Akan datang, Jatuh tempo hari ini, …). */
export const BILL_STATUS_KEY: Record<CreditBillStatus, "open" | "upcoming" | "dueToday" | "overdue" | "paid" | "paidLate"> = {
  open: "open",
  upcoming: "upcoming",
  due_today: "dueToday",
  overdue: "overdue",
  paid: "paid",
  paid_late: "paidLate",
};

/** The text tone of a status word: due today and overdue warn, a late payment is muted, the rest stay ink. */
export function billStatusTone(status: CreditBillStatus): "warning" | "muted" | "plain" {
  if (status === "due_today" || status === "overdue") return "warning";
  if (status === "paid" || status === "paid_late") return "muted";
  return "plain";
}
