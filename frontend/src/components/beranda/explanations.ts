import type { ExplanationLine } from "@/components/workspace/explanation";
import type { RincianData } from "@/components/workspace/ExplanationDialog";
import type { Translate, Words } from "@/lib/i18n/types";
import type { CycleSummary, ExpectedReason, SafeScheduledLine, SummaryLine } from "./types";
import { bucketName } from "./words";

/**
 * "Rincian hitungan" content for the Beranda hero. Everything here formats the server's figures and lines;
 * nothing is added up on the client (design D13), so the web and any later view cannot drift. The words come
 * from the catalogue and the formats from the region (add-language-system D5 to D7).
 */
export type Money = (amount: number) => string;

export function signed(amount: number, money: Money): string {
  if (amount > 0) return `+${money(amount)}`;
  if (amount < 0) return `−${money(-amount)}`;
  return money(0);
}

/** A balance: no plus sign, a minus only when below zero. */
export function balance(amount: number, money: Money): string {
  return amount < 0 ? `−${money(-amount)}` : money(amount);
}

/** "cycle" or "month", the ICU `select` value of every period-worded message. */
export function periodBasis(summary: CycleSummary): "cycle" | "month" {
  return summary.period.basis === "cycle" ? "cycle" : "month";
}

/** A server line's label: the user's own name as stored, fixed wording from its key (D7). */
export function lineLabel(line: SummaryLine, t: Translate): string {
  if (line.label_source !== "system") return line.label;
  if (line.key === "uncategorized") return t("terms.noCategory");
  return bucketName(t, line.key);
}

function expectedBasisNote(summary: CycleSummary, { t, format }: Words): string | null {
  const expected = summary.expected_salary;
  if (!expected.date || expected.basis !== "regular_receipts") return null;
  const receipts = expected.receipt_dates.map(format.shortDate).join(", ");
  return t("breakdown.expectedBasis", { date: format.longDate(expected.date), receipts });
}

const EXPECTED_REASONS = {
  too_few_receipts: "breakdown.reasonTooFewReceipts",
  irregular_receipts: "breakdown.reasonIrregularReceipts",
  no_salary_source: "breakdown.reasonNoSalarySource",
  no_cycle: "breakdown.reasonNoCycle",
} as const satisfies Record<ExpectedReason, string>;

/** Why the days to the next salary are unknown: the full reason for the Rincian and the hero's short line. */
export function unavailableReason(summary: CycleSummary, { t, format }: Words): { reason: string; line: string } {
  const safe = summary.safe_to_spend;
  if (safe.reason === "salary_overdue") {
    const date = summary.expected_salary.date;
    return {
      reason: date ? t("breakdown.reasonSalaryOverdue", { date: format.longDate(date) }) : t("breakdown.reasonSalaryOverdueNoDate"),
      line: t("breakdown.lineSalaryOverdue"),
    };
  }
  if (safe.reason === "no_cycle") return { reason: t(EXPECTED_REASONS.no_cycle), line: t("breakdown.lineNoCycle") };
  const detail = safe.detail ?? summary.expected_salary.reason ?? "too_few_receipts";
  return {
    reason: t(EXPECTED_REASONS[detail]),
    line: detail === "no_salary_source" ? t("breakdown.lineNoSalarySource") : t("breakdown.lineNextSalaryUnknown"),
  };
}

/** A capitalised sentence from a reason that is written to follow a colon. */
function sentence(text: string): string {
  return text.charAt(0).toUpperCase() + text.slice(1);
}

/** The note under the Rincian's total: the shortfall, else the expected-salary basis or why the date is unknown. */
function basisNote(summary: CycleSummary, money: Money, words: Words): string | null {
  if (summary.safe_amount.short_by > 0) return words.t("breakdown.shortNote", { amount: money(summary.safe_amount.short_by) });
  if (summary.safe_to_spend.status !== "available") return sentence(unavailableReason(summary, words).reason);
  return expectedBasisNote(summary, words);
}

/** A cost line of the Rincian: "−Rp …", or Rp 0 without a sign. */
function cost(amount: number, money: Money): string {
  return amount > 0 ? `−${money(amount)}` : money(0);
}

/** Every daily-money pocket with its bank, largest first, as the server orders them (D2, D7). */
function dailyMoneyParts(summary: CycleSummary, money: Money): ExplanationLine[] {
  return summary.safe_amount.daily_money.accounts.map((account) => ({
    label: account.parent_name ? `${account.parent_name} · ${account.name}` : account.name,
    amount: balance(account.amount, money),
  }));
}

/** Every credit account with what it owes and its next due date (D3, D7). */
function creditParts(summary: CycleSummary, money: Money, { t, format }: Words): ExplanationLine[] {
  return summary.safe_amount.credit_owed.accounts.map((account) => ({
    label: account.name,
    note: account.next_due ? t("breakdown.creditDue", { date: format.shortDate(account.next_due.due_date) }) : null,
    amount: cost(account.amount, money),
  }));
}

const NOT_COUNTED_KEYS = {
  in_credit_owed: "breakdown.inCreditOwed",
  paid_from_savings: "breakdown.paidFromSavings",
} as const satisfies Record<NonNullable<SafeScheduledLine["not_counted_reason"]>, string>;

/** Each plan and set-aside with what it takes from daily money; one counted elsewhere says where (D4). */
function scheduledParts(summary: CycleSummary, money: Money, { t, format }: Words): ExplanationLine[] {
  return summary.safe_amount.scheduled.lines.map((line) => {
    const elsewhere = line.not_counted_reason ? t(NOT_COUNTED_KEYS[line.not_counted_reason]) : null;
    if (line.kind === "goal") {
      return {
        label: t("breakdown.goalDepositLine", { label: line.label }),
        note: t("breakdown.goalDepositNote", { amount: money(line.set_aside), funded: money(line.funded) }),
        amount: cost(line.safe_amount, money),
      };
    }
    const due = line.overdue
      ? t("breakdown.dueOnOverdue", { date: format.shortDate(line.due_date) })
      : t("breakdown.dueOn", { date: format.shortDate(line.due_date) });
    return {
      label: t("breakdown.billLine", { label: line.label }),
      note: elsewhere ? `${due} · ${elsewhere}` : due,
      amount: cost(line.safe_amount, money),
    };
  });
}

/**
 * The Rincian of "Aman dipakai" (count-safe-amount-from-daily-money D7): Uang harian less Tagihan kredit and
 * Terjadwal, each opening the server lines behind it, then a shortfall if any and the total.
 */
export function safeAmountRincian(summary: CycleSummary, money: Money, words: Words): RincianData {
  const { t, format } = words;
  const safe = summary.safe_amount;
  const window =
    safe.scheduled.window_basis === "expected_salary"
      ? t("breakdown.windowExpectedSalary", { date: format.shortDate(safe.scheduled.window_end) })
      : t("breakdown.windowNext30Days", { date: format.shortDate(safe.scheduled.window_end) });
  return {
    title: t("terms.safeToSpend"),
    rule: t("breakdown.safeRuleDaily"),
    lines: [
      { key: "daily", label: t("terms.everydayMoney"), amount: balance(safe.daily_money.total, money), parts: dailyMoneyParts(summary, money) },
      { key: "credit", label: t("terms.creditBills"), amount: cost(safe.credit_owed.total, money), parts: creditParts(summary, money, words) },
      {
        key: "scheduled",
        label: t("breakdown.scheduledUntil", { date: format.shortDate(safe.scheduled.window_end) }),
        amount: cost(safe.scheduled.total, money),
        parts: scheduledParts(summary, money, words),
        partNotes: [window],
      },
    ],
    // Short ends in what is missing, then the result Rp 0, so "Aman dipakai" is never negative.
    over: safe.short_by > 0 ? { label: t("breakdown.short"), amount: money(safe.short_by) } : null,
    total: { label: t("terms.safeToSpend"), amount: money(safe.amount) },
    notes: [basisNote(summary, money, words)].filter((note): note is string => Boolean(note)),
    link: { href: "/accounts", label: t("breakdown.openAccounts") },
  };
}
