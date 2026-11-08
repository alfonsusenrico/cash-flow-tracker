import type { ExplanationData, ExplanationLine } from "@/components/workspace/explain";
import type { RincianData } from "@/components/workspace/ExplanationDialog";
import { createFormat } from "@/lib/format";
import type { Translate, Words } from "@/lib/i18n/types";
import type { CycleSummary, ExpectedReason, SummaryLine, WaitingLine, WaitingSection } from "./types";
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

/** A server line's note from its codes; an older API without codes keeps its own note. */
function lineNote(line: SummaryLine, { t, format }: Words): string | null {
  if (!line.note_codes) return line.note ?? null;
  // The server writes these amounts in rupiah whatever the display currency, so the note stays as it reads today.
  const rupiah = createFormat({ language: format.language, region: format.region });
  const notes = line.note_codes.map((note) =>
    note.code === "includes_savings_moves"
      ? t("breakdown.includesSavingsMoves")
      : t("breakdown.reducedByReimbursementsNote", { amount: rupiah.money(note.params.amount ?? 0) }),
  );
  return notes.length ? notes.join(". ") : null;
}

function lineOf(line: SummaryLine, money: Money, words: Words, withHref = true): ExplanationLine {
  return { label: lineLabel(line, words.t), note: lineNote(line, words), amount: money(line.amount), href: withHref ? line.href ?? null : null };
}

function ledgerPeriod(summary: CycleSummary): string {
  return `/ledger?period=${summary.period.cycle_offset_basis}`;
}

const SECTION_KEYS = {
  questions: "breakdown.sectionQuestions",
  ambiguous: "breakdown.sectionAmbiguous",
  awaiting: "breakdown.sectionAwaiting",
  groups: "breakdown.sectionGroups",
} as const satisfies Record<WaitingSection, string>;

export function spendingExplanation(summary: CycleSummary, money: Money, words: Words): ExplanationData {
  const { t } = words;
  const notes = [t("breakdown.spendingNote")];
  if (summary.keluar.offsets_total > 0) notes.push(t("breakdown.reducedByReimbursements", { amount: money(summary.keluar.offsets_total) }));
  if (summary.waiting.in_keluar > 0) notes.push(t("breakdown.waitingIncluded", { amount: money(summary.waiting.in_keluar) }));
  return {
    title: t("breakdown.spendingTitle", { period: periodBasis(summary) }),
    rule: t("breakdown.spendingRule"),
    lines: summary.spending.lines.map((line) => lineOf(line, money, words)),
    total: { label: t("breakdown.totalSpending"), amount: money(summary.spending.amount) },
    notes,
    link: { href: `${ledgerPeriod(summary)}&type=expense`, label: t("breakdown.viewSpending") },
  };
}

export function savingExplanation(summary: CycleSummary, money: Money, words: Words): ExplanationData {
  const { t } = words;
  const parts = summary.tabungan;
  const together = t("breakdown.togetherTotal", { spending: money(summary.spending.amount), total: money(summary.keluar.total) });
  const link = { href: `${ledgerPeriod(summary)}&kakeibo=saving`, label: t("breakdown.viewSaved") };
  if (parts) {
    const lines: ExplanationLine[] = [
      { label: t("breakdown.intoSavings"), amount: signed(parts.masuk, money) },
      { label: t("breakdown.takenFromSavings"), amount: signed(-parts.diambil, money) },
      { label: t("breakdown.spendingFromSavings"), note: t("breakdown.stillCountedAsSpending"), amount: signed(-parts.belanja, money) },
    ];
    if (parts.pilar_tabungan) lines.push({ label: t("breakdown.savingsPillarSpending"), amount: signed(parts.pilar_tabungan, money) });
    return {
      title: t("terms.savedInvested"),
      rule: t("breakdown.savingsRule"),
      lines,
      total: { label: parts.total < 0 ? t("home.captionSavingsDown") : t("home.captionSavingsUp"), amount: money(Math.abs(parts.total)) },
      notes: [together],
      link,
    };
  }
  return {
    title: t("terms.savedInvested"),
    rule: t("breakdown.savedRule"),
    lines: summary.fresh_saving.lines.map((line) => lineOf(line, money, words)),
    total: { label: t("breakdown.totalSaved"), amount: money(summary.fresh_saving.amount) },
    notes: [together],
    link,
  };
}

export function masukExplanation(summary: CycleSummary, money: Money, words: Words): ExplanationData {
  const { t } = words;
  return {
    title: t("breakdown.incomeTitle", { period: periodBasis(summary) }),
    rule: t("breakdown.incomeRule"),
    lines: summary.masuk.lines.map((line) => lineOf(line, money, words)),
    total: { label: t("breakdown.totalIncome"), amount: money(summary.masuk.total) },
    notes: [t("breakdown.incomeNote")],
    link: { href: `${ledgerPeriod(summary)}&type=income`, label: t("breakdown.viewIncome") },
  };
}

function scheduledDetailLines(summary: CycleSummary, money: Money, { t, format }: Words): ExplanationLine[] {
  return summary.scheduled.lines.map((line) =>
    line.kind === "plan"
      ? {
          label: t("breakdown.billLine", { label: line.label }),
          note: line.overdue
            ? t("breakdown.dueOnOverdue", { date: format.shortDate(line.due_date) })
            : t("breakdown.dueOn", { date: format.shortDate(line.due_date) }),
          amount: `−${money(line.amount)}`,
        }
      : {
          label: t("breakdown.goalDepositLine", { label: line.label }),
          note: t("breakdown.goalDepositNote", { amount: money(line.set_aside), funded: money(line.funded) }),
          amount: `−${money(line.amount)}`,
        },
  );
}

function expectedBasisNote(summary: CycleSummary, { t, format }: Words): string | null {
  const expected = summary.expected_salary;
  if (!expected.date || expected.basis !== "regular_receipts") return null;
  const receipts = expected.receipt_dates.map(format.shortDate).join(", ");
  return t("breakdown.expectedBasis", { date: format.longDate(expected.date), receipts });
}

function waitingNote(summary: CycleSummary, money: Money, { t }: Words): string | null {
  const { waiting } = summary;
  if (!waiting.count) return null;
  return t("breakdown.waitingNote", { count: waiting.count, total: money(waiting.total), included: money(waiting.in_keluar) });
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

export function scheduledExplanation(summary: CycleSummary, money: Money, words: Words): ExplanationData {
  const { t, format } = words;
  const { scheduled } = summary;
  const window =
    scheduled.window_basis === "expected_salary"
      ? t("breakdown.windowExpectedSalary", { date: format.shortDate(scheduled.window_end) })
      : t("breakdown.windowNext30Days", { date: format.shortDate(scheduled.window_end) });
  return {
    title: t("terms.scheduled"),
    rule: t("breakdown.scheduledRule"),
    lines: scheduledDetailLines(summary, money, words).map((line) => ({ ...line, amount: line.amount.replace("−", "") })),
    total: { label: t("breakdown.totalScheduled"), amount: money(scheduled.total) },
    notes: [window, t("breakdown.plansNote")],
    link: { href: "/goals", label: t("breakdown.openGoals") },
  };
}

/** Where a waiting row's amount already sits, in the words of the figures it is counted in. */
function waitingPlace(line: WaitingLine, t: Translate): string {
  if (!line.in_keluar) return t("breakdown.notCountedYet");
  if (line.in_keluar_as === "saving") return t("breakdown.countedInSaved");
  return t("breakdown.countedInSpendingAs", { pillar: bucketName(t, line.in_keluar_as ?? "unclassified") });
}

/** A waiting row's label: the stored text, or the fixed fallback from its key (D7). */
function waitingLabel(line: WaitingLine, t: Translate): string {
  if (line.label_source !== "system") return line.label;
  return line.label_key === "uncategorized" ? t("terms.noCategory") : t("terms.noDescription");
}

function waitingDisclosure(summary: CycleSummary, money: Money, { t }: Words): RincianData["waiting"] {
  const { waiting } = summary;
  if (!waiting.count) return null;
  return {
    label: t("breakdown.waiting"),
    amount: money(waiting.total),
    lines: waiting.lines.map((line) => ({
      label: waitingLabel(line, t),
      note: t("breakdown.waitingLine", { section: t(SECTION_KEYS[line.section]), place: waitingPlace(line, t) }),
      amount: money(line.amount),
    })),
    link: { href: "/review", label: t("breakdown.answerInReview") },
  };
}

/** A capitalised sentence from a reason that is written to follow a colon. */
function sentence(text: string): string {
  return text.charAt(0).toUpperCase() + text.slice(1);
}

/** The note under the Rincian's total: the expected-salary basis, or why the period is over or the date unknown. */
function basisNote(summary: CycleSummary, money: Money, words: Words): string | null {
  if (summary.remaining.total < 0) return words.t("breakdown.overNote", { amount: money(-summary.remaining.total) });
  if (summary.safe_to_spend.status !== "available") return sentence(unavailableReason(summary, words).reason);
  return expectedBasisNote(summary, words);
}

/** A cost line of the Rincian: "−Rp …", or Rp 0 without a sign. */
function cost(amount: number, money: Money): string {
  return amount > 0 ? `−${money(amount)}` : money(0);
}

/**
 * The Rincian of "Aman dipakai sampai gajian" (apply-markas-beranda D5): Masuk less Pengeluaran, Ditabung &
 * investasi and Terjadwal, each opening the server lines behind it, the total, its notes and the waiting rows.
 */
export function safeAmountRincian(summary: CycleSummary, money: Money, words: Words): RincianData {
  const { t, format } = words;
  const { masuk, spending, fresh_saving: saving, scheduled, remaining } = summary;
  const period = periodBasis(summary);
  const savingLines = savingExplanation(summary, money, words);
  const scheduledLines = scheduledExplanation(summary, money, words);
  return {
    title: t("terms.safeToSpendUntilPayday"),
    rule: t("breakdown.safeRule", { period }),
    lines: [
      { key: "masuk", label: t("terms.income"), amount: money(masuk.total), parts: masukExplanation(summary, money, words).lines },
      { key: "spending", label: t("terms.spending"), amount: cost(spending.amount, money), parts: spendingExplanation(summary, money, words).lines },
      saving.amount < 0
        ? { key: "saving", label: t("home.captionSavingsDown"), amount: money(-saving.amount), parts: savingLines.lines }
        : { key: "saving", label: t("terms.savedInvested"), amount: cost(saving.amount, money), parts: savingLines.lines },
      {
        key: "scheduled",
        label: t("breakdown.scheduledUntil", { date: format.shortDate(scheduled.window_end) }),
        amount: cost(scheduled.total, money),
        parts: scheduledLines.lines,
        partNotes: scheduledLines.notes?.slice(0, 1),
      },
    ],
    // An over period ends in what it is over by, then the result Rp 0, so "Aman dipakai" is never negative.
    over: remaining.total < 0 ? { label: t("breakdown.moreThanIncome"), amount: money(-remaining.total) } : null,
    total: { label: t("terms.safeToSpend"), amount: money(Math.max(0, remaining.total)) },
    notes: [basisNote(summary, money, words), waitingNote(summary, money, words)].filter((note): note is string => Boolean(note)),
    waiting: waitingDisclosure(summary, money, words),
    link: { href: ledgerPeriod(summary), label: t("breakdown.viewTransactions", { period }) },
  };
}
