import { describe, expect, it, vi } from "vitest";
import { testWords } from "@/lib/i18n/testing";
import type { CreditBill } from "@/lib/creditBills";
import type { RecurringRule } from "@/types/recurring";
import { safeAmountRincian, spendingExplanation } from "./explanations";
import { todoItems } from "./TodoSection";
import type { CycleSummary } from "./types";

const TODAY = "2026-11-05";
const words = testWords();
const plan = (id: string, name: string, due: string): RecurringRule => ({
  id, name, type: "expense", amount: 100_000, source_account_id: "jago", schedule_type: "monthly_day", is_payroll_allocation: false, auto_post: false,
  next_due_date: due, is_active: true,
});
const bill = (account: string, name: string, due: string, left: number, status: CreditBill["status"]): CreditBill => ({
  account_id: account, account_name: name, due_date: due, statement_date: "2026-10-25", amount: left, paid: 0, left_to_pay: left, status, settled_on: null,
});
const queue = { questions: [{ transaction: { id: "q1", merchant: "QRIS Contoh", notes: null, type: "expense", amount: 25_000, date: "2026-11-05T03:00:00Z" } }], awaiting: [], ambiguous: [], groups: [] } as never;

describe("Perlu tindakan with credit bills (apply-markas-target D16)", () => {
  it("Credit bill ranked with plans: question, then plans and bills of the same urgency by due date and name", () => {
    const items = todoItems({
      queue,
      plans: [plan("p-yoga", "Kelas Yoga", "2026-11-02"), plan("p-iuran", "Iuran", TODAY), plan("p-net", "Internet", "2026-11-06")],
      creditBills: [
        bill("bca", "Kartu BCA", "2026-10-30", 1_250_000, "overdue"),
        bill("spay", "SPayLater", TODAY, 434_000, "due_today"),
        bill("akulaku", "Akulaku", "2026-11-06", 90_000, "upcoming"),
      ],
      autoLinks: [],
      today: TODAY,
      words,
      onOpenPlans: () => undefined,
      onPayBill: () => undefined,
    });
    expect(items.map((item) => item.title)).toEqual([
      "QRIS Contoh", "Tagihan Kartu BCA", "Kelas Yoga", "Iuran", "Tagihan SPayLater", "Tagihan Akulaku", "Internet",
    ]);
  });

  it("Paying from Perlu tindakan: a bill opens the payment with its account and the amount left, and sends nothing itself", () => {
    const pay = vi.fn();
    const items = todoItems({
      queue: undefined, plans: [], creditBills: [bill("bca", "Kartu BCA", "2026-10-30", 1_250_000, "overdue")], autoLinks: [], today: TODAY, words,
      onOpenPlans: () => undefined, onPayBill: pay,
    });
    expect(items[0].href).toBeUndefined();
    items[0].onSelect?.();
    expect(pay).toHaveBeenCalledWith(expect.objectContaining({ account_id: "bca", left_to_pay: 1_250_000 }));
  });
});

const summary = {
  period: { basis: "cycle", cycle_offset_basis: "cycle" },
  keluar: { total: 3_000_000, offsets_total: 0, lines: [] },
  spending: {
    amount: 2_000_000,
    lines: [{ key: "need", label: "Kebutuhan", label_source: "system", amount: 2_000_000 }],
    credit: {
      spent_on_credit: 579_000, paid_to_credit: 470_000,
      lines: [
        { key: "credit:spay", account_id: "spay", label: "SPayLater", label_source: "user", spent_on_credit: 579_000, paid_to_credit: 0 },
        { key: "credit:bca", account_id: "bca", label: "Kartu BCA", label_source: "user", spent_on_credit: 0, paid_to_credit: 470_000 },
      ],
    },
  },
  fresh_saving: { amount: 1_000_000, lines: [] },
  masuk: { total: 9_000_000, lines: [] },
  waiting: { count: 0, total: 0, in_keluar: 0, not_in_keluar: 0, lines: [] },
  scheduled: { total: 500_000, window_start: TODAY, window_end: "2026-11-25", window_basis: "expected_salary", lines: [] },
  remaining: { total: 5_500_000, lines: [] },
  safe_to_spend: { status: "available", reason: null, detail: null, per_day: 275_000, days_left: 20, rounding: 1000, over_by: null },
  expected_salary: { status: "estimated", date: "2026-11-25", basis: null, receipt_dates: [], reason: null },
} as unknown as CycleSummary;

describe("Credit lines in the Pengeluaran Rincian (apply-markas-target D17)", () => {
  const money = words.format.money;

  it("Credit lines: lists both after the buckets with their notes and accounts, total unchanged", () => {
    const explanation = spendingExplanation(summary, money, words);
    expect(explanation.lines.map((line) => [line.label, line.amount, line.note])).toEqual([
      ["Kebutuhan", "Rp 2.000.000", null],
      ["Pengeluaran pakai kredit", "Rp 579.000", "sudah termasuk di atas · SPayLater Rp 579.000"],
      ["Tagihan kredit dibayar", "Rp 470.000", "uang keluar, tidak dihitung lagi · Kartu BCA Rp 470.000"],
    ]);
    expect(explanation.total).toEqual({ label: "Total pengeluaran", amount: "Rp 2.000.000" });
    const rincian = safeAmountRincian(summary, money, words);
    expect(rincian.lines.find((line) => line.key === "spending")?.amount).toBe("−Rp 2.000.000");
  });

  it("No credit use: the Pengeluaran line lists the buckets only", () => {
    const without = { ...summary, spending: { ...summary.spending, credit: { spent_on_credit: 0, paid_to_credit: 0, lines: [] } } } as CycleSummary;
    expect(spendingExplanation(without, money, words).lines.map((line) => line.label)).toEqual(["Kebutuhan"]);
    const absent = { ...summary, spending: { amount: 2_000_000, lines: summary.spending.lines } } as CycleSummary;
    expect(spendingExplanation(absent, money, words).lines).toHaveLength(1);
  });
});
