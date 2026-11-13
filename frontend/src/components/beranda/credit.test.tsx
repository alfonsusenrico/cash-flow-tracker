import { describe, expect, it, vi } from "vitest";
import { testWords } from "@/lib/i18n/testing";
import type { CreditBill } from "@/lib/creditBills";
import type { RecurringRule } from "@/types/recurring";
import { todoItems } from "./TodoSection";

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
