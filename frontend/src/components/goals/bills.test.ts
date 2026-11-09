import { describe, expect, it } from "vitest";
import type { PlanLine } from "@/components/beranda/types";
import type { CreditBill } from "@/lib/creditBills";
import { billRows, billTotals, creditBillRows, mergeBillRows } from "./bills";

const plan: PlanLine = { kind: "plan", rule_id: "r1", label: "Internet Rumah", due_date: "2026-11-10", overdue: false, amount: 330_000 };
const charge = (key: string, date: string | null, existing: boolean) => ({
  key, merchant: key, cadence: "monthly" as const, typical_amount: 250_000, next_expected_date: date, existing_plan: existing ? { id: "rule" } : null,
});
const bill = (fields: Partial<CreditBill>): CreditBill => ({
  account_id: "spay", account_name: "SPayLater", due_date: "2026-11-08", statement_date: "2026-10-25", amount: 434_000, paid: 0, left_to_pay: 434_000,
  status: "due_today", settled_on: null, ...fields,
});

describe("bill rows", () => {
  it("adds charges seen recurring that no plan covers, from today to the window end, as estimates", () => {
    const rows = billRows([plan], [
      charge("Token Listrik", "2026-11-05", false),
      charge("Sudah lewat", "2026-11-01", false),
      charge("Sesudah jendela", "2026-12-20", false),
      charge("Sudah direncanakan", "2026-11-06", true),
      charge("Tanpa tanggal", null, false),
    ], "2026-11-04", "2026-11-30");
    expect(rows.map((row) => [row.kind, row.label, row.date])).toEqual([
      ["charge", "Token Listrik", "2026-11-05"],
      ["plan", "Internet Rumah", "2026-11-10"],
    ]);
  });

  it("marks plans overdue, due today or scheduled and shows a credit-bill plan's full amount", () => {
    const rows = billRows([
      { ...plan, rule_id: "a", due_date: "2026-11-01", overdue: true },
      { ...plan, rule_id: "b", due_date: "2026-11-04" },
      { ...plan, rule_id: "c", amount: 310_000, plan_amount: 434_000, counted_elsewhere: 124_000, credit_account_id: "spay" },
    ], [], "2026-11-04", "2026-11-30");
    expect(rows.map((row) => (row.kind === "plan" ? [row.status, row.shown, row.counted, row.countedElsewhere] : null))).toEqual([
      ["overdue", 330_000, 330_000, 0],
      ["dueToday", 330_000, 330_000, 0],
      ["scheduled", 434_000, 310_000, 124_000],
    ]);
  });

  it("keeps unpaid credit bills outside the count and total and drops one a plan already stands for", () => {
    const lines: PlanLine[] = [{ ...plan, due_date: "2026-11-08", amount: 310_000, plan_amount: 434_000, counted_elsewhere: 124_000, credit_account_id: "spay" }];
    const credit = creditBillRows([
      bill({}),
      bill({ account_id: "kartu", account_name: "Kartu", due_date: "2026-11-02", status: "overdue", left_to_pay: 15_000 }),
      bill({ account_id: "lunas", account_name: "Lunas", status: "paid", left_to_pay: 0 }),
    ], lines, "2026-11-08");
    expect(credit.map((row) => [row.label, row.shown])).toEqual([["Kartu", 15_000]]);
    const plain = billRows(lines, [], "2026-11-08", "2026-11-30");
    const rows = mergeBillRows(plain, credit);
    expect(rows.map((row) => row.kind)).toEqual(["credit", "plan"]);
    expect(billTotals(rows)).toEqual({ count: 1, total: 310_000 });
  });
});
