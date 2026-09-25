import { describe, expect, it } from "vitest";
import { parseRecurringRuleForm } from "./recurringForm";

const baseRule = {
  name: "  Tabungan bulanan  ",
  type: "transfer",
  amount: 250_000,
  sourceAccountId: "source-account",
  targetAccountId: "savings-account",
  categoryId: null,
  obligationId: null,
  scheduleType: "weekly",
  scheduleDay: 1,
  isPayrollAllocation: false,
  autoPost: false,
  isActive: true,
  notes: "",
};

describe("recurringRuleFormSchema", () => {
  it("normalizes a valid weekly rule and accepts Monday through Sunday", () => {
    const parsed = parseRecurringRuleForm(baseRule);

    expect(parsed.success).toBe(true);
    if (!parsed.success) return;
    expect(parsed.data.name).toBe("Tabungan bulanan");
    expect(parsed.data.scheduleDay).toBe(1);
    expect(parseRecurringRuleForm({ ...baseRule, scheduleDay: 7 }).success).toBe(true);
  });

  it("rejects incompatible references and invalid schedule combinations", () => {
    const wrongKind = parseRecurringRuleForm({
      ...baseRule,
      type: "income",
      targetAccountId: null,
      isPayrollAllocation: false,
      categoryId: null,
      obligationId: "debt",
    });
    const invalidWeekday = parseRecurringRuleForm({ ...baseRule, scheduleDay: 8 });
    const sameTransferAccount = parseRecurringRuleForm({
      ...baseRule,
      targetAccountId: "source-account",
    });

    expect(wrongKind.success).toBe(false);
    expect(invalidWeekday.success).toBe(false);
    expect(sameTransferAccount.success).toBe(false);
  });
});
