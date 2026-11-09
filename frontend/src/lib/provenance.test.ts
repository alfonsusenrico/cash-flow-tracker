import { describe, expect, it } from "vitest";
import { categoryOffsetsSpending, isHabitRecorded, offsetCategoryOptions } from "./provenance";

describe("habit records", () => {
  it("treats only habit-classified rows with a basis as habit records", () => {
    expect(isHabitRecorded("habit", { agreeing: 3, support: 3 })).toBe(true);
    expect(isHabitRecorded("owner", { agreeing: 3, support: 3 })).toBe(false);
    expect(isHabitRecorded("habit", null)).toBe(false);
  });
});

describe("reimbursement offsets", () => {
  const categories = [
    { id: "dinner", kind: "expense" },
    { id: "movement", kind: "expense", is_excluded_from_budget: true },
    { id: "old", kind: "expense", is_archived: true },
    { id: "legacy-movement", name: "Transfer", kind: "expense", system_key: "internal_movement" },
    { id: "renamed-investment", name: "Investments", kind: "expense", system_key: "investment" },
    { id: "later-investasi", name: "Investasi", kind: "expense", system_key: null },
    { id: "salary", kind: "income" },
    { id: "repayment", kind: "income", offsets_spending: true },
  ];

  it("offers only active, reportable expense categories", () => {
    expect(offsetCategoryOptions(categories).map((category) => category.id)).toEqual(["dinner", "later-investasi"]);
  });

  it("detects a category that offsets spending", () => {
    expect(categoryOffsetsSpending(categories, "repayment")).toBe(true);
    expect(categoryOffsetsSpending(categories, "salary")).toBe(false);
    expect(categoryOffsetsSpending(categories, null)).toBe(false);
  });
});
