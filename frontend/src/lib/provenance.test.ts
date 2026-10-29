import { describe, expect, it } from "vitest";
import {
  categoryOffsetsSpending,
  classifierLabel,
  needsOwnerReview,
  offsetCategoryOptions,
  reviewStatusLabel,
  sourceLabel,
} from "./provenance";

describe("provenance labels", () => {
  it("names every source, review status and classifier in Indonesian", () => {
    expect(["manual", "notification", "recurring", "payroll", "movement", "reconcile", "investment", "import", "capture"].map(sourceLabel)).toEqual([
      "Dicatat manual", "Notifikasi bank", "Transaksi rutin", "Alokasi gaji", "Pindah saldo",
      "Penyesuaian saldo", "Investasi", "Impor", "Diketik di ponsel",
    ]);
    expect(["auto", "needs_review", "reviewed"].map(reviewStatusLabel)).toEqual([
      "Otomatis, belum ditinjau", "Perlu ditinjau", "Sudah ditinjau",
    ]);
    expect(["rule", "ai", "owner", "system"].map(classifierLabel)).toEqual(["Aturan", "AI", "Anda", "Sistem"]);
  });

  it("shows unknown values as sent and missing values as absent", () => {
    expect(sourceLabel("future_source")).toBe("future_source");
    expect(classifierLabel(null)).toBeNull();
  });

  it("asks for review only for rows the owner has not confirmed", () => {
    expect(needsOwnerReview("auto")).toBe(true);
    expect(needsOwnerReview("needs_review")).toBe(true);
    expect(needsOwnerReview("reviewed")).toBe(false);
    expect(needsOwnerReview(undefined)).toBe(false);
  });
});

describe("reimbursement offsets", () => {
  const categories = [
    { id: "dinner", kind: "expense" },
    { id: "movement", kind: "expense", is_excluded_from_budget: true },
    { id: "old", kind: "expense", is_archived: true },
    { id: "legacy-movement", name: "Internal Movement", kind: "expense" },
    { id: "salary", kind: "income" },
    { id: "repayment", kind: "income", offsets_spending: true },
  ];

  it("offers only active, reportable expense categories", () => {
    expect(offsetCategoryOptions(categories).map((category) => category.id)).toEqual(["dinner"]);
  });

  it("detects a category that offsets spending", () => {
    expect(categoryOffsetsSpending(categories, "repayment")).toBe(true);
    expect(categoryOffsetsSpending(categories, "salary")).toBe(false);
    expect(categoryOffsetsSpending(categories, null)).toBe(false);
  });
});
