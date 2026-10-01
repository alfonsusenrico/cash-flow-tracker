import { describe, expect, it } from "vitest";
import { canConvertExpenseToTopup, canSwitchToAmountTracking, listAmountInvestmentProducts } from "./investmentTopups";

describe("amount product choices", () => {
  it("excludes unit tracking, archived products and aggregate parents and deduplicates nested products", () => {
    const product = { id: "fund", name: "Fund", type: "investment", instrument_type: "mutual_fund", units: null, parent_id: "bibit" };
    const accounts = [
      { id: "bibit", name: "Bibit", type: "investment", instrument_type: "mutual_fund", children: [product] },
      product,
      { ...product, id: "tracked", units: 0 },
      { ...product, id: "archived", is_archived: true },
      { ...product, id: "stock", instrument_type: "stock" },
    ];
    expect(listAmountInvestmentProducts(accounts).map((item) => item.id)).toEqual(["fund"]);
  });
});

describe("amount tracking switch", () => {
  const unitFund = { id: "fund", name: "Fund", type: "investment", instrument_type: "mutual_fund", units: 2871.1295 };

  it("offers the switch only for active unit-tracked mutual-fund leaves", () => {
    expect(canSwitchToAmountTracking(unitFund)).toBe(true);
    expect(canSwitchToAmountTracking({ ...unitFund, units: 0 })).toBe(true);
    expect(canSwitchToAmountTracking({ ...unitFund, units: null })).toBe(false);
    expect(canSwitchToAmountTracking({ ...unitFund, investment_tracking_mode: "amount" })).toBe(false);
    expect(canSwitchToAmountTracking({ ...unitFund, instrument_type: "stock" })).toBe(false);
    expect(canSwitchToAmountTracking({ ...unitFund, is_archived: true })).toBe(false);
    expect(canSwitchToAmountTracking({ ...unitFund, children: [{ ...unitFund, id: "child" }] })).toBe(false);
  });
});

describe("expense to top-up conversion", () => {
  const rdn = { id: "rdn", name: "RDN BCA", type: "bank" };
  const accounts = [rdn, { id: "bibit", name: "Bibit", type: "investment", children: [{ id: "fund", name: "Fund", type: "investment", instrument_type: "mutual_fund" }] }];
  const debit = { type: "expense", account_id: "rdn" };

  it("accepts a standalone liquid expense", () => {
    expect(canConvertExpenseToTopup(debit, accounts)).toBe(true);
  });

  it.each([
    ["income", { type: "income" }],
    ["movement leg", { movement_id: "m" }],
    ["goal", { goal_id: "g" }],
    ["debt", { obligation_id: "o" }],
    ["debt split", { obligation_allocations: [{}] }],
    ["recurring", { recurring_rule_id: "r" }],
    ["investment source", { account_id: "fund" }],
    ["unknown source", { account_id: "missing" }],
  ])("refuses %s", (_, change) => {
    expect(canConvertExpenseToTopup({ ...debit, ...change }, accounts)).toBe(false);
  });
});
