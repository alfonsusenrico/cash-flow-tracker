import { describe, expect, it } from "vitest";
import {
  DEFAULT_LEDGER_FILTERS,
  ledgerApiParams,
  ledgerFilterQuery,
  ledgerHref,
  parseLedgerFilters,
  reportPeriodFilters,
} from "./ledgerFilters";

const CATEGORY = "11111111-2222-4333-8444-555555555555";

describe("ledger URL filters", () => {
  it("round-trips through the URL and leaves out defaults", () => {
    const filters = parseLedgerFilters(new URLSearchParams(`period=month&offset=-2&category=${CATEGORY}&kakeibo=culture&type=expense&review=auto`));
    expect(filters).toEqual({ period: "month", offset: -2, category: CATEGORY, kakeibo: "culture", type: "expense", account: "all", review: "auto" });
    expect(parseLedgerFilters(new URLSearchParams(ledgerFilterQuery(filters)))).toEqual(filters);
    expect(ledgerFilterQuery(DEFAULT_LEDGER_FILTERS)).toBe("");
    expect(ledgerHref({})).toBe("/ledger");
  });

  it("falls back to all for invalid or missing values", () => {
    expect(parseLedgerFilters(null)).toEqual(DEFAULT_LEDGER_FILTERS);
    expect(parseLedgerFilters(new URLSearchParams("period=week&offset=x&category=drop table&kakeibo=luxury&type=transfer&review=maybe&account=1"))).toEqual(DEFAULT_LEDGER_FILTERS);
    expect(parseLedgerFilters(new URLSearchParams("offset=-3")).offset).toBe(0);
    expect(parseLedgerFilters(new URLSearchParams("period=cycle&offset=-99")).offset).toBe(-36);
  });

  it("maps filters to the transactions API", () => {
    const params = ledgerApiParams(parseLedgerFilters(new URLSearchParams("period=cycle&offset=-1&category=uncategorized&type=expense")));
    expect(Object.fromEntries(params)).toEqual({ period: "cycle", period_offset: "-1", uncategorized: "true", type: "expense" });
  });

  it("links report periods only for cycle and month windows", () => {
    expect(reportPeriodFilters("month", -1)).toEqual({ period: "month", offset: -1 });
    expect(reportPeriodFilters("cycle", 0)).toEqual({ period: "cycle", offset: 0 });
    expect(reportPeriodFilters("30d", 0)).toBeNull();
    expect(ledgerHref({ period: "month", offset: -1, kakeibo: "want", type: "expense" })).toBe("/ledger?period=month&offset=-1&kakeibo=want&type=expense");
  });
});
