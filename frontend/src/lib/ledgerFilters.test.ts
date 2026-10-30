import { describe, expect, it } from "vitest";
import {
  DEFAULT_LEDGER_FILTERS,
  hasRowFilters,
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
    expect(filters).toEqual({ period: "month", offset: -2, category: CATEGORY, kakeibo: "culture", type: "expense", account: "all", review: "auto", from: null, to: null, q: "" });
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

  it("carries an exact window and search text from report links", () => {
    const from = "2026-10-01T00:00:00+07:00";
    const to = "2026-10-10T23:59:59.999999+07:00";
    const filters = parseLedgerFilters(new URLSearchParams({ from, to, kakeibo: "need", type: "expense", q: " Musik Contoh " }));
    expect([filters.from, filters.to, filters.q]).toEqual([from, to, "Musik Contoh"]);
    expect(Object.fromEntries(ledgerApiParams(filters))).toEqual({ kakeibo_type: "need", type: "expense", from_date: from, to_date: to });
    expect(parseLedgerFilters(new URLSearchParams(ledgerFilterQuery(filters)))).toEqual(filters);
    expect(hasRowFilters(filters)).toBe(true);
    // A malformed instant is dropped, and a period wins over a range.
    expect(parseLedgerFilters(new URLSearchParams("from=yesterday")).from).toBeNull();
    expect(parseLedgerFilters(new URLSearchParams(`period=month&from=${encodeURIComponent(from)}`)).from).toBeNull();
    expect(parseLedgerFilters(new URLSearchParams({ q: "x".repeat(150) })).q).toHaveLength(100);
  });

  it("links report periods only for cycle and month windows", () => {
    expect(reportPeriodFilters("month", -1)).toEqual({ period: "month", offset: -1 });
    expect(reportPeriodFilters("cycle", 0)).toEqual({ period: "cycle", offset: 0 });
    expect(reportPeriodFilters("30d", 0)).toBeNull();
    expect(ledgerHref({ period: "month", offset: -1, kakeibo: "want", type: "expense" })).toBe("/ledger?period=month&offset=-1&kakeibo=want&type=expense");
  });
});
