import { describe, expect, it } from "vitest";
import {
  chipOf,
  listsLogicalMovements,
  withChip,
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
    expect(filters).toEqual({ period: "month", offset: -2, category: CATEGORY, kakeibo: "culture", type: "expense", account: "all", review: "auto", from: null, to: null, q: "", dup: false, source: "all", waiting: false });
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

  it("carries the unclassified bucket and possible duplicates", () => {
    const filters = parseLedgerFilters(new URLSearchParams("kakeibo=unclassified&type=expense&dup=1"));
    expect([filters.kakeibo, filters.dup]).toEqual(["unclassified", true]);
    expect(Object.fromEntries(ledgerApiParams(filters))).toEqual({ kakeibo_type: "unclassified", type: "expense", possible_duplicates: "true" });
    expect(ledgerFilterQuery(filters)).toBe("kakeibo=unclassified&type=expense&dup=1");
    expect(hasRowFilters(parseLedgerFilters(new URLSearchParams("dup=1")))).toBe(true);
    expect(parseLedgerFilters(new URLSearchParams("dup=yes")).dup).toBe(false);
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

  it("maps the Transaksi tabs and chips onto the URL and the API", () => {
    const base = parseLedgerFilters(new URLSearchParams("period=cycle&type=movement"));
    const waiting = withChip(base, "waiting");
    expect(ledgerFilterQuery(waiting)).toBe("period=cycle&type=movement&waiting=1");
    expect(ledgerApiParams(waiting).get("waiting")).toBe("true");
    expect(ledgerApiParams(waiting).get("type")).toBe("movement");
    const phone = withChip(waiting, "phone");
    expect(chipOf(phone)).toBe("phone");
    expect(ledgerFilterQuery(phone)).toBe("period=cycle&type=movement&source=notification");
    expect(chipOf(withChip(phone, "phone"))).toBe("none");
    expect(ledgerFilterQuery(withChip(base, "unreviewed"))).toBe("period=cycle&type=movement&review=auto");
  });

  it("keeps one row per movement only without row filters", () => {
    const parse = (query: string) => parseLedgerFilters(new URLSearchParams(query));
    expect(listsLogicalMovements(parse("period=cycle&type=investment"), "")).toBe(true);
    expect(listsLogicalMovements(parse("period=cycle"), "kopi")).toBe(false);
    expect(listsLogicalMovements(parse("type=expense"), "")).toBe(false);
    expect(listsLogicalMovements(parse("waiting=1"), "")).toBe(false);
  });

  it("falls back to all for unknown tab and chip values", () => {
    const filters = parseLedgerFilters(new URLSearchParams("type=everything&source=fax&waiting=yes"));
    expect([filters.type, filters.source, filters.waiting]).toEqual(["all", "all", false]);
  });
});
