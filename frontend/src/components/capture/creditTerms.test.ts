import { describe, expect, it } from "vitest";
import { derivedFirstDueDate, instalmentAmounts, jakartaDate, statementClose } from "./creditTerms";

describe("credit due date preview (credit-accounts Credit Due Dates, apply-markas-catat D10)", () => {
  it.each([
    ["2026-11-05", 1, 11, "2026-12-11"],
    ["2026-12-01", 1, 11, "2027-01-11"],
    ["2026-11-20", 25, 5, "2026-12-05"],
    ["2026-11-25", 25, 5, "2027-01-05"],
  ])("a purchase on %s with bill day %i and payment day %i is first due %s", (purchase, statement, payment, due) => {
    expect(derivedFirstDueDate(purchase, statement, payment)).toBe(due);
  });

  it("uses bill day 1 when the account has none", () => {
    expect(statementClose("2026-11-05", null)).toBe("2026-12-01");
    expect(derivedFirstDueDate("2026-11-05", null, 11)).toBe("2026-12-11");
  });

  it("puts a purchase on the bill day into the next bill", () => {
    expect(statementClose("2026-11-01", 1)).toBe("2026-12-01");
    expect(statementClose("2026-11-25", 25)).toBe("2026-12-25");
  });

  it("clamps days to short months", () => {
    // Bill day 31 closes on 28 February; payment day 31 is due on 30 April.
    expect(statementClose("2027-02-10", 31)).toBe("2027-02-28");
    expect(statementClose("2028-02-10", 31)).toBe("2028-02-29");
    expect(derivedFirstDueDate("2027-03-31", 31, 31)).toBe("2027-04-30");
    expect(derivedFirstDueDate("2027-01-31", 31, 5)).toBe("2027-03-05");
  });

  it("has no due date without a payment day", () => {
    expect(derivedFirstDueDate("2026-11-05", 1, null)).toBeNull();
    expect(derivedFirstDueDate("2026-11-05", 1, undefined)).toBeNull();
  });

  it("reads the purchase date in Asia/Jakarta", () => {
    // 23.30 and 00.30 in Jakarta (UTC+7).
    expect(jakartaDate("2026-11-30T16:30:00Z")).toBe("2026-11-30");
    expect(jakartaDate("2026-11-30T17:30:00Z")).toBe("2026-12-01");
    expect(derivedFirstDueDate(jakartaDate("2026-11-30T17:30:00Z"), 1, 11)).toBe("2027-01-11");
  });
});

describe("instalment amounts", () => {
  it("puts the remainder on the first instalment", () => {
    expect(instalmentAmounts(1_000_000, 3)).toEqual([333_334, 333_333, 333_333]);
    expect(instalmentAmounts(120_000, 1)).toEqual([120_000]);
  });

  it("always adds up to the amount, for 1 to 36 instalments", () => {
    for (const amount of [1, 999, 1_000_000, 1_234_567_890]) {
      for (let count = 1; count <= 36; count += 1) {
        const parts = instalmentAmounts(amount, count);
        expect(parts).toHaveLength(count);
        expect(parts.reduce((sum, part) => sum + part, 0)).toBe(amount);
        expect(parts.slice(1).every((part) => part === parts[1])).toBe(true);
      }
    }
  });
});
