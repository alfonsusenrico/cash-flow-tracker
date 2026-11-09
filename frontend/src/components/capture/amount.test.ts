import { describe, expect, it } from "vitest";
import { isExpression, parseAmountExpression } from "./amount";

describe("inline arithmetic in the amount (tactile-pulse-ui Inline Arithmetic in the Amount)", () => {
  it("reads grouped digits", () => {
    expect(parseAmountExpression("35.000")).toBe(35_000);
    expect(isExpression("35.000")).toBe(false);
  });

  it("evaluates + - * / with precedence and without eval", () => {
    expect(parseAmountExpression("25000+15000")).toBe(40_000);
    expect(parseAmountExpression("10000+2*5000")).toBe(20_000);
    expect(parseAmountExpression("100000/3")).toBe(33_333);
    expect(parseAmountExpression("50000-20000")).toBe(30_000);
    expect(isExpression("25000+15000")).toBe(true);
  });

  it("reads nothing for an empty, broken, zero or negative expression", () => {
    expect(parseAmountExpression("")).toBe(0);
    expect(parseAmountExpression("5000*")).toBe(0);
    expect(parseAmountExpression("1000-5000")).toBe(0);
    expect(parseAmountExpression("5000/0")).toBe(0);
  });
});
