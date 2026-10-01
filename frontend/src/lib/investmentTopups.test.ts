import { describe, expect, it } from "vitest";
import { listAmountInvestmentProducts } from "./investmentTopups";

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
