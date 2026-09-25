import { describe, expect, it } from "vitest";
import { queryKeys } from "./queryKeys";

describe("queryKeys", () => {
  it("groups dashboard views under one invalidation prefix", () => {
    expect(queryKeys.dashboard.overview("cycle", 0).slice(0, 1)).toEqual(
      queryKeys.dashboard.all,
    );
    expect(queryKeys.dashboard.analytics("month", 1).slice(0, 1)).toEqual(
      queryKeys.dashboard.all,
    );
    expect(queryKeys.dashboard.netWorth.slice(0, 1)).toEqual(
      queryKeys.dashboard.all,
    );
  });

  it("groups ledger results under the transaction invalidation prefix", () => {
    expect(queryKeys.transactions.ledger("type=expense").slice(0, 1)).toEqual(
      queryKeys.transactions.all,
    );
  });

  it("separates archived category management data while preserving invalidation grouping", () => {
    expect(queryKeys.categoryManagement).toEqual(["categories", "management"]);
    expect(queryKeys.categoryManagement.slice(0, 1)).toEqual(queryKeys.categories);
    expect(queryKeys.categoryManagement).not.toEqual(queryKeys.categories);
  });
});
