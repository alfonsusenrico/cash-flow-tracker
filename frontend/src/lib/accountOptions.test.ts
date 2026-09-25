import { describe, expect, it } from "vitest";
import { listLiquidAccountChoices } from "./accountOptions";

describe("account choices", () => {
  it("keeps liquid investment-funding accounts but excludes instrument positions", () => {
    const accounts = [
      { id: "cash", type: "cash" },
      { id: "rdn", type: "bank" },
      { id: "position", type: "investment", instrument_type: "stock" },
      { id: "instrument-metadata", type: "bank", instrument_type: "stock" },
    ];

    expect(listLiquidAccountChoices(accounts).map((account) => account.id)).toEqual(["cash", "rdn"]);
  });
});
