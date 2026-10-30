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

  it("lists each pocket once when the API returns pockets both flat and nested", () => {
    const pocketA = { id: "pocket-a", type: "bank", parent_id: "bank" };
    const pocketB = { id: "pocket-b", type: "bank", parent_id: "bank" };
    const accounts = [
      pocketA,
      { id: "bank", type: "bank", children: [pocketA, pocketB] },
      { id: "cash", type: "cash" },
      pocketB,
    ];

    expect(listLiquidAccountChoices(accounts).map((account) => account.id)).toEqual(["bank", "pocket-a", "pocket-b", "cash"]);
  });
});
