import { describe, expect, it } from "vitest";
import { defaultMovementTargetId, listCreditAccountChoices, listLiquidAccountChoices } from "./accountOptions";

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

  it("keeps credit accounts out of the liquid choices and lists each one once on its own", () => {
    const credit = { id: "spaylater", type: "credit" };
    const accounts = [{ id: "cash", type: "cash" }, credit, credit, { id: "position", type: "investment" }];
    expect(listLiquidAccountChoices(accounts).map((account) => account.id)).toEqual(["cash"]);
    expect(listCreditAccountChoices(accounts).map((account) => account.id)).toEqual(["spaylater"]);
  });
});

describe("Perpindahan default Ke (apply-markas-cleanup D13)", () => {
  const bca = (defaultPocket: string | null) => ({ id: "bca", type: "bank", default_pocket_id: defaultPocket });
  const utama = { id: "utama", type: "bank", parent_id: "bca" };
  const harian = { id: "harian", type: "bank", parent_id: "bca" };
  const jago = { id: "jago", type: "bank" };

  it("skips the pocket a parent Dari resolves to and every account with pockets", () => {
    const liquid = [bca("utama"), utama, harian, jago];
    expect(defaultMovementTargetId("bca", liquid)).toBe("harian");
    expect(defaultMovementTargetId("utama", liquid)).toBe("harian");
    expect(defaultMovementTargetId("harian", liquid)).toBe("utama");
    expect(defaultMovementTargetId("jago", liquid)).toBe("utama");
  });

  it("skips every pocket of a parent Dari without a default pocket", () => {
    expect(defaultMovementTargetId("bca", [bca(null), utama, harian, jago])).toBe("jago");
  });

  it("stays inside one account family when that is all there is", () => {
    expect(defaultMovementTargetId("bca", [bca("utama"), utama, harian])).toBe("harian");
  });

  it("leaves Ke empty when nothing qualifies", () => {
    expect(defaultMovementTargetId("bca", [bca(null), utama, harian])).toBe("");
    expect(defaultMovementTargetId("jago", [jago])).toBe("");
  });

  it("keeps the former first-other-account default for standalone accounts", () => {
    expect(defaultMovementTargetId("cash-a", [{ id: "cash-a", type: "cash" }, { id: "cash-b", type: "bank" }])).toBe("cash-b");
  });
});
