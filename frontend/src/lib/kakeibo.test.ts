import { describe, expect, it } from "vitest";
import {
  KAKEIBO_PILLARS,
  kakeiboGroups,
  kakeiboLabel,
  pillarOrNeed,
  type KakeiboBreakdown,
} from "./kakeibo";

const breakdown: KakeiboBreakdown = {
  need_spent: 400_000, need_pct: 40,
  want_spent: 250_000, want_pct: 25,
  culture_spent: 150_000, culture_pct: 15,
  unexpected_spent: 0, unexpected_pct: 0,
  saving_spent: 200_000, saving_pct: 20,
  essentials_pct: 40, lifestyle_pct: 40,
  kakeibo_status: "warning",
};

describe("kakeibo vocabulary", () => {
  it("lists the five pillars in report order with Indonesian labels", () => {
    expect(KAKEIBO_PILLARS.map((pillar) => [pillar.value, pillar.label])).toEqual([
      ["need", "Kebutuhan"],
      ["want", "Keinginan"],
      ["culture", "Budaya"],
      ["unexpected", "Tak Terduga"],
      ["saving", "Tabungan"],
    ]);
  });

  it("defaults unknown or missing pillars to need, like the backend", () => {
    expect(pillarOrNeed("culture")).toBe("culture");
    expect(pillarOrNeed(null)).toBe("need");
    expect(pillarOrNeed("luxury")).toBe("need");
  });

  it("labels known pillars and leaves anything else readable", () => {
    expect(kakeiboLabel("unexpected")).toBe("Tak Terduga");
    expect(kakeiboLabel(null)).toBe("");
  });
});

describe("kakeiboGroups", () => {
  it("groups need with unexpected and want with culture against 50/30/20", () => {
    const [essentials, lifestyle, saving] = kakeiboGroups(breakdown);
    expect(essentials).toMatchObject({ key: "essentials", spent: 400_000, pct: 40, withinTarget: true });
    expect(lifestyle).toMatchObject({ key: "lifestyle", spent: 400_000, pct: 40, withinTarget: false });
    expect(saving).toMatchObject({ key: "saving", spent: 200_000, pct: 20, withinTarget: true });
  });

  it("sums pillar percentages when an older response has no group fields", () => {
    const legacy: KakeiboBreakdown = {
      need_spent: 300_000, need_pct: 30, want_spent: 100_000, want_pct: 10,
      saving_spent: 50_000, saving_pct: 5, kakeibo_status: "healthy",
    };
    const [essentials, lifestyle, saving] = kakeiboGroups(legacy);
    expect([essentials.pct, lifestyle.pct, saving.pct]).toEqual([30, 10, 5]);
    expect(saving.withinTarget).toBe(false);
  });

  it("reports zero rather than invented values when there is no data", () => {
    expect(kakeiboGroups(undefined).map((group) => [group.spent, group.pct])).toEqual([[0, 0], [0, 0], [0, 0]]);
  });
});
