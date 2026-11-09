import { describe, expect, it } from "vitest";
import { KAKEIBO_PILLARS, pillarOrNeed } from "./kakeibo";

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
});

describe("unclassified bucket", () => {
  it("adds Belum diklasifikasi after the five pillars, never as a pillar", async () => {
    const { KAKEIBO_BUCKETS, isKakeiboBucket, isKakeiboPillar, kakeiboBucketInfo, pillarOrUnclassified } = await import("./kakeibo");
    expect(KAKEIBO_BUCKETS.map((bucket) => bucket.value)).toEqual(["need", "want", "culture", "unexpected", "saving", "unclassified"]);
    expect(kakeiboBucketInfo("unclassified")?.label).toBe("Belum diklasifikasi");
    expect([isKakeiboBucket("unclassified"), isKakeiboPillar("unclassified")]).toEqual([true, false]);
    expect([pillarOrUnclassified(null), pillarOrUnclassified("want"), pillarOrUnclassified("luxury")]).toEqual(["", "want", ""]);
  });
});

describe("kakeibo names in both languages (add-language-system D4, D17)", () => {
  it("names every bucket with its glossary term and description in English and Indonesian", async () => {
    const { testWords } = await import("@/lib/i18n/testing");
    const { bucketName } = await import("@/components/beranda/words");
    const { KAKEIBO_TERMS, KAKEIBO_BUCKETS } = await import("./kakeibo");
    const en = testWords("en").t;
    const id = testWords("id").t;
    expect(Object.keys(KAKEIBO_TERMS)).toEqual(KAKEIBO_BUCKETS.map((bucket) => bucket.value));
    expect(KAKEIBO_BUCKETS.map((bucket) => bucketName(en, bucket.value))).toEqual(["Needs", "Wants", "Culture", "Unexpected", "Savings", "No pillar yet"]);
    expect(KAKEIBO_BUCKETS.map((bucket) => bucketName(id, bucket.value))).toEqual(KAKEIBO_BUCKETS.map((bucket) => bucket.label));
    expect(KAKEIBO_BUCKETS.map((bucket) => id(`kakeibo.${KAKEIBO_TERMS[bucket.value].help}`))).toEqual(KAKEIBO_BUCKETS.map((bucket) => bucket.help));
    expect(en("kakeibo.needHelp")).toBe("Everyday essentials: daily meals, groceries, regular transport, bills, installments.");
    expect(bucketName(en, "mystery")).toBe("mystery");
  });
});
