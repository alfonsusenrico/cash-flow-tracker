import { describe, expect, it } from "vitest";
import { createFormat, groupDigits } from "./format";
import { fmtIDR } from "./utils";

/** The formatter by language and region (add-language-system D6), against the design's table. */
const THURSDAY = "2026-11-05";
const EVENING = new Date("2026-11-05T11:23:00Z"); // 18.23 in Asia/Jakarta

describe("createFormat", () => {
  const idID = createFormat({ language: "id", region: "ID" });
  const enID = createFormat({ language: "en", region: "ID" });
  const enUS = createFormat({ language: "en", region: "US" });
  const idUS = createFormat({ language: "id", region: "US" });

  it("writes September with three letters in every English short month, as the server does (apply-markas-cleanup D11)", () => {
    const september = new Date("2026-09-29T05:00:00Z");
    for (const format of [enID, enUS]) {
      expect(format.shortDate("2026-09-29")).not.toContain("Sept");
      expect(format.shortMonth("2026-09-29")).toBe("Sep");
      expect(format.shortMonthYear("2026-09-29")).toBe("Sep 2026");
      expect(format.yearDate("2026-09-29")).toContain("Sep");
      expect(format.yearDate("2026-09-29")).not.toContain("Sept");
      expect(format.shortDateAt(september)).not.toContain("Sept");
      expect(format.yearDateAt(september)).not.toContain("Sept");
    }
    expect(enID.shortDate("2026-09-29")).toBe("29 Sep");
    expect(enUS.shortDate("2026-09-29")).toBe("Sep 29");
    expect(enID.shortDate("2026-10-27")).toBe("27 Oct");
    expect(idID.shortDate("2026-09-29")).toBe("29 Sep");
    expect(idID.shortDate("2026-08-29")).toBe("29 Agu");
    expect(enID.longDate("2026-09-29")).toBe("29 September");
  });

  it("writes IDR exactly as today's fmtIDR in region Indonesia, in both languages", () => {
    for (const amount of [0, 1234, -1234, 1_234_567_890]) {
      expect(idID.money(amount)).toBe(fmtIDR(amount));
      expect(enID.money(amount)).toBe(fmtIDR(amount));
    }
    expect(idID.money(1_234_567)).toBe("Rp 1.234.567");
    expect(enID.money(-1234)).toBe("-Rp 1.234");
  });

  it("groups IDR the US way in region United States: Rp 1,234,567 (OD2)", () => {
    expect(enUS.money(1_234_567)).toBe("Rp 1,234,567");
    expect(enUS.money(-1234)).toBe("-Rp 1,234");
    expect(idUS.money(1_234_567)).toBe("Rp 1,234,567");
  });

  it("writes no no-break space in any money string", () => {
    for (const format of [idID, enID, enUS, idUS]) {
      for (const amount of [0, 1234, -1234, 1_234_567_890]) expect(format.money(amount)).not.toMatch(/[  ]/);
    }
  });

  it("keeps today's en-US currency for the USD display in every locale", () => {
    for (const language of ["id", "en"] as const) {
      for (const region of ["ID", "US"] as const) {
        expect(createFormat({ language, region, currency: "USD", rate: 1000 }).money(1_234_560)).toBe("$1,234.56");
      }
    }
  });

  it("writes dates in the language with the region's order", () => {
    expect(idID.weekdayDate(THURSDAY)).toBe("Kamis, 5 November");
    expect(enID.weekdayDate(THURSDAY)).toBe("Thursday, 5 November");
    expect(enUS.weekdayDate(THURSDAY)).toBe("Thursday, November 5");
    expect(idID.shortDate(THURSDAY)).toBe("5 Nov");
    expect(enID.shortDate(THURSDAY)).toBe("5 Nov");
    expect(enUS.shortDate(THURSDAY)).toBe("Nov 5");
    expect(idID.shortDate("2026-10-31")).toBe("31 Okt");
    expect(enID.shortDate("2026-10-31")).toBe("31 Oct");
    expect(enID.weekdayAt(EVENING)).toBe("Thursday, 5 November");
    expect(idID.monthYear("2026-10-01")).toBe("Oktober 2026");
    expect(enID.shortMonthYear("2026-10-01")).toBe("Oct 2026");
  });

  it("writes the clock and decimals by region", () => {
    expect(idID.time(EVENING)).toBe("18.23");
    expect(enID.time(EVENING)).toBe("18.23");
    expect(enUS.time(EVENING)).toBe("6:23 PM");
    expect(idID.decimal(5.4)).toBe("5,4");
    expect(enID.decimal(5.4)).toBe("5,4");
    expect(enUS.decimal(5.4)).toBe("5.4");
  });

  it("writes a Tinjau row's date and time, and a group's range, by language and region (apply-markas-tinjau D18)", () => {
    const row = new Date("2026-11-04T14:40:00Z"); // 21.40 in Asia/Jakarta
    const when = (format: typeof idID) => `${format.shortDateAt(row)} ${format.time(row)}`;
    expect(when(idID)).toBe("4 Nov 21.40");
    expect(when(enID)).toBe("4 Nov 21.40");
    expect(when(enUS)).toBe("Nov 4 9:40 PM");
    expect(idID.dateRangeAt("2026-10-19T05:00:00Z", "2026-10-20T05:00:00Z")).toBe("19 Okt – 20 Okt 2026");
    expect(enUS.dateRangeAt("2026-10-19T05:00:00Z", "2026-10-20T05:00:00Z")).toBe("Oct 19 – Oct 20, 2026");
    expect(idID.dateRangeAt("2026-10-20T01:00:00Z", "2026-10-20T05:00:00Z")).toBe("20 Okt 2026");
    expect(idID.dateRangeAt(null, "2026-10-20T05:00:00Z")).toBeNull();
    expect(idID.yearDate("2026-10-03")).toBe("3 Okt 2026");
    expect(enUS.yearDate("2026-10-03")).toBe("Oct 3, 2026");
  });

  it("keeps a calendar date on its day in every time zone of the input", () => {
    expect(enID.shortDate("2026-11-01T00:00:00+07:00")).toBe("1 Nov");
  });
});

describe("amount input grouping", () => {
  it("groups by region and keeps only digits", () => {
    expect(groupDigits("20000000", "ID")).toBe("20.000.000");
    expect(groupDigits("20000000", "US")).toBe("20,000,000");
    expect(groupDigits("2.000.000", "US")).toBe("2,000,000");
    expect(groupDigits("20,000,00", "ID")).toBe("2.000.000");
    expect(groupDigits("007", "ID")).toBe("7");
    expect(groupDigits("abc", "ID")).toBe("");
  });
});

describe("compactMoney (apply-markas-analisis D22)", () => {
  const idID = createFormat({ language: "id", region: "ID" });
  const enID = createFormat({ language: "en", region: "ID" });
  const enUS = createFormat({ language: "en", region: "US" });

  it("writes thousands and millions short in each language", () => {
    expect(idID.compactMoney(80_500)).toBe("81rb");
    expect(enID.compactMoney(80_500)).toBe("81K");
    expect(idID.compactMoney(1_303_500)).toBe("1,3jt");
    expect(enUS.compactMoney(1_303_500)).toBe("1.3M");
    expect(enID.compactMoney(1_303_500)).toBe("1,3M");
    expect(idID.compactMoney(1_000_000)).toBe("1jt");
    expect(enID.compactMoney(1_000_000)).toBe("1M");
    expect(idID.compactMoney(999_600)).toBe("1jt");
    expect(idID.compactMoney(500)).toBe("500");
    expect(idID.compactMoney(1_234_567_890)).toBe("1,2M");
    expect(enUS.compactMoney(1_234_567_890)).toBe("1.2B");
  });

  it("drops the decimal in the narrow form and masks hidden amounts", () => {
    expect(idID.compactMoney(1_303_500, { narrow: true })).toBe("1jt");
    expect(idID.compactMoney(1_303_500, { hidden: true })).toBe("••");
  });

  it("converts the USD display before shortening", () => {
    const usd = createFormat({ language: "id", region: "ID", currency: "USD", rate: 1000 });
    expect(usd.compactMoney(1_300_000)).toBe("$1.3K");
    expect(usd.compactMoney(50_000)).toBe("$50");
  });
});
