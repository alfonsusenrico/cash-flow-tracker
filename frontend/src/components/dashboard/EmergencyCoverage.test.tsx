import { describe, expect, it } from "vitest";
import { createTranslator } from "next-intl";
import { createFormat } from "@/lib/format";
import { CATALOGUES } from "@/lib/i18n/testing";
import type { Translate } from "@/lib/i18n/types";
import { coverageText, essentialBasisText, type EmergencyRunway } from "./EmergencyCoverage";

// The basis itself is in Target & Tagihan's Ketahanan Dana Rincian (apply-markas-target D4), tested with the page.
const t = createTranslator({ locale: "id", messages: CATALOGUES.id }) as unknown as Translate;
const format = createFormat({ language: "id", region: "ID" });

const known: EmergencyRunway = {
  runway_days: 61, runway_months: 2, coverage_months: 2, status: "critical",
  emergency_fund_balance: 4_000_000, balance_basis: "emergency_goals", emergency_goal_names: ["Cadangan Contoh"],
  monthly_primary_expense: 2_500_000, target_months: 6,
  essential_cost: {
    amount: 2_500_000, living_amount: 2_000_000, debt_minimum: 500_000, method: "median_need_periods", basis: "history",
    period_count: 2, limited_history: true, periods_used: [],
  },
};

describe("Ketahanan Dana basis text", () => {
  it("states the median basis with minimum installments and the coverage", () => {
    expect(essentialBasisText(known.essential_cost, t)).toBe("Median 2 periode, kebutuhan (need) dan yang belum diklasifikasi + cicilan minimum");
    expect(coverageText(known, t, format)).toBe("2x biaya pokok · 61 hari");
  });

  it("never invents a coverage while the cost is unknown", () => {
    const unknown: EmergencyRunway = { ...known, runway_days: null, runway_months: null, coverage_months: null, status: "unknown", monthly_primary_expense: null };
    expect(coverageText(unknown, t, format)).toBe("Belum dapat dihitung");
    expect(essentialBasisText(undefined, t)).toBe("Belum diketahui: belum ada periode lengkap dan belum ada anggaran kategori Kebutuhan.");
  });

  it("describes the budget basis and a zero balance", () => {
    expect(essentialBasisText({ ...known.essential_cost!, method: "need_budget", basis: "budget", debt_minimum: 0 }, t))
      .toBe("Dari anggaran kategori Kebutuhan (belum ada periode lengkap)");
    expect(coverageText({ ...known, status: "zero", runway_days: 0, coverage_months: 0, runway_months: 0 }, t, format)).toBe("0 hari");
  });
});
