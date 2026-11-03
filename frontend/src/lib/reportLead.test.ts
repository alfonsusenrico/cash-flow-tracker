import { describe, expect, it } from "vitest";
import type { MonthlyReport } from "@/lib/monthlyReport";
import { reportLead } from "./reportLead";

const money = (amount: number) => `Rp ${amount.toLocaleString("id-ID")}`;
const hidden = () => "Rp ••••";

function report(overrides: Record<string, unknown> = {}): MonthlyReport {
  return {
    period: { label: "Oktober 2026", complete: true },
    flow: { inflow: 8_200_000, gross_outflow: 4_910_000, reimbursement_offset: 0, outflow: 4_910_000, net: 3_290_000 },
    comparison: {
      previous: {
        previous_has_data: true, previous_label: "September 2026",
        pillars: [
          { pillar: "need", current: 2_000_000, previous: 2_100_000, change: -100_000 },
          { pillar: "want", current: 700_000, previous: 1_010_000, change: -310_000 },
          { pillar: "saving", current: 1_500_000, previous: 1_400_000, change: 100_000 },
        ],
      },
    },
    data_quality: { unclassified: { count: 2, amount: 90_000 } },
    ...overrides,
  } as unknown as MonthlyReport;
}

const SLANG = /\b(kamu|aku|anda|nggak|udah|gak|banget|aja)\b|-mu\b/i;
const VERDICT = /\b(hemat|boros|bagus|buruk|hebat|sayang)\b/i;

describe("reportLead", () => {
  it.each([
    ["complete", report(), [
      "Oktober 2026: masuk Rp 8.200.000, belanja Rp 4.910.000.",
      "Pengeluaran Keinginan turun Rp 310.000 dibanding September 2026.",
      "2 transaksi belum diklasifikasi.",
    ]],
    ["incomplete", report({ period: { label: "November 2026", complete: false }, data_quality: { unclassified: { count: 0, amount: 0 } } }), [
      "November 2026 sampai hari ini: masuk Rp 8.200.000, belanja Rp 4.910.000.",
      "Pengeluaran Keinginan turun Rp 310.000 dibanding September 2026.",
    ]],
    ["no comparison", report({ comparison: { previous: { previous_has_data: false, previous_label: null, pillars: [] } } }), [
      "Oktober 2026: masuk Rp 8.200.000, belanja Rp 4.910.000.",
      "2 transaksi belum diklasifikasi.",
    ]],
    ["unavailable figure", report({ flow: undefined, data_quality: { unclassified: { count: 0, amount: 0 } } }), [
      "Pengeluaran Keinginan turun Rp 310.000 dibanding September 2026.",
    ]],
    ["saving moved most", report({ comparison: { previous: { previous_has_data: true, previous_label: "September 2026", pillars: [{ pillar: "saving", current: 2_000_000, previous: 1_000_000, change: 1_000_000 }] } } }), [
      "Oktober 2026: masuk Rp 8.200.000, belanja Rp 4.910.000.",
      "Tabungan naik Rp 1.000.000 dibanding September 2026.",
      "2 transaksi belum diklasifikasi.",
    ]],
  ])("builds the %s lead from the report's figures only", (_name, input, expected) => {
    const sentences = reportLead(input, money).map((sentence) => sentence.text);
    expect(sentences).toEqual(expected);
    for (const sentence of sentences) {
      expect(sentence).not.toMatch(SLANG);
      expect(sentence).not.toMatch(VERDICT);
      expect(sentence).not.toContain("Rp 0");
    }
  });

  it("keeps hidden balances hidden", () => {
    const text = reportLead(report(), hidden).map((sentence) => sentence.text).join(" ");
    expect(text).toContain("masuk Rp ••••, belanja Rp ••••");
    expect(text).not.toMatch(/\d{3}\.\d{3}/);
  });
});
