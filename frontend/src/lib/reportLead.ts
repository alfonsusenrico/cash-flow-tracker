import { kakeiboLabel } from "@/lib/kakeibo";
import type { MonthlyReport } from "@/lib/monthlyReport";

/**
 * The Laporan written lead (redesign-analysis-and-reports D12): two or three neutral sentences built only
 * from the report's own figures. Amounts are the server's, passed through `money`; nothing is added or
 * subtracted here. A sentence whose figure is unavailable is left out, never filled with Rp 0. The lead is
 * a record that is re-read later, so it is impersonal and gives no verdict (monthly-report, ui-copy-register).
 */
export interface LeadSentence {
  key: "flow" | "change" | "unclassified";
  text: string;
}

export function reportLead(report: MonthlyReport, money: (amount: number) => string): LeadSentence[] {
  const sentences: LeadSentence[] = [];
  const { period, flow } = report;
  if (flow && typeof flow.inflow === "number" && typeof flow.outflow === "number") {
    const when = period.complete ? period.label : `${period.label} sampai hari ini`;
    sentences.push({ key: "flow", text: `${when}: masuk ${money(flow.inflow)}, belanja ${money(flow.outflow)}.` });
  }
  const comparison = report.comparison?.previous;
  if (comparison?.previous_has_data && comparison.previous_label) {
    const largest = comparison.pillars
      .filter((entry) => typeof entry.change === "number" && entry.change !== 0)
      .sort((left, right) => Math.abs(right.change ?? 0) - Math.abs(left.change ?? 0) || left.pillar.localeCompare(right.pillar))[0];
    if (largest && largest.change !== null) {
      const direction = largest.change > 0 ? "naik" : "turun";
      const subject = largest.pillar === "saving" ? "Tabungan" : `Pengeluaran ${kakeiboLabel(largest.pillar)}`;
      sentences.push({
        key: "change",
        text: `${subject} ${direction} ${money(Math.abs(largest.change))} dibanding ${comparison.previous_label}.`,
      });
    }
  }
  const unclassified = report.data_quality?.unclassified?.count ?? 0;
  if (unclassified > 0) {
    sentences.push({ key: "unclassified", text: `${unclassified} transaksi belum diklasifikasi.` });
  }
  return sentences;
}
