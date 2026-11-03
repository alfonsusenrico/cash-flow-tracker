import type { FlowView } from "./flow/PetaAliranTab";
import type { AnalysisTab } from "./types";

export const ANALYSIS_TABS: { value: AnalysisTab; label: string }[] = [
  { value: "ringkasan", label: "Ringkasan" },
  { value: "kalender", label: "Kalender" },
  { value: "aliran", label: "Peta Aliran" },
];

export const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

/** A missing or unknown `tab` is Ringkasan, so `/insights` and every older link keep working (D1). */
export function parseTab(value: string | null): AnalysisTab {
  return ANALYSIS_TABS.some((tab) => tab.value === value) ? (value as AnalysisTab) : "ringkasan";
}

/** The Analisis URL: `tab`, the calendar `day` and the map `view` (D1, D14); Ringkasan has no parameter. */
export function analysisHref(tab: AnalysisTab, { day, view }: { day?: string | null; view?: FlowView | null } = {}): string {
  const params = new URLSearchParams();
  if (tab !== "ringkasan") params.set("tab", tab);
  if (tab === "kalender" && day) params.set("day", day);
  if (tab === "aliran" && view === "tabel") params.set("view", "tabel");
  const query = params.toString();
  return query ? `/insights?${query}` : "/insights";
}
