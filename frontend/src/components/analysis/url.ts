import type { AnalysisTab, FlowView } from "./types";

export const ANALYSIS_TABS: AnalysisTab[] = ["ringkasan", "kalender", "aliran"];

export const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

/** A missing or unknown `tab` is Ringkasan, so `/insights` and every older link keep working (analysis-workspace). */
export function parseTab(value: string | null): AnalysisTab {
  return ANALYSIS_TABS.includes(value as AnalysisTab) ? (value as AnalysisTab) : "ringkasan";
}

/**
 * The Analisis URL after a change of `tab`, `day` or `view`: every other parameter of `current` is kept
 * (analysis-workspace "Analisis Tabs in the URL"); Ringkasan writes `tab=ringkasan` only when the URL had a tab.
 */
export function analysisHref(
  tab: AnalysisTab,
  { day, view }: { day?: string | null; view?: FlowView | null } = {},
  current: URLSearchParams = new URLSearchParams(),
): string {
  const params = new URLSearchParams(current);
  if (tab === "ringkasan" && !params.has("tab")) params.delete("tab");
  else params.set("tab", tab);
  if (tab === "kalender" && day) params.set("day", day);
  else if (tab !== "kalender") params.delete("day");
  if (tab === "aliran" && view) {
    if (view === "tabel") params.set("view", "tabel");
    else params.delete("view");
  } else if (tab !== "aliran") params.delete("view");
  const query = params.toString();
  return query ? `/insights?${query}` : "/insights";
}
