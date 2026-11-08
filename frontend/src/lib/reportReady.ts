export interface ReportReadiness {
  /** The report's period basis; absent from an API older than the language change's bell. */
  basis?: "cycle" | "month";
  ready: boolean;
  period: { label: string; start_date?: string; end_date: string } | null;
  url: string | null;
}

// A freshly closed period is news for two weeks; after that the report simply stays in Laporan.
export const REPORT_FRESH_DAYS = 14;

function daysSince(isoDate: string, today: Date): number {
  const [year, month, day] = isoDate.split("-").map(Number);
  const end = Date.UTC(year, month - 1, day);
  const now = Date.UTC(today.getFullYear(), today.getMonth(), today.getDate());
  return Math.round((now - end) / 86_400_000);
}

/** The latest report when it is ready and still fresh, else null (the Laporan badge and notification rule). */
export function freshReport(data: ReportReadiness | undefined, today = new Date()) {
  if (!data?.ready || !data.period || !data.url) return null;
  if (daysSince(data.period.end_date, today) > REPORT_FRESH_DAYS) return null;
  return {
    label: data.period.label,
    basis: data.basis ?? "month",
    startDate: data.period.start_date,
    endDate: data.period.end_date,
    url: data.url,
  };
}
