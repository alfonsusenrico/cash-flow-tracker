"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { Icon } from "@/components/ui/Icon";
import { api } from "@/lib/api";
import { queryKeys } from "@/lib/queryKeys";

interface Readiness {
  ready: boolean;
  period: { label: string; end_date: string } | null;
  url: string | null;
}

// The notice points to a freshly closed period only; after two weeks the report stays in the menu.
const FRESH_DAYS = 14;

function daysSince(isoDate: string, today = new Date()): number {
  const [year, month, day] = isoDate.split("-").map(Number);
  const end = Date.UTC(year, month - 1, day);
  const now = Date.UTC(today.getFullYear(), today.getMonth(), today.getDate());
  return Math.round((now - end) / 86_400_000);
}

/** "Laporan Oktober 2026 siap dibaca" on Beranda right after a period closes. */
export function ReportReadyNotice() {
  const { data } = useQuery<Readiness>({
    queryKey: queryKeys.reports.latest,
    queryFn: () => api.get("/reports/monthly/latest"),
    staleTime: 60 * 60 * 1000,
  });
  if (!data?.ready || !data.period || !data.url || daysSince(data.period.end_date) > FRESH_DAYS) return null;
  return (
    <Link
      href={data.url}
      className="flex min-h-11 items-center justify-between gap-3 rounded-2xl border border-[var(--border)] bg-[var(--surface)] px-4 py-3 text-sm shadow-xs focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-[var(--primary)]"
    >
      <span className="min-w-0">
        <span className="block font-semibold text-[var(--text)]">Laporan {data.period.label} siap dibaca</span>
        <span className="block text-xs text-[var(--text-secondary)]">Uang masuk, tabungan, belanja, dan satu catatan untuk bulan depan.</span>
      </span>
      <Icon name="arrow-right" className="h-4 w-4 shrink-0 text-[var(--muted)]" />
    </Link>
  );
}
