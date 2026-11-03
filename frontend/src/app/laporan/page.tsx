"use client";

import { Suspense, useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { usePathname, useSearchParams } from "next/navigation";
import { replaceSearch } from "@/lib/urlState";
import { PageHeader } from "@/components/ui/PageHeader";
import { BudgetEditorDialog, BudgetPanel } from "@/components/report/BudgetPanel";
import { ClassificationDialog } from "@/components/report/ClassificationDialog";
import { ComparisonPanel } from "@/components/report/ComparisonPanel";
import { DataQualityPanel, dataQualityIssues } from "@/components/report/DataQualityPanel";
import { IncomeAnswer, SavingsAnswer } from "@/components/report/IncomeSavingsAnswers";
import { ReflectionAnswer } from "@/components/report/ReflectionAnswer";
import { SpendingAnswer } from "@/components/report/SpendingAnswer";
import { api } from "@/lib/api";
import {
  type MonthlyReport,
  type MonthlyReportResponse,
  type ReportBasis,
  type ReportPeriodList,
  reportQuery,
  shortDate,
} from "@/lib/monthlyReport";
import { queryKeys } from "@/lib/queryKeys";
import { cn } from "@/lib/utils";

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const BASIS_OPTIONS: { value: ReportBasis; label: string }[] = [
  { value: "month", label: "Bulan kalender" },
  { value: "cycle", label: "Siklus gaji" },
];

function periodNote(report: MonthlyReport): string | null {
  const { period } = report;
  if (!period.complete) return `Periode ini masih berjalan sampai hari ini. Laporan lengkapnya siap setelah ${period.label} selesai.`;
  if (period.tracking_started_inside && period.first_recorded_date) {
    return `Pencatatan baru dimulai ${shortDate(period.first_recorded_date)}, jadi periode ini belum lengkap.`;
  }
  return null;
}

function ReportView() {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const basis: ReportBasis = searchParams?.get("basis") === "cycle" ? "cycle" : "month";
  const rawStart = searchParams?.get("start") ?? null;
  const start = rawStart && DATE_PATTERN.test(rawStart) ? rawStart : null;
  const [classifyOpen, setClassifyOpen] = useState(searchParams?.get("lengkapi") === "1");
  const [budgetOpen, setBudgetOpen] = useState(searchParams?.get("atur") === "anggaran");

  const periods = useQuery<ReportPeriodList>({
    queryKey: queryKeys.reports.periods(basis),
    queryFn: () => api.get(`/reports/monthly/periods?basis=${basis}`),
  });
  const reportQueryResult = useQuery<MonthlyReportResponse>({
    queryKey: queryKeys.reports.monthly(basis, start),
    queryFn: () => api.get(reportQuery(basis, start)),
    retry: false,
  });

  const navigate = (next: { basis?: ReportBasis; start?: string | null }) => {
    const params = new URLSearchParams();
    params.set("basis", next.basis ?? basis);
    const nextStart = next.start === undefined ? start : next.start;
    if (nextStart) params.set("start", nextStart);
    // The history API, not `router.replace`: from an initial URL with a query Next.js commits no same-path
    // navigation (redesign-transactions-workspace D13, OD21).
    replaceSearch(`${pathname}?${params.toString()}`);
  };

  const report = reportQueryResult.data && !reportQueryResult.data.empty ? reportQueryResult.data : null;
  useEffect(() => {
    if (report) document.title = `Laporan ${report.period.label}`;
  }, [report]);

  const cycleAvailable = periods.data?.cycle_available ?? reportQueryResult.data?.cycle_available ?? false;
  const header = (
    <PageHeader
      eyebrow="Laporan Bulanan"
      title={report ? report.period.label : "Laporan Bulanan"}
      description="Empat pertanyaan Kakeibo untuk menutup periode: uang masuk, tabungan, belanja, dan rencana perbaikan."
    />
  );

  const controls = (
    <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
      <div role="group" aria-label="Dasar periode" className="flex w-full rounded-2xl border border-[var(--border)] bg-[var(--surface-raised)] p-1 sm:w-auto">
        {BASIS_OPTIONS.map((option) => {
          const unavailable = option.value === "cycle" && !cycleAvailable;
          return (
            <button
              key={option.value}
              type="button"
              aria-pressed={basis === option.value}
              disabled={unavailable}
              onClick={() => navigate({ basis: option.value, start: null })}
              className={cn(
                "min-h-11 flex-1 rounded-xl px-3 text-sm font-semibold focus-visible:outline-2 focus-visible:outline-[var(--primary)] disabled:cursor-not-allowed disabled:opacity-60 sm:flex-none",
                basis === option.value
                  ? "border border-[var(--border)] bg-[var(--surface)] text-[var(--text)] shadow-xs"
                  : "text-[var(--text-secondary)] hover:text-[var(--text)]",
              )}
            >
              {option.label}
            </button>
          );
        })}
      </div>
      {periods.data && periods.data.periods.length > 0 && (
        <div className="flex flex-col gap-1">
          <label htmlFor="report-period" className="text-xs font-semibold text-[var(--muted)]">Laporan lain</label>
          <select
            id="report-period"
            value={report?.period.start_date ?? ""}
            onChange={(event) => navigate({ start: event.target.value })}
            className="min-h-11 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)] px-3 text-sm text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
          >
            {periods.data.periods.map((item) => (
              <option key={item.start_date} value={item.start_date}>
                {item.label}
                {item.complete ? "" : " (berjalan)"}
                {item.has_reflection ? " · ada catatan" : ""}
              </option>
            ))}
          </select>
        </div>
      )}
    </div>
  );

  if (reportQueryResult.isLoading) {
    return (
      <div className="space-y-4">
        {header}
        <p role="status" className="text-sm text-[var(--muted)]">Menyiapkan laporan…</p>
        <div aria-hidden="true" className="h-40 animate-pulse rounded-3xl bg-[var(--border)]/30 motion-reduce:animate-none" />
      </div>
    );
  }
  if (reportQueryResult.isError || !reportQueryResult.data) {
    return (
      <div className="space-y-4">
        {header}
        {controls}
        <div role="alert" className="cockpit-card border border-[var(--border)] p-5 text-sm text-[var(--text)]">
          {(reportQueryResult.error as Error | null)?.message || "Laporan belum bisa dimuat. Coba muat ulang halaman."}
        </div>
      </div>
    );
  }
  if (!report) {
    return (
      <div className="space-y-4">
        {header}
        <div className="cockpit-card space-y-2 border border-[var(--border)] p-5">
          <h2 className="text-base font-bold text-[var(--text)]">Belum ada laporan</h2>
          <p className="text-sm text-[var(--text-secondary)]">
            Laporan pertama muncul setelah ada transaksi tercatat. Catat pengeluaran atau sambungkan ponsel untuk mulai.
          </p>
        </div>
      </div>
    );
  }

  const note = periodNote(report);
  const issues = dataQualityIssues(report) > 0;
  const openClassify = () => setClassifyOpen(true);

  return (
    <div className="space-y-4">
      {header}
      {controls}
      {note && <p className="rounded-xl border border-[var(--border)] bg-[var(--surface-raised)]/40 p-3 text-sm text-[var(--text)]">{note}</p>}

      <div className="flex flex-col gap-4 lg:grid lg:grid-flow-row-dense lg:grid-cols-12 lg:items-start">
        {/* On the phone the data-quality box comes first when there is something to fix. */}
        <div className={cn("lg:col-span-4 lg:col-start-9", issues ? "order-first" : "order-last lg:order-none")}>
          <DataQualityPanel report={report} onClassify={openClassify} />
        </div>
        <div className="space-y-4 lg:col-span-8 lg:col-start-1 lg:row-span-3 lg:row-start-1">
          <IncomeAnswer report={report} />
          <SavingsAnswer report={report} />
          <SpendingAnswer report={report} onClassify={openClassify} />
          <ReflectionAnswer report={report} />
        </div>
        <div className="space-y-4 lg:col-span-4 lg:col-start-9">
          <ComparisonPanel report={report} />
          <BudgetPanel report={report} onEdit={() => setBudgetOpen(true)} />
        </div>
      </div>

      <ClassificationDialog
        open={classifyOpen}
        onClose={() => setClassifyOpen(false)}
        scope={{ start: report.period.start, end: report.period.end }}
      />
      <BudgetEditorDialog open={budgetOpen} onClose={() => setBudgetOpen(false)} />
    </div>
  );
}

export default function LaporanPage() {
  return (
    <Suspense fallback={null}>
      <ReportView />
    </Suspense>
  );
}
