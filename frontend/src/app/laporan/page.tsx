"use client";

import { Suspense, useEffect, useState } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { usePathname, useSearchParams } from "next/navigation";
import styles from "@/components/analysis/analysis.module.css";
import { useAppCtx } from "@/components/layout/AppLayout";
import { PeriodStepper } from "@/components/ui/PeriodStepper";
import { TopBarControls } from "@/components/workspace/context";
import { Card, CardHeader, EmptyState, ErrorState, Pill, Segmented, Skeleton } from "@/components/workspace/primitives";
import { reportLead } from "@/lib/reportLead";
import { replaceSearch } from "@/lib/urlState";
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
  type ReportPeriod,
  type ReportPeriodList,
  reportQuery,
  shortDate,
} from "@/lib/monthlyReport";
import { queryKeys } from "@/lib/queryKeys";

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
  const { bal } = useAppCtx();
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
  // Newest first, so the stepper's offset 0 is the latest report and −n steps back n reports.
  const ordered = [...(periods.data?.periods ?? [])].sort((left, right) => right.start_date.localeCompare(left.start_date));
  const shownStart = report?.period.start_date ?? null;
  const position = Math.max(0, ordered.findIndex((item) => item.start_date === shownStart));

  const controls = (
    <TopBarControls>
      <div className={styles.toolbar}>
        <Segmented
          label="Dasar periode"
          options={BASIS_OPTIONS.filter((option) => option.value === "month" || cycleAvailable)}
          value={basis}
          onChange={(value) => navigate({ basis: value, start: null })}
        />
        {ordered.length > 0 && (
          <PeriodStepper
            unit={basis === "cycle" ? "Siklus" : "Bulan"}
            label={report?.period.label ?? "Memuat…"}
            offset={-position}
            minOffset={-(ordered.length - 1)}
            onChange={(offset) => navigate({ start: offset === 0 ? null : ordered[-offset]?.start_date ?? null })}
          />
        )}
      </div>
    </TopBarControls>
  );

  if (reportQueryResult.isLoading) {
    return (
      <div className={styles.root}>
        {controls}
        <p role="status" className={styles.caption}>Menyiapkan laporan…</p>
        <Skeleton style={{ height: 160 }} />
      </div>
    );
  }
  if (reportQueryResult.isError || !reportQueryResult.data) {
    return (
      <div className={styles.root}>
        {controls}
        <ErrorState
          message={(reportQueryResult.error as Error | null)?.message || "Laporan belum bisa dimuat."}
          onRetry={() => reportQueryResult.refetch()}
        />
      </div>
    );
  }
  if (!report) {
    return (
      <div className={styles.root}>
        {controls}
        <Card>
          <EmptyState icon="file-text" title="Belum ada laporan">
            Laporan pertama muncul setelah ada transaksi tercatat. Catat pengeluaran atau sambungkan ponsel untuk mulai.
          </EmptyState>
        </Card>
      </div>
    );
  }

  const note = periodNote(report);
  const issues = dataQualityIssues(report) > 0;
  const openClassify = () => setClassifyOpen(true);
  const lead = reportLead(report, bal);

  const side = (
    <>
      <ComparisonPanel report={report} />
      {!issues && <DataQualityPanel report={report} onClassify={openClassify} />}
      <BudgetPanel report={report} onEdit={() => setBudgetOpen(true)} />
      <PastReports periods={ordered} current={shownStart} basis={basis} />
    </>
  );

  return (
    <div className={styles.root}>
      {controls}
      <div className={styles.grid}>
        <div className={styles.stack}>
          <Card labelledBy="report-title">
            <p className={styles.kicker}>{basis === "cycle" ? "Laporan siklus gaji" : "Laporan bulan kalender"} · {shortDate(report.period.start_date)} – {shortDate(report.period.end_date)}</p>
            <h2 id="report-title" className={styles.reportTitle}>Laporan {report.period.label}</h2>
            {lead.length > 0 && (
              <p className={styles.reportLead} data-testid="report-lead">
                {lead.map((sentence) => (
                  <span key={sentence.key}>
                    {sentence.text}{" "}
                    {sentence.key === "unclassified" && (
                      <button type="button" className={styles.linkButton} onClick={openClassify}>Lengkapi klasifikasi</button>
                    )}
                  </span>
                ))}
              </p>
            )}
            {note && <p className={styles.caption} style={{ marginTop: 10 }}>{note}</p>}
          </Card>
          {/* Something to fix comes right after the lead, on every width. */}
          {issues && <DataQualityPanel report={report} onClassify={openClassify} />}
          <IncomeAnswer report={report} />
          <SavingsAnswer report={report} />
          <SpendingAnswer report={report} onClassify={openClassify} />
          <ReflectionAnswer report={report} />
        </div>
        <div className={styles.stack}>{side}</div>
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

/** "Laporan sebelumnya": each period with whether it is complete, linking to its report (D11). */
function PastReports({ periods, current, basis }: { periods: ReportPeriod[]; current: string | null; basis: ReportBasis }) {
  if (periods.length < 2) return null;
  return (
    <Card labelledBy="past-reports-heading">
      <CardHeader id="past-reports-heading" icon="file-text" title="Laporan sebelumnya" />
      <div>
        {periods.map((item) => (
          <Link
            key={item.start_date}
            className={styles.reportRow}
            href={`/laporan?basis=${basis}&start=${item.start_date}`}
            aria-current={item.start_date === current ? "page" : undefined}
          >
            <span style={{ flex: 1 }}>
              <b>{item.label}</b>
              <span>{shortDate(item.start_date)} – {shortDate(item.end_date)}{item.has_reflection ? " · ada catatan" : ""}</span>
            </span>
            <Pill tone={item.complete ? "ok" : "neutral"}>{item.complete ? "Lengkap" : "Berjalan"}</Pill>
          </Link>
        ))}
      </div>
    </Card>
  );
}

export default function LaporanPage() {
  return (
    <Suspense fallback={null}>
      <ReportView />
    </Suspense>
  );
}
