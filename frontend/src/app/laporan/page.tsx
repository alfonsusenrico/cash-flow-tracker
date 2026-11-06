"use client";

import { Suspense, useEffect, useState } from "react";
import { ReportLayout } from "@/components/report/ReportLayout";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { usePathname, useSearchParams } from "next/navigation";
import styles from "@/components/analysis/analysis.module.css";
import { useAppCtx } from "@/components/layout/AppLayout";
import { PeriodStepper } from "@/components/ui/PeriodStepper";
import { PageActions } from "@/components/workspace/context";
import { Card, CardHeader, EmptyState, ErrorState, Kpi, KpiStrip, Pill, Segmented, Skeleton } from "@/components/workspace/primitives";
import { replaceSearch } from "@/lib/urlState";
import { BudgetEditorDialog, BudgetPanel } from "@/components/report/BudgetPanel";
import { ClassificationDialog } from "@/components/report/ClassificationDialog";
import {
  ChangeLine,
  ImprovementFacts,
  MonthlyHistory,
  NoBaseline,
  PillarTable,
  Progress,
  baselineName,
  previousShort,
} from "@/components/report/ComparisonParts";
import { DataQualityLine } from "@/components/report/DataQualityPanel";
import { IncomeSourcesCard, SavingsCard } from "@/components/report/IncomeSavingsAnswers";
import { ReflectionAnswer } from "@/components/report/ReflectionAnswer";
import { SpendingDetailsCard, TopMerchantsCard } from "@/components/report/SpendingAnswer";
import reportStyles from "@/components/report/report.module.css";
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

/** The running or partial period, said once above the figures. */
function periodNote(report: MonthlyReport): string | null {
  const { period } = report;
  const previous = report.comparison.previous;
  if (!period.complete) {
    const against = previous && !previous.partial ? ` Angka ini kemajuan terhadap total ${previous.label}, belum perbandingan.` : "";
    return `${period.label} masih berjalan.${against}`;
  }
  if (period.tracking_started_inside && period.first_recorded_date) {
    return `Pencatatan baru dimulai ${shortDate(period.first_recorded_date)}, jadi periode ini belum lengkap.`;
  }
  return null;
}

function ReportKpis({ report, money }: { report: MonthlyReport; money: (amount: number) => string }) {
  const comparison = report.comparison;
  const { totals, previous, baseline, progress } = comparison;
  const against = previousShort(comparison);
  const average = baselineName(comparison);
  if (comparison.running) {
    return (
      <KpiStrip label={`Ringkasan ${report.period.label}`}>
        <Kpi
          icon="arrow-up-right"
          label="Pengeluaran sejauh ini"
          value={money(totals.spending)}
          caption={<Progress share={progress?.share ?? null} label={progress?.previous_label ?? null} />}
        />
        <Kpi icon="arrow-down-left" label="Masuk" value={money(totals.inflow)} />
        <Kpi icon="target" label="Ditabung & investasi" value={money(totals.fresh_saving)} />
      </KpiStrip>
    );
  }
  return (
    <KpiStrip label={`Ringkasan ${report.period.label}`}>
      <Kpi
        icon="arrow-up-right"
        label="Pengeluaran"
        value={money(totals.spending)}
        caption={
          <span className={reportStyles.kpiChips}>
            {previous && against && <ChangeLine change={previous.change.spending} against={against} money={money} partial={previous.partial} />}
            {baseline && average ? <ChangeLine change={baseline.change.spending} against={average} money={money} /> : <NoBaseline basis={report.basis} />}
          </span>
        }
      />
      <Kpi
        icon="arrow-down-left"
        label="Masuk"
        value={money(totals.inflow)}
        caption={previous && against ? (
          <span className={reportStyles.kpiChips}>
            <ChangeLine change={previous.change.inflow} against={against} money={money} kind="income" partial={previous.partial} />
          </span>
        ) : undefined}
      />
      <Kpi
        icon="target"
        label="Ditabung & investasi"
        value={money(totals.fresh_saving)}
        caption={totals.inflow > 0 ? `${Math.round((totals.fresh_saving / totals.inflow) * 100)}% dari uang masuk` : undefined}
      />
      <Kpi icon="layers" label="Selisih" value={money(totals.difference)} caption="masuk dikurangi pengeluaran" />
    </KpiStrip>
  );
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
    <PageActions>
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
            // Without `start` the API opens the latest closed period, so the running one needs its date.
            onChange={(offset) => navigate({ start: offset === 0 && ordered[0]?.complete ? null : ordered[-offset]?.start_date ?? null })}
          />
        )}
      </div>
    </PageActions>
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
  const openClassify = () => setClassifyOpen(true);
  const comparison = report.comparison;
  const pillars = <PillarTable comparison={comparison} money={bal} />;
  const facts = <ImprovementFacts comparison={comparison} money={bal} />;
  const history = <MonthlyHistory comparison={comparison} money={bal} basis={basis} />;
  const merchants = <TopMerchantsCard report={report} />;
  const savingsCard = <SavingsCard report={report} />;
  const details = <SpendingDetailsCard report={report} />;
  const incomeCard = <IncomeSourcesCard report={report} />;
  const budget = <BudgetPanel report={report} onEdit={() => setBudgetOpen(true)} />;
  const reflection = <ReflectionAnswer report={report} />;
  const past = <PastReports periods={ordered} current={shownStart} basis={basis} />;

  return (
    <div className={styles.root}>
      {controls}
      <h2 className={reportStyles.srOnly} id="report-title">Laporan {report.period.label}</h2>
      {note ? <p className={reportStyles.notice} data-tone="info">{note}</p> : null}
      <DataQualityLine report={report} onClassify={openClassify} />
      <ReportKpis report={report} money={bal} />
      <ReportLayout
        wide={[[pillars, merchants, savingsCard, details, budget], [facts, history, incomeCard, reflection, past]]}
        narrow={[facts, pillars, history, merchants, savingsCard, details, incomeCard, budget, reflection, past]}
      />

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
