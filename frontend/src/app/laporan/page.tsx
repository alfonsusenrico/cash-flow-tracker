"use client";

import { Suspense, useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { usePathname, useSearchParams } from "next/navigation";
import { useTranslations } from "next-intl";
import { BudgetDialog } from "@/components/categories/BudgetDialog";
import { ClassificationDialog } from "@/components/report/ClassificationDialog";
import { ImprovementQuestion } from "@/components/report/ImprovementQuestion";
import { IncomeQuestion } from "@/components/report/IncomeQuestion";
import { ReportContext } from "@/components/report/ReportContext";
import { ReportHero } from "@/components/report/ReportHero";
import { SavingQuestion } from "@/components/report/SavingQuestion";
import { SpendingQuestion } from "@/components/report/SpendingQuestion";
import { PeriodStepper } from "@/components/ui/PeriodStepper";
import { PageActions } from "@/components/workspace/context";
import { useMoney } from "@/components/workspace/money";
import { EmptyState, ErrorState, Segmented } from "@/components/workspace/primitives";
import { api } from "@/lib/api";
import {
  type MonthlyReportResponse,
  type ReportBasis,
  type ReportPeriodList,
  reportQuery,
} from "@/lib/monthlyReport";
import { queryKeys } from "@/lib/queryKeys";
import { replaceSearch } from "@/lib/urlState";

const DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;

export default function LaporanPage() {
  return (
    <Suspense fallback={null}>
      <ReportView />
    </Suspense>
  );
}

/**
 * Laporan (apply-markas-laporan): the basis segment and the stepper in the title block, then one reading article
 * (the Pengeluaran hero and four numbered Kakeibo questions) beside the context panel from 1200 px and above it
 * below. Every figure is the server's; the URL keeps `basis`, `start`, `lengkapi` and `atur`.
 */
function ReportView() {
  const t = useTranslations("report");
  const terms = useTranslations("terms");
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const money = useMoney();
  const basis: ReportBasis = searchParams?.get("basis") === "cycle" ? "cycle" : "month";
  const rawStart = searchParams?.get("start") ?? null;
  const start = rawStart && DATE_PATTERN.test(rawStart) ? rawStart : null;
  const [classifyOpen, setClassifyOpen] = useState(searchParams?.get("lengkapi") === "1");
  const [budgetOpen, setBudgetOpen] = useState(searchParams?.get("atur") === "anggaran");
  const classifyRef = useRef<HTMLButtonElement>(null);
  const budgetRef = useRef<HTMLButtonElement>(null);

  const periods = useQuery<ReportPeriodList>({
    queryKey: queryKeys.reports.periods(basis),
    queryFn: () => api.get(`/reports/monthly/periods?basis=${basis}`),
  });
  const reportResult = useQuery<MonthlyReportResponse>({
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

  const report = reportResult.data && !reportResult.data.empty ? reportResult.data : null;
  const documentTitle = report ? t("page.documentTitle", { period: report.period.label }) : null;
  useEffect(() => {
    if (documentTitle) document.title = documentTitle;
  }, [documentTitle]);

  const cycleAvailable = periods.data?.cycle_available ?? reportResult.data?.cycle_available ?? false;
  // Newest first, so the stepper's offset 0 is the latest report and −n steps back n reports.
  const ordered = [...(periods.data?.periods ?? [])].sort((left, right) => right.start_date.localeCompare(left.start_date));
  // After a failure the stepper still names the requested period, from the list, so it never reads "Memuat…".
  const requested = reportResult.isError ? ordered.find((item) => (start ? item.start_date === start : item.complete)) ?? null : null;
  const shown = report?.period ?? requested;
  const shownStart = shown?.start_date ?? null;
  const position = Math.max(0, ordered.findIndex((item) => item.start_date === shownStart));
  const options: { value: ReportBasis; label: string }[] = [
    { value: "month", label: terms("calendarMonth") },
    { value: "cycle", label: terms("payCycle") },
  ];

  const controls = (
    <PageActions>
      <Segmented
        label={t("page.basis")}
        options={options.filter((option) => option.value === "month" || cycleAvailable)}
        value={basis}
        onChange={(value) => navigate({ basis: value, start: null })}
      />
      {ordered.length > 0 && (
        <PeriodStepper
          unit={basis === "cycle" ? "Siklus" : "Bulan"}
          monthStyle="long"
          label={shown?.label ?? null}
          period={shown ? { start: shown.start_date, end: shown.complete ? shown.end_date : null } : null}
          offset={-position}
          minOffset={-(ordered.length - 1)}
          // Without `start` the API opens the latest closed period, so the running one needs its date.
          onChange={(offset) => navigate({ start: offset === 0 && ordered[0]?.complete ? null : ordered[-offset]?.start_date ?? null })}
        />
      )}
    </PageActions>
  );

  let body;
  if (reportResult.isPending) {
    body = <ReportSkeleton label={t("states.loading")} />;
  } else if (reportResult.isError || !reportResult.data) {
    body = <ErrorState message={(reportResult.error as Error | null)?.message || t("states.error")} onRetry={() => reportResult.refetch()} />;
  } else if (!report) {
    body = <EmptyState title={t("states.emptyTitle")}>{t("states.emptyText")}</EmptyState>;
  } else {
    body = (
      <div className="cols lp-cols">
        <article className="lp-article">
          <ReportHero report={report} money={money} />
          <IncomeQuestion report={report} money={money} />
          <SavingQuestion report={report} money={money} />
          <SpendingQuestion report={report} money={money} onEditBudget={() => setBudgetOpen(true)} budgetRef={budgetRef} />
          <ImprovementQuestion report={report} money={money} />
        </article>
        <ReportContext
          report={report}
          periods={ordered}
          money={money}
          onClassify={() => setClassifyOpen(true)}
          classifyRef={classifyRef}
          onOpenPeriod={(startDate) => navigate({ start: startDate })}
        />
      </div>
    );
  }

  return (
    <div className="lp-page">
      {controls}
      {body}
      {report && (
        <ClassificationDialog
          open={classifyOpen}
          onClose={() => {
            setClassifyOpen(false);
            classifyRef.current?.focus();
          }}
          scope={{ start: report.period.start, end: report.period.end }}
        />
      )}
      <BudgetDialog
        open={budgetOpen}
        onClose={() => {
          setBudgetOpen(false);
          budgetRef.current?.focus();
        }}
      />
    </div>
  );
}

/** The hero and four question blocks as static bars while the report loads (D16). */
function ReportSkeleton({ label }: { label: string }) {
  return (
    <div className="cols lp-cols">
      <div className="lp-article" role="status" aria-busy="true" aria-label={label}>
        <div>
          <span className="skel is-label" style={{ width: 200 }} />
          <span className="skel is-hero" style={{ width: 240 }} />
          <span className="skel is-line" style={{ width: 320 }} />
        </div>
        {[1, 2, 3, 4].map((number) => (
          <div key={number} className="q lp-skel-q">
            <span className="skel lp-skel-num" />
            <div>
              <span className="skel lp-skel-h" />
              <span className="skel an-skel-row" />
              <span className="skel an-skel-row" />
              <span className="skel an-skel-row" />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
