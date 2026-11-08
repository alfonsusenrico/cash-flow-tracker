"use client";

import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTranslations } from "next-intl";
import { ExplanationDialog, type RincianData } from "@/components/workspace/ExplanationDialog";
import { HeroFigure } from "@/components/workspace/figures";
import { Panel } from "@/components/workspace/lists";
import { MaskedMoney, useMoney } from "@/components/workspace/money";
import { EmptyState, ErrorState } from "@/components/workspace/primitives";
import { api } from "@/lib/api";
import { reportPeriodFilters, ledgerHref } from "@/lib/ledgerFilters";
import { type ReportComparison, type ReportComparisonResponse, comparisonQuery } from "@/lib/monthlyReport";
import { queryKeys } from "@/lib/queryKeys";
import type { ReportReadiness } from "@/lib/reportReady";
import { BillsPanel, ReportLink } from "./BillsPanel";
import { CategorySpending } from "./CategorySpending";
import { ComparisonRows, inSentence } from "./ComparisonRows";
import { DailySpendingChart } from "./DailySpendingChart";
import { KakeiboRows } from "./KakeiboRows";
import { useAnalysisPeriods, usePeriodName } from "./period";
import type { AnalyticsResponse, PeriodBasis, ServerExplanation } from "./types";

type Money = (amount: number) => string;

function signedLine(amount: number, money: Money): string {
  return amount < 0 ? `−${money(-amount)}` : money(amount);
}

/**
 * Ringkasan (apply-markas-analisis D4 to D10): Pengeluaran as the one hero figure with one line and one "Rincian",
 * the whole-period comparison rows, Kakeibo per pillar, daily spending and spending by category, beside the panel
 * with bills and the monthly report. Every money figure comes from the server.
 */
export function RingkasanTab({ basis, offset, onWindowLabel }: { basis: PeriodBasis; offset: number; onWindowLabel: (label: string) => void }) {
  const t = useTranslations("analysis");
  const money = useMoney();
  const { viewed, periods } = useAnalysisPeriods(basis, offset);
  const analytics = useQuery<AnalyticsResponse>({
    queryKey: queryKeys.dashboard.analytics(basis, offset),
    queryFn: () => api.get(`/dashboard/analytics?timeframe=${basis}&cycle_offset=${offset}`),
  });
  const comparisonResult = useQuery<ReportComparisonResponse>({
    queryKey: queryKeys.reports.comparison(basis, viewed?.start_date ?? null),
    queryFn: () => api.get(comparisonQuery(basis, viewed?.start_date ?? null)),
    enabled: Boolean(viewed),
    retry: false,
  });
  const latest = useQuery<ReportReadiness>({ queryKey: queryKeys.reports.latest, queryFn: () => api.get("/reports/monthly/latest"), staleTime: 60 * 60 * 1000 });
  const windowLabel = analytics.data?.timeframe_label ?? null;
  useEffect(() => {
    if (windowLabel) onWindowLabel(windowLabel);
  }, [windowLabel, onWindowLabel]);
  const periodName = usePeriodName(basis, viewed, windowLabel);

  const comparison = comparisonResult.data && !comparisonResult.data.empty ? comparisonResult.data.comparison : null;
  const comparisonLoading = periods.isPending || (Boolean(viewed) && comparisonResult.isPending);
  const comparisonFailed = periods.isError || comparisonResult.isError;
  const report = latest.data?.ready && latest.data.period && latest.data.url ? { label: latest.data.period.label, url: latest.data.url } : null;

  const panel = (
    <Panel label={t("page.context")}>
      <BillsPanel money={money} />
      {report && <ReportLink label={report.label} url={report.url} />}
    </Panel>
  );

  let reading;
  if (analytics.isPending || comparisonLoading) {
    reading = <RingkasanSkeleton />;
  } else if (analytics.isError) {
    reading = <ErrorState message={t("states.overviewError")} onRetry={() => analytics.refetch()} />;
  } else {
    const data = analytics.data;
    const spending = comparison?.totals.spending ?? data.explanations?.spending?.total ?? null;
    const empty = spending === 0 && (data.daily_cadence ?? []).every((day) => day.expense === 0);
    const categories = (
      <CategorySpending categories={data.category_variance ?? []} period={reportPeriodFilters(basis, offset)} money={money} />
    );
    reading = empty ? (
      <div className="stack">
        <EmptyState title={t("states.empty")} />
        {categories}
      </div>
    ) : (
      <div className="stack">
        <Hero
          basis={basis}
          offset={offset}
          periodName={periodName}
          running={comparison?.running ?? viewed?.complete === false}
          spending={spending}
          comparison={comparison}
          explanation={data.explanations?.spending ?? null}
          failed={comparisonFailed}
          onRetry={() => {
            periods.refetch();
            comparisonResult.refetch();
          }}
          money={money}
        />
        {comparison && <KakeiboRows comparison={comparison} money={money} />}
        <DailySpendingChart days={data.daily_cadence ?? []} money={money} />
        {categories}
      </div>
    );
  }

  return (
    <div className="cols an-cols">
      {reading}
      {panel}
    </div>
  );
}

function RingkasanSkeleton() {
  return (
    <div className="stack" role="status" aria-busy="true">
      <div>
        <span className="skel is-label" style={{ width: 200 }} />
        <span className="skel is-hero" style={{ width: 240 }} />
        <span className="skel is-line" style={{ width: 320 }} />
        <span className="skel an-skel-row" />
        <span className="skel an-skel-row" />
      </div>
      <span className="skel an-skel-chart" />
    </div>
  );
}

function Hero({
  basis,
  offset,
  periodName,
  running,
  spending,
  comparison,
  explanation,
  failed,
  onRetry,
  money,
}: {
  basis: PeriodBasis;
  offset: number;
  periodName: string | null;
  running: boolean;
  spending: number | null;
  comparison: ReportComparison | null;
  explanation: ServerExplanation | null;
  failed: boolean;
  onRetry: () => void;
  money: Money;
}) {
  const t = useTranslations("analysis");
  const terms = useTranslations("terms");
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const name = periodName ? inSentence(periodName, basis) : "";
  const label = running ? t("hero.labelRunning", { period: name }) : t("hero.label", { period: name });
  // The Rincian is offered only when the server's lines add up to the figure shown (analysis-workspace).
  const rincianSource = explanation && spending !== null && explanation.total === spending ? explanation : null;
  const rincian: RincianData | null = rincianSource ? {
    title: label,
    rule: t("hero.rule"),
    lines: rincianSource.lines.map((line) => ({ key: line.key, label: line.label, amount: signedLine(line.amount, money) })),
    total: { label: t("hero.total"), amount: money(rincianSource.total) },
    notes: [
      ...rincianSource.lines.filter((line) => line.note).map((line) => `${line.label}: ${line.note}`),
      ...(rincianSource.caveat ? [rincianSource.caveat] : []),
    ],
    link: {
      href: rincianSource.href ?? ledgerHref({ ...(reportPeriodFilters(basis, offset) ?? {}), type: "expense" }),
      label: t("hero.viewRows"),
    },
  } : null;
  const line = comparison
    ? t.rich("hero.line", {
      income: () => <MaskedMoney text={money(comparison.totals.inflow)} className="t-amt hero-amt" />,
      saved: () => <MaskedMoney text={money(comparison.totals.fresh_saving)} className="t-amt hero-amt" />,
    })
    : undefined;
  return (
    <HeroFigure
      id="an-hero"
      label={label}
      action={rincian ? (
        <button
          ref={triggerRef}
          type="button"
          className="ws-btn ws-btn-quiet ws-btn-sm"
          aria-haspopup="dialog"
          aria-label={t("hero.breakdownLabel")}
          onClick={() => setOpen(true)}
        >
          {terms("breakdown")}
        </button>
      ) : undefined}
      figure={spending === null ? <span className="is-words">{t("hero.unavailable")}</span> : <MaskedMoney text={money(spending)} />}
      line={line}
    >
      <div className="an-compare">
        {failed ? <ErrorState message={t("states.compareError")} onRetry={onRetry} /> : comparison && <ComparisonRows comparison={comparison} money={money} />}
      </div>
      <ExplanationDialog
        open={open}
        onClose={() => {
          setOpen(false);
          triggerRef.current?.focus();
        }}
        data={open ? rincian : null}
      />
    </HeroFigure>
  );
}
