"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { RecurringCharges } from "@/components/insights/ObservedInsights";
import { QuestionRate } from "@/components/insights/QuestionRate";
import { HabitsLearnedCard } from "@/components/analysis/HabitsLearnedCard";
import { PlansDialog } from "@/components/recurring/PlansDialog";
import { ExplainableFigure, type ExplanationData } from "@/components/workspace/explain";
import { Card, CardHeader, ErrorState, Kpi, KpiStrip, Skeleton } from "@/components/workspace/primitives";
import { Change, NoBaseline, Progress, baselineName } from "@/components/report/ComparisonParts";
import reportStyles from "@/components/report/report.module.css";
import { ReportLayout } from "@/components/report/ReportLayout";
import { type ReportComparison, type ReportComparisonResponse, type ReportPeriodList, comparisonQuery, refHref } from "@/lib/monthlyReport";
import { api } from "@/lib/api";
import { kakeiboBucketInfo } from "@/lib/kakeibo";
import { reportPeriodFilters } from "@/lib/ledgerFilters";
import { queryKeys } from "@/lib/queryKeys";
import { cn } from "@/lib/utils";
import type { ExpensePlanDraft } from "@/types/recurring";
import { CategoryBudgetSection } from "./CategoryBudgetSection";
import { shortDay, todayIso } from "./format";
import styles from "./analysis.module.css";
import { type AnalyticsResponse, type PeriodBasis, type ServerExplanation } from "./types";

type Money = (amount: number) => string;



function signedLine(amount: number, money: Money): string {
  return amount < 0 ? `−${money(-amount)}` : money(amount);
}

/** The phase 1 explanation panel content, from the server's lines; nothing is added up here (D2). */
export function serverExplanation(title: string, rule: string, data: ServerExplanation, money: Money, totalLabel: string): ExplanationData {
  return {
    title,
    rule,
    lines: data.lines.map((line) => ({ label: line.label, note: line.note, amount: signedLine(line.amount, money), href: line.href })),
    total: { label: totalLabel, amount: money(data.total) },
    notes: data.caveat ? [data.caveat] : [],
    link: data.href ? { href: data.href, label: "Lihat transaksinya" } : null,
  };
}

/**
 * The report periods of the basis, newest first, and the one the stepper offset points at. Ringkasan
 * compares whole periods with the report's rules (compare-whole-months D9): the viewed period while it
 * runs, and the last completed period against its three-period average.
 */
function useComparisons(basis: PeriodBasis, offset: number) {
  const reportBasis = basis === "cycle" ? "cycle" : "month";
  const periods = useQuery<ReportPeriodList>({
    queryKey: queryKeys.reports.periods(reportBasis),
    queryFn: () => api.get(`/reports/monthly/periods?basis=${reportBasis}`),
  });
  const ordered = [...(periods.data?.periods ?? [])].sort((left, right) => right.start_date.localeCompare(left.start_date));
  const viewed = ordered[-offset] ?? null;
  const lastCompleted = viewed?.complete ? viewed : ordered.slice(-offset + 1).find((item) => item.complete) ?? null;
  const fetchComparison = (start: string | null) => ({
    queryKey: queryKeys.reports.comparison(reportBasis, start),
    queryFn: () => api.get<ReportComparisonResponse>(comparisonQuery(reportBasis, start)),
    enabled: Boolean(start),
    retry: false,
  });
  const current = useQuery(fetchComparison(viewed?.start_date ?? null));
  const completed = useQuery(fetchComparison(lastCompleted && lastCompleted !== viewed ? lastCompleted.start_date : null));
  const pick = (data: ReportComparisonResponse | undefined) => (data && !data.empty ? data.comparison : null);
  const viewedComparison = pick(current.data);
  return {
    loading: periods.isPending || (Boolean(viewed) && current.isPending),
    failed: periods.isError || current.isError,
    retry: () => { periods.refetch(); current.refetch(); },
    viewed: viewedComparison,
    completed: lastCompleted === viewed ? viewedComparison : pick(completed.data),
  };
}

export function RingkasanTab({ basis, offset, money, onLabel }: { basis: PeriodBasis; offset: number; money: Money; onLabel: (label: string) => void }) {
  const [planDraft, setPlanDraft] = useState<ExpensePlanDraft | null>(null);
  const analytics = useQuery<AnalyticsResponse>({
    queryKey: queryKeys.dashboard.analytics(basis, offset),
    queryFn: () => api.get(`/dashboard/analytics?timeframe=${basis}&cycle_offset=${offset}`),
  });
  const comparisons = useComparisons(basis, offset);
  const periodLabel = analytics.data?.timeframe_label;
  useEffect(() => {
    if (periodLabel) onLabel(periodLabel);
  }, [periodLabel, onLabel]);

  if (analytics.isPending) {
    return (
      <div className={styles.stack} aria-busy="true">
        <Skeleton style={{ height: 104 }} />
        <Skeleton style={{ height: 280 }} />
      </div>
    );
  }
  if (analytics.isError) {
    return <ErrorState message="Ringkasan periode ini belum bisa dimuat." onRetry={() => analytics.refetch()} />;
  }
  const data = analytics.data;
  const viewed = comparisons.viewed;
  const spending = viewed?.totals.spending ?? null;
  // The explanation is shown only when its server lines add up to the figure shown (analysis-workspace).
  const serverSpending = data.explanations?.spending;
  const spendingExplanation = serverSpending && spending !== null && serverSpending.total === spending ? serverSpending : null;

  const progressCard = viewed?.running ? <ProgressCard comparison={viewed} money={money} /> : null;
  const baselineCard = comparisons.completed ? <BaselineCard comparison={comparisons.completed} money={money} /> : null;
  const daily = <DailySpendCard days={data.daily_cadence ?? []} money={money} />;
  const bills = <RecurringCharges bal={money} onCreatePlan={setPlanDraft} />;
  const asked = <QuestionRate />;
  const habits = <HabitsLearnedCard />;
  const unavailable = "Tidak tersedia";

  return (
    <div className={styles.stack}>
      <KpiStrip label="Arus kas tercatat">
        <Kpi icon="arrow-down-left" label="Masuk" value={viewed ? money(viewed.totals.inflow) : unavailable} />
        <Kpi
          icon="arrow-up-right"
          label="Pengeluaran"
          value={spending === null ? unavailable : spendingExplanation ? (
            <ExplainableFigure
              label="Pengeluaran"
              amountText={money(spending)}
              explanation={serverExplanation("Pengeluaran periode ini", "Semua pilar Kakeibo kecuali Tabungan, dikurangi ganti uang. Sama dengan Pengeluaran di Beranda dan Transaksi.", spendingExplanation, money, "Total pengeluaran")}
            />
          ) : money(spending)}
          caption="Tanpa tabungan"
        />
        <Kpi icon="target" label="Ditabung & investasi" value={viewed ? money(viewed.totals.fresh_saving) : unavailable} />
      </KpiStrip>
      {comparisons.failed && <ErrorState message="Perbandingan periode belum bisa dimuat." onRetry={comparisons.retry} />}
      {/* Running: progress and the chart left, the average and the compact cards right. Otherwise the average leads left. */}
      <ReportLayout
        wide={progressCard
          ? [[progressCard, daily], [baselineCard, bills, asked, habits]]
          : [[baselineCard, daily], [bills, asked, habits]]}
        narrow={[progressCard, baselineCard, daily, bills, asked, habits]}
      />
      <CategoryBudgetSection rawCategories={data.category_variance ?? []} reportPeriod={reportPeriodFilters(basis, offset)} bal={money} />
      <PlansDialog open={planDraft !== null} onClose={() => setPlanDraft(null)} expenseDraft={planDraft} />
    </div>
  );
}

/** The running period's spending so far against the previous whole period, in total and per pillar, without a verdict (D4). */
export function ProgressCard({ comparison, money }: { comparison: ReportComparison; money: Money }) {
  const progress = comparison.progress;
  if (!progress) return null;
  return (
    <Card labelledBy="progress-heading">
      <CardHeader
        id="progress-heading"
        icon="buckets"
        title={`${comparison.period.label} sejauh ini`}
        caption={progress.previous_label ? `Dibanding total ${progress.previous_label}, tanpa penilaian sampai periodenya selesai` : undefined}
      />
      <p style={{ margin: 0, fontWeight: 600, fontSize: 20 }} className="tabular-nums">{money(progress.spending)}</p>
      <Progress share={progress.share} label={progress.previous_label} />
      <ul className={reportStyles.facts} style={{ marginTop: 10 }}>
        {progress.pillars.filter((pillar) => pillar.amount > 0 || (pillar.previous ?? 0) > 0).map((pillar) => {
          const info = kakeiboBucketInfo(pillar.pillar);
          const unclassified = pillar.pillar === "unclassified";
          return (
            <li key={pillar.pillar} className={reportStyles.listRow}>
              <span>
                <span aria-hidden="true" className={reportStyles.dot} style={{ background: info?.color }} />
                {info?.label ?? pillar.pillar}
                {unclassified && pillar.amount > 0 && <> · <Link href="/laporan?lengkapi=1" className="ws-linkbtn">Perlu dipilih</Link></>}
              </span>
              <span>
                {money(pillar.amount)}
                {pillar.share !== null && <span className={reportStyles.progressText}> · {pillar.share}%</span>}
              </span>
            </li>
          );
        })}
      </ul>
    </Card>
  );
}

/** "{last completed} vs rata-rata 3 bulan": each pillar against its own average, as amounts (D9). */
export function BaselineCard({ comparison, money }: { comparison: ReportComparison; money: Money }) {
  const average = baselineName(comparison);
  return (
    <Card labelledBy="baseline-heading">
      <CardHeader
        id="baseline-heading"
        icon="analysis"
        title={average ? `${comparison.period.label} vs ${average}` : comparison.period.label}
        caption={comparison.baseline ? comparison.baseline.labels.join(", ") : undefined}
      />
      {!comparison.baseline ? (
        <NoBaseline basis={comparison.basis} />
      ) : (
        <ul className={reportStyles.facts}>
          {comparison.pillars.map((pillar) => {
            const info = kakeiboBucketInfo(pillar.pillar);
            return (
              <li key={pillar.pillar} className={reportStyles.listRow}>
                <span>
                  <span aria-hidden="true" className={reportStyles.dot} style={{ background: info?.color }} />
                  <Link href={refHref(pillar.ref)} className={reportStyles.amountLink}>{info?.label ?? pillar.pillar}</Link>
                  {pillar.baseline !== null && <span className={reportStyles.progressText}> · biasanya {money(pillar.baseline)}</span>}
                </span>
                <span>
                  {money(pillar.amount)} <Change change={pillar.baseline_change} />
                </span>
              </li>
            );
          })}
        </ul>
      )}
    </Card>
  );
}

/** Highest bottom offset of the hover label, in percent of the 180 px plot, leaving room for its two lines. */
const TIP_MAX_BOTTOM_PERCENT = 70;

/** Belanja harian: one bar per day up to today, the peak in the accent, today outlined, and a table (D4). */
export function DailySpendCard({ days, money, today = todayIso() }: { days: { date: string; expense: number }[]; money: Money; today?: string }) {
  const [asTable, setAsTable] = useState(false);
  // The day whose amount is shown above its bar: set by mouse hover, toggled by a tap, cleared by Esc, by the
  // mouse leaving the chart, or by a tap outside it. A lifted finger also "leaves", so only a mouse leave clears it.
  const [activeDay, setActiveDay] = useState<string | null>(null);
  const plotRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!activeDay) return;
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setActiveDay(null);
    };
    const onOutside = (event: PointerEvent) => {
      if (!plotRef.current?.contains(event.target as Node)) setActiveDay(null);
    };
    window.addEventListener("keydown", onKey);
    document.addEventListener("pointerdown", onOutside);
    return () => {
      window.removeEventListener("keydown", onKey);
      document.removeEventListener("pointerdown", onOutside);
    };
  }, [activeDay]);
  const shown = days.filter((day) => day.date <= today);
  const peak = shown.reduce<{ date: string; expense: number } | null>((best, day) => (day.expense > 0 && (!best || day.expense > best.expense) ? day : best), null);
  const max = Math.max(1, ...shown.map((day) => day.expense));
  const todayEntry = shown.find((day) => day.date === today);
  return (
    <Card labelledBy="daily-spend-heading">
      <CardHeader
        id="daily-spend-heading"
        icon="analysis"
        title="Belanja harian"
        caption="Di luar tabungan dan investasi."
        action={shown.length ? (
          <button type="button" className="ws-btn ws-btn-sm" aria-expanded={asTable} onClick={() => setAsTable((value) => !value)}>
            {asTable ? "Lihat sebagai grafik" : "Lihat sebagai tabel"}
          </button>
        ) : undefined}
      />
      {shown.length === 0 ? (
        <p className={styles.caption}>Belum ada hari di periode ini.</p>
      ) : asTable ? (
        <div className={styles.tableScroll}>
          <table className={styles.table}>
            <caption>Belanja per hari</caption>
            <thead><tr><th scope="col">Tanggal</th><th scope="col" className={styles.r}>Belanja</th></tr></thead>
            <tbody>
              {shown.map((day) => (
                <tr key={day.date}>
                  <td>{shortDay(day.date)}{day.date === today ? " · hari ini, masih berjalan" : ""}{peak?.date === day.date ? " · tertinggi" : ""}</td>
                  <td className={styles.r}>{money(day.expense)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <>
          <div className={styles.chart} aria-hidden="true">
            <div className={styles.yax}><span>{money(max)}</span><span>{money(Math.round(max / 2))}</span><span>{money(0)}</span></div>
            <div
              ref={plotRef}
              className={styles.plot}
              onPointerLeave={(event) => {
                if (event.pointerType === "mouse") setActiveDay(null);
              }}
            >
              <div className={styles.gridline} style={{ top: 0 }} />
              <div className={styles.gridline} style={{ top: "50%" }} />
              {shown.map((day, index) => (
                <div
                  key={day.date}
                  className={cn(styles.col, peak?.date === day.date && styles.peak, day.date === today && styles.today)}
                  data-active={activeDay === day.date || undefined}
                  data-edge={index === 0 ? "start" : index === shown.length - 1 ? "end" : undefined}
                  onPointerEnter={(event) => {
                    if (event.pointerType === "mouse") setActiveDay(day.date);
                  }}
                  onPointerDown={(event) => {
                    if (event.pointerType !== "mouse") setActiveDay((current) => (current === day.date ? null : day.date));
                  }}
                >
                  <div className={styles.bar} style={{ height: `${(day.expense / max) * 100}%`, minHeight: day.expense > 0 ? 2 : 0 }} />
                  {activeDay === day.date && (
                    // Capped so the label of a tall bar stays inside the plot instead of covering the card heading.
                    <span className={styles.tip} style={{ bottom: `calc(${Math.min((day.expense / max) * 100, TIP_MAX_BOTTOM_PERCENT)}% + 6px)` }} data-testid="daily-spend-tip">
                      <span>{shortDay(day.date)}{day.date === today ? " · hari ini" : ""}</span>
                      <b>{money(day.expense)}</b>
                    </span>
                  )}
                </div>
              ))}
            </div>
            <div className={styles.xax}><span>{shortDay(shown[0].date)}</span><span>{shortDay(shown[shown.length - 1].date)}</span></div>
          </div>
          <div className={styles.chartFacts}>
            {peak ? <span>Tertinggi: <b>{shortDay(peak.date)} {money(peak.expense)}</b></span> : <span>Belum ada belanja.</span>}
            {todayEntry && <span>Hari ini, masih berjalan: <b>{money(todayEntry.expense)}</b></span>}
          </div>
        </>
      )}
    </Card>
  );
}
