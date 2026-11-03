"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { CashFlowHistory, PeriodComparison, RecurringCharges } from "@/components/insights/ObservedInsights";
import { QuestionRate } from "@/components/insights/QuestionRate";
import { RecurringRulesModal } from "@/components/recurring/RecurringRulesModal";
import { ExplainableFigure, type ExplanationData } from "@/components/workspace/explain";
import { Card, CardHeader, EmptyState, ErrorState, Kpi, KpiStrip, Pill, Skeleton } from "@/components/workspace/primitives";
import { useWidthAtMost } from "@/hooks/useWidthAtMost";
import { api } from "@/lib/api";
import { KAKEIBO_BUCKETS as BUCKET_INFO } from "@/lib/kakeibo";
import { reportPeriodFilters } from "@/lib/ledgerFilters";
import { queryKeys } from "@/lib/queryKeys";
import { cn } from "@/lib/utils";
import type { ExpensePlanDraft } from "@/types/recurring";
import { CategoryBudgetSection } from "./CategoryBudgetSection";
import { rangeLabel, shortDay, todayIso } from "./format";
import styles from "./analysis.module.css";
import {
  KAKEIBO_BUCKETS,
  type AnalyticsResponse,
  type KakeiboBucket,
  type PeriodBasis,
  type PeriodComparisonResponse,
  type ServerExplanation,
} from "./types";

type Money = (amount: number) => string;

const BUCKET_COLORS: Record<KakeiboBucket, string> = {
  need: "var(--k-need)",
  want: "var(--k-want)",
  culture: "var(--k-culture)",
  unexpected: "var(--k-unexpected)",
  saving: "var(--k-saving)",
  unclassified: "var(--k-none)",
};

function bucketLabel(bucket: KakeiboBucket): string {
  return BUCKET_INFO.find((info) => info.value === bucket)?.label ?? bucket;
}

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

export function RingkasanTab({ basis, offset, money, onLabel }: { basis: PeriodBasis; offset: number; money: Money; onLabel: (label: string) => void }) {
  const [planDraft, setPlanDraft] = useState<ExpensePlanDraft | null>(null);
  const gridRef = useRef<HTMLDivElement>(null);
  const oneColumn = useWidthAtMost(gridRef, 900);
  const analytics = useQuery<AnalyticsResponse>({
    queryKey: queryKeys.dashboard.analytics(basis, offset),
    queryFn: () => api.get(`/dashboard/analytics?timeframe=${basis}&cycle_offset=${offset}`),
  });
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
  const comparison = data.comparison;
  const keluarTotal = data.explanations?.keluar.total ?? (typeof data.kakeibo?.total_allocated === "number" ? data.kakeibo.total_allocated : null);

  const left = (
    <>
      <KakeiboCard basis={basis} offset={offset} money={money} />
      <DailySpendCard days={data.daily_cadence ?? []} money={money} />
      <CashFlowHistory basis={basis} bal={money} />
    </>
  );
  const right = (
    <>
      <RecurringCharges bal={money} onCreatePlan={setPlanDraft} />
      <QuestionRate />
      <PeriodComparison basis={basis} offset={offset} bal={money} />
    </>
  );

  return (
    <div className={styles.stack}>
      <KpiStrip label="Arus kas tercatat">
        <Kpi icon="arrow-down-left" label="Masuk" value={comparison ? money(comparison.total_inflow) : "Tidak tersedia"} />
        <Kpi
          icon="arrow-up-right"
          label="Keluar"
          value={keluarTotal === null ? "Tidak tersedia" : data.explanations ? (
            <ExplainableFigure
              label="Keluar"
              amountText={money(keluarTotal)}
              explanation={serverExplanation("Keluar periode ini", "Semua yang keluar menurut pilar Kakeibo, termasuk yang ditabung atau diinvestasikan, dikurangi ganti uang. Sama dengan Keluar di Beranda.", data.explanations.keluar, money, "Total keluar")}
            />
          ) : money(keluarTotal)}
          caption="Termasuk yang ditabung"
        />
        <Kpi
          icon="receipt"
          label="Belanja"
          value={!comparison ? "Tidak tersedia" : data.explanations ? (
            <ExplainableFigure
              label="Belanja"
              amountText={money(comparison.total_outflow)}
              explanation={serverExplanation("Belanja periode ini", "Pengeluaran yang dihitung belanja, dikurangi ganti uang. Sama dengan Belanja di Transaksi.", data.explanations.belanja, money, "Total belanja")}
            />
          ) : money(comparison.total_outflow)}
          caption="Tanpa tabungan dan investasi"
        />
      </KpiStrip>
      <p className={styles.caption}>Keluar = Belanja ditambah yang ditabung atau diinvestasikan.</p>
      <div ref={gridRef} className={cn(styles.grid, oneColumn && styles.one)}>
        {oneColumn ? (
          <div className={styles.stack}>{left}{right}</div>
        ) : (
          <>
            <div className={styles.stack}>{left}</div>
            <div className={styles.stack}>{right}</div>
          </>
        )}
      </div>
      <CategoryBudgetSection rawCategories={data.category_variance ?? []} reportPeriod={reportPeriodFilters(basis, offset)} bal={money} />
      <RecurringRulesModal open={planDraft !== null} onClose={() => setPlanDraft(null)} expenseDraft={planDraft} />
    </div>
  );
}

function changePill(bucket: KakeiboBucket, change: number, money: Money) {
  if (change === 0) return <Pill tone="neutral">Sama</Pill>;
  const up = change > 0;
  // More saving and less spending are the directions the owner wants; the words say which way it went.
  const good = bucket === "saving" ? up : !up;
  return <Pill tone={good ? "ok" : "warn"}>{up ? "Naik" : "Turun"} {money(Math.abs(change))}</Pill>;
}

/** Kakeibo against the same elapsed days of the previous period, as amounts (D3, OD41). */
export function KakeiboCard({ basis, offset, money }: { basis: PeriodBasis; offset: number; money: Money }) {
  const query = useQuery<PeriodComparisonResponse>({
    queryKey: queryKeys.dashboard.periodComparison(basis, offset),
    queryFn: () => api.get(`/dashboard/period-comparison?timeframe=${basis}&cycle_offset=${offset}`),
  });
  const heading = <CardHeader id="kakeibo-compare-heading" icon="buckets" title="Kakeibo" caption={query.data ? windowsCaption(query.data) : undefined} />;
  if (query.isPending) return <Card labelledBy="kakeibo-compare-heading">{heading}<Skeleton style={{ height: 180 }} /></Card>;
  if (query.isError) return <Card labelledBy="kakeibo-compare-heading">{heading}<ErrorState message="Perbandingan Kakeibo belum bisa dimuat." onRetry={() => query.refetch()} /></Card>;
  const kakeibo = query.data.kakeibo;
  if (!kakeibo) {
    return <Card labelledBy="kakeibo-compare-heading">{heading}<p className={styles.caption}>Data pembanding belum tersedia.</p></Card>;
  }
  const values = KAKEIBO_BUCKETS.flatMap((bucket) => [kakeibo.current[bucket], kakeibo.previous?.[bucket] ?? 0]);
  const max = Math.max(1, ...values) * 1.08;
  const everything = KAKEIBO_BUCKETS.every((bucket) => kakeibo.current[bucket] === 0 && !kakeibo.previous?.[bucket]);
  return (
    <Card labelledBy="kakeibo-compare-heading">
      {heading}
      {everything ? (
        <EmptyState icon="buckets" title="Belum ada pengeluaran">Kakeibo muncul setelah ada pengeluaran di periode ini.</EmptyState>
      ) : (
        <div>
          {KAKEIBO_BUCKETS.map((bucket) => {
            const current = kakeibo.current[bucket];
            const previous = kakeibo.previous ? kakeibo.previous[bucket] : null;
            if (bucket === "unclassified" && current === 0 && !previous) return null;
            const name = bucketLabel(bucket);
            const accessible = previous === null ? `${name}: ${money(current)}` : `${name}: ${money(current)}, periode lalu ${money(previous)}`;
            return (
              <div key={bucket} className={styles.cmp} style={{ ["--kc" as string]: BUCKET_COLORS[bucket] }}>
                <div className={styles.cmpName}>
                  <b>{name}</b>
                  {previous !== null && <span>periode lalu {money(previous)}</span>}
                </div>
                <div className={styles.bench} role="img" aria-label={accessible}>
                  <i style={{ width: `${(current / max) * 100}%` }} />
                  {previous !== null && previous > 0 && <b style={{ left: `${(previous / max) * 100}%` }} />}
                </div>
                <div className={styles.cmpValue}>{money(current)}</div>
                <div className={styles.cmpState}>
                  {bucket === "unclassified" && current > 0 ? (
                    <Link href="/laporan?lengkapi=1" className="ws-linkbtn">Perlu dipilih</Link>
                  ) : previous !== null ? changePill(bucket, current - previous, money) : null}
                </div>
              </div>
            );
          })}
          <p className={styles.caption} style={{ marginTop: 8 }}>
            {kakeibo.previous ? "Garis tegak menandai jumlah pada hari yang sama periode lalu." : "Belum ada data periode lalu untuk dibandingkan."}
          </p>
        </div>
      )}
    </Card>
  );
}

function windowsCaption(data: PeriodComparisonResponse): string {
  const current = rangeLabel(data.current.start_date, data.current.end_date);
  if (!data.previous || !data.previous_has_data) return current;
  return `${current} dibanding ${rangeLabel(data.previous.start_date, data.previous.end_date)}`;
}

/** Belanja harian: one bar per day up to today, the peak in the accent, today outlined, and a table (D4). */
export function DailySpendCard({ days, money, today = todayIso() }: { days: { date: string; expense: number }[]; money: Money; today?: string }) {
  const [asTable, setAsTable] = useState(false);
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
            <div className={styles.plot}>
              <div className={styles.gridline} style={{ top: 0 }} />
              <div className={styles.gridline} style={{ top: "50%" }} />
              {shown.map((day) => (
                <div key={day.date} className={cn(styles.col, peak?.date === day.date && styles.peak, day.date === today && styles.today)} title={`${shortDay(day.date)}: ${money(day.expense)}`}>
                  <div className={styles.bar} style={{ height: `${(day.expense / max) * 100}%`, minHeight: day.expense > 0 ? 2 : 0 }} />
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
