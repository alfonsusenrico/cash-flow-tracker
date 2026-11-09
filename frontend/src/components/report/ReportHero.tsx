"use client";

import { Fragment, type ReactNode } from "react";
import { useTranslations } from "next-intl";
import { inSentence } from "@/components/analysis/ComparisonRows";
import { ChangeFigure, HeroFigure, ProgressBar } from "@/components/workspace/figures";
import { MaskedMoney } from "@/components/workspace/money";
import { useFormat } from "@/lib/format";
import type { MonthlyReport } from "@/lib/monthlyReport";

type Money = (amount: number) => string;

/** The previous period's short name: "September" for a month, the cycle's own label otherwise. */
export function usePreviousName(report: MonthlyReport): string | null {
  const format = useFormat();
  const previous = report.comparison.previous;
  if (!previous) return null;
  return report.basis === "month" ? format.monthName(previous.start_date) : inSentence(previous.label, report.basis);
}

/**
 * The one hero of Laporan (apply-markas-laporan D3): "Pengeluaran {period}" with the comparison's Pengeluaran, and
 * one line against the whole previous period and the 3-period average (owner rule). While the period runs the line
 * gives its shares of both with a bar and no verdict. Every amount and share is the server's.
 */
export function ReportHero({ report, money }: { report: MonthlyReport; money: Money }) {
  const t = useTranslations("report.hero");
  const format = useFormat();
  const comparison = report.comparison;
  const { previous, baseline, progress, totals } = comparison;
  const unit = report.basis === "cycle" ? "cycle" : "month";
  const previousName = usePreviousName(report);
  const parts: ReactNode[] = [];
  let bar: number | null = null;

  if (comparison.running) {
    if (progress?.share !== null && progress?.share !== undefined && progress.previous_label) {
      parts.push(t("shareOfPrevious", { share: format.integer(progress.share), period: progress.previous_label }));
      bar = progress.share;
    }
    const averageShare = progress?.baseline_share ?? null;
    if (averageShare !== null && baseline) {
      parts.push(t("shareOfAverage", { share: format.integer(averageShare), count: baseline.period_count, unit }));
      bar = bar ?? averageShare;
    }
  } else if (previous) {
    if (previous.partial) {
      parts.push(t("partialPrevious", { period: previous.label, date: format.shortDate(previous.tracking_from ?? previous.start_date) }));
    } else if (previous.change.spending !== null) {
      const change = previous.change.spending;
      parts.push(t.rich("vsPrevious", { change: () => <ChangeFigure change={change} money={money} />, period: previousName ?? previous.label }));
    }
    if (baseline && baseline.change.spending !== null) {
      const change = baseline.change.spending;
      parts.push(t.rich("vsAverage", { change: () => <ChangeFigure change={change} money={money} />, count: baseline.period_count, unit }));
    } else if (!baseline) {
      parts.push(t("noAverage", { unit }));
    }
  }

  const line = parts.length ? (
    <>
      {parts.map((part, index) => (
        <Fragment key={index}>
          {index > 0 && <span aria-hidden="true">{" · "}</span>}
          <span className="lp-hero-part">{part}</span>
        </Fragment>
      ))}
    </>
  ) : null;
  const period = report.period;

  return (
    <HeroFigure
      id="lp-hero"
      label={t("label", { period: inSentence(period.label, report.basis) })}
      figure={<MaskedMoney text={money(totals.spending)} />}
      line={line}
    >
      {comparison.running && (
        <div className="lp-running">
          {bar !== null && <ProgressBar value={bar} width={240} color="var(--out-solid)" />}
          <p className="caption">{t("running")}</p>
        </div>
      )}
      {period.tracking_started_inside && period.first_recorded_date && (
        <p className="caption">{t("trackingStarted", { date: format.shortDate(period.first_recorded_date) })}</p>
      )}
    </HeroFigure>
  );
}
