"use client";

import { useTranslations } from "next-intl";
import { jakartaDay } from "@/components/transactions/rows";
import { ChangeFigure, ProgressBar } from "@/components/workspace/figures";
import { MaskedMoney } from "@/components/workspace/money";
import { useFormat } from "@/lib/format";
import type { ReportComparison } from "@/lib/monthlyReport";

type Money = (amount: number) => string;

/** A server period label inside a sentence: a cycle's leading word in lower case ("siklus 25 Sep – 22 Okt"). */
export function inSentence(label: string, basis: ReportComparison["basis"]): string {
  return basis === "cycle" ? label.charAt(0).toLocaleLowerCase() + label.slice(1) : label;
}

/** "Agu–Okt" for months, "25 Jul – 22 Okt" for cycles: the first and last period of the baseline. */
function useBaselineSpan(comparison: ReportComparison): string | null {
  const format = useFormat();
  const labels = comparison.baseline?.labels ?? [];
  const items = labels.map((label) => comparison.history.find((item) => item.label === label)).filter((item) => item !== undefined);
  if (!items.length) return labels.length ? `${labels[0]}–${labels[labels.length - 1]}` : null;
  const first = jakartaDay(items[0].start);
  const last = jakartaDay(items[items.length - 1].end);
  if (comparison.basis === "month") return `${format.shortMonth(first)}–${format.shortMonth(last)}`;
  return `${format.shortDate(first)} – ${format.shortDate(last)}`;
}

/**
 * The two whole-period rows under Ringkasan's hero (apply-markas-analisis D5): while the period runs, the share of
 * the whole previous period and of the average with bars and no verdict; once complete, the changes against both
 * as ▲/▼ amounts. Every amount and share is the server's.
 */
export function ComparisonRows({ comparison, money }: { comparison: ReportComparison; money: Money }) {
  const t = useTranslations("analysis.compare");
  const format = useFormat();
  const span = useBaselineSpan(comparison);
  const { previous, baseline, progress, running, basis } = comparison;
  const unit = basis === "cycle" ? "cycle" : "month";
  const count = baseline?.period_count ?? 0;
  const per = basis === "cycle" ? t("perCycle") : t("perMonth");

  if (!previous) {
    return <p className="caption">{t("noPrevious", { period: comparison.period.label })}</p>;
  }

  const previousName = inSentence(previous.label, basis);
  let first;
  if (previous.partial) {
    first = (
      <div className="cmp-row is-compare">
        <span className="t-body">{t("partialFrom", { period: previous.label, date: format.shortDate(previous.tracking_from ?? previous.start_date) })}</span>
        <span className="t-meta cmp-num">{t("partial")}</span>
      </div>
    );
  } else if (running) {
    first = progress && progress.share !== null && progress.previous_spending !== null && (
      <div className="cmp-row is-compare">
        <span className="t-body">{t("shareOfPrevious", { share: format.integer(progress.share), period: previousName })}</span>
        <MaskedMoney text={money(progress.previous_spending)} className="t-amt cmp-num" />
        <span className="t-meta cmp-num ph-hide">{t("wholePeriod")}</span>
        <span className="ph-hide cmp-bar"><ProgressBar value={progress.share} color="var(--out-solid)" /></span>
      </div>
    );
  } else {
    first = (
      <div className="cmp-row is-compare">
        <span className="t-body">{t("againstPrevious", { period: previousName })}</span>
        <span className="cmp-num">{previous.change.spending === null ? <span className="t-amt">—</span> : <ChangeFigure change={previous.change.spending} money={money} />}</span>
        <span className="t-meta cmp-num ph-hide">{t("wholePeriod")}</span>
        <span className="ph-hide cmp-bar" aria-hidden="true" />
      </div>
    );
  }

  let second;
  if (!baseline) {
    second = (
      <div className="cmp-row is-compare">
        <span className="t-body">{basis === "cycle" ? t("noBaselineCycle") : t("noBaseline")}</span>
      </div>
    );
  } else if (running) {
    const share = progress?.baseline_share ?? null;
    second = (
      <div className="cmp-row is-compare">
        <span className="t-body">
          {share === null
            ? t("average", { count, unit, span: span ?? "" })
            : t("shareOfAverage", { share: format.integer(share), count, unit, span: span ?? "" })}
        </span>
        <MaskedMoney text={money(baseline.totals.spending)} className="t-amt cmp-num" />
        <span className="t-meta cmp-num ph-hide">{per}</span>
        <span className="ph-hide cmp-bar">{share !== null && <ProgressBar value={share} color="var(--out-solid)" />}</span>
      </div>
    );
  } else {
    second = (
      <div className="cmp-row is-compare">
        <span className="t-body">{t("againstAverage", { count, unit, span: span ?? "" })}</span>
        <span className="cmp-num">{baseline.change.spending === null ? <span className="t-amt">—</span> : <ChangeFigure change={baseline.change.spending} money={money} />}</span>
        <span className="t-meta cmp-num ph-hide">{per}</span>
        <span className="ph-hide cmp-bar" aria-hidden="true" />
      </div>
    );
  }

  return (
    <div className="cmp">
      {first}
      {second}
      {running && <p className="caption">{t("runningNote")}</p>}
    </div>
  );
}
