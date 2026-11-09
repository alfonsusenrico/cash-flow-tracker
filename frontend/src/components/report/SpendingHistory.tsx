"use client";

import { useState } from "react";
import { useTranslations } from "next-intl";
import { useAppCtx } from "@/components/layout/AppLayout";
import { ChartTable, ChartTableButton } from "@/components/charts/ChartTable";
import { jakartaDay } from "@/components/transactions/rows";
import { SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { useFormat } from "@/lib/format";
import type { ComparisonHistoryItem, MonthlyReport } from "@/lib/monthlyReport";
import { cn } from "@/lib/utils";

type Money = (amount: number) => string;

/** The bar area of "Pengeluaran per bulan" in px; the amount and period labels sit outside it (D11). */
export const BAR_AREA = 112;

/** A bar's height: proportional to the highest period, at least 2 px when it has any spending. */
export function barHeight(spending: number, max: number): number {
  if (spending <= 0 || max <= 0) return 0;
  return Math.max(2, Math.round((spending / max) * BAR_AREA));
}

/**
 * "Pengeluaran per bulan" (apply-markas-laporan D11): the comparison's last four completed periods and the running
 * one as one series of proportional bars with their compact amounts, the viewed period in the expense colour, the
 * running one hatched, a legend naming both, and the same values as a table.
 */
export function SpendingHistory({ report, money }: { report: MonthlyReport; money: Money }) {
  const t = useTranslations("report.history");
  const terms = useTranslations("terms");
  const common = useTranslations("common");
  const format = useFormat();
  const { hideBalances } = useAppCtx();
  const [asTable, setAsTable] = useState(false);
  const items = report.comparison.history;
  if (items.length === 0) return null;
  const cycle = report.basis === "cycle";
  const title = cycle ? t("titleCycle") : t("title");
  const max = Math.max(0, ...items.map((item) => item.spending));
  const short = (item: ComparisonHistoryItem) => (cycle ? format.shortDate(jakartaDay(item.start)) : format.shortMonth(jakartaDay(item.start)));
  const barLabel = (item: ComparisonHistoryItem) => (item.complete ? short(item) : t("barRunning", { period: short(item) }));
  const amountText = (amount: number) => (hideBalances ? common("hidden") : money(amount));
  const list = items.map((item) => `${item.label}${item.complete ? "" : ` (${t("running")})`} ${amountText(item.spending)}`).join(", ");
  const viewed = items.find((item) => item.viewed);
  const running = items.some((item) => !item.complete);

  const chart = (
    <>
      <div className="bars-v" style={{ "--n": items.length } as React.CSSProperties} role="img" aria-label={t("chartLabel", { title, list })}>
        {items.map((item) => (
          <div key={item.start} className="b" data-viewed={item.viewed || undefined}>
            <span className="t-meta b-amt">{format.compactMoney(item.spending, { hidden: hideBalances })}</span>
            <span className="b-area">
              <span
                className={cn("bar", item.viewed && "is-viewed", !item.complete && "is-running")}
                style={{ height: barHeight(item.spending, max) }}
              />
            </span>
            <span className={cn("t-meta", "b-label", item.viewed && "is-viewed")}>{barLabel(item)}</span>
          </div>
        ))}
      </div>
      <ul className="chart-foot t-meta lp-legend">
        {viewed && (
          <li className="leg">
            <span className="leg-sw" style={{ background: "var(--out-solid)" }} aria-hidden="true" />
            {t("legendViewed", { period: viewed.label })}
          </li>
        )}
        {running && (
          <li className="leg">
            <span className="leg-sw is-hatch" aria-hidden="true" />
            {t("legendRunning")}
          </li>
        )}
      </ul>
    </>
  );

  const table = (
    <div className="tbl-scroll">
      <table className="tbl">
        <caption className="sr-only">{t("tableCaption")}</caption>
        <thead>
          <tr>
            <th scope="col">{t("period")}</th>
            <th scope="col" className="num">{terms("spending")}</th>
          </tr>
        </thead>
        <tbody>
          {items.map((item) => (
            <tr key={item.start}>
              <th scope="row" className="t-body lp-cell">
                {item.label}
                {item.viewed && ` · ${t("viewed")}`}
                {!item.complete && ` · ${t("running")}`}
              </th>
              <td className="num"><MaskedMoney text={money(item.spending)} className="t-amt" /></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );

  return (
    <section aria-labelledby="lp-history">
      <SectionHeader
        level={3}
        id="lp-history"
        label={title}
        aside={<ChartTableButton asTable={asTable} onToggle={() => setAsTable((value) => !value)} showTable={t("showTable")} showChart={t("showChart")} />}
      />
      <ChartTable asTable={asTable} chart={chart} table={table} />
      <p className="caption">{cycle ? t("captionCycle") : t("caption")}</p>
    </section>
  );
}
