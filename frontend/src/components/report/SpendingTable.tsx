"use client";

import Link from "next/link";
import { useTranslations } from "next-intl";
import { inSentence } from "@/components/analysis/ComparisonRows";
import { PILLAR_TERM, PillarDot } from "@/components/analysis/KakeiboRows";
import { ChangeFigure } from "@/components/workspace/figures";
import { SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { useFormat } from "@/lib/format";
import { type MonthlyReport, refHref } from "@/lib/monthlyReport";
import { cn } from "@/lib/utils";
import { usePreviousName } from "./ReportHero";

type Money = (amount: number) => string;

/**
 * "Pengeluaran per pilar dan kategori" (apply-markas-laporan D7): pillar rows, then their categories by amount, and
 * the total. Each row is one target: its row header holds the ledger link, stretched over the row, so the table
 * keeps its semantics. The last column is the change against the whole previous period, "belum lengkap" against a
 * partial one, or the running share; it is left out without a previous period.
 */
export function SpendingTable({ report, money }: { report: MonthlyReport; money: Money }) {
  const t = useTranslations("report.table");
  const terms = useTranslations("terms");
  const format = useFormat();
  const comparison = report.comparison;
  const { running, previous, progress } = comparison;
  const previousName = usePreviousName(report);
  const runningShares = new Map((progress?.pillars ?? []).map((item) => [item.pillar, item.share]));
  let lastHeading: string | null = null;
  if (running && progress?.previous_label && previousName) lastHeading = t("of", { period: previousName });
  else if (!running && previous && previousName) lastHeading = t("vs", { period: previousName });
  const share = (value: number | null) => (value === null ? "—" : `${format.integer(value)}%`);
  const rowLabel = (name: string, amount: number) => t("rowLink", { name, amount: money(amount) });

  const change = (value: number | null) => {
    if (previous?.partial) return <span className="t-meta">{t("partial")}</span>;
    return value === null ? <span className="t-meta">—</span> : <ChangeFigure change={value} money={money} />;
  };

  return (
    <section aria-labelledby="lp-table">
      <SectionHeader level={3} id="lp-table" label={t("title")} />
      {comparison.pillars.length === 0 ? (
        <p className="t-body muted">{t("empty")}</p>
      ) : (
        <div className="tbl-scroll">
          <table className="tbl lp-tbl">
            <caption className="sr-only">{t("caption", { period: inSentence(report.period.label, report.basis) })}</caption>
            <thead>
              <tr>
                <th scope="col">{t("pillarCategory")}</th>
                <th scope="col" className="num">{t("amount")}</th>
                <th scope="col" className="num ph-hide">{t("share")}</th>
                {lastHeading && <th scope="col" className="num ph-hide">{lastHeading}</th>}
              </tr>
            </thead>
            <tbody>
              {comparison.pillars.flatMap((pillar) => {
                const name = terms(PILLAR_TERM[pillar.pillar]);
                const runningShare = runningShares.get(pillar.pillar) ?? null;
                return [
                  <tr key={pillar.pillar} className="lp-pillar-row">
                    <th scope="row" className="t-label">
                      <Link href={refHref(pillar.ref)} className="lp-rowlink" aria-label={rowLabel(name, pillar.amount)}>
                        <PillarDot pillar={pillar.pillar} />
                        <span>{name}</span>
                      </Link>
                    </th>
                    <td className="num"><MaskedMoney text={money(pillar.amount)} className="t-amt" /></td>
                    <td className="num t-meta ph-hide">{share(pillar.share)}</td>
                    {lastHeading && (
                      <td className="num ph-hide">
                        {running ? <span className="t-meta">{share(runningShare)}</span> : change(pillar.change)}
                      </td>
                    )}
                  </tr>,
                  ...pillar.categories.map((category) => (
                    <tr key={`${pillar.pillar}-${category.category_id ?? "none"}`}>
                      <th scope="row" className={cn("t-body", "lp-cat")}>
                        <Link href={refHref(category.ref)} className="lp-rowlink" aria-label={rowLabel(category.name, category.amount)}>
                          {category.name}
                        </Link>
                      </th>
                      <td className="num"><MaskedMoney text={money(category.amount)} className="t-amt" /></td>
                      <td className="num t-meta ph-hide">{share(category.share)}</td>
                      {lastHeading && <td className="num ph-hide">{running ? null : change(category.change)}</td>}
                    </tr>
                  )),
                ];
              })}
            </tbody>
            <tfoot>
              <tr>
                <th scope="row" className="t-label">{t("total")}</th>
                <td className="num"><MaskedMoney text={money(comparison.totals.spending)} className="t-amt" /></td>
                <td className="num t-meta ph-hide">{comparison.totals.spending > 0 ? share(100) : "—"}</td>
                {lastHeading && <td className="ph-hide" />}
              </tr>
            </tfoot>
          </table>
        </div>
      )}
    </section>
  );
}
