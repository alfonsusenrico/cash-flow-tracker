"use client";

import { useId, type Ref } from "react";
import Link from "next/link";
import { useTranslations } from "next-intl";
import { useAppCtx } from "@/components/layout/AppLayout";
import { Panel, PanelBlock, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { useFormat } from "@/lib/format";
import { type MonthlyReport, type ReportBasis, type ReportPeriod, reportLedgerHref } from "@/lib/monthlyReport";

type Money = (amount: number) => string;

/** Whether the period has anything open in its data quality. */
export function dataQualityIssues(report: MonthlyReport): number {
  const quality = report.data_quality;
  return quality.pending_review + quality.unclassified.count + quality.possible_duplicates + quality.days_without_records.count;
}

/**
 * The context panel (apply-markas-laporan D14): "Kualitas data" in one sentence with its fixes, "Selisih" for a
 * completed period, and "Laporan sebelumnya" with the shown report marked by the straight selection bar.
 */
export function ReportContext({ report, periods, money, onClassify, classifyRef, onOpenPeriod }: {
  report: MonthlyReport;
  periods: ReportPeriod[];
  money: Money;
  onClassify: () => void;
  classifyRef: Ref<HTMLButtonElement>;
  /** Opens a listed report in place, through the page's URL state. */
  onOpenPeriod: (startDate: string) => void;
}) {
  const t = useTranslations("report");
  const terms = useTranslations("terms");
  const difference = report.comparison.totals.difference;
  const sign = difference > 0 ? "+" : difference < 0 ? "−" : "";
  return (
    <Panel label={t("page.context")}>
      <PanelBlock>
        <SectionHeader label={t("quality.title")} />
        <DataQuality report={report} onClassify={onClassify} classifyRef={classifyRef} />
      </PanelBlock>
      {report.period.complete && (
        <PanelBlock>
          <SectionHeader label={terms("difference")} />
          <p className="t-fig">
            <MaskedMoney text={`${sign}${money(Math.abs(difference))}`} />
          </p>
          <p className="t-meta">{t("context.differenceMeta")}</p>
        </PanelBlock>
      )}
      {periods.length >= 2 && (
        <PanelBlock>
          <SectionHeader id="lp-past" label={t("context.past")} />
          <PastReports periods={periods} current={report.period.start_date} basis={report.basis} onOpen={onOpenPeriod} />
        </PanelBlock>
      )}
    </Panel>
  );
}

function DataQuality({ report, onClassify, classifyRef }: { report: MonthlyReport; onClassify: () => void; classifyRef: Ref<HTMLButtonElement> }) {
  const t = useTranslations("report.quality");
  const format = useFormat();
  const { openQuickAdd } = useAppCtx();
  const datesId = useId();
  const { period, data_quality: quality } = report;
  const days = quality.days_without_records;
  if (dataQualityIssues(report) === 0) return <p className="t-body">{t("complete")}</p>;
  const parts = [
    quality.pending_review > 0 && t("review", { count: quality.pending_review }),
    quality.unclassified.count > 0 && t("unclassified", { count: quality.unclassified.count }),
    quality.possible_duplicates > 0 && t("duplicates", { count: quality.possible_duplicates }),
    days.count > 0 && t("emptyDays", { count: days.count }),
  ].filter(Boolean);
  return (
    <>
      <p className="t-body">{parts.join(" · ")}</p>
      <div className="panel-actions lp-fixes">
        {quality.pending_review > 0 && <Link href="/review" className="btn-link">{t("openReview")}</Link>}
        {quality.unclassified.count > 0 && (
          <button ref={classifyRef} type="button" className="btn-link" aria-haspopup="dialog" onClick={onClassify}>
            {t("classify")}
          </button>
        )}
        {quality.possible_duplicates > 0 && <Link href={reportLedgerHref(period, { dup: true })} className="btn-link">{t("checkDuplicates")}</Link>}
        {days.count > 0 && (
          <>
            <button type="button" className="btn-link" aria-describedby={datesId} onClick={() => openQuickAdd("expense")}>
              {t("record")}
            </button>
            <span id={datesId} hidden>
              {t("emptyDates", { dates: days.dates.map((date) => format.shortDate(date)).join(", ") })}
            </span>
          </>
        )}
      </div>
    </>
  );
}

/** "Laporan sebelumnya": every period of the basis, newest first, as whole-row links; the shown one is current. */
function PastReports({ periods, current, basis, onOpen }: { periods: ReportPeriod[]; current: string; basis: ReportBasis; onOpen: (startDate: string) => void }) {
  const t = useTranslations("report.context");
  return (
    <ul className="list lp-past" aria-labelledby="lp-past">
      {periods.map((item) => {
        const status = [item.complete ? t("complete") : t("running"), item.has_reflection ? t("hasNote") : null].filter(Boolean).join(" · ");
        return (
          <li key={item.start_date}>
            <Link
              href={`/laporan?basis=${basis}&start=${item.start_date}`}
              className="row ws-select-row lp-past-row"
              aria-current={item.start_date === current ? "page" : undefined}
              onClick={(event) => {
                // A plain click stays on the page through the URL state; Next.js commits no same-path navigation from
                // a URL with a query (redesign-transactions-workspace D13). Modified clicks keep the link.
                if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button !== 0) return;
                event.preventDefault();
                onOpen(item.start_date);
              }}
            >
              <span className="r-main">
                <span className="r-title">{item.label}</span>
              </span>
              <span className="t-meta">{status}</span>
            </Link>
          </li>
        );
      })}
    </ul>
  );
}
