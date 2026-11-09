"use client";

import { useTranslations } from "next-intl";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { useFormat } from "@/lib/format";
import { type MonthlyReport, reportLedgerHref } from "@/lib/monthlyReport";
import { Question } from "./Question";

type Money = (amount: number) => string;

/**
 * Question 2 "Berapa yang ditabung?" (apply-markas-laporan D6): Ditabung & investasi with its share of Masuk, and
 * "Rincian tabungan" with today's two parts. The net move is not a sum of rows, so it is not a link.
 */
export function SavingQuestion({ report, money }: { report: MonthlyReport; money: Money }) {
  const t = useTranslations("report");
  const format = useFormat();
  const { totals } = report.comparison;
  const { savings, period } = report;
  return (
    <Question number={2} id="lp-q2" title={t("questions.saving")}>
      <p className="lp-fig-line">
        <MaskedMoney text={money(totals.fresh_saving)} className="t-fig" />
        {totals.inflow > 0 && (
          <span className="t-meta">{t("saving.shareOfIncome", { share: format.integer((totals.fresh_saving / totals.inflow) * 100) })}</span>
        )}
      </p>
      <div>
        <SectionHeader level={3} id="lp-saving-parts" label={t("saving.breakdown")} />
        <ul className="list" aria-labelledby="lp-saving-parts">
          <li>
            <ListRow
              href={reportLedgerHref(period, { type: "expense", kakeibo: "saving" })}
              title={t("saving.pillarExpenses")}
              amount={<MaskedMoney text={money(savings.saving_expenses)} className="t-amt" />}
            />
          </li>
          <li>
            <ListRow
              title={t("saving.netMoves")}
              meta={t("saving.netMovesMeta")}
              amount={<MaskedMoney text={money(savings.net_saving_transfers)} className="t-amt c-invest" />}
            />
          </li>
        </ul>
      </div>
    </Question>
  );
}
