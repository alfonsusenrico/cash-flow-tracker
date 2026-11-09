"use client";

import { useTranslations } from "next-intl";
import { KindAmount } from "@/components/transactions/rows";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { type MonthlyReport, categoryLedgerHref } from "@/lib/monthlyReport";
import { Question } from "./Question";

type Money = (amount: number) => string;

/**
 * Question 1 "Berapa uang masuk?" (apply-markas-laporan D5): Masuk, then each income source as a whole-row link to
 * its income rows in the report's window, with a green "+Rp" amount.
 */
export function IncomeQuestion({ report, money }: { report: MonthlyReport; money: Money }) {
  const t = useTranslations("report");
  const { period, income } = report;
  return (
    <Question number={1} id="lp-q1" title={t("questions.income")}>
      <p className="t-fig">
        <MaskedMoney text={money(report.comparison.totals.inflow)} />
      </p>
      <div>
        <SectionHeader level={3} id="lp-income-sources" label={t("income.sources")} />
        {income.sources.length === 0 ? (
          <p className="t-body muted">{t("income.empty")}</p>
        ) : (
          <ul className="list" aria-labelledby="lp-income-sources">
            {income.sources.map((source) => (
              <li key={source.category_id ?? "uncategorized"}>
                <ListRow
                  href={categoryLedgerHref(period, { category_id: source.category_id, is_uncategorized: !source.category_id }, "income")}
                  title={source.name}
                  meta={t("income.count", { count: source.count })}
                  amount={<KindAmount kind="income" amount={source.amount} money={money} />}
                />
              </li>
            ))}
          </ul>
        )}
        <p className="caption">{t("income.note")}</p>
      </div>
    </Question>
  );
}
