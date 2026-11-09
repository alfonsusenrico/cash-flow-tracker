"use client";

import type { Ref } from "react";
import { useTranslations } from "next-intl";
import { KindAmount } from "@/components/transactions/rows";
import { ListRow, SectionHeader } from "@/components/workspace/lists";
import { useFormat } from "@/lib/format";
import { type MonthlyReport, reportLedgerHref } from "@/lib/monthlyReport";
import { BudgetPanel } from "./BudgetPanel";
import { Question } from "./Question";
import { SpendingDetails } from "./SpendingDetails";
import { SpendingHistory } from "./SpendingHistory";
import { SpendingTable } from "./SpendingTable";

type Money = (amount: number) => string;

/**
 * Question 3 "Berapa pengeluarannya?" (apply-markas-laporan D7 to D11): the pillar and category table, the top
 * places, the spending details, the budget and the spending per period.
 */
export function SpendingQuestion({ report, money, onEditBudget, budgetRef }: {
  report: MonthlyReport;
  money: Money;
  onEditBudget: () => void;
  budgetRef: Ref<HTMLButtonElement>;
}) {
  const t = useTranslations("report");
  return (
    <Question number={3} id="lp-q3" title={t("questions.spending")}>
      <SpendingTable report={report} money={money} />
      <TopPlaces report={report} money={money} />
      <SpendingDetails report={report} money={money} />
      <BudgetPanel report={report} money={money} onEdit={onEditBudget} editRef={budgetRef} />
      <SpendingHistory report={report} money={money} />
    </Question>
  );
}

/**
 * "Tempat belanja terbesar" (D8, D29): the five largest places of Pengeluaran, without expenses that have the
 * Tabungan pillar, each a whole-row link to its rows with a red "−Rp" amount.
 */
function TopPlaces({ report, money }: { report: MonthlyReport; money: Money }) {
  const t = useTranslations("report.merchants");
  const format = useFormat();
  const merchants = report.spending.top_spending_merchants;
  return (
    <section aria-labelledby="lp-places">
      <SectionHeader level={3} id="lp-places" label={t("title")} />
      {merchants.length === 0 ? (
        <p className="t-body muted">{t("empty")}</p>
      ) : (
        <ol className="list" aria-labelledby="lp-places">
          {merchants.map((merchant, index) => (
            <li key={merchant.search_text}>
              <ListRow
                href={reportLedgerHref(report.period, { type: "expense", q: merchant.search_text })}
                time={format.integer(index + 1)}
                title={merchant.name}
                amount={<KindAmount kind="expense" amount={merchant.amount} money={money} />}
              />
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
