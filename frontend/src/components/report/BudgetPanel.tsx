"use client";

import Link from "next/link";
import type { Ref } from "react";
import { useTranslations } from "next-intl";
import { PILLAR_COLOR } from "@/components/analysis/KakeiboRows";
import { ProgressBar } from "@/components/workspace/figures";
import { SectionHeader } from "@/components/workspace/lists";
import { type MonthlyReport, type SpendingBucket, categoryLedgerHref } from "@/lib/monthlyReport";
import { cn } from "@/lib/utils";

type Money = (amount: number) => string;

/**
 * "Anggaran" (apply-markas-laporan D10, D29): one whole-row link per budgeted category with its 160 px meter and
 * "{spent} dari {budget}" with what is left or over, all on the Pengeluaran basis, then the overall budget. "Atur
 * anggaran" opens the shared budget dialog. Without any budget the report shows no budget figure.
 */
export function BudgetPanel({ report, money, onEdit, editRef }: { report: MonthlyReport; money: Money; onEdit: () => void; editRef: Ref<HTMLButtonElement> }) {
  const t = useTranslations("report.budget");
  const { budget, period, comparison } = report;
  // A budget row takes its category's pillar colour from the comparison; a category without spending has none.
  const pillarOf = new Map<string, SpendingBucket>();
  for (const pillar of comparison.pillars) {
    for (const category of pillar.categories) if (category.category_id) pillarOf.set(category.category_id, pillar.pillar);
  }
  const rows = [...(budget?.categories ?? [])].sort((left, right) => left.spending_remaining - right.spending_remaining || left.name.localeCompare(right.name));
  const rest = (remaining: number) => (remaining >= 0 ? t("left", { amount: money(remaining) }) : t("over", { amount: money(-remaining) }));

  return (
    <section aria-labelledby="lp-budget">
      <SectionHeader
        level={3}
        id="lp-budget"
        label={t("title")}
        aside={(
          <button ref={editRef} type="button" className="btn-link" aria-haspopup="dialog" onClick={onEdit}>
            {t("set")}
          </button>
        )}
      />
      {!budget ? (
        <p className="t-body muted">{t("empty")}</p>
      ) : (
        <>
          {rows.length > 0 && (
            <ul className="list" aria-labelledby="lp-budget">
              {rows.map((row) => {
                const used = row.budget > 0 ? (row.spending / row.budget) * 100 : 0;
                const over = row.spending_remaining < 0;
                const pillar = pillarOf.get(row.category_id);
                return (
                  <li key={row.category_id}>
                    <Link href={categoryLedgerHref(period, { category_id: row.category_id })} className="row lp-budget-row">
                      <span className="r-main">
                        <span className="r-title">{row.name}</span>
                      </span>
                      <span className="ph-hide lp-meter">
                        <ProgressBar value={used} width={160} color={over ? "var(--out-solid)" : pillar ? PILLAR_COLOR[pillar] : "var(--k-none)"} />
                      </span>
                      <span className="r-amt lp-budget-amt">
                        <span className="t-amt">{t("spentOf", { used: money(row.spending), budget: money(row.budget) })}</span>
                        <span className={cn("t-meta", over && "c-expense")}>{rest(row.spending_remaining)}</span>
                      </span>
                    </Link>
                  </li>
                );
              })}
            </ul>
          )}
          {budget.overall && (
            <p className="t-body lp-budget-total">
              {t("total", { used: money(budget.overall.spending), budget: money(budget.overall.budget), rest: rest(budget.overall.spending_remaining) })}
            </p>
          )}
          {budget.period_basis === "cycle" && <p className="caption">{t("cycleNote")}</p>}
        </>
      )}
    </section>
  );
}
