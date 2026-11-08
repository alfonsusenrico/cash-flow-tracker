"use client";

import { useRef, useState } from "react";
import Link from "next/link";
import { useTranslations } from "next-intl";
import { BudgetDialog } from "@/components/categories/BudgetDialog";
import { CategoryManagerDialog } from "@/components/categories/CategoryManagerDialog";
import { Meter } from "@/components/workspace/figures";
import { SectionHeader } from "@/components/workspace/lists";
import { MaskedMoney } from "@/components/workspace/money";
import { useFormat } from "@/lib/format";
import { pillarOrUnclassified } from "@/lib/kakeibo";
import { type LedgerFilters, UNCATEGORIZED, ledgerHref } from "@/lib/ledgerFilters";
import { PILLAR_COLOR, PILLAR_TERM } from "./KakeiboRows";
import type { CategoryVariance } from "./types";

type Money = (amount: number) => string;
type Group = "need" | "want" | "culture" | "unexpected" | "saving" | "unclassified";

const GROUP_ORDER: Group[] = ["need", "want", "culture", "unexpected", "saving", "unclassified"];

function groupOf(category: CategoryVariance): Group {
  if (category.is_uncategorized) return "unclassified";
  return (pillarOrUnclassified(category.kakeibo_type) || "unclassified") as Group;
}

/**
 * Expense categories grouped by pillar in Kakeibo order, each sorted by amount (then name). Categories without
 * spending are left out unless `withZero`; a budgeted category stays listed at Rp 0 so its limit stays visible.
 */
export function groupCategories(categories: CategoryVariance[], withZero: boolean): { group: Group; rows: CategoryVariance[] }[] {
  const shown = categories.filter((category) => category.kind !== "income" && (withZero || category.spent > 0 || (category.budget ?? 0) > 0));
  return GROUP_ORDER.map((group) => ({
    group,
    rows: shown.filter((category) => groupOf(category) === group).sort((left, right) => right.spent - left.spent || left.name.localeCompare(right.name)),
  })).filter((entry) => entry.rows.length > 0);
}

/**
 * "Pengeluaran per kategori" (apply-markas-analisis D8): pillar groups under 2 px rails, budget bars and "59% dari
 * Rp 1.000.000" when budgets exist, each row a link to the ledger for the period and category. "Tanpa kategori" and
 * archived categories with spending stay listed and offer no budget or edit.
 */
export function CategorySpending({
  categories,
  period,
  money,
}: {
  categories: CategoryVariance[];
  period: Pick<LedgerFilters, "period" | "offset"> | null;
  money: Money;
}) {
  const t = useTranslations("analysis.categories");
  const terms = useTranslations("terms");
  const format = useFormat();
  const [withZero, setWithZero] = useState(false);
  const [managing, setManaging] = useState(false);
  const [budgeting, setBudgeting] = useState(false);
  const manageRef = useRef<HTMLButtonElement>(null);
  const budgetRef = useRef<HTMLButtonElement>(null);
  const expense = categories.filter((category) => category.kind !== "income");
  const anyBudget = expense.some((category) => (category.budget ?? 0) > 0);
  const zeroCount = expense.filter((category) => category.spent <= 0 && !((category.budget ?? 0) > 0)).length;
  const groups = groupCategories(expense, withZero);

  const href = (category: CategoryVariance) =>
    period ? ledgerHref({ ...period, type: "expense", category: category.is_uncategorized ? UNCATEGORIZED : category.id ?? "all" }) : null;

  const budgetMeta = (category: CategoryVariance) => {
    const budget = category.budget ?? 0;
    if (category.is_archived) return t("archivedNoBudget");
    if (category.is_uncategorized || budget <= 0) return t("noBudget");
    const used = Math.round(category.percentage_used ?? 0);
    const over = used > 100;
    return t.rich("ofBudget", {
      share: format.integer(used),
      budget: money(budget),
      pct: (chunks) => (over ? <span className="c-expense">{chunks}</span> : <>{chunks}</>),
    });
  };

  return (
    <section aria-labelledby="an-categories">
      <SectionHeader
        id="an-categories"
        label={t("title")}
        aside={
          <span className="sec-actions">
            <button ref={manageRef} type="button" className="ws-btn ws-btn-quiet ws-btn-sm" aria-haspopup="dialog" onClick={() => setManaging(true)}>{t("manage")}</button>
            <button ref={budgetRef} type="button" className="btn-link" aria-haspopup="dialog" onClick={() => setBudgeting(true)}>{t("setBudgets")}</button>
          </span>
        }
      />
      {groups.map(({ group, rows }) => (
        <div key={group} className="grp" style={{ "--rail": PILLAR_COLOR[group] } as React.CSSProperties}>
          <div className="grp-h">
            <h3 className="t-label">{terms(PILLAR_TERM[group])}</h3>
            <span className="count">{t("count", { count: rows.length })}</span>
          </div>
          <ul className="list">
            {rows.map((category) => {
              const budget = category.budget ?? 0;
              const budgeted = budget > 0 && !category.is_uncategorized && !category.is_archived;
              const used = category.percentage_used ?? 0;
              const name = category.is_uncategorized ? terms("noCategory") : category.name;
              const meta = anyBudget ? budgetMeta(category) : category.is_archived ? t("archived") : null;
              const content = (
                <>
                  <span className="r-main">
                    <span className="r-title">{name}</span>
                    {meta && <span className="r-meta ph-only">{meta}</span>}
                  </span>
                  {anyBudget && (budgeted ? (
                    <span className="ph-hide cat-meter"><Meter value={used} width={160} tone={used > 100 ? "var(--out-solid)" : PILLAR_COLOR[group]} /></span>
                  ) : (
                    <span className="ph-hide cat-meter" aria-hidden="true" />
                  ))}
                  {meta && <span className="t-meta cat-budget dt-only">{meta}</span>}
                  <span className="r-amt">
                    <MaskedMoney text={money(category.spent)} className="t-amt" />
                  </span>
                </>
              );
              const target = href(category);
              return (
                <li key={category.id ?? "uncategorized"}>
                  {target ? <Link href={target} className="row cat-row">{content}</Link> : <div className="row is-plain cat-row">{content}</div>}
                </li>
              );
            })}
          </ul>
        </div>
      ))}
      {zeroCount > 0 && (
        <div className="an-more">
          <button type="button" className="ws-btn ws-btn-quiet" aria-expanded={withZero} onClick={() => setWithZero((value) => !value)}>
            {withZero ? t("hideZero", { count: zeroCount }) : t("showZero", { count: zeroCount })}
          </button>
        </div>
      )}
      <CategoryManagerDialog
        open={managing}
        onClose={() => {
          setManaging(false);
          manageRef.current?.focus();
        }}
      />
      <BudgetDialog
        open={budgeting}
        onClose={() => {
          setBudgeting(false);
          budgetRef.current?.focus();
        }}
      />
    </section>
  );
}
