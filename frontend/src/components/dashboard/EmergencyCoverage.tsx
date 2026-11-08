"use client";

import Link from "next/link";
import { useTranslations } from "next-intl";
import { useFormat, type Format } from "@/lib/format";
import type { Translate } from "@/lib/i18n/types";
import { ledgerHref } from "@/lib/ledgerFilters";

export interface EssentialCostPeriod {
  kind: "cycle" | "month";
  label: string;
  start: string;
  end: string;
  start_date: string;
  end_date: string;
  days: number;
  gross_need: number;
  reimbursement_offset: number;
  amount: number;
  /** The part of `amount` with no pillar yet; counted so a missing pillar never lowers the cost. */
  unclassified_amount?: number;
}

export interface EssentialCost {
  amount: number | null;
  living_amount: number | null;
  debt_minimum: number;
  method: "median_need_periods" | "need_budget" | "unknown";
  basis: "history" | "budget" | "unknown";
  period_count: number;
  periods_used: EssentialCostPeriod[];
  limited_history: boolean;
}

export interface EmergencyRunway {
  runway_days: number | null;
  runway_months: number | null;
  coverage_months?: number | null;
  status: "healthy" | "moderate" | "critical" | "zero" | "unknown";
  emergency_fund_balance: number;
  balance_basis?: "emergency_goals" | "all_liquid";
  emergency_goal_names?: string[];
  monthly_primary_expense: number | null;
  target_months?: number;
  essential_cost?: EssentialCost;
}

/** The coverage figure, or a plain statement that it cannot be calculated yet. */
export function coverageText(runway: EmergencyRunway, t: Translate, format: Format): string {
  const months = runway.coverage_months ?? runway.runway_months;
  if (months == null || runway.runway_days == null) return t("breakdown.coverageUnknown");
  if (runway.status === "zero") return t("breakdown.coverageZero");
  return t("breakdown.coverage", { monthsText: format.decimal(months, 3), days: runway.runway_days, daysText: format.integer(runway.runway_days) });
}

export function coverageTone(status: EmergencyRunway["status"]): string {
  if (status === "healthy") return "bg-[var(--surface-2)] text-in-fg border-[var(--line)]";
  if (status === "moderate") return "bg-[var(--surface-2)] text-warn-fg border-[var(--line)]";
  if (status === "critical") return "bg-[var(--surface-2)] text-out-fg border-[var(--line)]";
  return "bg-[var(--surface-2)] text-[var(--text-2)] border-[var(--line)]";
}

/** How the essential monthly cost was obtained, in one sentence. */
export function essentialBasisText(cost: EssentialCost | undefined, t: Translate): string {
  if (!cost || cost.method === "unknown") return t("breakdown.essentialUnknown");
  const debt = cost.debt_minimum > 0 ? t("breakdown.plusMinimumInstallments") : "";
  if (cost.method === "need_budget") return t("breakdown.essentialFromBudget", { debt });
  return t("breakdown.essentialMedian", { count: cost.period_count, debt });
}

interface Props {
  runway: EmergencyRunway;
  bal: (amount: number) => string;
}

/** The basis of Ketahanan Dana: the reserve, the essential cost and the periods behind it. */
export function EmergencyCoverageBasis({ runway, bal }: Props) {
  const t = useTranslations();
  const format = useFormat();
  const cost = runway.essential_cost;
  const periods = cost?.periods_used ?? [];
  return (
    <section aria-labelledby="coverage-basis-title" className="ws-card space-y-3 text-xs">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <h2 id="coverage-basis-title" className="text-sm font-semibold text-[var(--text)]">{t("breakdown.safetyNetBasis")}</h2>
          <p className="mt-0.5 text-[var(--text-2)]">{t("breakdown.safetyNetRule")}</p>
        </div>
        <span className={`inline-flex items-center rounded-full border px-2.5 py-1 font-semibold tabular-nums ${coverageTone(runway.status)}`}>
          {coverageText(runway, t, format)}
        </span>
      </div>

      <dl className="grid gap-3 sm:grid-cols-2">
        <div className="min-w-0 space-y-0.5">
          <dt className="font-semibold text-[var(--muted)]">{t("breakdown.reserveBalance")}</dt>
          <dd className="text-base font-semibold tabular-nums text-[var(--text)]">{bal(runway.emergency_fund_balance)}</dd>
          <dd className="text-[var(--text-2)]">
            {runway.balance_basis === "all_liquid"
              ? t("breakdown.noDedicatedFund")
              : t("breakdown.fromEmergencyGoals", { names: (runway.emergency_goal_names ?? []).join(", ") })}
          </dd>
        </div>
        <div className="min-w-0 space-y-0.5">
          <dt className="font-semibold text-[var(--muted)]">{t("breakdown.monthlyEssentialCosts")}</dt>
          <dd className="text-base font-semibold tabular-nums text-[var(--text)]">
            {runway.monthly_primary_expense == null ? t("breakdown.unknown") : t("breakdown.perMonth", { amount: bal(runway.monthly_primary_expense) })}
          </dd>
          <dd className="text-[var(--text-2)]">{essentialBasisText(cost, t)}</dd>
          {cost?.limited_history && (
            <dd>
              <span className="inline-flex rounded-md border border-[var(--line)] bg-[var(--surface-2)] px-2 py-0.5 font-semibold text-warn-fg">
                {t("breakdown.limitedHistory", { count: cost.period_count })}
              </span>
            </dd>
          )}
          {cost && cost.debt_minimum > 0 && (
            <dd className="tabular-nums text-[var(--text-2)]">{t("breakdown.includesMinimumInstallments", { amount: bal(cost.debt_minimum) })}</dd>
          )}
        </div>
      </dl>

      {periods.length > 0 && (
        <details className="border-t border-[var(--line)] pt-2">
          <summary className="flex min-h-11 cursor-pointer items-center font-semibold text-[var(--text)]">
            {t("breakdown.viewPeriodsUsed", { count: periods.length })}
          </summary>
          <p className="mb-2 text-[var(--text-2)]">{t("breakdown.periodsRule")}</p>
          <ul className="space-y-1">
            {periods.map((period) => {
              const unclassified = period.unclassified_amount ?? 0;
              return (
                <li key={period.start} className="min-w-0">
                  <div className="flex min-w-0 items-center justify-between gap-2">
                    <span className="min-w-0 truncate">
                      {period.label} <span className="text-[var(--muted)]">{t("breakdown.periodDays", { count: period.days })}</span>
                    </span>
                    <Link
                      href={ledgerHref({ from: period.start, to: period.end, kakeibo: "need", type: "expense" })}
                      aria-label={t("breakdown.viewNeedsTransactions", { amount: bal(period.amount - unclassified), period: period.label })}
                      className="inline-flex min-h-11 shrink-0 items-center font-semibold tabular-nums text-[var(--text)] underline underline-offset-2"
                    >
                      {bal(period.amount - unclassified)}
                    </Link>
                  </div>
                  {unclassified > 0 && (
                    <div className="flex min-w-0 items-center justify-between gap-2 pl-3 text-[var(--text-2)]">
                      <span className="min-w-0 truncate">{t("breakdown.plusNoPillarYet")}</span>
                      <Link
                        href={ledgerHref({ from: period.start, to: period.end, kakeibo: "unclassified", type: "expense" })}
                        aria-label={t("breakdown.viewNoPillarTransactions", { amount: bal(unclassified), period: period.label })}
                        className="inline-flex min-h-11 shrink-0 items-center tabular-nums underline underline-offset-2"
                      >
                        {bal(unclassified)}
                      </Link>
                    </div>
                  )}
                </li>
              );
            })}
          </ul>
        </details>
      )}
    </section>
  );
}
