import type { Format } from "@/lib/format";
import type { Translate } from "@/lib/i18n/types";

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
  /** `target_months` × the essential monthly cost, rounded by the server; null while the cost is unknown. */
  target_amount?: number | null;
  /** The reserve over `target_amount` in percent with one decimal, never below 0; null while the cost is unknown. */
  coverage_pct?: number | null;
  essential_cost?: EssentialCost;
}

/** The coverage figure, or a plain statement that it cannot be calculated yet. */
export function coverageText(runway: EmergencyRunway, t: Translate, format: Format): string {
  const months = runway.coverage_months ?? runway.runway_months;
  if (months == null || runway.runway_days == null) return t("breakdown.coverageUnknown");
  if (runway.status === "zero") return t("breakdown.coverageZero");
  return t("breakdown.coverage", { monthsText: format.decimal(months, 3), days: runway.runway_days, daysText: format.integer(runway.runway_days) });
}

/** How the essential monthly cost was obtained, in one sentence. */
export function essentialBasisText(cost: (Pick<EssentialCost, "method" | "debt_minimum" | "period_count"> & Partial<EssentialCost>) | undefined, t: Translate): string {
  if (!cost || cost.method === "unknown") return t("breakdown.essentialUnknown");
  const debt = cost.debt_minimum > 0 ? t("breakdown.plusMinimumInstallments") : "";
  if (cost.method === "need_budget") return t("breakdown.essentialFromBudget", { debt });
  return t("breakdown.essentialMedian", { count: cost.period_count, debt });
}
