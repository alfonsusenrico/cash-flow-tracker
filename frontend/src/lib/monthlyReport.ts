import type { KakeiboBucket, KakeiboPillar } from "@/lib/kakeibo";
import { type LedgerFilters, UNCATEGORIZED, ledgerHref } from "@/lib/ledgerFilters";

export type ReportBasis = "month" | "cycle";

export interface ReportPeriod {
  kind: string;
  basis: ReportBasis;
  label: string;
  /** ISO instants of the window, inclusive; the ledger drill-down uses them as `from` and `to`. */
  start: string;
  end: string;
  start_date: string;
  end_date: string;
  days: number;
  complete: boolean;
  tracking_started_inside: boolean;
  first_recorded_date: string | null;
  url: string;
  has_reflection?: boolean;
}

export interface ReportPeriodList {
  basis: ReportBasis;
  cycle_available: boolean;
  periods: ReportPeriod[];
}

export interface FlowFigures {
  inflow: number;
  gross_outflow: number;
  reimbursement_offset: number;
  outflow: number;
  net: number;
}

export interface ComparisonWindow {
  start: string;
  end: string;
  inflow: number;
  outflow: number;
  net: number;
}

export interface ReportComparison {
  previous: {
    compared_days: number;
    current_truncated: boolean;
    current: ComparisonWindow;
    previous: ComparisonWindow | null;
    previous_label: string | null;
    previous_has_data: boolean;
    pillars: { pillar: KakeiboBucket; current: number; previous: number | null; change: number | null }[];
  };
  previous_partial: boolean;
  previous_period: ReportPeriod | null;
  median:
    | { available: false; period_count: number; required: number }
    | {
        available: true;
        period_count: number;
        required: number;
        compared_days: number;
        periods: { label: string; start: string; end: string }[];
        current: ComparisonWindow & { label: string };
        median: { inflow: number; outflow: number; net: number };
      };
}

export interface RecurringCharge {
  merchant: string;
  search_text: string;
  cadence: "weekly" | "monthly";
  typical_amount: number;
  last_amount: number;
  amount_changed: boolean;
  last_date: string;
  monthly_equivalent: number;
  existing_plan: { id: string; name: string } | null;
  category_name: string | null;
}

export interface EssentialSummary {
  amount: number | null;
  method: string;
  limited_history: boolean;
  period_count: number;
  coverage_months: number | null;
  target_months: number;
  status: string;
  balance_basis: string;
}

export interface ReportBudget {
  period_basis: ReportBasis;
  categories: { category_id: string; name: string; budget: number; spent: number; remaining: number }[];
  total_budget: number;
  total_spent: number;
  overall: { budget: number; spent: number; remaining: number } | null;
}

export interface Reflection {
  note: string;
  start_date: string;
  end_date: string;
  updated_at: string;
  label?: string;
}

export interface MonthlyReport {
  empty: false;
  basis: ReportBasis;
  cycle_available: boolean;
  period: ReportPeriod;
  flow: FlowFigures;
  income: { total: number; sources: { category_id: string | null; name: string; amount: number; count: number }[] };
  savings: { fresh_saving: number; saving_expenses: number; net_saving_transfers: number; surplus: number };
  spending: {
    total: number;
    pillars: { pillar: KakeiboBucket; amount: number }[];
    top_categories: { category_id: string | null; name: string; amount: number; is_uncategorized: boolean; kakeibo_type: KakeiboPillar | null }[];
    top_merchants: { name: string; search_text: string; amount: number }[];
    unexpected: {
      amount: number;
      count: number;
      items: { id: string; date: string; amount: number; merchant: string | null; notes: string | null; category_name: string | null }[];
    };
    recurring: { charges: RecurringCharge[]; monthly_equivalent_total: number };
    essential: EssentialSummary;
  };
  comparison: ReportComparison;
  data_quality: {
    pending_review: number;
    needs_attention: number;
    unreviewed: number;
    unclassified: { count: number; amount: number };
    possible_duplicates: number;
    days_without_records: { count: number; dates: string[] };
  };
  budget: ReportBudget | null;
  reflection: Reflection | null;
  previous_reflection: Reflection | null;
}

export type MonthlyReportResponse = MonthlyReport | { empty: true; basis: ReportBasis; cycle_available: boolean };

export function reportQuery(basis: ReportBasis, startDate: string | null): string {
  const params = new URLSearchParams({ basis });
  if (startDate) params.set("start_date", startDate);
  return `/reports/monthly?${params.toString()}`;
}

/** The ledger rows behind a report figure: the period's exact window plus the figure's own filters. */
export function reportLedgerHref(period: Pick<ReportPeriod, "start" | "end">, filters: Partial<LedgerFilters> = {}): string {
  return ledgerHref({ from: period.start, to: period.end, ...filters });
}

export function categoryLedgerHref(
  period: Pick<ReportPeriod, "start" | "end">,
  category: { category_id: string | null; is_uncategorized?: boolean },
  type: "expense" | "income" = "expense",
): string {
  const id = category.is_uncategorized || !category.category_id ? UNCATEGORIZED : category.category_id;
  return reportLedgerHref(period, { type, category: id });
}

/** "7 Okt" for a local ISO date. */
export function shortDate(isoDate: string): string {
  const [year, month, day] = isoDate.slice(0, 10).split("-").map(Number);
  return new Date(Date.UTC(year, month - 1, day)).toLocaleDateString("id-ID", { day: "numeric", month: "short", timeZone: "UTC" });
}

export function percentOf(part: number, whole: number): number {
  return whole > 0 ? Math.round((part / whole) * 1000) / 10 : 0;
}

export function formatPercent(value: number): string {
  return `${value.toLocaleString("id-ID", { maximumFractionDigits: 1 })}%`;
}
