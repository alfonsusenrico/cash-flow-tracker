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
  /** The reimbursements subtracted from Pengeluaran (apply-markas-laporan D29); `reimbursement_offset` is Belanja's. */
  spending_reimbursement_offset: number;
}

export type SpendingBucket = Exclude<KakeiboBucket, "saving">;

export interface ComparisonTotals {
  spending: number;
  inflow: number;
  fresh_saving: number;
  /** Masuk − Pengeluaran. */
  difference: number;
}

/** A change is `null` where no verdict is given: a running period, or a partial previous period. */
export type ComparisonChange = { [K in keyof ComparisonTotals]: number | null };

export interface ComparisonPeriodInfo {
  label: string;
  start: string;
  end: string;
  start_date: string;
  end_date: string;
  complete: boolean;
  partial: boolean;
  tracking_from: string | null;
}

/** A ledger window plus its filters, exactly what the figure next to it adds up from. */
export interface ComparisonRef {
  from: string;
  to: string;
  category_id?: string;
  uncategorized?: boolean;
  kakeibo_type?: KakeiboBucket;
  q?: string;
}

export interface ComparisonCategory {
  category_id: string | null;
  name: string;
  amount: number;
  share: number | null;
  previous: number | null;
  change: number | null;
  baseline: number | null;
  baseline_change: number | null;
  ref: ComparisonRef;
}

export interface ComparisonPillar {
  pillar: SpendingBucket;
  amount: number;
  share: number | null;
  previous: number | null;
  change: number | null;
  baseline: number | null;
  baseline_change: number | null;
  ref: ComparisonRef;
  categories: ComparisonCategory[];
}

export interface ComparisonFact {
  kind: "category_above_average" | "category_new" | "want_share" | "top_merchant" | "recurring" | "unexpected";
  /** Rendered by the server in the account's language from `label_code` (localize-server-text). */
  label: string;
  label_code?: string;
  label_params?: Record<string, string | number>;
  amount: number;
  difference?: number;
  baseline?: number;
  share?: number;
  previous_share?: number;
  count?: number;
  ref: ComparisonRef | null;
}

export interface ComparisonHistoryItem {
  label: string;
  start: string;
  end: string;
  complete: boolean;
  partial: boolean;
  viewed: boolean;
  spending: number;
  pillars: Record<SpendingBucket, number>;
}

/** The whole-period comparison (compare-whole-months D7), the same object as `GET /reports/monthly/comparison`. */
export interface ReportComparison {
  basis: ReportBasis;
  period: ComparisonPeriodInfo;
  running: boolean;
  totals: ComparisonTotals;
  previous: (ComparisonPeriodInfo & { totals: ComparisonTotals; change: ComparisonChange }) | null;
  baseline: { period_count: number; labels: string[]; totals: ComparisonTotals; change: ComparisonChange } | null;
  progress: {
    previous_label: string | null;
    previous_spending: number | null;
    spending: number;
    share: number | null;
    /** Pengeluaran so far as a whole percent of the baseline's; null without a baseline or an older API. */
    baseline_share?: number | null;
    pillars: { pillar: SpendingBucket; amount: number; previous: number | null; share: number | null }[];
  } | null;
  pillars: ComparisonPillar[];
  facts: ComparisonFact[];
  history: ComparisonHistoryItem[];
  /** The top-merchant fact on the Pengeluaran basis, shown on Laporan instead of the one in `facts` (D29). */
  spending_top_merchant: ComparisonFact | null;
}

export type ReportComparisonResponse =
  | { empty: false; cycle_available: boolean; comparison: ReportComparison }
  | { empty: true; cycle_available: boolean };

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
  living_amount: number | null;
  debt_minimum: number;
  method: "median_need_periods" | "need_budget" | "unknown";
  limited_history: boolean;
  period_count: number;
  coverage_months: number | null;
  target_months: number;
  status: string;
  balance_basis: string;
}

/**
 * Budget against actual. `spent` and `remaining` are on the Belanja basis; Laporan shows `spending` and
 * `spending_remaining`, on the Pengeluaran basis (apply-markas-laporan D29).
 */
export interface ReportBudget {
  period_basis: ReportBasis;
  categories: {
    category_id: string; name: string; budget: number; spent: number; remaining: number; spending: number; spending_remaining: number;
  }[];
  total_budget: number;
  total_spent: number;
  total_spending: number;
  overall: { budget: number; spent: number; remaining: number; spending: number; spending_remaining: number } | null;
}

export type MerchantSpending = { name: string; search_text: string; amount: number };

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
    /** Belanja basis; Laporan shows `top_spending_merchants`, which leave out the Tabungan pillar (D29). */
    top_merchants: MerchantSpending[];
    top_spending_merchants: MerchantSpending[];
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

export function reportQuery(basis: ReportBasis, startDate: string | null, path = "/reports/monthly"): string {
  const params = new URLSearchParams({ basis });
  if (startDate) params.set("start_date", startDate);
  return `${path}?${params.toString()}`;
}

export function comparisonQuery(basis: ReportBasis, startDate: string | null): string {
  return reportQuery(basis, startDate, "/reports/monthly/comparison");
}

/** The ledger link of a comparison figure: its own window and filters. */
export function refHref(ref: ComparisonRef): string {
  const filters: Partial<LedgerFilters> = { from: ref.from, to: ref.to, type: "expense" };
  if (ref.uncategorized) filters.category = UNCATEGORIZED;
  else if (ref.category_id) filters.category = ref.category_id;
  if (ref.kakeibo_type) filters.kakeibo = ref.kakeibo_type;
  if (ref.q) filters.q = ref.q;
  return ledgerHref(filters);
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
