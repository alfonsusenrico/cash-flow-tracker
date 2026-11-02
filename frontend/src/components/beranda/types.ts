import type { KakeiboBucket } from "@/lib/kakeibo";

/** `GET /api/dashboard/cycle-summary` (design D12): every Beranda figure, computed on the server. */
export interface SummaryLine {
  key: string;
  label: string;
  amount: number;
  note?: string | null;
  href?: string | null;
}

export type WaitingSection = "questions" | "ambiguous" | "awaiting" | "groups";

export interface WaitingLine {
  section: WaitingSection;
  transaction_id: string;
  label: string;
  account_name: string | null;
  type: "expense" | "income";
  date: string;
  amount: number;
  in_keluar: boolean;
  in_keluar_as: KakeiboBucket | null;
}

export interface PlanLine {
  kind: "plan";
  rule_id: string;
  label: string;
  due_date: string;
  overdue: boolean;
  amount: number;
}

export interface GoalLine {
  kind: "goal";
  goal_id: string;
  label: string;
  target_date: string;
  months_left: number;
  set_aside: number;
  funded: number;
  amount: number;
}

export interface ComparisonFigure {
  current: number;
  previous: number | null;
  change: number | null;
}

export type SafeReason = "no_cycle" | "no_expected_salary" | "salary_overdue";
export type ExpectedReason = "too_few_receipts" | "irregular_receipts" | "no_salary_source" | "no_cycle";

export interface CycleSummary {
  ok: boolean;
  as_of: string;
  period: {
    basis: "cycle" | "month";
    status: string;
    cycle_status: string;
    start_date: string;
    today: string;
    day_index: number;
    label: string;
    cycle_offset_basis: "cycle" | "month";
  };
  expected_salary: {
    status: "estimated" | "overdue" | "unknown";
    date: string | null;
    basis: "regular_receipts" | null;
    receipt_dates: string[];
    reason: ExpectedReason | null;
  };
  keluar: { total: number; offsets_total: number; lines: (SummaryLine & { key: KakeiboBucket })[] };
  masuk: { total: number; lines: SummaryLine[] };
  waiting: { count: number; total: number; in_keluar: number; not_in_keluar: number; lines: WaitingLine[] };
  scheduled: {
    total: number;
    window_start: string;
    window_end: string;
    window_basis: "expected_salary" | "next_30_days";
    lines: (PlanLine | GoalLine)[];
  };
  remaining: { total: number; lines: SummaryLine[] };
  safe_to_spend: {
    status: "available" | "unavailable";
    reason: SafeReason | null;
    detail: ExpectedReason | null;
    per_day: number | null;
    days_left: number | null;
    rounding: number;
    over_by: number | null;
  };
  net_worth: {
    total: number;
    liquid: number;
    investment: number;
    debts: number;
    reconciliation_pending?: number;
    lines: SummaryLine[];
  };
  comparison: {
    status: "available" | "too_short" | "no_previous";
    compared_days: number;
    label: string | null;
    previous_label: string | null;
    keluar: ComparisonFigure;
    masuk: ComparisonFigure;
    belanja: ComparisonFigure;
  };
}

/** A row of the recent-activity list: one ledger row, or two movement legs consolidated into one. */
export interface ActivityRow {
  id: string;
  account_id: string;
  account_name: string;
  category_name: string | null;
  type: "expense" | "income";
  amount: number;
  date: string;
  notes: string | null;
  merchant?: string | null;
  kakeibo_type?: string | null;
  review_status?: string | null;
  source?: string | null;
  classified_by?: string | null;
  movement_id?: string | null;
  investment_topup_id?: string | null;
  movement_kind?: "investment_topup" | null;
  movement_role?: "outbound" | "inbound" | null;
  partner_id?: string | null;
  transfer_target_account_id?: string | null;
  transfer_target_account_name?: string | null;
  target_account_id?: string;
  target_account_name?: string;
  is_consolidated_transfer?: boolean;
  is_inferred_transfer?: boolean;
  is_excluded_from_budget?: boolean;
}
