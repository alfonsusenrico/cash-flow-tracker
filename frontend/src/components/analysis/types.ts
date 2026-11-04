/** Response shapes of the Analisis endpoints (redesign-analysis-and-reports D3, D6, D8, D9). Amounts are integer rupiah. */

export type AnalysisTab = "ringkasan" | "kalender" | "aliran";
export type PeriodBasis = "cycle" | "month";
export type KakeiboBucket = "need" | "want" | "culture" | "unexpected" | "saving" | "unclassified";

export const KAKEIBO_BUCKETS: KakeiboBucket[] = ["need", "want", "culture", "unexpected", "saving", "unclassified"];

export interface ServerExplanationLine {
  key: string;
  label: string;
  amount: number;
  note: string | null;
  href: string | null;
}

export interface ServerExplanation {
  total: number;
  lines: ServerExplanationLine[];
  caveat: string | null;
  href: string | null;
}

export interface DailyCadenceEntry {
  date: string;
  label: string;
  expense: number;
  income: number;
}

export interface AnalyticsResponse {
  timeframe?: string;
  timeframe_label?: string;
  daily_cadence?: DailyCadenceEntry[];
  category_variance?: CategoryVariance[];
  comparison?: {
    total_inflow: number;
    total_outflow: number;
    net_cashflow: number;
    gross_outflow?: number;
    reimbursement_offset?: number;
  };
  kakeibo?: { total_allocated: number } & Record<string, number | string>;
  explanations?: { keluar: ServerExplanation; belanja: ServerExplanation; spending?: ServerExplanation };
}

export interface CategoryVariance {
  id: string | null;
  name: string;
  icon?: string | null;
  color?: string | null;
  kind?: string;
  kakeibo_type?: string | null;
  is_archived?: boolean;
  is_uncategorized?: boolean;
  budget: number | null;
  monthly_budget?: number | null;
  spent: number;
  variance?: number | null;
  percentage_used?: number | null;
  status: string;
}

export interface ComparisonWindow {
  start: string;
  end: string;
  start_date: string;
  end_date: string;
  inflow: number;
  outflow: number;
  net: number;
}


export interface CalendarPlan {
  rule_id: string;
  label: string;
  amount: number;
  status: "due" | "overdue" | "met" | "skipped";
}

export interface CalendarWaitingRow {
  transaction_id: string;
  section: "questions" | "ambiguous" | "awaiting" | "groups";
  label: string;
  account_name: string | null;
  type: string;
  amount: number;
}

export interface CalendarDay {
  date: string;
  spend: number;
  spend_count: number;
  heat: number;
  income_total: number;
  income_count: number;
  movement_total: number;
  movement_count: number;
  topup_total: number;
  topup_count: number;
  waiting_count: number;
  waiting_amount: number;
  waiting: CalendarWaitingRow[];
  plans: CalendarPlan[];
}

export interface CalendarResponse {
  window: {
    start: string;
    end: string;
    label: string;
    basis: PeriodBasis;
    today: string;
    day_index: number | null;
    days_total: number;
    is_current: boolean;
    open: boolean;
  };
  expected_salary: { date: string | null; available: boolean; status: string; reason: string | null };
  totals: { spend: number; gross_spend: number; offset_applied: number };
  days: CalendarDay[];
  upcoming: { date: string; rule_id: string; label: string; amount: number; status: CalendarPlan["status"] }[];
  upcoming_total: number;
  upcoming_basis: "expected_salary" | "window_end";
  heat_thresholds: number[];
}

export type FlowNodeKind =
  | "source"
  | "account"
  | "saving"
  | "investment"
  | "pillar"
  | "other_out"
  | "waiting"
  | "remaining"
  | "negative_opening";

export type FlowLinkKind =
  | "income"
  | "opening"
  | "negative_opening"
  | "negative_closing"
  | "transfer"
  | "spend"
  | "saving"
  | "investment"
  | "other"
  | "waiting"
  | "remaining";

export interface FlowNode {
  id: string;
  kind: FlowNodeKind;
  label: string;
  rank: number;
  account_id?: string;
  opening?: number;
  closing?: number;
  archived?: boolean;
  members?: string[];
  pillar?: KakeiboBucket;
  folded_by_default?: boolean;
}

export interface FlowLink {
  key: string;
  from: string;
  to: string;
  kind: FlowLinkKind;
  amount: number;
  count: number;
  waiting_count: number;
  waiting_amount: number;
}

export interface FlowResponse {
  window: { start: string; end: string; through: string; label?: string; is_current?: boolean };
  nodes: FlowNode[];
  links: FlowLink[];
  summary: {
    masuk: number;
    spend_gross: number;
    belanja_gross: number;
    reimbursement_offset: number;
    belanja: number;
    keluar: number;
    saving_net: number;
    investment_net: number;
    waiting_out: number;
    waiting_in: number;
    waiting_in_buckets: number;
    remaining: number;
    other_out: number;
  };
}

export interface FlowRow {
  id: string;
  link: string;
  type: string;
  amount: number;
  row_amount: number;
  date: string;
  local_date: string;
  account_id: string;
  account_name: string;
  category_name: string | null;
  merchant: string | null;
  notes: string | null;
  movement_id: string | null;
  waiting: boolean;
}

export interface FlowRowsResponse {
  rows: FlowRow[];
  total_count: number;
  total_amount: number;
  offset: number;
  limit: number;
}
