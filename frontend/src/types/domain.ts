/**
 * Domain types — hand-written until openapi-typescript generation is wired.
 * Run `npm run generate-types` to regenerate from the live OpenAPI spec.
 */

export interface User {
  username: string;
  full_name: string;
  tz: string;
}

export interface Account {
  account_id: string;
  account_name: string;
  profile_type: "tabungan" | "fixed_spending" | "dynamic_spending";
  is_payroll_source: boolean;
  is_no_limit: boolean;
  is_buffer: boolean;
  fixed_limit_amount: number | null;
  institution: string | null;
  account_number: string | null;
  balance?: number;
}

export interface Category {
  category_id: string;
  name: string;
  kind: "income" | "expense" | "transfer" | "adjustment";
  parent_category_id: string | null;
  color: string | null;
  icon: string | null;
  is_archived: boolean;
}

export interface Transaction {
  transaction_id: string;
  account_id: string;
  account_name: string;
  date: string;
  transaction_name: string;
  debit: number;
  credit: number;
  balance: number;
  is_transfer: boolean;
  is_cycle_topup: boolean;
  transfer_id: string | null;
  category_id: string | null;
  notes: string | null;
  currency: "IDR" | "USD";
  tags: string[];
  is_reviewed: boolean;
}

export interface SummaryAccount {
  account_id: string;
  account_name: string;
  starting_balance: number;
  current_balance: number;
  total_in: number;
  total_out: number;
  budget: number | null;
  budget_used: number | null;
  budget_remaining: number | null;
  budget_pct: number | null;
  budget_status: "ok" | "warn" | "critical" | null;
}

export interface SummaryResponse {
  range: { from: string; to: string };
  month: string;
  payday: { day: number; source: string; default_day: number; override_day: number | null };
  total_asset: number;
  accounts: SummaryAccount[];
}

export interface MonthlyPeriod {
  period_id: string;
  month: string;
  from_date: string;
  to_date: string;
  payday_day: number;
  status: "open" | "closed" | "reviewed";
  notes: string | null;
  closed_at: string | null;
}

export interface LedgerRow {
  no: number;
  account_id: string | null;
  account_name: string;
  date: string;
  transaction_id: string;
  transaction_name: string;
  debit: number;
  credit: number;
  balance: number;
  is_transfer: boolean;
  is_cycle_topup: boolean;
  transfer_id: string | null;
  notes: string | null;
  category_id: string | null;
  is_reviewed: boolean;
  tags: string[];
}

export interface LedgerResponse {
  scope: string;
  range: { from: string; to: string };
  rows: LedgerRow[];
  paging: { limit: number; offset: number; has_more: boolean; next_offset: number };
}

export interface Counterparty {
  counterparty_id: string;
  name: string;
  type: "person" | "client" | "vendor" | "institution" | "other";
  notes: string | null;
}

export interface Obligation {
  obligation_id: string;
  kind: "receivable" | "payable";
  title: string;
  description: string | null;
  principal_amount: number;
  outstanding_amount: number;
  settled_amount: number;
  currency: "IDR";
  status: "open" | "partial" | "settled" | "cancelled" | "written_off";
  issue_date: string;
  due_date: string | null;
  default_account_id: string | null;
  default_account_name: string | null;
  category_id: string | null;
  category_name: string | null;
  counterparty_id: string | null;
  counterparty_name: string | null;
  counterparty_type: Counterparty["type"] | null;
  notes: string | null;
  recurrence_frequency: "none" | "weekly" | "monthly" | "quarterly" | "yearly";
  auto_post_enabled: boolean;
  auto_post_day: number | null;
}

export interface ObligationSettlement {
  settlement_id: string;
  transaction_id: string | null;
  account_id: string;
  account_name: string;
  amount: number;
  settled_at: string;
  notes: string | null;
  reversed_at: string | null;
  reversed_by: string | null;
}

export interface ObligationSummary {
  receivable_outstanding: number;
  payable_outstanding: number;
  receivable_overdue: number;
  payable_overdue: number;
  due_soon: number;
  open_count: number;
  net_expected: number;
}
