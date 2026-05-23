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
}

export interface LedgerResponse {
  scope: string;
  range: { from: string; to: string };
  rows: LedgerRow[];
  paging: { limit: number; has_more: boolean; next_cursor: string | null };
}
