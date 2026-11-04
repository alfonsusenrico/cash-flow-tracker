import type { EmergencyRunway } from "@/components/dashboard/EmergencyCoverage";

export interface AccountItem {
  id: string;
  parent_id?: string | null;
  default_funding_account_id?: string | null;
  default_funding_account_name?: string | null;
  name: string;
  type: "cash" | "bank" | "wallet" | "investment";
  initial_balance: number;
  balance: number;
  color?: string;
  display_order?: number;
  instrument_type?: "stock" | "mutual_fund" | "gold" | "crypto" | "deposit" | "other" | null;
  instrument_symbol?: string | null;
  units?: number | null;
  avg_buy_price?: number | null;
  last_price?: number | null;
  last_price_at?: string | null;
  capital_gain?: number | null;
  capital_gain_pct?: number | null;
  cost_basis?: number | null;
  /** A platform's own total over its products; null when a product has no cost basis (refine-workspace-layout D8). */
  group_capital_gain?: number | null;
  group_cost_basis?: number | null;
  investment_tracking_mode?: string | null;
  investment_value_estimated?: boolean;
  reconciliation_required?: boolean;
  is_archived: boolean;
  is_parent?: boolean;
  default_pocket_id?: string | null;
  children?: AccountItem[];
  created_at: string;
}

export interface AccountsResponse {
  ok: boolean;
  accounts: AccountItem[];
  total_balance: number;
}

export interface NetWorthResponse {
  ok: boolean;
  net_worth: number;
  total_assets: number;
  total_liabilities: number;
  accounts: { id: string; name: string; type: string; balance: number; share: number }[];
  obligations: { id: string; name: string; total_amount: number; remaining_amount: number; due_date: string | null; minimum_payment: number | null; notes: string | null }[];
  runway: EmergencyRunway;
  liquid: number;
  investment: number;
  reconciliation_pending: number;
  lines: { key: string; label: string; amount: number }[];
}
