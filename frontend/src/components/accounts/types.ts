import type { EmergencyRunway } from "@/components/dashboard/EmergencyCoverage";

/** What the money in a kantong or produk is for ("Kegunaan", organize-accounts-by-use D1). */
export type AccountRole = "harian" | "tabungan" | "dana_investasi" | "investasi" | "utang";
/** The investment kind of a product (D6); null for every account that is not a product. */
export type InvestmentKind = "emas" | "reksa_dana" | "saham" | "lainnya";

export interface AccountItem {
  id: string;
  parent_id?: string | null;
  default_funding_account_id?: string | null;
  default_funding_account_name?: string | null;
  name: string;
  type: "cash" | "bank" | "wallet" | "investment" | "credit";
  initial_balance: number;
  balance: number;
  /** A parent's balance outside its pockets; equal to `balance` for an account without pockets. */
  own_balance?: number;
  role?: AccountRole;
  kind?: InvestmentKind | null;
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
  /** Credit accounts only (add-credit-accounts D2): the provider's limit and the day the bill is paid. */
  credit_limit?: number | null;
  payment_day?: number | null;
  /** The day of the month a credit bill is issued (add-credit-spending-flow V54). */
  statement_day?: number | null;
  /** What a credit account owes, as a positive amount; null for every other type. */
  credit_owed?: number | null;
  over_limit?: boolean;
  /** The limit minus what is owed; negative over the limit, null without a limit (H8). */
  limit_left?: number | null;
  /** The next unpaid credit bill, computed at read time (H8); null without one. */
  next_due?: CreditNextDue | null;
  is_archived: boolean;
  is_parent?: boolean;
  default_pocket_id?: string | null;
  children?: AccountItem[];
  created_at: string;
}

export type CreditBillStatus = "open" | "upcoming" | "due_today" | "overdue" | "paid" | "paid_late";

export interface CreditNextDue {
  due_date: string;
  amount: number;
  left_to_pay: number;
  status: CreditBillStatus;
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
  /** Saldo tersedia: Uang harian and Tabungan, without Dana investasi (D2). */
  liquid: number;
  /** Cash waiting to be invested, such as an RDN; part of net worth, not of Saldo tersedia. */
  dana_investasi?: number;
  investment: number;
  reconciliation_pending: number;
  /** What credit accounts owe; already part of `total_liabilities`. */
  credit_owed?: number;
  lines: { key: string; label: string; amount: number }[];
}

export type LayoutGrouping = "use" | "rekening";

/** The owner's saved order of rows and cards for one grouping (D5). */
export interface AccountLayout {
  grouping: LayoutGrouping;
  sections: string[];
  cards: Record<string, string[]>;
}
