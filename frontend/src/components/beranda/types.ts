import type { KakeiboBucket } from "@/lib/kakeibo";

/** `GET /api/dashboard/cycle-summary` (design D12): every Beranda figure, computed on the server. */
/** Whether a label is a name the user gave or fixed wording the web renders from its key (add-language-system D7). */
export type LabelSource = "user" | "system";

/** A note's facts as a code with integer parameters, rendered by the web in the interface language. */
export interface NoteCode {
  code: "includes_savings_moves" | "reduced_by_reimbursements";
  params: { amount?: number };
}

export interface SummaryLine {
  key: string;
  /** Today's Indonesian wording; the web shows it only for `label_source` "user". */
  label: string;
  label_source?: LabelSource;
  amount: number;
  note?: string | null;
  note_codes?: NoteCode[];
  href?: string | null;
}

export type WaitingSection = "questions" | "ambiguous" | "awaiting" | "groups";

export interface WaitingLine {
  section: WaitingSection;
  transaction_id: string;
  label: string;
  label_source?: LabelSource;
  /** The fixed wording of a system label, such as "undescribed". */
  label_key?: "undescribed" | "uncategorized" | null;
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
  /** For a plan paying a credit bill: the part not yet in Pengeluaran (add-credit-spending-flow D9). */
  amount: number;
  /** A plan paying a credit bill: its full amount, of which `counted_elsewhere` is already in Pengeluaran. */
  plan_amount?: number;
  counted_elsewhere?: number;
  credit_account_id?: string;
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

/** A scheduled line as the safe amount counts it: `safe_amount` is 0 with the reason when it is counted elsewhere. */
export type SafeScheduledLine = (PlanLine | GoalLine) & {
  safe_amount: number;
  not_counted_reason: "in_credit_owed" | "paid_from_savings" | null;
};

export interface ComparisonFigure {
  current: number;
  previous: number | null;
  change: number | null;
}

/** `spending.credit` (add-credit-spending-flow D11): spending booked on credit, inside Pengeluaran, and cash paid to credit. */
export interface CreditSpending {
  spent_on_credit: number;
  paid_to_credit: number;
  lines: { key: string; account_id: string; label: string; label_source?: LabelSource; spent_on_credit: number; paid_to_credit: number }[];
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
  /** Pengeluaran: Keluar without the saving bucket (refine-workspace-layout D4). */
  spending: { amount: number; lines: (SummaryLine & { key: KakeiboBucket })[]; credit?: CreditSpending };
  /** Ditabung & investasi: the saving bucket of Keluar. `spending + fresh_saving = keluar`. */
  fresh_saving: { amount: number; lines: (SummaryLine & { key: KakeiboBucket })[] };
  /**
   * The savings change behind `fresh_saving.amount` (organize-accounts-by-use D4): moves into savings, moves
   * out, spending paid from savings and saving-pillar spending, each positive; `total` may be negative.
   * Absent from an API older than account roles.
   */
  tabungan?: { total: number; masuk: number; diambil: number; belanja: number; pilar_tabungan: number };
  masuk: { total: number; lines: SummaryLine[] };
  waiting: { count: number; total: number; in_keluar: number; not_in_keluar: number; lines: WaitingLine[] };
  scheduled: {
    total: number;
    /** The bills (plan lines) alone; absent from an API older than Target & Tagihan's list. */
    plans_total?: number;
    window_start: string;
    window_end: string;
    window_basis: "expected_salary" | "next_30_days";
    lines: (PlanLine | GoalLine)[];
  };
  /**
   * Aman dipakai from what the owner holds (count-safe-amount-from-daily-money D1 to D5): daily money less
   * everything owed on credit less the plans paid from daily money, never below 0; `short_by` holds a shortfall.
   */
  safe_amount: {
    amount: number;
    short_by: number;
    daily_money: { total: number; accounts: { account_id: string; name: string; parent_name: string | null; amount: number }[] };
    credit_owed: { total: number; accounts: { account_id: string; name: string; amount: number; next_due: { due_date: string; left_to_pay: number } | null }[] };
    scheduled: {
      total: number;
      window_end: string;
      window_basis: "expected_salary" | "next_30_days";
      lines: SafeScheduledLine[];
    };
  };
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
    /** Active obligations only; credit accounts' debt is `credit_owed`. */
    debts: number;
    reconciliation_pending?: number;
    /** Cash that can only be invested, such as an RDN: an asset outside Saldo tersedia. */
    dana_investasi?: number;
    /** What credit accounts owe, a positive amount; utang is `debts + credit_owed`. */
    credit_owed?: number;
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
export type { RowFacts as ActivityRow } from "@/components/transactions/types";
