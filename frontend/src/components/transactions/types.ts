import type { HabitBasis } from "@/lib/provenance";

/** A transfer-fee row and its movement, linked both ways (movement pairing D5). */
export interface FeeLinkFields {
  movement_fee?: { transaction_id: string; amount: number } | null;
  fee_movement?: {
    movement_id: string;
    source_account_name: string | null;
    target_account_name: string | null;
    amount: number;
  } | null;
}

/**
 * What a move or an expense did to "Ditabung & investasi", by the server's savings rule (apply-markas-transaksi
 * D27); null when the row changes nothing there.
 */
export type SavingFlow = "saved" | "invested" | "taken" | "paid_from_savings" | "saving_pillar" | "debt_payment" | null;

export type CreditRole = "purchase" | "fee" | "cash_advance" | "payment" | "refund";

/** One instalment of a credit purchase, as the server's schedule sees it (add-credit-spending-flow, H5). */
export interface CreditInstallment {
  kind: string;
  transaction_id: string | null;
  date: string;
  due_date: string;
  installment: { number: number; count: number } | null;
  amount: number;
  left_to_pay: number;
  settled_on: string | null;
  days_to_settle: number | null;
  settled_on_time: boolean | null;
  bill_status?: string | null;
}

/** One item a credit payment paid, with the share it paid (show-credit-status-and-payment-settlement D9). */
export interface CreditSettledItem {
  kind: "purchase" | "fee" | "cash_advance" | "opening";
  /** Null for opening debt, which has no ledger row to open. */
  transaction_id: string | null;
  title: string | null;
  installment: { number: number; count: number } | null;
  amount: number;
  item_amount: number;
  due_date: string;
}

/** The row's tie to a credit account (`credit` on `GET /transactions`); null for every other row. */
export interface CreditBlock {
  role: CreditRole;
  account_id: string;
  account_name: string;
  installment_count?: number;
  installments_settled?: number;
  next_due_date?: string | null;
  left_to_pay?: number | null;
  settled_on?: string | null;
  days_to_settle?: number | null;
  settled_on_time?: boolean | null;
  due_dates_unavailable?: boolean;
  /** Only on `GET /transactions/{id}`. */
  installments?: CreditInstallment[];
  /** Only on `GET /transactions/{id}`: the stored first due date and who set it ("owner" or "system"), or null. */
  first_due_date?: string | null;
  first_due_date_set_by?: "owner" | "system" | null;
  /** Only on `GET /transactions/{id}` of a payment, in settlement order. */
  settled_items?: CreditSettledItem[];
  /** Only on `GET /transactions/{id}` of a payment: the part that covers nothing yet. */
  kept_for_later?: number;
}

/** The facts a list row describes itself with: one ledger row, or two movement legs consolidated into one. */
export interface RowFacts {
  id: string;
  account_id: string;
  account_name: string;
  /** The bank of a pocket; null for an account without a parent. */
  account_parent_name?: string | null;
  category_name: string | null;
  /** The category's system key (investment, internal_movement, balance_adjustment); null for an ordinary one. */
  category_system_key?: string | null;
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
  /** The partner leg's bank when its account is a pocket. */
  transfer_target_account_parent_name?: string | null;
  target_account_id?: string;
  target_account_name?: string;
  target_account_parent_name?: string | null;
  is_consolidated_transfer?: boolean;
  is_inferred_transfer?: boolean;
  is_excluded_from_budget?: boolean;
  /** Absent from an API older than apply-markas-transaksi. */
  saving_flow?: SavingFlow;
  /** Absent from an API older than add-credit-spending-flow. */
  credit?: CreditBlock | null;
}

/** A row of `GET /api/transactions` and `GET /api/transactions/{id}`. */
export interface LedgerRow extends RowFacts, FeeLinkFields {
  category_id: string | null;
  category_icon?: string | null;
  category_color?: string | null;
  goal_id: string | null;
  goal_name: string | null;
  obligation_id: string | null;
  obligation_name: string | null;
  obligation_allocations?: { obligation_id: string; obligation_name: string; amount: number }[];
  recurring_rule_id?: string | null;
  /** The server's local day of the row (`settings.tz`); older APIs leave it out. */
  local_date?: string;
  receipt_path: string | null;
  created_at: string;
  review_status?: string;
  source?: string;
  habit_basis?: HabitBasis | null;
  offset_category_id?: string | null;
  offset_category_name?: string | null;
  is_reimbursement?: boolean;
}

/** The detail adds what only one row needs: its waiting state and the owner's own source text. */
export interface LedgerRowDetail extends LedgerRow {
  waiting?: boolean;
  origin?: { kind: "notification"; app: string | null; title: string | null; text: string } | { kind: "capture"; text: string } | null;
}
