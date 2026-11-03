import type { FeeLinkFields } from "@/components/ledger/LedgerRowMarks";
import type { HabitBasis } from "@/lib/provenance";

/** A row of `GET /api/transactions` and `GET /api/transactions/{id}`. */
export interface LedgerRow extends FeeLinkFields {
  id: string;
  account_id: string;
  account_name: string;
  category_id: string | null;
  category_name: string | null;
  category_icon?: string | null;
  category_color?: string | null;
  goal_id: string | null;
  goal_name: string | null;
  obligation_id: string | null;
  obligation_name: string | null;
  obligation_allocations?: { obligation_id: string; obligation_name: string; amount: number }[];
  recurring_rule_id?: string | null;
  type: "expense" | "income";
  kakeibo_type?: string | null;
  amount: number;
  notes: string | null;
  date: string;
  /** The server's local day of the row (`settings.tz`); older APIs leave it out. */
  local_date?: string;
  receipt_path: string | null;
  created_at: string;
  is_excluded_from_budget?: boolean;
  partner_id?: string | null;
  is_consolidated_transfer?: boolean;
  target_account_name?: string;
  target_account_id?: string;
  movement_id?: string | null;
  investment_topup_id?: string | null;
  movement_kind?: "investment_topup" | null;
  movement_role?: "outbound" | "inbound" | null;
  transfer_target_account_id?: string | null;
  transfer_target_account_name?: string | null;
  is_inferred_transfer?: boolean;
  source?: string;
  merchant?: string | null;
  review_status?: string;
  classified_by?: string | null;
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
