export interface RecurringRule {
  id: string;
  name: string;
  type: "expense" | "income" | "transfer" | "investment_topup";
  amount: number;
  source_account_id: string;
  source_account_name?: string;
  target_account_id?: string | null;
  target_account_name?: string | null;
  category_id?: string | null;
  category_name?: string | null;
  category_icon?: string | null;
  category_color?: string | null;
  obligation_id?: string | null;
  obligation_name?: string | null;
  schedule_type: "monthly_day" | "payday" | "weekly";
  schedule_day?: number | null;
  notes?: string | null;
  /** Historical flag; payroll allocations are no longer executed. */
  is_payroll_allocation: boolean;
  /** Always false: plans never post transactions. */
  auto_post: boolean;
  legacy_auto_post?: boolean | null;
  last_executed_at?: string | null;
  next_due_date: string;
  is_active: boolean;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface PayrollItemPayload {
  rule_id?: string;
  source_account_id: string;
  target_account_id: string;
  amount: number;
  notes?: string;
}

export interface OccurrenceCandidate {
  id: string;
  amount: number;
  date: string;
  notes?: string | null;
  merchant?: string | null;
  source?: string | null;
  account_name: string;
  exact_amount: boolean;
}

/** A due plan with recorded transactions that might satisfy it; none is linked automatically. */
export interface DueExpectation extends RecurringRule {
  match_state: "none" | "single" | "ambiguous";
  window_days: number;
  candidates: OccurrenceCandidate[];
}

/** An occurrence linked automatically to its one certain recorded payment; the owner can undo it. */
export interface AutoLinkedOccurrence {
  rule_id: string;
  scheduled_for: string;
  linked_at: string;
  name: string;
  amount: number;
  type: RecurringRule["type"];
  obligation_name?: string | null;
  transaction_id?: string | null;
  transaction_date?: string | null;
  merchant?: string | null;
  notes?: string | null;
  account_name?: string | null;
}

/** Prefill for a non-posting expense plan, from a detected recurring charge; the owner confirms it. */
export interface ExpensePlanDraft {
  name: string;
  type: "expense";
  amount: number;
  source_account_id: string;
  category_id: string | null;
  schedule_type: "monthly_day" | "weekly";
  schedule_day: number;
}
