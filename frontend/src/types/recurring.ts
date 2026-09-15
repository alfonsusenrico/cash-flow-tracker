export interface RecurringRule {
  id: string;
  name: string;
  type: "expense" | "income" | "transfer";
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
  is_payroll_allocation: boolean;
  auto_post: boolean;
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
