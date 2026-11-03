export interface GoalItem {
  id: string;
  name: string;
  target_amount: number;
  current_amount: number;
  remaining_amount: number;
  percentage_completed: number;
  target_date: string | null;
  monthly_target_pace: number | null;
  color: string;
  icon: string;
  is_emergency?: boolean;
  is_archived: boolean;
  account_ids?: string[];
  linked_accounts?: Array<{
    id: string;
    name: string;
    type: string;
    balance: number;
  }>;
  created_at: string;
}

export interface ObligationItem {
  id: string;
  name: string;
  total_amount: number;
  remaining_amount: number;
  paid_amount: number;
  payoff_percentage: number;
  due_date: string | null;
  minimum_payment: number | null;
  estimated_payoff_months: number | null;
  notes: string | null;
  is_archived: boolean;
  created_at: string;
}

export const GOAL_COLORS = [
  { value: "#10b981", label: "Hijau" },
  { value: "#3b82f6", label: "Biru" },
  { value: "#f59e0b", label: "Amber" },
  { value: "#8b5cf6", label: "Ungu" },
  { value: "#ec4899", label: "Merah muda" },
];
