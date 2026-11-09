/** `GET /api/habits` (redesign-money-and-settings D7): learned habits and top-up patterns, read only. */
export type HabitState = "learning" | "ready" | "conflicting";

export interface HabitEntry {
  key: string;
  label: string;
  account: { id: string; name: string | null };
  category: { id: string; name: string };
  kakeibo: string | null;
  amount_anchor: number;
  confirmations: number;
  agreeing: number;
  agreement: number;
  state: HabitState;
  last_confirmed_at: string;
}

export interface TopupHabitEntry {
  key: string;
  source_account: { id: string; name: string };
  amount: number;
  product: { id: string; name: string; parent_name: string | null };
  confirmations: number;
  state: HabitState;
  last_confirmed_at: string | null;
}

export interface HabitList {
  ok: boolean;
  auto_record: { habits: boolean; topups: boolean };
  thresholds: { min_confirmations: number; min_agreement: number };
  habits: HabitEntry[];
  topups: TopupHabitEntry[];
  truncated: boolean;
}

/** A row's status as a code; `settings.habits.status.<code>` words it (spec "Kebiasaan Status and Empty States"). */
export type HabitStatus = "learning" | "conflicting" | "auto" | "readyOff";

export function habitStatus(state: HabitState, autoOn: boolean): HabitStatus {
  if (state === "learning") return "learning";
  if (state === "conflicting") return "conflicting";
  return autoOn ? "auto" : "readyOff";
}
