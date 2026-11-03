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

/** The status words of a row (spec "Kebiasaan Status and Empty States"). */
export function habitStatus(state: HabitState, confirmations: number, needed: number, autoOn: boolean): string {
  if (state === "learning") return `Masih belajar · ${confirmations} dari ${needed}`;
  if (state === "conflicting") return "Jawaban berbeda, tetap ditanya";
  return autoOn ? "Dicatat otomatis" : "Siap, pencatatan otomatis nonaktif";
}
