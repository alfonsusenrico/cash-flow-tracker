import type { KakeiboPillar } from "@/lib/kakeibo";

/** Suggestions per accept or reject request; the backend rejects larger batches. */
export const SUGGESTION_DECISION_LIMIT = 200;

export type SuggestionSource = "memory" | "rule" | "ai";

export interface SuggestionValues {
  merchant: string | null;
  notes: string | null;
  category_id: string | null;
  category_name: string | null;
  kakeibo: KakeiboPillar | null;
}

export interface HistorySuggestion {
  id: string;
  transaction_id: string;
  type: "expense" | "income";
  amount: number;
  date: string;
  account_name: string;
  source: SuggestionSource;
  confidence: number | null;
  reason: string;
  before: SuggestionValues;
  after: SuggestionValues & { offset_category_id: string | null; offset_category_name: string | null };
}

export interface SuggestionGroup {
  key: string;
  type: "expense" | "income";
  label: string;
  merchant: string | null;
  category: { id: string; name: string };
  kakeibo: KakeiboPillar | null;
  offset_category_names: string[];
  sources: SuggestionSource[];
  before: { label: string; category_name: string; count: number }[];
  count: number;
  total: number;
  first_date: string | null;
  last_date: string | null;
  suggestions: HistorySuggestion[];
}

export interface SuggestionList {
  ok: boolean;
  truncated: boolean;
  pending: number;
  groups: SuggestionGroup[];
}

export interface SuggestionEstimate {
  from_date: string;
  to_date: string;
  eligible_count: number;
  selected_count: number;
  truncated: boolean;
  max_rows: number;
  memory_count: number;
  rule_count: number;
  ai_rows: number;
  ai_calls: number;
  batch_size: number;
  ai_available: boolean;
  ai_unavailable_reason: string | null;
}

export interface SuggestionRun {
  id: string;
  status: "running" | "completed" | "failed";
  from_date: string;
  to_date: string;
  eligible_count: number;
  selected_count: number;
  processed_count: number;
  memory_count: number;
  rule_count: number;
  ai_count: number;
  unchanged_count: number;
  rejected_before_count: number;
  ai_calls: number;
  error_code: string | null;
  model: string | null;
  prompt_version: string | null;
  created_at: string;
  finished_at: string | null;
}

export interface SuggestionDecisionResult {
  ok: boolean;
  accepted?: number;
  rejected?: number;
  skipped: string[];
  corrections?: number;
}

/** Why a run made fewer suggestions than it could, as a `review.dialog.history.notice` key; null when nothing needs saying. */
export type RunNotice = { key: "aiDisabled" | "interrupted" | "failed" | "provider" | "other"; code: string };

export function runNotice(run: SuggestionRun): RunNotice | null {
  const code = run.error_code;
  if (!code) return null;
  if (code === "ai_disabled") return { key: "aiDisabled", code };
  if (code === "run_interrupted") return { key: "interrupted", code };
  if (code === "run_failed") return { key: "failed", code };
  if (code.startsWith("provider_")) return { key: "provider", code };
  return { key: "other", code };
}

export function chunkSuggestionIds(ids: readonly string[], size = SUGGESTION_DECISION_LIMIT): string[][] {
  const chunks: string[][] = [];
  for (let index = 0; index < ids.length; index += size) chunks.push(ids.slice(index, index + size));
  return chunks;
}

function isoDate(value: Date): string {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, "0");
  const day = String(value.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

/** Default window: the last three months up to today, in local dates. */
export function defaultWindow(today = new Date()): { from: string; to: string } {
  const start = new Date(today.getFullYear(), today.getMonth() - 3, today.getDate());
  return { from: isoDate(start), to: isoDate(today) };
}

const MAX_WINDOW_DAYS = 366;

/** Why the backend would reject a window, as a `review.dialog.history.window` key, or null when it is valid. */
export type WindowProblem = "empty" | "order" | "future" | "tooLong";

export function windowError(from: string, to: string, today = new Date()): WindowProblem | null {
  if (!from || !to) return "empty";
  if (to < from) return "order";
  if (to > defaultWindow(today).to) return "future";
  const [fromYear, fromMonth, fromDay] = from.split("-").map(Number);
  const [toYear, toMonth, toDay] = to.split("-").map(Number);
  const days = Math.round((Date.UTC(toYear, toMonth - 1, toDay) - Date.UTC(fromYear, fromMonth - 1, fromDay)) / 86_400_000) + 1;
  if (days > MAX_WINDOW_DAYS) return "tooLong";
  return null;
}
