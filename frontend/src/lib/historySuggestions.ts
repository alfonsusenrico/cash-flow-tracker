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

const SOURCE_LABELS: Record<SuggestionSource, string> = {
  memory: "Dari ingatan Anda",
  rule: "Aturan",
  ai: "Saran AI",
};

export function suggestionSourceLabel(source: SuggestionSource): string {
  return SOURCE_LABELS[source] ?? source;
}

/** Why a run made fewer suggestions than it could; null when nothing needs saying. */
export function runNotice(run: SuggestionRun): string | null {
  const code = run.error_code;
  if (!code) return null;
  if (code === "ai_disabled") return "AI tidak aktif, jadi hanya saran dari ingatan dan aturan yang dibuat.";
  if (code === "run_interrupted") return "Proses terhenti sebelum selesai. Saran yang sudah dibuat tetap tersimpan; jalankan lagi untuk sisanya.";
  if (code === "run_failed") return "Proses gagal. Saran yang sudah dibuat tetap tersimpan; coba lagi nanti.";
  if (code.startsWith("provider_")) return `AI tidak dapat dihubungi (${code}). Saran dari ingatan dan aturan tetap tersimpan.`;
  return `Sebagian saran tidak dibuat (${code}).`;
}

export function aiUnavailableLabel(reason: string | null): string {
  if (!reason || reason === "ai_disabled") return "AI tidak aktif: hanya saran dari ingatan dan aturan.";
  return `AI tidak tersedia (${reason}): hanya saran dari ingatan dan aturan.`;
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

export function shortDate(value: string): string {
  return new Date(value).toLocaleDateString("id-ID", { day: "numeric", month: "short", year: "numeric" });
}

export function dateOnlyLabel(value: string): string {
  const [year, month, day] = value.split("-").map(Number);
  return new Date(year, month - 1, day).toLocaleDateString("id-ID", { day: "numeric", month: "short", year: "numeric" });
}

const MAX_WINDOW_DAYS = 366;

/** Indonesian message for a window the backend would reject, or null when it is valid. */
export function windowError(from: string, to: string, today = new Date()): string | null {
  if (!from || !to) return "Isi tanggal awal dan akhir.";
  if (to < from) return "Tanggal akhir tidak boleh sebelum tanggal awal.";
  if (to > defaultWindow(today).to) return "Tanggal akhir tidak boleh melewati hari ini.";
  const [fromYear, fromMonth, fromDay] = from.split("-").map(Number);
  const [toYear, toMonth, toDay] = to.split("-").map(Number);
  const days = Math.round((Date.UTC(toYear, toMonth - 1, toDay) - Date.UTC(fromYear, fromMonth - 1, fromDay)) / 86_400_000) + 1;
  if (days > MAX_WINDOW_DAYS) return "Periode paling panjang 366 hari.";
  return null;
}
