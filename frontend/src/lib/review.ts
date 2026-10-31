import type { KakeiboPillar } from "@/lib/kakeibo";
import type { HabitBasis } from "@/lib/provenance";

export type ReviewAction = "accept" | "recategorise" | "rename" | "keep_separate";

/** Rows per bulk request; the backend rejects larger batches. */
export const REVIEW_BULK_LIMIT = 200;

export interface ReviewItem {
  id: string;
  type: "expense" | "income";
  amount: number;
  date: string | null;
  account_id: string;
  account_name: string;
  category_id: string | null;
  category_name: string | null;
  kakeibo_type: KakeiboPillar | null;
  merchant: string | null;
  notes: string | null;
  review_status: "auto" | "needs_review" | "reviewed";
  source: string;
  classified_by: string | null;
  movement_id: string | null;
  movement_role: "outbound" | "inbound" | null;
  fee_for_movement_id: string | null;
  reasons: string[];
}

export interface ReviewGroup {
  key: string;
  kind: "merchant" | "signature" | "notes" | "category" | "movement";
  type: "expense" | "income" | "movement";
  label: string;
  count: number;
  total: number;
  needs_review_count: number;
  auto_count: number;
  first_date: string | null;
  last_date: string | null;
  reasons: string[];
  category: { id: string; name: string | null } | null;
  editable: boolean;
  transactions: ReviewItem[];
}

export interface PurposeSuggestion {
  id: string;
  kind: "habit" | "history" | "current" | "repayment";
  label: string;
  category_name: string | null;
  kakeibo: KakeiboPillar | null;
  /** Present on a `habit` guess: how many earlier owner answers agree with it. */
  basis?: HabitBasis | null;
}

export interface PurposeQuestionEntry {
  event_id: string;
  post_time: string;
  question: {
    question_id: string;
    prompt: string;
    allows_text?: boolean;
    text_max_length?: number;
    suggestions: PurposeSuggestion[];
  };
  transaction: {
    id: string;
    type: "expense" | "income";
    amount: number;
    date: string | null;
    account_name: string;
    notes: string | null;
    merchant: string | null;
    category_name: string | null;
  };
}

export interface AmbiguousCandidate {
  id: string;
  type: "expense" | "income";
  amount: number;
  date: string;
  account_id: string;
  account_name: string | null;
}

export interface AmbiguousItem extends ReviewItem {
  candidates: AmbiguousCandidate[];
}

export interface ReviewQueue {
  ok: boolean;
  truncated: boolean;
  counts: {
    questions: number;
    needs_review: number;
    auto: number;
    groups: number;
    awaiting: number;
    ambiguous: number;
  };
  questions: PurposeQuestionEntry[];
  groups: ReviewGroup[];
  awaiting: ReviewItem[];
  ambiguous: AmbiguousItem[];
}

export interface ReviewSummary {
  ok: boolean;
  attention: number;
  unreviewed: number;
}

const REASON_LABELS: Record<string, string> = {
  uncertain_category: "Kategori belum pasti",
  model_fallback: "AI ragu, dicatat dengan aturan cadangan",
  low_confidence: "Keyakinan AI rendah",
  model_flagged: "Ditandai AI untuk dicek",
  ambiguous_movement: "Pasangan pindah saldo belum pasti",
  purpose_question: "Menunggu jawaban \"Apa ini?\"",
  awaiting_counterpart: "Menunggu rekening pasangan",
};

/** Indonesian label of a review reason; an unknown code is shown as sent rather than guessed. */
export function reviewReasonLabel(reason: string): string {
  return REASON_LABELS[reason] ?? reason;
}

const KIND_LABELS: Record<ReviewGroup["kind"], string> = {
  merchant: "Toko",
  signature: "Tanpa nama toko",
  notes: "Catatan",
  category: "Kategori",
  movement: "Pindah saldo",
};

export function groupKindLabel(kind: ReviewGroup["kind"]): string {
  return KIND_LABELS[kind];
}

export function chunkIds(ids: readonly string[], size = REVIEW_BULK_LIMIT): string[][] {
  const chunks: string[][] = [];
  for (let index = 0; index < ids.length; index += size) chunks.push(ids.slice(index, index + size));
  return chunks;
}

/** "3 Okt" or "3 – 17 Okt 2026" style range of a group's rows. */
export function dateRangeLabel(first: string | null, last: string | null): string | null {
  if (!first || !last) return null;
  const start = new Date(first);
  const end = new Date(last);
  const short = { day: "numeric", month: "short" } as const;
  const full = { day: "numeric", month: "short", year: "numeric" } as const;
  if (start.toDateString() === end.toDateString()) return end.toLocaleDateString("id-ID", full);
  return `${start.toLocaleDateString("id-ID", short)} – ${end.toLocaleDateString("id-ID", full)}`;
}

const ERROR_MESSAGES: Record<string, string> = {
  stale_question: "Pertanyaan ini sudah dijawab atau berubah. Daftar dimuat ulang.",
  purpose_not_applicable: "Transaksi ini sudah dihapus atau menjadi pindah saldo.",
  purpose_answer_conflict: "Jawaban lain sudah tersimpan untuk pertanyaan ini.",
  unknown_suggestion: "Pilihan ini sudah tidak tersedia. Muat ulang daftar.",
  invalid_suggestion: "Kategori pilihan ini sudah tidak aktif. Jawab dengan teks atau ubah di buku kas.",
  invalid_purpose_answer: "Jawaban harus 1–200 karakter tanpa karakter khusus.",
};

/** An existing row on the chosen account that may already be the missing leg of a transfer. */
export interface CompletionCandidate {
  id: string;
  type: "expense" | "income";
  amount: number;
  date: string;
  account_id: string;
  category_name: string | null;
  notes: string | null;
}

/** Candidates the server returns instead of completing, when the chosen account already has a match. */
export function completionCandidates(error: unknown): CompletionCandidate[] | null {
  if (!error || typeof error !== "object" || !("detail" in error)) return null;
  const detail = (error as { detail: unknown }).detail;
  if (!detail || typeof detail !== "object") return null;
  const { code, candidates } = detail as { code?: string; candidates?: unknown };
  return code === "counterpart_candidates" && Array.isArray(candidates) ? (candidates as CompletionCandidate[]) : null;
}

/** Message for a failed review request; backend messages are already Indonesian. */
export function reviewErrorMessage(error: unknown): string {
  if (error && typeof error === "object" && "detail" in error) {
    const detail = (error as { detail: unknown }).detail;
    if (detail && typeof detail === "object") {
      const { code, message } = detail as { code?: string; message?: string };
      if (code && ERROR_MESSAGES[code]) return ERROR_MESSAGES[code];
      if (message) return message;
    }
  }
  return error instanceof Error && error.message ? error.message : "Permintaan gagal. Coba lagi.";
}

/** A v4 UUID for a reply id; `crypto.randomUUID` is missing outside secure contexts. */
export function newReplyId(): string {
  if (typeof crypto.randomUUID === "function") return crypto.randomUUID();
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}
