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
  kind: "merchant" | "signature" | "notes" | "category";
  type: "expense" | "income";
  label: string;
  /** Whether `label` is the user's text or the fixed fallback named by `label_key` (add-language-system D7). */
  label_source?: "user" | "system";
  label_key?: "undescribed" | "uncategorized" | null;
  count: number;
  total: number;
  needs_review_count: number;
  auto_count: number;
  first_date: string | null;
  last_date: string | null;
  reasons: string[];
  category: { id: string; name: string | null } | null;
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

export interface TopupProductChoice {
  id: string;
  name: string;
  /** "Induk · Produk" */
  label: string;
}

/** A learned top-up for an awaiting debit; without a target the owner chooses among `choices`. */
export interface TopupSuggestion {
  target_account_id: string | null;
  target_label: string | null;
  choices: TopupProductChoice[];
  prompt: string;
  explanation: string;
  basis: { agreeing: number; support: number; agreement: number } | null;
}

export interface AwaitingItem extends ReviewItem {
  topup_suggestion: TopupSuggestion | null;
}

export interface ReviewQueue {
  ok: boolean;
  truncated: boolean;
  counts: {
    /** Rows that need the owner, as the navigation badge counts them. */
    attention: number;
    /** Automatic rows nobody looked at yet; checking them is optional. */
    unreviewed: number;
    questions: number;
    needs_review: number;
    auto: number;
    groups: number;
    awaiting: number;
    ambiguous: number;
    seen_twice?: number;
    /** Reports the owner's account links skipped; never counted as work. */
    skipped?: number;
  };
  questions: PurposeQuestionEntry[];
  groups: ReviewGroup[];
  awaiting: AwaitingItem[];
  ambiguous: AmbiguousItem[];
  /** Second reports of one money movement still waiting (recognise-one-arrival-seen-twice D3). */
  seen_twice?: SeenTwiceItem[];
  /** Reports skipped by an owner's account link in the last 30 days (reconcile-linked-account-reports D6). */
  skipped?: SeenTwiceItem[];
}

/** The leg a second report repeats; `movement` is its route when the leg is part of a movement. */
export interface SeenTwiceKeptLeg {
  transaction_id: string;
  type: "expense" | "income";
  amount: number;
  date: string | null;
  account_name: string | null;
  movement: { source: string | null; target: string | null } | null;
}

/** The owner's account link that skipped a report: the account, also reported by another app. */
export interface SkippedByLink {
  id: string;
  account_label: string | null;
  institution_label: string;
}

export interface SeenTwiceItem {
  event_id: string;
  /** "duplicate": nothing was booked for it and it waits; "skipped": an owner's account link skipped it. */
  kind: "duplicate" | "skipped";
  app: string;
  type: "expense" | "income" | null;
  amount: number | null;
  date: string;
  account_name: string | null;
  kept: SeenTwiceKeptLeg | null;
  link?: SkippedByLink;
}

/** The account link "Benar, satu transaksi" learned; `created` is false when it already existed. */
export interface LearnedAccountLink {
  id: string;
  account_id: string;
  account_label: string | null;
  institution_label: string;
  created: boolean;
}

export interface ReviewSummary {
  ok: boolean;
  attention: number;
  unreviewed: number;
}

export function chunkIds(ids: readonly string[], size = REVIEW_BULK_LIMIT): string[][] {
  const chunks: string[][] = [];
  for (let index = 0; index < ids.length; index += size) chunks.push(ids.slice(index, index + size));
  return chunks;
}

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

/**
 * The text of a failed review request: `lib/api` has already rendered a known error code in the interface
 * language (localize-server-text D8), so this keeps that message; null when the failure carries none.
 */
export function reviewErrorMessage(error: unknown): string | null {
  if (error && typeof error === "object" && "detail" in error) {
    const detail = (error as { detail: unknown }).detail;
    if (typeof detail === "string" && detail) return detail;
    if (detail && typeof detail === "object") {
      const { message } = detail as { message?: string };
      if (message) return message;
    }
  }
  return error instanceof Error && error.message ? error.message : null;
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
