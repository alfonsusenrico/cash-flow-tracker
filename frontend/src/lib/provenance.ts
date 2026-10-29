export type TransactionSource =
  | "manual"
  | "notification"
  | "recurring"
  | "payroll"
  | "movement"
  | "reconcile"
  | "investment"
  | "import"
  | "capture";

export type ReviewStatus = "auto" | "needs_review" | "reviewed";

export type Classifier = "rule" | "ai" | "owner" | "system";

const SOURCE_LABELS: Record<TransactionSource, string> = {
  manual: "Dicatat manual",
  notification: "Notifikasi bank",
  recurring: "Transaksi rutin",
  payroll: "Alokasi gaji",
  movement: "Pindah saldo",
  reconcile: "Penyesuaian saldo",
  investment: "Investasi",
  import: "Impor",
  capture: "Diketik di ponsel",
};

const REVIEW_STATUS_LABELS: Record<ReviewStatus, string> = {
  auto: "Otomatis, belum ditinjau",
  needs_review: "Perlu ditinjau",
  reviewed: "Sudah ditinjau",
};

const CLASSIFIER_LABELS: Record<Classifier, string> = {
  rule: "Aturan",
  ai: "AI",
  owner: "Anda",
  system: "Sistem",
};

export const MERCHANT_MAX_LENGTH = 120;

/** Labels for a value the backend sent; an unknown value is shown as-is rather than guessed. */
export function sourceLabel(value: string | null | undefined): string | null {
  if (!value) return null;
  return SOURCE_LABELS[value as TransactionSource] ?? value;
}

export function reviewStatusLabel(value: string | null | undefined): string | null {
  if (!value) return null;
  return REVIEW_STATUS_LABELS[value as ReviewStatus] ?? value;
}

export function classifierLabel(value: string | null | undefined): string | null {
  if (!value) return null;
  return CLASSIFIER_LABELS[value as Classifier] ?? value;
}

export function needsOwnerReview(value: string | null | undefined): boolean {
  return value === "auto" || value === "needs_review";
}

interface OffsetCategoryCandidate {
  id: string;
  name?: string;
  kind: string;
  is_archived?: boolean;
  is_excluded_from_budget?: boolean;
}

// Reports leave these out by name even where the excluded flag was never set, as the backend does.
const NON_REPORTABLE_CATEGORY_NAMES = new Set(["Internal Movement", "Investasi"]);

/** Expense categories a reimbursement may offset: active and counted in spending reports. */
export function offsetCategoryOptions<T extends OffsetCategoryCandidate>(categories: readonly T[]): T[] {
  return categories.filter(
    (category) =>
      category.kind === "expense"
      && !category.is_archived
      && !category.is_excluded_from_budget
      && !NON_REPORTABLE_CATEGORY_NAMES.has(category.name ?? ""),
  );
}

export function categoryOffsetsSpending(
  categories: readonly { id: string; offsets_spending?: boolean }[],
  categoryId: string | null | undefined,
): boolean {
  if (!categoryId) return false;
  return Boolean(categories.find((category) => category.id === categoryId)?.offsets_spending);
}
