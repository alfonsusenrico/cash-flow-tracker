import { isNonReportableCategory } from "@/lib/systemCategories";

/** How many earlier owner answers a habit-recorded row followed. */
export interface HabitBasis {
  agreeing: number;
  support: number;
}

export const MERCHANT_MAX_LENGTH = 120;

export function isHabitRecorded(classifiedBy: string | null | undefined, basis: HabitBasis | null | undefined): basis is HabitBasis {
  return classifiedBy === "habit" && Boolean(basis);
}

interface OffsetCategoryCandidate {
  id: string;
  name?: string;
  kind: string;
  is_archived?: boolean;
  is_excluded_from_budget?: boolean;
  system_key?: string | null;
}

/** Expense categories a reimbursement may offset: active and counted in spending reports. */
export function offsetCategoryOptions<T extends OffsetCategoryCandidate>(categories: readonly T[]): T[] {
  return categories.filter(
    (category) =>
      category.kind === "expense"
      && !category.is_archived
      && !category.is_excluded_from_budget
      // Reports leave these out by key even where the excluded flag was never set, as the backend does.
      && !isNonReportableCategory(category.system_key),
  );
}

export function categoryOffsetsSpending(
  categories: readonly { id: string; offsets_spending?: boolean }[],
  categoryId: string | null | undefined,
): boolean {
  if (!categoryId) return false;
  return Boolean(categories.find((category) => category.id === categoryId)?.offsets_spending);
}
