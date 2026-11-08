/**
 * System categories: the investment, movement and balance-adjustment categories that money rules treat specially
 * (add-category-system-keys D8). The API returns their key as `system_key` on a category and
 * `category_system_key` on a transaction row; the web reads the key, never the display name, so a renamed
 * system category keeps its treatment. The backend owner is `app/services/system_categories.py`.
 */

export const SYSTEM_CATEGORY_KEYS = ["investment", "internal_movement", "balance_adjustment"] as const;

export type SystemCategoryKey = (typeof SYSTEM_CATEGORY_KEYS)[number];

/** Neither spending nor income in any report, whatever the category is named. */
const NON_REPORTABLE_KEYS: ReadonlySet<string> = new Set<SystemCategoryKey>(["internal_movement", "investment"]);

export function isMovementCategory(key: string | null | undefined): boolean {
  return key === "internal_movement";
}

export function isNonReportableCategory(key: string | null | undefined): boolean {
  return key != null && NON_REPORTABLE_KEYS.has(key);
}
