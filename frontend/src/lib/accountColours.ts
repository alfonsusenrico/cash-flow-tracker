/** Tag colour a new account form starts with; the owner can pick another before saving. */
export const DEFAULT_ACCOUNT_TAG_COLOR = "#2563EB";
/** Lavender investment fill (`--color-investment`, light theme); the API gives it to investment accounts created without a colour. */
export const INVESTMENT_ACCOUNT_TAG_COLOR = "#8B5CF6";

export function defaultAccountTagColor(type: string): string {
  return type === "investment" ? INVESTMENT_ACCOUNT_TAG_COLOR : DEFAULT_ACCOUNT_TAG_COLOR;
}
