/** Tag colour a new account form starts with; the owner can pick another before saving. */
export const DEFAULT_ACCOUNT_TAG_COLOR = "#2563EB";
/** Lavender investment fill (`--color-investment`, light theme); the API gives it to investment accounts created without a colour. */
export const INVESTMENT_ACCOUNT_TAG_COLOR = "#8B5CF6";
/** Neutral slate (the picker's "Slate", `--k-none` in the light theme): a credit account holds debt, not money to spend. */
export const CREDIT_ACCOUNT_TAG_COLOR = "#475569";

export function defaultAccountTagColor(type: string): string {
  if (type === "investment") return INVESTMENT_ACCOUNT_TAG_COLOR;
  if (type === "credit") return CREDIT_ACCOUNT_TAG_COLOR;
  return DEFAULT_ACCOUNT_TAG_COLOR;
}
