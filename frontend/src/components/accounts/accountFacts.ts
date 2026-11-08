import type { AccountItem } from "./types";

export function isHolding(account: AccountItem): boolean {
  return account.type === "investment" || Boolean(account.instrument_type);
}

export function isCredit(account: AccountItem): boolean {
  return account.type === "credit";
}

/** What a credit account owes (positive); the API sends it, and a missing value falls back to the balance. */
export function owedOn(account: AccountItem): number {
  return account.credit_owed ?? Math.max(0, -account.balance);
}

export function isPosition(account: AccountItem): boolean {
  return !account.children?.length && Boolean(
    account.instrument_type || account.instrument_symbol || account.units != null || account.avg_buy_price != null || account.last_price != null,
  );
}

export function initials(name: string): string {
  const words = name.trim().split(/\s+/);
  return (words.length > 1 ? words[0][0] + words[1][0] : name.slice(0, 2)).toUpperCase();
}
