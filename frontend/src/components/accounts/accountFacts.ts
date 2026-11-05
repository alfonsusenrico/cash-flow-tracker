import { fmtMoney } from "@/lib/utils";
import type { AccountItem } from "./types";

export const KIND: Record<AccountItem["type"], string> = { bank: "Bank", wallet: "E-wallet", cash: "Tunai", investment: "Investasi", credit: "Kredit" };

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

/** The headline of a credit account: what it owes, or the overpayment, never its negative balance (add-credit-accounts D3). */
export function creditHeadline(account: AccountItem, money: (value: number) => string): string {
  if (account.balance > 0) return `Lebih bayar ${money(account.balance)}`;
  return `Utang ${money(owedOn(account))}`;
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

export function trackingText(account: AccountItem, amountProducts: Set<string>): string {
  if (amountProducts.has(account.id) || account.investment_tracking_mode === "amount") return "Dicatat per nominal";
  if (account.instrument_type === "stock" && account.units) {
    const lots = account.units / 100;
    return `${lots.toLocaleString("id-ID")} lot × 100 lembar${account.last_price ? ` × ${fmtMoney(account.last_price)}` : ""}`;
  }
  if (account.units) return `${account.units.toLocaleString("id-ID")} unit`;
  return "Dicatat per nominal";
}

export function trackingPill(account: AccountItem, amountProducts: Set<string>): string {
  if (account.instrument_type === "stock") return "Saham";
  if (account.instrument_type === "mutual_fund") return amountProducts.has(account.id) || account.investment_tracking_mode === "amount" ? "Reksa dana · nominal" : "Reksa dana · unit";
  if (account.instrument_type === "gold") return "Emas";
  if (account.instrument_type === "crypto") return "Kripto";
  if (account.instrument_type === "deposit") return "Deposito";
  return "Investasi";
}

/** The short holding line of a product card: lots for shares, otherwise how the product is tracked. */
export function holdingLine(account: AccountItem, amountProducts: Set<string>): string {
  if (account.instrument_type === "stock" && account.units) return `${(account.units / 100).toLocaleString("id-ID")} lot`;
  if (amountProducts.has(account.id) || account.investment_tracking_mode === "amount") return "dicatat per nominal";
  if (account.units) return `${account.units.toLocaleString("id-ID")} unit`;
  return "";
}
