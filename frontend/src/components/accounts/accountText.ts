"use client";

import { useTranslations } from "next-intl";
import { useFormat } from "@/lib/format";
import { isCredit } from "./accountFacts";
import { roleOf, USES, DEBT_USE, type UseInfo } from "./wallet";
import type { AccountItem, AccountRole, InvestmentKind } from "./types";

/**
 * The words Rekening & Kantong writes about an account, in the interface language (apply-markas-rekening D16):
 * use and kind names, the type line of a top-level account and the holding line of a product. Account names are
 * user data and are never translated.
 */
export function useAccountText() {
  const t = useTranslations("accounts");
  const format = useFormat();

  const roleInfo = (role: AccountRole): UseInfo => USES.find((use) => use.key === role) ?? DEBT_USE;
  const roleLabel = (role: AccountRole) => t(`groups.${roleInfo(role).labelKey}`);
  const kindLabel = (kind: InvestmentKind) => t(`kinds.${kind}`);

  /** "Bank", "E-wallet", "Uang tunai", "Rekening dana investasi", "Akun investasi" or "Kredit". */
  const typeLabel = (account: AccountItem): string => {
    if (isCredit(account)) return t("tile.type.credit");
    if (account.type === "investment") return t("tile.type.investment");
    if (roleOf(account) === "dana_investasi") return t("tile.type.brokerCash");
    if (account.type === "cash") return t("tile.type.cash");
    if (account.type === "wallet") return t("tile.type.wallet");
    return t("tile.type.bank");
  };

  /** Lots for shares, "dicatat per nominal" for an amount-tracked product, units otherwise; empty when unknown. */
  const holdingLine = (account: AccountItem, amountProducts: Set<string>): string => {
    if (account.instrument_type === "stock" && account.units) return t("tile.lots", { lots: format.decimal(account.units / 100, 3) });
    if (amountProducts.has(account.id) || account.investment_tracking_mode === "amount") return t("tile.byAmount");
    if (account.units) return t("tile.units", { units: format.decimal(account.units, 3) });
    return "";
  };

  /** The detail's "Cara hitung": how the product's value is worked out. */
  const methodLine = (account: AccountItem, amountProducts: Set<string>): string => {
    let method: string;
    if (amountProducts.has(account.id) || account.investment_tracking_mode === "amount") method = t("detail.methodByAmount");
    else if (account.instrument_type === "stock" && account.units) {
      const lots = format.decimal(account.units / 100, 3);
      method = account.last_price ? t("detail.methodLotsPrice", { lots, price: format.money(account.last_price) }) : t("detail.methodLots", { lots });
    } else if (account.units) method = t("detail.methodUnits", { units: format.decimal(account.units, 3) });
    else method = t("detail.methodByAmount");
    return account.investment_value_estimated ? t("detail.estimated", { method }) : method;
  };

  return { t, format, roleInfo, roleLabel, kindLabel, typeLabel, holdingLine, methodLine };
}
