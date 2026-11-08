"use client";

import { useTranslations } from "next-intl";
import { useMemo } from "react";
import type { RowLabels } from "@/components/transactions/rowLabels";
import { useFormat } from "@/lib/format";
import type { Translate, Words } from "@/lib/i18n/types";
import { KAKEIBO_TERMS, type KakeiboBucket } from "@/lib/kakeibo";

/** A Kakeibo bucket's glossary name; the key itself for a value the app does not know. */
export function bucketName(t: Translate, bucket: string): string {
  const entry = KAKEIBO_TERMS[bucket as KakeiboBucket];
  return entry ? t(`terms.${entry.term}`) : bucket;
}

/** The catalogue and the region's formats for Beranda's text-building functions (add-language-system D5, D6). */
export function useWords(): Words {
  const t = useTranslations();
  const format = useFormat();
  return useMemo(() => ({ t, format }), [t, format]);
}

/** How a ledger row describes itself on Beranda, from the catalogue. */
export function rowLabelsFor(t: Translate): RowLabels {
  return {
    investmentTopUp: t("terms.investmentTopUp"),
    undescribed: t("terms.noDescription"),
    noCategory: t("terms.noCategory"),
    waiting: t("home.statusWaiting"),
    notCounted: t("home.statusNotCounted"),
    income: t("home.statusIncome"),
    transfer: t("terms.transfer"),
    unpaired: t("home.pairNotFound"),
    inFrom: (other) => t("home.inFrom", { other }),
    outTo: (other) => t("home.outTo", { other }),
    bucket: (bucket) => bucketName(t, bucket),
  };
}
