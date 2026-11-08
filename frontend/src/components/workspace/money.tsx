"use client";

import { useTranslations } from "next-intl";
import { useAppCtx } from "@/components/layout/AppLayout";
import { useFormat } from "@/lib/format";
import { cn } from "@/lib/utils";
import { useInterfaceLang } from "./untranslated";

/**
 * An amount that "Sembunyikan saldo" can hide (apply-markas-beranda D11). The caller formats the amount with its
 * sign; while balances are hidden the text is replaced by the currency's mask, which is an image named
 * "Disembunyikan", so neither the figure nor its sign reaches the screen or an accessible name.
 */
export function MaskedMoney({ text, className }: { text: string; className?: string }) {
  const { hideBalances, bal } = useAppCtx();
  const t = useTranslations("common");
  const lang = useInterfaceLang();
  if (hideBalances) {
    // Its accessible name is `common.hidden` (apply-markas-shell owner decision 12, add-language-system D6).
    return (
      <span className={cn("mask", className)} role="img" aria-label={t("hidden")} lang={lang}>
        {bal(0)}
      </span>
    );
  }
  // Without a class the amount stays part of its sentence's text, as the prototype writes "aset Rp … · utang Rp …".
  return className ? <span className={className}>{text}</span> : <>{text}</>;
}

/**
 * Money for the translated pages (add-language-system D6): the region's digit grouping, the display currency, and
 * the currency's mask while balances are hidden. Untranslated pages keep `bal` until their phase moves them.
 */
export function useMoney(): (amount: number) => string {
  const { hideBalances, bal } = useAppCtx();
  const format = useFormat();
  // While hidden, `bal` returns the display currency's mask.
  return (amount: number) => (hideBalances ? bal(amount) : format.money(amount));
}
