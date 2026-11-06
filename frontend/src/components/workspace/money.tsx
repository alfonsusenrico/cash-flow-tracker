"use client";

import { useAppCtx } from "@/components/layout/AppLayout";
import { cn } from "@/lib/utils";

/** The accessible name of a hidden amount (apply-markas-shell owner decision 12). */
export const HIDDEN_AMOUNT_NAME = "Disembunyikan";

/**
 * An amount that "Sembunyikan saldo" can hide (apply-markas-beranda D11). The caller formats the amount with its
 * sign; while balances are hidden the text is replaced by the currency's mask, which is an image named
 * "Disembunyikan", so neither the figure nor its sign reaches the screen or an accessible name.
 */
export function MaskedMoney({ text, className }: { text: string; className?: string }) {
  const { hideBalances, bal } = useAppCtx();
  if (hideBalances) {
    return (
      <span className={cn("mask", className)} role="img" aria-label={HIDDEN_AMOUNT_NAME}>
        {bal(0)}
      </span>
    );
  }
  return <span className={className}>{text}</span>;
}
