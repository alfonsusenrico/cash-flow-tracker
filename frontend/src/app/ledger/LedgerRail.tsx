"use client";

import { useTranslations } from "next-intl";
import { Rail, RailItem, RailLabel, RailRule } from "@/components/workspace/rail";
import type { Format } from "@/lib/format";
import { LEDGER_TYPES, type LedgerPeriod, type LedgerTypeFilter } from "@/lib/ledgerFilters";

export type TypeCounts = Partial<Record<LedgerTypeFilter, number>>;

const PERIODS: LedgerPeriod[] = ["cycle", "month", "all"];

/** Each type's name, from the one ordered list the rail and the type control share (D5, D6, D28, D30). */
export function useTypeLabel(): (type: LedgerTypeFilter) => string {
  const t = useTranslations();
  return (type) => {
    switch (type) {
      case "all":
        return t("transactions.types.all");
      case "expense":
        return t("terms.spending");
      case "income":
        return t("terms.income");
      case "movement":
        return t("terms.transfer");
      case "investment":
        return t("terms.investments");
      case "saving":
        return t("terms.savingsAndInvestments");
      case "credit":
        return t("transactions.types.credit");
    }
  };
}

export function usePeriodLabel(): (period: LedgerPeriod) => string {
  const t = useTranslations();
  return (period) => (period === "cycle" ? t("terms.payCycle") : period === "month" ? t("terms.calendarMonth") : t("transactions.periods.all"));
}

interface ControlProps {
  types?: readonly LedgerTypeFilter[];
  type: LedgerTypeFilter;
  counts: TypeCounts | null;
  format: Format;
  onType: (type: LedgerTypeFilter) => void;
}

/** From 1200 px: JENIS with counts, a rule and PERIODE, all toggles (D5). */
export function LedgerRail({ types = LEDGER_TYPES, type, counts, format, onType, period, onPeriod }: ControlProps & { period: LedgerPeriod; onPeriod: (period: LedgerPeriod) => void }) {
  const t = useTranslations("transactions.rail");
  const typeLabel = useTypeLabel();
  const periodLabel = usePeriodLabel();
  return (
    <Rail label={t("label")} showLabel={false} className="tx-rail">
      <RailLabel>{t("types")}</RailLabel>
      {types.map((value) => {
        const count = counts?.[value];
        return (
          <RailItem
            key={value}
            label={typeLabel(value)}
            count={count === undefined ? undefined : format.integer(count)}
            countName={count === undefined ? undefined : t("count", { count })}
            pressed={type === value}
            onSelect={() => onType(value)}
          />
        );
      })}
      <RailRule />
      <RailLabel>{t("periods")}</RailLabel>
      {PERIODS.map((value) => (
        <RailItem key={value} label={periodLabel(value)} pressed={period === value} onSelect={() => onPeriod(value)} />
      ))}
    </Rail>
  );
}

/** Below 1200 px: the same types as one scrolling segmented control above the toolbar (D6). */
export function LedgerTypeControl({ types = LEDGER_TYPES, type, counts, format, onType }: ControlProps) {
  const t = useTranslations("transactions.rail");
  const typeLabel = useTypeLabel();
  return (
    <div className="ws-seg tx-types" role="group" aria-label={t("types")}>
      {types.map((value) => {
        const count = counts?.[value];
        return (
          <button key={value} type="button" aria-pressed={type === value} onClick={() => onType(value)}>
            {typeLabel(value)}
            {count !== undefined && (
              <>
                <span className="muted" aria-hidden="true">{format.integer(count)}</span>
                <span className="sr-only">{t("count", { count })}</span>
              </>
            )}
          </button>
        );
      })}
    </div>
  );
}

/** Under "Kredit": the credit accounts to choose one provider's view from (H5, D30). */
export function ProviderControl({
  providers,
  account,
  onAccount,
}: {
  providers: { id: string; name: string }[];
  account: string;
  onAccount: (account: string) => void;
}) {
  const t = useTranslations("transactions.provider");
  if (!providers.length) return null;
  return (
    <div className="ws-seg tx-providers" role="group" aria-label={t("label")}>
      <button type="button" aria-pressed={!providers.some((provider) => provider.id === account)} onClick={() => onAccount("all")}>
        {t("all")}
      </button>
      {providers.map((provider) => (
        <button key={provider.id} type="button" aria-pressed={provider.id === account} onClick={() => onAccount(provider.id)}>
          {provider.name}
        </button>
      ))}
    </div>
  );
}
