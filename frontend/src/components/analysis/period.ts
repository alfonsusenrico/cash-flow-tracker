"use client";

import { useQuery } from "@tanstack/react-query";
import { useLocale, useTranslations } from "next-intl";
import { api } from "@/lib/api";
import { useFormat } from "@/lib/format";
import { todayLocal } from "@/lib/ledgerDays";
import { cycleLabelEnd, type ReportPeriod, type ReportPeriodList } from "@/lib/monthlyReport";
import { queryKeys } from "@/lib/queryKeys";
import type { PeriodBasis } from "./types";

/**
 * The report periods of the basis, newest first, and the one the stepper offset points at: offset 0 is the running
 * period, −n the n-th earlier one (apply-markas-analisis D2). Ringkasan's comparison reads the same query.
 */
export function useAnalysisPeriods(basis: PeriodBasis, offset: number) {
  const periods = useQuery<ReportPeriodList>({
    queryKey: queryKeys.reports.periods(basis),
    queryFn: () => api.get(`/reports/monthly/periods?basis=${basis}`),
  });
  const ordered = [...(periods.data?.periods ?? [])].sort((left, right) => right.start_date.localeCompare(left.start_date));
  const viewed: ReportPeriod | null = ordered[-offset] ?? null;
  return { periods, viewed };
}

/**
 * The period's name as the stepper writes it: Indonesian keeps the server's label, English names the period from its
 * dates, so the hero and the stepper always say the same thing ("25 Sep – 22 Oct", "Oct 2026"). A running cycle
 * ends on its estimated last day, or today without one (improve-ledger-readability D5). Without a listed period the
 * server's window label stands in.
 */
export function usePeriodName(basis: PeriodBasis, period: ReportPeriod | null, fallback: string | null): string | null {
  const t = useTranslations("common.period");
  const locale = useLocale();
  const format = useFormat();
  if (!period) return fallback;
  if (locale === "id") return period.label;
  if (basis === "month") return format.shortMonthYear(period.start_date);
  return t("cycleRange", { start: format.shortDate(period.start_date), end: format.shortDate(cycleLabelEnd(period) ?? todayLocal()) });
}
