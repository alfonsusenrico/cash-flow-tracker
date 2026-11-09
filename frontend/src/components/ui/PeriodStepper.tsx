"use client";

import { useLocale, useTranslations } from "next-intl";
import { Icon } from "@/components/ui/Icon";
import { useInterfaceLang } from "@/components/workspace/untranslated";
import { useFormat } from "@/lib/format";

/** The period's own dates, so the stepper can name it in a language the server label is not written in. */
export interface StepperPeriod {
  start: string;
  end?: string | null;
}

interface PeriodStepperProps {
  /** The server's Indonesian period label; null while it loads. */
  label: string | null;
  offset: number;
  onChange: (offset: number) => void;
  unit: "Siklus" | "Bulan";
  minOffset?: number;
  period?: StepperPeriod | null;
  /** "long" names a month in full ("October 2026"), as Laporan's server labels do (apply-markas-laporan D2). */
  monthStyle?: "short" | "long";
}

/**
 * Previous/next stepper for a report period, in the pattern of the top bar month stepper. It follows the interface
 * language on every page (add-language-system D17): Indonesian keeps the server's label, English names the period
 * from its dates ("Oct 2026", "Cycle 25 Sep – 24 Oct").
 */
export function PeriodStepper({ label, offset, onChange, unit, minOffset = -36, period, monthStyle = "short" }: PeriodStepperProps) {
  const t = useTranslations("common");
  const locale = useLocale();
  const format = useFormat();
  const lang = useInterfaceLang();
  const cycle = unit === "Siklus";
  const shown = (() => {
    if (label === null) return t("loading");
    if (locale === "id" || !period?.start) return label;
    if (!cycle) return monthStyle === "long" ? format.monthYear(period.start) : format.shortMonthYear(period.start);
    return period.end
      ? t("period.cycleRange", { start: format.shortDate(period.start), end: format.shortDate(period.end) })
      : t("period.cycleSince", { start: format.shortDate(period.start) });
  })();
  const previous = cycle ? t("period.previousCycle") : t("period.previousMonth");
  const next = cycle ? t("period.nextCycle") : t("period.nextMonth");
  // SPEC `.stepper` (apply-markas-transaksi D3): two 36 px icon buttons around a 168 px live label, no box; the
  // reset after a step back stays, because a far step back would otherwise need many presses (deviation 4).
  return (
    <div className="stepper" role="group" aria-label={t("period.label")} lang={lang}>
      <button type="button" className="ws-icon-btn" onClick={() => onChange(offset - 1)} disabled={offset <= minOffset} aria-label={previous} title={previous}>
        <Icon name="chevron-left" className="h-4 w-4" />
      </button>
      <span className="cur" aria-live="polite">
        {shown}
      </span>
      <button type="button" className="ws-icon-btn" onClick={() => onChange(offset + 1)} disabled={offset >= 0} aria-label={next} title={next}>
        <Icon name="chevron-right" className="h-4 w-4" />
      </button>
      {offset !== 0 && (
        <button type="button" className="ws-btn ws-btn-quiet ws-btn-sm" onClick={() => onChange(0)}>
          {cycle ? t("period.thisCycle") : t("period.thisMonth")}
        </button>
      )}
    </div>
  );
}
