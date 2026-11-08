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
}

/**
 * Previous/next stepper for a report period, in the pattern of the top bar month stepper. It follows the interface
 * language on every page (add-language-system D17): Indonesian keeps the server's label, English names the period
 * from its dates ("Oct 2026", "Cycle 25 Sep – 24 Oct").
 */
export function PeriodStepper({ label, offset, onChange, unit, minOffset = -36, period }: PeriodStepperProps) {
  const t = useTranslations("common");
  const locale = useLocale();
  const format = useFormat();
  const lang = useInterfaceLang();
  const cycle = unit === "Siklus";
  const shown = (() => {
    if (label === null) return t("loading");
    if (locale === "id" || !period?.start) return label;
    if (!cycle) return format.shortMonthYear(period.start);
    return period.end
      ? t("period.cycleRange", { start: format.shortDate(period.start), end: format.shortDate(period.end) })
      : t("period.cycleSince", { start: format.shortDate(period.start) });
  })();
  const previous = cycle ? t("period.previousCycle") : t("period.previousMonth");
  const next = cycle ? t("period.nextCycle") : t("period.nextMonth");
  return (
    <div className="inline-flex min-h-10 items-center gap-1 rounded-xl border border-[var(--line)] bg-[var(--surface-2)]/60 px-1.5 text-xs" lang={lang}>
      <button
        type="button"
        onClick={() => onChange(offset - 1)}
        disabled={offset <= minOffset}
        aria-label={previous}
        title={previous}
        className="inline-flex min-h-10 min-w-10 items-center justify-center rounded-md text-[var(--muted)] transition-colors hover:bg-[var(--surface)] hover:text-[var(--text)] disabled:opacity-30"
      >
        <Icon name="chevron-left" className="h-3.5 w-3.5" />
      </button>
      <span aria-live="polite" className="min-w-[7.5rem] select-none px-1 text-center font-semibold tabular text-[var(--text)]">
        {shown}
      </span>
      <button
        type="button"
        onClick={() => onChange(offset + 1)}
        disabled={offset >= 0}
        aria-label={next}
        title={next}
        className="inline-flex min-h-10 min-w-10 items-center justify-center rounded-md text-[var(--muted)] transition-colors hover:bg-[var(--surface)] hover:text-[var(--text)] disabled:opacity-30"
      >
        <Icon name="chevron-right" className="h-3.5 w-3.5" />
      </button>
      {offset !== 0 && (
        <button
          type="button"
          onClick={() => onChange(0)}
          className="min-h-10 px-2 text-xs font-semibold text-[var(--text)] underline-offset-2 hover:underline"
        >
          {cycle ? t("period.thisCycle") : t("period.thisMonth")}
        </button>
      )}
    </div>
  );
}
