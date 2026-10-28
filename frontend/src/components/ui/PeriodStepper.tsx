"use client";

import { Icon } from "@/components/ui/Icon";

interface PeriodStepperProps {
  label: string;
  offset: number;
  onChange: (offset: number) => void;
  unit: "Siklus" | "Bulan";
  minOffset?: number;
}

/** Previous/next stepper for a report period, in the pattern of the top bar month stepper. */
export function PeriodStepper({ label, offset, onChange, unit, minOffset = -36 }: PeriodStepperProps) {
  return (
    <div className="inline-flex min-h-10 items-center gap-1 rounded-xl border border-[var(--border)] bg-[var(--surface-raised)]/60 px-1.5 text-xs">
      <button
        type="button"
        onClick={() => onChange(offset - 1)}
        disabled={offset <= minOffset}
        aria-label={`${unit} sebelumnya`}
        title={`${unit} sebelumnya`}
        className="inline-flex min-h-10 min-w-10 items-center justify-center rounded-lg text-[var(--muted)] transition-colors hover:bg-[var(--surface)] hover:text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)] disabled:opacity-30"
      >
        <Icon name="chevron-left" className="h-3.5 w-3.5" />
      </button>
      <span aria-live="polite" className="min-w-[7.5rem] select-none px-1 text-center font-bold tabular text-[var(--text)]">
        {label}
      </span>
      <button
        type="button"
        onClick={() => onChange(offset + 1)}
        disabled={offset >= 0}
        aria-label={`${unit} berikutnya`}
        title={`${unit} berikutnya`}
        className="inline-flex min-h-10 min-w-10 items-center justify-center rounded-lg text-[var(--muted)] transition-colors hover:bg-[var(--surface)] hover:text-[var(--text)] focus-visible:outline-2 focus-visible:outline-[var(--primary)] disabled:opacity-30"
      >
        <Icon name="chevron-right" className="h-3.5 w-3.5" />
      </button>
      {offset !== 0 && (
        <button
          type="button"
          onClick={() => onChange(0)}
          className="min-h-10 px-2 text-[11px] font-semibold text-[var(--text)] underline-offset-2 hover:underline focus-visible:outline-2 focus-visible:outline-[var(--primary)]"
        >
          {unit === "Siklus" ? "Siklus ini" : "Bulan ini"}
        </button>
      )}
    </div>
  );
}
