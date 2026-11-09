"use client";

import { useTranslations } from "next-intl";
import type { ReactNode } from "react";
import type { IconName } from "@/components/ui/Icon";
import { cn } from "@/lib/utils";

export function Segmented<T extends string>({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: { value: T; label: string }[];
  value: T;
  onChange: (value: T) => void;
}) {
  return (
    <div className="ws-seg" role="group" aria-label={label}>
      {options.map((option) => (
        <button key={option.value} type="button" aria-pressed={option.value === value} onClick={() => onChange(option.value)}>
          {option.label}
        </button>
      ))}
    </div>
  );
}

/**
 * A segmented radio group (SPEC `.seg`): one tab stop, arrow keys move and choose, `aria-checked` marks the choice.
 * With `value` null no option is checked, and the first option takes the tab stop.
 */
export function RadioSegments<T extends string>({
  label,
  labelledBy,
  options,
  value,
  onChange,
  className,
}: {
  label?: string;
  labelledBy?: string;
  /** An option may name its own `lang`, such as a language written in its own words. */
  options: { value: T; label: string; disabled?: boolean; lang?: string }[];
  value: T | null;
  onChange: (value: T) => void;
  className?: string;
}) {
  const enabled = options.filter((option) => !option.disabled);
  const focusable = value !== null && enabled.some((option) => option.value === value) ? value : enabled[0]?.value;
  const move = (event: React.KeyboardEvent<HTMLButtonElement>, from: T) => {
    const step = event.key === "ArrowRight" || event.key === "ArrowDown" ? 1 : event.key === "ArrowLeft" || event.key === "ArrowUp" ? -1 : 0;
    if (!step) return;
    event.preventDefault();
    const index = enabled.findIndex((option) => option.value === from);
    const next = enabled[(index + step + enabled.length) % enabled.length];
    if (!next) return;
    onChange(next.value);
    event.currentTarget.parentElement?.querySelector<HTMLElement>(`[data-value="${next.value}"]`)?.focus();
  };
  return (
    <div className={cn("ws-seg", className)} role="radiogroup" aria-label={label} aria-labelledby={labelledBy}>
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          role="radio"
          data-value={option.value}
          lang={option.lang}
          aria-checked={option.value === value}
          disabled={option.disabled}
          tabIndex={option.value === focusable ? 0 : -1}
          onClick={() => onChange(option.value)}
          onKeyDown={(event) => move(event, option.value)}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

export function Skeleton({ className, style }: { className?: string; style?: React.CSSProperties }) {
  return <div className={cn("ws-skeleton", className)} style={style} aria-hidden="true" />;
}

// Empty state (SPEC §7): centred 14/500 title, one 12/400 line and an optional action; no icon tile.
export function EmptyState({ title, children, action }: { icon?: IconName; title: string; children?: ReactNode; action?: ReactNode }) {
  return (
    <div className="ws-empty">
      <h2>{title}</h2>
      {children && <p>{children}</p>}
      {action}
    </div>
  );
}

/** Failure without fabricated figures: the message and a retry. */
export function ErrorState({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div className="ws-error" role="alert">
      <p>{message}</p>
      {onRetry && (
        <RetryButton onRetry={onRetry} />
      )}
    </div>
  );
}

function RetryButton({ onRetry }: { onRetry: () => void }) {
  const t = useTranslations("common");
  return (
    <button type="button" className="ws-btn" onClick={onRetry}>
      {t("tryAgain")}
    </button>
  );
}
