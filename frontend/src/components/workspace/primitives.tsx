"use client";

import { useTranslations } from "next-intl";
import { useInterfaceLang } from "./untranslated";
import type { ReactNode } from "react";
import { Icon, type IconName } from "@/components/ui/Icon";
import { cn } from "@/lib/utils";

/** Card on the panel surface: flat, no shadow and no outline (SPEC §5). */
export function Card({
  children,
  className,
  labelledBy,
  label,
  as: Tag = "section",
}: {
  children: ReactNode;
  className?: string;
  labelledBy?: string;
  label?: string;
  as?: "section" | "div" | "article";
}) {
  return (
    <Tag className={cn("ws-card", className)} aria-labelledby={labelledBy} aria-label={label}>
      {children}
    </Tag>
  );
}

/** Card header: decorative icon tile, title (with optional count), caption and one action. */
export function CardHeader({
  id,
  title,
  icon,
  count,
  countLabel,
  caption,
  action,
  level = 2,
  extra,
}: {
  id?: string;
  title: ReactNode;
  icon?: IconName;
  count?: number;
  /** Words for the count so it is not a bare number for screen readers. */
  countLabel?: string;
  caption?: ReactNode;
  action?: ReactNode;
  level?: 2 | 3;
  extra?: ReactNode;
}) {
  const Heading = level === 2 ? "h2" : "h3";
  return (
    <div className="ws-card-h">
      {icon && (
        <div className="ws-tile" aria-hidden="true">
          <Icon name={icon} />
        </div>
      )}
      <div className="ws-card-t">
        <Heading id={id}>
          {title}
          {count !== undefined && (
            <span className="ws-cnt">
              <span aria-hidden="true">{count}</span>
              <span className="sr-only">{countLabel ?? <ItemCount count={count} />}</span>
            </span>
          )}
          {extra}
        </Heading>
        {caption && <div className="ws-cap">{caption}</div>}
      </div>
      {action}
    </div>
  );
}

export type Tone = "in" | "out" | "tr" | "inv" | "warn" | "ok" | "neutral";

export function Pill({ tone = "neutral", icon, children, className }: { tone?: Tone; icon?: IconName; children: ReactNode; className?: string }) {
  return (
    <span className={cn("ws-pill", `ws-tone-${tone}`, className)}>
      {icon && <Icon name={icon} />}
      {children}
    </span>
  );
}

export function DeltaChip({ tone, children, title }: { tone: Tone; children: ReactNode; title?: string }) {
  return (
    <span className={cn("ws-chip-delta", `ws-tone-${tone}`)} title={title}>
      {children}
    </span>
  );
}

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

/** "{count} item" for a section count, in the interface language (add-language-system D17). */
function ItemCount({ count }: { count: number }) {
  const t = useTranslations("home");
  const lang = useInterfaceLang();
  return <span lang={lang}>{t("itemCount", { count })}</span>;
}

function RetryButton({ onRetry }: { onRetry: () => void }) {
  const t = useTranslations("common");
  const lang = useInterfaceLang();
  return (
    <button type="button" className="ws-btn" onClick={onRetry} lang={lang}>
      {t("tryAgain")}
    </button>
  );
}
