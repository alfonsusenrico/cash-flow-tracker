"use client";

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
              <span className="sr-only">{countLabel ?? `${count} item`}</span>
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

export function KpiStrip({ label, children }: { label: string; children: ReactNode }) {
  return (
    <section className="ws-card ws-kpis" aria-label={label}>
      {children}
    </section>
  );
}

/** One KPI: label, the figure (usually an explainable button), at most one chip and a caption. */
export function Kpi({
  icon,
  label,
  value,
  chip,
  caption,
  valueIsWords = false,
}: {
  icon: IconName;
  label: string;
  value: ReactNode;
  chip?: ReactNode;
  caption?: ReactNode;
  valueIsWords?: boolean;
}) {
  return (
    <div className="ws-kpi">
      <div className="ws-tile" aria-hidden="true">
        <Icon name={icon} />
      </div>
      <div className="ws-kpi-body">
        <div className="ws-kpi-label">{label}</div>
        <div className="ws-kpi-row">
          <div className={cn("ws-kpi-value", valueIsWords && "is-words")}>{value}</div>
          {chip}
        </div>
        {caption && <div className="ws-kpi-caption">{caption}</div>}
      </div>
    </div>
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

export function ChipGroup<T extends string>({
  label,
  options,
  selected,
  onToggle,
}: {
  label: string;
  options: { value: T; label: string; count?: number }[];
  selected: readonly T[];
  onToggle: (value: T) => void;
}) {
  return (
    <div className="ws-chips" role="group" aria-label={label}>
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          className="ws-chip"
          aria-pressed={selected.includes(option.value)}
          onClick={() => onToggle(option.value)}
        >
          {option.label}
          {option.count !== undefined && <span className="ws-cnt">{option.count}</span>}
        </button>
      ))}
    </div>
  );
}

export interface MoneySegment {
  key: string;
  label: string;
  /** Integer rupiah from the server; only used for proportions. */
  amount: number;
  display: string;
  className: string;
}

/**
 * Labelled money segments. The bar is decorative (`aria-hidden`); every value is also text, either in
 * the built-in legend or in the caller's own labelled columns (`legend={false}`).
 */
export function MoneyBar({ segments, legend = true, label }: { segments: MoneySegment[]; legend?: boolean; label?: string }) {
  const visible = segments.filter((segment) => segment.amount > 0);
  return (
    <div>
      <div className="ws-moneybar" aria-hidden="true">
        {visible.map((segment) => (
          <span key={segment.key} className={segment.className} style={{ flex: segment.amount }}>
            <span className="ws-moneybar-text">{segment.display}</span>
          </span>
        ))}
      </div>
      {legend && (
        <ul className="ws-legend" aria-label={label}>
          {segments.map((segment) => (
            <li key={segment.key}>
              <span className="ws-legend-h">{segment.label}</span>
              <span className="ws-legend-v">{segment.display}</span>
            </li>
          ))}
        </ul>
      )}
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
        <button type="button" className="ws-btn" onClick={onRetry}>
          Coba lagi
        </button>
      )}
    </div>
  );
}

/** A KPI card: small label with a colour dot, a large figure and one muted line (refine-workspace-layout D5). */
export function KpiCard({
  label,
  tone,
  hero,
  value,
  valueIsWords,
  caption,
}: {
  label: string;
  tone: "in" | "out" | "save" | "net" | "safe" | "inv" | "neutral";
  hero?: boolean;
  value: ReactNode;
  valueIsWords?: boolean;
  caption: ReactNode;
}) {
  return (
    <div className="ws-kcard" data-hero={hero || undefined} data-tone={tone}>
      <div className="ws-kcard-l">
        {!hero && <span className="ws-kcard-dot" aria-hidden="true" />}
        {label}
      </div>
      <div className={valueIsWords ? "ws-kcard-v is-words" : "ws-kcard-v"}>{value}</div>
      <div className="ws-kcard-c">{caption}</div>
    </div>
  );
}
