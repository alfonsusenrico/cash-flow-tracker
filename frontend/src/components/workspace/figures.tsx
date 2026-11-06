"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * Figure blocks of the Markas+ pages (option-6 SPEC §7, apply-markas-beranda D3 and D4): the hero with its
 * label row, the page's one 44 px figure and one muted line; supporting figures with a colour bar, a label, the
 * 20 px figure and a caption; and the split bar whose segments add up to the whole.
 */
export function HeroFigure({
  id,
  label,
  mark,
  action,
  figure,
  line,
  children,
}: {
  id: string;
  label: string;
  /** The colour token of the 16 × 2 px mark above the label. */
  mark: string;
  action?: ReactNode;
  figure: ReactNode;
  line: ReactNode;
  children?: ReactNode;
}) {
  return (
    <section className="hero" aria-labelledby={id}>
      <div className="hero-top">
        <span className="sw-bar" style={{ background: mark }} aria-hidden="true" />
        <h2 className="t-label" id={id}>
          {label}
        </h2>
        {action}
      </div>
      <p className="t-hero hero-fig">{figure}</p>
      <p className="hero-sub t-body">{line}</p>
      {children}
    </section>
  );
}

export function FigureBlock({
  label,
  mark,
  figure,
  caption,
  phoneHidden = false,
}: {
  label: string;
  /** The colour token of the bar above the label; without one the slot stays empty so labels align. */
  mark?: string;
  figure: ReactNode;
  caption?: ReactNode;
  /** Left out below 768 px, where the figure is read in the Rincian instead (D9). */
  phoneHidden?: boolean;
}) {
  return (
    <div className={cn("fig-block", phoneHidden && "is-phone-hide")}>
      <div className="fb-label">
        <span className="sw-bar" style={mark ? { background: mark } : undefined} aria-hidden="true" />
        <span className="t-meta">{label}</span>
      </div>
      <p className="t-fig">{figure}</p>
      {caption && <p className="t-meta">{caption}</p>}
    </div>
  );
}

export interface SplitSegment {
  key: string;
  label: string;
  /** Integer amount from the server; only its share of the total is drawn. */
  amount: number;
  color: string;
}

/** Whole-number share of a total for an accessible name; display only. */
function share(amount: number, total: number): string {
  return `${Math.round((100 * amount) / total)}%`;
}

/**
 * The split bar: square segments 2 px apart, each as wide as its amount and never narrower than 2 px. A segment
 * without an amount takes no width, and a bar without any amount is not drawn. The bar is one image whose name
 * lists every segment's share of the total.
 */
export function SplitBar({
  label,
  emptyLabel,
  total,
  segments,
}: {
  /** The name's opening, such as "Uang siklus ini dari Rp 9.518.400 masuk". */
  label: string;
  /** The whole name when the total is zero, so no share is computed. */
  emptyLabel: string;
  total: number;
  segments: SplitSegment[];
}) {
  const drawn = segments.filter((segment) => segment.amount > 0);
  if (!drawn.length) return null;
  const name = total > 0 ? `${label}: ${segments.map((segment) => `${segment.label} ${share(segment.amount, total)}`).join(", ")}` : emptyLabel;
  return (
    <div className="split" role="img" aria-label={name}>
      {drawn.map((segment) => (
        <span key={segment.key} data-segment={segment.key} style={{ flex: segment.amount, background: segment.color }} />
      ))}
    </div>
  );
}
