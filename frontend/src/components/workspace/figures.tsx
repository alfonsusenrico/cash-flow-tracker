"use client";

import type { ReactNode } from "react";
import { useTranslations } from "next-intl";
import { useAppCtx } from "@/components/layout/AppLayout";
import { useFormat } from "@/lib/format";
import { cn } from "@/lib/utils";
import { MaskedMoney } from "./money";

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
  aside,
  busy = false,
  children,
}: {
  id: string;
  label: string;
  /** The colour token of the 16 × 2 px mark above the label; a count such as Tinjau's has none (apply-markas-tinjau D3). */
  mark?: string;
  action?: ReactNode;
  figure: ReactNode;
  /** One muted line under the figure; left out when the page has nothing to qualify (apply-markas-transaksi D4). */
  line?: ReactNode;
  /** Supporting figures beside the hero (`.hero.is-split`, Transaksi D4); they wrap under it on a narrow column. */
  aside?: ReactNode;
  /** The figures are still loading: the section is a busy region. */
  busy?: boolean;
  children?: ReactNode;
}) {
  const main = (
    <>
      <div className="hero-top">
        {/* Inside the label so it stays 3 px above its first line when a longer language wraps it. */}
        <h2 className="t-label" id={id}>
          {mark && <span className="sw-bar" style={{ background: mark }} aria-hidden="true" />}
          {label}
        </h2>
        {action}
      </div>
      <p className="t-hero hero-fig">{figure}</p>
      {line !== undefined && line !== null && line !== false && <p className="hero-sub t-body">{line}</p>}
      {children}
    </>
  );
  if (aside === undefined) {
    return (
      <section className="hero" aria-labelledby={id} aria-busy={busy || undefined}>
        {main}
      </section>
    );
  }
  return (
    <section className="hero is-split" aria-labelledby={id} aria-busy={busy || undefined}>
      <div className="hero-main">{main}</div>
      <div className="supports hero-aside">{aside}</div>
    </section>
  );
}

export function FigureBlock({
  label,
  mark,
  figure,
  caption,
  phoneHidden = false,
  bar = true,
  className,
}: {
  label: string;
  /** The colour token of the bar above the label; without one the slot stays empty so labels align. */
  mark?: string;
  figure: ReactNode;
  caption?: ReactNode;
  /** Left out below 768 px, where the figure is read in the Rincian instead (D9). */
  phoneHidden?: boolean;
  /** Without the colour-bar slot: Transaksi's supporting figures have a label only (apply-markas-transaksi D4). */
  bar?: boolean;
  /** Classes of the figure itself, such as its kind's colour. */
  className?: string;
}) {
  return (
    <div className={cn("fig-block", phoneHidden && "is-phone-hide")}>
      <div className="fb-label">
        {bar && <span className="sw-bar" style={mark ? { background: mark } : undefined} aria-hidden="true" />}
        <span className="t-meta">{label}</span>
      </div>
      <p className={cn("t-fig", className)}>{figure}</p>
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

/**
 * A change between two server amounts (apply-markas-analisis D5, D24): "▲ Rp 753.830" when the amount rose, "▼ Rp
 * 120.000" when it fell, "sama" when equal, with the direction as hidden words for screen readers. For spending a
 * rise reads in the expense ink and a fall in the income ink; the amount hides with "Sembunyikan saldo".
 */
export function ChangeFigure({ change, money, riseIsCost = true }: { change: number; money: (amount: number) => string; riseIsCost?: boolean }) {
  const t = useTranslations("common.change");
  if (change === 0) return <span className="t-amt">{t("same")}</span>;
  const rose = change > 0;
  const tone = rose === riseIsCost ? "c-expense" : "c-income";
  return (
    <span className={cn("t-amt chg", tone)}>
      <span aria-hidden="true">{rose ? "▲" : "▼"} </span>
      <span className="sr-only">{rose ? t("up") : t("down")} </span>
      <MaskedMoney text={money(Math.abs(change))} />
    </span>
  );
}

/**
 * The progress bar (SPEC `.prog`, apply-markas-analisis D24, apply-markas-rekening D18): a 4 px track, 8 px with
 * `thick`, optionally `width` px wide, and a square fill of `value` over `max` (a percent by default), clamped to the
 * track, in a colour token. Decorative: the caller writes the value in text beside it.
 */
export function ProgressBar({ value, max = 100, thick = false, width, color }: {
  value: number;
  max?: number;
  thick?: boolean;
  width?: number;
  color?: string;
}) {
  const share = max > 0 && Number.isFinite(value) ? Math.min(1, Math.max(0, value / max)) : 0;
  return (
    <span className={cn("prog", thick && "is-thick")} style={width ? { width } : undefined} aria-hidden="true">
      <span style={{ width: `${share * 100}%`, ...(color ? { background: color } : {}) }} />
    </span>
  );
}

/** A gain over a set of products: its amount and cost, or `unknown` when a cost basis is missing (D6). */
export type GainFigure = { amount: number; cost: number } | "unknown";

/**
 * A capital gain as "▲ +Rp 396.000 · +8,0%" or "▼ −Rp 164.000 · −10,7%" (SPEC `M.gain`, apply-markas-rekening D6):
 * the triangle, the sign and a hidden "untung" or "rugi" besides the colour, the percent of the cost with one
 * decimal, and the cost note when one is unknown. Nothing without products. With "Sembunyikan saldo" the amount
 * and its sign are masked and the percent stays (D17).
 */
export function GainText({ gain, unknownLabel, className }: { gain: GainFigure | null; unknownLabel?: string; className?: string }) {
  const t = useTranslations("accounts.tile");
  const format = useFormat();
  const { hideBalances } = useAppCtx();
  if (!gain) return null;
  if (gain === "unknown") return <span className={cn("gain", "muted", className)}>{unknownLabel ?? t("costMissing")}</span>;
  const up = gain.amount >= 0;
  const sign = up ? "+" : "\u2212";
  const percent = gain.cost > 0 ? format.percent((Math.abs(gain.amount) / gain.cost) * 100) : null;
  return (
    <span className={cn("gain", up ? "c-income" : "c-expense", className)}>
      <span aria-hidden="true">{up ? "\u25B2" : "\u25BC"} </span>
      {!hideBalances && sign}
      <MaskedMoney text={format.money(Math.abs(gain.amount))} />
      {percent && ` \u00B7 ${sign}${percent}`}
      <span className="sr-only"> {up ? t("gainUp") : t("gainDown")}</span>
    </span>
  );
}
