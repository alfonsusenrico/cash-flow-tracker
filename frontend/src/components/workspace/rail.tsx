"use client";

import Link from "next/link";
import type { ReactNode } from "react";
import { Icon, type IconName } from "@/components/ui/Icon";
import { cn } from "@/lib/utils";

/**
 * The rail of folders, filters or sections (option-6 SPEC "Rail item", apply-markas-tinjau D5): a labelled `nav`
 * of 36 px items, the current one with its fill and the straight 2 × 20 px bar 8 px to its left. With `chips` it
 * becomes one sideways-scrolling line of 44 px chips below 768 px, where the current chip is a fill and an edge and
 * carries no bar, because a rounded chip never gets a coloured edge.
 */
export function Rail({
  label,
  chips = false,
  showLabel = true,
  className,
  children,
}: {
  label: string;
  chips?: boolean;
  /** Without the visible label, when the rail names its own groups with `RailLabel` (Transaksi's JENIS and PERIODE). */
  showLabel?: boolean;
  className?: string;
  children: ReactNode;
}) {
  return (
    <nav className={cn("rail", chips && "is-chips", className)} aria-label={label}>
      {showLabel && (
        <span className="t-label rail-label" aria-hidden="true">
          {label}
        </span>
      )}
      {children}
    </nav>
  );
}

/** A group's label inside a rail, such as "PERIODE" after the rule; with `id` it names the group's list. */
export function RailLabel({ id, children }: { id?: string; children: ReactNode }) {
  return (
    <span className="t-label rail-label" id={id}>
      {children}
    </span>
  );
}

export function RailItem({
  label,
  count,
  countName,
  current = false,
  pressed,
  href,
  icon,
  onSelect,
}: {
  label: string;
  count?: string;
  /** The count in words for assistive technology, such as "5 item". */
  countName?: string;
  current?: boolean;
  /**
   * A toggle that filters the list rather than navigating (apply-markas-transaksi D5): `aria-pressed`, with the same
   * fill and bar as `current` while pressed.
   */
  pressed?: boolean;
  /**
   * An address, such as a Pengaturan section (apply-markas-pengaturan D2): the item is a link and the current one
   * carries `aria-current="page"`.
   */
  href?: string;
  /** A 16 px icon before the label, such as "Keluar"'s. */
  icon?: IconName;
  onSelect?: () => void;
}) {
  const chosen = pressed ?? current;
  if (href) {
    return (
      <Link href={href} className="rail-item ws-select-bar" aria-current={current ? "page" : undefined} onClick={onSelect}>
        {icon && <Icon name={icon} className="h-4 w-4" />}
        <span className="lbl">{label}</span>
      </Link>
    );
  }
  return (
    <button
      type="button"
      className="rail-item ws-select-bar"
      aria-pressed={pressed === undefined ? undefined : pressed}
      aria-current={chosen ? "true" : undefined}
      onClick={onSelect}
    >
      {icon && <Icon name={icon} className="h-4 w-4" />}
      <span className="lbl">{label}</span>
      {count !== undefined && (
        <span className="count" aria-hidden={countName ? "true" : undefined}>
          {count}
        </span>
      )}
      {countName && <span className="sr-only"> {countName}</span>}
    </button>
  );
}

export function RailRule() {
  return <span className="rail-sep" aria-hidden="true" />;
}
