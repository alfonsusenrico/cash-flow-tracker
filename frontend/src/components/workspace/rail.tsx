"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * The rail of folders, filters or sections (option-6 SPEC "Rail item", apply-markas-tinjau D5): a labelled `nav`
 * of 36 px items, the current one with its fill and the straight 2 × 20 px bar 8 px to its left. With `chips` it
 * becomes one sideways-scrolling line of 44 px chips below 768 px, where the current chip is a fill and an edge and
 * carries no bar, because a rounded chip never gets a coloured edge.
 */
export function Rail({ label, chips = false, className, children }: { label: string; chips?: boolean; className?: string; children: ReactNode }) {
  return (
    <nav className={cn("rail", chips && "is-chips", className)} aria-label={label}>
      <span className="t-label rail-label" aria-hidden="true">
        {label}
      </span>
      {children}
    </nav>
  );
}

export function RailItem({
  label,
  count,
  countName,
  current = false,
  onSelect,
}: {
  label: string;
  count?: string;
  /** The count in words for assistive technology, such as "5 item". */
  countName?: string;
  current?: boolean;
  onSelect: () => void;
}) {
  return (
    <button type="button" className="rail-item ws-select-bar" aria-current={current ? "true" : undefined} onClick={onSelect}>
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
