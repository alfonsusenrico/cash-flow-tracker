"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * One numbered answer of a Tinjau item (redesign-review-triage D4). The key hint is visible; 1–9 press the
 * nth choice of the open picker, else of the item, and E presses the highlighted (`primary`) one.
 */
export function Choice({
  n,
  label,
  sub,
  primary = false,
  onClick,
  disabled,
  expanded,
  controls,
  hint,
  className,
  topup = false,
}: {
  n?: number;
  label: ReactNode;
  sub?: ReactNode;
  primary?: boolean;
  onClick: () => void;
  disabled?: boolean;
  expanded?: boolean;
  controls?: string;
  /** Overrides the key hint, for example "E" or "T". */
  hint?: string;
  className?: string;
  /** Marks the control that T opens. */
  topup?: boolean;
}) {
  const key = hint ?? (n !== undefined && n <= 9 ? String(n) : undefined);
  return (
    <button
      type="button"
      className={cn("ws-choice", className)}
      data-choice={n}
      data-primary-choice={primary ? "" : undefined}
      data-topup={topup ? "" : undefined}
      aria-expanded={expanded}
      aria-controls={controls}
      aria-keyshortcuts={key && /^[1-9]$/.test(key) ? key : undefined}
      disabled={disabled}
      onClick={onClick}
    >
      <span className="min-w-0">
        <span className="ws-lbl">{label}</span>
        {sub && <span className="ws-sub">{sub}</span>}
      </span>
      {key && (
        <kbd className="ws-kbd" aria-hidden="true">
          {key}
        </kbd>
      )}
    </button>
  );
}

/** A group of choices that opened inside an item; while open, 1–9 press its choices. */
export function Picker({ id, title, children }: { id: string; title: string; children: ReactNode }) {
  return (
    <div className="ws-picker" role="group" aria-labelledby={`${id}-title`} id={id} data-picker="">
      <h3 id={`${id}-title`}>{title}</h3>
      {children}
    </div>
  );
}

/** The key press helpers the inbox uses to reach the choices of the shown item. */
export function pressChoice(root: HTMLElement | null, n: number): boolean {
  if (!root) return false;
  const pickers = root.querySelectorAll<HTMLElement>("[data-picker]");
  const scope = pickers.length ? pickers[pickers.length - 1] : root;
  const candidates = [...scope.querySelectorAll<HTMLButtonElement>(`[data-choice="${n}"]`)].filter(
    (button) => scope !== root || !button.closest("[data-picker]"),
  );
  const target = candidates[0];
  if (!target || target.disabled) return false;
  target.click();
  return true;
}

export function pressMarked(root: HTMLElement | null, marker: "data-primary-choice" | "data-topup"): boolean {
  const target = root?.querySelector<HTMLButtonElement>(`[${marker}]`);
  if (!target || target.disabled) return false;
  target.click();
  return true;
}
