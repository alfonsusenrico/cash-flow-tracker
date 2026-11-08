"use client";

import { forwardRef, type ReactNode } from "react";
import { Icon } from "@/components/ui/Icon";

/**
 * Tinjau's numbered answers (apply-markas-tinjau D8): each answer is one whole-row button of at least 56 px with its
 * key cap 1–9, its label and detail and a chevron. 1–9 press the nth answer of the last open nested list, else of the
 * item; E presses the answer marked `primary` and T the one marked `topup`. Only digits show a key cap; E and T are
 * listed in "Bantuan & pintasan".
 */
export function AnswerList({ labelledBy, children }: { labelledBy: string; children: ReactNode }) {
  return (
    <div className="answers" role="group" aria-labelledby={labelledBy}>
      {children}
    </div>
  );
}

/** A list of answers that opened under one answer (accounts, products, existing legs); while open, 1–9 press its answers. */
export function NestedAnswers({ id, title, children, note }: { id: string; title: string; children: ReactNode; note?: ReactNode }) {
  return (
    <div className="answers-nested" id={id} data-picker="">
      <h3 className="t-label" id={`${id}-title`}>
        {title}
      </h3>
      {note}
      <AnswerList labelledBy={`${id}-title`}>{children}</AnswerList>
    </div>
  );
}

export function KeyCap({ children }: { children: ReactNode }) {
  return (
    <kbd className="ws-kbd" aria-hidden="true">
      {children}
    </kbd>
  );
}

export interface AnswerProps {
  n: number;
  label: ReactNode;
  detail?: ReactNode;
  /** The answer E presses. */
  primary?: boolean;
  /** The answer T presses. */
  topup?: boolean;
  /** For an answer that opens a nested list. */
  expanded?: boolean;
  controls?: string;
  disabled?: boolean;
  onClick: () => void;
}

/** One numbered answer; its ref lets a dialog it opens give focus back to it. */
export const Answer = forwardRef<HTMLButtonElement, AnswerProps>(function Answer(
  { n, label, detail, primary = false, topup = false, expanded, controls, disabled, onClick },
  ref,
) {
  const digit = n <= 9 ? String(n) : null;
  const shortcuts = [digit, primary ? "E" : null, topup ? "T" : null].filter(Boolean).join(" ");
  return (
    <button
      ref={ref}
      type="button"
      className="answer"
      data-choice={n}
      data-primary-choice={primary ? "" : undefined}
      data-topup={topup ? "" : undefined}
      aria-keyshortcuts={shortcuts || undefined}
      aria-expanded={expanded}
      aria-controls={controls}
      disabled={disabled}
      onClick={onClick}
    >
      {digit ? <KeyCap>{digit}</KeyCap> : <span className="a-cap" aria-hidden="true" />}
      <span className="a-main">
        <span className="t-strong">{label}</span>
        {detail && <span className="t-meta">{detail}</span>}
      </span>
      <Icon name="chevR" className="a-chev h-4 w-4" />
    </button>
  );
});

/** Presses the nth answer of the last open nested list, else of the item; false when there is none to press. */
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
