import type { ReactNode } from "react";
import type { Words } from "@/lib/i18n/types";

/** The outcome of one Tinjau action; a failure carries its text for the dialog that sent it. */
export type SaveResult = { saved: true } | { saved: false; error: string | null };

/**
 * What one item shows in the item pane (apply-markas-tinjau D7, D9): the question, one meta line, optional warning,
 * note and reasons lines, the numbered answers, the rows below and the notification text.
 */
export interface ItemParts {
  question: string;
  meta: ReactNode[];
  warning?: string;
  note?: ReactNode;
  reasons?: string[];
  answers: ReactNode;
  rows?: ReactNode;
  raw?: string | null;
}

export interface ItemContext {
  words: Words;
  /** The display currency's figure, or its mask while balances are hidden. */
  money: (amount: number) => string;
  hidden: boolean;
  pending: boolean;
  /** The question's id, which names each answer group. */
  headingId: string;
}
