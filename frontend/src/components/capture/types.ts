export type CaptureMode = "expense" | "income" | "movement";

/** What the dialog's primary footer button shows for the current view. */
export interface FooterState {
  label: string;
  disabled: boolean;
  /** No primary action, such as while a follow-up question waits for its answer. */
  hidden?: boolean;
}

/** How the capture dialog opens (apply-markas-catat D2). */
export type CaptureRequest =
  | { view: "sentence" }
  | {
      view: "manual";
      mode: CaptureMode;
      sourceAccountId?: string;
      targetAccountId?: string;
      /** A whole IDR amount to prefill, such as a credit bill's amount left. */
      amount?: number | null;
    };
