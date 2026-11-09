"use client";

import { type ReactNode, useCallback, useEffect, useId, useRef } from "react";
import { Icon } from "@/components/ui/Icon";
import { focusableIn, syncDialog, useEscapeLayer, useFocusTrap, useScrollLock } from "./layers";

/**
 * A shell dialog (Lainnya, Bantuan & pintasan, Rincian): the prototype's `.dialog` at `--dw`, a bottom sheet below 768 px.
 * Modal through the native <dialog>; Tab is trapped, Esc and the close button call `onClose`, and the caller
 * returns focus to whatever opened it: the dialog is closed first, so the rest of the page is no longer inert.
 */
export function ShellDialog({
  open,
  onClose,
  title,
  width,
  closeLabel,
  className,
  meta,
  footer,
  focusTitle = false,
  fullHeight = false,
  scrollFocusable = false,
  initialFocus,
  children,
}: {
  open: boolean;
  onClose: () => void;
  title: string;
  width: number;
  closeLabel: string;
  className?: string;
  /** One muted line under the title, such as "Rincian hitungan". */
  meta?: ReactNode;
  /** The footer row with its 1 px top line; actions sit at the right. */
  footer?: ReactNode;
  /**
   * The title takes focus on opening instead of a control, so a phone's soft keyboard does not cover the context
   * (Catat below 768 px, apply-markas-catat D15).
   */
  focusTitle?: boolean;
  /** Below 768 px the whole screen, square and without the grab (Tinjau's item, apply-markas-tinjau D13). */
  fullHeight?: boolean;
  /**
   * The body is a tab stop, so a long body without controls of its own can still be scrolled by keyboard (WCAG
   * 2.1.1; Transaksi's delete confirmation on a short screen).
   */
  scrollFocusable?: boolean;
  /**
   * A selector inside the body that takes focus on opening instead of its first control, such as Tinjau's item
   * heading. Focusing it here, after the body is committed, cannot be overtaken by this effect.
   */
  initialFocus?: string;
  children: ReactNode;
}) {
  const ref = useRef<HTMLDialogElement>(null);
  const bodyRef = useRef<HTMLDivElement>(null);
  const titleId = useId();
  const close = useCallback(() => {
    syncDialog(ref.current, false);
    onClose();
  }, [onClose]);

  useEffect(() => {
    syncDialog(ref.current, open);
    if (!open) return;
    const body = bodyRef.current;
    if (focusTitle) {
      ref.current?.querySelector<HTMLElement>(".dlg-t h2")?.focus();
      return;
    }
    const chosen = initialFocus ? body?.querySelector<HTMLElement>(initialFocus) : null;
    const first = chosen ?? (body ? focusableIn(body)[0] : undefined);
    (first ?? ref.current?.querySelector<HTMLElement>("button"))?.focus();
    // Only the opening moment decides the first focus; a later selector change must not move focus.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);
  useFocusTrap(ref, open);
  useEscapeLayer(open, close);
  useScrollLock(open);

  return (
    <dialog
      ref={ref}
      className={["shell-dialog", fullHeight && "is-full", className].filter(Boolean).join(" ")}
      style={{ "--dw": `${width}px` } as React.CSSProperties}
      aria-labelledby={titleId}
      onCancel={(event) => {
        event.preventDefault();
        close();
      }}
      onClick={(event) => {
        if (event.target === event.currentTarget) close();
      }}
    >
      {open && (
        <>
          <span className="sheet-grab" aria-hidden="true" />
          <div className="dlg-h">
            <div className="dlg-t">
              <h2 id={titleId} className="t-dtitle" tabIndex={focusTitle ? -1 : undefined}>
                {title}
              </h2>
              {meta && <p className="t-meta">{meta}</p>}
            </div>
            <button type="button" className="ws-icon-btn" aria-label={closeLabel} onClick={close}>
              <Icon name="x" className="h-4 w-4" />
            </button>
          </div>
          <div
            ref={bodyRef}
            className="dlg-b"
            tabIndex={scrollFocusable ? 0 : undefined}
            role={scrollFocusable ? "region" : undefined}
            aria-labelledby={scrollFocusable ? titleId : undefined}
          >
            {children}
          </div>
          {footer && <div className="dlg-f">{footer}</div>}
        </>
      )}
    </dialog>
  );
}
