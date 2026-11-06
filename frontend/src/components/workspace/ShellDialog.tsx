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
  children,
}: {
  open: boolean;
  onClose: () => void;
  title: string;
  width: number;
  closeLabel: string;
  className?: string;
  /** One muted line under the title, such as "Rincian hitungan". */
  meta?: string;
  /** The footer row with its 1 px top line; actions sit at the right. */
  footer?: ReactNode;
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
    const first = body ? focusableIn(body)[0] : undefined;
    (first ?? ref.current?.querySelector<HTMLElement>("button"))?.focus();
  }, [open]);
  useFocusTrap(ref, open);
  useEscapeLayer(open, close);
  useScrollLock(open);

  return (
    <dialog
      ref={ref}
      className={["shell-dialog", className].filter(Boolean).join(" ")}
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
              <h2 id={titleId} className="t-dtitle">
                {title}
              </h2>
              {meta && <p className="t-meta">{meta}</p>}
            </div>
            <button type="button" className="ws-icon-btn" aria-label={closeLabel} onClick={close}>
              <Icon name="x" className="h-4 w-4" />
            </button>
          </div>
          <div ref={bodyRef} className="dlg-b">
            {children}
          </div>
          {footer && <div className="dlg-f">{footer}</div>}
        </>
      )}
    </dialog>
  );
}
