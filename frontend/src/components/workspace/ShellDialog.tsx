"use client";

import { type ReactNode, useCallback, useEffect, useId, useRef } from "react";
import { Icon } from "@/components/ui/Icon";
import { UntranslatedNotice, useUntranslated } from "./untranslated";
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
  untranslated: untranslatedContent,
  fullHeight = false,
  scrollFocusable = false,
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
  /** Content a later phase translates: Indonesian with `lang="id"` and the notice line in an English interface. */
  untranslated?: boolean;
  /** Below 768 px the whole screen, square and without the grab (Tinjau's item, apply-markas-tinjau D13). */
  fullHeight?: boolean;
  /**
   * The body is a tab stop, so a long body without controls of its own can still be scrolled by keyboard (WCAG
   * 2.1.1; Transaksi's delete confirmation on a short screen).
   */
  scrollFocusable?: boolean;
  children: ReactNode;
}) {
  const untranslated = useUntranslated(untranslatedContent);
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
              <h2 id={titleId} className="t-dtitle" lang={untranslated ? "id" : undefined}>
                {title}
              </h2>
              {meta && <p className="t-meta">{meta}</p>}
              {untranslated && <UntranslatedNotice />}
            </div>
            <button type="button" className="ws-icon-btn" aria-label={closeLabel} onClick={close}>
              <Icon name="x" className="h-4 w-4" />
            </button>
          </div>
          <div
            ref={bodyRef}
            className="dlg-b"
            lang={untranslated ? "id" : undefined}
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
