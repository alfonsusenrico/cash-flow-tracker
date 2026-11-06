"use client";

import { useLocale, useTranslations } from "next-intl";
import { UntranslatedNotice, useUntranslated } from "@/components/workspace/untranslated";
import { useId, useLayoutEffect, useRef } from "react";
import { createPortal } from "react-dom";
import { lockPageScroll } from "@/components/workspace/layers";
import { cn } from "@/lib/utils";

interface ModalProps {
  /** Content a later phase translates: Indonesian with `lang="id"` and the notice line in an English interface. */
  untranslated?: boolean;
  open: boolean;
  onClose: () => void;
  title: string;
  /** A short line under the title, e.g. the account kind. */
  subtitle?: string;
  /** A decorative mark before the title; it must be aria-hidden. */
  leading?: React.ReactNode;
  children: React.ReactNode;
  wide?: boolean;
  centered?: boolean;
  hideCloseButton?: boolean;
}

export function Modal({ open, onClose, title, subtitle, leading, children, wide, centered, hideCloseButton, untranslated: untranslatedContent }: ModalProps) {
  const t = useTranslations("common");
  const locale = useLocale();
  const untranslated = useUntranslated(untranslatedContent);
  const ref = useRef<HTMLDivElement>(null);
  const titleId = useId();
  const onCloseRef = useRef(onClose);

  useLayoutEffect(() => {
    onCloseRef.current = onClose;
  }, [onClose]);

  useLayoutEffect(() => {
    if (!open) return;
    const previouslyFocused = document.activeElement as HTMLElement | null;
    const dialog = ref.current;
    const focusableSelector =
      'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';
    const isDesktop =
      typeof window.matchMedia !== "function" ||
      window.matchMedia("(min-width: 640px)").matches;
    const preferred = isDesktop
      ? dialog?.querySelector<HTMLElement>("[data-autofocus]")
      : null;
    const firstFocusable = dialog?.querySelector<HTMLElement>(focusableSelector);
    (preferred ?? firstFocusable ?? dialog)?.focus();
    const handler = (event: KeyboardEvent) => {
      // A dialog opened from inside this one is portalled after it; only the topmost owns the keyboard.
      const modals = document.querySelectorAll('[role="dialog"][aria-modal="true"]');
      if (dialog && modals[modals.length - 1] !== dialog) return;
      if (event.key === "Escape") {
        event.preventDefault();
        onCloseRef.current();
        return;
      }
      if (event.key !== "Tab" || !dialog) return;
      const focusable = Array.from(dialog.querySelectorAll<HTMLElement>(focusableSelector));
      if (!focusable.length) {
        event.preventDefault();
        dialog.focus();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    const releaseScroll = lockPageScroll();
    document.addEventListener("keydown", handler);
    return () => {
      releaseScroll();
      document.removeEventListener("keydown", handler);
      previouslyFocused?.focus();
    };
  }, [open]);

  if (!open) return null;

  return createPortal(
    <div
      className={cn(
        "fixed inset-0 z-[100] flex justify-center overflow-y-auto overscroll-contain bg-[var(--scrim)] ws-scrim-in",
        centered ? "items-center p-4" : "items-end p-0 sm:items-center sm:p-6",
      )}
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      {/* Dialog geometry from SPEC §7: radius 16 (sheet 16 16 0 0), 1 px line, the dialog surface and the
          layer shadow; header 24 24 16 with the Manrope dialog title. */}
      <div
        ref={ref}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        className={cn(
          "w-full overflow-y-auto border border-[var(--line)] bg-[var(--dialog)] text-[var(--text)] shadow-float",
          centered
            ? "rounded-2xl max-h-[calc(100dvh-32px)] ws-layer-in"
            : "rounded-t-2xl border-b-0 sm:border-b sm:rounded-2xl max-h-[92dvh] sm:max-h-[calc(100dvh-96px)] ws-sheet-in sm:ws-layer-in",
          wide ? "sm:max-w-2xl" : "sm:max-w-md"
        )}
      >
        {!centered && (
          <div className="sm:hidden flex justify-center pt-2 select-none" aria-hidden="true">
            <div className="h-1 w-8 rounded-md bg-[var(--line-strong)]" />
          </div>
        )}

        <div className="flex items-start justify-between gap-3 px-4 pt-4 pb-3 sm:px-6 sm:pt-6 sm:pb-4">
          <div className="flex min-w-0 items-center gap-3">
            {leading}
            <div className="min-w-0">
              <h2 id={titleId} className="t-dtitle text-[var(--text)]" lang={untranslated ? "id" : undefined}>
                {title}
              </h2>
              {subtitle && (
                <p className="mt-1 text-xs text-[var(--muted)]" lang={untranslated ? "id" : undefined}>
                  {subtitle}
                </p>
              )}
              {untranslated && <UntranslatedNotice className="mt-1 text-xs text-[var(--muted)]" />}
            </div>
          </div>
          {!hideCloseButton && (
            <button
              onClick={onClose}
              type="button"
              className="ws-icon-btn -mr-2 -mt-1.5"
              aria-label={t("close")}
              lang={untranslated ? locale : undefined}
            >
              <svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M6 6l12 12M18 6 6 18" />
              </svg>
            </button>
          )}
        </div>
        <div className="px-4 pb-4 sm:px-6 sm:pb-6" lang={untranslated ? "id" : undefined}>
          {children}
        </div>
      </div>
    </div>,
    document.body
  );
}
