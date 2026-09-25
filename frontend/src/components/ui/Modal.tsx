"use client";
import { useId, useLayoutEffect, useRef } from "react";
import { createPortal } from "react-dom";
import { cn } from "@/lib/utils";

interface ModalProps {
  open: boolean;
  onClose: () => void;
  title: string;
  children: React.ReactNode;
  wide?: boolean;
}

export function Modal({ open, onClose, title, children, wide }: ModalProps) {
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
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    document.addEventListener("keydown", handler);
    return () => {
      document.body.style.overflow = previousOverflow;
      document.removeEventListener("keydown", handler);
      previouslyFocused?.focus();
    };
  }, [open]);

  if (!open) return null;

  return createPortal(
    <div
      className="fixed inset-0 z-[100] flex items-end justify-center overflow-y-auto overscroll-contain bg-black/60 p-0 backdrop-blur-xs transition-opacity motion-reduce:transition-none sm:items-center sm:p-4"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div
        ref={ref}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        tabIndex={-1}
        className={cn(
          "w-full overflow-y-auto border-t sm:border border-[var(--border)] bg-[var(--surface)] shadow-2xl",
          "rounded-t-[28px] sm:rounded-[var(--radius-card,20px)] max-h-[90dvh] sm:max-h-[calc(100dvh-48px)]",
          "animate-in slide-in-from-bottom-5 duration-200 ease-out motion-reduce:animate-none motion-reduce:transition-none sm:fade-in",
          wide ? "sm:max-w-2xl" : "sm:max-w-md"
        )}
      >
        {/* Mobile Pull/Drag Indicator */}
        <div className="sm:hidden flex justify-center pt-2.5 pb-1 select-none">
          <div className="w-10 h-1.5 rounded-full bg-[var(--muted)]/30" />
        </div>

        <div className="flex items-center justify-between px-4 sm:px-6 py-3 sm:py-4 border-b border-[var(--border)]">
          <h2 id={titleId} className="font-bold text-sm sm:text-base tracking-tight text-[var(--text)]">
            {title}
          </h2>
          <button
            onClick={onClose}
            type="button"
            className="inline-flex min-h-11 min-w-11 items-center justify-center rounded-xl text-lg leading-none text-[var(--muted)] transition-colors hover:bg-[var(--surface-raised)] hover:text-[var(--text)] motion-reduce:transition-none"
            aria-label="Tutup"
          >
            ✕
          </button>
        </div>
        <div className="px-4 sm:px-6 py-3.5 sm:py-4">{children}</div>
      </div>
    </div>,
    document.body
  );
}
