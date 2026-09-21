"use client";
import { useEffect, useRef } from "react";
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

  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    document.addEventListener("keydown", handler);
    return () => {
      document.body.style.overflow = previousOverflow;
      document.removeEventListener("keydown", handler);
    };
  }, [open, onClose]);

  if (!open) return null;

  return createPortal(
    <div
      className="fixed inset-0 z-[100] flex items-end sm:items-center justify-center overflow-y-auto bg-black/60 p-0 sm:p-4 backdrop-blur-xs transition-opacity"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div
        ref={ref}
        role="dialog"
        aria-modal="true"
        aria-labelledby="modal-title"
        className={cn(
          "w-full overflow-y-auto border-t sm:border border-[var(--border)] bg-[var(--surface)] shadow-2xl",
          "rounded-t-[28px] sm:rounded-[var(--radius-card,20px)] max-h-[90dvh] sm:max-h-[calc(100dvh-48px)]",
          "animate-in slide-in-from-bottom-5 sm:fade-in duration-200 ease-out",
          wide ? "sm:max-w-2xl" : "sm:max-w-md"
        )}
      >
        {/* Mobile Pull/Drag Indicator */}
        <div className="sm:hidden flex justify-center pt-2.5 pb-1 select-none">
          <div className="w-10 h-1.5 rounded-full bg-[var(--muted)]/30" />
        </div>

        <div className="flex items-center justify-between px-4 sm:px-6 py-3 sm:py-4 border-b border-[var(--border)]">
          <h2 id="modal-title" className="font-bold text-sm sm:text-base tracking-tight text-[var(--text)]">
            {title}
          </h2>
          <button
            onClick={onClose}
            className="p-1 rounded-xl text-[var(--muted)] hover:text-[var(--text)] hover:bg-[var(--surface-raised)] transition-colors text-lg leading-none"
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
