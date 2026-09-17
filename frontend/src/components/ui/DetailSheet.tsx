"use client";

import React, { useEffect } from "react";
import { cn } from "@/lib/utils";

interface DetailSheetProps {
  open: boolean;
  onClose: () => void;
  title: string;
  subtitle?: string;
  children: React.ReactNode;
  footer?: React.ReactNode;
  className?: string;
}

export function DetailSheet({
  open,
  onClose,
  title,
  subtitle,
  children,
  footer,
  className,
}: DetailSheetProps) {
  // ESC key handler
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape" && open) {
        onClose();
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [open, onClose]);

  // Lock scroll when open
  useEffect(() => {
    if (open) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "";
    }
    return () => {
      document.body.style.overflow = "";
    };
  }, [open]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex justify-end">
      {/* Backdrop */}
      <div
        onClick={onClose}
        className="fixed inset-0 bg-black/35 backdrop-blur-[2px] transition-opacity duration-200"
      />

      {/* Slide-out Sheet Panel */}
      <aside
        className={cn(
          "relative z-10 w-full max-w-[480px] h-full bg-[var(--canvas-card)] border-l border-[var(--border-structural)] shadow-2xl flex flex-col justify-between overflow-hidden animate-in slide-in-from-right duration-200",
          className
        )}
      >
        {/* Header */}
        <div className="px-5 py-4 border-b border-[var(--border-structural)] flex items-center justify-between gap-3 bg-[var(--canvas-app)]">
          <div className="min-w-0">
            <h2 className="text-sm sm:text-base font-bold text-[var(--text-primary)] tracking-tight truncate">
              {title}
            </h2>
            {subtitle && (
              <p className="text-xs text-[var(--text-muted)] truncate mt-0.5">
                {subtitle}
              </p>
            )}
          </div>

          <button
            type="button"
            onClick={onClose}
            className="h-8 w-8 rounded-xl border border-[var(--border-structural)] bg-[var(--canvas-card)] flex items-center justify-center text-[var(--text-muted)] hover:text-[var(--text-primary)] hover:bg-[var(--canvas-subtle)] transition-colors cursor-pointer"
            aria-label="Tutup"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Scrollable Body */}
        <div className="flex-1 overflow-y-auto p-5 space-y-4">
          {children}
        </div>

        {/* Footer Actions */}
        {footer && (
          <div className="p-4 border-t border-[var(--border-structural)] bg-[var(--canvas-app)]">
            {footer}
          </div>
        )}
      </aside>
    </div>
  );
}
