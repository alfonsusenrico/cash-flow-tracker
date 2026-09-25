"use client";

import { useState } from "react";
import { cn } from "@/lib/utils";

interface ConfirmActionButtonProps {
  label: string;
  confirmation: string;
  children: React.ReactNode;
  onConfirm: () => void;
  className?: string;
  disabled?: boolean;
  stopPropagation?: boolean;
}

export function ConfirmActionButton({
  label,
  confirmation,
  children,
  onConfirm,
  className,
  disabled = false,
  stopPropagation = false,
}: ConfirmActionButtonProps) {
  const [isConfirming, setIsConfirming] = useState(false);
  const stopEventPropagation = (event: React.MouseEvent) => {
    if (stopPropagation) event.stopPropagation();
  };

  if (!isConfirming) {
    return (
      <button
        type="button"
        aria-label={label}
        onClick={(event) => {
          stopEventPropagation(event);
          setIsConfirming(true);
        }}
        disabled={disabled}
        className={cn("inline-flex min-h-11 min-w-11 items-center justify-center", className)}
      >
        {children}
      </button>
    );
  }

  return (
    <span className="inline-flex items-center gap-1.5" role="group" aria-label={confirmation}>
      <span className="sr-only" role="alert">{confirmation}</span>
      <button
        type="button"
        onClick={(event) => {
          stopEventPropagation(event);
          setIsConfirming(false);
          onConfirm();
        }}
        disabled={disabled}
        className={cn("min-h-11 rounded-lg bg-rose-600 px-2.5 text-[11px] font-semibold text-white", className)}
      >
        Ya, lanjutkan
      </button>
      <button
        type="button"
        onClick={(event) => {
          stopEventPropagation(event);
          setIsConfirming(false);
        }}
        className="min-h-11 rounded-lg border border-[var(--border)] px-2.5 text-[11px] font-semibold text-[var(--muted)]"
      >
        Batal
      </button>
    </span>
  );
}
