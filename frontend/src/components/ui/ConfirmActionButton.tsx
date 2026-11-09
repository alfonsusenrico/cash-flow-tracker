"use client";

import { useState } from "react";
import { useTranslations } from "next-intl";
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
  const t = useTranslations("common");
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
        className="min-h-11 rounded-md bg-[var(--danger-solid)] px-2.5 text-xs font-semibold text-[var(--danger-contrast)] hover:bg-[var(--danger-solid-hover)]"
      >
        {t("confirm")}
      </button>
      <button
        type="button"
        onClick={(event) => {
          stopEventPropagation(event);
          setIsConfirming(false);
        }}
        className="min-h-11 rounded-md border border-[var(--line)] px-2.5 text-xs font-semibold text-[var(--muted)]"
      >
        {t("cancel")}
      </button>
    </span>
  );
}
