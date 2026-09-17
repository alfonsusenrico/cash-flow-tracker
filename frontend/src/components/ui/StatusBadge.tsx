import React from "react";
import { cn } from "@/lib/utils";

export type BadgeVariant =
  | "success"
  | "warning"
  | "danger"
  | "info"
  | "purple"
  | "neutral";

interface StatusBadgeProps {
  variant?: BadgeVariant;
  children: React.ReactNode;
  className?: string;
  dot?: boolean;
}

const VARIANT_CLASSES: Record<BadgeVariant, { bg: string; text: string; dot: string }> = {
  success: {
    bg: "bg-[#E8F8EA] dark:bg-emerald-950/40",
    text: "text-[#1E7E34] dark:text-emerald-400",
    dot: "bg-[#1E7E34] dark:bg-emerald-400",
  },
  warning: {
    bg: "bg-[#FFF3E0] dark:bg-amber-950/40",
    text: "text-[#B45309] dark:text-amber-400",
    dot: "bg-[#B45309] dark:bg-amber-400",
  },
  danger: {
    bg: "bg-[#FDE8E8] dark:bg-rose-950/40",
    text: "text-[#DC2626] dark:text-rose-400",
    dot: "bg-[#DC2626] dark:bg-rose-400",
  },
  info: {
    bg: "bg-[#EBF5FF] dark:bg-sky-950/40",
    text: "text-[#0369A1] dark:text-sky-400",
    dot: "bg-[#0369A1] dark:bg-sky-400",
  },
  purple: {
    bg: "bg-[#F3E8FF] dark:bg-purple-950/40",
    text: "text-[#7E22CE] dark:text-purple-400",
    dot: "bg-[#7E22CE] dark:bg-purple-400",
  },
  neutral: {
    bg: "bg-[#F2F2ED] dark:bg-[#2A2E2A]",
    text: "text-[#4A4D48] dark:text-[#C4C8C2]",
    dot: "bg-[#787A74] dark:text-[#8E948B]",
  },
};

export function StatusBadge({
  variant = "neutral",
  children,
  className,
  dot = false,
}: StatusBadgeProps) {
  const c = VARIANT_CLASSES[variant];

  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[11px] font-semibold tracking-tight whitespace-nowrap transition-colors",
        c.bg,
        c.text,
        className
      )}
    >
      {dot && <span className={cn("h-1.5 w-1.5 rounded-full shrink-0", c.dot)} />}
      <span>{children}</span>
    </span>
  );
}
