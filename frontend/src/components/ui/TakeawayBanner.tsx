import React from "react";
import { cn } from "@/lib/utils";

interface TakeawayBannerProps {
  title?: string;
  children: React.ReactNode;
  icon?: React.ReactNode;
  badgeText?: string;
  variant?: "sage" | "blue" | "neutral";
  className?: string;
}

export function TakeawayBanner({
  title = "Insight Keuangan",
  children,
  icon,
  badgeText = "Auto Takeaway",
  variant = "sage",
  className,
}: TakeawayBannerProps) {
  const isSage = variant === "sage";
  const isBlue = variant === "blue";

  return (
    <div
      className={cn(
        "rounded-2xl p-4 sm:p-4.5 flex items-start gap-3.5 border transition-all select-none",
        isSage && "bg-[#EBF7EE] dark:bg-emerald-950/20 border-[#D0EBD5] dark:border-emerald-800/30 text-[#1E7E34] dark:text-emerald-300",
        isBlue && "bg-[#EBF5FF] dark:bg-sky-950/20 border-[#BAE6FD] dark:border-sky-800/30 text-[#0369A1] dark:text-sky-300",
        !isSage && !isBlue && "bg-[#F8F8F6] dark:bg-[#202320] border-[var(--border-structural)] text-[var(--text-primary)]",
        className
      )}
    >
      <div
        className={cn(
          "h-8 w-8 rounded-xl flex items-center justify-center shrink-0 mt-0.5",
          isSage && "bg-[#D8F2DD] dark:bg-emerald-900/40 text-[#1E7E34] dark:text-emerald-300",
          isBlue && "bg-[#E0F2FE] dark:bg-sky-900/40 text-[#0369A1] dark:text-sky-300",
          !isSage && !isBlue && "bg-[#EFEFEA] text-[var(--text-secondary)]"
        )}
      >
        {icon || (
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2.2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
        )}
      </div>

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1">
          <span className="text-xs font-bold uppercase tracking-wider text-[var(--text-secondary)]">
            {title}
          </span>
          {badgeText && (
            <span
              className={cn(
                "text-[10px] font-bold px-1.5 py-0.2 rounded-full uppercase tracking-tight",
                isSage && "bg-white/80 dark:bg-emerald-900/60 text-[#1E7E34] dark:text-emerald-300 border border-[#D0EBD5]",
                isBlue && "bg-white/80 dark:bg-sky-900/60 text-[#0369A1] dark:text-sky-300 border border-[#BAE6FD]",
                !isSage && !isBlue && "bg-[var(--canvas-card)] text-[var(--text-muted)] border border-[var(--border-structural)]"
              )}
            >
              {badgeText}
            </span>
          )}
        </div>

        <div className="text-xs sm:text-[13px] leading-relaxed text-[var(--text-primary)] font-medium">
          {children}
        </div>
      </div>
    </div>
  );
}
