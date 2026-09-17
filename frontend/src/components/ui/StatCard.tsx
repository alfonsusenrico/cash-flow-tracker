import React from "react";
import { cn } from "@/lib/utils";
import { StatusBadge, BadgeVariant } from "@/components/ui/StatusBadge";

interface StatCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  badge?: {
    text: string;
    variant?: BadgeVariant;
  };
  icon?: React.ReactNode;
  iconBg?: string;
  className?: string;
  onClick?: () => void;
}

export function StatCard({
  title,
  value,
  subtitle,
  badge,
  icon,
  iconBg = "bg-[#F2F2ED] dark:bg-[#272B27]",
  className,
  onClick,
}: StatCardProps) {
  return (
    <div
      onClick={onClick}
      className={cn(
        "card-crisp p-4 sm:p-5 flex flex-col justify-between transition-all select-none",
        onClick && "cursor-pointer hover:border-[var(--border-strong)] active:scale-[0.99]",
        className
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2.5 min-w-0">
          {icon && (
            <div
              className={cn(
                "h-8 w-8 rounded-xl flex items-center justify-center shrink-0 text-[#1A1D1A] dark:text-[#F5F7F5]",
                iconBg
              )}
            >
              {icon}
            </div>
          )}
          <span className="text-xs font-semibold text-[var(--text-muted)] truncate">
            {title}
          </span>
        </div>

        {badge && (
          <StatusBadge variant={badge.variant || "neutral"}>
            {badge.text}
          </StatusBadge>
        )}
      </div>

      <div className="mt-3.5 flex items-baseline justify-between gap-2">
        <div className="text-2xl sm:text-[28px] font-bold tracking-tight text-[var(--text-primary)] tabular leading-none">
          {value}
        </div>
      </div>

      {subtitle && (
        <div className="mt-2 text-[11px] text-[var(--text-muted)] font-medium truncate">
          {subtitle}
        </div>
      )}
    </div>
  );
}
