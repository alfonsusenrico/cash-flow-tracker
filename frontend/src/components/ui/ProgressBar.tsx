import { cn } from "@/lib/utils";

interface ProgressBarProps {
  value: number; // 0-100
  max?: number;
  color?: "green" | "yellow" | "red" | "blue" | "orange";
  intent?: "usage" | "completion" | "quota";
  size?: "sm" | "md";
  className?: string;
  showLabel?: boolean;
}

const COLORS = {
  green: "bg-[var(--primary)]",
  yellow: "bg-[var(--warning)]",
  red: "bg-[var(--danger)]",
  blue: "bg-[var(--info)]",
  orange: "bg-[var(--warning)]",
};

export function ProgressBar({ value, max = 100, color = "green", intent = "usage", size = "sm", className, showLabel }: ProgressBarProps) {
  const pct = Math.min(Math.max((value / max) * 100, 0), 100);
  const usageColor = pct >= 80 ? "red" : pct >= 60 ? "yellow" : "green";
  const quotaColor = pct <= 20 ? "red" : pct <= 40 ? "yellow" : "green";
  const autoColor = intent === "quota" ? quotaColor : usageColor;
  const barColor = COLORS[(intent === "usage" || intent === "quota") && color === "green" ? autoColor : color];
  const h = size === "sm" ? "h-1.5" : "h-2";

  return (
    <div className={cn("flex items-center gap-2", className)}>
      <div className={cn("flex-1 bg-[var(--color-paper-3)] rounded-full overflow-hidden", h)}>
        <div className={cn("h-full rounded-full transition-all", barColor)} style={{ width: `${pct}%` }} />
      </div>
      {showLabel && <span className="text-xs text-[var(--muted)] tabular w-8 text-right">{Math.round(pct)}%</span>}
    </div>
  );
}
